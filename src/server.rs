//! Database registry, a port of couch_server.

use crate::db::Db;
use crate::doc::is_valid_db_name;
use crate::config::Config;
use crate::error::{CouchError, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Server {
    pub data_dir: PathBuf,
    pub config: Arc<Config>,
    pub uuid: String,
    dbs: Mutex<HashMap<String, Arc<Db>>>,
    /// Bumped whenever databases are created or deleted.
    pub db_events: tokio::sync::broadcast::Sender<(String, &'static str)>,
}

impl Server {
    pub fn new(data_dir: PathBuf, config: Arc<Config>) -> Result<Arc<Server>> {
        std::fs::create_dir_all(&data_dir)?;
        crate::stats::init(config.clone());
        let uuid = prepare_config(&data_dir, &config)?;
        let (tx, _) = tokio::sync::broadcast::channel(256);
        Ok(Arc::new(Server {
            data_dir,
            config,
            uuid,
            dbs: Mutex::new(HashMap::new()),
            db_events: tx,
        }))
    }

    pub fn db_path(&self, name: &str) -> PathBuf {
        // Each database is a RocksDB directory.
        self.data_dir.join(format!("{name}.rocks"))
    }

    pub fn index_dir(&self, name: &str) -> PathBuf {
        self.data_dir.join(format!(".{name}_design"))
    }

    fn check_name(name: &str) -> Result<()> {
        if !is_valid_db_name(name) {
            return Err(CouchError::bad_request(
                "Name: '".to_string()
                    + name
                    + "'. Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed. Must begin with a letter.",
            ));
        }
        Ok(())
    }

    pub fn create_db(&self, name: &str) -> Result<Arc<Db>> {
        self.create_db_with(name, &serde_json::json!({}))
    }

    /// Create a database with properties ({"partitioned": true}).
    pub fn create_db_with(&self, name: &str, props: &serde_json::Value) -> Result<Arc<Db>> {
        Self::check_name(name)?;
        let mut dbs = self.dbs.lock();
        let path = self.db_path(name);
        if dbs.contains_key(name) || crate::db::exists(&path) {
            return Err(CouchError::file_exists());
        }
        let db = Db::create(&path, name, &crate::auth::default_security(&self.config), props)?;
        dbs.insert(name.to_string(), db.clone());
        let _ = self.db_events.send((name.to_string(), "created"));
        Ok(db)
    }

    pub fn open_db(&self, name: &str) -> Result<Arc<Db>> {
        let mut dbs = self.dbs.lock();
        if let Some(db) = dbs.get(name) {
            return Ok(db.clone());
        }
        if !is_valid_db_name(name) {
            return Err(CouchError::not_found("Database does not exist."));
        }
        let t = std::time::Instant::now();
        let db = Db::open(&self.db_path(name), name, false)?;
        crate::stat!("couchdb.db_open_time").observe(crate::stats::ms_since(t));
        dbs.insert(name.to_string(), db.clone());
        Ok(db)
    }

    pub fn delete_db(&self, name: &str) -> Result<()> {
        let mut dbs = self.dbs.lock();
        // Deleting goes through an open instance even when none is
        // cached: requests may still hold an earlier one, and only the
        // instance knows which files are its own.
        let db = match dbs.remove(name) {
            Some(db) => db,
            None => Db::open(&self.db_path(name), name, false)?,
        };
        db.delete_files();
        let _ = std::fs::remove_dir_all(self.index_dir(name));
        let _ = self.db_events.send((name.to_string(), "deleted"));
        Ok(())
    }

    /// Each open database's writer queue length.
    pub fn writer_queues(&self) -> Vec<u64> {
        self.dbs.lock().values().map(|d| d.queue_len()).collect()
    }

    pub fn all_dbs(&self) -> Vec<String> {
        let mut out = Vec::new();
        walk(&self.data_dir, &self.data_dir, &mut out);
        out.sort();
        out
    }
}

impl Server {
    /// What a single node does at start (setup's enable_single_node and
    /// chttpd_auth_cache): the users and replicator databases exist, and
    /// the users database has its _design/_auth validator.
    pub async fn ensure_system_dbs(&self) -> Result<()> {
        let users = crate::auth::users_db_name(&self.config);
        if self.config.get_bool("couchdb", "single_node", true) {
            for name in [users.as_str(), "_replicator"] {
                if !crate::db::exists(&self.db_path(name)) {
                    match self.create_db(name) {
                        Ok(_) => {}
                        Err(e) if e.error == "file_exists" => {}
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        if let Ok(db) = self.open_db(&users) {
            ensure_auth_ddoc(&db).await?;
        }
        Ok(())
    }
}

/// Write the users database's _design/_auth validator if it's missing
/// or differs from this server's.
pub async fn ensure_auth_ddoc(db: &Arc<Db>) -> Result<()> {
    let snap = db.snapshot();
    let mut ddoc = serde_json::json!({
        "_id": "_design/_auth",
        "language": "javascript",
        "validate_doc_update": crate::auth::AUTH_VALIDATE,
    });
    if let Some(fdi) = snap.get_fdi("_design/_auth")?
        && !fdi.deleted
    {
        let cur = snap.winner_json(&fdi, &crate::doc::DocOpts::default())?;
        if cur.get("validate_doc_update").and_then(|v| v.as_str()) == Some(crate::auth::AUTH_VALIDATE) {
            return Ok(());
        }
        ddoc["_rev"] = cur["_rev"].clone();
    }
    let doc = crate::doc::from_json(ddoc)?;
    match db.update_docs(vec![doc], true).await?.pop() {
        Some(Err(e)) if e.error != "conflict" => Err(e),
        _ => Ok(()),
    }
}

/// First-start and every-start config duties, as couch_server and the
/// password hasher do them: a server uuid and cookie secret are generated
/// and saved once, and plaintext admin passwords are replaced by hashes
/// (saved, unless they came from the command line). Returns the uuid.
fn prepare_config(data_dir: &Path, config: &Config) -> Result<String> {
    let uuid = match config.get("couchdb", "uuid") {
        Some(u) => u,
        None => {
            // Servers from before the config file kept it in .uuid.
            let u = std::fs::read_to_string(data_dir.join(".uuid"))
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
            config.set("couchdb", "uuid", &u, true)?;
            u
        }
    };
    if config.get("chttpd_auth", "secret").is_none() {
        config.set("chttpd_auth", "secret", &uuid::Uuid::new_v4().simple().to_string(), true)?;
    }
    for (name, value) in config.section("admins") {
        if !crate::passwords::is_hashed(&value) {
            let hashed = crate::passwords::hash_admin_password(config, &value);
            config.set("admins", &name, &hashed, !config.is_override("admins", &name))?;
        }
    }
    Ok(uuid)
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        let fname = e.file_name().to_string_lossy().to_string();
        if fname.starts_with('.') {
            continue;
        }
        if let Some(stem) = fname.strip_suffix(".rocks").filter(|_| p.is_dir()) {
            if !crate::db::exists(&p) {
                continue;
            }
            let rel = p.parent().unwrap().strip_prefix(root).unwrap();
            let name = if rel.as_os_str().is_empty() {
                stem.to_string()
            } else {
                format!("{}/{}", rel.to_string_lossy(), stem)
            };
            if is_valid_db_name(&name) {
                out.push(name);
            }
        } else if p.is_dir() {
            // Database names with '/' live in subdirectories.
            walk(root, &p, out);
        }
    }
}
