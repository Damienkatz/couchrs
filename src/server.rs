//! Database registry, a port of couch_server.

use crate::db::Db;
use crate::doc::is_valid_db_name;
use crate::error::{CouchError, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Config {
    pub data_dir: PathBuf,
    pub bind: String,
    /// Optional admin credentials; when absent the server is in admin party.
    pub admin: Option<(String, String)>,
}

pub struct Server {
    pub config: Config,
    pub uuid: String,
    dbs: Mutex<HashMap<String, Arc<Db>>>,
    /// Bumped whenever databases are created or deleted.
    pub db_events: tokio::sync::broadcast::Sender<(String, &'static str)>,
}

impl Server {
    pub fn new(config: Config) -> Result<Arc<Server>> {
        std::fs::create_dir_all(&config.data_dir)?;
        let uuid_path = config.data_dir.join(".uuid");
        let uuid = match std::fs::read_to_string(&uuid_path) {
            Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => {
                let u = uuid::Uuid::new_v4().simple().to_string();
                std::fs::write(&uuid_path, &u)?;
                u
            }
        };
        let (tx, _) = tokio::sync::broadcast::channel(256);
        Ok(Arc::new(Server {
            config,
            uuid,
            dbs: Mutex::new(HashMap::new()),
            db_events: tx,
        }))
    }

    pub fn db_path(&self, name: &str) -> PathBuf {
        // Each database is a RocksDB directory.
        self.config.data_dir.join(format!("{name}.rocks"))
    }

    pub fn index_dir(&self, name: &str) -> PathBuf {
        self.config.data_dir.join(format!(".{name}_design"))
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
        Self::check_name(name)?;
        let mut dbs = self.dbs.lock();
        let path = self.db_path(name);
        if dbs.contains_key(name) || crate::db::exists(&path) {
            return Err(CouchError::file_exists());
        }
        let db = Db::open(&path, name, true)?;
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
        let db = Db::open(&self.db_path(name), name, false)?;
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

    pub fn all_dbs(&self) -> Vec<String> {
        let mut out = Vec::new();
        walk(&self.config.data_dir, &self.config.data_dir, &mut out);
        out.sort();
        out
    }
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
