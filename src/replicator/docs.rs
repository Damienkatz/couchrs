//! Replications kept as documents in _replicator databases
//! (couch_replicator_doc_processor and couch_replicator_docs). Every
//! database named `_replicator` or ending in `/_replicator` is followed
//! from the start: each doc is checked, given its replication id (for a
//! design doc filter, from the filter's code, rechecked now and then) and
//! added to the scheduler. A one-shot replication that finishes marks its
//! doc `completed`; a doc that can't run is marked `failed`, with why.
//! Docs are checked when written, get an owner, and show other users
//! their URLs without credentials.

use super::RepError;
use super::rep::{self, Rep};
use super::scheduler;
use crate::doc::Doc;
use crate::error::{CouchError, Result};
use crate::http::{Req, ok_json};
use crate::server::Server;
use axum::http::StatusCode;
use axum::response::Response;
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, SystemTime};

const STATES: &str = "[initializing,error,running,pending,crashing,completed,failed]";
const WORKER_TIMEOUT: Duration = Duration::from_secs(61);
const ERROR_MAX_BACKOFF_EXPONENT: u32 = 12;
const INITIAL_BACKOFF_EXPONENT: u64 = 64;
const MIN_FILTER_DELAY_SEC: u64 = 60;

pub fn is_replicator_db(name: &str) -> bool {
    name == "_replicator" || name.ends_with("/_replicator")
}

fn secs(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn iso(t: SystemTime) -> String {
    crate::auth::iso8601(secs(t))
}

/// How a bad doc's error reads (couch_replicator_parse's bad_rep_doc).
fn bad_doc_reason(e: &RepError) -> String {
    match e {
        RepError::BadRequest(m) => format!("{{throw,{{bad_request,<<\"{m}\">>}}}}"),
        other => other.erl_text(),
    }
}

// ------------------------------------------------------- doc write hooks

/// before_doc_update/3 for replicator databases: an owner, and a valid
/// replication.
pub fn before_doc_update(server: &Server, user: &crate::auth::UserCtx, sec: &Value, doc: &mut Doc, replicated: bool) -> Result<()> {
    if doc.is_design() || doc.is_local() || replicated {
        return Ok(());
    }
    let name = user.name.clone().map(Value::String).unwrap_or(Value::Null);
    match doc.body.get("owner").cloned() {
        None => {
            doc.body.insert("owner".into(), name);
        }
        Some(o) if o == name => {}
        Some(o) => {
            if !crate::auth::is_db_admin(user, sec) {
                return Err(CouchError::forbidden("Can't update replication documents from other users."));
            }
            if o.is_null() {
                doc.body.insert("owner".into(), name);
            }
        }
    }
    let failed = doc.body.get("_replication_state").and_then(Value::as_str) == Some("failed");
    if !doc.deleted && !failed {
        rep::parse_without_id(&doc.body, rep::doc_user_ctx(&doc.body), server).map_err(|e| CouchError::forbidden(bad_doc_reason(&e)))?;
    }
    Ok(())
}

/// after_doc_read/2: others' docs lose their credentials (and so get a
/// different revision id).
pub fn after_doc_read(user: &crate::auth::UserCtx, sec: &Value, mut v: Value) -> Value {
    let id = v.get("_id").and_then(Value::as_str).unwrap_or("").to_string();
    if id.starts_with("_design/") || crate::auth::is_db_admin(user, sec) {
        return v;
    }
    let owner = v.get("owner").cloned().unwrap_or(Value::Null);
    if owner == user.name.clone().map(Value::String).unwrap_or(Value::Null) {
        return v;
    }
    let Some(o) = v.as_object_mut() else { return v };
    for k in ["source", "target", "proxy", "source_proxy", "target_proxy"] {
        if let Some(x) = o.get(k).cloned() {
            o.insert(k.into(), strip_credentials(&x));
        }
    }
    if let Some((pos, rev)) = o.get("_rev").and_then(Value::as_str).and_then(|r| r.split_once('-').map(|(p, r)| (p.to_string(), r.to_string()))) {
        // The body as CouchDB holds it: fields, and the special ones it keeps.
        let kept = |k: &str| !k.starts_with('_') || k.starts_with("_replication_") || k == "_access";
        let body: Map<String, Value> = o.iter().filter(|(k, _)| kept(k)).map(|(k, v)| (k.clone(), v.clone())).collect();
        let doc = Doc { id: id.clone(), revs: (0, vec![]), deleted: false, body, atts: vec![], att_gen: 0 };
        let pos: u64 = pos.parse().unwrap_or(1);
        // new_revid of the doc as it was, at its own position.
        let new = crate::doc::new_rev_id(&doc, Some((pos, &rev)));
        o.insert("_rev".into(), json!(format!("{pos}-{new}")));
    }
    v
}

fn strip_credentials(v: &Value) -> Value {
    match v {
        Value::String(url) => strip_url_userinfo(url),
        Value::Object(o) => {
            let mut o = o.clone();
            o.remove("headers");
            o.remove("auth");
            if let Some(Value::String(u)) = o.get("url").cloned() {
                o.insert("url".into(), strip_url_userinfo(&u));
            }
            Value::Object(o)
        }
        _ => Value::Null,
    }
}

fn strip_url_userinfo(url: &str) -> Value {
    let re = regex::Regex::new(r"(?s)^(\s*[a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]*@").expect("regex");
    json!(re.replace(url, "$1").into_owned())
}

// ------------------------------------------------------- doc processing

#[derive(Clone, Copy, PartialEq, Debug)]
enum RState {
    Initializing,
    Error,
    Scheduled,
    Pending,
}

impl RState {
    fn name(self) -> &'static str {
        match self {
            RState::Initializing => "initializing",
            RState::Error => "error",
            RState::Scheduled => "scheduled",
            RState::Pending => "pending",
        }
    }
}

struct RDoc {
    state: RState,
    rep: Rep,
    rid: Option<String>,
    user_filter: bool,
    info: Option<String>,
    errcnt: u32,
    worker: Option<u64>,
    last_updated: SystemTime,
}

pub struct Processor {
    server: Weak<Server>,
    docs: Mutex<HashMap<(String, String), RDoc>>,
    next_ref: AtomicU64,
    watchers: Mutex<HashMap<String, tokio::task::AbortHandle>>,
}

static PROCESSOR: OnceLock<Arc<Processor>> = OnceLock::new();

enum StartResult {
    Ok(String),
    Ignore,
    Temporary(String),
    /// Failed for good (the doc is marked failed).
    Permanent,
}

pub fn start(server: &Arc<Server>) {
    let p = PROCESSOR
        .get_or_init(|| {
            Arc::new(Processor { server: Arc::downgrade(server), docs: Mutex::new(HashMap::new()), next_ref: AtomicU64::new(1), watchers: Mutex::new(HashMap::new()) })
        })
        .clone();
    let default_delay = server.config.get_int("replicator", "cluster_start_period", 5u64) * 1000;
    let delay = server.config.get_int("replicator", "startup_delay", default_delay.max(500));
    let server = server.clone();
    tokio::spawn(async move {
        let mut events = crate::db::events().subscribe();
        tokio::time::sleep(Duration::from_millis(delay)).await;
        for name in server.all_dbs() {
            if is_replicator_db(&name) {
                p.watch(name);
            }
        }
        loop {
            match events.recv().await {
                Ok((name, crate::db::DbEvent::Created)) if is_replicator_db(&name) => p.watch(name),
                Ok((name, crate::db::DbEvent::Deleted)) if is_replicator_db(&name) => {
                    crate::stat!("couch_replicator.docs.dbs_deleted").inc();
                    if let Some(h) = p.watchers.lock().remove(&name) {
                        h.abort();
                    }
                    p.removed_db(&name);
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => return,
            }
        }
    });
}

/// Whether doc states are written for triggered and erroring docs too.
fn update_docs(server: &Server) -> bool {
    server.config.get_bool("replicator", "update_docs", false)
}

impl Processor {
    /// Follow a replicator db's changes from the start.
    fn watch(self: &Arc<Self>, name: String) {
        crate::stat!("couch_replicator.docs.dbs_found").inc();
        let p = self.clone();
        let n = name.clone();
        let h = tokio::spawn(async move {
            let Some(server) = p.server.upgrade() else { return };
            let Ok(db) = server.open_db(&n) else { return };
            let mut rx = db.subscribe();
            let mut since = 0u64;
            loop {
                let snap = db.snapshot();
                let mut changed: Vec<(String, Option<Map<String, Value>>)> = Vec::new();
                let _ = snap.fold_changes(since, crate::btree::Dir::Fwd, |fdi| {
                    if fdi.id.starts_with("_design/") {
                        return Ok(true);
                    }
                    let doc = if fdi.deleted { None } else { snap.winner_json(fdi, &crate::doc::DocOpts::default()).ok().and_then(|v| v.as_object().cloned()) };
                    changed.push((fdi.id.clone(), doc));
                    Ok(true)
                });
                since = snap.update_seq();
                drop(snap);
                drop(server.clone());
                for (id, doc) in changed {
                    p.process_change(&n, &id, doc).await;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            }
        });
        self.watchers.lock().insert(name, h.abort_handle());
    }

    async fn process_change(self: &Arc<Self>, db: &str, id: &str, doc: Option<Map<String, Value>>) {
        crate::stat!("couch_replicator.docs.db_changes").inc();
        let Some(server) = self.server.upgrade() else { return };
        let key = (db.to_string(), id.to_string());
        let Some(doc) = doc else {
            self.removed_doc(&key);
            return;
        };
        match doc.get("_replication_state").and_then(Value::as_str) {
            Some("completed") => {
                self.docs.lock().remove(&key);
            }
            Some("failed") => {}
            state => {
                if matches!(state, Some("triggered") | Some("error")) && !update_docs(&server) {
                    remove_state_fields(&server, db, id).await;
                }
                if let Err(e) = self.process_updated(&server, &key, &doc) {
                    update_failed(&server, db, id, &bad_doc_reason(&e)).await;
                }
            }
        }
    }

    fn process_updated(self: &Arc<Self>, server: &Server, key: &(String, String), doc: &Map<String, Value>) -> std::result::Result<(), RepError> {
        let mut r = rep::parse_without_id(doc, rep::doc_user_ctx(doc), server)?;
        r.db_name = Some(key.0.clone());
        r.doc_id = Some(key.1.clone());
        r.start_time = SystemTime::now();
        let user_filter = matches!(rep::filter(&r.options)?, rep::Filter::User(..));
        self.updated_doc(key, r, user_filter);
        Ok(())
    }

    fn current_rep(&self, key: &(String, String)) -> Option<Rep> {
        let docs = self.docs.lock();
        let row = docs.get(key)?;
        if row.state == RState::Scheduled
            && let Some(id) = &row.rid
            && let Some(r) = scheduler::get().and_then(|s| s.rep_state(id))
        {
            return Some(r);
        }
        Some(row.rep.clone())
    }

    fn updated_doc(self: &Arc<Self>, key: &(String, String), r: Rep, user_filter: bool) {
        let same = self.current_rep(key).is_some_and(|cur| {
            cur.source.url == r.source.url
                && cur.target.url == r.target.url
                && cur.source.headers == r.source.headers
                && cur.target.headers == r.target.headers
                && cur.source.basic == r.source.basic
                && cur.target.basic == r.target.basic
                && format!("{:?}", cur.options) == format!("{:?}", r.options)
        });
        if same {
            return;
        }
        self.removed_doc(key);
        self.docs.lock().insert(
            key.clone(),
            RDoc { state: RState::Initializing, rep: r, rid: None, user_filter, info: None, errcnt: 0, worker: None, last_updated: SystemTime::now() },
        );
        self.maybe_start_worker(key);
    }

    fn removed_doc(&self, key: &(String, String)) {
        self.docs.lock().remove(key);
        if let Some(s) = scheduler::get() {
            for id in s.jobs_of_doc(&key.0, Some(&key.1)) {
                s.remove_job(&id);
            }
        }
    }

    fn removed_db(&self, db: &str) {
        self.docs.lock().retain(|(d, _), _| d != db);
        if let Some(s) = scheduler::get() {
            for id in s.jobs_of_doc(db, None) {
                s.remove_job(&id);
            }
        }
    }

    fn maybe_start_worker(self: &Arc<Self>, key: &(String, String)) {
        let (rep, wait, wref) = {
            let mut docs = self.docs.lock();
            let total = docs.len();
            let Some(row) = docs.get_mut(key) else { return };
            if row.state == RState::Scheduled && !row.user_filter {
                return;
            }
            let wait = match row.state {
                // Design doc filters are fetched again now and then.
                RState::Scheduled => {
                    let range = 1.0 + (2.0 * (total as f64 / 10.0)).min(86400.0);
                    MIN_FILTER_DELAY_SEC + 1 + rand::random::<u64>() % (range.round() as u64).max(1)
                }
                RState::Error => {
                    let exp = row.errcnt.min(ERROR_MAX_BACKOFF_EXPONENT);
                    1 + rand::random::<u64>() % (INITIAL_BACKOFF_EXPONENT << exp)
                }
                RState::Initializing | RState::Pending => 0,
            };
            let wref = self.next_ref.fetch_add(1, Ordering::Relaxed);
            row.worker = Some(wref);
            (row.rep.clone(), wait, wref)
        };
        let (p, key) = (self.clone(), key.clone());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(wait)).await;
            let res = match tokio::time::timeout(WORKER_TIMEOUT, p.maybe_start_replication(&key, rep, wref)).await {
                Ok(r) => r,
                Err(_) => StartResult::Temporary(format!(
                    "Replication for db \"{}\" doc \"{}\" failed to start due to timeout after {} seconds",
                    key.0,
                    key.1,
                    WORKER_TIMEOUT.as_secs()
                )),
            };
            p.worker_returned(wref, &key, res).await;
        });
    }

    async fn maybe_start_replication(&self, key: &(String, String), mut r: Rep, wref: u64) -> StartResult {
        let Some(server) = self.server.upgrade() else { return StartResult::Ignore };
        r.id = match rep::replication_id(&r, &server).await {
            Ok(id) => id,
            Err(e) => return StartResult::Temporary(e.erl_text()),
        };
        let id = r.id_str();
        let Some(sched) = scheduler::get() else { return StartResult::Ignore };
        match sched.rep_state(&id) {
            None => {
                let current = self.docs.lock().get(key).and_then(|d| d.worker) == Some(wref);
                if current {
                    drop(sched.add_job(r));
                    StartResult::Ok(id)
                } else {
                    StartResult::Ignore
                }
            }
            Some(other) if other.doc_id.as_deref() == Some(key.1.as_str()) => StartResult::Ok(id),
            Some(other) if other.doc_id.is_none() => StartResult::Temporary(format!(
                "Replication `{id}` specified by document `{}` already running as a transient replication, started via `_replicate` API endpoint",
                key.1
            )),
            Some(other) => {
                let msg = format!(
                    "Replication `{id}` specified by document `{}` already started, triggered by document `{}` from db `{}`",
                    key.1,
                    other.doc_id.unwrap_or_default(),
                    other.db_name.unwrap_or_default()
                );
                update_failed(&server, &key.0, &key.1, &msg).await;
                StartResult::Permanent
            }
        }
    }

    async fn worker_returned(self: &Arc<Self>, wref: u64, key: &(String, String), res: StartResult) {
        let Some(server) = self.server.upgrade() else { return };
        let mut triggered = None;
        let mut errored = None;
        {
            let mut docs = self.docs.lock();
            let Some(row) = docs.get_mut(key) else { return };
            if row.worker != Some(wref) {
                return;
            }
            match res {
                StartResult::Ok(id) => {
                    row.state = RState::Scheduled;
                    row.errcnt = 0;
                    row.worker = None;
                    row.last_updated = SystemTime::now();
                    match row.rid.clone() {
                        Some(old) if row.user_filter && old != id => {
                            if let Some(s) = scheduler::get() {
                                s.remove_job(&old);
                            }
                            row.info = Some(format!("Replication id changed: {old} -> {id}"));
                        }
                        Some(_) if row.user_filter => {}
                        _ => row.info = None,
                    }
                    row.rid = Some(id.clone());
                    triggered = Some(id);
                }
                StartResult::Ignore => return,
                StartResult::Temporary(reason) => {
                    row.rid = None;
                    row.state = RState::Error;
                    row.info = Some(reason.clone());
                    row.errcnt += 1;
                    row.worker = None;
                    row.last_updated = SystemTime::now();
                    errored = Some(reason);
                }
                StartResult::Permanent => {
                    docs.remove(key);
                    return;
                }
            }
        }
        if update_docs(&server) {
            if let Some(id) = triggered {
                update_rep_doc(&server, &key.0, &key.1, vec![
                    ("_replication_state", Some(json!("triggered"))),
                    ("_replication_state_reason", None),
                    ("_replication_id", Some(json!(id))),
                    ("_replication_stats", None),
                ])
                .await;
            } else if let Some(reason) = errored {
                update_rep_doc(&server, &key.0, &key.1, vec![
                    ("_replication_state", Some(json!("error"))),
                    ("_replication_state_reason", Some(json!(reason))),
                    ("_replication_stats", None),
                    ("_replication_id", Some(Value::Null)),
                ])
                .await;
            }
        }
        self.maybe_start_worker(key);
    }

    /// ejson_doc/2: a processed doc's state.
    fn doc_json(&self, key: &(String, String)) -> Option<Map<String, Value>> {
        let docs = self.docs.lock();
        let row = docs.get(key)?;
        let mut m = Map::new();
        m.insert("doc_id".into(), json!(key.1));
        m.insert("database".into(), json!(key.0));
        if row.state == RState::Scheduled
            && let Some(id) = &row.rid
            && let Some(summary) = scheduler::get().and_then(|s| s.job_summary(id))
        {
            m.insert("id".into(), json!(id));
            m.insert("node".into(), json!(crate::http::NODE_NAME));
            m.extend(summary);
            return Some(m);
        }
        let state = if row.state == RState::Scheduled { RState::Pending } else { row.state };
        m.insert("id".into(), json!(row.rid));
        m.insert("state".into(), json!(state.name()));
        m.insert("info".into(), row.info.as_ref().map(|i| json!({"error": i})).unwrap_or(Value::Null));
        m.insert("error_count".into(), json!(row.errcnt));
        m.insert("node".into(), json!(crate::http::NODE_NAME));
        m.insert("last_updated".into(), json!(iso(row.last_updated)));
        m.insert("start_time".into(), json!(iso(row.rep.start_time)));
        Some(m)
    }
}

// ------------------------------------------------------------ doc states

/// Apply field changes to a replicator doc, as an admin; a changed
/// _replication_state also gets a new _replication_state_time.
async fn update_rep_doc(server: &Server, db: &str, doc_id: &str, kvs: Vec<(&str, Option<Value>)>) {
    let Ok(db) = server.open_db(db) else { return };
    let mut wait = 1u64;
    for _ in 0..10 {
        let snap = db.snapshot();
        let Ok(Some(fdi)) = snap.get_fdi(doc_id) else { return };
        if fdi.deleted {
            return;
        }
        let Ok(Value::Object(cur)) = snap.winner_json(&fdi, &crate::doc::DocOpts::default()) else { return };
        let mut new = cur.clone();
        for (k, v) in &kvs {
            match v {
                None => {
                    new.remove(*k);
                }
                Some(v) if *k == "_replication_state" => {
                    if new.get(*k) != Some(v) {
                        new.insert(k.to_string(), v.clone());
                        new.insert("_replication_state_time".into(), json!(crate::auth::iso8601(crate::http::now_secs())));
                    }
                }
                Some(v) => {
                    new.insert(k.to_string(), v.clone());
                }
            }
        }
        if new == cur {
            return;
        }
        let Ok(doc) = crate::doc::from_json(Value::Object(new)) else { return };
        match db.update_docs(vec![doc], true).await.map(|mut r| r.pop()) {
            Ok(Some(Ok(_))) => return,
            Ok(Some(Err(e))) if e.error == "conflict" => {
                tracing::error!("Conflict when updating replication doc `{doc_id}`. Retrying.");
                tokio::time::sleep(Duration::from_millis(100 * (1 + rand::random::<u64>() % wait.min(128)))).await;
                wait *= 2;
            }
            _ => return,
        }
    }
}

async fn remove_state_fields(server: &Server, db: &str, doc: &str) {
    update_rep_doc(server, db, doc, vec![
        ("_replication_state", None),
        ("_replication_state_time", None),
        ("_replication_state_reason", None),
        ("_replication_id", None),
        ("_replication_stats", None),
    ])
    .await;
}

async fn update_failed(server: &Server, db: &str, doc: &str, reason: &str) {
    tracing::error!("Error processing replication doc `{doc}` from `{db}`: {reason}");
    update_rep_doc(server, db, doc, vec![
        ("_replication_state", Some(json!("failed"))),
        ("_replication_stats", None),
        ("_replication_state_reason", Some(json!(reason))),
    ])
    .await;
    crate::stat!("couch_replicator.docs.failed_state_updates").inc();
}

/// A doc's one-shot replication finished: mark it completed, with its
/// stats.
pub fn completed(db: String, doc: String, start: SystemTime, info: Value) {
    let Some(server) = PROCESSOR.get().and_then(|p| p.server.upgrade()) else { return };
    let mut stats = info.as_object().cloned().unwrap_or_default();
    stats.remove("source_seq");
    stats.remove("through_seq");
    stats.insert("start_time".into(), json!(iso(start)));
    tokio::spawn(async move {
        update_rep_doc(&server, &db, &doc, vec![
            ("_replication_state", Some(json!("completed"))),
            ("_replication_state_reason", None),
            ("_replication_stats", Some(Value::Object(stats))),
        ])
        .await;
        crate::stat!("couch_replicator.docs.completed_state_updates").inc();
        tracing::info!("Replication completed (triggered by `{doc}`)");
    });
}

/// A doc's replication crashed: its doc says so only with
/// [replicator] update_docs.
pub fn crashed(db: String, doc: String, id: String, reason: String) {
    let Some(server) = PROCESSOR.get().and_then(|p| p.server.upgrade()) else { return };
    if !update_docs(&server) {
        return;
    }
    tokio::spawn(async move {
        update_rep_doc(&server, &db, &doc, vec![
            ("_replication_state", Some(json!("error"))),
            ("_replication_state_reason", Some(json!(reason))),
            ("_replication_stats", None),
            ("_replication_id", Some(json!(id))),
        ])
        .await;
    });
}

// --------------------------------------------------------- _scheduler/docs

/// A URL as _scheduler/docs shows it: without credentials (strip_url_creds/1).
fn endpoint_url(v: Option<&Value>, server: &Server) -> Value {
    let Some(v) = v else { return Value::Null };
    if let Value::String(s) = v
        && !s.starts_with("http://")
        && !s.starts_with("https://")
    {
        return v.clone();
    }
    let doc: Map<String, Value> = [("source".to_string(), v.clone()), ("target".to_string(), v.clone())].into_iter().collect();
    match rep::parse_without_id(&doc, rep::UserCtx::default(), server) {
        Ok(r) => json!(r.source.url),
        Err(_) => Value::Null,
    }
}

/// info_from_doc/2: what a doc's own fields say about its state.
fn info_from_doc(db: &str, doc: &Map<String, Value>, server: &Server) -> Map<String, Value> {
    let state = doc.get("_replication_state").and_then(Value::as_str);
    let state_time = doc.get("_replication_state_time").cloned().unwrap_or(Value::Null);
    let (state, info, errors, start) = match state {
        Some("completed") => {
            let mut s = doc.get("_replication_stats").and_then(Value::as_object).cloned().unwrap_or_default();
            let start = s.remove("start_time").unwrap_or(Value::Null);
            (json!("completed"), Value::Object(s), 0, start)
        }
        Some("failed") => {
            let reason = doc.get("_replication_state_reason").cloned().unwrap_or(Value::Null);
            let info = match reason {
                Value::Null => Value::Null,
                Value::String(r) => json!({"error": r}),
                other => other,
            };
            (json!("failed"), info, 1, state_time.clone())
        }
        _ => (Value::Null, Value::Null, 0, Value::Null),
    };
    let mut m = Map::new();
    m.insert("doc_id".into(), doc.get("_id").cloned().unwrap_or(Value::Null));
    m.insert("database".into(), json!(db));
    m.insert("id".into(), Value::Null);
    m.insert("source".into(), endpoint_url(doc.get("source"), server));
    m.insert("target".into(), endpoint_url(doc.get("target"), server));
    m.insert("state".into(), state);
    m.insert("error_count".into(), json!(errors));
    m.insert("info".into(), info);
    m.insert("start_time".into(), start);
    m.insert("last_updated".into(), if state_time.is_null() { Value::Null } else { state_time });
    m
}

fn parse_states(req: &Req) -> Result<Vec<String>> {
    let Some(q) = req.q("states") else { return Ok(vec![]) };
    let known = ["initializing", "error", "running", "pending", "crashing", "completed", "failed"];
    let states: Vec<String> = q.split(',').filter(|s| !s.is_empty()).map(|s| s.to_lowercase()).collect();
    if let Some(bad) = states.iter().find(|s| !known.contains(&s.as_str())) {
        let _ = bad;
        return Err(CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", format!("States must be one or more of {STATES}")));
    }
    Ok(states)
}

/// The state a doc row reports: its own fields, else the processor's.
fn doc_state(db: &str, id: &str, doc: &Map<String, Value>, server: &Server) -> Option<Map<String, Value>> {
    let info = info_from_doc(db, doc, server);
    if !info.get("state").is_some_and(Value::is_null) {
        return Some(info);
    }
    PROCESSOR.get().and_then(|p| p.doc_json(&(db.to_string(), id.to_string())))
}

/// GET /_scheduler/docs[/DB].
pub fn http_docs(req: &Req, db_name: &str) -> Result<Response> {
    let states = parse_states(req)?;
    let db = req.server.open_db(db_name)?;
    let snap = db.snapshot();
    let skip = req.q_u64("skip")?.unwrap_or(0) as usize;
    let limit = req.q_u64("limit")?.map(|l| l as usize).unwrap_or(usize::MAX);
    let mut rows = Vec::new();
    let mut seen = 0usize;
    snap.fold_docs(crate::btree::Dir::Fwd, &crate::btree::Range::default(), |fdi| {
        if fdi.deleted || fdi.id.starts_with("_design/") {
            return Ok(true);
        }
        let Ok(Value::Object(doc)) = snap.winner_json(&fdi, &crate::doc::DocOpts::default()) else { return Ok(true) };
        let Some(info) = doc_state(db_name, &fdi.id, &doc, &req.server) else { return Ok(true) };
        let state = info.get("state").and_then(Value::as_str).unwrap_or("");
        if !states.is_empty() && !states.iter().any(|s| s == state) {
            return Ok(true);
        }
        seen += 1;
        if seen > skip {
            rows.push(Value::Object(info));
        }
        Ok(rows.len() < limit)
    })?;
    ok_json(json!({"total_rows": snap.doc_count(), "offset": skip.min(snap.doc_count() as usize), "docs": rows}))
}

/// GET /_scheduler/docs/DB/DOCID.
pub fn http_doc(req: &Req, db_name: &str, doc_id: &str) -> Result<Response> {
    if let Some(info) = PROCESSOR.get().and_then(|p| p.doc_json(&(db_name.to_string(), doc_id.to_string()))) {
        return ok_json(Value::Object(info));
    }
    let db = req.server.open_db(db_name).map_err(|_| CouchError::not_found("missing"))?;
    let snap = db.snapshot();
    match snap.get_fdi(doc_id)? {
        Some(fdi) if !fdi.deleted => match snap.winner_json(&fdi, &crate::doc::DocOpts::default())? {
            Value::Object(doc) => ok_json(Value::Object(info_from_doc(db_name, &doc, &req.server))),
            _ => Err(CouchError::not_found("missing")),
        },
        _ => Err(CouchError::not_found("missing")),
    }
}
