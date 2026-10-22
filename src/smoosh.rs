//! Automatic compaction, a port of smoosh. When a database or view index
//! changes, the channels of its kind (upgrade, ratio and slack by
//! default, plus any configured) are asked in turn, by name, for a
//! priority; the first to give one queues it. Each channel compacts the
//! highest priority first, `concurrency` at a time, inside its from/to
//! window, starting `[smoosh] wait_secs` after the server does. A view
//! compaction also queues its database's index cleanup, which removes
//! index files no design doc uses.
//!
//! Something queued is not considered again for `[smoosh] staleness`
//! minutes. One thing differs from CouchDB: a database or index whose
//! priority is zero is looked at no more than once a second (changes in
//! between are looked at together, after it), as working out RocksDB's
//! file sizes costs more than reading couch_file's.
//! Running compactions can't be suspended, so a strict_window only stops
//! new ones from starting.

use crate::db::DbEvent;
use crate::server::Server;
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

const BUILT_IN_DB_CHANNELS: &str = "upgrade_dbs,ratio_dbs,slack_dbs";
const BUILT_IN_VIEW_CHANNELS: &str = "upgrade_views,ratio_views,slack_views";
const BUILT_IN_CLEANUP_CHANNELS: &str = "index_cleanup";
const ACCESS_MAX_SIZE: usize = 250_000;
const RE_ENQUEUE_INTERVAL: Duration = Duration::from_secs(5);
const CHECK_WINDOW_INTERVAL: Duration = Duration::from_secs(60);
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(180);
const ACCESS_CLEAN_INTERVAL: Duration = Duration::from_secs(3);
const MIN_RECHECK: Duration = Duration::from_secs(1);

/// Something to compact.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Object {
    Db(String),
    /// A database and a design doc id.
    View(String, String),
    /// A database's unused index files.
    Cleanup(String),
}

impl Object {
    fn db(&self) -> &str {
        match self {
            Object::Db(d) | Object::View(d, _) | Object::Cleanup(d) => d,
        }
    }

    fn describe(&self) -> String {
        match self {
            Object::Db(d) => d.clone(),
            Object::View(d, g) => format!("{d} {g}"),
            Object::Cleanup(d) => format!("{d} index_cleanup"),
        }
    }

    fn to_json(&self) -> Value {
        match self {
            Object::Db(d) => json!({"db": d}),
            Object::View(d, g) => json!({"db": d, "ddoc": g}),
            Object::Cleanup(d) => json!({"db": d, "cleanup": true}),
        }
    }

    fn from_json(v: &Value) -> Option<Object> {
        let db = v.get("db")?.as_str()?.to_string();
        Some(match (v.get("ddoc").and_then(Value::as_str), v.get("cleanup")) {
            (Some(g), _) => Object::View(db, g.to_string()),
            (None, Some(_)) => Object::Cleanup(db),
            _ => Object::Db(db),
        })
    }
}

// ----------------------------------------------------------- priorities

/// A priority as smoosh computes it: a size difference (integer), a
/// ratio (float), or infinity for work that was running at a checkpoint.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Prio {
    Int(i64),
    Float(f64),
    Infinity,
}

impl Prio {
    fn value(self) -> f64 {
        match self {
            Prio::Int(i) => i as f64,
            Prio::Float(f) => f,
            Prio::Infinity => f64::INFINITY,
        }
    }

    fn is_zero(self) -> bool {
        self.value() == 0.0
    }

    fn to_json(self) -> Value {
        match self {
            Prio::Int(i) => json!(i),
            Prio::Float(f) => json!(f),
            Prio::Infinity => json!("infinity"),
        }
    }

    fn from_json(v: &Value) -> Option<Prio> {
        match v {
            Value::String(s) if s == "infinity" => Some(Prio::Infinity),
            Value::Number(n) if n.is_f64() => n.as_f64().map(Prio::Float),
            Value::Number(n) => n.as_i64().map(Prio::Int),
            _ => None,
        }
    }

    /// quantize/1: ratios to a sixteenth below 16, whole numbers above.
    fn quantize(self) -> Prio {
        match self {
            Prio::Float(f) if f >= 16.0 => Prio::Int(f.round() as i64),
            Prio::Float(f) => Prio::Float((f * 16.0).round() / 16.0),
            p => p,
        }
    }
}

/// A configured number: "infinity", a float, or an integer.
fn to_number(channel: &str, name: &str, default: &str) -> Prio {
    let v = chan_get(channel, name).unwrap_or_else(|| default.to_string());
    let v = v.trim();
    if v == "infinity" {
        return Prio::Infinity;
    }
    if v.contains('.') {
        return v.parse().map(Prio::Float).unwrap_or(Prio::Int(0));
    }
    v.parse().map(Prio::Int).unwrap_or(Prio::Int(0))
}

/// get_priority/4: ratio, slack or upgrade, inside the channel's limits.
fn priority_for(channel: &str, disk: u64, data: u64, needs_upgrade: bool) -> Prio {
    let kind = channel_priority(channel);
    let min_size = to_number(channel, "min_size", "1048576").value();
    let max_size = to_number(channel, "max_size", "infinity").value();
    let default_min = if kind == "slack" { "536870912" } else { "2.0" };
    let min_priority = to_number(channel, "min_priority", default_min);
    let max_priority = to_number(channel, "max_priority", "infinity").value();
    let (disk_f, data_f) = (disk as f64, data as f64);
    if kind == "upgrade" && needs_upgrade {
        return Prio::Int(1);
    }
    if disk_f <= min_size || disk_f > max_size {
        return Prio::Int(0);
    }
    if data == 0 {
        return min_priority;
    }
    match kind.as_str() {
        "ratio" => {
            let r = disk_f / data_f;
            if r <= min_priority.value() || r > max_priority { Prio::Int(0) } else { Prio::Float(r) }
        }
        "slack" => {
            let s = disk as i64 - data as i64;
            if (s as f64) <= min_priority.value() || (s as f64) > max_priority { Prio::Int(0) } else { Prio::Int(s) }
        }
        _ => Prio::Int(0),
    }
}

fn channel_priority(channel: &str) -> String {
    match channel {
        "ratio_dbs" | "ratio_views" => "ratio".into(),
        "slack_dbs" | "slack_views" => "slack".into(),
        "upgrade_dbs" | "upgrade_views" => "upgrade".into(),
        c => chan_get(c, "priority").unwrap_or_else(|| "ratio".into()),
    }
}

/// The priority of `obj` in `channel` now; zero when it can't be found.
fn get_priority(server: &Server, channel: &str, obj: &Object) -> Prio {
    match obj {
        Object::Cleanup(db) => {
            if crate::db::exists(&server.db_path(db)) { Prio::Int(1) } else { Prio::Int(0) }
        }
        Object::Db(name) => {
            let Ok(db) = server.open_db(name) else { return Prio::Int(0) };
            let info = db.info();
            let min_changes: u64 = chan_get(channel, "min_changes").and_then(|v| v.trim().parse().ok()).unwrap_or(0);
            let changed = match db.compacted_seq() {
                None => true,
                Some(c) => db.snapshot().update_seq().saturating_sub(c) >= min_changes,
            };
            if !changed {
                return Prio::Int(0);
            }
            let sizes = &info["sizes"];
            priority_for(channel, sizes["file"].as_u64().unwrap_or(0), sizes["active"].as_u64().unwrap_or(0), false)
        }
        Object::View(name, ddoc_id) => {
            let Some(idx) = open_index(server, name, ddoc_id) else { return Prio::Int(0) };
            let info = idx.info(ddoc_id);
            let sizes = &info["view_index"]["sizes"];
            priority_for(channel, sizes["file"].as_u64().unwrap_or(0), sizes["active"].as_u64().unwrap_or(0), false)
        }
    }
}

fn open_index(server: &Server, db_name: &str, ddoc_id: &str) -> Option<Arc<crate::views::Index>> {
    let db = server.open_db(db_name).ok()?;
    let ddoc = crate::changes::load_ddoc(&db.snapshot(), ddoc_id).ok()?;
    crate::views::get_index(&db, &ddoc, crate::views::index_dir_of(server, db_name)).ok()
}

// ---------------------------------------------------------------- config

fn smoosh_get(key: &str) -> Option<String> {
    SMOOSH.get().and_then(|s| s.server.upgrade()).and_then(|srv| srv.config.get("smoosh", key))
}

fn chan_get(channel: &str, key: &str) -> Option<String> {
    SMOOSH.get().and_then(|s| s.server.upgrade()).and_then(|srv| srv.config.get(&format!("smoosh.{channel}"), key))
}

fn split(csv: &str) -> Vec<String> {
    csv.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

/// A kind's channels: the built-in ones and the configured ones, sorted.
fn channel_list(key: &str, builtin: &str) -> Vec<String> {
    let mut v = split(builtin);
    v.extend(split(&smoosh_get(key).unwrap_or_default()));
    v.sort();
    v.dedup();
    v
}

fn channels_for(obj: &Object) -> Vec<String> {
    match obj {
        Object::Db(_) => channel_list("db_channels", BUILT_IN_DB_CHANNELS),
        Object::View(..) => channel_list("view_channels", BUILT_IN_VIEW_CHANNELS),
        Object::Cleanup(_) => channel_list("cleanup_channels", BUILT_IN_CLEANUP_CHANNELS),
    }
}

fn all_channels() -> Vec<String> {
    let mut v = channel_list("db_channels", BUILT_IN_DB_CHANNELS);
    v.extend(channel_list("view_channels", BUILT_IN_VIEW_CHANNELS));
    v.extend(channel_list("cleanup_channels", BUILT_IN_CLEANUP_CHANNELS));
    v
}

fn ignored(server: &Server, obj: &Object) -> bool {
    server.config.get("smoosh.ignore", obj.db()).as_deref() == Some("true")
}

fn staleness() -> Duration {
    Duration::from_secs(smoosh_get("staleness").and_then(|v| v.trim().parse::<u64>().ok()).unwrap_or(5) * 60)
}

fn concurrency(channel: &str) -> usize {
    chan_get(channel, "concurrency").and_then(|v| v.trim().parse().ok()).unwrap_or(1)
}

fn capacity(channel: &str) -> usize {
    chan_get(channel, "capacity").and_then(|v| v.trim().parse().ok()).unwrap_or(9999)
}

fn persist_enabled() -> bool {
    smoosh_get("persist").as_deref() == Some("true")
}

fn state_dir() -> std::path::PathBuf {
    match smoosh_get("state_dir") {
        Some(d) => std::path::PathBuf::from(d),
        None => SMOOSH.get().and_then(|s| s.server.upgrade()).map(|s| s.data_dir.clone()).unwrap_or_else(|| ".".into()),
    }
}

/// Log at `[smoosh] compaction_log_level` (CouchDB's notice is info).
fn log(msg: &str) {
    match smoosh_get("compaction_log_level").as_deref() {
        Some("debug") => tracing::debug!("{msg}"),
        Some("warning") => tracing::warn!("{msg}"),
        Some("error") => tracing::error!("{msg}"),
        _ => tracing::info!("{msg}"),
    }
}

/// Whether now (UTC) is in the channel's from/to window, "HH:MM" each.
fn in_allowed_window(channel: &str) -> bool {
    let parse = |key: &str, default: (u32, u32)| -> (u32, u32) {
        match chan_get(channel, key) {
            None => default,
            Some(s) => match s.split_once(':').map(|(h, m)| (h.trim().parse(), m.trim().parse())) {
                Some((Ok(h), Ok(m))) => (h, m),
                _ => {
                    tracing::error!("Malformed compaction schedule configuration: {s}");
                    default
                }
            },
        }
    };
    let from = parse("from", (0, 0));
    let to = parse("to", (24, 0));
    let secs = crate::http::now_secs() % 86_400;
    let now = ((secs / 3600) as u32, ((secs % 3600) / 60) as u32);
    if from < to { now >= from && now < to } else { now >= from || now < to }
}

// ---------------------------------------------------------------- queue

/// The waiting list: highest priority out first; of equal priorities,
/// the newest. Over capacity, the lowest is dropped.
#[derive(Default)]
struct Queue {
    by_key: HashMap<Object, (Prio, u64)>,
    order: BTreeMap<(OrdF64, u64), Object>,
    next: u64,
}

#[derive(Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&o.0)
    }
}

impl Queue {
    fn put(&mut self, key: Object, prio: Prio, capacity: usize) {
        if let Some((p, seq)) = self.by_key.get(&key).copied() {
            if p == prio {
                return;
            }
            self.order.remove(&(OrdF64(p.value()), seq));
        }
        self.next += 1;
        self.order.insert((OrdF64(prio.value()), self.next), key.clone());
        self.by_key.insert(key, (prio, self.next));
        while self.order.len() > capacity.max(1) {
            if let Some((_, k)) = self.order.pop_first() {
                self.by_key.remove(&k);
            }
        }
    }

    fn pop(&mut self) -> Option<Object> {
        let (_, k) = self.order.pop_last()?;
        self.by_key.remove(&k);
        Some(k)
    }

    fn info(&self) -> Value {
        let prio = |e: Option<(&(OrdF64, u64), &Object)>| -> Value {
            e.and_then(|(_, k)| self.by_key.get(k)).map(|(p, _)| p.to_json()).unwrap_or(json!(0))
        };
        json!({"size": self.order.len(), "min": prio(self.order.first_key_value()), "max": prio(self.order.last_key_value())})
    }
}

// -------------------------------------------------------------- channels

struct ChanState {
    waiting: Queue,
    active: HashSet<Object>,
    paused: bool,
    closed: bool,
}

struct Channel {
    name: String,
    state: Mutex<ChanState>,
    wake: tokio::sync::Notify,
}

pub struct Smoosh {
    server: Weak<Server>,
    channels: Mutex<BTreeMap<String, Arc<Channel>>>,
    /// When each object was last queued (smoosh_access).
    access: Mutex<HashMap<Object, Instant>>,
    /// Objects whose priority is being worked out, and when each was
    /// last found to have none.
    evaluating: Mutex<HashSet<Object>>,
    unqueued: Mutex<HashMap<Object, Instant>>,
    /// Objects that changed while being worked out or too soon after,
    /// to be looked at again.
    again: Mutex<HashSet<Object>>,
}

static SMOOSH: OnceLock<Arc<Smoosh>> = OnceLock::new();

/// Start compacting automatically, in the background.
pub fn start(server: &Arc<Server>) {
    let s = Arc::new(Smoosh {
        server: Arc::downgrade(server),
        channels: Mutex::new(BTreeMap::new()),
        access: Mutex::new(HashMap::new()),
        evaluating: Mutex::new(HashSet::new()),
        unqueued: Mutex::new(HashMap::new()),
        again: Mutex::new(HashSet::new()),
    });
    if SMOOSH.set(s.clone()).is_err() {
        return;
    }
    s.sync_channels();
    let mut rx = crate::db::events().subscribe();
    let s2 = s.clone();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok((db, DbEvent::Updated | DbEvent::LocalUpdated)) => s2.enqueue(Object::Db(db)),
                Ok((db, DbEvent::IndexCommit(ddoc))) => s2.enqueue(Object::View(db, ddoc)),
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });
    let s3 = s.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(ACCESS_CLEAN_INTERVAL).await;
            let limit = staleness();
            s3.access.lock().retain(|_, t| t.elapsed() < limit);
            s3.unqueued.lock().retain(|_, t| t.elapsed() < MIN_RECHECK);
        }
    });
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(CHECKPOINT_INTERVAL).await;
            let s4 = s.clone();
            let _ = tokio::task::spawn_blocking(move || s4.checkpoint()).await;
        }
    });
}

impl Smoosh {
    /// Start channels the config names and close the others.
    fn sync_channels(self: &Arc<Self>) {
        let names = all_channels();
        let mut chans = self.channels.lock();
        chans.retain(|name, c| {
            let keep = names.contains(name);
            if !keep {
                c.state.lock().closed = true;
                c.wake.notify_one();
            }
            keep
        });
        for name in names {
            if chans.contains_key(&name) {
                continue;
            }
            let mut waiting = Queue::default();
            for (obj, prio) in unpersist(&name) {
                self.access.lock().insert(obj.clone(), Instant::now());
                waiting.put(obj, prio, capacity(&name));
            }
            let c = Arc::new(Channel {
                name: name.clone(),
                state: Mutex::new(ChanState { waiting, active: HashSet::new(), paused: true, closed: false }),
                wake: tokio::sync::Notify::new(),
            });
            chans.insert(name, c.clone());
            tokio::spawn(run_channel(c, self.clone()));
        }
    }

    fn channel(&self, name: &str) -> Option<Arc<Channel>> {
        self.channels.lock().get(name).cloned()
    }

    fn stale_enough(&self, obj: &Object) -> bool {
        self.access.lock().get(obj).is_none_or(|t| t.elapsed() >= staleness())
    }

    /// Consider `obj` for compaction (smoosh_server:enqueue/1).
    fn enqueue(self: &Arc<Self>, obj: Object) {
        if !self.stale_enough(&obj) {
            return;
        }
        let since = self.unqueued.lock().get(&obj).map(|t| t.elapsed());
        if let Some(e) = since.filter(|e| *e < MIN_RECHECK) {
            // Too soon after the last look: look again once it's time.
            if self.again.lock().insert(obj.clone()) {
                let s = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(MIN_RECHECK - e).await;
                    s.again.lock().remove(&obj);
                    s.unqueued.lock().remove(&obj);
                    s.enqueue(obj);
                });
            }
            return;
        }
        if !self.evaluating.lock().insert(obj.clone()) {
            self.again.lock().insert(obj);
            return;
        }
        let s = self.clone();
        tokio::spawn(async move {
            let s2 = s.clone();
            let o = obj.clone();
            let found = tokio::task::spawn_blocking(move || s2.find_channel(&o)).await.ok().flatten();
            match found {
                Some((chan, prio)) => {
                    {
                        let mut a = s.access.lock();
                        if a.len() <= ACCESS_MAX_SIZE {
                            a.insert(obj.clone(), Instant::now());
                        }
                    }
                    chan.add(obj.clone(), prio.quantize());
                }
                None => {
                    s.unqueued.lock().insert(obj.clone(), Instant::now());
                }
            }
            s.evaluating.lock().remove(&obj);
            // A change while it was being looked at gets a look of its own.
            if s.again.lock().remove(&obj) {
                s.enqueue(obj);
            }
        });
    }

    /// The first channel, by name, to give `obj` a priority.
    fn find_channel(self: &Arc<Self>, obj: &Object) -> Option<(Arc<Channel>, Prio)> {
        let server = self.server.upgrade()?;
        self.sync_channels();
        if !self.stale_enough(obj) || ignored(&server, obj) {
            return None;
        }
        for name in channels_for(obj) {
            let p = get_priority(&server, &name, obj);
            if !p.is_zero() {
                return Some((self.channel(&name)?, p));
            }
        }
        None
    }

    /// _node/_local/_smoosh/status.
    pub fn status(self: &Arc<Self>) -> Value {
        self.sync_channels();
        let mut channels = Map::new();
        for (name, c) in self.channels.lock().iter() {
            let st = c.state.lock();
            channels.insert(name.clone(), json!({"active": st.active.len(), "starting": 0, "waiting": st.waiting.info()}));
        }
        json!({"channels": channels})
    }

    fn checkpoint(&self) {
        if !persist_enabled() {
            return;
        }
        for (name, c) in self.channels.lock().iter() {
            let st = c.state.lock();
            let mut entries: Vec<Value> =
                st.waiting.by_key.iter().map(|(k, (p, _))| json!({"object": k.to_json(), "priority": p.to_json()})).collect();
            entries.extend(st.active.iter().map(|k| json!({"object": k.to_json(), "priority": "infinity"})));
            drop(st);
            if let Err(e) = write_state(name, &entries) {
                tracing::warn!("smoosh: could not save {name}'s queue: {e}");
            }
        }
    }
}

fn state_path(channel: &str) -> std::path::PathBuf {
    state_dir().join(format!("{channel}.waiting"))
}

fn write_state(channel: &str, entries: &[Value]) -> std::io::Result<()> {
    let path = state_path(channel);
    if entries.is_empty() {
        let _ = std::fs::remove_file(&path);
        return Ok(());
    }
    std::fs::create_dir_all(state_dir())?;
    let tmp = path.with_extension(format!("{}.tmp", crate::http::now_secs()));
    std::fs::write(&tmp, serde_json::to_vec(entries)?)?;
    std::fs::rename(tmp, path)
}

fn unpersist(channel: &str) -> Vec<(Object, Prio)> {
    if !persist_enabled() {
        return vec![];
    }
    let Ok(bytes) = std::fs::read(state_path(channel)) else { return vec![] };
    let Ok(Value::Array(entries)) = serde_json::from_slice::<Value>(&bytes) else { return vec![] };
    entries
        .iter()
        .filter_map(|e| Some((Object::from_json(&e["object"])?, Prio::from_json(&e["priority"])?)))
        .collect()
}

/// The status, or empty channels before smoosh starts.
pub fn status() -> Value {
    SMOOSH.get().map(|s| s.status()).unwrap_or_else(|| json!({"channels": {}}))
}

/// smoosh's part of _prometheus (couch_prometheus:get_smoosh_stats/0).
pub fn prometheus() -> Vec<String> {
    use crate::stats::{plain, to_prom};
    let Some(s) = SMOOSH.get() else { return vec![] };
    let chans = s.channels.lock();
    let active: usize = chans.values().map(|c| c.state.lock().active.len()).sum();
    let waiting: usize = chans.values().map(|c| c.state.lock().waiting.order.len()).sum();
    [
        to_prom("smoosh_channel_count", "gauge", "total active smoosh channels", plain(chans.len())),
        to_prom("smoosh_active_jobs", "gauge", "global total active compaction jobs", plain(active)),
        to_prom("smoosh_starting_jobs", "gauge", "global total starting compaction jobs", plain(0)),
        to_prom("smoosh_waiting_jobs", "gauge", "global total waiting compaction jobs", plain(waiting)),
    ]
    .concat()
}

impl Channel {
    fn add(&self, obj: Object, prio: Prio) {
        {
            let mut st = self.state.lock();
            if st.active.contains(&obj) {
                return;
            }
            log(&format!("{}: enqueueing {} to compact with priority {}", self.name, obj.describe(), prio.to_json()));
            st.waiting.put(obj, prio, capacity(&self.name));
        }
        self.wake.notify_one();
    }

    /// Start what the queue and concurrency allow.
    fn maybe_start(self: &Arc<Self>, smoosh: &Arc<Smoosh>) {
        loop {
            let obj = {
                let mut st = self.state.lock();
                if st.paused || st.closed || st.active.len() >= concurrency(&self.name) {
                    return;
                }
                let Some(obj) = st.waiting.pop() else { return };
                st.active.insert(obj.clone());
                obj
            };
            let (c, s) = (self.clone(), smoosh.clone());
            tokio::spawn(async move {
                let (c2, s2, o) = (c.clone(), s.clone(), obj.clone());
                let r = tokio::task::spawn_blocking(move || compact(&s2, &c2.name, &o)).await;
                let failed = !matches!(r, Ok(Ok(())));
                if let Ok(Err(e)) = &r {
                    tracing::warn!("{}: compaction error for {}: {}", c.name, obj.describe(), e.reason);
                }
                c.state.lock().active.remove(&obj);
                if failed {
                    // re_enqueue/1, through the usual checks.
                    let s3 = s.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(RE_ENQUEUE_INTERVAL).await;
                        s3.unqueued.lock().remove(&obj);
                        s3.enqueue(obj);
                    });
                }
                c.wake.notify_one();
            });
        }
    }
}

/// Compact one object, if it still has a priority.
fn compact(smoosh: &Arc<Smoosh>, channel: &str, obj: &Object) -> crate::error::Result<()> {
    let Some(server) = smoosh.server.upgrade() else { return Ok(()) };
    if ignored(&server, obj) || get_priority(&server, channel, obj).is_zero() {
        return Ok(());
    }
    log(&format!("{channel}: Starting compaction for {}", obj.describe()));
    match obj {
        Object::Db(name) => server.open_db(name)?.compact_and_wait()?,
        Object::View(name, ddoc_id) => {
            let Some(idx) = open_index(&server, name, ddoc_id) else { return Ok(()) };
            if server.config.get_bool("smoosh", "cleanup_index_files", true) {
                smoosh.enqueue(Object::Cleanup(name.clone()));
            }
            idx.compact()?;
        }
        Object::Cleanup(name) => {
            if server.config.get_bool("smoosh", "cleanup_index_files", true) {
                crate::views::cleanup_index_files(&server, &server.open_db(name)?)?;
            }
        }
    }
    log(&format!("{channel}: Finished compaction for {}", obj.describe()));
    Ok(())
}

async fn run_channel(chan: Arc<Channel>, smoosh: Arc<Smoosh>) {
    let wait = smoosh_get("wait_secs").and_then(|v| v.trim().parse::<u64>().ok()).unwrap_or(30);
    let unpause_at = tokio::time::Instant::now() + Duration::from_secs(wait);
    let mut unpaused = false;
    let mut next_window = tokio::time::Instant::now() + CHECK_WINDOW_INTERVAL;
    loop {
        if chan.state.lock().closed {
            return;
        }
        let now = tokio::time::Instant::now();
        if !unpaused && now >= unpause_at {
            unpaused = true;
            chan.state.lock().paused = false;
        }
        if now >= next_window {
            next_window = now + CHECK_WINDOW_INTERVAL;
            let in_window = in_allowed_window(&chan.name);
            let mut st = chan.state.lock();
            // As check_window: resume inside the window, pause outside
            // it (running compactions finish).
            if in_window && st.paused {
                st.paused = false;
            } else if !in_window && !st.paused {
                st.paused = true;
            }
        }
        chan.maybe_start(&smoosh);
        let mut until = next_window;
        if !unpaused {
            until = until.min(unpause_at);
        }
        tokio::select! {
            _ = chan.wake.notified() => {}
            _ = tokio::time::sleep_until(until) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_order_and_capacity() {
        let mut q = Queue::default();
        q.put(Object::Db("a".into()), Prio::Float(2.5), 3);
        q.put(Object::Db("b".into()), Prio::Float(4.0), 3);
        q.put(Object::Db("c".into()), Prio::Int(3), 3);
        eq(q.info(), json!({"size": 3, "min": 2.5, "max": 4.0}));
        q.put(Object::Db("d".into()), Prio::Int(10), 3);
        // The lowest fell out.
        eq(q.info(), json!({"size": 3, "min": 3, "max": 10}));
        q.put(Object::Db("c".into()), Prio::Int(20), 3);
        assert_eq!(q.pop(), Some(Object::Db("c".into())));
        assert_eq!(q.pop(), Some(Object::Db("d".into())));
        assert_eq!(q.pop(), Some(Object::Db("b".into())));
        assert_eq!(q.pop(), None);
        eq(q.info(), json!({"size": 0, "min": 0, "max": 0}));
    }

    fn eq(a: Value, b: Value) {
        assert_eq!(a, b);
    }

    #[test]
    fn quantize_like_couchdb() {
        assert_eq!(Prio::Float(2.31).quantize(), Prio::Float(2.3125));
        assert_eq!(Prio::Float(17.4).quantize(), Prio::Int(17));
        assert_eq!(Prio::Int(7).quantize(), Prio::Int(7));
    }
}
