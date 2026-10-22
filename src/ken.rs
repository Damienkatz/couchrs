//! Background indexing, a port of ken. When a database changes, its
//! design docs' view indexes are brought up to date in the background,
//! so queries find them current. Databases are worked through one at a
//! time, their design docs in random order. At most `[ken]
//! batch_channels` index updates run at once, and up to
//! `incremental_channels` more for indexes less than
//! `max_incremental_updates` changes behind; an index just updated waits
//! for new changes, or 5 seconds, before it is updated again. A database
//! that couldn't be finished is looked at again a minute later.
//! `[ken.ignore] DB = true` skips a database, and a design doc's
//! `"autoupdate": false` (or `{"views": false}`) skips its views.

use crate::db::DbEvent;
use crate::server::Server;
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

const DELAY: Duration = Duration::from_millis(5000);
const BATCH_SIZE: u64 = 1;
const PRUNE_INTERVAL: Duration = Duration::from_millis(60_000);

struct Job {
    running: bool,
    /// The database's update seq when this job last started.
    seq: u64,
    lru: Instant,
}

#[derive(Default)]
struct State {
    q: VecDeque<String>,
    pending: HashSet<String>,
    resubmit: HashSet<String>,
    dbworker: Option<String>,
    jobs: HashMap<(String, String), Job>,
}

struct Ken {
    server: Weak<Server>,
    st: Mutex<State>,
    wake: tokio::sync::Notify,
}

/// Why a database's indexes weren't all started.
enum Outcome {
    Done,
    Resubmit,
}

fn config_int(server: &Server, key: &str, default: u64) -> u64 {
    server.config.get_int("ken", key, default)
}

/// Start indexing in the background.
pub fn start(server: &Arc<Server>) {
    let ken = Arc::new(Ken { server: Arc::downgrade(server), st: Mutex::new(State::default()), wake: tokio::sync::Notify::new() });
    let mut rx = crate::db::events().subscribe();
    tokio::spawn(async move {
        let mut pruned_last = Instant::now();
        loop {
            tokio::select! {
                ev = rx.recv() => match ev {
                    Ok((db, DbEvent::Updated | DbEvent::DdocUpdated)) => ken.add(db),
                    Ok((db, DbEvent::Deleted)) => ken.remove(&db),
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(_) => return,
                },
                _ = ken.wake.notified() => {}
                _ = tokio::time::sleep(PRUNE_INTERVAL) => {}
            }
            if pruned_last.elapsed() > DELAY {
                ken.st.lock().jobs.retain(|_, j| j.running || j.lru.elapsed() < DELAY);
                pruned_last = Instant::now();
            }
            ken.maybe_start_next_queued_job();
        }
    });
}

impl Ken {
    fn add(&self, db: String) {
        let mut st = self.st.lock();
        if st.pending.insert(db.clone()) {
            st.q.push_back(db);
        }
    }

    fn remove(&self, db: &str) {
        let mut st = self.st.lock();
        st.q.retain(|d| d != db);
        st.pending.remove(db);
        st.jobs.retain(|(d, _), _| d != db);
    }

    fn active_count(st: &State) -> u64 {
        st.jobs.values().filter(|j| j.running).count() as u64
    }

    /// Look at the next database, unless one is being looked at.
    fn maybe_start_next_queued_job(self: &Arc<Self>) {
        let Some(server) = self.server.upgrade() else { return };
        let total = config_int(&server, "incremental_channels", 80) + config_int(&server, "batch_channels", 20);
        let mut st = self.st.lock();
        while st.dbworker.is_none() {
            let Some(db) = st.q.pop_front() else { return };
            if st.resubmit.contains(&db) || server.config.get("ken.ignore", &db).as_deref() == Some("true") {
                st.pending.remove(&db);
                continue;
            }
            if Self::active_count(&st) >= total {
                st.q.push_front(db);
                return;
            }
            st.pending.remove(&db);
            st.dbworker = Some(db.clone());
            let ken = self.clone();
            tokio::spawn(async move {
                let k2 = ken.clone();
                let d2 = db.clone();
                let r = tokio::task::spawn_blocking(move || k2.update_db_indexes(&d2)).await;
                ken.st.lock().dbworker = None;
                match r {
                    Ok(Ok(Outcome::Done)) => {}
                    Ok(Ok(Outcome::Resubmit)) => ken.resubmit(Duration::from_secs(60), db),
                    _ => ken.resubmit(Duration::from_secs(5), db),
                }
                ken.wake.notify_one();
            });
        }
    }

    fn resubmit(self: &Arc<Self>, delay: Duration, db: String) {
        if !self.st.lock().resubmit.insert(db.clone()) {
            return;
        }
        let ken = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            ken.st.lock().resubmit.remove(&db);
            ken.add(db);
            ken.wake.notify_one();
        });
    }

    /// Start updating each of a database's view indexes that should be.
    fn update_db_indexes(self: &Arc<Self>, name: &str) -> crate::error::Result<Outcome> {
        let Some(server) = self.server.upgrade() else { return Ok(Outcome::Done) };
        if !crate::db::exists(&server.db_path(name)) {
            return Ok(Outcome::Done);
        }
        let db = server.open_db(name)?;
        let snap = db.snapshot();
        let mut ids = crate::views::design_doc_ids(&snap)?;
        // Random order, so no design doc always waits on another.
        for i in (1..ids.len()).rev() {
            ids.swap(i, (rand::random::<u64>() % (i as u64 + 1)) as usize);
        }
        let seq = snap.update_seq();
        let mut outcome = Outcome::Done;
        for id in ids {
            let Ok(ddoc) = crate::changes::load_ddoc(&snap, &id) else { continue };
            if !should_update(&ddoc, "views") {
                continue;
            }
            let Ok(def) = crate::views::IndexDef::from_ddoc(&ddoc, snap.partitioned()) else { continue };
            if def.views.is_empty() {
                continue;
            }
            let Ok(idx) = crate::views::get_index(&db, &ddoc, crate::views::index_dir_of(&server, name)) else { continue };
            let (db2, cur) = (db.clone(), idx.state().seq);
            let run = Box::new(move || idx.update(&db2).map(|_| ()));
            if !self.maybe_start_job(&server, (name.to_string(), id), cur, run, seq) {
                outcome = Outcome::Resubmit;
            }
        }
        // Search indexes too (ken_server:nouveau_updated/4).
        if crate::nouveau::enabled(&server) {
            for d in crate::views::all_ddocs(&snap)? {
                if !should_update(&d, "nouveau") {
                    continue;
                }
                for spec in crate::nouveau::design_doc_to_indexes(&d) {
                    if !crate::nouveau::outdated(&server, &db, &spec) {
                        continue;
                    }
                    let key = (name.to_string(), format!("{}/_nouveau/{}", spec.ddoc_id, spec.name));
                    let (server2, db2) = (server.clone(), db.clone());
                    let run = Box::new(move || {
                        crate::nouveau::update_in_background(&server2, &db2, &spec);
                        Ok(())
                    });
                    if !self.maybe_start_job(&server, key, 0, run, seq) {
                        outcome = Outcome::Resubmit;
                    }
                }
            }
        }
        Ok(outcome)
    }

    /// should_start_job/2, and the start; false when it isn't time.
    fn maybe_start_job(
        self: &Arc<Self>,
        server: &Server,
        name: (String, String),
        index_seq: u64,
        run: Box<dyn FnOnce() -> crate::error::Result<()> + Send>,
        seq: u64,
    ) -> bool {
        let threshold = config_int(server, "max_incremental_updates", 1000);
        let batch = config_int(server, "batch_channels", 20);
        let total = config_int(server, "incremental_channels", 80) + batch;
        let mut st = self.st.lock();
        let active = Self::active_count(&st);
        let start = match st.jobs.get(&name) {
            None => active < batch || (active < total && seq.saturating_sub(index_seq) < threshold),
            Some(j) if !j.running => {
                let dt = j.lru.elapsed();
                let behind = seq.saturating_sub(j.seq);
                (active < batch && behind >= BATCH_SIZE)
                    || (active < batch && dt > DELAY)
                    || (active < total && behind < threshold && dt > DELAY)
            }
            Some(_) => false,
        };
        if !start {
            return false;
        }
        st.jobs.insert(name.clone(), Job { running: true, seq, lru: Instant::now() });
        drop(st);
        let ken = self.clone();
        tokio::task::spawn_blocking(move || {
            let r = run();
            if let Some(j) = ken.st.lock().jobs.get_mut(&name) {
                j.running = false;
            }
            if let Err(e) = r {
                tracing::warn!("ken: updating {} {} failed: {}", name.0, name.1, e.reason);
                let ken2 = ken.clone();
                let db_name = name.0.clone();
                // Back on the runtime to schedule the retry.
                if let Ok(h) = tokio::runtime::Handle::try_current() {
                    h.spawn(async move { ken2.resubmit(Duration::from_secs(5), db_name) });
                }
            }
            ken.wake.notify_one();
        });
        true
    }
}

/// Whether a design doc's indexes of this kind update in the background:
/// not when "autoupdate" is false, or an object with the kind false.
fn should_update(ddoc: &Value, kind: &str) -> bool {
    match ddoc.get("autoupdate") {
        Some(Value::Bool(false)) => false,
        Some(Value::Object(o)) => o.get(kind) != Some(&Value::Bool(false)),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn autoupdate() {
        assert!(super::should_update(&json!({}), "views"));
        assert!(!super::should_update(&json!({"autoupdate": false}), "views"));
        assert!(!super::should_update(&json!({"autoupdate": {"views": false}}), "views"));
        assert!(super::should_update(&json!({"autoupdate": {"indexes": false}}), "views"));
    }
}
