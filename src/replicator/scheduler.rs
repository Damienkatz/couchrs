//! The replication scheduler (couch_replicator_scheduler): jobs from
//! _replicate and replicator databases, at most `[replicator] max_jobs`
//! running. Every `interval` it stops jobs over the limit and, when jobs
//! are waiting, swaps up to `max_churn` continuous ones for those that
//! ran least recently. A job that keeps crashing waits longer each time
//! before it may run again (30 seconds, doubling, up to about 8 hours).
//! A failed or finished job from _replicate is removed.
//!
//! Which jobs run is fair across replicator databases (and, for
//! _replicate, users) by couch_replicator_share's measure: a job's
//! priority grows with how long its database's jobs have run lately,
//! divided by the database's `[replicator.shares]` squared (100 unless
//! set), and the lowest priorities run first.

use super::RepError;
use super::job::{self, JobEnd, JobError, Live, Stats};
use super::rep::{Rep, url_strip_password};
use crate::server::Server;
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, SystemTime};
use tokio::sync::{oneshot, watch};

const DEFAULT_HEALTH_THRESHOLD_SEC: u64 = 2 * 60;
const MAX_BACKOFF_EXPONENT: u32 = 10;
const BACKOFF_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub enum Event {
    Added,
    Started,
    Stopped,
    Crashed(String),
}

struct Running {
    stop: watch::Sender<bool>,
    live: Arc<Mutex<Live>>,
    pid: String,
}

pub struct JobEntry {
    pub rep: Rep,
    running: Option<Running>,
    /// Newest first.
    history: Vec<(Event, SystemTime)>,
    /// The last info a run reported (Rep#rep.stats).
    info: Value,
}

/// What a _replicate caller waiting on a one-shot job hears.
pub enum Outcome {
    Finished(Value),
    Error(JobError),
    Stopped,
}

pub struct Scheduler {
    server: Weak<Server>,
    jobs: Mutex<BTreeMap<String, JobEntry>>,
    waiters: Mutex<HashMap<String, Vec<oneshot::Sender<Outcome>>>>,
    share: Mutex<Share>,
}

/// couch_replicator_share's tables.
#[derive(Default)]
struct Share {
    priorities: HashMap<String, i64>,
    usage: HashMap<String, i64>,
    charges: HashMap<String, i64>,
    num_jobs: HashMap<String, i64>,
}

/// What a job's fair share is counted against: its replicator db, or the
/// user who started it.
fn share_key(rep: &Rep) -> String {
    rep.db_name.clone().unwrap_or_else(|| rep.user_ctx.name.clone().unwrap_or_default())
}

impl Share {
    fn shares(server: &Server, key: &str) -> i64 {
        server.config.get_int("replicator.shares", key, 100i64).clamp(1, 1000)
    }

    fn coeff(server: &Server, key: &str, default: f64) -> f64 {
        server.config.get("replicator", key).and_then(|v| v.trim().parse::<f64>().ok()).unwrap_or(default).clamp(0.0, 1.0)
    }

    fn added(&mut self, server: &Server, id: &str, rep: &Rep) {
        *self.num_jobs.entry(share_key(rep)).or_default() += 1;
        self.update_priority(server, id, rep);
    }

    fn removed(&mut self, id: &str, rep: &Rep) {
        self.priorities.remove(id);
        let key = share_key(rep);
        if let Some(n) = self.num_jobs.get_mut(&key) {
            *n -= 1;
            if *n <= 0 {
                self.num_jobs.remove(&key);
            }
        }
    }

    /// Charge a running job for its time running, up to an interval.
    fn charge(&mut self, j: &JobEntry, interval_ms: u64) {
        if j.running.is_none() {
            return;
        }
        let ran = SystemTime::now().duration_since(Scheduler::last_started(j)).unwrap_or_default().as_micros() as i64;
        *self.charges.entry(share_key(&j.rep)).or_default() += ran.clamp(0, interval_ms as i64 * 1000);
    }

    fn update_priority(&mut self, server: &Server, id: &str, rep: &Rep) {
        let key = share_key(rep);
        let shares = Self::shares(server, &key);
        let p = (*self.usage.get(&key).unwrap_or(&0) as f64 * *self.num_jobs.get(&key).unwrap_or(&0) as f64) / (shares * shares) as f64;
        *self.priorities.entry(id.to_string()).or_default() += p as i64;
    }

    fn decay(map: &mut HashMap<String, i64>, coeff: f64) {
        for v in map.values_mut() {
            *v = (*v as f64 * coeff) as i64;
        }
        map.retain(|_, v| *v > 0);
    }

    /// update/3, each reschedule: charge the running, roll charges into
    /// usage, decay, and raise the running jobs' priorities.
    fn update(&mut self, server: &Server, jobs: &BTreeMap<String, JobEntry>, interval_ms: u64) {
        for j in jobs.values() {
            self.charge(j, interval_ms);
        }
        Self::decay(&mut self.usage, Self::coeff(server, "usage_coeff", 0.5));
        for (k, c) in std::mem::take(&mut self.charges) {
            *self.usage.entry(k).or_default() += c;
        }
        Self::decay(&mut self.priorities, Self::coeff(server, "priority_coeff", 0.98));
        for (id, j) in jobs {
            if j.running.is_some() {
                self.update_priority(server, id, &j.rep);
            }
        }
    }

    fn priority(&self, id: &str) -> i64 {
        *self.priorities.get(id).unwrap_or(&0)
    }
}

static SCHEDULER: OnceLock<Arc<Scheduler>> = OnceLock::new();

pub fn get() -> Option<&'static Arc<Scheduler>> {
    SCHEDULER.get()
}

fn cfg(server: &Server, key: &str, default: u64) -> u64 {
    server.config.get_int("replicator", key, default)
}

fn secs(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

pub fn start(server: &Arc<Server>) -> Arc<Scheduler> {
    let s = SCHEDULER
        .get_or_init(|| {
            Arc::new(Scheduler {
                server: Arc::downgrade(server),
                jobs: Mutex::new(BTreeMap::new()),
                waiters: Mutex::new(HashMap::new()),
                share: Mutex::new(Share::default()),
            })
        })
        .clone();
    let s2 = s.clone();
    tokio::spawn(async move {
        loop {
            let interval = s2.server.upgrade().map(|srv| cfg(&srv, "interval", 60_000)).unwrap_or(60_000);
            tokio::time::sleep(Duration::from_millis(interval.max(100))).await;
            s2.reschedule();
        }
    });
    s
}

/// Same replication, ignoring times and stats (normalize_rep/1).
fn same_rep(a: &Rep, b: &Rep) -> bool {
    a.id == b.id
        && a.source.url == b.source.url
        && a.target.url == b.target.url
        && a.source.headers == b.source.headers
        && a.target.headers == b.target.headers
        && a.source.basic == b.source.basic
        && a.target.basic == b.target.basic
        && a.doc_id == b.doc_id
        && a.db_name == b.db_name
        && format!("{:?}", a.options) == format!("{:?}", b.options)
}

impl Scheduler {
    fn health_threshold(&self) -> u64 {
        self.server.upgrade().map(|s| cfg(&s, "health_threshold", DEFAULT_HEALTH_THRESHOLD_SEC)).unwrap_or(DEFAULT_HEALTH_THRESHOLD_SEC)
    }

    fn max_jobs(&self) -> usize {
        self.server.upgrade().map(|s| cfg(&s, "max_jobs", 500)).unwrap_or(500) as usize
    }

    fn interval_ms(&self) -> u64 {
        self.server.upgrade().map(|s| cfg(&s, "interval", 60_000)).unwrap_or(60_000)
    }

    fn max_history(&self) -> usize {
        self.server.upgrade().map(|s| cfg(&s, "max_history", 20)).unwrap_or(20).max(1) as usize
    }

    /// The replication a job id names, if there is one.
    pub fn rep_state(&self, id: &str) -> Option<Rep> {
        self.jobs.lock().get(id).map(|j| j.rep.clone())
    }

    /// add_job/1, and a receiver for how it ends.
    pub fn add_job(self: &Arc<Self>, rep: Rep) -> oneshot::Receiver<Outcome> {
        let id = rep.id_str();
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().entry(id.clone()).or_default().push(tx);
        let existing = self.jobs.lock().get(&id).is_some_and(|j| same_rep(&j.rep, &rep));
        if existing {
            return rx;
        }
        self.remove_job_int(&id);
        if let Some(server) = self.server.upgrade() {
            self.share.lock().added(&server, &id, &rep);
        }
        let entry = JobEntry { rep, running: None, history: vec![(Event::Added, SystemTime::now())], info: Value::Null };
        self.jobs.lock().insert(id.clone(), entry);
        crate::stat!("couch_replicator.jobs.adds").inc();
        let (total, running) = self.counts();
        if total < self.max_jobs() && running < self.max_jobs() {
            self.start_job(&id);
        }
        self.update_gauges();
        rx
    }

    /// remove_job/1: stop it (with a last checkpoint) and forget it.
    pub fn remove_job(self: &Arc<Self>, id: &str) -> bool {
        let found = self.remove_job_int(id);
        if found {
            crate::stat!("couch_replicator.jobs.removes").inc();
        }
        self.update_gauges();
        found
    }

    fn remove_job_int(&self, id: &str) -> bool {
        match self.jobs.lock().remove(id) {
            Some(j) => {
                let mut share = self.share.lock();
                share.charge(&j, self.interval_ms());
                share.removed(id, &j.rep);
                if let Some(r) = j.running {
                    let _ = r.stop.send(true);
                    crate::stat!("couch_replicator.jobs.stops").inc();
                }
                true
            }
            None => false,
        }
    }

    fn counts(&self) -> (usize, usize) {
        let jobs = self.jobs.lock();
        (jobs.len(), jobs.values().filter(|j| j.running.is_some()).count())
    }

    fn push_history(&self, j: &mut JobEntry, e: Event) {
        j.history.insert(0, (e, SystemTime::now()));
        j.history.truncate(self.max_history());
    }

    fn start_job(self: &Arc<Self>, id: &str) {
        let Some(server) = self.server.upgrade() else { return };
        let mut jobs = self.jobs.lock();
        let Some(j) = jobs.get_mut(id) else { return };
        if j.running.is_some() {
            return;
        }
        let (stop, stop_rx) = watch::channel(false);
        let live = Arc::new(Mutex::new(Live { info: j.info.clone() }));
        let pid = crate::tasks::new_pid();
        let initial = Stats::from_json(&j.info);
        let rep = j.rep.clone();
        j.running = Some(Running { stop, live: live.clone(), pid: pid.clone() });
        self.push_history(j, Event::Started);
        crate::stat!("couch_replicator.jobs.starts").inc();
        tracing::info!("couch_replicator_scheduler: Job {id} started as {pid}");
        let (s, id) = (self.clone(), id.to_string());
        tokio::spawn(async move {
            let r = job::run(rep, server, stop_rx, live, initial, pid.clone()).await;
            s.job_exited(&id, &pid, r);
        });
    }

    fn job_exited(self: &Arc<Self>, id: &str, pid: &str, r: Result<JobEnd, JobError>) {
        let mut jobs = self.jobs.lock();
        let current = jobs.get(id).is_some_and(|j| j.running.as_ref().is_some_and(|r| r.pid == pid));
        if current && let Some(j) = jobs.get(id) {
            self.share.lock().charge(j, self.interval_ms());
        }
        let outcome = match r {
            Ok(JobEnd::Finished(h)) => {
                if current && let Some(j) = jobs.remove(id) {
                    self.share.lock().removed(id, &j.rep);
                    tracing::info!("couch_replicator_scheduler: Job {id} completed normally");
                    if let (Some(db), Some(doc)) = (&j.rep.db_name, &j.rep.doc_id) {
                        super::docs::completed(db.clone(), doc.clone(), j.rep.start_time, j.running.as_ref().map(|r| r.live.lock().info.clone()).unwrap_or(Value::Null));
                    }
                }
                Outcome::Finished(h)
            }
            Ok(JobEnd::Stopped) => Outcome::Stopped,
            Err(e) => {
                if current {
                    let transient = jobs.get(id).is_some_and(|j| j.rep.db_name.is_none());
                    if transient {
                        tracing::error!("couch_replicator_scheduler : Transient job {id} failed, removing. Error: {}", e.reason());
                        if let Some(j) = jobs.remove(id) {
                            self.share.lock().removed(id, &j.rep);
                        }
                    } else if let Some(j) = jobs.get_mut(id) {
                        j.info = j.running.take().map(|r| r.live.lock().info.clone()).unwrap_or(Value::Null);
                        self.push_history(j, Event::Crashed(e.reason()));
                        crate::stat!("couch_replicator.jobs.crashes").inc();
                        if let (Some(db), Some(doc)) = (&j.rep.db_name, &j.rep.doc_id) {
                            super::docs::crashed(db.clone(), doc.clone(), j.rep.id_str(), e.reason());
                        }
                    }
                }
                Outcome::Error(e)
            }
        };
        drop(jobs);
        if let Some(ws) = self.waiters.lock().remove(id) {
            let mut outcome = Some(outcome);
            for w in ws {
                let o = match outcome.take() {
                    Some(o) => o,
                    None => Outcome::Stopped,
                };
                let _ = w.send(o);
            }
        }
        self.start_pending();
        self.update_gauges();
    }

    /// consecutive_crashes/2: crashes in a row, each soon after the event
    /// before it.
    fn consecutive_crashes(&self, h: &[(Event, SystemTime)]) -> u32 {
        let threshold = Duration::from_secs(self.health_threshold());
        let mut n = 0;
        let mut i = 0;
        while i < h.len() {
            match (&h[i].0, h.get(i + 1)) {
                (Event::Crashed(_), Some((_, prev))) => {
                    if h[i].1.duration_since(*prev).unwrap_or_default() > threshold {
                        return n;
                    }
                    n += 1;
                }
                (Event::Crashed(_), None) => return n + 1,
                (Event::Stopped, Some((Event::Started, _))) => return n,
                _ => {}
            }
            i += 1;
        }
        n
    }

    fn healthy(&self, j: &JobEntry) -> bool {
        match j.history.first() {
            Some((Event::Added, _)) if j.history.len() == 1 => true,
            Some((Event::Stopped, _)) => true,
            _ => {
                let latest = j.history.iter().find_map(|(e, t)| matches!(e, Event::Crashed(_)).then_some(*t)).unwrap_or(SystemTime::UNIX_EPOCH);
                let n = self.consecutive_crashes(&j.history);
                let backoff = BACKOFF_INTERVAL * (1u32 << n.saturating_sub(1).min(MAX_BACKOFF_EXPONENT));
                SystemTime::now().duration_since(latest).unwrap_or_default() >= backoff
            }
        }
    }

    fn last_started(j: &JobEntry) -> SystemTime {
        j.history.iter().find_map(|(e, t)| matches!(e, Event::Started).then_some(*t)).unwrap_or(SystemTime::UNIX_EPOCH)
    }

    fn start_pending(self: &Arc<Self>) {
        let (_, running) = self.counts();
        let slots = self.max_jobs().saturating_sub(running);
        self.start_jobs(slots);
    }

    /// Start up to `n` healthy waiting jobs: lowest priority, then those
    /// that ran least recently, first.
    fn start_jobs(self: &Arc<Self>, n: usize) {
        if n == 0 {
            return;
        }
        let mut pending: Vec<(i64, SystemTime, String)> = {
            let jobs = self.jobs.lock();
            let share = self.share.lock();
            jobs.iter()
                .filter(|(_, j)| j.running.is_none() && self.healthy(j))
                .map(|(id, j)| (share.priority(id), Self::last_started(j), id.clone()))
                .collect()
        };
        pending.sort();
        for (_, _, id) in pending.into_iter().take(n) {
            self.start_job(&id);
        }
    }

    /// Stop up to `n` running jobs of a kind: highest priority, then those
    /// started longest ago, first.
    fn stop_jobs(&self, n: usize, continuous: bool) -> usize {
        let mut jobs = self.jobs.lock();
        let mut running: Vec<(i64, SystemTime, String)> = {
            let share = self.share.lock();
            jobs.iter()
                .filter(|(_, j)| j.running.is_some() && j.rep.options.continuous == continuous)
                .map(|(id, j)| (-share.priority(id), Self::last_started(j), id.clone()))
                .collect()
        };
        running.sort();
        let mut stopped = 0;
        for (_, _, id) in running.into_iter().take(n) {
            if let Some(j) = jobs.get_mut(&id)
                && let Some(r) = j.running.take()
            {
                j.info = r.live.lock().info.clone();
                let _ = r.stop.send(true);
                self.push_history(j, Event::Stopped);
                crate::stat!("couch_replicator.jobs.stops").inc();
                stopped += 1;
            }
        }
        stopped
    }

    /// reschedule/1: trim to max_jobs, then rotate.
    fn reschedule(self: &Arc<Self>) {
        if let Some(server) = self.server.upgrade() {
            let jobs = self.jobs.lock();
            self.share.lock().update(&server, &jobs, self.interval_ms());
        }
        let max_jobs = self.max_jobs();
        let churn_max = self.server.upgrade().map(|s| cfg(&s, "max_churn", 20)).unwrap_or(20) as usize;
        let (_, running) = self.counts();
        let excess = running.saturating_sub(max_jobs);
        let stopped = self.stop_jobs(excess, true);
        self.stop_jobs(excess - stopped, false);
        let (total, running) = self.counts();
        let pending = total - running;
        let churn = churn_max.saturating_sub(excess);
        if max_jobs >= running {
            let slots = max_jobs - running;
            let stop = pending.saturating_sub(slots).min(running).min(churn);
            self.stop_jobs(stop, true);
            let (_, running) = self.counts();
            self.start_jobs(max_jobs.saturating_sub(running));
        }
        self.update_gauges();
    }

    fn update_gauges(&self) {
        let jobs = self.jobs.lock();
        let running = jobs.values().filter(|j| j.running.is_some()).count() as i64;
        let crashed = jobs.values().filter(|j| j.running.is_none() && matches!(j.history.first(), Some((Event::Crashed(_), _)))).count() as i64;
        crate::stat!("couch_replicator.jobs.running").set(running);
        crate::stat!("couch_replicator.jobs.pending").set(jobs.len() as i64 - running);
        crate::stat!("couch_replicator.jobs.crashed").set(crashed);
        crate::stat!("couch_replicator.jobs.total").set(jobs.len() as i64);
    }

    fn job_json(id: &str, j: &JobEntry) -> Value {
        let history: Vec<Value> = j
            .history
            .iter()
            .map(|(e, t)| match e {
                Event::Crashed(r) => json!({"timestamp": crate::auth::iso8601(secs(*t)), "type": "crashed", "reason": r}),
                other => json!({"timestamp": crate::auth::iso8601(secs(*t)), "type": format!("{other:?}").to_lowercase()}),
            })
            .collect();
        let info = j.running.as_ref().map(|r| r.live.lock().info.clone()).unwrap_or_else(|| j.info.clone());
        json!({
            "database": j.rep.db_name,
            "id": id,
            "pid": j.running.as_ref().map(|r| r.pid.clone()),
            "source": url_strip_password(&j.rep.source.url),
            "target": url_strip_password(&j.rep.target.url),
            "user": j.rep.user_ctx.name,
            "doc_id": j.rep.doc_id,
            "info": if info.as_object().is_some_and(|o| o.is_empty()) { Value::Null } else { info },
            "history": history,
            "node": crate::http::NODE_NAME,
            "start_time": crate::auth::iso8601(secs(j.rep.start_time)),
        })
    }

    /// _scheduler/jobs: every job, by id.
    pub fn jobs_json(&self) -> Vec<Value> {
        self.jobs.lock().iter().map(|(id, j)| Self::job_json(id, j)).collect()
    }

    pub fn job_json_by_id(&self, id: &str) -> Option<Value> {
        self.jobs.lock().get(id).map(|j| Self::job_json(id, j))
    }

    /// job_summary/2, for _scheduler/docs: the state of a replicator doc's
    /// job.
    pub fn job_summary(&self, id: &str) -> Option<Map<String, Value>> {
        let jobs = self.jobs.lock();
        let j = jobs.get(id)?;
        let errors = self.consecutive_crashes(&j.history);
        let crash = |h: &[(Event, SystemTime)]| match h.first() {
            Some((Event::Crashed(r), _)) => json!({"error": r}),
            _ => Value::Null,
        };
        let (state, info) = match (&j.running, errors) {
            (Some(r), _) => ("running", r.live.lock().info.clone()),
            (None, 0) if matches!(j.history.first(), Some((Event::Crashed(_), _))) => ("crashing", crash(&j.history)),
            (None, 0) => ("pending", j.info.clone()),
            (None, _) => ("crashing", crash(&j.history)),
        };
        let mut m = Map::new();
        m.insert("source".into(), json!(url_strip_password(&j.rep.source.url)));
        m.insert("target".into(), json!(url_strip_password(&j.rep.target.url)));
        m.insert("state".into(), json!(state));
        m.insert("info".into(), if info.as_object().is_some_and(|o| o.is_empty()) { Value::Null } else { info });
        m.insert("error_count".into(), json!(errors));
        m.insert("last_updated".into(), json!(j.history.first().map(|(_, t)| crate::auth::iso8601(secs(*t)))));
        m.insert("start_time".into(), json!(crate::auth::iso8601(secs(j.rep.start_time))));
        m.insert("source_proxy".into(), json!(j.rep.source.proxy.as_deref().map(url_strip_password)));
        m.insert("target_proxy".into(), json!(j.rep.target.proxy.as_deref().map(url_strip_password)));
        Some(m)
    }

    /// Ids of the jobs a replicator db's docs started.
    pub fn jobs_of_doc(&self, db: &str, doc: Option<&str>) -> Vec<String> {
        self.jobs
            .lock()
            .iter()
            .filter(|(_, j)| j.rep.db_name.as_deref() == Some(db) && (doc.is_none() || j.rep.doc_id.as_deref() == doc))
            .map(|(id, _)| id.clone())
            .collect()
    }
}

/// check_authorization/2: the job's owner or an admin may touch a job.
pub fn check_authorization(id: &str, user: &crate::auth::UserCtx) -> Result<bool, RepError> {
    let Some(s) = get() else { return Ok(false) };
    match s.rep_state(id) {
        None => Ok(false),
        Some(rep) if rep.user_ctx.name == user.name => Ok(true),
        Some(_) if user.is_server_admin() => Ok(true),
        Some(_) => Err(RepError::Unauthorized("You are not a server admin.".into())),
    }
}
