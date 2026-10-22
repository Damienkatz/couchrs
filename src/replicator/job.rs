//! One replication running (couch_replicator_scheduler_job, _worker and
//! _changes_reader). A reader follows the source's _changes into a
//! bounded queue; workers take batches, ask the target which revisions
//! it lacks (_revs_diff), fetch them (_bulk_get, else open_revs), and
//! write them with new_edits=false; the job tracks the sequence below
//! which everything is done and checkpoints it, every checkpoint_interval,
//! in _local/<replication id> on both ends.

use super::RepError;
use super::client::{Change, Client, DocWriteError, feed_stream, seq_encode};
use super::rep::{Filter, REP_ID_VERSION, Rep, filter};
use crate::server::Server;
use futures::StreamExt;
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;

const DOC_BUFFER_BYTE_SIZE: usize = 4 * 1024 * 1024;
const BULK_GET_RETRY_SEC: u64 = 37;
const REVS_DIFF_RETRY_SEC: u64 = 37;

/// Counts a replication keeps (couch_replicator_stats).
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub missing_checked: u64,
    pub missing_found: u64,
    pub docs_read: u64,
    pub docs_written: u64,
    pub doc_write_failures: u64,
    pub bulk_get_docs: u64,
    pub bulk_get_attempts: u64,
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        self.missing_checked += o.missing_checked;
        self.missing_found += o.missing_found;
        self.docs_read += o.docs_read;
        self.docs_written += o.docs_written;
        self.doc_write_failures += o.doc_write_failures;
        self.bulk_get_docs += o.bulk_get_docs;
        self.bulk_get_attempts += o.bulk_get_attempts;
    }

    fn max(&mut self, o: &Stats) {
        self.missing_checked = self.missing_checked.max(o.missing_checked);
        self.missing_found = self.missing_found.max(o.missing_found);
        self.docs_read = self.docs_read.max(o.docs_read);
        self.docs_written = self.docs_written.max(o.docs_written);
        self.doc_write_failures = self.doc_write_failures.max(o.doc_write_failures);
        self.bulk_get_docs = self.bulk_get_docs.max(o.bulk_get_docs);
        self.bulk_get_attempts = self.bulk_get_attempts.max(o.bulk_get_attempts);
    }

    /// From a history entry or a job's info (couch_replicator_stats:new/1).
    pub fn from_json(v: &Value) -> Stats {
        let n = |a: &str, b: &str| v.get(a).or_else(|| v.get(b)).and_then(Value::as_u64).unwrap_or(0);
        Stats {
            missing_checked: n("missing_checked", "revisions_checked"),
            missing_found: n("missing_found", "missing_revisions_found"),
            docs_read: n("docs_read", "docs_read"),
            docs_written: n("docs_written", "docs_written"),
            doc_write_failures: n("doc_write_failures", "doc_write_failures"),
            bulk_get_docs: n("bulk_get_docs", "bulk_get_docs"),
            bulk_get_attempts: n("bulk_get_attempts", "bulk_get_attempts"),
        }
    }
}

/// How a job ended.
pub enum JobEnd {
    /// One-shot, done: the checkpoint history.
    Finished(Value),
    /// Stopped by the scheduler or a cancel.
    Stopped,
}

/// Why a job failed: while starting (reported as an error response), or
/// later (a crash).
#[derive(Debug, Clone)]
pub enum JobError {
    Start(RepError),
    Crash(String),
}

impl JobError {
    pub fn reason(&self) -> String {
        match self {
            JobError::Start(e) => e.erl_text(),
            JobError::Crash(s) => s.clone(),
        }
    }
}

enum Msg {
    /// A batch handed to a worker: its (ts, seq).
    ReportSeq(u64, Value),
    SeqDone(u64, Value, Stats),
    SumStats(Stats),
    WorkerDone(Result<(), String>),
    ReaderDone(Result<(), String>),
}

/// Live figures a running job shares with the scheduler.
#[derive(Default)]
pub struct Live {
    /// The info object _scheduler/jobs reports.
    pub info: Value,
}

fn rfc1123(t: SystemTime) -> String {
    let secs = t.duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    crate::auth::rfc1123(secs)
}

struct State {
    rep: Rep,
    source: Arc<Client>,
    target: Arc<Client>,
    source_name: String,
    target_name: String,
    history: Vec<Value>,
    checkpoint_history: Value,
    start_seq: Value,
    committed_seq: (u64, Value),
    current_through_seq: (u64, Value),
    /// Batches handed out and not done, by ts.
    in_progress: BTreeMap<u64, Value>,
    highest_seq_done: (u64, Value),
    source_log: Map<String, Value>,
    target_log: Map<String, Value>,
    src_starttime: Value,
    tgt_starttime: Value,
    stats: Stats,
    session_id: String,
    task: crate::tasks::TaskHandle,
    pending: Option<(Instant, Value)>,
    live: Arc<Mutex<Live>>,
}

/// Run a replication until it finishes (one-shot) or is stopped.
pub async fn run(
    rep: Rep,
    server: Arc<Server>,
    mut stop: tokio::sync::watch::Receiver<bool>,
    live: Arc<Mutex<Live>>,
    initial_stats: Stats,
    pid: String,
) -> Result<JobEnd, JobError> {
    let jitter = server.config.get_int("replicator", "startup_jitter", 5000u64).max(1);
    let wait = Duration::from_millis(rand::random::<u64>() % jitter);
    tokio::select! {
        _ = tokio::time::sleep(wait) => {}
        _ = stop.changed() => return Ok(JobEnd::Stopped),
    }
    let mut st = init_state(&rep, &server, live, initial_stats, pid).await.map_err(JobError::Start)?;
    let workers = rep.options.worker_processes.max(1) as usize;
    let batch = rep.options.worker_batch_size.max(1) as usize;
    let (queue_tx, queue_rx) = mpsc::channel::<Change>((batch * workers * 2).max(1));
    let (tx, mut rx) = mpsc::unbounded_channel::<Msg>();
    let filt = filter(&rep.options).map_err(JobError::Start)?;
    // The changes reader.
    {
        let (source, tx, rep, start) = (st.source.clone(), tx.clone(), rep.clone(), st.start_seq.clone());
        let max_id = server.config.get("replicator", "max_document_id_length").and_then(|v| v.parse::<usize>().ok()).filter(|&n| n > 0);
        tokio::spawn(async move {
            let r = read_changes(&source, &rep, &filt, start, queue_tx, &tx, max_id).await;
            let _ = tx.send(Msg::ReaderDone(r));
        });
    }
    // The changes manager, shared by the workers.
    let manager = Arc::new(Manager { rx: tokio::sync::Mutex::new(queue_rx), ts: Mutex::new(0), batch, tx: tx.clone() });
    for _ in 0..workers {
        crate::stat!("couch_replicator.workers_started").inc();
        let w = Worker::new(&st, &rep, &server, manager.clone(), tx.clone());
        let tx = tx.clone();
        tokio::spawn(async move {
            let r = w.run().await;
            let _ = tx.send(Msg::WorkerDone(r));
        });
    }
    drop(tx);
    st.log_start(workers, batch);
    let interval = Duration::from_millis(rep.options.checkpoint_interval.max(100));
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    let mut workers_left = workers;
    loop {
        tokio::select! {
            m = rx.recv() => match m {
                Some(Msg::ReportSeq(ts, seq)) => {
                    st.in_progress.insert(ts, seq);
                }
                Some(Msg::SeqDone(ts, seq, stats)) => st.seq_done(ts, seq, stats).await,
                Some(Msg::SumStats(stats)) => st.stats.add(&stats),
                Some(Msg::ReaderDone(Ok(()))) => {}
                Some(Msg::ReaderDone(Err(e))) => {
                    crate::stat!("couch_replicator.changes_reader_deaths").inc();
                    tracing::error!("ChangesReader process died with reason: {e}");
                    return Err(JobError::Crash(e));
                }
                Some(Msg::WorkerDone(Ok(()))) => {
                    workers_left -= 1;
                    if workers_left == 0 {
                        return st.last_checkpoint().await;
                    }
                }
                Some(Msg::WorkerDone(Err(e))) => {
                    crate::stat!("couch_replicator.worker_deaths").inc();
                    tracing::error!("Worker died with reason: {e}");
                    return Err(JobError::Crash(e));
                }
                None => return st.last_checkpoint().await,
            },
            _ = timer.tick() => {
                match st.checkpoint().await {
                    Ok(()) => crate::stat!("couch_replicator.checkpoints.success").inc(),
                    Err(e) => {
                        crate::stat!("couch_replicator.checkpoints.failure").inc();
                        return Err(JobError::Crash(e));
                    }
                }
            }
            _ = stop.changed() => {
                // terminate(shutdown, ...): one last checkpoint.
                if let Err(e) = st.checkpoint().await {
                    tracing::error!("Failed last checkpoint. Job: {} Error: {e}", rep.id_str());
                }
                st.update_task();
                return Ok(JobEnd::Stopped);
            }
        }
    }
}

async fn init_state(rep: &Rep, server: &Server, live: Arc<Mutex<Live>>, initial_stats: Stats, pid: String) -> Result<State, RepError> {
    let mut src = rep.source.clone();
    if src.http_connections == 1 {
        src.http_connections = 2;
    }
    let source = Arc::new(Client::new(&src)?);
    let target = Arc::new(Client::new(&rep.target)?);
    source.db_open(false, &Map::new()).await?;
    target.db_open(rep.options.create_target, &rep.options.create_target_params).await?;
    let source_info = source.db_info().await?;
    let target_info = target.db_info().await?;
    let (source_log, target_log) = find_logs(rep, server, &source, &target).await;
    let (start_seq0, history) = compare_logs(&source_log, &target_log);
    let mut stats = initial_stats;
    if let Some(h) = history.first() {
        stats.max(&Stats::from_json(h));
    }
    let start_seq = if start_seq0 == json!(0) { rep.options.since_seq.clone().unwrap_or(json!(0)) } else { start_seq0 };
    let mut ch = Map::new();
    ch.insert("no_changes".into(), json!(true));
    for (k, v) in &source_log {
        if k != "_id" && k != "_rev" {
            ch.insert(k.clone(), v.clone());
        }
    }
    let task = crate::tasks::add_with_pid(json!({
        "type": "replication",
        "user": rep.user_ctx.name,
        "replication_id": rep.id_str(),
        "database": rep.db_name,
        "doc_id": rep.doc_id,
        "source": source.ep.uri(),
        "target": target.ep.uri(),
        "continuous": rep.options.continuous,
        "source_seq": seq_encode(&json!(0)),
        "checkpoint_interval": rep.options.checkpoint_interval,
    }), pid);
    let mut st = State {
        rep: rep.clone(),
        source_name: source.ep.uri(),
        target_name: target.ep.uri(),
        source,
        target,
        history,
        checkpoint_history: Value::Object(ch),
        start_seq: start_seq.clone(),
        committed_seq: (0, start_seq.clone()),
        current_through_seq: (0, start_seq.clone()),
        in_progress: BTreeMap::new(),
        highest_seq_done: (0, json!(0)),
        source_log,
        target_log,
        src_starttime: source_info.get("instance_start_time").cloned().unwrap_or(Value::Null),
        tgt_starttime: target_info.get("instance_start_time").cloned().unwrap_or(Value::Null),
        stats,
        session_id: uuid::Uuid::new_v4().simple().to_string(),
        task,
        pending: None,
        live,
    };
    // As CouchDB: worked out once at the start, then cached.
    st.refresh_pending().await;
    st.update_task();
    Ok(st)
}

/// The checkpoint docs on both ends, from this id or an older version's.
async fn find_logs(rep: &Rep, server: &Server, source: &Client, target: &Client) -> (Map<String, Value>, Map<String, Value>) {
    let new_id = format!("_local/{}", rep.id.0);
    let mut out = Vec::new();
    for db in [source, target] {
        let mut found = None;
        for vsn in (3..=REP_ID_VERSION).rev() {
            let id = if vsn == REP_ID_VERSION {
                new_id.clone()
            } else {
                match super::rep::replication_id_version(rep, server, vsn).await {
                    Ok(old) => format!("_local/{old}"),
                    Err(_) => break,
                }
            };
            if let Ok(Value::Object(doc)) = db.open_doc(&id).await {
                if id == new_id {
                    found = Some(doc);
                } else {
                    // Migrate it to the new id.
                    let mut m = doc.clone();
                    m.insert("_id".into(), json!(new_id));
                    m.remove("_rev");
                    if rep.options.use_checkpoints && let Ok(rev) = db.update_doc(&Value::Object(m.clone()), false).await {
                        m.insert("_rev".into(), json!(rev));
                        tracing::info!("Migrated replication checkpoint. Db:{} {id} -> {new_id}", db.ep.uri());
                    }
                    found = Some(m);
                }
                break;
            }
        }
        out.push(found.unwrap_or_else(|| {
            let mut m = Map::new();
            m.insert("_id".into(), json!(new_id));
            m
        }));
    }
    let t = out.pop().unwrap_or_default();
    let s = out.pop().unwrap_or_default();
    (s, t)
}

/// compare_replication_logs/2: where to start, and the history so far.
fn compare_logs(src: &Map<String, Value>, tgt: &Map<String, Value>) -> (Value, Vec<Value>) {
    let hist = |m: &Map<String, Value>| m.get("history").and_then(Value::as_array).cloned().unwrap_or_default();
    if src.get("session_id") == tgt.get("session_id") {
        return (src.get("source_last_seq").cloned().unwrap_or(json!(0)), hist(src));
    }
    tracing::info!("Replication records differ. Scanning histories to find a common ancestor.");
    let (s, t) = (hist(src), hist(tgt));
    let has = |sid: Option<&Value>, list: &[Value]| list.iter().any(|h| h.get("session_id") == sid && sid.is_some());
    let (mut si, mut ti) = (0, 0);
    while si < s.len() && ti < t.len() {
        let sid = s[si].get("session_id");
        if has(sid, &t[ti..]) {
            return (s[si].get("recorded_seq").cloned().unwrap_or(json!(0)), s[si + 1..].to_vec());
        }
        let tid = t[ti].get("session_id");
        if has(tid, &s[si + 1..]) {
            return (t[ti].get("recorded_seq").cloned().unwrap_or(json!(0)), t[ti + 1..].to_vec());
        }
        si += 1;
        ti += 1;
    }
    tracing::info!("no common ancestry -- performing full replication");
    (json!(0), vec![])
}

impl State {
    fn log_start(&self, workers: usize, batch: usize) {
        let from = match (&self.rep.db_name, &self.rep.doc_id) {
            (Some(db), Some(doc)) => format!("from doc {db}:{doc}"),
            _ => "from _replicate endpoint".into(),
        };
        tracing::info!(
            "Starting replication {} ({} -> {}) {from} worker_procesess:{workers} worker_batch_size:{batch} session_id:{}",
            self.rep.id_str(),
            self.source_name,
            self.target_name,
            self.session_id
        );
    }

    async fn seq_done(&mut self, ts: u64, seq: Value, inc: Stats) {
        let first = self.in_progress.keys().next().copied();
        let mut through = match first {
            None => (ts, seq.clone()),
            Some(f) if f == ts => {
                self.in_progress.remove(&ts);
                (ts, seq.clone())
            }
            Some(_) => {
                self.in_progress.remove(&ts);
                self.current_through_seq.clone()
            }
        };
        if ts > self.highest_seq_done.0 {
            self.highest_seq_done = (ts, seq);
        }
        if self.in_progress.is_empty() && self.highest_seq_done.0 > through.0 {
            through = self.highest_seq_done.clone();
        }
        self.current_through_seq = through;
        self.stats.add(&inc);
        self.refresh_pending().await;
        self.update_task();
    }

    /// changes_pending, refreshed at most every connection_timeout.
    async fn refresh_pending(&mut self) {
        let stale = self.pending.as_ref().is_none_or(|(t, _)| t.elapsed() > Duration::from_millis(self.rep.options.connection_timeout));
        if stale {
            let v = self.source.pending_count(&self.highest_seq_done.1).await;
            self.pending = Some((Instant::now(), v));
        }
    }

    fn rep_stats(&self) -> Map<String, Value> {
        let s = &self.stats;
        let mut m = Map::new();
        m.insert("revisions_checked".into(), json!(s.missing_checked));
        m.insert("missing_revisions_found".into(), json!(s.missing_found));
        m.insert("docs_read".into(), json!(s.docs_read));
        m.insert("docs_written".into(), json!(s.docs_written));
        m.insert("changes_pending".into(), self.pending.as_ref().map(|p| p.1.clone()).unwrap_or(Value::Null));
        m.insert("doc_write_failures".into(), json!(s.doc_write_failures));
        m.insert("bulk_get_docs".into(), json!(s.bulk_get_docs));
        m.insert("bulk_get_attempts".into(), json!(s.bulk_get_attempts));
        m.insert("checkpointed_source_seq".into(), json!(seq_encode(&self.committed_seq.1)));
        m
    }

    fn update_task(&self) {
        let mut info = self.rep_stats();
        info.insert("source_seq".into(), json!(seq_encode(&self.highest_seq_done.1)));
        info.insert("through_seq".into(), json!(seq_encode(&self.current_through_seq.1)));
        self.task.update(Value::Object(info.clone()));
        self.live.lock().info = Value::Object(info);
    }

    async fn last_checkpoint(&mut self) -> Result<JobEnd, JobError> {
        if self.in_progress.is_empty() && self.highest_seq_done.1 == json!(0) {
            return Ok(JobEnd::Finished(self.checkpoint_history.clone()));
        }
        self.current_through_seq = self.highest_seq_done.clone();
        match self.checkpoint().await {
            Ok(()) => {
                crate::stat!("couch_replicator.checkpoints.success").inc();
                self.update_task();
                Ok(JobEnd::Finished(self.checkpoint_history.clone()))
            }
            Err(e) => {
                crate::stat!("couch_replicator.checkpoints.failure").inc();
                Err(JobError::Crash(e))
            }
        }
    }

    /// do_checkpoint/1.
    async fn checkpoint(&mut self) -> Result<(), String> {
        if !self.rep.options.use_checkpoints {
            self.checkpoint_history = json!({"use_checkpoints": false});
            return Ok(());
        }
        if self.current_through_seq.1 == self.committed_seq.1 && self.current_through_seq.0 == self.committed_seq.0 {
            self.update_task();
            return Ok(());
        }
        let (src_commit, tgt_commit) = tokio::join!(self.source.ensure_full_commit(), self.target.ensure_full_commit());
        let t = match tgt_commit {
            Ok(t) => t,
            Err(e) => return Err(format!("{{checkpoint_commit_failure,<<\"Failure on target commit: {e}\">>}}")),
        };
        let s = match src_commit {
            Ok(s) => s,
            Err(e) => return Err(format!("{{checkpoint_commit_failure,<<\"Failure on source commit: {e}\">>}}")),
        };
        let zero = json!("0");
        let src_ok = s == self.src_starttime || s == zero || self.src_starttime == zero;
        let tgt_ok = t == self.tgt_starttime || t == zero || self.tgt_starttime == zero;
        let changed = match (src_ok, tgt_ok) {
            (true, true) => None,
            (true, false) => Some("target"),
            (false, true) => Some("source"),
            (false, false) => Some("source and target"),
        };
        if let Some(which) = changed {
            return Err(format!("{{checkpoint_commit_failure,<<\"instance_start_time on {which} database has changed since last checkpoint.\">>}}"));
        }
        let new_seq = self.current_through_seq.1.clone();
        tracing::info!("recording a checkpoint for `{}` -> `{}` at source update_seq {}", self.source_name, self.target_name, new_seq);
        let start = rfc1123(self.rep.start_time);
        let end = rfc1123(SystemTime::now());
        let s = &self.stats;
        let entry = json!({
            "session_id": self.session_id,
            "start_time": start,
            "end_time": end,
            "start_last_seq": self.start_seq,
            "end_last_seq": new_seq,
            "recorded_seq": new_seq,
            "missing_checked": s.missing_checked,
            "missing_found": s.missing_found,
            "docs_read": s.docs_read,
            "docs_written": s.docs_written,
            "doc_write_failures": s.doc_write_failures,
            "bulk_get_docs": s.bulk_get_docs,
            "bulk_get_attempts": s.bulk_get_attempts,
        });
        let mut base = Map::new();
        base.insert("session_id".into(), json!(self.session_id));
        base.insert("source_last_seq".into(), new_seq.clone());
        base.insert("replication_id_version".into(), json!(REP_ID_VERSION));
        if self.rep.options.doc_ids.is_some() {
            base.insert("start_time".into(), json!(start));
            base.insert("end_time".into(), json!(end));
            base.insert("docs_read".into(), json!(s.docs_read));
            base.insert("docs_written".into(), json!(s.docs_written));
            base.insert("doc_write_failures".into(), json!(s.doc_write_failures));
        }
        let mut hist = vec![entry];
        hist.extend(self.history.iter().cloned());
        hist.truncate(50);
        base.insert("history".into(), Value::Array(hist));
        let body = Value::Object(base);
        let src_rev = update_checkpoint(&self.source, &self.source_log, &body, "source").await?;
        let tgt_rev = update_checkpoint(&self.target, &self.target_log, &body, "target").await?;
        self.source_log.insert("_rev".into(), json!(src_rev));
        self.target_log.insert("_rev".into(), json!(tgt_rev));
        // The new history is what's built on next time.
        self.history = body.get("history").and_then(Value::as_array).cloned().unwrap_or_default();
        self.checkpoint_history = body;
        self.committed_seq = self.current_through_seq.clone();
        self.update_task();
        Ok(())
    }
}

/// Save a checkpoint doc; on a conflict, fine if what's there is ours.
async fn update_checkpoint(db: &Client, log: &Map<String, Value>, body: &Value, which: &str) -> Result<String, String> {
    let mut doc = body.as_object().cloned().unwrap_or_default();
    doc.insert("_id".into(), log.get("_id").cloned().unwrap_or(Value::Null));
    if let Some(r) = log.get("_rev") {
        doc.insert("_rev".into(), r.clone());
    }
    let fail = |r: String| format!("{{checkpoint_commit_failure,<<\"Error updating the {which} checkpoint document: {r}\">>}}");
    match db.update_doc(&Value::Object(doc), false).await {
        Ok(rev) => Ok(rev),
        Err(DocWriteError::Conflict) => {
            let id = log.get("_id").and_then(Value::as_str).unwrap_or_default();
            match db.open_doc(id).await {
                Ok(Value::Object(cur)) => {
                    let same = body.as_object().is_some_and(|b| b.iter().all(|(k, v)| cur.get(k) == Some(v)));
                    match cur.get("_rev").and_then(Value::as_str) {
                        Some(rev) if same => Ok(rev.to_string()),
                        _ => Err(fail("conflict".into())),
                    }
                }
                _ => Err(fail("conflict".into())),
            }
        }
        Err(e) => Err(fail(format!("{e:?}"))),
    }
}

// ------------------------------------------------------- changes reader

#[allow(clippy::too_many_arguments)]
async fn read_changes(
    source: &Client,
    rep: &Rep,
    filt: &Filter,
    start: Value,
    queue: mpsc::Sender<Change>,
    job: &mpsc::UnboundedSender<Msg>,
    max_id: Option<usize>,
) -> Result<(), String> {
    let continuous = rep.options.continuous;
    let style = if rep.options.winning_revs_only { "main_only" } else { "all_docs" };
    let timeout = (source.ep.timeout_ms / 3).max(1000);
    let mut last_seq = start.clone();
    let mut retries_left = source.ep.retries;
    let mut wait = Duration::from_millis(250);
    loop {
        let since = last_seq.clone();
        let result: Result<(), String> = match source.changes(continuous, style, &since, filt, timeout).await {
            Ok(resp) => {
                let mut ended_at: Option<Value> = None;
                let mut r = Ok(());
                let mut feed = std::pin::pin!(feed_stream(resp, continuous));
                while let Some(item) = feed.next().await {
                    match item {
                        Err(e) => {
                            r = Err(e);
                            break;
                        }
                        Ok(Change::Doc { ref id, ref seq, .. }) if id.is_empty() => {
                            tracing::error!("Replicator: ignoring document with empty ID in source database `{}` (_changes sequence {seq})", source.ep.uri());
                        }
                        Ok(Change::Doc { ref id, .. }) if max_id.is_some_and(|m| id.len() > m) => {
                            tracing::error!("Replicator: document id `{id}...` from source db `{}` is too long, ignoring.", source.ep.uri());
                            let _ = job.send(Msg::SumStats(Stats { doc_write_failures: 1, ..Default::default() }));
                        }
                        Ok(c @ Change::Doc { .. }) => {
                            if let Change::Doc { seq, .. } = &c {
                                last_seq = seq.clone();
                            }
                            // A full queue is back pressure: wait.
                            if queue.send(c).await.is_err() {
                                return Ok(());
                            }
                        }
                        Ok(Change::LastSeq(s)) => {
                            if continuous {
                                ended_at = Some(if s.is_null() { last_seq.clone() } else { s });
                            }
                            break;
                        }
                    }
                }
                match (r, ended_at) {
                    (Ok(()), Some(s)) => {
                        // The end of one continuous request: carry on from it.
                        if s != last_seq && queue.send(Change::LastSeq(s.clone())).await.is_err() {
                            return Ok(());
                        }
                        last_seq = s;
                        continue;
                    }
                    (Ok(()), None) if !continuous => return Ok(()),
                    (Ok(()), None) => continue,
                    // A feed cut off mid-stream is picked up where it left off.
                    (Err(e), _) if e.contains("connection closed") => continue,
                    (Err(e), _) => Err(e),
                }
            }
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = result {
            crate::stat!("couch_replicator.changes_read_failures").inc();
            if retries_left == 0 {
                return Err(e);
            }
            retries_left -= 1;
            if last_seq == start {
                tracing::info!("Retrying _changes request to source database {} with since={last_seq} in {} seconds", source.ep.uri(), wait.as_secs_f64());
                tokio::time::sleep(wait).await;
                wait *= 2;
            } else {
                tracing::info!("Retrying _changes request to source database {} with since={last_seq}", source.ep.uri());
            }
        }
    }
}

// --------------------------------------------------------------- workers

/// Hands batches of changes to workers (the changes manager), reporting
/// each batch's (ts, seq) to the job first.
struct Manager {
    rx: tokio::sync::Mutex<mpsc::Receiver<Change>>,
    ts: Mutex<u64>,
    batch: usize,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Manager {
    /// The next batch: (ts, seq to report, docs); None once closed.
    async fn get(&self) -> Option<(u64, Value, Vec<(String, Vec<String>)>)> {
        let mut rx = self.rx.lock().await;
        let first = rx.recv().await?;
        let mut items = vec![first];
        while items.len() < self.batch {
            match rx.try_recv() {
                Ok(c) => items.push(c),
                Err(_) => break,
            }
        }
        let ts = {
            let mut t = self.ts.lock();
            *t += 1;
            *t
        };
        let seq = match items.last() {
            Some(Change::Doc { seq, .. }) | Some(Change::LastSeq(seq)) => seq.clone(),
            None => Value::Null,
        };
        let docs = items
            .into_iter()
            .filter_map(|c| match c {
                Change::Doc { id, revs, .. } => Some((id, revs)),
                Change::LastSeq(_) => None,
            })
            .collect();
        let _ = self.tx.send(Msg::ReportSeq(ts, seq.clone()));
        Some((ts, seq, docs))
    }
}

/// A fetch success ratio that decides whether a request is worth trying.
#[derive(Clone, Copy)]
struct FetchStats {
    ratio: f64,
    t: Instant,
}

impl FetchStats {
    fn attempt(&self, retry_sec: u64, limit: f64) -> bool {
        self.t.elapsed().as_secs() > retry_sec || self.ratio <= limit
    }

    fn update(&mut self, successes: usize, attempts: usize, decay: f64) {
        let r = if attempts > 0 { (attempts - successes) as f64 / attempts as f64 } else { 0.0 };
        self.ratio += decay * (r - self.ratio);
        self.t = Instant::now();
    }
}

struct Worker {
    source: Arc<Client>,
    target: Arc<Client>,
    manager: Arc<Manager>,
    tx: mpsc::UnboundedSender<Msg>,
    use_bulk_get: bool,
    max_conns: usize,
    missing_retry: Duration,
    bulk_get: Mutex<FetchStats>,
    revs_diff: Mutex<FetchStats>,
}

/// Docs read and waiting to be written in one _bulk_docs.
#[derive(Default)]
struct Batch {
    docs: Vec<String>,
    size: usize,
}

impl Worker {
    fn new(st: &State, rep: &Rep, server: &Server, manager: Arc<Manager>, tx: mpsc::UnboundedSender<Msg>) -> Worker {
        let fresh = FetchStats { ratio: 0.0, t: Instant::now() };
        Worker {
            source: st.source.clone(),
            target: st.target.clone(),
            manager,
            tx,
            use_bulk_get: rep.options.use_bulk_get,
            max_conns: rep.options.http_connections.max(1) as usize,
            missing_retry: Duration::from_millis(server.config.get_int("replicator", "missing_doc_retry_msec", 2000u64)),
            bulk_get: Mutex::new(fresh),
            revs_diff: Mutex::new(fresh),
        }
    }

    async fn run(self) -> Result<(), String> {
        while let Some((ts, seq, changes)) = self.manager.get().await {
            let mut stats = Stats::default();
            if !changes.is_empty() {
                self.copy(changes, &mut stats).await?;
            }
            let _ = self.tx.send(Msg::SeqDone(ts, seq, stats));
        }
        Ok(())
    }

    /// Copy one batch of changes' missing revisions.
    async fn copy(&self, changes: Vec<(String, Vec<String>)>, stats: &mut Stats) -> Result<(), String> {
        let missing = self.find_missing(changes, stats).await?;
        let (ddocs, others): (Vec<_>, Vec<_>) = missing.into_iter().partition(|((id, _), _)| id.starts_with("_design/"));
        let mut fetch: Vec<((String, String), Vec<String>)> = ddocs;
        let mut batch = Batch::default();
        // _bulk_get for what it can (docs without attachments).
        let mut rest = others;
        let attempt = self.bulk_get.lock().attempt(BULK_GET_RETRY_SEC, 0.5);
        if self.use_bulk_get && !rest.is_empty() && attempt {
            let reqs: Vec<(String, String, Vec<String>)> = rest.iter().map(|((id, rev), pas)| (id.clone(), rev.clone(), pas.clone())).collect();
            let results = self.source.bulk_get(&reqs).await.unwrap_or_default();
            let mut got = HashMap::new();
            for (req, r) in reqs.iter().zip(results) {
                if let Ok(doc) = r
                    && doc.get("_attachments").is_none_or(|a| a.as_object().is_none_or(|o| o.is_empty()))
                {
                    got.insert((req.0.clone(), req.1.clone()), doc);
                }
            }
            stats.bulk_get_docs += got.len() as u64;
            stats.bulk_get_attempts += reqs.len() as u64;
            self.bulk_get.lock().update(got.len(), reqs.len(), 0.25);
            let mut ordered: Vec<_> = got.into_iter().collect();
            ordered.sort_by(|a, b| a.0.cmp(&b.0));
            let fetched: std::collections::HashSet<(String, String)> = ordered.iter().map(|(k, _)| k.clone()).collect();
            for (_, doc) in ordered {
                self.batch_doc(&doc, &mut batch, stats).await?;
            }
            rest.retain(|(k, _)| !fetched.contains(k));
        }
        fetch.extend(rest);
        // Each remaining revision on its own, a few at once.
        let fetched: Vec<Result<Vec<Value>, String>> = futures::stream::iter(fetch)
            .map(|((id, rev), pas)| async move { self.fetch_doc(&id, &rev, &pas).await })
            .buffer_unordered(self.max_conns)
            .collect()
            .await;
        for docs in fetched {
            for doc in docs? {
                let has_atts = doc.get("_attachments").and_then(Value::as_object).is_some_and(|o| !o.is_empty());
                let is_ddoc = doc.get("_id").and_then(Value::as_str).is_some_and(|i| i.starts_with("_design/"));
                if has_atts || is_ddoc {
                    stats.docs_read += 1;
                    if self.flush_doc(&doc).await? {
                        stats.docs_written += 1;
                    } else {
                        stats.doc_write_failures += 1;
                    }
                } else {
                    self.batch_doc(&doc, &mut batch, stats).await?;
                }
            }
        }
        self.flush_docs(std::mem::take(&mut batch.docs), stats).await
    }

    /// _revs_diff, or everything if lately it has found nearly nothing
    /// already on the target: (id, rev) -> possible ancestors.
    async fn find_missing(&self, changes: Vec<(String, Vec<String>)>, stats: &mut Stats) -> Result<Vec<((String, String), Vec<String>)>, String> {
        let id_revs: Vec<(String, Vec<String>)> = changes.into_iter().filter(|(_, r)| !r.is_empty()).collect();
        let all: usize = id_revs.iter().map(|(_, r)| r.len()).sum();
        let missing: Vec<((String, String), Vec<String>)> = if self.revs_diff.lock().attempt(REVS_DIFF_RETRY_SEC, 0.95) {
            let res = self.target.revs_diff(&id_revs).await.map_err(|e| e.erl_text())?;
            let mut out = Vec::new();
            let mut without_pas = 0;
            for (id, revs, pas) in res {
                for rev in revs {
                    if pas.is_empty() {
                        without_pas += 1;
                    }
                    out.push(((id.clone(), rev), pas.clone()));
                }
            }
            self.revs_diff.lock().update(all - without_pas.min(all), all, 0.4);
            out
        } else {
            id_revs.into_iter().flat_map(|(id, revs)| revs.into_iter().map(move |r| ((id.clone(), r), vec![]))).collect()
        };
        stats.missing_checked += all as u64;
        stats.missing_found += missing.len() as u64;
        Ok(missing)
    }

    /// fetch_doc/4: open_revs with attachments since the ancestors; once
    /// more without them if the source seems to lack the revision.
    async fn fetch_doc(&self, id: &str, rev: &str, pas: &[String]) -> Result<Vec<Value>, String> {
        match self.source.open_doc_revs(id, &[rev.to_string()], pas).await {
            Ok(d) => Ok(d),
            Err(RepError::MissingDoc) => {
                tracing::error!("Retrying fetch and update of document `{id}` as it is unexpectedly missing. Missing revisions are: {rev}");
                tokio::time::sleep(self.missing_retry).await;
                self.source.open_doc_revs(id, &[rev.to_string()], &[]).await.map_err(|e| e.erl_text())
            }
            Err(e) => Err(e.erl_text()),
        }
    }

    /// Add a doc to the batch, writing the batch first if it would pass
    /// 4MB.
    async fn batch_doc(&self, doc: &Value, batch: &mut Batch, stats: &mut Stats) -> Result<(), String> {
        let json = doc.to_string();
        stats.docs_read += 1;
        if batch.size + json.len() > DOC_BUFFER_BYTE_SIZE {
            let mut docs = std::mem::take(&mut batch.docs);
            docs.push(json);
            batch.size = 0;
            return self.flush_docs(docs, stats).await;
        }
        batch.size += json.len();
        batch.docs.push(json);
        Ok(())
    }

    /// _bulk_docs with new_edits=false, halving the batch when it's too
    /// large.
    async fn flush_docs(&self, docs: Vec<String>, stats: &mut Stats) -> Result<(), String> {
        if docs.is_empty() {
            return Ok(());
        }
        match self.target.bulk_docs(&docs).await.map_err(|e| e.erl_text())? {
            Some(errors) => {
                for e in &errors {
                    let f = |k: &str| e.get(k).and_then(Value::as_str).unwrap_or("");
                    tracing::error!(
                        "Replicator: couldn't write document `{}`, revision `{}`, to target database `{}`. Error: `{}`, reason: `{}`.",
                        f("id"),
                        f("rev"),
                        self.target.ep.uri(),
                        f("error"),
                        f("reason")
                    );
                }
                stats.docs_written += (docs.len() - errors.len()) as u64;
                stats.doc_write_failures += errors.len() as u64;
                Ok(())
            }
            None if docs.len() == 1 => {
                tracing::error!("Replicator: failed to write doc {}. Too large", docs[0]);
                stats.doc_write_failures += 1;
                Ok(())
            }
            None => {
                let mut a = docs;
                let b = a.split_off(a.len() / 2);
                Box::pin(self.flush_docs(a, stats)).await?;
                Box::pin(self.flush_docs(b, stats)).await
            }
        }
    }

    /// Write one doc (design docs, docs with attachments); false when it
    /// is skipped.
    async fn flush_doc(&self, doc: &Value) -> Result<bool, String> {
        let id = doc.get("_id").and_then(Value::as_str).unwrap_or_default().to_string();
        let rev = doc.get("_rev").and_then(Value::as_str).unwrap_or_default().to_string();
        match self.target.update_doc(doc, true).await {
            Ok(_) => Ok(true),
            Err(DocWriteError::MissingStub) => {
                // Out-of-sync stubs: fetch everything and try once more.
                tracing::error!("Retrying fetch and update of document `{id}` due to out of sync attachment stubs. Missing revisions are: {rev}");
                tokio::time::sleep(self.missing_retry).await;
                let docs = self.source.open_doc_revs(&id, &[rev], &[]).await.map_err(|e| e.erl_text())?;
                for d in docs {
                    if self.target.update_doc(&d, true).await.is_err() {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Err(DocWriteError::Skip(err, reason)) => {
                tracing::error!(
                    "Replicator: skipping writing document `{id}`, revision `{rev}`, to target database `{}`. Error: `{err}`, reason: `{reason}`.",
                    self.target.ep.uri()
                );
                Ok(false)
            }
            Err(DocWriteError::Conflict) => Err(format!("{{doc_write_failed,{{conflict,<<\"{id}\">>}}}}")),
            Err(DocWriteError::Other(e)) => {
                tracing::error!("Replicator: error writing document `{id}`, revision `{rev}`, to target database `{}`. Error: `{e}`.", self.target.ep.uri());
                Err(format!("{{doc_write_failed,{e}}}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_common_history() {
        let log = |sid: &str, hist: Value| {
            let mut m = Map::new();
            m.insert("session_id".into(), json!(sid));
            m.insert("source_last_seq".into(), json!(9));
            m.insert("history".into(), hist);
            m
        };
        // Same session: start from the source's last seq.
        assert_eq!(compare_logs(&log("a", json!([])), &log("a", json!([]))).0, json!(9));
        // Different: the newest session both histories have.
        let s = log("x", json!([{"session_id": "x", "recorded_seq": 5}, {"session_id": "b", "recorded_seq": 3}]));
        let t = log("y", json!([{"session_id": "y", "recorded_seq": 4}, {"session_id": "b", "recorded_seq": 3}]));
        assert_eq!(compare_logs(&s, &t).0, json!(3));
        assert_eq!(compare_logs(&log("x", json!([])), &log("y", json!([{"session_id": "z"}]))).0, json!(0));
    }
}
