//! Background work in progress, as couch_task_status keeps it for
//! _active_tasks: each task is its properties (type, database, progress,
//! changes_done, ...) plus a pid, process_status, started_on and
//! updated_on. A task is listed while its handle lives.

use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

struct Task {
    pid: String,
    props: Mutex<BTreeMap<String, Value>>,
}

static TASKS: Mutex<Vec<Arc<Task>>> = parking_lot::const_mutex(Vec::new());
static NEXT_PID: AtomicU64 = AtomicU64::new(1000);

/// A process-id-like name, `<0.N.0>`, as CouchDB's tasks carry.
pub fn new_pid() -> String {
    format!("<0.{}.0>", NEXT_PID.fetch_add(1, Ordering::Relaxed))
}

/// A registered task; dropping it removes the task.
pub struct TaskHandle(Arc<Task>);

/// Register a task with these properties (an object).
pub fn add(props: Value) -> TaskHandle {
    let now = json!(crate::http::now_secs());
    let mut map: BTreeMap<String, Value> = props.as_object().cloned().unwrap_or_default().into_iter().collect();
    map.insert("started_on".into(), now.clone());
    map.insert("updated_on".into(), now);
    let task = Arc::new(Task { pid: new_pid(), props: Mutex::new(map) });
    TASKS.lock().push(task.clone());
    TaskHandle(task)
}

impl TaskHandle {
    /// Merge in new property values.
    pub fn update(&self, props: Value) {
        let mut p = self.0.props.lock();
        for (k, v) in props.as_object().cloned().unwrap_or_default() {
            p.insert(k, v);
        }
        p.insert("updated_on".into(), json!(crate::http::now_secs()));
    }

    /// Count `n` more changes done toward total_changes, updating progress.
    pub fn add_changes(&self, n: u64) {
        let (done, total) = {
            let p = self.0.props.lock();
            (p.get("changes_done").and_then(Value::as_u64).unwrap_or(0) + n, p.get("total_changes").and_then(Value::as_u64).unwrap_or(0))
        };
        let progress = if total == 0 { 0 } else { (done * 100 / total).min(100) };
        self.update(json!({"changes_done": done, "progress": progress}));
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        TASKS.lock().retain(|t| !Arc::ptr_eq(t, &self.0));
    }
}

/// Every task, as _active_tasks lists them: node, pid and process_status,
/// then the properties in name order; tasks in pid order.
pub fn all() -> Vec<Value> {
    let mut tasks = TASKS.lock().clone();
    tasks.sort_by(|a, b| a.pid.cmp(&b.pid));
    tasks
        .iter()
        .map(|t| {
            let mut m = Map::new();
            m.insert("node".into(), json!(crate::http::NODE_NAME));
            m.insert("pid".into(), json!(t.pid));
            m.insert("process_status".into(), json!("running"));
            for (k, v) in t.props.lock().iter() {
                m.insert(k.clone(), v.clone());
            }
            Value::Object(m)
        })
        .collect()
}
