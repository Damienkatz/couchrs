//! The node's own state for /_node/_local/_system, _versions and the
//! system half of _prometheus (chttpd_node:get_stats/0 and
//! couch_prometheus). Erlang VM figures have Rust stand-ins where one
//! exists: the process's resident memory, its context switches, bytes
//! through the HTTP sockets, tokio's run queue and live tasks, the
//! QuickJS engines, and each database writer's queue. The rest (atoms,
//! reductions, garbage collection, ETS, distribution) are zero.

use crate::server::Server;
use crate::stats::{labeled, plain, queue_stats, queue_summary, to_prom};
use serde_json::{Value, json};
use std::pin::Pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The ICU4X collator in Cargo.lock (checked by a test).
pub const ICU4X_VERSION: &str = "2.3.1";

static STARTED: OnceLock<Instant> = OnceLock::new();
static IO_IN: AtomicU64 = AtomicU64::new(0);
static IO_OUT: AtomicU64 = AtomicU64::new(0);

/// Note when the server started, for uptime.
pub fn mark_start() {
    let _ = STARTED.get_or_init(Instant::now);
}

pub fn uptime_secs() -> u64 {
    STARTED.get_or_init(Instant::now).elapsed().as_secs()
}

// ------------------------------------------------- counting HTTP sockets

/// A connection that counts the bytes through it (Erlang's io statistics).
pub struct Counted<S>(S);

impl<S> Counted<S> {
    pub fn new(s: S) -> Counted<S> {
        Counted(s)
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Counted<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.0).poll_read(cx, buf);
        IO_IN.fetch_add((buf.filled().len() - before) as u64, Relaxed);
        r
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Counted<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let r = Pin::new(&mut self.0).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &r {
            IO_OUT.fetch_add(*n as u64, Relaxed);
        }
        r
    }

    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[std::io::IoSlice<'_>]) -> Poll<std::io::Result<usize>> {
        let r = Pin::new(&mut self.0).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(n)) = &r {
            IO_OUT.fetch_add(*n as u64, Relaxed);
        }
        r
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// A TCP listener whose connections count their bytes.
pub struct CountingListener(pub tokio::net::TcpListener);

impl axum::serve::Listener for CountingListener {
    type Io = Counted<tokio::net::TcpStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.0.accept().await {
                Ok((s, a)) => return (Counted(s), a),
                // As axum's own listener does: per-connection errors are
                // the client's; anything else (out of file descriptors)
                // gets a pause before trying again.
                Err(e) if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset
                ) => {}
                Err(e) => {
                    tracing::error!("accept error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

// ------------------------------------------------------- process figures

fn rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
        // SAFETY: proc_pidinfo writes at most `size` bytes into `info`.
        let n = unsafe {
            libc::proc_pidinfo(libc::getpid(), libc::PROC_PIDTASKINFO, 0, &mut info as *mut _ as *mut libc::c_void, size)
        };
        if n == size { info.pti_resident_size } else { 0 }
    }
    #[cfg(target_os = "linux")]
    {
        let pages = std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| s.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
            .unwrap_or(0);
        // SAFETY: sysconf has no preconditions.
        pages * unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

fn context_switches() -> u64 {
    // SAFETY: getrusage fills the struct it is given.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        return 0;
    }
    (ru.ru_nvcsw + ru.ru_nivcsw) as u64
}

struct Runtime {
    run_queue: usize,
    tasks: usize,
}

fn runtime() -> Runtime {
    match tokio::runtime::Handle::try_current() {
        Ok(h) => {
            let m = h.metrics();
            Runtime { run_queue: m.global_queue_depth(), tasks: m.num_alive_tasks() }
        }
        Err(_) => Runtime { run_queue: 0, tasks: 0 },
    }
}

fn js_engines() -> i64 {
    crate::stat!("couchdb.query_server.process_count").get()
}

// --------------------------------------------------------------- reports

/// GET /_node/_local/_system.
pub fn system_json(server: &Server) -> Value {
    let rt = runtime();
    json!({
        "uptime": uptime_secs(),
        "memory": {
            "other": rss_bytes(), "atom": 0, "atom_used": 0, "processes": 0,
            "processes_used": 0, "binary": 0, "code": 0, "ets": 0
        },
        "run_queue": rt.run_queue,
        "run_queue_dirty_cpu": 0,
        "ets_table_count": 0,
        "bt_engine_cache": crate::btree::cache_info(),
        "context_switches": context_switches(),
        "reductions": 0,
        "garbage_collection_count": 0,
        "words_reclaimed": 0,
        "io_input": IO_IN.load(Relaxed),
        "io_output": IO_OUT.load(Relaxed),
        "os_proc_count": js_engines(),
        "stale_proc_count": 0,
        "process_count": rt.tasks,
        // Tokio has no task limit.
        "process_limit": 0,
        "message_queues": {
            "couch_file": {},
            "couch_db_updater": queue_stats(server.writer_queues())
        },
        "internal_replication_jobs": 0,
        "distribution": {},
        "distribution_events": {}
    })
}

/// The system metrics of _prometheus (couch_prometheus:get_system_stats/0).
pub fn system_prom(server: &Server) -> Vec<String> {
    let rt = runtime();
    let rss = rss_bytes();
    let queues = server.writer_queues();
    let total: u64 = queues.iter().sum();
    let min = queues.iter().min().copied().unwrap_or(0);
    let max = queues.iter().max().copied().unwrap_or(0);
    let cache = crate::btree::cache_info();
    let mem: Vec<_> = [("total", rss), ("processes", 0), ("processes_used", 0), ("system", rss), ("atom", 0), ("atom_used", 0), ("binary", 0), ("code", 0), ("ets", 0)]
        .into_iter()
        .map(|(t, v)| labeled("memory_type", t, v))
        .collect();
    [
        to_prom("uptime_seconds", "counter", "couchdb uptime", plain(uptime_secs())),
        to_prom("erlang_io_recv_bytes_total", "counter", "the total number of bytes received through ports", plain(IO_IN.load(Relaxed))),
        to_prom("erlang_io_sent_bytes_total", "counter", "the total number of bytes output to ports", plain(IO_OUT.load(Relaxed))),
        to_prom("erlang_message_queues", "gauge", "total size of all message queues", plain(total)),
        to_prom("erlang_message_queue_min", "gauge", "minimum size across all message queues", plain(min)),
        to_prom("erlang_message_queue_max", "gauge", "maximum size across all message queues", plain(max)),
        queue_summary("erlang_message_queue_couch_db_updater", "size of message queue across couch_db_updater processes", queues),
        to_prom("erlang_scheduler_queues", "gauge", "the total size of all normal run queues", plain(rt.run_queue)),
        to_prom("erlang_dirty_cpu_scheduler_queues", "gauge", "the total size of all dirty CPU scheduler run queues", plain(0)),
        to_prom("erlang_memory_bytes", "gauge", "size of memory (in bytes) dynamically allocated by the Erlang emulator", mem),
        to_prom("erlang_gc_collections_total", "counter", "number of garbage collections by the Erlang emulator", plain(0)),
        to_prom("erlang_gc_words_reclaimed_total", "counter", "number of words reclaimed by garbage collections", plain(0)),
        to_prom("erlang_context_switches_total", "counter", "total number of context switches", plain(context_switches())),
        to_prom("erlang_reductions_total", "counter", "total number of reductions", plain(0)),
        to_prom("erlang_processes", "gauge", "the number of Erlang processes", plain(rt.tasks)),
        to_prom("erlang_process_limit", "gauge", "the maximum number of simultaneously existing Erlang processes", plain(0)),
        to_prom("erlang_ets_table", "gauge", "number of ETS tables", plain(0)),
        to_prom("internal_replication_jobs", "gauge", "count of internal replication changes to process", plain(0)),
        to_prom(
            "membership",
            "gauge",
            "count of nodes in the cluster",
            vec![labeled("nodes", "cluster_nodes", 1), labeled("nodes", "all_nodes", 1)],
        ),
        // CouchDB names these with the prefix twice.
        to_prom("couchdb_bt_engine_cache_memory", "gauge", "memory used by the btree cache", plain(&cache["memory"])),
        to_prom("couchdb_bt_engine_cache_size", "gauge", "number of entries in the btree cache", plain(&cache["size"])),
        to_prom("clouseau_connected", "gauge", "clouseau connectivity status", plain(0)),
        to_prom("database_count", "gauge", "total database count", plain(server.all_dbs().len())),
        to_prom("ioq_interactive_requests", "gauge", "IOQ interactive queue requests", plain(0)),
        to_prom("ioq_background_requests", "gauge", "IOQ background queue requests", plain(0)),
        to_prom("ioq_total_requests", "gauge", "IOQ total active requests", plain(0)),
        crate::smoosh::prometheus(),
    ]
    .concat()
}

/// GET /_node/_local/_prometheus: the metrics, then the system's, a line
/// each.
pub fn prometheus(server: &Server) -> String {
    let mut out = String::new();
    for line in crate::stats::prometheus_metrics().into_iter().chain(system_prom(server)) {
        out.push_str(&line);
        out.push('\n');
    }
    out
}

/// GET /_node/_local/_versions.
pub fn versions() -> Value {
    json!({
        "javascript_engine": {"name": "quickjs"},
        "collation_driver": {"name": "icu4x", "library_version": ICU4X_VERSION}
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn icu4x_version_matches_lock_file() {
        let lock = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.lock")).unwrap();
        let at = lock.find("name = \"icu_collator\"\nversion = \"").expect("icu_collator in Cargo.lock");
        let rest = &lock[at + "name = \"icu_collator\"\nversion = \"".len()..];
        assert_eq!(&rest[..rest.find('"').unwrap()], super::ICU4X_VERSION);
    }
}
