//! Connection-scaling benchmark: a growing number of simulated hosts, each
//! holding thousands of keep-alive clients, every client running a closed
//! loop of GET doc / PUT doc / GET attachment requests.
//!
//! Each host is one loopback source address (127.0.0.2, 127.0.0.3, ...),
//! since macOS gives a source address only ~16K ephemeral ports. Every step
//! adds one host's clients, lets the load settle, then measures a window.
//!
//!   couchscale --url http://admin:bench@127.0.0.1:25984 --name rust
//!     [--hosts 127.0.0.2,...] [--per-host 15000] [--warm 15] [--measure 60]
//!     [--ramp 3000] [--server-pid PID] [--out results.json]

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

const POOL_DOCS: usize = 100_000;
const DOC_BYTES: usize = 1000;
const ATT_BYTES: usize = 4000;
const REQ_TIMEOUT: Duration = Duration::from_secs(120);
const OPS: [&str; 3] = ["get", "put", "get_att"];

struct Args {
    url: String,
    name: String,
    db: String,
    out: String,
    hosts: Vec<IpAddr>,
    per_host: usize,
    warm: u64,
    measure: u64,
    ramp: usize,
    server_pid: Option<i32>,
    /// Reuse the database another run set up (for several generators).
    no_setup: bool,
    /// Offset for client ids, so several generators own distinct docs.
    id_base: usize,
}

fn parse_args() -> Args {
    let mut a = Args {
        url: String::new(),
        name: String::new(),
        db: "scale".into(),
        out: "results.json".into(),
        hosts: (2..=7).map(|i| format!("127.0.0.{i}").parse().unwrap()).collect(),
        per_host: 15_000,
        warm: 15,
        measure: 60,
        ramp: 3000,
        server_pid: None,
        no_setup: false,
        id_base: 0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut v = || it.next().unwrap_or_else(|| panic!("{k} needs a value"));
        match k.as_str() {
            "--url" => a.url = v(),
            "--name" => a.name = v(),
            "--db" => a.db = v(),
            "--out" => a.out = v(),
            "--hosts" => a.hosts = v().split(',').map(|h| h.parse().expect("host address")).collect(),
            "--per-host" => a.per_host = v().parse().unwrap(),
            "--warm" => a.warm = v().parse().unwrap(),
            "--measure" => a.measure = v().parse().unwrap(),
            "--ramp" => a.ramp = v().parse().unwrap(),
            "--server-pid" => a.server_pid = Some(v().parse().unwrap()),
            "--no-setup" => a.no_setup = true,
            "--id-base" => a.id_base = v().parse().unwrap(),
            _ => panic!("unknown argument {k}"),
        }
    }
    assert!(!a.url.is_empty() && !a.name.is_empty(), "--url and --name are required");
    a
}

// ------------------------------------------------------------ helpers

fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            out.push(if i <= c.len() { T[(n >> (18 - 6 * i) & 63) as usize] as char } else { '=' });
        }
    }
    out
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const WORDS: &[&str] = &[
    "the", "of", "and", "replication", "document", "revision", "server", "a", "to", "in", "view", "index", "is",
    "that", "database", "for", "conflict", "it", "with", "as", "change", "node", "was", "on", "attachment",
];

fn text(rng: &mut Rng, len: usize) -> String {
    let mut s = String::with_capacity(len + 16);
    while s.len() < len {
        s.push_str(WORDS[rng.below(WORDS.len() as u64) as usize]);
        s.push(' ');
    }
    s.truncate(len);
    s
}

/// Latency histogram with ~2% wide log buckets from 10 µs, filled by many
/// clients at once without locking.
struct Hist {
    buckets: Vec<AtomicU64>,
    sum_us: AtomicU64,
}

const HIST_BASE_US: f64 = 10.0;
const HIST_GROWTH: f64 = 1.02;
const HIST_N: usize = 1000; // up to ~4,000 s

impl Hist {
    fn new() -> Hist {
        Hist { buckets: (0..HIST_N).map(|_| AtomicU64::new(0)).collect(), sum_us: AtomicU64::new(0) }
    }
    fn record(&self, d: Duration) {
        let us = d.as_secs_f64() * 1e6;
        let i = if us <= HIST_BASE_US { 0 } else { ((us / HIST_BASE_US).ln() / HIST_GROWTH.ln()).ceil() as usize };
        self.buckets[i.min(HIST_N - 1)].fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us as u64, Relaxed);
    }
    fn summary(&self, secs: f64) -> Value {
        let counts: Vec<u64> = self.buckets.iter().map(|b| b.load(Relaxed)).collect();
        let n: u64 = counts.iter().sum();
        let pct = |p: f64| {
            let target = ((n as f64 * p).ceil() as u64).max(1);
            let mut acc = 0;
            for (i, c) in counts.iter().enumerate() {
                acc += c;
                if acc >= target {
                    return HIST_BASE_US * HIST_GROWTH.powi(i as i32) / 1000.0;
                }
            }
            0.0
        };
        json!({
            "ops": n,
            "ops_per_sec": n as f64 / secs,
            "mean_ms": if n == 0 { 0.0 } else { self.sum_us.load(Relaxed) as f64 / n as f64 / 1000.0 },
            "p50_ms": pct(0.50), "p90_ms": pct(0.90), "p95_ms": pct(0.95), "p99_ms": pct(0.99), "max_ms": pct(1.0),
        })
    }
}

/// What every client reports into: the current measurement window's
/// histograms (None outside a window) and running counters.
struct Shared {
    window: std::sync::RwLock<Option<Arc<Window>>>,
    stop: AtomicBool,
    connected: AtomicUsize,
    connect_errors: AtomicU64,
}

struct Window {
    hists: [Hist; 3],
    http_errors: AtomicU64,
    io_errors: AtomicU64,
    timeouts: AtomicU64,
}

impl Window {
    fn new() -> Window {
        Window {
            hists: [Hist::new(), Hist::new(), Hist::new()],
            http_errors: AtomicU64::new(0),
            io_errors: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
        }
    }
}

struct Target {
    addr: SocketAddr,
    host_header: String,
    auth: String,
    db: String,
}

async fn connect(t: &Target, src: IpAddr) -> std::io::Result<SendRequest<Full<Bytes>>> {
    let sock = tokio::net::TcpSocket::new_v4()?;
    // Optional cap on the kernel socket buffers (COUCHSCALE_SOCKBUF bytes),
    // to see whether the kernel's network memory pool is the limit.
    if let Some(n) = std::env::var("COUCHSCALE_SOCKBUF").ok().and_then(|v| v.parse().ok()) {
        sock.set_recv_buffer_size(n)?;
        sock.set_send_buffer_size(n)?;
    }
    sock.bind(SocketAddr::new(src, 0))?;
    let stream = sock.connect(t.addr).await?;
    stream.set_nodelay(true)?;
    let (send, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(std::io::Error::other)?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(send)
}

/// One request on this client's connection; returns status and body.
async fn call(
    send: &mut SendRequest<Full<Bytes>>,
    t: &Target,
    method: Method,
    path: &str,
    body: Option<Bytes>,
) -> Result<(StatusCode, Bytes), String> {
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header("host", &t.host_header)
        .header("authorization", &t.auth);
    if body.is_some() {
        b = b.header("content-type", "application/json");
    }
    let req = b.body(Full::new(body.unwrap_or_default())).unwrap();
    send.ready().await.map_err(|e| e.to_string())?;
    let resp = send.send_request(req).await.map_err(|e| e.to_string())?;
    let status = resp.status();
    let body = resp.into_body().collect().await.map_err(|e| e.to_string())?.to_bytes();
    Ok((status, body))
}

/// One simulated client: its own connection from its host's address,
/// looping get / put (its own doc) / get attachment until told to stop.
async fn client(id: usize, src: IpAddr, t: Arc<Target>, sh: Arc<Shared>) {
    let mut rng = Rng(0x9E3779B97F4A7C15 ^ (id as u64).wrapping_mul(0xBF58476D1CE4E5B9) | 1);
    let doc_id = format!("c-{id:06}");
    let mut rev: Option<String> = None;
    let mut send = None;
    let mut backoff = Duration::from_millis(100);
    // COUCHSCALE_OPS limits the mix, e.g. "get,get_att" or "put".
    let mix: Vec<usize> = match std::env::var("COUCHSCALE_OPS") {
        Ok(v) => v.split(',').map(|o| OPS.iter().position(|k| *k == o).expect("op name")).collect(),
        Err(_) => (0..OPS.len()).collect(),
    };
    while !sh.stop.load(Relaxed) {
        let s = match send.as_mut() {
            Some(s) => s,
            None => match connect(&t, src).await {
                Ok(s) => {
                    sh.connected.fetch_add(1, Relaxed);
                    backoff = Duration::from_millis(100);
                    send.insert(s)
                }
                Err(_) => {
                    sh.connect_errors.fetch_add(1, Relaxed);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                    continue;
                }
            },
        };
        let op = mix[rng.below(mix.len() as u64) as usize];
        let (method, path, body) = match op {
            0 => (Method::GET, format!("/{}/p-{:06}", t.db, rng.below(POOL_DOCS as u64)), None),
            1 => {
                let mut d = json!({"_id": doc_id, "client": id, "text": text(&mut rng, DOC_BYTES)});
                if let Some(r) = &rev {
                    d["_rev"] = json!(r);
                }
                (Method::PUT, format!("/{}/{doc_id}", t.db), Some(Bytes::from(d.to_string())))
            }
            _ => (Method::GET, format!("/{}/p-{:06}/a.txt", t.db, rng.below(POOL_DOCS as u64)), None),
        };
        let t0 = Instant::now();
        let r = tokio::time::timeout(REQ_TIMEOUT, call(s, &t, method, &path, body)).await;
        let win = sh.window.read().unwrap().clone();
        match r {
            Ok(Ok((st, body))) if st.is_success() => {
                if let Some(w) = &win {
                    w.hists[op].record(t0.elapsed());
                }
                if op == 1
                    && let Ok(v) = serde_json::from_slice::<Value>(&body)
                {
                    rev = v["rev"].as_str().map(str::to_string);
                }
            }
            Ok(Ok((st, _))) => {
                if let Some(w) = &win {
                    w.http_errors.fetch_add(1, Relaxed);
                }
                if op == 1 && st == StatusCode::CONFLICT {
                    rev = None; // re-learn the rev below
                    if let Ok(Ok((_, b))) =
                        tokio::time::timeout(REQ_TIMEOUT, call(s, &t, Method::GET, &format!("/{}/{doc_id}", t.db), None)).await
                        && let Ok(v) = serde_json::from_slice::<Value>(&b)
                    {
                        rev = v["_rev"].as_str().map(str::to_string);
                    }
                }
            }
            Ok(Err(_)) | Err(_) => {
                if let Some(w) = &win {
                    if r.is_err() { &w.timeouts } else { &w.io_errors }.fetch_add(1, Relaxed);
                }
                send = None;
                sh.connected.fetch_sub(1, Relaxed);
            }
        }
    }
    if send.is_some() {
        sh.connected.fetch_sub(1, Relaxed);
    }
}

// ---------------------------------------------------- server resources

/// (physical footprint bytes, CPU seconds) of a process, from
/// rusage_info_v4. Read in-process: spawning `ps` from a process holding
/// tens of thousands of sockets fails on macOS.
#[cfg(target_os = "macos")]
fn proc_usage(pid: i32) -> (u64, f64) {
    unsafe extern "C" {
        fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut u64) -> i32;
        fn mach_timebase_info(info: *mut [u32; 2]) -> i32;
    }
    let mut buf = [0u64; 64];
    // SAFETY: buf is larger than rusage_info_v4.
    if unsafe { proc_pid_rusage(pid, 4, buf.as_mut_ptr()) } != 0 {
        return (0, 0.0);
    }
    let mut tb = [0u32; 2];
    // SAFETY: mach_timebase_info fills {numer, denom}.
    unsafe { mach_timebase_info(&mut tb) };
    // After the 16-byte uuid: user time, system time (mach ticks), two
    // wakeup counts, pageins, wired, resident, physical footprint.
    let ticks = (buf[2] + buf[3]) as f64;
    (buf[9], ticks * tb[0] as f64 / tb[1] as f64 / 1e9)
}

/// (proportional set size bytes, CPU seconds) of a process from /proc.
#[cfg(not(target_os = "macos"))]
fn proc_usage(pid: i32) -> (u64, f64) {
    let pss = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup"))
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("Pss:")).and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()))
        .unwrap_or(0)
        * 1024;
    let cpu = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|s| {
            let f: Vec<&str> = s.rsplit_once(')')?.1.split_whitespace().collect();
            Some((f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?) / 100.0)
        })
        .unwrap_or(0.0);
    (pss, cpu)
}

fn self_cpu() -> f64 {
    let mut ru: std::mem::MaybeUninit<[i64; 18]> = std::mem::MaybeUninit::zeroed();
    unsafe extern "C" {
        fn getrusage(who: i32, usage: *mut [i64; 18]) -> i32;
    }
    // SAFETY: struct rusage is 18 words on 64-bit macOS; utime and stime
    // are the first two timevals.
    unsafe { getrusage(0, ru.as_mut_ptr()) };
    let r = unsafe { ru.assume_init() };
    (r[0] as f64 + r[1] as f64 / 1e6) + (r[2] as f64 + r[3] as f64 / 1e6)
}

// --------------------------------------------------------------- setup

async fn setup(t: &Target) {
    // Any local address: the clients' host addresses may be loopback aliases
    // or, across machines, the generator's own interface addresses.
    let src: IpAddr = "0.0.0.0".parse().unwrap();
    let mut s = connect(t, src).await.expect("connect for setup");
    let db = &t.db;
    let _ = call(&mut s, t, Method::DELETE, &format!("/{db}"), None).await;
    let (st, b) = call(&mut s, t, Method::PUT, &format!("/{db}"), None).await.unwrap();
    assert!(st.is_success(), "create {db}: {st} {}", String::from_utf8_lossy(&b));
    let mut rng = Rng(42);
    for start in (0..POOL_DOCS).step_by(1000) {
        let docs: Vec<Value> = (start..start + 1000)
            .map(|i| {
                json!({
                    "_id": format!("p-{i:06}"),
                    "text": text(&mut rng, DOC_BYTES),
                    "_attachments": {"a.txt": {"content_type": "text/plain", "data": base64(text(&mut rng, ATT_BYTES).as_bytes())}},
                })
            })
            .collect();
        let body = Bytes::from(json!({"docs": docs}).to_string());
        let (st, _) = call(&mut s, t, Method::POST, &format!("/{db}/_bulk_docs"), Some(body)).await.unwrap();
        assert!(st.is_success(), "bulk_docs: {st}");
    }
}

// ---------------------------------------------------------------- main

#[tokio::main]
async fn main() {
    let a = parse_args();
    let (scheme_rest, auth) = match a.url.split_once("://").map(|(_, r)| r).unwrap_or(&a.url).split_once('@') {
        Some((creds, host)) => (host.trim_end_matches('/').to_string(), format!("Basic {}", base64(creds.as_bytes()))),
        None => (a.url.split_once("://").map(|(_, r)| r).unwrap_or(&a.url).trim_end_matches('/').to_string(), String::new()),
    };
    let addr: SocketAddr = scheme_rest.parse().expect("--url must be http://[user:pass@]IP:PORT");
    let t = Arc::new(Target { addr, host_header: scheme_rest.clone(), auth, db: a.db.clone() });
    eprintln!("{}: setting up {} ({} docs with attachments)", a.name, a.db, POOL_DOCS);
    if !a.no_setup {
        setup(&t).await;
    }

    let sh = Arc::new(Shared {
        window: std::sync::RwLock::new(None),
        stop: AtomicBool::new(false),
        connected: AtomicUsize::new(0),
        connect_errors: AtomicU64::new(0),
    });
    let mut tasks = Vec::new();
    let mut steps = Vec::new();
    for (hi, &host) in a.hosts.iter().enumerate() {
        let clients = (hi + 1) * a.per_host;
        eprintln!("== {} step {}: host {host}, ramping to {clients} clients", a.name, hi + 1);
        let ramp0 = Instant::now();
        for j in 0..a.per_host {
            let id = a.id_base + hi * a.per_host + j;
            tasks.push(tokio::spawn(client(id, host, t.clone(), sh.clone())));
            if j % 100 == 99 {
                // Pace new connections at --ramp per second.
                let due = Duration::from_secs_f64((j + 1) as f64 / a.ramp as f64);
                if let Some(wait) = due.checked_sub(ramp0.elapsed()) {
                    tokio::time::sleep(wait).await;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(a.warm)).await;
        let w = Arc::new(Window::new());
        let (fp0, cpu0) = a.server_pid.map(proc_usage).unwrap_or((0, 0.0));
        let gen_cpu0 = self_cpu();
        let conn_err0 = sh.connect_errors.load(Relaxed);
        *sh.window.write().unwrap() = Some(w.clone());
        let unix = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();
        let window_start = unix();
        let t0 = Instant::now();
        let mut fp_peak = fp0;
        while t0.elapsed() < Duration::from_secs(a.measure) {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if let Some(pid) = a.server_pid {
                fp_peak = fp_peak.max(proc_usage(pid).0);
            }
        }
        *sh.window.write().unwrap() = None;
        let secs = t0.elapsed().as_secs_f64();
        let (_, cpu1) = a.server_pid.map(proc_usage).unwrap_or((0, 0.0));
        let ops: serde_json::Map<String, Value> =
            OPS.iter().enumerate().map(|(i, k)| (k.to_string(), w.hists[i].summary(secs))).collect();
        let total: f64 = ops.values().map(|v| v["ops_per_sec"].as_f64().unwrap()).sum();
        let step = json!({
            "hosts": hi + 1,
            "window_start_unix": window_start,
            "window_end_unix": unix(),
            "clients": clients,
            "connected": sh.connected.load(Relaxed),
            "secs": secs,
            "ops_per_sec": total,
            "ops": ops,
            "http_errors": w.http_errors.load(Relaxed),
            "io_errors": w.io_errors.load(Relaxed),
            "timeouts": w.timeouts.load(Relaxed),
            "connect_errors": sh.connect_errors.load(Relaxed) - conn_err0,
            "server_footprint_bytes": fp_peak,
            "server_cpu_cores": (cpu1 - cpu0) / secs,
            "generator_cpu_cores": (self_cpu() - gen_cpu0) / secs,
        });
        eprintln!(
            "   {clients} clients ({} connected): {total:.0} req/s | get p50 {:.1} p99 {:.1} ms | put p99 {:.1} ms | att p99 {:.1} ms | errors http {} io {} timeout {} connect {} | server {:.1} cores {:.2} GB",
            step["connected"],
            step["ops"]["get"]["p50_ms"].as_f64().unwrap(),
            step["ops"]["get"]["p99_ms"].as_f64().unwrap(),
            step["ops"]["put"]["p99_ms"].as_f64().unwrap(),
            step["ops"]["get_att"]["p99_ms"].as_f64().unwrap(),
            step["http_errors"], step["io_errors"], step["timeouts"], step["connect_errors"],
            step["server_cpu_cores"].as_f64().unwrap(),
            fp_peak as f64 / 1e9,
        );
        steps.push(step);
        save(&a, &steps);
    }
    sh.stop.store(true, Relaxed);
    // Clients finish their in-flight request and exit; don't wait on stragglers.
    let _ = tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(tasks)).await;
}

fn save(a: &Args, steps: &[Value]) {
    let mut all: Value =
        std::fs::read(&a.out).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or(json!({}));
    if !all[&a.name].is_object() {
        all[&a.name] = json!({});
    }
    all[&a.name]["scale"] = json!({
        "per_host": a.per_host, "warm_secs": a.warm, "measure_secs": a.measure,
        "pool_docs": POOL_DOCS, "doc_bytes": DOC_BYTES, "att_bytes": ATT_BYTES,
        "steps": steps,
    });
    std::fs::write(&a.out, serde_json::to_vec_pretty(&all).unwrap()).unwrap();
}
