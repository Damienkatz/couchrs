//! couchbench: load generator and benchmark for CouchDB-compatible servers.
//!
//! Loads a deterministic dataset (default 100 GB, document sizes from just
//! over 100 B to 1 GB) into one server, then measures put/get/delete speed
//! by size class, view build and query speed, and replication speed (driven
//! by a CouchDB replicator). Results are merged into a JSON file keyed by
//! server name, for the report generator.

use bytes::Bytes;
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

// ------------------------------------------------------------- config

/// Size bands (decades) and the share of total volume each one gets.
/// Sizes are log-uniform within a band; band 2 is (100 B, 1 KB] and band 8
/// is (100 MB, 1 GB].
const BANDS: [(u32, f64); 7] = [(2, 0.01), (3, 0.04), (4, 0.10), (5, 0.15), (6, 0.20), (7, 0.25), (8, 0.25)];

/// Docs up to this size are plain JSON; larger ones are a small JSON doc
/// plus a single binary attachment carrying the bytes.
const JSON_MAX: u64 = 64 * 1024;

/// Operation benchmark classes: (label, doc size, ops, concurrency).
const CLASSES: [(&str, u64, usize, usize); 8] = [
    ("100B", 100, 5000, 16),
    ("1KB", 1_000, 5000, 16),
    ("10KB", 10_000, 5000, 16),
    ("100KB", 100_000, 3000, 16),
    ("1MB", 1_000_000, 1000, 16),
    ("10MB", 10_000_000, 200, 8),
    ("100MB", 100_000_000, 20, 4),
    ("1GB", 1_000_000_000, 4, 2),
];

struct Args {
    url: String,
    name: String,
    replicator: String,
    db: String,
    total_gb: f64,
    seed: u64,
    phases: Vec<String>,
    out: String,
    keep_replica: bool,
    ops_scale: f64,
}

fn usage() -> ! {
    eprintln!(
        "usage: couchbench --url http://user:pass@host:port --name NAME [--replicator URL]\n\
         \x20      [--db bench] [--total-gb 100] [--seed 42] [--out results.json]\n\
         \x20      [--phases load,view,get,put,delete,replicate,compact] [--keep-replica] [--ops-scale 1.0]"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut a = Args {
        url: String::new(),
        name: String::new(),
        replicator: String::new(),
        db: "bench".into(),
        total_gb: 100.0,
        seed: 42,
        phases: "load,view,get,put,delete,replicate".split(',').map(String::from).collect(),
        out: "results.json".into(),
        keep_replica: false,
        ops_scale: 1.0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut v = || it.next().unwrap_or_else(|| usage());
        match k.as_str() {
            "--url" => a.url = v(),
            "--name" => a.name = v(),
            "--replicator" => a.replicator = v(),
            "--db" => a.db = v(),
            "--total-gb" => a.total_gb = v().parse().unwrap_or_else(|_| usage()),
            "--seed" => a.seed = v().parse().unwrap_or_else(|_| usage()),
            "--phases" => a.phases = v().split(',').map(String::from).collect(),
            "--out" => a.out = v(),
            "--keep-replica" => a.keep_replica = true,
            "--ops-scale" => a.ops_scale = v().parse().unwrap_or_else(|_| usage()),
            _ => usage(),
        }
    }
    if a.url.is_empty() || a.name.is_empty() {
        usage();
    }
    if a.replicator.is_empty() {
        a.replicator = a.url.clone();
    }
    a
}

// ---------------------------------------------------------------- data

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Fast deterministic PRNG (xorshift64*).
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(splitmix(seed) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// Vocabulary for generated text. Words are drawn at random (seeded), so
/// the content is deterministic but compresses like real prose rather than
/// like random bytes.
const WORDS: &[&str] = &[
    "the", "of", "and", "to", "in", "is", "that", "for", "it", "as", "was", "with", "be", "by", "on", "not",
    "he", "this", "are", "or", "his", "from", "at", "which", "but", "have", "an", "had", "they", "you", "were",
    "their", "one", "all", "we", "can", "her", "has", "there", "been", "if", "more", "when", "will", "would",
    "who", "so", "no", "other", "into", "time", "only", "some", "could", "new", "these", "two", "may", "first",
    "then", "do", "any", "like", "my", "now", "over", "such", "our", "man", "me", "even", "most", "made", "after",
    "also", "did", "many", "before", "must", "through", "back", "years", "where", "much", "your", "way", "well",
    "down", "should", "because", "each", "just", "those", "people", "how", "too", "little", "state", "good",
    "very", "make", "world", "still", "own", "see", "men", "work", "long", "get", "here", "between", "both",
    "life", "being", "under", "never", "day", "same", "another", "know", "while", "last", "might", "us", "great",
    "old", "year", "off", "come", "since", "against", "go", "came", "right", "used", "take", "three", "states",
    "himself", "few", "house", "use", "during", "without", "again", "place", "american", "around", "however",
    "home", "small", "found", "thought", "went", "say", "part", "once", "general", "high", "upon", "school",
    "every", "does", "got", "united", "left", "number", "course", "war", "until", "always", "away", "something",
    "fact", "though", "water", "less", "public", "put", "think", "almost", "hand", "enough", "far", "took", "head",
    "yet", "government", "system", "better", "set", "told", "nothing", "night", "end", "why", "called", "eyes",
    "find", "going", "look", "asked", "later", "knew", "point", "next", "program", "city", "business", "give",
    "group", "toward", "young", "days", "let", "room", "president", "side", "social", "given", "present", "several",
    "order", "national", "possible", "rather", "second", "face", "per", "among", "form", "important", "often",
    "things", "looked", "early", "white", "case", "become", "large", "big", "need", "four", "within", "felt",
    "children", "along", "saw", "best", "church", "ever", "least", "power", "development", "light", "thing",
    "seemed", "family", "interest", "want", "members", "mind", "country", "area", "others", "although", "turned",
    "done", "open", "service", "certain", "kind", "problem", "began", "different", "door", "thus", "help", "sense",
    "means", "whole", "matter", "perhaps", "itself", "york", "times", "law", "human", "line", "above", "name",
    "example", "action", "company", "hands", "local", "show", "whether", "five", "history", "gave", "today",
    "either", "act", "feet", "across", "taken", "past", "quite", "anything", "seen", "having", "death", "week",
];

impl Rng {
    /// Fill `buf` with space-separated words (ASCII, JSON-safe).
    fn fill_text(&mut self, buf: &mut [u8]) {
        let mut i = 0;
        while i < buf.len() {
            let w = WORDS[self.below(WORDS.len() as u64) as usize].as_bytes();
            let n = w.len().min(buf.len() - i);
            buf[i..i + n].copy_from_slice(&w[..n]);
            i += n;
            if i < buf.len() {
                buf[i] = b' ';
                i += 1;
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct DocSpec {
    idx: u64,
    band: u32,
    size: u64,
}

impl DocSpec {
    fn id(&self, seed: u64) -> String {
        format!("{:016x}", splitmix(seed ^ (self.idx.wrapping_mul(0x9E3779B97F4A7C15))))
    }
    fn group(&self) -> u64 {
        self.idx % 100
    }
}

/// The full dataset, deterministic for a given seed and size.
fn plan(total_bytes: f64, seed: u64) -> Vec<DocSpec> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::new();
    let mut idx = 0u64;
    for (band, share) in BANDS {
        let target = (total_bytes * share) as u64;
        let lo = 10f64.powi(band as i32).ln();
        let hi = 10f64.powi(band as i32 + 1).ln();
        let mut vol = 0u64;
        while vol < target {
            let mut size = (lo + rng.f64() * (hi - lo)).exp() as u64;
            size = size.clamp(101, 1_000_000_000);
            // Don't overshoot a small band's budget with one giant doc.
            if vol > 0 && vol + size > target + target / 10 {
                size = (target - vol).max(101);
            }
            out.push(DocSpec { idx, band, size });
            idx += 1;
            vol += size;
        }
    }
    // Shuffle so the load mixes sizes, as real traffic would.
    for i in (1..out.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        out.swap(i, j);
    }
    out
}

fn json_doc(spec: &DocSpec, seed: u64, with_id: bool, size: u64) -> Vec<u8> {
    let mut rng = Rng::new(seed ^ spec.idx ^ 0xD0C);
    let mut head = String::with_capacity(size as usize + 64);
    head.push('{');
    if with_id {
        head.push_str(&format!("\"_id\":\"{}\",", spec.id(seed)));
    }
    head.push_str(&format!(
        "\"band\":{},\"group\":{},\"size\":{},\"payload\":\"",
        spec.band,
        spec.group(),
        spec.size
    ));
    let pad = size.saturating_sub(head.len() as u64 + 2) as usize;
    let mut payload = vec![0u8; pad];
    rng.fill_text(&mut payload);
    let mut v = head.into_bytes();
    v.extend_from_slice(&payload);
    v.extend_from_slice(b"\"}");
    v
}

/// A streamed multipart/related body: doc JSON + one text/plain attachment
/// of `size` bytes of generated text. Returns (content-type, length, body).
fn multipart_doc(spec: &DocSpec, seed: u64) -> (String, u64, reqwest::Body) {
    let boundary = format!("bench{:016x}", splitmix(spec.idx ^ seed));
    let doc = json!({
        "band": spec.band,
        "group": spec.group(),
        "size": spec.size,
        "_attachments": {"blob": {"follows": true, "content_type": "text/plain", "length": spec.size}}
    });
    let head = format!(
        "--{boundary}\r\nContent-Type: application/json\r\n\r\n{doc}\r\n--{boundary}\r\nContent-Disposition: attachment; filename=\"blob\"\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
        spec.size
    );
    let tail = format!("\r\n--{boundary}--");
    let len = head.len() as u64 + spec.size + tail.len() as u64;
    let mut rng = Rng::new(seed ^ spec.idx ^ 0xA77);
    let total = spec.size;
    let data = futures::stream::unfold(0u64, move |sent| {
        let mut rng_local = Rng(rng.next() | 1);
        async move {
            if sent >= total {
                return None;
            }
            let n = (total - sent).min(1 << 20) as usize;
            let mut buf = vec![0u8; n];
            rng_local.fill_text(&mut buf);
            Some((Ok::<Bytes, std::io::Error>(Bytes::from(buf)), sent + n as u64))
        }
    });
    let body = futures::stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(head)) })
        .chain(data)
        .chain(futures::stream::once(async move { Ok(Bytes::from(tail)) }));
    (
        format!("multipart/related; boundary=\"{boundary}\""),
        len,
        reqwest::Body::wrap_stream(body),
    )
}

// -------------------------------------------------------------- client

#[derive(Clone)]
struct Couch {
    http: reqwest::Client,
    base: String,
    user: String,
    pass: String,
    /// Base URL with credentials, as given (for replication documents).
    full: String,
}

fn split_creds(url: &str) -> (String, String, String) {
    let (scheme, rest) = url.split_once("://").unwrap_or(("http", url));
    match rest.split_once('@') {
        Some((creds, host)) => {
            let (u, p) = creds.split_once(':').unwrap_or((creds, ""));
            (format!("{scheme}://{}", host.trim_end_matches('/')), u.to_string(), p.to_string())
        }
        None => (format!("{scheme}://{}", rest.trim_end_matches('/')), String::new(), String::new()),
    }
}

impl Couch {
    fn new(url: &str) -> Couch {
        let (base, user, pass) = split_creds(url);
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .no_proxy()
            .build()
            .unwrap();
        Couch { http, base, user, pass, full: url.trim_end_matches('/').to_string() }
    }
    fn req(&self, m: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let r = self.http.request(m, format!("{}{}", self.base, path));
        if self.user.is_empty() { r } else { r.basic_auth(&self.user, Some(&self.pass)) }
    }
    async fn json(&self, m: reqwest::Method, path: &str, body: Option<Value>) -> Result<(u16, Value), String> {
        let mut r = self.req(m, path);
        if let Some(b) = body {
            r = r.header("Content-Type", "application/json").body(b.to_string());
        }
        let resp = r.send().await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let text = resp.bytes().await.map_err(|e| e.to_string())?;
        Ok((status, serde_json::from_slice(&text).unwrap_or(Value::Null)))
    }
}

// ------------------------------------------------------------- metrics

#[derive(Default)]
struct Lat(Vec<f64>);
impl Lat {
    fn summary(mut self, elapsed: f64, bytes: u64) -> Value {
        self.0.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = self.0.len();
        // Nearest-rank percentile over every recorded operation.
        let pct = |p: f64| if n == 0 { 0.0 } else { self.0[((n as f64 * p).ceil() as usize).clamp(1, n) - 1] };
        let mean = if n == 0 { 0.0 } else { self.0.iter().sum::<f64>() / n as f64 };
        json!({
            "ops": n,
            "secs": elapsed,
            "ops_per_sec": n as f64 / elapsed.max(1e-9),
            "mb_per_sec": bytes as f64 / 1e6 / elapsed.max(1e-9),
            "bytes": bytes,
            "mean_ms": mean,
            "p50_ms": pct(0.50),
            "p90_ms": pct(0.90),
            "p95_ms": pct(0.95),
            "p99_ms": pct(0.99),
            "max_ms": pct(1.0),
        })
    }
}

fn mb(b: u64) -> f64 {
    b as f64 / 1e6
}

// ---------------------------------------------------------------- load

enum Job {
    Bulk(Vec<DocSpec>),
    Big(DocSpec),
}

async fn phase_load(c: &Couch, a: &Args, plan: &[DocSpec]) -> Value {
    let _ = c.json(reqwest::Method::DELETE, &format!("/{}", a.db), None).await;
    let (st, v) = c.json(reqwest::Method::PUT, &format!("/{}?q=1", a.db), None).await.unwrap();
    assert!(st == 201 || st == 202, "create db failed: {st} {v}");

    let mut jobs = Vec::new();
    let mut batch = Vec::new();
    let mut batch_bytes = 0u64;
    for s in plan {
        if s.size <= JSON_MAX {
            batch.push(*s);
            batch_bytes += s.size;
            if batch.len() >= 1000 || batch_bytes >= 4 << 20 {
                jobs.push(Job::Bulk(std::mem::take(&mut batch)));
                batch_bytes = 0;
            }
        } else {
            jobs.push(Job::Big(*s));
        }
    }
    if !batch.is_empty() {
        jobs.push(Job::Bulk(batch));
    }

    let total: u64 = plan.iter().map(|s| s.size).sum();
    let done_bytes = Arc::new(AtomicU64::new(0));
    let done_docs = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    // Bound concurrency by requests and by bytes in flight (MB permits).
    let reqs = Arc::new(Semaphore::new(16));
    let mem = Arc::new(Semaphore::new(2048));
    let t0 = Instant::now();
    let reporter = {
        let (db, dd) = (done_bytes.clone(), done_docs.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                let b = db.load(Ordering::Relaxed);
                let e = t0.elapsed().as_secs_f64();
                eprintln!(
                    "  load: {:.1} / {:.1} GB, {} docs, {:.0} MB/s, {:.0}s",
                    b as f64 / 1e9, total as f64 / 1e9, dd.load(Ordering::Relaxed), mb(b) / e, e
                );
            }
        })
    };
    let mut handles = Vec::new();
    for job in jobs {
        let mb_need = match &job {
            Job::Bulk(v) => (v.iter().map(|s| s.size).sum::<u64>() >> 20).max(1),
            Job::Big(s) => (s.size >> 20).clamp(1, 2048),
        } as u32;
        let r = reqs.clone().acquire_owned().await.unwrap();
        let m = mem.clone().acquire_many_owned(mb_need).await.unwrap();
        let (c, db, seed) = (c.clone(), a.db.clone(), a.seed);
        let (dbytes, ddocs, errs) = (done_bytes.clone(), done_docs.clone(), errors.clone());
        handles.push(tokio::spawn(async move {
            let _p = (r, m);
            match job {
                Job::Bulk(specs) => {
                    let docs: Vec<Vec<u8>> = specs.iter().map(|s| json_doc(s, seed, true, s.size)).collect();
                    let mut body = Vec::with_capacity(docs.iter().map(|d| d.len() + 1).sum::<usize>() + 16);
                    body.extend_from_slice(b"{\"docs\":[");
                    for (i, d) in docs.iter().enumerate() {
                        if i > 0 {
                            body.push(b',');
                        }
                        body.extend_from_slice(d);
                    }
                    body.extend_from_slice(b"]}");
                    for attempt in 0..5 {
                        let resp = c
                            .req(reqwest::Method::POST, &format!("/{db}/_bulk_docs"))
                            .header("Content-Type", "application/json")
                            .body(body.clone())
                            .send()
                            .await;
                        match resp {
                            Ok(r) if r.status().is_success() => {
                                let v: Value = r.json().await.unwrap_or(Value::Null);
                                let bad = v.as_array().map(|a| a.iter().filter(|x| x.get("error").is_some()).count()).unwrap_or(0);
                                errs.fetch_add(bad as u64, Ordering::Relaxed);
                                break;
                            }
                            Ok(r) => {
                                eprintln!("bulk_docs status {} (attempt {attempt})", r.status());
                                if attempt == 4 { errs.fetch_add(specs.len() as u64, Ordering::Relaxed); }
                            }
                            Err(e) => {
                                eprintln!("bulk_docs error {e} (attempt {attempt})");
                                if attempt == 4 { errs.fetch_add(specs.len() as u64, Ordering::Relaxed); }
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    dbytes.fetch_add(specs.iter().map(|s| s.size).sum(), Ordering::Relaxed);
                    ddocs.fetch_add(specs.len() as u64, Ordering::Relaxed);
                }
                Job::Big(s) => {
                    let mut ok = false;
                    for attempt in 0..3 {
                        let (ct, len, body) = multipart_doc(&s, seed);
                        let r = c
                            .req(reqwest::Method::PUT, &format!("/{db}/{}", s.id(seed)))
                            .header("Content-Type", ct)
                            .header("Content-Length", len)
                            .body(body)
                            .send()
                            .await;
                        match r {
                            Ok(r) if r.status().is_success() => {
                                let _ = r.bytes().await;
                                ok = true;
                                break;
                            }
                            Ok(r) => eprintln!("PUT big {} status {} attempt {attempt}: {}", s.size, r.status(), r.text().await.unwrap_or_default()),
                            Err(e) => eprintln!("PUT big {} error {e} attempt {attempt}", s.size),
                        }
                    }
                    if !ok {
                        errs.fetch_add(1, Ordering::Relaxed);
                    }
                    dbytes.fetch_add(s.size, Ordering::Relaxed);
                    ddocs.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    reporter.abort();
    let secs = t0.elapsed().as_secs_f64();
    let info = c.json(reqwest::Method::GET, &format!("/{}", a.db), None).await.unwrap().1;
    let small: Vec<&DocSpec> = plan.iter().filter(|s| s.size <= JSON_MAX).collect();
    let mut per_band = BTreeMap::new();
    for (band, _) in BANDS {
        let docs: Vec<&DocSpec> = plan.iter().filter(|s| s.band == band).collect();
        per_band.insert(
            format!("1e{band}"),
            json!({"docs": docs.len(), "bytes": docs.iter().map(|s| s.size).sum::<u64>()}),
        );
    }
    json!({
        "secs": secs,
        "bytes": total,
        "docs": plan.len(),
        "json_docs": small.len(),
        "attachment_docs": plan.len() - small.len(),
        "mb_per_sec": mb(total) / secs,
        "docs_per_sec": plan.len() as f64 / secs,
        "errors": errors.load(Ordering::Relaxed),
        "doc_count_after": info["doc_count"],
        "file_bytes_after": info["sizes"]["file"],
        "bands": per_band,
    })
}

// ------------------------------------------------------------ op benches

/// Run `n` operations with bounded concurrency; `op(i)` returns bytes moved.
async fn run_ops<F, Fut>(n: usize, conc: usize, op: F) -> (Lat, u64, f64, u64)
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<u64, String>> + Send + 'static,
{
    let op = Arc::new(op);
    let t0 = Instant::now();
    let results: Vec<(f64, Result<u64, String>)> = futures::stream::iter(0..n)
        .map(|i| {
            let op = op.clone();
            async move {
                let t = Instant::now();
                let r = op(i).await;
                (t.elapsed().as_secs_f64() * 1000.0, r)
            }
        })
        .buffer_unordered(conc)
        .collect()
        .await;
    let secs = t0.elapsed().as_secs_f64();
    let mut lat = Lat::default();
    let mut bytes = 0;
    let mut errs = 0;
    for (ms, r) in results {
        match r {
            Ok(b) => {
                lat.0.push(ms);
                bytes += b;
            }
            Err(e) => {
                if errs < 3 {
                    eprintln!("    op error: {e}");
                }
                errs += 1;
            }
        }
    }
    (lat, bytes, secs, errs)
}

fn class_ops(n: usize, scale: f64) -> usize {
    ((n as f64 * scale).round() as usize).max(2)
}

/// Docs created by the put phase, kept for the delete phase.
type Created = Arc<tokio::sync::Mutex<BTreeMap<String, Vec<(String, String)>>>>;

async fn phase_put(c: &Couch, a: &Args, created: Created) -> Value {
    let mut out = BTreeMap::new();
    for (label, size, ops, conc) in CLASSES {
        let n = class_ops(ops, a.ops_scale);
        let (c2, db, seed) = (c.clone(), a.db.clone(), a.seed);
        let made: Arc<std::sync::Mutex<Vec<(String, String)>>> = Arc::default();
        let made2 = made.clone();
        let (lat, bytes, secs, errs) = run_ops(n, conc, move |i| {
            let (c, db, made) = (c2.clone(), db.clone(), made2.clone());
            async move {
                let spec = DocSpec { idx: 1_000_000_000 + (size.ilog10() as u64) * 10_000_000 + i as u64, band: 99, size };
                let id = format!("put-{label}-{i:06}");
                let resp = if size <= JSON_MAX {
                    c.req(reqwest::Method::PUT, &format!("/{db}/{id}"))
                        .header("Content-Type", "application/json")
                        .body(json_doc(&spec, seed, false, size))
                        .send()
                        .await
                } else {
                    let (ct, len, body) = multipart_doc(&spec, seed);
                    c.req(reqwest::Method::PUT, &format!("/{db}/{id}"))
                        .header("Content-Type", ct)
                        .header("Content-Length", len)
                        .body(body)
                        .send()
                        .await
                }
                .map_err(|e| e.to_string())?;
                let st = resp.status();
                let v: Value = resp.json().await.unwrap_or(Value::Null);
                if !st.is_success() {
                    return Err(format!("{st} {v}"));
                }
                made.lock().unwrap().push((id, v["rev"].as_str().unwrap_or("").to_string()));
                Ok(size)
            }
        })
        .await;
        let mut s = lat.summary(secs, bytes);
        s["errors"] = json!(errs);
        s["concurrency"] = json!(conc);
        eprintln!("  put {label:>6}: {:>9.1} ops/s {:>8.1} MB/s mean {:.1}ms p50 {:.1}ms p90 {:.1}ms p99 {:.1}ms", s["ops_per_sec"].as_f64().unwrap(), s["mb_per_sec"].as_f64().unwrap(), s["mean_ms"].as_f64().unwrap(), s["p50_ms"].as_f64().unwrap(), s["p90_ms"].as_f64().unwrap(), s["p99_ms"].as_f64().unwrap());
        created.lock().await.insert(label.to_string(), std::mem::take(&mut *made.lock().unwrap()));
        out.insert(label.to_string(), s);
    }
    json!(out)
}

async fn phase_get(c: &Couch, a: &Args, plan: &[DocSpec]) -> Value {
    let mut out = BTreeMap::new();
    for (label, size, ops, conc) in CLASSES {
        // Random docs from the loaded dataset in this class's size band:
        // with ~100 GB loaded and less RAM than that, many reads hit disk.
        let band = size.ilog10();
        let pool: Vec<DocSpec> = plan.iter().filter(|s| s.band == band).copied().collect();
        if pool.is_empty() {
            continue;
        }
        let n = class_ops(ops, a.ops_scale);
        let mut rng = Rng::new(a.seed ^ 0x6E7 ^ size);
        let picks: Arc<Vec<DocSpec>> = Arc::new((0..n).map(|_| pool[rng.below(pool.len() as u64) as usize]).collect());
        let avg = picks.iter().map(|s| s.size).sum::<u64>() / n as u64;
        let (c2, db, seed) = (c.clone(), a.db.clone(), a.seed);
        let (lat, bytes, secs, errs) = run_ops(n, conc, move |i| {
            let (c, db, spec) = (c2.clone(), db.clone(), picks[i]);
            async move {
                let path = if spec.size <= JSON_MAX {
                    format!("/{db}/{}", spec.id(seed))
                } else {
                    format!("/{db}/{}/blob", spec.id(seed))
                };
                let resp = c.req(reqwest::Method::GET, &path).send().await.map_err(|e| e.to_string())?;
                if !resp.status().is_success() {
                    return Err(format!("{} {}", resp.status(), path));
                }
                let mut stream = resp.bytes_stream();
                let mut got = 0u64;
                while let Some(chunk) = stream.next().await {
                    got += chunk.map_err(|e| e.to_string())?.len() as u64;
                }
                Ok(got)
            }
        })
        .await;
        let mut s = lat.summary(secs, bytes);
        s["errors"] = json!(errs);
        s["concurrency"] = json!(conc);
        s["avg_doc_bytes"] = json!(avg);
        s["band"] = json!(format!("1e{band}"));
        eprintln!("  get {label:>6}: {:>9.1} ops/s {:>8.1} MB/s mean {:.1}ms p50 {:.1}ms p90 {:.1}ms p99 {:.1}ms (avg {} B)", s["ops_per_sec"].as_f64().unwrap(), s["mb_per_sec"].as_f64().unwrap(), s["mean_ms"].as_f64().unwrap(), s["p50_ms"].as_f64().unwrap(), s["p90_ms"].as_f64().unwrap(), s["p99_ms"].as_f64().unwrap(), avg);
        out.insert(label.to_string(), s);
    }
    json!(out)
}

async fn phase_delete(c: &Couch, a: &Args, created: Created) -> Value {
    let mut out = BTreeMap::new();
    let created = created.lock().await.clone();
    for (label, _, _, conc) in CLASSES {
        let Some(docs) = created.get(label) else { continue };
        if docs.is_empty() {
            continue;
        }
        let docs = Arc::new(docs.clone());
        let n = docs.len();
        let (c2, db) = (c.clone(), a.db.clone());
        let (lat, bytes, secs, errs) = run_ops(n, conc.max(16), move |i| {
            let (c, db, docs) = (c2.clone(), db.clone(), docs.clone());
            async move {
                let (id, rev) = &docs[i];
                let r = c
                    .req(reqwest::Method::DELETE, &format!("/{db}/{id}?rev={rev}"))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !r.status().is_success() {
                    return Err(format!("{}", r.status()));
                }
                let _ = r.bytes().await;
                Ok(0)
            }
        })
        .await;
        let mut s = lat.summary(secs, bytes);
        s["errors"] = json!(errs);
        eprintln!("  delete {label:>6}: {:>9.1} ops/s mean {:.1}ms p50 {:.1}ms p90 {:.1}ms p99 {:.1}ms", s["ops_per_sec"].as_f64().unwrap(), s["mean_ms"].as_f64().unwrap(), s["p50_ms"].as_f64().unwrap(), s["p90_ms"].as_f64().unwrap(), s["p99_ms"].as_f64().unwrap());
        out.insert(label.to_string(), s);
    }
    json!(out)
}

// ----------------------------------------------------------------- views

async fn phase_view(c: &Couch, a: &Args, plan: &[DocSpec]) -> Value {
    let db = &a.db;
    let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
    let ddoc = json!({
        "views": {
            "by_group": {
                // The run nonce changes the index signature, so every run
                // builds a brand-new index instead of reusing an old one.
                "map": format!("function(doc) {{ /* run {nonce} */ if (doc.group !== undefined) {{ emit([doc.band, doc.group], doc.size); }} }}"),
                "reduce": "_sum"
            }
        }
    });
    let (_, old) = c.json(reqwest::Method::GET, &format!("/{db}/_design/bench"), None).await.unwrap();
    let mut ddoc = ddoc;
    if let Some(r) = old.get("_rev") {
        ddoc["_rev"] = r.clone();
    }
    let (st, v) = c.json(reqwest::Method::PUT, &format!("/{db}/_design/bench"), Some(ddoc)).await.unwrap();
    assert!(st == 201 || st == 202, "ddoc: {st} {v}");
    let info = c.json(reqwest::Method::GET, &format!("/{db}"), None).await.unwrap().1;
    let docs_in_db = info["doc_count"].as_u64().unwrap_or(0);

    eprintln!("  view: building index over {docs_in_db} docs");
    // CouchDB answers a view request that outlives its request timeout
    // with 500 {"error":"timeout"} but keeps indexing, so keep asking until
    // the index answers; build time is until the first successful reply.
    let t0 = Instant::now();
    let mut timeouts = 0;
    let body: Value = loop {
        let r = c
            .req(reqwest::Method::GET, &format!("/{db}/_design/bench/_view/by_group?limit=1&reduce=false"))
            .timeout(Duration::from_secs(24 * 3600))
            .send()
            .await;
        match r {
            Ok(r) if r.status().is_success() => break r.json().await.unwrap_or(Value::Null),
            Ok(r) => {
                let st = r.status();
                let v: Value = r.json().await.unwrap_or(Value::Null);
                assert!(v["error"] == "timeout" && timeouts < 10_000, "view build failed: {st} {v}");
                timeouts += 1;
            }
            Err(e) => {
                assert!(timeouts < 10_000, "view build failed: {e}");
                timeouts += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    let build_secs = t0.elapsed().as_secs_f64();
    let rows = body["total_rows"].as_u64().unwrap_or(0);
    // Remove index files left by earlier runs' design docs.
    let _ = c.json(reqwest::Method::POST, &format!("/{db}/_view_cleanup"), Some(json!({}))).await;
    let vinfo = c.json(reqwest::Method::GET, &format!("/{db}/_design/bench/_info"), None).await.unwrap().1;
    eprintln!("  view: built {rows} rows in {build_secs:.1}s ({:.0} docs/s)", docs_in_db as f64 / build_secs);

    // Verify the total reduction matches the dataset.
    let (_, red) = c.json(reqwest::Method::GET, &format!("/{db}/_design/bench/_view/by_group"), None).await.unwrap();
    let expected: u64 = plan.iter().map(|s| s.size).sum();

    let mut queries = BTreeMap::new();
    let bands: Vec<u32> = BANDS.iter().map(|b| b.0).collect();
    let cases: [(&str, usize, usize); 4] = [
        ("key_lookup_limit20", 5000, 16),
        ("key_reduce", 3000, 16),
        ("group_level1_reduce", 500, 8),
        ("include_docs_limit10", 2000, 16),
    ];
    for (name, ops, conc) in cases {
        let n = class_ops(ops, a.ops_scale);
        let (c2, db2, bands2, seed) = (c.clone(), db.clone(), bands.clone(), a.seed);
        let (lat, bytes, secs, errs) = run_ops(n, conc, move |i| {
            let (c, db) = (c2.clone(), db2.clone());
            let mut rng = Rng::new(seed ^ i as u64 ^ 0x51E);
            // include_docs only on small-doc bands so it measures lookups,
            // not bulk attachment-stub transfer.
            let band = if name == "include_docs_limit10" { 2 + rng.below(3) as u32 } else { bands2[rng.below(bands2.len() as u64) as usize] };
            let group = rng.below(100);
            let key = format!("[{band},{group}]");
            let q = match name {
                "key_lookup_limit20" => format!("key={key}&reduce=false&limit=20"),
                "key_reduce" => format!("key={key}"),
                "group_level1_reduce" => "group_level=1".to_string(),
                _ => format!("key={key}&reduce=false&limit=10&include_docs=true"),
            };
            async move {
                let q = q.replace('[', "%5B").replace(']', "%5D").replace(',', "%2C");
                let r = c
                    .req(reqwest::Method::GET, &format!("/{db}/_design/bench/_view/by_group?{q}&update=false"))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !r.status().is_success() {
                    return Err(format!("{}", r.status()));
                }
                Ok(r.bytes().await.map_err(|e| e.to_string())?.len() as u64)
            }
        })
        .await;
        let mut s = lat.summary(secs, bytes);
        s["errors"] = json!(errs);
        eprintln!("  view {name:>22}: {:>9.1} q/s mean {:.1}ms p50 {:.1}ms p90 {:.1}ms p99 {:.1}ms", s["ops_per_sec"].as_f64().unwrap(), s["mean_ms"].as_f64().unwrap(), s["p50_ms"].as_f64().unwrap(), s["p90_ms"].as_f64().unwrap(), s["p99_ms"].as_f64().unwrap());
        queries.insert(name.to_string(), s);
    }
    json!({
        "build_secs": build_secs,
        "docs_indexed": docs_in_db,
        "rows": rows,
        "docs_per_sec": docs_in_db as f64 / build_secs,
        "request_timeouts_during_build": timeouts,
        "index_file_bytes": vinfo["view_index"]["sizes"]["file"],
        "reduce_total": red["rows"][0]["value"],
        "reduce_expected": expected,
        "queries": queries,
    })
}

// ----------------------------------------------------------- replication

async fn phase_replicate(c: &Couch, a: &Args) -> Value {
    let rep = Couch::new(&a.replicator);
    let target = format!("{}_repl", a.db);
    let _ = c.json(reqwest::Method::DELETE, &format!("/{target}"), None).await;
    let info = c.json(reqwest::Method::GET, &format!("/{}", a.db), None).await.unwrap().1;
    let src_docs = info["doc_count"].as_u64().unwrap_or(0);
    let body = json!({
        "source": format!("{}/{}", c.full, a.db),
        "target": format!("{}/{}", c.full, target),
        "create_target": true,
    });
    eprintln!("  replicate: {} -> {} via {} ({} docs)", a.db, target, rep.base, src_docs);
    let t0 = Instant::now();
    let resp = rep
        .req(reqwest::Method::POST, "/_replicate")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .timeout(Duration::from_secs(48 * 3600))
        .send()
        .await
        .unwrap();
    let st = resp.status();
    let v: Value = resp.json().await.unwrap_or(Value::Null);
    let secs = t0.elapsed().as_secs_f64();
    let tinfo = c.json(reqwest::Method::GET, &format!("/{target}"), None).await.unwrap().1;
    let tgt_docs = tinfo["doc_count"].as_u64().unwrap_or(0);
    let hist = v["history"].get(0).cloned().unwrap_or(Value::Null);
    let src_bytes = info["sizes"]["external"].as_u64().unwrap_or(0);
    eprintln!("  replicate: {st} in {secs:.1}s, {tgt_docs}/{src_docs} docs");
    if !a.keep_replica {
        let _ = c.json(reqwest::Method::DELETE, &format!("/{target}"), None).await;
    }
    json!({
        "ok": st.is_success() && v["ok"] == json!(true),
        "secs": secs,
        "source_docs": src_docs,
        "target_docs": tgt_docs,
        "docs_per_sec": tgt_docs as f64 / secs,
        "source_external_bytes": src_bytes,
        "mb_per_sec": mb(src_bytes) / secs,
        "docs_written": hist["docs_written"],
        "doc_write_failures": hist["doc_write_failures"],
        "error": v.get("error"),
        "reason": v.get("reason"),
    })
}

// ------------------------------------------------------------ compaction

async fn db_sizes(c: &Couch, db: &str) -> (u64, u64, bool) {
    let v = c.json(reqwest::Method::GET, &format!("/{db}"), None).await.unwrap().1;
    (
        v["sizes"]["file"].as_u64().unwrap_or(0),
        v["sizes"]["active"].as_u64().unwrap_or(0),
        v["compact_running"].as_bool().unwrap_or(false),
    )
}

async fn phase_compact(c: &Couch, a: &Args) -> Value {
    let db = &a.db;
    let (file0, active0, _) = db_sizes(c, db).await;
    eprintln!("  compact: {:.2} GB file, {:.2} GB live", file0 as f64 / 1e9, active0 as f64 / 1e9);
    let t0 = Instant::now();
    let (st, v) = c.json(reqwest::Method::POST, &format!("/{db}/_compact"), Some(json!({}))).await.unwrap();
    assert!(st == 202, "compact request failed: {st} {v}");
    // Wait until compaction has been seen running and then stopped (or never
    // showed as running within a few seconds, for a very fast compaction).
    let mut seen = false;
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (_, _, running) = db_sizes(c, db).await;
        seen |= running;
        if !running && (seen || t0.elapsed() > Duration::from_secs(5)) {
            break;
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let (file1, active1, _) = db_sizes(c, db).await;
    eprintln!(
        "  compact: {secs:.1}s -> {:.2} GB file, {:.2} GB live ({:.0} MB/s of live data)",
        file1 as f64 / 1e9, active1 as f64 / 1e9, mb(active1) / secs
    );
    json!({
        "secs": secs,
        "file_bytes_before": file0,
        "active_bytes_before": active0,
        "file_bytes_after": file1,
        "active_bytes_after": active1,
        "reclaimed_bytes": file0.saturating_sub(file1),
        "dead_bytes_after": file1.saturating_sub(active1),
        "live_mb_per_sec": mb(active1) / secs,
    })
}

/// Bring the bench view up to date, retrying CouchDB's request timeouts.
async fn refresh_view(c: &Couch, db: &str) -> f64 {
    let t0 = Instant::now();
    for _ in 0..10_000 {
        let r = c
            .req(reqwest::Method::GET, &format!("/{db}/_design/bench/_view/by_group?limit=1&reduce=false"))
            .timeout(Duration::from_secs(24 * 3600))
            .send()
            .await;
        if let Ok(r) = r && r.status().is_success() {
            return t0.elapsed().as_secs_f64();
        }
    }
    panic!("view refresh failed");
}

async fn view_sizes(c: &Couch, db: &str) -> (u64, u64, bool) {
    let v = c.json(reqwest::Method::GET, &format!("/{db}/_design/bench/_info"), None).await.unwrap().1;
    let vi = &v["view_index"];
    (
        vi["sizes"]["file"].as_u64().unwrap_or(0),
        vi["sizes"]["active"].as_u64().unwrap_or(0),
        vi["compact_running"].as_bool().unwrap_or(false),
    )
}

const CHURN_DOCS: usize = 200_000;
const CHURN_ROUNDS: usize = 3;

/// View compaction after incremental churn: insert CHURN_DOCS small docs,
/// update each CHURN_ROUNDS times and then delete them, refreshing the
/// index after every round, so the index ends with the rows it started
/// with plus the dead space incremental updates leave. Then compact it.
async fn phase_view_compact(c: &Couch, a: &Args) -> Value {
    let db = &a.db;
    refresh_view(c, db).await;
    let (file_start, active_start, _) = view_sizes(c, db).await;
    let mut revs: Vec<Option<String>> = vec![None; CHURN_DOCS];
    let mut rounds = Vec::new();
    for round in 0..=CHURN_ROUNDS + 1 {
        let delete = round == CHURN_ROUNDS + 1;
        let t0 = Instant::now();
        for start in (0..CHURN_DOCS).step_by(10_000) {
            let docs: Vec<Value> = (start..(start + 10_000).min(CHURN_DOCS))
                .map(|i| {
                    let mut d = json!({"_id": format!("vc-{i:07}"), "band": 99, "group": (i + round) % 100, "size": round});
                    if let Some(r) = &revs[i] {
                        d["_rev"] = json!(r);
                    }
                    if delete {
                        d["_deleted"] = json!(true);
                    }
                    d
                })
                .collect();
            let (st, v) = c.json(reqwest::Method::POST, &format!("/{db}/_bulk_docs"), Some(json!({"docs": docs}))).await.unwrap();
            assert!(st == 201, "churn bulk_docs: {st}");
            for (k, r) in v.as_array().unwrap().iter().enumerate() {
                revs[start + k] = r["rev"].as_str().map(str::to_string);
            }
        }
        let write_secs = t0.elapsed().as_secs_f64();
        let index_secs = refresh_view(c, db).await;
        let (file, active, _) = view_sizes(c, db).await;
        eprintln!(
            "  view_compact churn {round}: write {write_secs:.1}s, index update {index_secs:.1}s ({:.0} docs/s), file {:.2} GB",
            CHURN_DOCS as f64 / index_secs, file as f64 / 1e9
        );
        rounds.push(json!({"write_secs": write_secs, "index_update_secs": index_secs,
            "index_docs_per_sec": CHURN_DOCS as f64 / index_secs, "file_bytes": file, "active_bytes": active}));
    }
    let (file0, active0, _) = view_sizes(c, db).await;
    eprintln!("  view_compact: {:.2} GB file, {:.2} GB live", file0 as f64 / 1e9, active0 as f64 / 1e9);
    let unix = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();
    let started_unix = unix();
    let t0 = Instant::now();
    let (st, v) = c.json(reqwest::Method::POST, &format!("/{db}/_compact/bench"), Some(json!({}))).await.unwrap();
    assert!(st == 202, "view compact request failed: {st} {v}");
    // Done once compaction has been seen running and then stopped, or once
    // the file has been swapped: main's B-tree build never reports
    // compact_running.
    let mut seen = false;
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let (file, _, running) = view_sizes(c, db).await;
        seen |= running;
        if !running && (seen || file != file0 || t0.elapsed() > Duration::from_secs(3600)) {
            break;
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let (file1, active1, _) = view_sizes(c, db).await;
    eprintln!(
        "  view_compact: {secs:.1}s -> {:.2} GB file, {:.2} GB live",
        file1 as f64 / 1e9, active1 as f64 / 1e9
    );
    json!({
        "started_unix": started_unix,
        "ended_unix": unix(),
        "churn_docs": CHURN_DOCS,
        "churn_rounds": rounds,
        "file_bytes_start": file_start,
        "active_bytes_start": active_start,
        "secs": secs,
        "file_bytes_before": file0,
        "active_bytes_before": active0,
        "file_bytes_after": file1,
        "active_bytes_after": active1,
        "reclaimed_bytes": file0.saturating_sub(file1),
        "live_mb_per_sec": mb(active1) / secs,
    })
}

// ------------------------------------------------------------------ main

fn save(out: &str, name: &str, key: &str, val: Value) {
    let mut all: Value = std::fs::read(out)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(json!({}));
    if !all[name].is_object() {
        all[name] = json!({});
    }
    all[name][key] = val;
    std::fs::write(out, serde_json::to_vec_pretty(&all).unwrap()).unwrap();
}

#[tokio::main]
async fn main() {
    let a = parse_args();
    let c = Couch::new(&a.url);
    let (_, welcome) = c.json(reqwest::Method::GET, "/", None).await.unwrap_or_else(|e| panic!("server unreachable at {}: {e}", c.base));
    let total = a.total_gb * 1e9;
    let plan = plan(total, a.seed);
    let total_bytes: u64 = plan.iter().map(|s| s.size).sum();
    eprintln!(
        "{}: {} {} | dataset {:.2} GB, {} docs (seed {})",
        a.name, welcome["vendor"]["name"], welcome["version"], total_bytes as f64 / 1e9, plan.len(), a.seed
    );
    save(&a.out, &a.name, "server", json!({
        "url": c.base, "welcome": welcome, "dataset_bytes": total_bytes, "dataset_docs": plan.len(),
        "seed": a.seed, "bands": BANDS.iter().map(|(b, s)| json!({"band": b, "share": s})).collect::<Vec<_>>(),
        "json_max_bytes": JSON_MAX,
    }));
    let created: Created = Arc::default();
    for phase in a.phases.clone() {
        eprintln!("== {} / {phase}", a.name);
        let t = Instant::now();
        let v = match phase.as_str() {
            "load" => phase_load(&c, &a, &plan).await,
            "view" => phase_view(&c, &a, &plan).await,
            "get" => phase_get(&c, &a, &plan).await,
            "put" => phase_put(&c, &a, created.clone()).await,
            "delete" => phase_delete(&c, &a, created.clone()).await,
            "replicate" => phase_replicate(&c, &a).await,
            "compact" => phase_compact(&c, &a).await,
            "view_compact" => phase_view_compact(&c, &a).await,
            other => {
                eprintln!("unknown phase {other}");
                continue;
            }
        };
        eprintln!("   ({phase} took {:.1}s)", t.elapsed().as_secs_f64());
        save(&a.out, &a.name, &phase, v);
    }
}
