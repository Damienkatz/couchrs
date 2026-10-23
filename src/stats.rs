//! Metrics, a port of couch_stats and couch_prometheus. The metrics are
//! CouchDB's own, read from share/stats_descriptions.cfg (every CouchDB
//! application's priv/stats_descriptions.cfg): counters, gauges, and
//! histograms that keep a set of bins per second, so a read covers the
//! last `[stats] interval` seconds. Metrics for machinery CouchRS
//! doesn't have (the cluster's fabric, mem3 and rexi, the design doc and
//! user caches, clouseau) are kept for monitoring tools and stay at zero.
//!
//! Code names a metric with `stat!("couchdb.httpd.requests")`, which
//! looks it up once per call site.

use crate::config::Config;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

const DESCRIPTIONS: &str = include_str!("../share/stats_descriptions.cfg");
const DEFAULT_INTERVAL_SEC: i64 = 10;
const HIST_WRAP_BUFFER_SIZE_SEC: i64 = 5;

/// A metric by its dotted name, looked up once per call site.
#[macro_export]
macro_rules! stat {
    ($name:literal) => {{
        static M: std::sync::OnceLock<&'static $crate::stats::Metric> = std::sync::OnceLock::new();
        *M.get_or_init(|| $crate::stats::metric($name))
    }};
}

/// One element of a metric's path. Paths sort in Erlang term order, which
/// puts integers (status codes) before atoms.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Elem {
    Int(i64),
    Atom(String),
}

impl Elem {
    fn text(&self) -> String {
        match self {
            Elem::Int(i) => i.to_string(),
            Elem::Atom(a) => a.clone(),
        }
    }
}

pub struct Metric {
    pub path: Vec<Elem>,
    pub desc: String,
    kind: Kind,
}

enum Kind {
    Counter(AtomicI64),
    Gauge(AtomicI64),
    Histogram(Histogram),
}

impl Metric {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn dec(&self) {
        self.add(-1);
    }

    /// Add to a counter.
    pub fn add(&self, n: i64) {
        match &self.kind {
            Kind::Counter(c) => {
                c.fetch_add(n, Relaxed);
            }
            _ => invalid(self, "counter"),
        }
    }

    /// Set a gauge.
    pub fn set(&self, v: i64) {
        match &self.kind {
            Kind::Gauge(g) => g.store(v, Relaxed),
            _ => invalid(self, "gauge"),
        }
    }

    /// Move a gauge that counts live things up or down.
    pub fn adjust(&self, delta: i64) {
        match &self.kind {
            Kind::Gauge(g) => {
                g.fetch_add(delta, Relaxed);
            }
            _ => invalid(self, "gauge"),
        }
    }

    /// Record a value (milliseconds, for times) in a histogram.
    pub fn observe(&self, v: f64) {
        match &self.kind {
            Kind::Histogram(h) => h.update(now_sec(), v),
            _ => invalid(self, "histogram"),
        }
    }

    /// Record how long `f` takes, in milliseconds.
    pub fn time<T>(&self, f: impl FnOnce() -> T) -> T {
        let t = Instant::now();
        let r = f();
        self.observe(ms_since(t));
        r
    }

    /// A counter's or gauge's value.
    pub fn get(&self) -> i64 {
        match &self.kind {
            Kind::Counter(c) | Kind::Gauge(c) => c.load(Relaxed),
            Kind::Histogram(_) => 0,
        }
    }

    fn type_name(&self) -> &'static str {
        match self.kind {
            Kind::Counter(_) => "counter",
            Kind::Gauge(_) => "gauge",
            Kind::Histogram(_) => "histogram",
        }
    }

    /// The value now; a histogram's covers `interval` seconds from `start`.
    fn value(&self, start: i64, interval: i64) -> Value {
        match &self.kind {
            Kind::Counter(c) | Kind::Gauge(c) => json!(c.load(Relaxed)),
            Kind::Histogram(h) => summary(&h.read(start, interval)),
        }
    }

    fn name(&self) -> String {
        self.path.iter().map(Elem::text).collect::<Vec<_>>().join(".")
    }
}

fn invalid(m: &Metric, want: &str) {
    tracing::error!("invalid metric: {} is a {}, not a {want}", m.name(), m.type_name());
    debug_assert!(false, "invalid metric {}", m.name());
}

pub fn ms_since(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

// -------------------------------------------------------------- registry

struct Registry {
    /// Sorted by path.
    all: Vec<&'static Metric>,
    by_name: HashMap<String, &'static Metric>,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();
static CONFIG: OnceLock<Arc<Config>> = OnceLock::new();
static EPOCH: OnceLock<Instant> = OnceLock::new();

/// The server's configuration, once the stats are loaded (for code that
/// has no server to ask).
pub fn config() -> Option<&'static Arc<Config>> {
    CONFIG.get()
}

/// Load the metrics with this config's `[stats] interval` and start the
/// thread that clears stale histogram bins.
pub fn init(config: Arc<Config>) {
    let _ = CONFIG.set(config);
    let _ = registry();
    static CLEANER: OnceLock<()> = OnceLock::new();
    CLEANER.get_or_init(|| {
        std::thread::Builder::new()
            .name("stats-cleaner".into())
            .spawn(|| {
                loop {
                    // Twice per interval, clear the most stale interval.
                    std::thread::sleep(Duration::from_millis((interval() * 1000 / 2).max(500) as u64));
                    clean();
                }
            })
            .expect("stats thread");
    });
}

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(|| {
        let size = (interval() * 2 + HIST_WRAP_BUFFER_SIZE_SEC * 2) as usize;
        let mut all: Vec<&'static Metric> = parse_descriptions(DESCRIPTIONS)
            .into_iter()
            .map(|(path, ty, desc)| {
                let kind = match ty.as_str() {
                    "gauge" => Kind::Gauge(AtomicI64::new(0)),
                    "histogram" => Kind::Histogram(Histogram::new(size)),
                    _ => Kind::Counter(AtomicI64::new(0)),
                };
                &*Box::leak(Box::new(Metric { path, desc, kind }))
            })
            .collect();
        all.sort_by(|a, b| a.path.cmp(&b.path));
        let by_name = all.iter().map(|m| (m.name(), *m)).collect();
        Registry { all, by_name }
    })
}

/// The `[stats] interval`, seconds.
pub fn interval() -> i64 {
    CONFIG
        .get()
        .and_then(|c| c.get("stats", "interval"))
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|&v| v >= 1)
        .unwrap_or(DEFAULT_INTERVAL_SEC)
}

fn now_sec() -> i64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_secs() as i64
}

/// A metric by dotted name. An unknown name is logged, as couch_stats
/// does, and counts into a metric nothing reads.
pub fn metric(name: &str) -> &'static Metric {
    match registry().by_name.get(name) {
        Some(m) => m,
        None => {
            tracing::error!("unknown metric: {name}");
            debug_assert!(false, "unknown metric {name}");
            static NOWHERE: OnceLock<Metric> = OnceLock::new();
            NOWHERE.get_or_init(|| Metric { path: vec![], desc: String::new(), kind: Kind::Counter(AtomicI64::new(0)) })
        }
    }
}

/// A metric whose name is only known at run time (a status code, say).
pub fn find(name: &str) -> Option<&'static Metric> {
    registry().by_name.get(name).copied()
}

fn clean() {
    let now = now_sec();
    let iv = interval();
    // The timeline is |buffer|stale|buffer|current|, current ending now.
    let start = now - iv - HIST_WRAP_BUFFER_SIZE_SEC - (iv - 1);
    for m in &registry().all {
        if let Kind::Histogram(h) = &m.kind {
            h.clear(start, iv);
        }
    }
}

/// Every metric as `{value, type, desc}`, nested by path (the _stats
/// response).
pub fn fetch_json() -> Value {
    let (now, iv) = (now_sec(), interval());
    let start = now - (iv - 1);
    let mut root = Map::new();
    for m in &registry().all {
        let mut node = &mut root;
        let (last, init) = m.path.split_last().expect("metric path");
        for e in init {
            node = node
                .entry(e.text())
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
                .expect("metric group");
        }
        node.insert(last.text(), json!({"value": m.value(start, iv), "type": m.type_name(), "desc": m.desc}));
    }
    Value::Object(root)
}

/// The part of `v` at `path`, or null (couch_stats_httpd:extract_path).
pub fn extract_path(v: Value, path: &[&str]) -> Value {
    let mut cur = v;
    for p in path {
        match cur {
            Value::Object(mut o) => cur = o.remove(*p).unwrap_or(Value::Null),
            _ => return Value::Null,
        }
    }
    cur
}

// ----------------------------------------------------- descriptions file

#[derive(Debug, Clone)]
enum Term {
    Tuple(Vec<Term>),
    List(Vec<Term>),
    Atom(String),
    Int(i64),
    Bin(String),
}

/// (path, type, desc) for each `{[path...], [{type, T}, {desc, <<"D">>}]}.`
/// in an Erlang terms file.
fn parse_descriptions(src: &str) -> Vec<(Vec<Elem>, String, String)> {
    let mut p = TermParser { s: src.as_bytes(), i: 0 };
    let mut out = Vec::new();
    loop {
        p.skip_ws();
        if p.i >= p.s.len() {
            break;
        }
        let t = p.term().expect("stats_descriptions.cfg term");
        p.expect(b'.');
        let Term::Tuple(parts) = t else { panic!("stats_descriptions.cfg: expected a tuple") };
        let (Some(Term::List(path)), Some(Term::List(props))) = (parts.first(), parts.get(1)) else {
            panic!("stats_descriptions.cfg: expected {{Path, Props}}")
        };
        let path = path
            .iter()
            .map(|e| match e {
                Term::Atom(a) => Elem::Atom(a.clone()),
                Term::Int(i) => Elem::Int(*i),
                other => panic!("stats_descriptions.cfg: bad path element {other:?}"),
            })
            .collect();
        let (mut ty, mut desc) = ("counter".to_string(), String::new());
        for prop in props {
            if let Term::Tuple(kv) = prop {
                match (kv.first(), kv.get(1)) {
                    (Some(Term::Atom(k)), Some(Term::Atom(v))) if k == "type" => ty = v.clone(),
                    (Some(Term::Atom(k)), Some(Term::Bin(v))) if k == "desc" => desc = v.clone(),
                    _ => {}
                }
            }
        }
        out.push((path, ty, desc));
    }
    out
}

struct TermParser<'a> {
    s: &'a [u8],
    i: usize,
}

impl TermParser<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.s.len() {
            match self.s[self.i] {
                b'%' => {
                    while self.i < self.s.len() && self.s[self.i] != b'\n' {
                        self.i += 1;
                    }
                }
                c if c.is_ascii_whitespace() => self.i += 1,
                _ => break,
            }
        }
    }

    fn expect(&mut self, c: u8) {
        self.skip_ws();
        assert_eq!(self.s.get(self.i), Some(&c), "stats_descriptions.cfg: expected '{}' at {}", c as char, self.i);
        self.i += 1;
    }

    fn seq(&mut self, close: u8) -> Option<Vec<Term>> {
        let mut items = Vec::new();
        self.skip_ws();
        if self.s.get(self.i) == Some(&close) {
            self.i += 1;
            return Some(items);
        }
        loop {
            items.push(self.term()?);
            self.skip_ws();
            match self.s.get(self.i)? {
                b',' => self.i += 1,
                c if *c == close => {
                    self.i += 1;
                    return Some(items);
                }
                _ => return None,
            }
        }
    }

    fn term(&mut self) -> Option<Term> {
        self.skip_ws();
        let c = *self.s.get(self.i)?;
        match c {
            b'{' => {
                self.i += 1;
                self.seq(b'}').map(Term::Tuple)
            }
            b'[' => {
                self.i += 1;
                self.seq(b']').map(Term::List)
            }
            b'<' => {
                // <<"text">>
                self.i += 3;
                let start = self.i;
                while *self.s.get(self.i)? != b'"' {
                    self.i += 1;
                }
                let text = String::from_utf8_lossy(&self.s[start..self.i]).to_string();
                self.i += 3;
                Some(Term::Bin(text))
            }
            b'\'' => {
                self.i += 1;
                let start = self.i;
                while *self.s.get(self.i)? != b'\'' {
                    self.i += 1;
                }
                let a = String::from_utf8_lossy(&self.s[start..self.i]).to_string();
                self.i += 1;
                Some(Term::Atom(a))
            }
            c if c.is_ascii_digit() => {
                let start = self.i;
                while self.s.get(self.i).is_some_and(|c| c.is_ascii_digit()) {
                    self.i += 1;
                }
                std::str::from_utf8(&self.s[start..self.i]).ok()?.parse().ok().map(Term::Int)
            }
            c if c.is_ascii_lowercase() => {
                let start = self.i;
                while self.s.get(self.i).is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'@') {
                    self.i += 1;
                }
                Some(Term::Atom(String::from_utf8_lossy(&self.s[start..self.i]).to_string()))
            }
            _ => None,
        }
    }
}

// ------------------------------------------------------------ histograms
//
// couch_stats_histogram: a value's bin is its float's exponent plus the
// top few mantissa bits, so bins are about 6% wide at every magnitude
// from 0.01 to 4,000,000, and each second of the window has its own bins.

const INDEX_BITS: u32 = 14;
const BIN_BITS: u32 = 64 - 1 - INDEX_BITS;
const MIN_VAL: f64 = 0.01;
const MAX_VAL: f64 = 4000000.0;
const BIN_OFFSET: i64 = -8129;
const BIN_COUNT: usize = 230;

type Bins = [u64; BIN_COUNT];

struct Histogram {
    slots: Vec<Box<[AtomicU64; BIN_COUNT]>>,
}

/// A value's bin, from 1 as in CouchDB.
fn bin_index(v: f64) -> usize {
    (((v.to_bits() >> BIN_BITS) & ((1 << INDEX_BITS) - 1)) as i64 + BIN_OFFSET) as usize
}

fn bin_bits(index: usize, low: u64) -> f64 {
    let biased = (index as i64 - BIN_OFFSET) as u64;
    f64::from_bits((biased << BIN_BITS) | low)
}

fn bin_min(index: usize) -> f64 {
    bin_bits(index, 0)
}

fn bin_max(index: usize) -> f64 {
    bin_bits(index, (1 << BIN_BITS) - 1)
}

fn bin_middle(index: usize) -> f64 {
    bin_bits(index, 1 << (BIN_BITS - 1))
}

impl Histogram {
    fn new(size: usize) -> Histogram {
        Histogram { slots: (0..size.max(1)).map(|_| Box::new(std::array::from_fn(|_| AtomicU64::new(0)))).collect() }
    }

    fn at(&self, time: i64) -> &[AtomicU64; BIN_COUNT] {
        &self.slots[time.rem_euclid(self.slots.len() as i64) as usize]
    }

    fn update(&self, time: i64, v: f64) {
        if v.is_nan() {
            return;
        }
        let v = v.clamp(MIN_VAL, MAX_VAL);
        self.at(time)[bin_index(v) - 1].fetch_add(1, Relaxed);
    }

    /// The bins of `ticks` seconds from `time`.
    fn read(&self, time: i64, ticks: i64) -> Bins {
        let mut out = [0u64; BIN_COUNT];
        for t in time..time + ticks.min(self.slots.len() as i64) {
            for (o, c) in out.iter_mut().zip(self.at(t).iter()) {
                *o += c.load(Relaxed);
            }
        }
        out
    }

    fn clear(&self, time: i64, ticks: i64) {
        for t in time..time + ticks.min(self.slots.len() as i64) {
            for c in self.at(t).iter() {
                c.store(0, Relaxed);
            }
        }
    }
}

/// couch_stats_math:summary/2.
fn summary(bins: &Bins) -> Value {
    let mut nonzero = Vec::new();
    let (mut n, mut sum, mut sum_log, mut sum_inv) = (0u64, 0f64, 0f64, 0f64);
    for (i, &count) in bins.iter().enumerate() {
        if count > 0 {
            let index = i + 1;
            let val = bin_middle(index);
            nonzero.push((index, count));
            n += count;
            sum += count as f64 * val;
            sum_log += count as f64 * val.ln();
            sum_inv += count as f64 / val;
        }
    }
    if n == 0 {
        return json!({
            "n": 0, "min": 0, "max": 0, "arithmetic_mean": 0, "geometric_mean": 0,
            "harmonic_mean": 0, "median": 0, "variance": 0, "standard_deviation": 0,
            "skewness": 0, "kurtosis": 0,
            "percentile": [[50, 0], [75, 0], [90, 0], [95, 0], [99, 0], [999, 0]],
            "histogram": [[0, 0]]
        });
    }
    let nf = n as f64;
    let mean = sum / nf;
    let qs = [0.50, 0.75, 0.90, 0.95, 0.99, 0.999];
    // Each percentile's remaining rank, and its value once found.
    let mut pct: Vec<(f64, Option<f64>)> = qs.iter().map(|q| (nf * q, None)).collect();
    let (mut d2, mut d3, mut d4) = (0f64, 0f64, 0f64);
    for &(index, count) in &nonzero {
        let c = count as f64;
        for p in pct.iter_mut() {
            if p.1.is_none() {
                if p.0 < c {
                    let (min, max) = (bin_min(index), bin_max(index));
                    p.1 = Some(min + (max - min) * (p.0 / c));
                } else {
                    p.0 -= c;
                }
            }
        }
        let diff = bin_middle(index) - mean;
        let diff2 = diff * diff;
        d2 += c * diff2;
        d3 += c * diff2 * diff;
        d4 += c * diff2 * diff2;
    }
    let variance = d2 / nf;
    let sd = variance.sqrt();
    let skewness = if sd.powi(3) < 1.0e-12 { json!(0) } else { json!((d3 / nf) / sd.powi(3)) };
    let kurtosis = if sd.powi(4) < 1.0e-12 { json!(0) } else { json!((d4 / nf) / sd.powi(4) - 3.0) };
    let p = |i: usize| -> Value { pct[i].1.map(|v| json!(v)).unwrap_or(json!(0)) };
    json!({
        "n": n,
        "min": bin_min(nonzero[0].0),
        "max": bin_max(nonzero[nonzero.len() - 1].0),
        "arithmetic_mean": mean,
        "geometric_mean": (sum_log / nf).exp(),
        "harmonic_mean": nf / sum_inv,
        "median": p(0),
        "variance": variance,
        "standard_deviation": sd,
        "skewness": skewness,
        "kurtosis": kurtosis,
        "percentile": [[50, p(0)], [75, p(1)], [90, p(2)], [95, p(3)], [99, p(4)], [999, p(5)]],
        "histogram": [[0, 0]]
    })
}

// ------------------------------------------------------------ prometheus

/// One sample: labels and a value, already formatted.
type Sample = (Vec<(String, String)>, String);

/// A number as Erlang's `~p` prints it: integers bare, floats with a
/// fractional part.
fn num(v: &Value) -> String {
    match v {
        Value::Number(n) if n.is_f64() => format!("{:?}", n.as_f64().unwrap_or(0.0)),
        Value::Number(n) => n.to_string(),
        _ => "0".into(),
    }
}

fn fnum(v: f64) -> String {
    format!("{v:?}")
}

fn prom_name(metric: &str) -> String {
    format!("couchdb_{metric}")
}

fn sample_line(metric: &str, s: &Sample) -> String {
    if s.0.is_empty() {
        format!("{} {}", prom_name(metric), s.1)
    } else {
        let labels: Vec<String> = s.0.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
        format!("{}{{{}}} {}", prom_name(metric), labels.join(", "), s.1)
    }
}

/// couch_prometheus_util:to_prom/4: HELP and TYPE, then each sample. No
/// samples, no lines.
pub fn to_prom(metric: &str, ty: &str, desc: &str, samples: Vec<Sample>) -> Vec<String> {
    if samples.is_empty() {
        return vec![];
    }
    let name = prom_name(metric);
    let mut out = vec![format!("\n# HELP {name} {desc}"), format!("# TYPE {name} {ty}")];
    out.extend(samples.iter().map(|s| sample_line(metric, s)));
    out
}

/// to_prom/2: samples alone, continuing the metric above.
fn to_prom_samples(metric: &str, samples: Vec<Sample>) -> Vec<String> {
    samples.iter().map(|s| sample_line(metric, s)).collect()
}

pub fn plain(v: impl ToString) -> Vec<Sample> {
    vec![(vec![], v.to_string())]
}

pub fn labeled(k: &str, v: impl ToString, val: impl ToString) -> Sample {
    (vec![(k.to_string(), v.to_string())], val.to_string())
}

fn path_to_name(path: &[String]) -> String {
    path.join("_")
}

fn counter_metric(path: &[String]) -> String {
    let name = path_to_name(path);
    if name.ends_with("_total") { name } else { format!("{name}_total") }
}

/// The metrics as Prometheus text, before the system ones
/// (couch_prometheus:get_couchdb_stats/0 and couch_to_prom/3).
pub fn prometheus_metrics() -> Vec<String> {
    let (now, iv) = (now_sec(), interval());
    let start = now - (iv - 1);
    let all: Vec<(Vec<String>, &Metric, Value)> =
        registry().all.iter().map(|m| (m.path.iter().map(Elem::text).collect(), *m, m.value(start, iv))).collect();
    let val = |name: &str| -> i64 { all.iter().find(|(p, _, _)| p.join(".") == name).and_then(|(_, _, v)| v.as_i64()).unwrap_or(0) };
    let mut out = Vec::new();
    for (path, m, v) in &all {
        let p: Vec<&str> = path.iter().map(String::as_str).collect();
        out.extend(couch_to_prom(&p, m, v, &val));
    }
    out
}

fn couch_to_prom(p: &[&str], m: &Metric, v: &Value, val: &dyn Fn(&str) -> i64) -> Vec<String> {
    let desc = m.desc.as_str();
    let n = || num(v);
    let total = |other: &str| (v.as_i64().unwrap_or(0) + val(other)).to_string();
    match p {
        ["couch_log", "level", "alert"] => to_prom("couch_log_requests_total", "counter", "number of logged messages", vec![labeled("level", "alert", n())]),
        ["couch_log", "level", level] => to_prom_samples("couch_log_requests_total", vec![labeled("level", level, n())]),
        ["couch_replicator", "checkpoints", "failure"] => to_prom("couch_replicator_checkpoints_failure_total", "counter", desc, plain(n())),
        ["couch_replicator", "checkpoints", "success"] => to_prom(
            "couch_replicator_checkpoints_total",
            "counter",
            "number of checkpoint saves",
            plain(total("couch_replicator.checkpoints.failure")),
        ),
        ["couch_replicator", "responses", "failure"] => to_prom("couch_replicator_responses_failure_total", "counter", desc, plain(n())),
        ["couch_replicator", "responses", "success"] => to_prom(
            "couch_replicator_responses_total",
            "counter",
            "number of HTTP responses received by the replicator",
            plain(total("couch_replicator.responses.failure")),
        ),
        ["couch_replicator", "stream_responses", "failure"] => {
            to_prom("couch_replicator_stream_responses_failure_total", "counter", desc, plain(n()))
        }
        ["couch_replicator", "stream_responses", "success"] => to_prom(
            "couch_replicator_stream_responses_total",
            "counter",
            "number of streaming HTTP responses received by the replicator",
            plain(total("couch_replicator.stream_responses.failure")),
        ),
        ["couchdb", "auth_cache_hits"] => to_prom(
            "auth_cache_requests_total",
            "counter",
            "number of authentication cache requests",
            plain(total("couchdb.auth_cache_misses")),
        ),
        ["couchdb", "auth_cache_misses"] => to_prom("auth_cache_misses_total", "counter", desc, plain(n())),
        ["couchdb", "httpd_request_methods", "COPY"] => {
            to_prom("httpd_request_methods", "counter", "number of HTTP requests by method", vec![labeled("method", "COPY", n())])
        }
        ["couchdb", "httpd_request_methods", method] => to_prom_samples("httpd_request_methods", vec![labeled("method", method, n())]),
        ["couchdb", "httpd_status_codes", "200"] => {
            to_prom("httpd_status_codes", "counter", "number of HTTP responses by status code", vec![labeled("code", 200, n())])
        }
        ["couchdb", "httpd_status_codes", code] => to_prom_samples("httpd_status_codes", vec![labeled("code", code, n())]),
        ["couchdb", "open_databases"] => to_prom("open_databases", "gauge", desc, plain(n())),
        ["couchdb", "open_os_files"] => to_prom("open_os_files", "gauge", desc, plain(n())),
        ["couchdb", "httpd", "clients_requesting_changes"] => to_prom("httpd_clients_requesting_changes", "gauge", desc, plain(n())),
        ["ddoc_cache", "hit"] => {
            to_prom("ddoc_cache_requests_total", "counter", "number of design doc cache requests", plain(total("ddoc_cache.miss")))
        }
        ["ddoc_cache", "miss"] => to_prom("ddoc_cache_requests_failures_total", "counter", desc, plain(n())),
        ["ddoc_cache", "recovery"] => to_prom("ddoc_cache_requests_recovery_total", "counter", desc, plain(n())),
        ["fabric", "read_repairs", "failure"] => to_prom("fabric_read_repairs_failures_total", "counter", desc, plain(n())),
        ["fabric", "read_repairs", "success"] => {
            to_prom("fabric_read_repairs_total", "counter", "number of fabric read repairs", plain(total("fabric.read_repairs.failure")))
        }
        ["rexi", "streams", "timeout", "init_stream"] => {
            to_prom("rexi_streams_timeout_total", "counter", "number of rexi stream timeouts", vec![labeled("stage", "init_stream", n())])
        }
        ["couchdb", rest @ ..] => couch_to_prom(rest, m, v, val),
        _ => {
            let path: Vec<String> = p.iter().map(|s| s.to_string()).collect();
            match m.kind {
                Kind::Counter(_) => to_prom(&counter_metric(&path), "counter", desc, plain(n())),
                Kind::Gauge(_) => to_prom(&path_to_name(&path), "gauge", desc, plain(n())),
                Kind::Histogram(_) => to_prom_summary(&path, desc, v),
            }
        }
    }
}

/// A histogram as a summary in seconds (from milliseconds), with _sum and
/// _count.
fn to_prom_summary(path: &[String], desc: &str, v: &Value) -> Vec<String> {
    let metric = format!("{}_seconds", path_to_name(path));
    let count = v["n"].as_u64().unwrap_or(0);
    let mean = v["arithmetic_mean"].as_f64().unwrap_or(0.0);
    let quantiles: Vec<Sample> = v["percentile"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|pv| {
            let q = match pv[0].as_u64()? {
                50 => "0.5",
                75 => "0.75",
                90 => "0.9",
                95 => "0.95",
                99 => "0.99",
                999 => "0.999",
                _ => return None,
            };
            Some(labeled("quantile", q, fnum(pv[1].as_f64().unwrap_or(0.0) / 1000.0)))
        })
        .collect();
    let mut out = to_prom(&metric, "summary", desc, quantiles);
    out.extend(to_prom_samples(&format!("{metric}_sum"), plain(fnum(count as f64 * mean / 1000.0))));
    out.extend(to_prom_samples(&format!("{metric}_count"), plain(count)));
    out
}

/// A summary of message queue lengths (pid_to_prom_summary/3).
pub fn queue_summary(metric: &str, desc: &str, mut lens: Vec<u64>) -> Vec<String> {
    if lens.is_empty() {
        return vec![];
    }
    lens.sort_unstable();
    let nth = |q: f64| lens[((lens.len() as f64 * q).round() as usize).clamp(1, lens.len()) - 1];
    let quantiles = vec![labeled("quantile", "0.5", nth(0.5)), labeled("quantile", "0.9", nth(0.9)), labeled("quantile", "0.99", nth(0.99))];
    let mut out = to_prom(metric, "summary", desc, quantiles);
    out.extend(to_prom_samples(&format!("{metric}_sum"), plain(lens.iter().sum::<u64>())));
    out.extend(to_prom_samples(&format!("{metric}_count"), plain(lens.len())));
    out.extend(to_prom_samples(&format!("{metric}_min"), plain(lens[0])));
    out.extend(to_prom_samples(&format!("{metric}_max"), plain(lens[lens.len() - 1])));
    out
}

/// Queue lengths as _system reports them (format_pid_stats/1): count,
/// min, max and percentiles, or nothing.
pub fn queue_stats(mut lens: Vec<u64>) -> Value {
    if lens.is_empty() {
        return json!({});
    }
    lens.sort_unstable();
    let n = lens.len();
    let nth = |q: f64| lens[((n as f64 * q).round() as usize).clamp(1, n) - 1];
    json!({"count": n, "min": lens[0], "max": lens[n - 1], "50": nth(0.5), "90": nth(0.9), "99": nth(0.99)})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bins_match_couchdb_constants() {
        // calc_bin_offset/0 and calc_bin_count/0.
        assert_eq!(1 - ((MIN_VAL.to_bits() >> BIN_BITS) as i64), BIN_OFFSET);
        assert_eq!(bin_index(MAX_VAL), BIN_COUNT);
        assert_eq!(bin_index(MIN_VAL), 1);
        for i in 1..=BIN_COUNT {
            assert!(bin_min(i) < bin_middle(i) && bin_middle(i) < bin_max(i));
            assert_eq!(bin_index(bin_middle(i)), i);
        }
    }

    #[test]
    fn descriptions_load() {
        let d = parse_descriptions(DESCRIPTIONS);
        assert_eq!(d.len(), 211);
        let r = registry();
        assert_eq!(r.by_name["couchdb.httpd_status_codes.200"].path[2], Elem::Int(200));
        assert_eq!(r.by_name["couchdb.request_time"].type_name(), "histogram");
        assert_eq!(r.by_name["couchdb.query_server.process_count"].type_name(), "gauge");
        assert_eq!(r.by_name["couchdb.httpd_request_methods.COPY"].desc, "number of HTTP COPY requests");
        // Codes sort before named groups, and numerically.
        let codes: Vec<String> =
            r.all.iter().filter(|m| m.path.len() == 3 && m.path[1] == Elem::Atom("httpd_status_codes".into())).map(|m| m.path[2].text()).collect();
        assert_eq!(&codes[..3], &["200", "201", "202"]);
    }

    #[test]
    fn histogram_summary() {
        let h = Histogram::new(30);
        for v in 1..=1000 {
            h.update(7, v as f64);
        }
        let s = summary(&h.read(7, 1));
        assert_eq!(s["n"], 1000);
        let median = s["median"].as_f64().unwrap();
        assert!((median - 500.0).abs() < 500.0 * 0.07, "{median}");
        let p99 = s["percentile"][4][1].as_f64().unwrap();
        assert!((p99 - 990.0).abs() < 990.0 * 0.07, "{p99}");
        assert!(s["min"].as_f64().unwrap() <= 1.0 && s["max"].as_f64().unwrap() >= 1000.0);
        assert_eq!(summary(&h.read(8, 1))["n"], 0);
        h.clear(7, 1);
        assert_eq!(summary(&h.read(7, 1))["n"], 0);
    }
}
