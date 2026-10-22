//! Map/reduce view indexes, a port of couch_mrview / couch_index.
//!
//! Each design doc's views form an index group stored in its own RocksDB
//! instance named by the group signature, so editing a ddoc builds a new
//! index while an identical ddoc (even under a different id) shares one.
//! Indexes update incrementally from the database's by-seq index.

use crate::api::{QueryArgs, blocking};
use crate::btree::{Dir, Range, Spec};
use crate::collate::{Json, cmp_json, cmp_json_raw};
use crate::db::{Db, Snapshot};
use crate::doc::{DocOpts, FullDocInfo};
use crate::error::{CouchError, Result};
use crate::keyenc;
use crate::rstore::{Store, View, rerr, successor};
use rocksdb::WriteBatch;
use crate::http::{Req, json_response, not_allowed, ok_json};
use crate::js::JsFuns;
use crate::extsort::Sorter;
use crate::mango::Selector;
use axum::http::{Method, StatusCode};
use axum::response::Response;
use base64::Engine;
use md5::{Digest, Md5};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

// ---------------------------------------------------------------- keys

/// Document id component of a view key, with sentinels for range bounds.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum IdBound {
    Min,
    Id(String),
    Max,
}

pub type VKey = (Json, IdBound);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum VVal {
    One(Json),
    /// The same doc emitted the same key more than once.
    Dups(Vec<Json>),
}

impl VVal {
    pub fn values(&self) -> Vec<&Value> {
        match self {
            VVal::One(j) => vec![&j.0],
            VVal::Dups(v) => v.iter().map(|j| &j.0).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VRed {
    pub count: u64,
    pub red: Option<Json>,
}

#[derive(Clone)]
pub enum Reducer {
    None,
    Count,
    Sum,
    Stats,
    ApproxCountDistinct,
    Js(JsFuns, usize),
}

pub struct ViewSpec {
    pub raw: bool,
    pub reducer: Reducer,
}

fn red_err(e: CouchError) -> Value {
    json!({"error": e.error, "reason": e.reason})
}

impl ViewSpec {
    pub fn cmp_key(&self, a: &Value, b: &Value) -> Ordering {
        if self.raw { cmp_json_raw(a, b) } else { cmp_json(a, b) }
    }

    pub fn reduce_values(&self, kvs: &[(Value, Value)]) -> Option<Value> {
        Some(match &self.reducer {
            Reducer::None => return None,
            Reducer::Count => json!(kvs.len()),
            Reducer::Sum => builtin_sum(kvs.iter().map(|(_, v)| v)),
            Reducer::Stats => builtin_stats(kvs.iter().map(|(_, v)| v.clone()).collect(), false),
            Reducer::ApproxCountDistinct => {
                let mut h = Hll::new();
                for (k, _) in kvs {
                    let key = k.get(0).cloned().unwrap_or(Value::Null);
                    h.add(&key);
                }
                h.to_json()
            }
            Reducer::Js(f, i) => f.reduce(*i, kvs).unwrap_or_else(red_err),
        })
    }

    pub fn rereduce_values(&self, vals: &[Value]) -> Option<Value> {
        Some(match &self.reducer {
            Reducer::None => return None,
            Reducer::Count => json!(vals.iter().filter_map(|v| v.as_u64()).sum::<u64>()),
            Reducer::Sum => builtin_sum(vals.iter()),
            Reducer::Stats => builtin_stats(vals.to_vec(), true),
            Reducer::ApproxCountDistinct => {
                let mut h = Hll::new();
                for v in vals {
                    h.merge(&Hll::from_json(v));
                }
                h.to_json()
            }
            Reducer::Js(f, i) => f.rereduce(*i, vals).unwrap_or_else(red_err),
        })
    }
}

impl Spec for ViewSpec {
    type K = VKey;
    type V = VVal;
    type R = VRed;
    fn cmp(&self, a: &VKey, b: &VKey) -> Ordering {
        self.cmp_key(&a.0.0, &b.0.0).then_with(|| a.1.cmp(&b.1))
    }
    fn reduce(&self, kvs: &[(VKey, VVal)]) -> VRed {
        let mut flat = Vec::with_capacity(kvs.len());
        for ((k, id), v) in kvs {
            let id = match id {
                IdBound::Id(s) => json!(s),
                _ => Value::Null,
            };
            for x in v.values() {
                flat.push((json!([k.0, id]), x.clone()));
            }
        }
        VRed {
            count: flat.len() as u64,
            red: self.reduce_values(&flat).map(Json),
        }
    }
    fn rereduce(&self, reds: &[VRed]) -> VRed {
        let vals: Vec<Value> = reds.iter().filter_map(|r| r.red.as_ref().map(|j| j.0.clone())).collect();
        VRed {
            count: reds.iter().map(|r| r.count).sum(),
            red: self.rereduce_values(&vals).map(Json),
        }
    }
}

fn builtin_sum<'a>(vals: impl Iterator<Item = &'a Value>) -> Value {
    let mut acc: Option<Value> = None;
    for v in vals {
        acc = Some(match acc {
            None => v.clone(),
            Some(a) => sum2(&a, v),
        });
    }
    acc.unwrap_or(json!(0))
}

fn num(v: f64) -> Value {
    if v.fract() == 0.0 && v.abs() < 9.0e15 {
        json!(v as i64)
    } else {
        json!(v)
    }
}

fn sum2(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            if let (Some(p), Some(q)) = (x.as_i64(), y.as_i64())
                && let Some(s) = p.checked_add(q) {
                    return json!(s);
                }
            num(x.as_f64().unwrap_or(0.0) + y.as_f64().unwrap_or(0.0))
        }
        (Value::Array(x), Value::Array(y)) => {
            let n = x.len().max(y.len());
            Value::Array(
                (0..n)
                    .map(|i| match (x.get(i), y.get(i)) {
                        (Some(p), Some(q)) => sum2(p, q),
                        (Some(p), None) | (None, Some(p)) => p.clone(),
                        _ => json!(0),
                    })
                    .collect(),
            )
        }
        (Value::Number(_), Value::Array(_)) => sum2(&json!([a]), b),
        (Value::Array(_), Value::Number(_)) => sum2(a, &json!([b])),
        (Value::Object(x), Value::Object(y)) => {
            let mut m = x.clone();
            for (k, v) in y {
                let nv = match m.get(k) {
                    Some(e) => sum2(e, v),
                    None => v.clone(),
                };
                m.insert(k.clone(), nv);
            }
            Value::Object(m)
        }
        _ => json!({"error": "builtin_reduce_error", "reason": "The _sum function requires that map values be numbers, arrays of numbers, or objects."}),
    }
}

fn builtin_stats(vals: Vec<Value>, rereduce: bool) -> Value {
    let mut sum = 0.0;
    let mut count = 0.0;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut sumsqr = 0.0;
    for v in vals {
        if rereduce {
            sum += v["sum"].as_f64().unwrap_or(0.0);
            count += v["count"].as_f64().unwrap_or(0.0);
            min = min.min(v["min"].as_f64().unwrap_or(f64::INFINITY));
            max = max.max(v["max"].as_f64().unwrap_or(f64::NEG_INFINITY));
            sumsqr += v["sumsqr"].as_f64().unwrap_or(0.0);
        } else if let Some(x) = v.as_f64() {
            sum += x;
            count += 1.0;
            min = min.min(x);
            max = max.max(x);
            sumsqr += x * x;
        } else if v.get("count").is_some() && v.get("sum").is_some() {
            // Pre-aggregated stats objects are accepted like CouchDB does.
            return builtin_stats(vec![v], true);
        } else {
            return json!({"error": "builtin_reduce_error", "reason": "The _stats function requires that map values be numbers or arrays of numbers"});
        }
    }
    json!({"sum": num(sum), "count": num(count), "min": num(min), "max": num(max), "sumsqr": num(sumsqr)})
}

/// HyperLogLog for _approx_count_distinct.
struct Hll {
    regs: Vec<u8>,
}

const HLL_P: u32 = 10;

impl Hll {
    fn new() -> Hll {
        Hll {
            regs: vec![0; 1 << HLL_P],
        }
    }
    fn add(&mut self, v: &Value) {
        let d = Md5::digest(v.to_string().as_bytes());
        let h = u64::from_le_bytes(d[..8].try_into().unwrap());
        let idx = (h >> (64 - HLL_P)) as usize;
        let rest = h << HLL_P | (1 << (HLL_P - 1));
        let rank = rest.leading_zeros() as u8 + 1;
        if rank > self.regs[idx] {
            self.regs[idx] = rank;
        }
    }
    fn merge(&mut self, o: &Hll) {
        for (a, b) in self.regs.iter_mut().zip(o.regs.iter()) {
            *a = (*a).max(*b);
        }
    }
    fn count(&self) -> u64 {
        let m = self.regs.len() as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self.regs.iter().map(|r| 2f64.powi(-(*r as i32))).sum();
        let est = alpha * m * m / sum;
        let zeros = self.regs.iter().filter(|r| **r == 0).count() as f64;
        if est <= 2.5 * m && zeros > 0.0 {
            (m * (m / zeros).ln()).round() as u64
        } else {
            est.round() as u64
        }
    }
    fn to_json(&self) -> Value {
        json!({"hll": crate::doc::B64.encode(&self.regs), "n": self.count()})
    }
    fn from_json(v: &Value) -> Hll {
        let regs = v
            .get("hll")
            .and_then(|s| s.as_str())
            .and_then(|s| crate::doc::B64.decode(s).ok())
            .filter(|r| r.len() == 1 << HLL_P)
            .unwrap_or_else(|| vec![0; 1 << HLL_P]);
        Hll { regs }
    }
}

/// Final user-visible form of a reduction.
fn finalize_red(r: &Reducer, v: Value) -> Value {
    match r {
        Reducer::ApproxCountDistinct => v.get("n").cloned().unwrap_or(json!(0)),
        _ => v,
    }
}

// ------------------------------------------------------------ ids entry

/// Keys a document emitted, per view, so its rows can be found and removed
/// when it changes.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IdEntry {
    /// (view index, emitted keys)
    pub keys: Vec<(u32, Vec<Json>)>,
}

// ---------------------------------------------------------- definition

#[derive(Clone)]
pub enum MapKind {
    Js(usize),
    Mango {
        fields: Vec<String>,
        partial: Option<Arc<Selector>>,
    },
}

#[derive(Clone)]
pub struct ViewDef {
    pub name: String,
    pub map: MapKind,
    pub reducer: Reducer,
    pub raw: bool,
    pub reduce_src: Option<String>,
}

#[derive(Clone)]
pub struct IndexDef {
    pub sig: String,
    pub ddoc_id: String,
    pub language: String,
    pub views: Vec<ViewDef>,
    pub js: Option<JsFuns>,
    pub include_design: bool,
    pub local_seq: bool,
}

fn parse_reducer(src: &str, js_red: &mut Vec<String>, ddoc: &Value) -> Result<Reducer> {
    let _ = ddoc;
    let s = src.trim();
    Ok(match s {
        "_count" => Reducer::Count,
        "_sum" => Reducer::Sum,
        "_stats" => Reducer::Stats,
        "_approx_count_distinct" => Reducer::ApproxCountDistinct,
        s if s.starts_with('_') => {
            return Err(CouchError::new(
                StatusCode::BAD_REQUEST,
                "invalid_design_doc",
                format!("Unknown builtin reduce function: {s}"),
            ));
        }
        s => {
            js_red.push(s.to_string());
            // placeholder index fixed up once JsFuns exists
            Reducer::Js(JsFuns::new(&Value::Null, &[], &[]), js_red.len() - 1)
        }
    })
}

impl IndexDef {
    pub fn from_ddoc(ddoc: &Value) -> Result<IndexDef> {
        let id = ddoc["_id"].as_str().unwrap_or("").to_string();
        let language = ddoc.get("language").and_then(|l| l.as_str()).unwrap_or("javascript").to_string();
        let empty = Map::new();
        let views = ddoc.get("views").and_then(|v| v.as_object()).unwrap_or(&empty);
        let opts = ddoc.get("options").cloned().unwrap_or(json!({}));
        let mut defs = Vec::new();
        let mut maps = Vec::new();
        let mut reds = Vec::new();
        let mut names: Vec<&String> = views.keys().filter(|k| *k != "lib").collect();
        names.sort();
        for name in names {
            let v = &views[name];
            let raw = v.get("options").and_then(|o| o.get("collation")).and_then(|c| c.as_str()) == Some("raw")
                || opts.get("collation").and_then(|c| c.as_str()) == Some("raw");
            let map = if language == "query" {
                let fields_v = v.get("map").and_then(|m| m.get("fields")).ok_or_else(|| {
                    CouchError::new(StatusCode::BAD_REQUEST, "invalid_design_doc", format!("View {name} has no map fields"))
                })?;
                let fields: Vec<String> = match fields_v {
                    Value::Object(o) => o.keys().cloned().collect(),
                    Value::Array(a) => a
                        .iter()
                        .filter_map(|f| match f {
                            Value::String(s) => Some(s.clone()),
                            Value::Object(o) => o.keys().next().cloned(),
                            _ => None,
                        })
                        .collect(),
                    _ => vec![],
                };
                let partial = match v.get("map").and_then(|m| m.get("partial_filter_selector")) {
                    Some(s) if s.as_object().map(|o| !o.is_empty()).unwrap_or(false) => {
                        Some(Arc::new(Selector::parse(s)?))
                    }
                    _ => None,
                };
                MapKind::Mango { fields, partial }
            } else {
                let src = v.get("map").and_then(|m| m.as_str()).ok_or_else(|| {
                    CouchError::new(StatusCode::BAD_REQUEST, "invalid_design_doc", format!("View {name} must have a map function"))
                })?;
                maps.push(src.to_string());
                MapKind::Js(maps.len() - 1)
            };
            let reduce_src = v.get("reduce").and_then(|r| r.as_str()).map(String::from);
            let reducer = match &reduce_src {
                Some(s) => parse_reducer(s, &mut reds, ddoc)?,
                None => Reducer::None,
            };
            defs.push(ViewDef {
                name: name.clone(),
                map,
                reducer,
                raw,
                reduce_src,
            });
        }
        let js = if maps.is_empty() && reds.is_empty() {
            None
        } else {
            Some(JsFuns::new(ddoc, &maps, &reds))
        };
        if let Some(j) = &js {
            for d in defs.iter_mut() {
                if let Reducer::Js(_, i) = &d.reducer {
                    d.reducer = Reducer::Js(j.clone(), *i);
                }
            }
        }
        // Signature covers everything that affects index contents.
        let mut sig_src = json!({
            "language": language,
            "views": views,
            "options": opts,
        });
        if language != "query" {
            sig_src["lib"] = views.get("lib").cloned().unwrap_or(Value::Null);
        }
        let sig = hex::encode(Md5::digest(serde_json::to_vec(&sig_src)?));
        Ok(IndexDef {
            sig,
            ddoc_id: id,
            language,
            views: defs,
            js,
            include_design: opts.get("include_design").and_then(|b| b.as_bool()).unwrap_or(false),
            local_seq: opts.get("local_seq").and_then(|b| b.as_bool()).unwrap_or(false),
        })
    }

    pub fn view_index(&self, name: &str) -> Option<usize> {
        self.views.iter().position(|v| v.name == name)
    }
}

/// Build empty indexes with the sort-and-ingest bulk path (default on;
/// COUCHDB_RS_VIEW_BULK=0 forces incremental batches, for comparison).
fn bulk_enabled() -> bool {
    static B: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *B.get_or_init(|| std::env::var("COUCHDB_RS_VIEW_BULK").map(|v| v != "0").unwrap_or(true))
}

/// Upper bound on JS map threads for a bulk build: COUCHDB_RS_MAP_THREADS,
/// else all cores but two (one for the collector, one for the reader).
fn map_threads_max() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("COUCHDB_RS_MAP_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or_else(|| {
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).saturating_sub(2)
        }).max(1)
    })
}

/// Docs per incremental view-index write batch (COUCHDB_RS_VIEW_BATCH).
fn view_batch_docs() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::env::var("COUCHDB_RS_VIEW_BATCH").ok().and_then(|v| v.parse().ok()).unwrap_or(500))
}

fn cmp_key(raw: bool, a: &Value, b: &Value) -> Ordering {
    if raw { cmp_json_raw(a, b) } else { cmp_json(a, b) }
}

// ----------------------------------------------------------- index state
//
// Each index is a RocksDB instance with three column families:
//   rows  [view#][order-preserving key][doc id] -> StoredRow
//   ids   doc id                                -> IdEntry (keys it emitted)
//   meta  "header"                              -> IndexHeader
// Row keys use keyenc, so RocksDB's byte order is CouchDB's view collation.

const CF_ROWS: &str = "rows";
const CF_IDS: &str = "ids";
const CF_META: &str = "meta";
const INDEX_CFS: &[(&str, bool)] = &[(CF_ROWS, false), (CF_IDS, false), (CF_META, false)];
const META_KEY: &[u8] = b"header";

#[derive(Serialize, Deserialize, Clone)]
struct IndexHeader {
    sig: String,
    seq: u64,
    purge_seq: u64,
    db_uuid: String,
    /// Rows (emitted values) per view: total_rows, kept as counters because
    /// RocksDB keeps no reductions.
    counts: Vec<u64>,
}

/// A view row as stored: the original key (the encoded one is lossy), the
/// doc id, and the emitted value(s).
#[derive(Serialize, Deserialize)]
struct StoredRow {
    key: Json,
    id: String,
    val: VVal,
}

#[derive(Clone)]
pub struct IndexState {
    pub seq: u64,
    pub purge_seq: u64,
    pub counts: Vec<u64>,
    view: Arc<View>,
}

pub struct Index {
    pub def: IndexDef,
    pub path: PathBuf,
    store: Arc<Store>,
    state: RwLock<IndexState>,
    update_lock: Mutex<()>,
    db_uuid: String,
    updater_running: std::sync::atomic::AtomicBool,
    /// How the last bulk build ran, reported in _info.
    last_build: Mutex<Option<Value>>,
}

type Mapped = Vec<(String, Vec<Vec<(Json, VVal)>>, IdEntry)>;
type BytePair = (Vec<u8>, Vec<u8>);
type ByteSorter = Sorter<BytePair, Box<dyn Fn(&BytePair, &BytePair) -> Ordering>>;

/// Metadata is stored uncompressed; only bodies and attachments use LZ4.
fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    crate::file::encode_term_raw(v)
}

fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    Ok(crate::file::decode_term(b)?)
}

fn byte_sorter(path: PathBuf) -> ByteSorter {
    Sorter::new(path, crate::extsort::budget_bytes(), Box::new(|a: &BytePair, b: &BytePair| a.0.cmp(&b.0)))
}

impl Index {
    fn row_key(&self, vi: usize, key: &Value, id: &str) -> Vec<u8> {
        keyenc::row_key(vi as u8, key, self.def.views[vi].raw, id)
    }

    fn open(db: &Db, def: IndexDef, dir: PathBuf) -> Result<Index> {
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.rocks", def.sig));
        let db_uuid = db.snapshot().header.uuid.clone();
        let store = Store::open(&path, INDEX_CFS, true)?;
        let n = def.views.len();
        let header: Option<IndexHeader> = match store.db.get_cf(&store.cf(CF_META), META_KEY).map_err(rerr)? {
            Some(b) => dec::<IndexHeader>(&b).ok(),
            None => None,
        };
        let (seq, purge_seq, counts) = match header {
            Some(h) if h.sig == def.sig && h.db_uuid == db_uuid && h.counts.len() == n => (h.seq, h.purge_seq, h.counts),
            _ => {
                wipe(&store)?;
                (0, 0, vec![0; n])
            }
        };
        let state = IndexState { seq, purge_seq, counts, view: View::new(store.clone()) };
        Ok(Index {
            def,
            path,
            store,
            state: RwLock::new(state),
            update_lock: Mutex::new(()),
            db_uuid,
            updater_running: std::sync::atomic::AtomicBool::new(false),
            last_build: Mutex::new(None),
        })
    }

    pub fn state(&self) -> IndexState {
        self.state.read().clone()
    }

    fn put_meta(&self, wb: &mut WriteBatch, seq: u64, purge_seq: u64, counts: &[u64]) {
        let h = IndexHeader { sig: self.def.sig.clone(), seq, purge_seq, db_uuid: self.db_uuid.clone(), counts: counts.to_vec() };
        wb.put_cf(&self.store.cf(CF_META), META_KEY, enc(&h));
    }

    /// Map one document into per-view rows.
    fn map_doc(&self, snap: &Snapshot, fdi: &FullDocInfo) -> Result<Vec<Vec<(Value, Value)>>> {
        let n = self.def.views.len();
        if fdi.deleted || (fdi.id.starts_with("_design/") && !self.def.include_design) {
            return Ok(vec![Vec::new(); n]);
        }
        let opts = DocOpts { local_seq: self.def.local_seq, ..Default::default() };
        let doc = snap.winner_json(fdi, &opts)?;
        let js_rows = match &self.def.js {
            Some(f) if self.def.views.iter().any(|v| matches!(v.map, MapKind::Js(_))) => Some(f.map(&doc)?),
            _ => None,
        };
        let mut out = Vec::with_capacity(n);
        for v in &self.def.views {
            match &v.map {
                MapKind::Js(i) => out.push(js_rows.as_ref().and_then(|r| r.get(*i).cloned()).unwrap_or_default()),
                MapKind::Mango { fields, partial } => {
                    if let Some(p) = partial {
                        if !p.matches(&doc) {
                            out.push(Vec::new());
                            continue;
                        }
                    }
                    let mut key = Vec::with_capacity(fields.len());
                    for f in fields {
                        match crate::mango::get_field(&doc, f) {
                            Some(val) => key.push(val.clone()),
                            None => break,
                        }
                    }
                    if key.len() == fields.len() {
                        out.push(vec![(Value::Array(key), Value::Null)]);
                    } else {
                        out.push(Vec::new());
                    }
                }
            }
        }
        Ok(out)
    }

    /// The rows one doc contributes: per view, (key, value) with duplicate
    /// keys from the same doc merged, plus its ids entry.
    fn doc_entries(&self, snap: &Snapshot, fdi: &FullDocInfo) -> Result<(Vec<Vec<(Json, VVal)>>, IdEntry)> {
        let rows = self.map_doc(snap, fdi)?;
        let mut entry = IdEntry { keys: Vec::new() };
        let mut per_view = Vec::with_capacity(rows.len());
        for (vi, vrows) in rows.into_iter().enumerate() {
            let mut out = Vec::new();
            if !vrows.is_empty() {
                let raw = self.def.views[vi].raw;
                let mut grouped: Vec<(Value, Vec<Json>)> = Vec::new();
                for (k, v) in vrows {
                    match grouped.iter_mut().find(|(gk, _)| cmp_key(raw, gk, &k) == Ordering::Equal) {
                        Some((_, vs)) => vs.push(Json(v)),
                        None => grouped.push((k, vec![Json(v)])),
                    }
                }
                let mut keys = Vec::new();
                for (k, mut vs) in grouped {
                    let val = if vs.len() == 1 { VVal::One(vs.pop().unwrap()) } else { VVal::Dups(vs) };
                    keys.push(Json(k.clone()));
                    out.push((Json(k), val));
                }
                entry.keys.push((vi as u32, keys));
            }
            per_view.push(out);
        }
        Ok((per_view, entry))
    }

    /// Bring the index up to date with the database.
    pub fn update(&self, db: &Db) -> Result<IndexState> {
        let _g = self.update_lock.lock();
        let snap = db.snapshot();
        let mut st = self.state();
        if snap.header.purge_seq != st.purge_seq || st.seq > snap.update_seq() {
            // Purges (and db recreation) invalidate the index: rebuild.
            wipe(&self.store)?;
            st = IndexState { seq: 0, purge_seq: snap.header.purge_seq, counts: vec![0; self.def.views.len()], view: View::new(self.store.clone()) };
        }
        if st.seq >= snap.update_seq() {
            return Ok(st);
        }
        self.updater_running.store(true, std::sync::atomic::Ordering::Relaxed);
        let res = (|| -> Result<IndexState> {
            if st.seq == 0 && st.counts.iter().all(|c| *c == 0) && bulk_enabled() {
                self.bulk_build(&snap, &mut st)?;
            } else {
                let mut batch: Vec<FullDocInfo> = Vec::new();
                let mut err = None;
                snap.fold_changes(st.seq, Dir::Fwd, |fdi| {
                    batch.push(fdi.clone());
                    if batch.len() >= view_batch_docs() {
                        if let Err(e) = self.apply_batch(&snap, &mut st, std::mem::take(&mut batch)) {
                            err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                })?;
                if let Some(e) = err {
                    return Err(e);
                }
                self.apply_batch(&snap, &mut st, batch)?;
                // Record that the index has seen everything up to the
                // snapshot, even if the last docs changed no rows.
                let mut wb = WriteBatch::default();
                st.seq = snap.update_seq();
                self.put_meta(&mut wb, st.seq, st.purge_seq, &st.counts);
                self.store.db.write(wb).map_err(rerr)?;
            }
            st.view = View::new(self.store.clone());
            *self.state.write() = st.clone();
            Ok(st)
        })();
        self.updater_running.store(false, std::sync::atomic::Ordering::Relaxed);
        res
    }

    /// Apply a batch of changed docs: delete their old rows, write their
    /// new ones, and advance the seq, all in one atomic WriteBatch.
    fn apply_batch(&self, snap: &Snapshot, st: &mut IndexState, batch: Vec<FullDocInfo>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let current = View::new(self.store.clone());
        let mut wb = WriteBatch::default();
        let (rows_cf, ids_cf) = (self.store.cf(CF_ROWS), self.store.cf(CF_IDS));
        let last_seq = batch.last().map(|f| f.update_seq).unwrap_or(st.seq);
        for fdi in batch {
            let old: Option<IdEntry> = match current.get(CF_IDS, fdi.id.as_bytes())? {
                Some(b) => Some(dec(&b)?),
                None => None,
            };
            if let Some(old) = &old {
                for (vi, keys) in &old.keys {
                    let vi = *vi as usize;
                    for k in keys {
                        let rk = self.row_key(vi, &k.0, &fdi.id);
                        if let Some(b) = current.get(CF_ROWS, &rk)? {
                            let row: StoredRow = dec(&b)?;
                            st.counts[vi] = st.counts[vi].saturating_sub(row.val.values().len() as u64);
                        }
                        wb.delete_cf(&rows_cf, rk);
                    }
                }
            }
            let (per_view, entry) = self.doc_entries(snap, &fdi)?;
            for (vi, rows) in per_view.into_iter().enumerate() {
                for (k, val) in rows {
                    st.counts[vi] += val.values().len() as u64;
                    let rk = self.row_key(vi, &k.0, &fdi.id);
                    wb.put_cf(&rows_cf, rk, enc(&StoredRow { key: k, id: fdi.id.clone(), val }));
                }
            }
            if entry.keys.is_empty() {
                if old.is_some() {
                    wb.delete_cf(&ids_cf, fdi.id.as_bytes());
                }
            } else {
                wb.put_cf(&ids_cf, fdi.id.as_bytes(), enc(&entry));
            }
        }
        st.seq = last_seq;
        self.put_meta(&mut wb, st.seq, st.purge_seq, &st.counts);
        self.store.db.write(wb).map_err(rerr)?;
        Ok(())
    }

    /// Build a fresh index: map every doc on a pool of threads, sort the
    /// encoded rows externally, then write each column family as one SST
    /// file and ingest it (RocksDB's bulk load: no memtable, no compaction).
    fn bulk_build(&self, snap: &Snapshot, st: &mut IndexState) -> Result<()> {
        let scratch = |name: &str| self.path.join(format!("bulk-{name}"));
        let mut row_sorter = byte_sorter(scratch("rows.sort"));
        let mut id_sorter = byte_sorter(scratch("ids.sort"));
        let mut counts = vec![0u64; self.def.views.len()];
        let feed = |mapped: Mapped, rows: &mut ByteSorter, ids: &mut ByteSorter, counts: &mut Vec<u64>| -> Result<()> {
            for (id, per_view, entry) in mapped {
                for (vi, vrows) in per_view.into_iter().enumerate() {
                    for (k, val) in vrows {
                        counts[vi] += val.values().len() as u64;
                        let rk = self.row_key(vi, &k.0, &id);
                        let v = enc(&StoredRow { key: k, id: id.clone(), val });
                        let approx = rk.len() + v.len();
                        rows.push((rk, v), approx)?;
                    }
                }
                if !entry.keys.is_empty() {
                    let v = enc(&entry);
                    let approx = id.len() + v.len();
                    ids.push((id.into_bytes(), v), approx)?;
                }
            }
            Ok(())
        };
        // Pipeline: one thread walks the by-seq index, JS map workers load and
        // map docs, and this thread feeds the sorters. Workers are added while
        // this (serial) stage sits idle waiting for them, up to the core count.
        let max_workers = map_threads_max();
        let (work_tx, work_rx) = std::sync::mpsc::sync_channel::<Vec<FullDocInfo>>(max_workers * 4);
        let work_rx = Arc::new(Mutex::new(work_rx));
        let (res_tx, res_rx) = std::sync::mpsc::sync_channel::<Result<Mapped>>(max_workers * 4);
        let producer_done = std::sync::atomic::AtomicBool::new(false);
        let alive = std::sync::atomic::AtomicUsize::new(0);
        let reader_blocked = std::sync::atomic::AtomicU64::new(0);
        let mut err: Option<CouchError> = None;
        let mut workers = 0usize;
        let mut busy = std::time::Duration::ZERO;
        let t_start = Instant::now();
        std::thread::scope(|sc| {
            sc.spawn(|| {
                let mut batch = Vec::with_capacity(256);
                let _ = snap.fold_changes(0, Dir::Fwd, |fdi| {
                    batch.push(fdi.clone());
                    if batch.len() >= 256 {
                        let t = Instant::now();
                        if work_tx.send(std::mem::take(&mut batch)).is_err() {
                            return Ok(false);
                        }
                        reader_blocked.fetch_add(t.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                    Ok(true)
                });
                if !batch.is_empty() {
                    let _ = work_tx.send(batch);
                }
                drop(work_tx);
                producer_done.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            let spawn_worker = |res_tx: std::sync::mpsc::SyncSender<Result<Mapped>>| {
                let work_rx = work_rx.clone();
                alive.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let alive = &alive;
                sc.spawn(move || {
                    loop {
                        let next = work_rx.lock().recv();
                        let Ok(batch) = next else { break };
                        let out: Result<Mapped> =
                            batch.iter().map(|f| self.doc_entries(snap, f).map(|(pv, e)| (f.id.clone(), pv, e))).collect();
                        if res_tx.send(out).is_err() {
                            break;
                        }
                    }
                    alive.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                });
            };
            spawn_worker(res_tx.clone());
            workers = 1;
            let mut window = Instant::now();
            let mut idle = std::time::Duration::ZERO;
            loop {
                let t = Instant::now();
                let got = res_rx.recv_timeout(std::time::Duration::from_millis(50));
                idle += t.elapsed();
                match got {
                    Ok(Ok(mapped)) => {
                        let t_work = Instant::now();
                        if let Err(e) = feed(mapped, &mut row_sorter, &mut id_sorter, &mut counts) {
                            err = Some(e);
                            break;
                        }
                        busy += t_work.elapsed();
                    }
                    Ok(Err(e)) => {
                        err = Some(e);
                        break;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
                let done = producer_done.load(std::sync::atomic::Ordering::SeqCst);
                if done && alive.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    while let Ok(r) = res_rx.try_recv() {
                        if let Err(e) = r.and_then(|m| feed(m, &mut row_sorter, &mut id_sorter, &mut counts)) {
                            err = Some(e);
                            break;
                        }
                    }
                    break;
                }
                if window.elapsed() >= std::time::Duration::from_millis(100) {
                    // Still waiting on workers more than 5% of the time: double
                    // the pool (capped), so it reaches full width quickly.
                    let frac_idle = idle.as_secs_f64() / window.elapsed().as_secs_f64();
                    if frac_idle > 0.05 && workers < max_workers && !done {
                        for _ in 0..workers.min(max_workers - workers) {
                            spawn_worker(res_tx.clone());
                            workers += 1;
                        }
                    }
                    window = Instant::now();
                    idle = std::time::Duration::ZERO;
                }
            }
            drop(res_rx);
            drop(res_tx);
        });
        if let Some(e) = err {
            return Err(e);
        }
        let map_secs = t_start.elapsed().as_secs_f64();
        let t_build = Instant::now();
        self.ingest_sorted(CF_ROWS, row_sorter, &scratch("rows.sst"))?;
        self.ingest_sorted(CF_IDS, id_sorter, &scratch("ids.sst"))?;
        st.seq = snap.update_seq();
        st.counts = counts;
        let mut wb = WriteBatch::default();
        self.put_meta(&mut wb, st.seq, st.purge_seq, &st.counts);
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);
        self.store.db.write_opt(wb, &wo).map_err(rerr)?;
        *self.last_build.lock() = Some(json!({
            "method": "bulk (sort + SST ingest)",
            "map_threads": workers,
            "map_threads_max": max_workers,
            "map_phase_secs": map_secs,
            "collector_busy": busy.as_secs_f64() / map_secs.max(1e-9),
            "reader_blocked": reader_blocked.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e9 / map_secs.max(1e-9),
            "sort_and_ingest_secs": t_build.elapsed().as_secs_f64(),
        }));
        Ok(())
    }

    /// Write sorted pairs into one SST file and ingest it into `cf`.
    fn ingest_sorted(&self, cf: &str, sorter: ByteSorter, sst: &std::path::Path) -> Result<()> {
        let mut sorted = sorter.finish()?;
        let opts = crate::rstore::cf_options(false);
        let mut w = rocksdb::SstFileWriter::create(&opts);
        w.open(sst).map_err(rerr)?;
        let mut any = false;
        while let Some((k, v)) = sorted.next_item()? {
            w.put(k, v).map_err(rerr)?;
            any = true;
        }
        if !any {
            drop(w);
            let _ = std::fs::remove_file(sst);
            return Ok(());
        }
        w.finish().map_err(rerr)?;
        self.store.db.ingest_external_file_cf(&self.store.cf(cf), vec![sst]).map_err(rerr)?;
        let _ = std::fs::remove_file(sst);
        Ok(())
    }

    pub fn info(&self, name: &str) -> Value {
        let st = self.state();
        let active: u64 = [CF_ROWS, CF_IDS].iter().map(|cf| self.store.prop(cf, "rocksdb.estimate-live-data-size")).sum();
        json!({
            "name": name.trim_start_matches("_design/"),
            "view_index": {
                "signature": self.def.sig,
                "language": self.def.language,
                "sizes": {"file": self.store.disk_bytes(), "external": 0, "active": active},
                "update_seq": st.seq,
                "purge_seq": st.purge_seq,
                "updater_running": self.updater_running.load(std::sync::atomic::Ordering::Relaxed),
                "compact_running": false,
                "waiting_clients": 0,
                "waiting_commit": false,
                "collator_versions": ["153.112"],
                "updates_pending": {"minimum": 0, "preferred": 0, "total": 0}
            },
            "couchdb_rs_last_build": self.last_build.lock().clone()
        })
    }

    /// Compact every column family (RocksDB reclaims deleted rows).
    pub fn compact(&self) -> Result<()> {
        let _g = self.update_lock.lock();
        let mut co = rocksdb::CompactOptions::default();
        co.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
        for (cf, _) in INDEX_CFS {
            self.store.db.compact_range_cf_opt(&self.store.cf(cf), None::<&[u8]>, None::<&[u8]>, &co);
        }
        Ok(())
    }

    /// RocksDB bounds [lower, upper) for a view query's key range, and
    /// whether to iterate in reverse. (`keys` is handled by the caller.)
    fn bounds(&self, vi: usize, args: &QueryArgs) -> (Vec<u8>, Vec<u8>, bool) {
        let p = vi as u8;
        let raw = self.def.views[vi].raw;
        let (all_lo, all_hi) = (vec![p], vec![p + 1]);
        let row = |k: &Value, d: &str| keyenc::row_key(p, k, raw, d);
        let incl = args.inclusive_end;
        if !args.descending {
            let lower = match (&args.start_key, &args.start_docid) {
                (Some(k), Some(d)) => row(k, d),
                (Some(k), None) => keyenc::key_min(p, k, raw),
                (None, _) => all_lo,
            };
            let upper = match (&args.end_key, &args.end_docid) {
                (Some(k), Some(d)) => if incl { successor(&row(k, d)) } else { row(k, d) },
                (Some(k), None) => if incl { keyenc::key_max(p, k, raw) } else { keyenc::key_min(p, k, raw) },
                (None, _) => all_hi,
            };
            (lower, upper, false)
        } else {
            let upper = match (&args.start_key, &args.start_docid) {
                (Some(k), Some(d)) => successor(&row(k, d)),
                (Some(k), None) => keyenc::key_max(p, k, raw),
                (None, _) => all_hi,
            };
            let lower = match (&args.end_key, &args.end_docid) {
                (Some(k), Some(d)) => if incl { row(k, d) } else { successor(&row(k, d)) },
                (Some(k), None) => if incl { keyenc::key_min(p, k, raw) } else { keyenc::key_max(p, k, raw) },
                (None, _) => all_lo,
            };
            (lower, upper, true)
        }
    }

    /// Visit the rows of view `vi` in `args`' range and direction.
    pub fn fold_rows<F>(&self, st: &IndexState, vi: usize, args: &QueryArgs, mut f: F) -> Result<()>
    where
        F: FnMut(&Value, &str, &VVal) -> Result<bool>,
    {
        let (lo, hi, rev) = self.bounds(vi, args);
        if lo >= hi {
            return Ok(());
        }
        st.view.scan(CF_ROWS, Some(&lo), Some(&hi), rev, |_, v| {
            let row: StoredRow = dec(v)?;
            f(&row.key.0, &row.id, &row.val)
        })
    }

    /// Emitted values sorting before the range start, in iteration order
    /// (the `offset` of a view response). RocksDB keeps no order
    /// statistics, so this counts by scanning.
    fn count_before(&self, st: &IndexState, vi: usize, args: &QueryArgs) -> Result<u64> {
        let p = vi as u8;
        let (lo, hi, rev) = self.bounds(vi, args);
        let (a, b) = if rev { (hi, vec![p + 1]) } else { (vec![p], lo) };
        let mut n = 0u64;
        if a < b {
            st.view.scan(CF_ROWS, Some(&a), Some(&b), false, |_, v| {
                n += dec::<StoredRow>(v)?.val.values().len() as u64;
                Ok(true)
            })?;
        }
        Ok(n)
    }
}

/// Delete everything in an index store (rebuild from scratch).
fn wipe(store: &Store) -> Result<()> {
    let mut wb = WriteBatch::default();
    wb.delete_range_cf(&store.cf(CF_ROWS), [0u8], [0xFFu8]);
    wb.delete_range_cf(&store.cf(CF_IDS), &[][..], &[0xFFu8][..]);
    wb.delete_cf(&store.cf(CF_META), META_KEY);
    store.db.write(wb).map_err(rerr)?;
    Ok(())
}

/// Find or open the index for a design doc.
pub fn get_index(db: &Arc<Db>, ddoc: &Value, index_dir: PathBuf) -> Result<Arc<Index>> {
    let def = IndexDef::from_ddoc(ddoc)?;
    let key = format!("mrview:{}", def.sig);
    if let Some(i) = db.ext.lock().get(&key).cloned() {
        if let Ok(idx) = i.downcast::<Index>() {
            return Ok(idx);
        }
    }
    let idx = Arc::new(Index::open(db, def, index_dir)?);
    db.ext.lock().insert(key, idx.clone());
    Ok(idx)
}

pub fn index_dir(req: &Req, db: &Db) -> PathBuf {
    req.server.index_dir(&db.name).join("mrview")
}

// ------------------------------------------------------------- querying

fn group_key(args: &QueryArgs, key: &Value) -> Value {
    if let Some(l) = args.group_level {
        if let Value::Array(a) = key {
            return Value::Array(a.iter().take(l).cloned().collect());
        }
        return key.clone();
    }
    if args.group { key.clone() } else { Value::Null }
}

fn same_group(args: &QueryArgs, raw: bool, a: &Value, b: &Value) -> bool {
    if let Some(l) = args.group_level {
        return match (a, b) {
            (Value::Array(x), Value::Array(y)) => {
                let (x, y) = (&x[..x.len().min(l)], &y[..y.len().min(l)]);
                x.len() == y.len() && x.iter().zip(y).all(|(p, q)| cmp_key(raw, p, q) == Ordering::Equal)
            }
            _ => cmp_key(raw, a, b) == Ordering::Equal,
        };
    }
    if args.group { cmp_key(raw, a, b) == Ordering::Equal } else { true }
}

/// Fetch the doc for include_docs, following `{"_id": ...}` values.
fn row_doc(snap: &Snapshot, id: &str, value: &Value, opts: &DocOpts) -> Result<Value> {
    let (target, rev) = match value.get("_id").and_then(|i| i.as_str()) {
        Some(linked) => (linked.to_string(), value.get("_rev").and_then(|r| r.as_str()).map(String::from)),
        None => (id.to_string(), None),
    };
    let Some(fdi) = snap.get_fdi(&target)? else { return Ok(Value::Null) };
    let ri = match rev {
        Some(r) => {
            let (p, rid) = crate::doc::parse_rev(&r)?;
            match Snapshot::find_rev(&fdi, p, &rid) {
                Some(ri) => ri,
                None => return Ok(Value::Null),
            }
        }
        None => {
            let w = fdi.winner();
            if w.deleted {
                return Ok(Value::Null);
            }
            w
        }
    };
    snap.doc_json(&fdi, &ri, opts)
}

pub struct ViewResult {
    pub total_rows: u64,
    pub offset: u64,
    pub rows: Vec<Value>,
    pub update_seq: u64,
    pub reduced: bool,
}

/// Running reduction of one group: kvs are reduced in chunks, chunk
/// results rereduced at the end (RocksDB has no stored reductions).
struct GroupAcc {
    key: Value,
    kvs: Vec<(Value, Value)>,
    partials: Vec<Value>,
}

impl GroupAcc {
    fn push(&mut self, spec: &ViewSpec, k: &Value, id: &str, v: &Value) {
        self.kvs.push((json!([k, id]), v.clone()));
        if self.kvs.len() >= 1000 {
            if let Some(r) = spec.reduce_values(&std::mem::take(&mut self.kvs)) {
                self.partials.push(r);
            }
        }
    }
    fn finish(mut self, spec: &ViewSpec) -> Option<Value> {
        if !self.kvs.is_empty() {
            if let Some(r) = spec.reduce_values(&self.kvs) {
                self.partials.push(r);
            }
        }
        match self.partials.len() {
            0 => None,
            1 => self.partials.pop(),
            _ => spec.rereduce_values(&self.partials),
        }
    }
}

pub fn query_view(snap: &Snapshot, idx: &Index, st: &IndexState, vi: usize, args: &QueryArgs) -> Result<ViewResult> {
    let def = &idx.def;
    let vdef = &def.views[vi];
    let has_reduce = !matches!(vdef.reducer, Reducer::None);
    let reduce = args.reduce.unwrap_or(has_reduce);
    if reduce && !has_reduce && args.reduce == Some(true) {
        return Err(CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", "Reduce is invalid for map-only views."));
    }
    if !has_reduce && (args.group || args.group_level.is_some()) {
        return Err(CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", "Invalid use of grouping on a map view."));
    }
    let reduce = reduce && has_reduce;
    if reduce && args.include_docs {
        return Err(CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", "`include_docs` is invalid for reduce"));
    }
    if reduce && args.keys.is_some() && !(args.group || args.group_level.is_some()) {
        return Err(CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", "Multi-key fetches for reduce views must use `group=true`"));
    }
    let raw = vdef.raw;
    let spec = ViewSpec { raw, reducer: vdef.reducer.clone() };
    let total = st.counts.get(vi).copied().unwrap_or(0);
    let opts = args.doc_opts();
    let mut rows = Vec::new();

    let key_args: Vec<QueryArgs> = match &args.keys {
        Some(keys) => keys
            .iter()
            .map(|k| QueryArgs {
                start_key: Some(k.clone()),
                end_key: Some(k.clone()),
                start_docid: None,
                end_docid: None,
                inclusive_end: true,
                keys: None,
                ..args.clone()
            })
            .collect(),
        None => vec![args.clone()],
    };

    if reduce {
        let mut skip = args.skip;
        let mut emit = |acc: GroupAcc, rows: &mut Vec<Value>| -> Result<()> {
            let key = group_key(args, &acc.key);
            let v = acc.finish(&spec).map(|j| finalize_red(&vdef.reducer, j)).unwrap_or(Value::Null);
            if let Some(e) = v.get("error").filter(|_| v.get("reason").is_some()) {
                return Err(CouchError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    e.as_str().unwrap_or("reduce_error"),
                    v["reason"].as_str().unwrap_or("").to_string(),
                ));
            }
            if skip > 0 {
                skip -= 1;
            } else if rows.len() < args.limit {
                rows.push(json!({"key": key, "value": v}));
            }
            Ok(())
        };
        for a in &key_args {
            let mut acc: Option<GroupAcc> = None;
            let mut err = None;
            idx.fold_rows(st, vi, a, |k, id, val| {
                if let Some(g) = &acc {
                    if !same_group(args, raw, &g.key, k) {
                        if let Err(e) = emit(acc.take().unwrap(), &mut rows) {
                            err = Some(e);
                            return Ok(false);
                        }
                        if rows.len() >= args.limit {
                            return Ok(false);
                        }
                    }
                }
                let g = acc.get_or_insert_with(|| GroupAcc { key: k.clone(), kvs: Vec::new(), partials: Vec::new() });
                for v in val.values() {
                    g.push(&spec, k, id, v);
                }
                Ok(true)
            })?;
            if let Some(e) = err {
                return Err(e);
            }
            if let Some(g) = acc.take() {
                emit(g, &mut rows)?;
            }
            if rows.len() >= args.limit {
                break;
            }
        }
        return Ok(ViewResult { total_rows: total, offset: 0, rows, update_seq: st.seq, reduced: true });
    }

    let mut offset = None;
    let mut skip = args.skip;
    for a in &key_args {
        if offset.is_none() {
            offset = Some(idx.count_before(st, vi, a)?);
        }
        let mut err = None;
        idx.fold_rows(st, vi, a, |k, id, v| {
            for val in v.values() {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                if rows.len() >= args.limit {
                    return Ok(false);
                }
                let mut row = json!({"id": id, "key": k, "value": val});
                if args.include_docs {
                    match row_doc(snap, id, val, &opts) {
                        Ok(d) => row["doc"] = d,
                        Err(e) => {
                            err = Some(e);
                            return Ok(false);
                        }
                    }
                }
                rows.push(row);
            }
            Ok(rows.len() < args.limit)
        })?;
        if let Some(e) = err {
            return Err(e);
        }
        if rows.len() >= args.limit {
            break;
        }
    }
    Ok(ViewResult { total_rows: total, offset: offset.unwrap_or(0) + args.skip as u64, rows, update_seq: st.seq, reduced: false })
}

/// Load ddoc + index, update per `args.update`, and return state.
pub fn prepare(db: &Arc<Db>, dir: PathBuf, ddoc_id: &str, update: &str) -> Result<(Snapshot, Arc<Index>, IndexState)> {
    let snap = db.snapshot();
    let ddoc = crate::changes::load_ddoc(&snap, ddoc_id)?;
    let idx = get_index(db, &ddoc, dir)?;
    let st = match update {
        "false" => idx.state(),
        "lazy" => {
            let st = idx.state();
            let (db2, idx2) = (db.clone(), idx.clone());
            std::thread::spawn(move || {
                let _ = idx2.update(&db2);
            });
            st
        }
        _ => idx.update(db)?,
    };
    Ok((db.snapshot(), idx, st))
}

fn view_response(res: ViewResult, args: &QueryArgs) -> Value {
    let mut out = if res.reduced {
        json!({"rows": res.rows})
    } else {
        json!({"total_rows": res.total_rows, "offset": res.offset, "rows": res.rows})
    };
    if args.update_seq {
        out["update_seq"] = json!(res.update_seq);
    }
    out
}

pub async fn http_view(req: &Req, db: &Arc<Db>, ddoc_id: &str, view: &str) -> Result<Response> {
    let mut args = QueryArgs::from_req(req)?;
    match req.method {
        Method::GET | Method::HEAD => {}
        Method::POST => {
            let body = req.json_obj_body()?;
            args = QueryArgs::from_obj(&body, args)?;
        }
        _ => return not_allowed("GET,POST,HEAD"),
    }
    let dir = index_dir(req, db);
    let v = blocking(|| -> Result<Value> {
        let (snap, idx, st) = prepare(db, dir, ddoc_id, &args.update)?;
        let vi = idx.def.view_index(view).ok_or_else(|| CouchError::not_found("missing_named_view"))?;
        Ok(view_response(query_view(&snap, &idx, &st, vi, &args)?, &args))
    })?;
    ok_json(v)
}

pub async fn http_view_queries(req: &Req, db: &Arc<Db>, ddoc_id: &str, view: &str) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let body = req.json_obj_body()?;
    let queries = body
        .get("queries")
        .and_then(|q| q.as_array())
        .cloned()
        .ok_or_else(|| CouchError::bad_request("`queries` member must be an array"))?;
    let base = QueryArgs::from_req(req)?;
    let dir = index_dir(req, db);
    let v = blocking(|| -> Result<Value> {
        let (snap, idx, st) = prepare(db, dir, ddoc_id, &base.update)?;
        let vi = idx.def.view_index(view).ok_or_else(|| CouchError::not_found("missing_named_view"))?;
        let mut results = Vec::new();
        for q in &queries {
            let obj = q.as_object().ok_or_else(|| CouchError::bad_request("query must be an object"))?;
            let args = QueryArgs::from_obj(obj, base.clone())?;
            results.push(view_response(query_view(&snap, &idx, &st, vi, &args)?, &args));
        }
        Ok(json!({"results": results}))
    })?;
    ok_json(v)
}

pub async fn http_ddoc_info(req: &Req, db: &Arc<Db>, ddoc_id: &str) -> Result<Response> {
    if req.method != Method::GET {
        return not_allowed("GET");
    }
    let snap = db.snapshot();
    let ddoc = crate::changes::load_ddoc(&snap, ddoc_id)?;
    let idx = get_index(db, &ddoc, index_dir(req, db))?;
    ok_json(idx.info(ddoc_id))
}

pub fn http_compact_view(req: &Req, db: &Arc<Db>, ddoc: &str) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let snap = db.snapshot();
    let ddoc = crate::changes::load_ddoc(&snap, &format!("_design/{ddoc}"))?;
    let idx = get_index(db, &ddoc, index_dir(req, db))?;
    std::thread::spawn(move || {
        if let Err(e) = idx.compact() {
            tracing::error!("view compaction failed: {}", e.reason);
        }
    });
    Ok(json_response(StatusCode::ACCEPTED, &json!({"ok": true})))
}

pub fn http_view_cleanup(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let snap = db.snapshot();
    let mut live = std::collections::HashSet::new();
    let range = Range {
        start: Some("_design/".to_string()),
        end: Some("_design0".to_string()),
        inclusive_end: false,
    };
    let mut ddocs = Vec::new();
    snap.fold_docs(Dir::Fwd, &range, |fdi| {
        if !fdi.deleted {
            ddocs.push(fdi.id.clone());
        }
        Ok(true)
    })?;
    for id in ddocs {
        if let Ok(d) = crate::changes::load_ddoc(&snap, &id)
            && let Ok(def) = IndexDef::from_ddoc(&d) {
                live.insert(def.sig);
            }
    }
    let dir = index_dir(req, db);
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(sig) = name.strip_suffix(".rocks")
                && !live.contains(sig) {
                    // Drop the open index first so its RocksDB handle closes.
                    db.ext.lock().remove(&format!("mrview:{sig}"));
                    let _ = std::fs::remove_dir_all(e.path());
                }
        }
    }
    Ok(json_response(StatusCode::ACCEPTED, &json!({"ok": true})))
}

/// Indexes keyed by ddoc id, used by mango to list and pick indexes.
pub fn all_ddocs(snap: &Snapshot) -> Result<Vec<Value>> {
    let range = Range {
        start: Some("_design/".to_string()),
        end: Some("_design0".to_string()),
        inclusive_end: false,
    };
    let mut ids = Vec::new();
    snap.fold_docs(Dir::Fwd, &range, |fdi| {
        if !fdi.deleted {
            ids.push(fdi.id.clone());
        }
        Ok(true)
    })?;
    let mut out = Vec::new();
    for id in ids {
        out.push(crate::changes::load_ddoc(snap, &id)?);
    }
    Ok(out)
}
