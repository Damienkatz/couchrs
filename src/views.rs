//! Map/reduce view indexes, a port of couch_mrview / couch_index.
//!
//! Each design doc's views form an index group stored in its own file named
//! by the group signature, so editing a ddoc builds a new index while an
//! identical ddoc (even under a different id) shares one. Indexes update
//! incrementally from the database's by-seq tree.

use crate::api::{QueryArgs, blocking};
use crate::btree::{Action, Btree, Dir, NodePtr, Range, Spec};
use crate::collate::{Json, cmp_json, cmp_json_raw};
use crate::db::{Db, Snapshot};
use crate::doc::{DocOpts, FullDocInfo};
use crate::error::{CouchError, Result};
use crate::file::CouchFile;
use crate::http::{Req, json_response, not_allowed, ok_json};
use crate::js::JsFuns;
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

    fn reduce_values(&self, kvs: &[(Value, Value)]) -> Option<Value> {
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

// --------------------------------------------------------- id btree

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IdEntry {
    /// (view index, emitted keys)
    pub keys: Vec<(u32, Vec<Json>)>,
}

pub struct IdIdxSpec;
impl Spec for IdIdxSpec {
    type K = String;
    type V = IdEntry;
    type R = u64;
    fn cmp(&self, a: &String, b: &String) -> Ordering {
        a.as_bytes().cmp(b.as_bytes())
    }
    fn reduce(&self, kvs: &[(String, IdEntry)]) -> u64 {
        kvs.len() as u64
    }
    fn rereduce(&self, reds: &[u64]) -> u64 {
        reds.iter().sum()
    }
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

// ----------------------------------------------------------- index state

#[derive(Serialize, Deserialize, Clone)]
struct IndexHeader {
    sig: String,
    seq: u64,
    purge_seq: u64,
    db_uuid: String,
    id_root: Option<NodePtr<u64>>,
    view_roots: Vec<Option<NodePtr<VRed>>>,
}

#[derive(Clone)]
pub struct IndexState {
    pub seq: u64,
    pub purge_seq: u64,
    pub ids: Btree<IdIdxSpec>,
    pub views: Vec<Btree<ViewSpec>>,
}

pub struct Index {
    pub def: IndexDef,
    pub path: PathBuf,
    file: RwLock<Arc<CouchFile>>,
    state: RwLock<IndexState>,
    update_lock: Mutex<()>,
    db_uuid: String,
    updater_running: std::sync::atomic::AtomicBool,
}

fn fresh_state(file: &Arc<CouchFile>, def: &IndexDef) -> IndexState {
    IndexState {
        seq: 0,
        purge_seq: 0,
        ids: Btree::new(file.clone(), None, Arc::new(IdIdxSpec)),
        views: def
            .views
            .iter()
            .map(|v| {
                Btree::new(
                    file.clone(),
                    None,
                    Arc::new(ViewSpec {
                        raw: v.raw,
                        reducer: v.reducer.clone(),
                    }),
                )
            })
            .collect(),
    }
}

impl Index {
    fn open(db: &Db, def: IndexDef, dir: PathBuf) -> Result<Index> {
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.view", def.sig));
        let db_uuid = db.snapshot().header.uuid.clone();
        let file = Arc::new(CouchFile::open(&path, true)?);
        let mut state = fresh_state(&file, &def);
        if let Some(h) = file.read_header::<IndexHeader>()? {
            if h.sig == def.sig && h.db_uuid == db_uuid && h.view_roots.len() == def.views.len() {
                state.seq = h.seq;
                state.purge_seq = h.purge_seq;
                state.ids = state.ids.with_root(h.id_root);
                for (t, r) in state.views.iter_mut().zip(h.view_roots) {
                    *t = t.with_root(r);
                }
            } else {
                file.truncate(0)?;
            }
        }
        Ok(Index {
            def,
            path,
            file: RwLock::new(file),
            state: RwLock::new(state),
            update_lock: Mutex::new(()),
            db_uuid,
            updater_running: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn state(&self) -> IndexState {
        self.state.read().clone()
    }

    fn commit(&self, st: &IndexState) -> Result<()> {
        let file = self.file.read().clone();
        let h = IndexHeader {
            sig: self.def.sig.clone(),
            seq: st.seq,
            purge_seq: st.purge_seq,
            db_uuid: self.db_uuid.clone(),
            id_root: st.ids.root.clone(),
            view_roots: st.views.iter().map(|v| v.root.clone()).collect(),
        };
        file.sync()?;
        file.write_header(&h)?;
        Ok(())
    }

    /// Map one document into per-view rows.
    fn map_doc(&self, snap: &Snapshot, fdi: &FullDocInfo) -> Result<Vec<Vec<(Value, Value)>>> {
        let n = self.def.views.len();
        if fdi.deleted || (fdi.id.starts_with("_design/") && !self.def.include_design) {
            return Ok(vec![Vec::new(); n]);
        }
        let opts = DocOpts {
            local_seq: self.def.local_seq,
            ..Default::default()
        };
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
                    if let Some(p) = partial
                        && !p.matches(&doc) {
                            out.push(Vec::new());
                            continue;
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

    /// Bring the index up to date with the database.
    pub fn update(&self, db: &Db) -> Result<IndexState> {
        let _g = self.update_lock.lock();
        let snap = db.snapshot();
        let mut st = self.state();
        if snap.header.purge_seq != st.purge_seq || st.seq > snap.update_seq() {
            // Purges (and db recreation) invalidate the index: rebuild.
            let file = self.file.read().clone();
            file.truncate(0)?;
            st = fresh_state(&file, &self.def);
            st.purge_seq = snap.header.purge_seq;
        }
        if st.seq >= snap.update_seq() {
            return Ok(st);
        }
        self.updater_running.store(true, std::sync::atomic::Ordering::Relaxed);
        let res = (|| -> Result<IndexState> {
            let mut batch: Vec<FullDocInfo> = Vec::new();
            let mut err = None;
            snap.fold_changes(st.seq, Dir::Fwd, |fdi| {
                batch.push(fdi.clone());
                if batch.len() >= 500
                    && let Err(e) = self.apply_batch(&snap, &mut st, std::mem::take(&mut batch)) {
                        err = Some(e);
                        return Ok(false);
                    }
                Ok(true)
            })?;
            if let Some(e) = err {
                return Err(e);
            }
            self.apply_batch(&snap, &mut st, batch)?;
            st.seq = snap.update_seq();
            self.commit(&st)?;
            *self.state.write() = st.clone();
            Ok(st)
        })();
        self.updater_running.store(false, std::sync::atomic::Ordering::Relaxed);
        res
    }

    fn apply_batch(&self, snap: &Snapshot, st: &mut IndexState, batch: Vec<FullDocInfo>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let nviews = self.def.views.len();
        let mut view_acts: Vec<Vec<Action<VKey, VVal>>> = (0..nviews).map(|_| Vec::new()).collect();
        let mut id_acts = Vec::new();
        let last_seq = batch.last().map(|f| f.update_seq).unwrap_or(st.seq);
        for fdi in batch {
            let old = st.ids.lookup(&fdi.id)?;
            if let Some(old) = &old {
                for (vi, keys) in &old.keys {
                    for k in keys {
                        view_acts[*vi as usize].push(Action::Remove((k.clone(), IdBound::Id(fdi.id.clone()))));
                    }
                }
            }
            let rows = self.map_doc(snap, &fdi)?;
            let mut entry = IdEntry { keys: Vec::new() };
            for (vi, vrows) in rows.into_iter().enumerate() {
                if vrows.is_empty() {
                    continue;
                }
                // Group duplicate keys from one doc.
                let spec = st.views[vi].spec.clone();
                let mut grouped: Vec<(Value, Vec<Json>)> = Vec::new();
                for (k, v) in vrows {
                    match grouped.iter_mut().find(|(gk, _)| spec.cmp_key(gk, &k) == Ordering::Equal) {
                        Some((_, vs)) => vs.push(Json(v)),
                        None => grouped.push((k, vec![Json(v)])),
                    }
                }
                let mut keys = Vec::new();
                for (k, mut vs) in grouped {
                    let val = if vs.len() == 1 { VVal::One(vs.pop().unwrap()) } else { VVal::Dups(vs) };
                    keys.push(Json(k.clone()));
                    view_acts[vi].push(Action::Insert((Json(k), IdBound::Id(fdi.id.clone())), val));
                }
                entry.keys.push((vi as u32, keys));
            }
            if entry.keys.is_empty() {
                if old.is_some() {
                    id_acts.push(Action::Remove(fdi.id.clone()));
                }
            } else {
                id_acts.push(Action::Insert(fdi.id.clone(), entry));
            }
        }
        st.ids = st.ids.modify(id_acts)?;
        for (vi, acts) in view_acts.into_iter().enumerate() {
            st.views[vi] = st.views[vi].modify(acts)?;
        }
        st.seq = last_seq;
        Ok(())
    }

    pub fn info(&self, name: &str) -> Value {
        let st = self.state();
        let file = self.file.read().clone();
        let active: u64 = st.ids.size() + st.views.iter().map(|v| v.size()).sum::<u64>();
        json!({
            "name": name.trim_start_matches("_design/"),
            "view_index": {
                "signature": self.def.sig,
                "language": self.def.language,
                "sizes": {"file": file.eof(), "external": 0, "active": active},
                "update_seq": st.seq,
                "purge_seq": st.purge_seq,
                "updater_running": self.updater_running.load(std::sync::atomic::Ordering::Relaxed),
                "compact_running": false,
                "waiting_clients": 0,
                "waiting_commit": false,
                "collator_versions": ["153.112"],
                "updates_pending": {"minimum": 0, "preferred": 0, "total": 0}
            }
        })
    }

    /// Rewrite the index file with only live data.
    pub fn compact(&self) -> Result<()> {
        let _g = self.update_lock.lock();
        let st = self.state();
        let tmp = self.path.with_extension("compact.view");
        let _ = std::fs::remove_file(&tmp);
        let nf = Arc::new(CouchFile::open(&tmp, true)?);
        let mut ns = fresh_state(&nf, &self.def);
        ns.seq = st.seq;
        ns.purge_seq = st.purge_seq;
        let mut acts = Vec::new();
        st.ids.fold(Dir::Fwd, &Range::default(), |k, v| {
            acts.push(Action::Insert(k.clone(), v.clone()));
            Ok(true)
        })?;
        ns.ids = ns.ids.modify(acts)?;
        for (i, v) in st.views.iter().enumerate() {
            let mut acts = Vec::new();
            v.fold(Dir::Fwd, &Range::default(), |k, val| {
                acts.push(Action::Insert(k.clone(), val.clone()));
                if acts.len() >= 10_000 {
                    // flushed below
                }
                Ok(true)
            })?;
            ns.views[i] = ns.views[i].modify(acts)?;
        }
        *self.file.write() = nf;
        self.commit(&ns)?;
        std::fs::rename(&tmp, &self.path)?;
        *self.state.write() = ns;
        Ok(())
    }
}

/// Find or open the index for a design doc.
pub fn get_index(db: &Arc<Db>, ddoc: &Value, index_dir: PathBuf) -> Result<Arc<Index>> {
    let def = IndexDef::from_ddoc(ddoc)?;
    let key = format!("mrview:{}", def.sig);
    if let Some(i) = db.ext.lock().get(&key).cloned()
        && let Ok(idx) = i.downcast::<Index>() {
            return Ok(idx);
        }
    let idx = Arc::new(Index::open(db, def, index_dir)?);
    db.ext.lock().insert(key, idx.clone());
    Ok(idx)
}

pub fn index_dir(req: &Req, db: &Db) -> PathBuf {
    req.server.index_dir(&db.name).join("mrview")
}

// ------------------------------------------------------------- querying

fn key_range(args: &QueryArgs, spec: &ViewSpec) -> Range<VKey> {
    let (start_id_default, end_id_default) = if args.descending {
        (IdBound::Max, IdBound::Min)
    } else {
        (IdBound::Min, IdBound::Max)
    };
    let start = args.start_key.as_ref().map(|k| {
        let id = args.start_docid.clone().map(IdBound::Id).unwrap_or(start_id_default.clone());
        (Json(k.clone()), id)
    });
    let end = args.end_key.as_ref().map(|k| {
        let id = match &args.end_docid {
            Some(d) => IdBound::Id(d.clone()),
            None if args.inclusive_end => end_id_default.clone(),
            // Exclusive end without a docid excludes every row of that key.
            None => if args.descending { IdBound::Max } else { IdBound::Min },
        };
        (Json(k.clone()), id)
    });
    let _ = spec;
    Range {
        start,
        end,
        inclusive_end: args.inclusive_end || args.end_docid.is_none(),
    }
}

fn group_key(args: &QueryArgs, key: &Value) -> Value {
    if let Some(l) = args.group_level {
        if let Value::Array(a) = key {
            return Value::Array(a.iter().take(l).cloned().collect());
        }
        return key.clone();
    }
    if args.group { key.clone() } else { Value::Null }
}

fn same_group(args: &QueryArgs, spec: &ViewSpec, a: &Value, b: &Value) -> bool {
    if let Some(l) = args.group_level {
        return match (a, b) {
            (Value::Array(x), Value::Array(y)) => {
                let (x, y) = (&x[..x.len().min(l)], &y[..y.len().min(l)]);
                x.len() == y.len() && x.iter().zip(y).all(|(p, q)| spec.cmp_key(p, q) == Ordering::Equal)
            }
            _ => spec.cmp_key(a, b) == Ordering::Equal,
        };
    }
    if args.group {
        spec.cmp_key(a, b) == Ordering::Equal
    } else {
        true
    }
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

pub fn query_view(snap: &Snapshot, st: &IndexState, def: &IndexDef, vi: usize, args: &QueryArgs) -> Result<ViewResult> {
    let tree = &st.views[vi];
    let vdef = &def.views[vi];
    let has_reduce = !matches!(vdef.reducer, Reducer::None);
    let reduce = args.reduce.unwrap_or(has_reduce);
    if reduce && !has_reduce
        && args.reduce == Some(true) {
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
    let dir = if args.descending { Dir::Rev } else { Dir::Fwd };
    let spec = tree.spec.clone();
    let total = tree.full_reduce().map(|r| r.count).unwrap_or(0);
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
        'outer: for a in &key_args {
            let range = key_range(a, &spec);
            let mut err = None;
            tree.fold_reduce(
                dir,
                &range,
                |x, y| same_group(args, &spec, &x.0.0, &y.0.0),
                |k, red| {
                    if skip > 0 {
                        skip -= 1;
                        return Ok(true);
                    }
                    if rows.len() >= args.limit {
                        return Ok(false);
                    }
                    let v = red.red.map(|j| finalize_red(&vdef.reducer, j.0)).unwrap_or(Value::Null);
                    if let Some(e) = v.get("error").filter(|_| v.get("reason").is_some()) {
                        err = Some(CouchError::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            e.as_str().unwrap_or("reduce_error"),
                            v["reason"].as_str().unwrap_or("").to_string(),
                        ));
                        return Ok(false);
                    }
                    rows.push(json!({"key": group_key(args, &k.0.0), "value": v}));
                    Ok(true)
                },
            )?;
            if let Some(e) = err {
                return Err(e);
            }
            if rows.len() >= args.limit {
                break 'outer;
            }
        }
        // An empty ungrouped reduce returns no rows, like CouchDB.
        return Ok(ViewResult {
            total_rows: total,
            offset: 0,
            rows,
            update_seq: st.seq,
            reduced: true,
        });
    }

    let mut offset = None;
    let mut skip = args.skip;
    for a in &key_args {
        let range = key_range(a, &spec);
        if offset.is_none() {
            offset = Some(match &range.start {
                Some(s) => tree.reduce_before(s, dir)?.map(|r| r.count).unwrap_or(0),
                None => 0,
            });
        }
        let mut err = None;
        tree.fold(dir, &range, |(k, id), v| {
            let id = match id {
                IdBound::Id(s) => s.clone(),
                _ => String::new(),
            };
            for val in v.values() {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                if rows.len() >= args.limit {
                    return Ok(false);
                }
                let mut row = json!({"id": id, "key": k.0, "value": val});
                if args.include_docs {
                    match row_doc(snap, &id, val, &opts) {
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
    Ok(ViewResult {
        total_rows: total,
        offset: offset.unwrap_or(0) + args.skip as u64,
        rows,
        update_seq: st.seq,
        reduced: false,
    })
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
        Ok(view_response(query_view(&snap, &st, &idx.def, vi, &args)?, &args))
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
            results.push(view_response(query_view(&snap, &st, &idx.def, vi, &args)?, &args));
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
            if let Some(sig) = name.strip_suffix(".view")
                && !live.contains(sig) {
                    let _ = std::fs::remove_file(e.path());
                    db.ext.lock().remove(&format!("mrview:{sig}"));
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
