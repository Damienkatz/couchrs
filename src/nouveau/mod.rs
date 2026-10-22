//! Nouveau full-text search (CouchDB's nouveau application). CouchDB
//! hands index updates and searches to a separate Java server running
//! Lucene; here that server's part is built in (index.rs, on tantivy) and
//! this module does the Erlang side's: index definitions in design
//! documents, running index functions over the changes, and the HTTP API
//! (_nouveau, _nouveau_info, _nouveau_analyze, _nouveau_cleanup).
//!
//! `[nouveau] enable = false` turns it off, as CouchDB ships it: its
//! endpoints are then not found.

pub mod analysis;
pub mod index;
pub mod mango;
pub mod query;
mod stopwords;

use crate::api::blocking;
use crate::db::{Db, Snapshot};
use crate::doc::{DocOpts, FullDocInfo};
use crate::error::{CouchError, Result};
use crate::http::{Req, json_response, not_allowed, ok_json};
use crate::replicator::rep::{Term, ejson};
use crate::server::Server;
use axum::http::{Method, StatusCode};
use axum::response::Response;
use base64::Engine;
use index::{Definition, FType, IndexField, SearchIndex, SearchRequest, Update, UpdateError};
use parking_lot::{Mutex, RwLock};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

pub const LEGACY_LUCENE_VERSION: i64 = 9;
pub const TARGET_LUCENE_VERSION: i64 = 10;
const BATCH_DOCS: usize = 256;

pub fn enabled(server: &Server) -> bool {
    server.config.get_bool("nouveau", "enable", true)
}

fn check_enabled(server: &Server) -> Result<()> {
    if enabled(server) { Ok(()) } else { Err(CouchError::not_found("missing")) }
}

/// One index a design document defines (nouveau.hrl's #index).
#[derive(Debug, Clone)]
pub struct IndexSpec {
    pub ddoc_id: String,
    pub name: String,
    pub lucene_version: Value,
    pub default_analyzer: Value,
    pub field_analyzers: Option<Value>,
    pub def: Value,
    pub def_lang: String,
    pub sig: String,
}

/// nouveau_util:design_doc_to_index/3.
pub fn design_doc_to_index(ddoc: &Value, name: &str) -> Result<IndexSpec> {
    let invalid = || CouchError::new(StatusCode::BAD_REQUEST, "invalid_design_doc", format!("index `{name}` must have parameter `index`"));
    let indexes = ddoc.get("nouveau").and_then(Value::as_object);
    let Some(entry) = indexes.and_then(|i| i.get(name)) else {
        return Err(CouchError::not_found(format!("{name} not found.")));
    };
    let Some(entry) = entry.as_object() else { return Err(invalid()) };
    let Some(def) = entry.get("index") else { return Err(invalid()) };
    let lucene_version = entry.get("lucene_version").cloned().unwrap_or(json!(LEGACY_LUCENE_VERSION));
    let default_analyzer = entry.get("default_analyzer").cloned().unwrap_or(json!("standard"));
    let field_analyzers = entry.get("field_analyzers").cloned();
    // The signature hashes what changes the index: as CouchDB computes it
    // (sha256 of the term), so it is the same as CouchDB's.
    let fa = field_analyzers.as_ref().map(ejson).unwrap_or(Term::Map(Vec::new()));
    let term = if lucene_version.as_i64() == Some(LEGACY_LUCENE_VERSION) {
        Term::Tuple(vec![ejson(&default_analyzer), fa, ejson(def)])
    } else {
        Term::Tuple(vec![ejson(&lucene_version), ejson(&default_analyzer), fa, ejson(def)])
    };
    Ok(IndexSpec {
        ddoc_id: ddoc.get("_id").and_then(Value::as_str).unwrap_or_default().to_string(),
        name: name.to_string(),
        lucene_version,
        default_analyzer,
        field_analyzers,
        def: def.clone(),
        def_lang: ddoc.get("language").and_then(Value::as_str).unwrap_or("javascript").to_string(),
        sig: hex::encode(Sha256::digest(term.encode())),
    })
}

/// nouveau_util:design_doc_to_indexes/2: the valid ones.
pub fn design_doc_to_indexes(ddoc: &Value) -> Vec<IndexSpec> {
    let Some(indexes) = ddoc.get("nouveau").and_then(Value::as_object) else { return Vec::new() };
    indexes.keys().filter_map(|n| design_doc_to_index(ddoc, n).ok()).collect()
}

/// nouveau_plugin_couch_db:before_doc_update/3: a new design document's
/// indexes get the current lucene_version unless they name one.
pub fn before_doc_update(doc: &mut crate::doc::Doc, interactive: bool) {
    if !interactive || !doc.is_design() || doc.revs.0 != 0 || !doc.revs.1.is_empty() {
        return;
    }
    if let Some(Value::Object(indexes)) = doc.body.get_mut("nouveau") {
        for idx in indexes.values_mut() {
            if let Value::Object(o) = idx
                && !o.contains_key("lucene_version") {
                    o.insert("lucene_version".into(), json!(TARGET_LUCENE_VERSION));
                }
        }
    }
}

// ------------------------------------------------------------- indexes

fn index_root(server: &Server, db: &str) -> PathBuf {
    server.index_dir(db).join("nouveau")
}

/// An index's place, its open index and the lock its updates take turns
/// on (nouveau_index_manager queues updaters per index the same way).
pub struct Handle {
    dir: PathBuf,
    idx: RwLock<Option<Arc<SearchIndex>>>,
    update: Mutex<()>,
}

impl Handle {
    fn get(&self) -> Result<Option<Arc<SearchIndex>>> {
        if let Some(i) = self.idx.read().clone() {
            return Ok(Some(i));
        }
        let mut slot = self.idx.write();
        if slot.is_none()
            && let Some(i) = SearchIndex::open(&self.dir)? {
                *slot = Some(Arc::new(i));
            }
        Ok(slot.clone())
    }

    /// A new, empty index in place of any there.
    fn create(&self, spec: &IndexSpec, purge_seq: u64, slots: usize) -> Result<Arc<SearchIndex>> {
        let def = definition(spec, slots)?;
        let mut slot = self.idx.write();
        *slot = None;
        let i = Arc::new(SearchIndex::create(&self.dir, def, purge_seq)?);
        *slot = Some(i.clone());
        Ok(i)
    }
}

fn handle(server: &Server, db: &Db, sig: &str) -> Arc<Handle> {
    let key = format!("nouveau:{sig}");
    let mut ext = db.ext.lock();
    if let Some(h) = ext.get(&key).cloned().and_then(|a| a.downcast::<Handle>().ok()) {
        return h;
    }
    let h = Arc::new(Handle { dir: index_root(server, &db.name).join(sig), idx: RwLock::new(None), update: Mutex::new(()) });
    ext.insert(key, h.clone());
    h
}

/// The definition the index is created from, checked as the Nouveau
/// server checks it (IndexDefinition, IndexResource.createIndex).
fn definition(spec: &IndexSpec, slots: usize) -> Result<Definition> {
    let version = match spec.lucene_version.as_i64() {
        Some(v @ LEGACY_LUCENE_VERSION..=TARGET_LUCENE_VERSION) => v,
        Some(v) if v > TARGET_LUCENE_VERSION => {
            return Err(CouchError::bad_request(format!("luceneVersion must be less than or equal to {TARGET_LUCENE_VERSION}")));
        }
        _ => return Err(CouchError::bad_request(format!("luceneVersion must be greater than or equal to {LEGACY_LUCENE_VERSION}"))),
    };
    if version != TARGET_LUCENE_VERSION {
        return Err(CouchError::bad_request(format!("Cannot create a new version {version} index")));
    }
    let default_analyzer = match &spec.default_analyzer {
        Value::String(s) if !s.is_empty() => s.clone(),
        _ => return Err(CouchError::bad_request("defaultAnalyzer must not be empty")),
    };
    let mut field_analyzers = std::collections::BTreeMap::new();
    if let Some(fa) = &spec.field_analyzers {
        let Some(o) = fa.as_object() else { return Err(CouchError::bad_request("field_analyzers must be an object")) };
        for (k, v) in o {
            let v = v.as_str().unwrap_or_default().to_string();
            if k.is_empty() || v.is_empty() {
                return Err(CouchError::bad_request("fieldAnalyzers must not be empty"));
            }
            field_analyzers.insert(k.clone(), v);
        }
    }
    for a in std::iter::once(&default_analyzer).chain(field_analyzers.values()) {
        if analysis::Analyzer::from_name(a).is_none() {
            return Err(CouchError::bad_request(format!("{a} is not a valid analyzer name")));
        }
    }
    Ok(Definition { lucene_version: version, default_analyzer, field_analyzers, slots })
}

/// nouveau_index_updater:update/1: bring the index up to date with the
/// database. Returns whether there was anything to do.
fn update(db: &Arc<Db>, h: &Handle, spec: &IndexSpec) -> Result<bool> {
    let _turn = h.update.lock();
    let snap = db.snapshot();
    let purge_seq = snap.header.purge_seq;
    let mut idx = match h.get()? {
        Some(i) => i,
        None => h.create(spec, purge_seq, index::INITIAL_SLOTS)?,
    };
    let (seq, ipurge) = idx.seqs();
    if ipurge != purge_seq || seq > snap.update_seq() {
        // Purged documents (or a database recreated) are found again by
        // building afresh.
        idx = h.create(spec, purge_seq, idx.def.slots)?;
    } else if seq == snap.update_seq() {
        return Ok(false);
    }
    loop {
        match run_updates(db, &snap, &idx, spec) {
            Ok(()) => return Ok(true),
            Err(UpdateError::SlotsFull) => {
                tracing::info!("search index {}/{} needs more fields: rebuilding", spec.ddoc_id, spec.name);
                idx = h.create(spec, purge_seq, idx.def.slots * 2)?;
            }
            Err(UpdateError::Bad(m)) => {
                idx.rollback();
                tracing::error!("nouveau: db:{} ddoc:{} index:{} failed with: {m}", db.name, spec.ddoc_id, spec.name);
                return Err(CouchError::bad_request(m));
            }
            Err(UpdateError::Other(e)) => {
                idx.rollback();
                tracing::error!("nouveau: db:{} ddoc:{} index:{} failed with: {}", db.name, spec.ddoc_id, spec.name, e.reason);
                return Err(e);
            }
        }
    }
}

fn run_updates(db: &Db, snap: &Snapshot, idx: &SearchIndex, spec: &IndexSpec) -> std::result::Result<(), UpdateError> {
    let (since, _) = idx.seqs();
    let total = if since == 0 { snap.header.doc_count + snap.header.del_count } else { snap.pending_after(since) };
    let task = crate::tasks::add(json!({
        "type": "search_indexer",
        "database": db.name,
        "design_document": spec.ddoc_id,
        "index": spec.name,
        "progress": 0,
        "changes_done": 0,
        "total_changes": total,
    }));
    // Mango's indexes (language "query") are indexed natively.
    let indexer = if spec.def_lang == "query" {
        Indexer::Mango(spec.def.clone())
    } else {
        Indexer::Js(crate::js::JsFuns::new(&Value::Null, &[spec.def.as_str().unwrap_or_default().to_string()], &[]))
    };
    let partitioned = snap.partitioned();
    let mut batch: Vec<FullDocInfo> = Vec::new();
    let mut err = None;
    let flush = |batch: Vec<FullDocInfo>| -> std::result::Result<(), UpdateError> {
        let n = batch.len() as u64;
        let updates = to_updates(snap, &indexer, partitioned, batch)?;
        idx.apply(&updates)?;
        task.add_changes(n);
        Ok(())
    };
    snap.fold_changes(since, crate::btree::Dir::Fwd, |fdi| {
        batch.push(fdi.clone());
        if batch.len() >= BATCH_DOCS
            && let Err(e) = flush(std::mem::take(&mut batch)) {
                err = Some(e);
                return Ok(false);
            }
        Ok(true)
    })
    .map_err(UpdateError::Other)?;
    if let Some(e) = err {
        return Err(e);
    }
    flush(batch)?;
    idx.set_seqs(snap.update_seq(), snap.header.purge_seq);
    idx.commit().map_err(UpdateError::Other)
}

/// What each changed document becomes in the index: its index
/// function's fields, or its removal. Design documents are skipped.
enum Indexer {
    Js(crate::js::JsFuns),
    Mango(Value),
}

impl Indexer {
    fn fields(&self, doc: &Value) -> Result<Vec<IndexField>> {
        match self {
            Indexer::Js(f) => Ok(f.index_doc(doc)?.into_iter().next().unwrap_or_default().iter().filter_map(index_field).collect()),
            Indexer::Mango(def) => mango::index_doc(def, doc),
        }
    }
}

fn to_updates(snap: &Snapshot, indexer: &Indexer, partitioned: bool, batch: Vec<FullDocInfo>) -> std::result::Result<Vec<Update>, UpdateError> {
    let one = |fdi: &FullDocInfo| -> std::result::Result<Option<Update>, UpdateError> {
        if fdi.id.starts_with("_design/") {
            return Ok(None);
        }
        if fdi.deleted {
            return Ok(Some(Update::Delete { id: fdi.id.clone() }));
        }
        let doc = snap.winner_json(fdi, &DocOpts::default()).map_err(UpdateError::Other)?;
        let fields = indexer.fields(&doc).map_err(UpdateError::Other)?;
        if fields.is_empty() {
            return Ok(Some(Update::Delete { id: fdi.id.clone() }));
        }
        let partition = if partitioned { crate::doc::partition_of(&fdi.id).map(String::from) } else { None };
        Ok(Some(Update::Upsert { id: fdi.id.clone(), partition, fields }))
    };
    // Index functions run in parallel over large batches.
    let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).min(batch.len() / 64).max(1);
    if threads == 1 {
        return Ok(batch.iter().map(one).collect::<std::result::Result<Vec<_>, _>>()?.into_iter().flatten().collect());
    }
    let chunk = batch.len().div_ceil(threads);
    let parts: Vec<std::result::Result<Vec<Option<Update>>, UpdateError>> = std::thread::scope(|s| {
        let hs: Vec<_> = batch.chunks(chunk).map(|c| s.spawn(move || c.iter().map(one).collect())).collect();
        hs.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(UpdateError::Other(CouchError::internal("index worker panicked"))))).collect()
    });
    let mut out = Vec::with_capacity(batch.len());
    for p in parts {
        out.extend(p?.into_iter().flatten());
    }
    Ok(out)
}

fn index_field(v: &Value) -> Option<IndexField> {
    let ty = match v.get("@type")?.as_str()? {
        "text" => FType::Text,
        "string" => FType::String,
        "double" => FType::Double,
        "stored" => FType::Stored,
        _ => return None,
    };
    Some(IndexField {
        ty,
        name: v.get("name")?.as_str()?.to_string(),
        value: v.get("value").cloned().unwrap_or(Value::Null),
        store: v.get("store").and_then(Value::as_bool).unwrap_or(false),
    })
}

/// Update one index in the background (ken). Errors are logged.
pub fn update_in_background(server: &Server, db: &Arc<Db>, spec: &IndexSpec) {
    if !enabled(server) {
        return;
    }
    let h = handle(server, db, &spec.sig);
    if let Err(e) = update(db, &h, spec) {
        tracing::debug!("background search index update {}/{}: {}", spec.ddoc_id, spec.name, e.reason);
    }
}

/// Whether an index is behind its database (nouveau_index_updater:outdated/1).
pub fn outdated(server: &Server, db: &Arc<Db>, spec: &IndexSpec) -> bool {
    let h = handle(server, db, &spec.sig);
    let snap = db.snapshot();
    match h.get() {
        Ok(Some(i)) => {
            let (seq, purge) = i.seqs();
            snap.update_seq() > seq || snap.header.purge_seq > purge
        }
        _ => true,
    }
}

// --------------------------------------------------------------- search

/// A query argument as it arrived: from the query string, or a JSON body.
#[derive(Debug, Clone)]
enum Arg {
    Qs(String),
    Json(Value),
}

fn query_parse_error(msg: impl Into<String>) -> CouchError {
    CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", msg)
}

/// nouveau_httpd's misspelt throw: an unknown error, so a 500.
fn query_parser_error(msg: impl Into<String>) -> CouchError {
    CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, "query_parser_error", msg)
}

/// How Erlang's ~p prints a value: strings from the query string are
/// lists ("x"), from JSON binaries (<<"x">>).
fn erl_p(a: &Arg) -> String {
    fn json(v: &Value) -> String {
        match v {
            Value::String(s) => format!("<<\"{s}\">>"),
            Value::Array(a) => format!("[{}]", a.iter().map(json).collect::<Vec<_>>().join(",")),
            Value::Object(o) => format!("#{{{}}}", o.iter().map(|(k, v)| format!("<<\"{k}\">> => {}", json(v))).collect::<Vec<_>>().join(",")),
            other => other.to_string(),
        }
    }
    match a {
        Arg::Qs(s) => format!("\"{s}\""),
        Arg::Json(v) => json(v),
    }
}

fn invalid(key: &str, a: &Arg) -> CouchError {
    query_parse_error(format!("Invalid value for {key}: {}", erl_p(a)))
}

fn decode(a: Arg) -> Result<Value> {
    match a {
        Arg::Qs(s) => serde_json::from_str(&s).map_err(|_| CouchError::bad_request("invalid UTF-8 JSON")),
        Arg::Json(v) => Ok(v),
    }
}

fn strings(name: &str, v: &Value) -> Result<Vec<String>> {
    match v {
        Value::Array(a) => a
            .iter()
            .map(|s| s.as_str().map(String::from).ok_or_else(|| query_parser_error(format!("all items in {name} parameter must be strings"))))
            .collect(),
        _ => Err(query_parser_error(format!("{name} parameter must be a list of strings"))),
    }
}

/// A search's arguments, checked as nouveau_httpd:validate_query_args/1
/// does (in its order: by key name).
#[derive(Debug, Clone)]
pub struct SearchArgs {
    pub bookmark: Option<Value>,
    pub counts: Option<Vec<String>>,
    pub include_docs: bool,
    pub limit: i64,
    pub locale: Option<String>,
    pub partition: Option<String>,
    pub query: String,
    pub ranges: Option<Vec<(String, Vec<index::RangeSpec>)>>,
    pub sort: Option<Vec<String>>,
    pub top_n: i64,
    pub update: bool,
}

fn int_arg(key: &str, a: Option<Arg>, default: i64) -> Result<i64> {
    match a {
        None => Ok(default),
        Some(Arg::Json(Value::Number(n))) if n.is_i64() || n.is_u64() => {
            let n = n.as_i64().unwrap_or(i64::MAX);
            if n > 0 { Ok(n) } else { Err(query_parse_error(format!("{key} parameter must be greater than zero"))) }
        }
        Some(Arg::Qs(s)) => s.parse::<i64>().map_err(|_| query_parse_error(format!("{key} parameter must be an integer"))),
        Some(a) => Err(invalid(key, &a)),
    }
}

fn bool_arg(key: &str, a: Option<Arg>, default: bool) -> Result<bool> {
    match a {
        None => Ok(default),
        Some(Arg::Json(Value::Bool(b))) => Ok(b),
        Some(Arg::Qs(s)) if s == "true" => Ok(true),
        Some(Arg::Qs(s)) if s == "false" => Ok(false),
        Some(a) => Err(invalid(key, &a)),
    }
}

fn str_arg(key: &str, a: Option<Arg>) -> Result<Option<String>> {
    match a {
        None => Ok(None),
        Some(Arg::Qs(s)) | Some(Arg::Json(Value::String(s))) => Ok(Some(s)),
        Some(a) => Err(invalid(key, &a)),
    }
}

fn range_number(v: Option<&Value>, default: f64) -> f64 {
    match v {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(default),
        Some(Value::String(s)) => s.trim().parse().unwrap_or(default),
        _ => default,
    }
}

impl SearchArgs {
    fn from(get: impl Fn(&str) -> Option<Arg>, partition: Option<String>) -> Result<SearchArgs> {
        let bookmark = get("bookmark").map(|a| match a {
            Arg::Qs(s) => Value::String(s),
            Arg::Json(v) => v,
        });
        let counts = match get("counts") {
            None => None,
            Some(a) => match decode(a.clone())? {
                v @ Value::Array(_) => Some(strings("counts", &v)?),
                _ => return Err(invalid("counts", &a)),
            },
        };
        let include_docs = bool_arg("include_docs", get("include_docs"), false)?;
        let limit = int_arg("limit", get("limit"), 25)?;
        let locale = str_arg("locale", get("locale"))?;
        let query = match get("q") {
            None => return Err(query_parse_error("q parameter is mandatory")),
            Some(Arg::Qs(s)) | Some(Arg::Json(Value::String(s))) => s,
            Some(a) => return Err(invalid("query", &a)),
        };
        let ranges = match get("ranges") {
            None => None,
            Some(a) => match decode(a.clone())? {
                Value::Object(o) => {
                    let mut out = Vec::new();
                    for (field, rs) in o {
                        let Value::Array(rs) = rs else { return Err(query_parse_error("range values must be lists of objects")) };
                        let mut specs = Vec::new();
                        for r in &rs {
                            let Value::Object(r) = r else { return Err(query_parser_error("all values in ranges parameter must be objects")) };
                            specs.push(index::RangeSpec {
                                label: r.get("label").and_then(Value::as_str).unwrap_or_default().to_string(),
                                min: range_number(r.get("min"), f64::NEG_INFINITY),
                                min_inclusive: r.get("min_inclusive").and_then(Value::as_bool).unwrap_or(true),
                                max: range_number(r.get("max"), f64::INFINITY),
                                max_inclusive: r.get("max_inclusive").and_then(Value::as_bool).unwrap_or(true),
                            });
                        }
                        out.push((field, specs));
                    }
                    Some(out)
                }
                _ => return Err(invalid("ranges", &a)),
            },
        };
        let sort = match get("sort") {
            None => None,
            Some(a) => match decode(a)? {
                Value::String(s) => Some(vec![s]),
                v => Some(strings("counts", &v)?),
            },
        };
        let top_n = int_arg("top_n", get("top_n"), 10)?;
        let update = bool_arg("update", get("update"), true)?;
        // What the Nouveau server's own checks (SearchRequest) refuse.
        let mut violations = Vec::new();
        if limit < 1 {
            violations.push("limit must be greater than 0".to_string());
        }
        if top_n < 1 {
            violations.push("topN must be greater than or equal to 1".to_string());
        } else if top_n > 1000 {
            violations.push("topN must be less than or equal to 1000".to_string());
        }
        if !violations.is_empty() {
            return Err(CouchError::bad_request(violations.join(" and ")));
        }
        Ok(SearchArgs { bookmark, counts, include_docs, limit, locale, partition, query, ranges, sort, top_n, update })
    }
}

/// b64url, as CouchDB's b64url module writes it (no padding).
fn b64url(b: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

/// nouveau_bookmark:unpack/2, for our one shard: the last hit's order.
fn unpack_bookmark(b: &Option<Value>) -> Result<Option<Vec<Value>>> {
    let s = match b {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::String(s)) if s == "nil" || s == "null" || s == "undefined" => return Ok(None),
        Some(Value::String(s)) => s.clone(),
        Some(_) => return Err(CouchError::bad_request("Invalid bookmark")),
    };
    let bad = || CouchError::bad_request("Invalid bookmark");
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s.trim_end_matches('=')).map_err(|_| bad())?;
    let v: Value = serde_json::from_slice(&bytes).map_err(|_| bad())?;
    let Value::Array(orders) = v else { return Err(bad()) };
    let mut last = None;
    for o in orders {
        let Value::Array(o) = o else { return Err(bad()) };
        if o.last().and_then(|l| l.get("@type")).and_then(Value::as_str) != Some("string") {
            return Err(bad());
        }
        last = Some(o);
    }
    Ok(last)
}

pub struct Searched {
    pub results: index::SearchResults,
    pub bookmark: Option<Vec<Value>>,
    pub update_latency: u64,
}

/// nouveau_fabric_search:go/4 and nouveau_rpc:search/3: update the
/// index (unless asked not to), then search it.
pub fn search(server: &Server, db: &Arc<Db>, spec: &IndexSpec, args: &SearchArgs) -> Result<Searched> {
    let after = unpack_bookmark(&args.bookmark)?;
    search_after(server, db, spec, args, after)
}

/// A search continuing after a hit's sort values.
pub fn search_after(server: &Server, db: &Arc<Db>, spec: &IndexSpec, args: &SearchArgs, after: Option<Vec<Value>>) -> Result<Searched> {
    let h = handle(server, db, &spec.sig);
    let mut update_latency = 0;
    if args.update {
        let t = Instant::now();
        if update(db, &h, spec)? {
            update_latency = (t.elapsed().as_micros() as u64).div_ceil(1000);
            crate::stat!("nouveau.update_latency").observe(update_latency as f64);
        }
    }
    let idx = h.get()?.ok_or_else(|| CouchError::not_found("Index does not exist"))?;
    let req = SearchRequest {
        query: args.query.clone(),
        locale: args.locale.clone(),
        partition: args.partition.clone(),
        limit: args.limit as usize,
        sort: args.sort.clone(),
        counts: args.counts.clone(),
        ranges: args.ranges.clone(),
        after: after.clone(),
        top_n: args.top_n as usize,
    };
    let results = idx.search(&req)?;
    let bookmark = results.hits.last().map(|h| h.order.clone()).or(after);
    Ok(Searched { results, bookmark, update_latency })
}

/// The packed bookmark for a search's results.
pub fn pack_bookmark(b: &Option<Vec<Value>>) -> String {
    let list: Vec<Value> = b.iter().map(|o| Value::Array(o.clone())).collect();
    b64url(&serde_json::to_vec(&list).unwrap_or_default())
}

/// A hit's stored fields, grouped by name (a lone value as itself).
pub fn hit_fields(fields: &[(String, Value)]) -> Map<String, Value> {
    let mut grouped: std::collections::BTreeMap<String, Vec<Value>> = std::collections::BTreeMap::new();
    for (k, v) in fields {
        grouped.entry(k.clone()).or_default().push(v.clone());
    }
    grouped.into_iter().map(|(k, mut v)| (k, if v.len() == 1 { v.pop().unwrap() } else { Value::Array(v) })).collect()
}

/// A document for include_docs, as all_docs with keys gives it: null for
/// a deleted one, not_found for none.
pub fn doc_for(snap: &Snapshot, id: &str) -> Result<Value> {
    match snap.get_fdi(id)? {
        None => Ok(json!("not_found")),
        Some(f) if f.deleted => Ok(Value::Null),
        Some(f) => {
            crate::db::count_reads(snap, 1);
            snap.winner_json(&f, &DocOpts::default())
        }
    }
}

// ------------------------------------------------------------------ HTTP

/// POST /_nouveau_analyze.
pub fn http_analyze(req: &Req) -> Result<Response> {
    if req.method != Method::POST {
        return Err(CouchError::new(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", "Only POST allowed"));
    }
    check_enabled(&req.server)?;
    validate_ctype(req)?;
    let body = req.json_obj_body()?;
    let (Some(Value::String(text)), Some(Value::String(analyzer))) = (body.get("text"), body.get("analyzer")) else {
        return Err(CouchError::bad_request("'text' and 'analyzer' fields must be non-empty strings"));
    };
    let mut violations = Vec::new();
    if analyzer.is_empty() {
        violations.push("analyzer must not be empty");
    }
    if text.is_empty() {
        violations.push("text must not be empty");
    }
    if !violations.is_empty() {
        return Err(CouchError::bad_request(violations.join(" and ")));
    }
    let a = analysis::Analyzer::from_name(analyzer).ok_or_else(|| CouchError::bad_request(format!("{analyzer} is not a valid analyzer name")))?;
    ok_json(json!({"tokens": a.token_texts(text)}))
}

/// couch_httpd:validate_ctype/2.
fn validate_ctype(req: &Req) -> Result<()> {
    let ct = req.content_type();
    if ct.split(';').next() == Some("application/json") {
        Ok(())
    } else {
        Err(CouchError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "bad_content_type", "Content-Type must be application/json"))
    }
}

fn open_ddoc(db: &Db, ddoc_id: &str) -> Result<Value> {
    let snap = db.snapshot();
    let fdi = snap.get_fdi(ddoc_id)?.filter(|f| !f.deleted).ok_or_else(|| CouchError::not_found("missing"))?;
    snap.winner_json(&fdi, &DocOpts::default())
}

/// GET|POST /db/_design/ddoc/_nouveau/index (and in a partition).
pub async fn http_search(req: &Req, db: &Arc<Db>, ddoc_id: &str, rest: &[&str], partition: Option<&str>) -> Result<Response> {
    check_enabled(&req.server)?;
    let ddoc = open_ddoc(db, ddoc_id)?;
    crate::stat!("nouveau.active_searches").inc();
    let t = Instant::now();
    let res = search_req(req, db, &ddoc, rest, partition).await;
    crate::stat!("nouveau.active_searches").dec();
    crate::stat!("nouveau.search_latency").observe(t.elapsed().as_millis() as f64);
    res
}

async fn search_req(req: &Req, db: &Arc<Db>, ddoc: &Value, rest: &[&str], partition: Option<&str>) -> Result<Response> {
    let [name] = rest else { return not_allowed("GET, POST") };
    let qs_partition = req.q("partition").map(String::from);
    if let (Some(p), Some(q)) = (partition, &qs_partition)
        && p != q {
            return Err(CouchError::bad_request("Conflicting value for `partition` in query string"));
        }
    let partition = partition.map(String::from).or(qs_partition);
    let args = match req.method {
        Method::GET => SearchArgs::from(|k| req.q(k).map(|v| Arg::Qs(v.to_string())), partition)?,
        Method::POST => {
            validate_ctype(req)?;
            let body = req.json_obj_body()?;
            SearchArgs::from(|k| body.get(k).map(|v| Arg::Json(v.clone())), partition)?
        }
        _ => return not_allowed("GET, POST"),
    };
    let spec = design_doc_to_index(ddoc, name)?;
    let server = req.server.clone();
    let db2 = db.clone();
    let args2 = args.clone();
    let found = blocking(move || search(&server, &db2, &spec, &args2))?;
    let snap = db.snapshot();
    let mut hits = Vec::with_capacity(found.results.hits.len());
    for h in &found.results.hits {
        let mut hit = Map::new();
        if args.include_docs {
            hit.insert("doc".into(), doc_for(&snap, &h.id)?);
        }
        hit.insert("fields".into(), Value::Object(hit_fields(&h.fields)));
        hit.insert("id".into(), json!(h.id));
        hit.insert("order".into(), Value::Array(h.order.clone()));
        hits.push(Value::Object(hit));
    }
    let empty = || Value::Object(Map::new());
    ok_json(json!({
        "bookmark": pack_bookmark(&found.bookmark),
        "counts": found.results.counts.clone().map(Value::Object).unwrap_or_else(empty),
        "hits": hits,
        "ranges": found.results.ranges.clone().map(Value::Object).unwrap_or_else(empty),
        "total_hits": found.results.total_hits,
        "total_hits_relation": "EQUAL_TO",
        "update_latency": found.update_latency,
    }))
}

/// GET /db/_design/ddoc/_nouveau_info/index.
pub fn http_info(req: &Req, db: &Arc<Db>, ddoc_id: &str, rest: &[&str]) -> Result<Response> {
    check_enabled(&req.server)?;
    let ddoc = open_ddoc(db, ddoc_id)?;
    let [name] = rest else { return Err(CouchError::bad_request("path not recognized")) };
    if req.method != Method::GET {
        return not_allowed("GET");
    }
    let spec = design_doc_to_index(&ddoc, name)?;
    let h = handle(&req.server, db, &spec.sig);
    let idx = h.get()?.ok_or_else(|| CouchError::not_found("Index does not exist"))?;
    let (update_seq, purge_seq) = idx.seqs();
    ok_json(json!({
        "name": format!("{ddoc_id}/{name}"),
        "search_index": {
            "disk_size": idx.disk_size(),
            "num_docs": idx.num_docs(),
            "purge_seq": purge_seq,
            "signature": spec.sig,
            "update_seq": update_seq,
        }
    }))
}

/// POST /db/_nouveau_cleanup: remove the indexes no design document
/// defines any more.
pub fn http_cleanup(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return Err(CouchError::new(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", "Only POST allowed"));
    }
    validate_ctype(req)?;
    cleanup(&req.server, db)?;
    Ok(json_response(StatusCode::ACCEPTED, &json!({"ok": true})))
}

pub fn cleanup(server: &Server, db: &Arc<Db>) -> Result<()> {
    let snap = db.snapshot();
    let live: std::collections::HashSet<String> =
        crate::views::all_ddocs(&snap)?.iter().flat_map(design_doc_to_indexes).map(|s| s.sig).collect();
    let root = index_root(server, &db.name);
    let Ok(dir) = std::fs::read_dir(&root) else { return Ok(()) };
    for e in dir.flatten() {
        let sig = e.file_name().to_string_lossy().to_string();
        if !live.contains(&sig) {
            if let Some(h) = db.ext.lock().remove(&format!("nouveau:{sig}"))
                && let Ok(h) = h.downcast::<Handle>() {
                    *h.idx.write() = None;
                }
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
    Ok(())
}

// -------------------------------------------------- dreyfus (Clouseau)

fn search_unavailable() -> CouchError {
    CouchError::new(StatusCode::SERVICE_UNAVAILABLE, "service unavailable", "Search is not available")
}

/// _search, _search_info and _search_disk_size: CouchDB's older text
/// search needs a Clouseau service, and without one answers like this.
pub fn http_dreyfus(req: &Req, db: &Db, ddoc_id: &str, kind: &str, rest: &[&str]) -> Result<Response> {
    open_ddoc(db, ddoc_id)?;
    let [_] = rest else { return Err(CouchError::bad_request("path not recognized")) };
    let allowed = if kind == "_search" { "GET,POST" } else { "GET" };
    let ok = match kind {
        "_search" => matches!(req.method, Method::GET | Method::POST),
        _ => req.method == Method::GET,
    };
    if !ok {
        return not_allowed(allowed);
    }
    Err(search_unavailable())
}

/// /_search_analyze.
pub fn http_search_analyze(req: &Req) -> Result<Response> {
    match req.method {
        Method::GET | Method::POST => Err(search_unavailable()),
        _ => not_allowed("GET,POST"),
    }
}

/// /db/_search_cleanup: nothing to clean.
pub fn http_search_cleanup(req: &Req) -> Result<Response> {
    if req.method != Method::POST {
        return Err(CouchError::new(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", "Only POST allowed"));
    }
    Ok(json_response(StatusCode::ACCEPTED, &json!({"ok": true})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signatures() {
        // An index as a new design document gets it (lucene_version 10).
        let ddoc = json!({"_id": "_design/foo", "nouveau": {"bar": {"index": "function(doc){}", "lucene_version": 10}}});
        let s = design_doc_to_index(&ddoc, "bar").unwrap();
        assert_eq!(s.sig.len(), 64);
        let legacy = json!({"_id": "_design/foo", "nouveau": {"bar": {"index": "function(doc){}"}}});
        assert_ne!(design_doc_to_index(&legacy, "bar").unwrap().sig, s.sig);
        assert_eq!(design_doc_to_index(&ddoc, "nope").unwrap_err().reason, "nope not found.");
    }

    #[test]
    fn bookmarks_round_trip() {
        let order = vec![json!({"@type": "float", "value": 1.0}), json!({"@type": "string", "value": "doc2"})];
        let packed = pack_bookmark(&Some(order.clone()));
        assert_eq!(unpack_bookmark(&Some(json!(packed))).unwrap(), Some(order));
        assert_eq!(pack_bookmark(&None), "W10");
    }
}
