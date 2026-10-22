//! Mango (/_find, /_index, /_explain): a port of mango_selector, mango_idx
//! and mango_cursor_view. JSON indexes are stored exactly as CouchDB stores
//! them, as design docs with `"language": "query"`, and are built by the
//! view engine.

use crate::api::{QueryArgs, blocking, save_docs};
use crate::btree::{Dir, Range};
use crate::collate::cmp_json;
use crate::db::{Db, Snapshot};
use crate::doc::{DocOpts, from_json};
use crate::error::{CouchError, Result};
use crate::http::{Req, json_response, not_allowed, ok_json};
use crate::views::{self, IdBound};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use base64::Engine;
use md5::{Digest, Md5};
use regex::Regex;
use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Instant;

// ------------------------------------------------------------- selector

#[derive(Clone, Debug)]
pub enum Sel {
    And(Vec<Sel>),
    Or(Vec<Sel>),
    Nor(Vec<Sel>),
    Not(Box<Sel>),
    Field(String, Cond),
}

#[derive(Clone, Debug)]
pub enum Cond {
    Eq(Value),
    Ne(Value),
    Lt(Value),
    Lte(Value),
    Gt(Value),
    Gte(Value),
    In(Vec<Value>),
    Nin(Vec<Value>),
    Exists(bool),
    Type(String),
    Size(u64),
    Mod(i64, i64),
    Regex(Regex),
    All(Vec<Value>),
    ElemMatch(Box<Sel>),
    AllMatch(Box<Sel>),
    KeyMapMatch(Box<Sel>),
    BeginsWith(String),
}

#[derive(Clone, Debug)]
pub struct Selector {
    pub root: Sel,
    pub raw: Value,
}

fn invalid(msg: impl Into<String>) -> CouchError {
    CouchError::new(StatusCode::BAD_REQUEST, "invalid_operator", msg)
}

fn join_path(prefix: &str, f: &str) -> String {
    if prefix.is_empty() { f.to_string() } else { format!("{prefix}.{f}") }
}

fn array_of(v: &Value, op: &str) -> Result<Vec<Value>> {
    v.as_array().cloned().ok_or_else(|| invalid(format!("Argument for {op} must be an array")))
}

fn parse_obj(obj: &Map<String, Value>, field: &str) -> Result<Sel> {
    let mut parts = Vec::new();
    for (k, v) in obj {
        parts.push(parse_kv(k, v, field)?);
    }
    Ok(if parts.len() == 1 { parts.pop().unwrap() } else { Sel::And(parts) })
}

fn parse_list(v: &Value, op: &str, field: &str) -> Result<Vec<Sel>> {
    let arr = v.as_array().ok_or_else(|| invalid(format!("Invalid argument for {op}: must be an array")))?;
    arr.iter()
        .map(|s| match s {
            Value::Object(o) => parse_obj(o, field),
            _ => Err(invalid(format!("Invalid argument for {op}"))),
        })
        .collect()
}

fn parse_kv(k: &str, v: &Value, field: &str) -> Result<Sel> {
    if let Some(op) = k.strip_prefix('$') {
        let cond = |c: Cond| Ok(Sel::Field(field.to_string(), c));
        return match op {
            "and" => Ok(Sel::And(parse_list(v, k, field)?)),
            "or" => Ok(Sel::Or(parse_list(v, k, field)?)),
            "nor" => Ok(Sel::Nor(parse_list(v, k, field)?)),
            "not" => match v {
                Value::Object(o) => Ok(Sel::Not(Box::new(parse_obj(o, field)?))),
                _ => Err(invalid("$not argument must be an object")),
            },
            "eq" => cond(Cond::Eq(v.clone())),
            "ne" => cond(Cond::Ne(v.clone())),
            "lt" => cond(Cond::Lt(v.clone())),
            "lte" => cond(Cond::Lte(v.clone())),
            "gt" => cond(Cond::Gt(v.clone())),
            "gte" => cond(Cond::Gte(v.clone())),
            "in" => cond(Cond::In(array_of(v, k)?)),
            "nin" => cond(Cond::Nin(array_of(v, k)?)),
            "all" => cond(Cond::All(array_of(v, k)?)),
            "exists" => cond(Cond::Exists(v.as_bool().ok_or_else(|| invalid("Bad argument for operator $exists"))?)),
            "type" => {
                let t = v.as_str().ok_or_else(|| invalid("Bad argument for operator $type"))?;
                if !["null", "boolean", "number", "string", "array", "object"].contains(&t) {
                    return Err(invalid(format!("Invalid type for $type: {t}")));
                }
                cond(Cond::Type(t.to_string()))
            }
            "size" => cond(Cond::Size(v.as_u64().ok_or_else(|| invalid("Bad argument for operator $size"))?)),
            "mod" => {
                let a = array_of(v, k)?;
                match (a.first().and_then(|x| x.as_i64()), a.get(1).and_then(|x| x.as_i64()), a.len()) {
                    (Some(d), Some(r), 2) if d != 0 => cond(Cond::Mod(d, r)),
                    _ => Err(invalid("Bad argument for operator $mod")),
                }
            }
            "regex" => {
                let s = v.as_str().ok_or_else(|| invalid("Bad argument for operator $regex"))?;
                let re = Regex::new(s).map_err(|_| invalid(format!("Invalid regular expression: {s}")))?;
                cond(Cond::Regex(re))
            }
            "beginsWith" => cond(Cond::BeginsWith(
                v.as_str().ok_or_else(|| invalid("Bad argument for operator $beginsWith"))?.to_string(),
            )),
            "elemMatch" | "allMatch" | "keyMapMatch" => {
                let o = v.as_object().ok_or_else(|| invalid(format!("Bad argument for operator {k}")))?;
                let inner = Box::new(parse_obj(o, "")?);
                cond(match op {
                    "elemMatch" => Cond::ElemMatch(inner),
                    "allMatch" => Cond::AllMatch(inner),
                    _ => Cond::KeyMapMatch(inner),
                })
            }
            // A text index did this matching (or, with none, every
            // document matches, as in mango_selector:match/3).
            "text" | "default" => Ok(Sel::And(vec![])),
            _ => Err(invalid(format!("Invalid operator: {k}"))),
        };
    }
    let path = join_path(field, k);
    match v {
        Value::Object(o) if !o.is_empty() && o.keys().all(|x| x.starts_with('$')) => parse_obj(o, &path),
        Value::Object(o) if !o.is_empty() && o.keys().any(|x| x.starts_with('$')) => Err(invalid(
            "Cannot mix operators and field names in the same object",
        )),
        Value::Object(o) if !o.is_empty() => parse_obj(o, &path),
        _ => Ok(Sel::Field(path, Cond::Eq(v.clone()))),
    }
}

impl Selector {
    pub fn parse(v: &Value) -> Result<Selector> {
        let obj = v
            .as_object()
            .ok_or_else(|| CouchError::bad_request("Selector must be a JSON object"))?;
        let root = if obj.is_empty() { Sel::And(vec![]) } else { parse_obj(obj, "")? };
        Ok(Selector { root, raw: v.clone() })
    }

    pub fn matches(&self, doc: &Value) -> bool {
        match_sel(&self.root, doc)
    }

    /// Fields that must exist for any matching document (used to decide
    /// whether a JSON index — which omits docs lacking a field — is safe).
    pub fn required_fields(&self) -> Vec<String> {
        let mut out = Vec::new();
        required(&self.root, &mut out);
        out
    }

    /// Conditions on a field that appear at top level (under $and only).
    pub fn top_conds(&self, field: &str) -> Vec<&Cond> {
        let mut out = Vec::new();
        top_conds(&self.root, field, &mut out);
        out
    }
}

fn required(s: &Sel, out: &mut Vec<String>) {
    match s {
        Sel::And(v) => v.iter().for_each(|x| required(x, out)),
        Sel::Field(f, c) => {
            let needs = !matches!(c, Cond::Exists(false));
            if needs && !out.contains(f) {
                out.push(f.clone());
            }
        }
        Sel::Or(v) => {
            // A field is required if every branch requires it.
            let sets: Vec<Vec<String>> = v
                .iter()
                .map(|x| {
                    let mut o = Vec::new();
                    required(x, &mut o);
                    o
                })
                .collect();
            if let Some(first) = sets.first() {
                for f in first {
                    if sets.iter().all(|s| s.contains(f)) && !out.contains(f) {
                        out.push(f.clone());
                    }
                }
            }
        }
        _ => {}
    }
}

fn top_conds<'a>(s: &'a Sel, field: &str, out: &mut Vec<&'a Cond>) {
    match s {
        Sel::And(v) => v.iter().for_each(|x| top_conds(x, field, out)),
        Sel::Field(f, c) if f == field => out.push(c),
        _ => {}
    }
}

/// Split a mango field path on unescaped dots.
pub fn split_path(path: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut chars = path.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' if chars.peek() == Some(&'.') => {
                cur.push('.');
                chars.next();
            }
            '.' => parts.push(std::mem::take(&mut cur)),
            c => cur.push(c),
        }
    }
    parts.push(cur);
    parts
}

pub fn get_field<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(doc);
    }
    let mut cur = doc;
    for p in split_path(path) {
        cur = match cur {
            Value::Object(o) => o.get(&p)?,
            Value::Array(a) => a.get(p.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn eq(a: &Value, b: &Value) -> bool {
    cmp_json(a, b) == Ordering::Equal
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn match_sel(s: &Sel, doc: &Value) -> bool {
    match s {
        Sel::And(v) => v.iter().all(|x| match_sel(x, doc)),
        Sel::Or(v) => v.iter().any(|x| match_sel(x, doc)),
        Sel::Nor(v) => !v.iter().any(|x| match_sel(x, doc)),
        Sel::Not(x) => !match_sel(x, doc),
        Sel::Field(f, c) => match get_field(doc, f) {
            None => matches!(c, Cond::Exists(false)),
            Some(v) => match_cond(c, v),
        },
    }
}

fn match_cond(c: &Cond, v: &Value) -> bool {
    match c {
        Cond::Eq(a) => eq(v, a),
        Cond::Ne(a) => !eq(v, a),
        Cond::Lt(a) => cmp_json(v, a) == Ordering::Less,
        Cond::Lte(a) => cmp_json(v, a) != Ordering::Greater,
        Cond::Gt(a) => cmp_json(v, a) == Ordering::Greater,
        Cond::Gte(a) => cmp_json(v, a) != Ordering::Less,
        Cond::In(args) => match v {
            Value::Array(items) => args.iter().any(|a| eq(v, a) || items.iter().any(|i| eq(i, a))),
            _ => args.iter().any(|a| eq(v, a)),
        },
        Cond::Nin(args) => !match_cond(&Cond::In(args.clone()), v),
        Cond::Exists(e) => *e,
        Cond::Type(t) => type_name(v) == t,
        Cond::Size(n) => v.as_array().map(|a| a.len() as u64 == *n).unwrap_or(false),
        Cond::Mod(d, r) => v.as_i64().map(|x| x % d == *r).unwrap_or(false),
        Cond::Regex(re) => v.as_str().map(|s| re.is_match(s)).unwrap_or(false),
        Cond::All(args) => match v {
            Value::Array(items) => args.iter().all(|a| items.iter().any(|i| eq(i, a))),
            _ => false,
        },
        Cond::ElemMatch(sel) => v.as_array().map(|a| a.iter().any(|e| match_sel(sel, e))).unwrap_or(false),
        Cond::AllMatch(sel) => v
            .as_array()
            .map(|a| !a.is_empty() && a.iter().all(|e| match_sel(sel, e)))
            .unwrap_or(false),
        Cond::KeyMapMatch(sel) => v
            .as_object()
            .map(|o| o.keys().any(|k| match_sel(sel, &json!(k))))
            .unwrap_or(false),
        Cond::BeginsWith(p) => v.as_str().map(|s| s.starts_with(p.as_str())).unwrap_or(false),
    }
}

// --------------------------------------------------------------- indexes

#[derive(Clone, Debug)]
pub struct MangoIndex {
    pub ddoc: Option<String>,
    pub name: String,
    pub kind: String,
    /// (field, ascending)
    pub fields: Vec<(String, bool)>,
    pub partial: Option<Value>,
    /// The index lives in a partitioned design doc (rows per partition).
    pub partitioned: bool,
    /// A nouveau index's definition, as stored.
    pub def: Option<Value>,
}

impl MangoIndex {
    fn all_docs() -> MangoIndex {
        MangoIndex {
            ddoc: None,
            name: "_all_docs".into(),
            kind: "special".into(),
            fields: vec![("_id".into(), true)],
            partial: None,
            partitioned: false,
            def: None,
        }
    }

    fn to_json(&self) -> Value {
        let fields: Vec<Value> = self
            .fields
            .iter()
            .map(|(f, asc)| json!({ f.clone(): if *asc { "asc" } else { "desc" } }))
            .collect();
        let mut def = json!({"fields": fields});
        if let Some(p) = &self.partial {
            def["partial_filter_selector"] = p.clone();
        }
        if self.kind == "special" {
            return json!({"ddoc": self.ddoc, "name": self.name, "type": self.kind, "def": def});
        }
        if let Some(d) = &self.def {
            def = crate::nouveau::mango::def_to_json(d);
        }
        json!({"ddoc": self.ddoc, "name": self.name, "type": self.kind, "partitioned": self.partitioned, "def": def})
    }
}

fn parse_sort_fields(v: &Value) -> Result<Vec<(String, bool)>> {
    let arr = v.as_array().ok_or_else(|| CouchError::bad_request("fields must be an array"))?;
    let mut out = Vec::new();
    for f in arr {
        match f {
            Value::String(s) => out.push((s.clone(), true)),
            Value::Object(o) if o.len() == 1 => {
                let (k, d) = o.iter().next().unwrap();
                let asc = match d.as_str() {
                    Some("asc") => true,
                    Some("desc") => false,
                    _ => return Err(CouchError::bad_request("Invalid sort direction")),
                };
                out.push((k.clone(), asc));
            }
            _ => return Err(CouchError::bad_request("Invalid sort field")),
        }
    }
    Ok(out)
}

/// The indexes Mango can use: _all_docs, then each query design doc's
/// (nouveau ones only while nouveau is enabled).
pub fn list_indexes(snap: &Snapshot, nouveau: bool) -> Result<Vec<MangoIndex>> {
    let mut out = vec![MangoIndex::all_docs()];
    for d in views::all_ddocs(snap)? {
        if d.get("language").and_then(|l| l.as_str()) != Some("query") {
            continue;
        }
        let id = d["_id"].as_str().unwrap_or("").to_string();
        let partitioned = match d.get("options").and_then(|o| o.get("partitioned")) {
            Some(Value::Bool(b)) => *b,
            _ => snap.partitioned(),
        };
        for (name, entry) in d.get("nouveau").and_then(Value::as_object).into_iter().flatten().filter(|_| nouveau) {
            let Some(def) = entry.get("index").filter(|i| crate::nouveau::mango::validate_def(i).is_ok()) else { continue };
            out.push(MangoIndex {
                ddoc: Some(id.clone()),
                name: name.clone(),
                kind: "nouveau".into(),
                fields: Vec::new(),
                partial: crate::nouveau::mango::partial_filter(def),
                partitioned,
                def: Some(def.clone()),
            });
        }
        let Some(vs) = d.get("views").and_then(|v| v.as_object()) else { continue };
        for (name, v) in vs {
            let fields = v
                .get("options")
                .and_then(|o| o.get("def"))
                .and_then(|d| d.get("fields"))
                .or_else(|| v.get("map").and_then(|m| m.get("fields")))
                .cloned()
                .unwrap_or(json!([]));
            let fields = match fields {
                Value::Object(o) => o
                    .iter()
                    .map(|(k, d)| (k.clone(), d.as_str() != Some("desc")))
                    .collect(),
                other => parse_sort_fields(&other).unwrap_or_default(),
            };
            let partial = v
                .get("map")
                .and_then(|m| m.get("partial_filter_selector"))
                .filter(|p| p.as_object().map(|o| !o.is_empty()).unwrap_or(false))
                .cloned();
            out.push(MangoIndex {
                ddoc: Some(id.clone()),
                name: name.clone(),
                kind: "json".into(),
                fields,
                partial,
                partitioned,
                def: None,
            });
        }
    }
    Ok(out)
}

pub async fn http_index(req: &Req, db: &Arc<Db>) -> Result<Response> {
    match req.method {
        Method::GET => {
            let snap = db.snapshot();
            let idx = blocking(|| list_indexes(&snap, crate::nouveau::enabled(&req.server)))?;
            let skip = req.q_u64("skip")?.unwrap_or(0) as usize;
            let limit = req.q_u64("limit")?.map(|l| l as usize).unwrap_or(usize::MAX);
            let total = idx.len();
            let list: Vec<Value> = idx.iter().skip(skip).take(limit).map(|i| i.to_json()).collect();
            ok_json(json!({"total_rows": total, "indexes": list}))
        }
        Method::POST => create_index(req, db).await,
        _ => not_allowed("GET,POST"),
    }
}

async fn create_index(req: &Req, db: &Arc<Db>) -> Result<Response> {
    let body = req.json_obj_body()?;
    let kind = body.get("type").and_then(|t| t.as_str()).unwrap_or("json");
    match kind {
        "json" => {}
        "nouveau" if crate::nouveau::enabled(&req.server) => return create_nouveau_index(req, db, &body).await,
        // Text indexes need a dreyfus (Clouseau) service, here as in a
        // CouchDB without one.
        "text" | "nouveau" => {
            return Err(CouchError::new(StatusCode::SERVICE_UNAVAILABLE, "required index service unavailable", kind));
        }
        _ => return Err(CouchError::new(StatusCode::BAD_REQUEST, "invalid_index", format!("Invalid type for index: {kind}"))),
    }
    let index = body
        .get("index")
        .and_then(|i| i.as_object())
        .ok_or_else(|| CouchError::bad_request("Missing required key: index"))?;
    let fields = parse_sort_fields(index.get("fields").ok_or_else(|| CouchError::bad_request("Missing required key: fields"))?)?;
    if fields.is_empty() {
        return Err(CouchError::bad_request("Index fields must not be empty"));
    }
    let partial = index
        .get("partial_filter_selector")
        .cloned()
        .or_else(|| body.get("partial_filter_selector").cloned());
    if let Some(p) = &partial {
        Selector::parse(p)?;
    }
    let fields_obj: Map<String, Value> = fields
        .iter()
        .map(|(f, asc)| (f.clone(), json!(if *asc { "asc" } else { "desc" })))
        .collect();
    let def_fields: Vec<Value> = fields.iter().map(|(f, asc)| json!({ f.clone(): if *asc { "asc" } else { "desc" } })).collect();
    let mut def = json!({"fields": def_fields});
    let mut map = json!({"fields": fields_obj});
    if let Some(p) = &partial {
        map["partial_filter_selector"] = p.clone();
        def["partial_filter_selector"] = p.clone();
    } else {
        map["partial_filter_selector"] = json!({});
    }
    let snap = db.snapshot();
    let partitioned = match body.get("partitioned") {
        None | Some(Value::Null) => snap.partitioned(),
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err(CouchError::bad_request("`partitioned` must be a boolean")),
    };
    // Partitioned and global indexes live in different design docs.
    let hash_src = if snap.partitioned() { json!([def, "json", partitioned]) } else { json!([def, "json"]) };
    let hash = hex::encode(Md5::digest(serde_json::to_vec(&hash_src)?));
    let ddoc_id = match body.get("ddoc").and_then(|d| d.as_str()) {
        Some(d) if d.starts_with("_design/") => d.to_string(),
        Some(d) => format!("_design/{d}"),
        None => format!("_design/{hash}"),
    };
    let name = body.get("name").and_then(|n| n.as_str()).unwrap_or(&hash).to_string();
    let view = json!({"map": map, "reduce": "_count", "options": {"def": def}});

    let existing = snap.get_fdi(&ddoc_id)?;
    let mut doc_json = match existing.as_ref().filter(|f| !f.deleted) {
        Some(f) => snap.winner_json(f, &DocOpts::default())?,
        None if snap.partitioned() || partitioned => {
            json!({"_id": ddoc_id, "language": "query", "views": {}, "options": {"partitioned": partitioned}})
        }
        None => json!({"_id": ddoc_id, "language": "query", "views": {}}),
    };
    if doc_json.get("language").and_then(|l| l.as_str()) != Some("query") {
        return Err(CouchError::bad_request(format!("Design document {ddoc_id} is not a query index design doc")));
    }
    if let Some(v) = doc_json["views"].get(&name)
        && v.get("options").and_then(|o| o.get("def")) == view.get("options").and_then(|o| o.get("def")) {
            return ok_json(json!({"result": "exists", "id": ddoc_id, "name": name}));
        }
    doc_json["views"][&name] = view;
    let doc = from_json(doc_json)?;
    save_docs(req, db, vec![doc], true).await?.pop().unwrap()?;
    ok_json(json!({"result": "created", "id": ddoc_id, "name": name}))
}

/// A nouveau index (mango_idx_nouveau): its definition kept in the
/// design document's "nouveau" section, indexed by Nouveau natively.
async fn create_nouveau_index(req: &Req, db: &Arc<Db>, body: &Map<String, Value>) -> Result<Response> {
    let index = body.get("index").ok_or_else(|| CouchError::bad_request("Missing required key: index"))?;
    let def = crate::nouveau::mango::validate_def(index)?;
    if def["fields"] == json!("all_fields") {
        match req.server.config.get("mango", "index_all_disabled").as_deref() {
            Some("true") => {
                return Err(CouchError::new(StatusCode::FORBIDDEN, "index_all_disabled", "New text indexes are forbidden to index all fields."));
            }
            Some("warn") => tracing::warn!("User {:?} is indexing all fields in db {}", req.auth.user.name, db.name),
            _ => {}
        }
    }
    let snap = db.snapshot();
    let partitioned = match body.get("partitioned") {
        None | Some(Value::Null) => snap.partitioned(),
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err(CouchError::bad_request("`partitioned` must be a boolean")),
    };
    let hash_src = if snap.partitioned() { json!([def, "nouveau", partitioned]) } else { json!([def, "nouveau"]) };
    let hash = hex::encode(Md5::digest(serde_json::to_vec(&hash_src)?));
    let ddoc_id = match body.get("ddoc").and_then(|d| d.as_str()) {
        Some(d) if d.starts_with("_design/") => d.to_string(),
        Some(d) => format!("_design/{d}"),
        None => format!("_design/{hash}"),
    };
    let name = body.get("name").and_then(|n| n.as_str()).unwrap_or(&hash).to_string();
    let existing = snap.get_fdi(&ddoc_id)?;
    let mut doc_json = match existing.as_ref().filter(|f| !f.deleted) {
        Some(f) => snap.winner_json(f, &DocOpts::default())?,
        None if snap.partitioned() || partitioned => json!({"_id": ddoc_id, "language": "query", "options": {"partitioned": partitioned}}),
        None => json!({"_id": ddoc_id, "language": "query"}),
    };
    if doc_json.get("language").and_then(|l| l.as_str()) != Some("query") {
        return Err(CouchError::bad_request(format!("Design document {ddoc_id} is not a query index design doc")));
    }
    if doc_json.get("nouveau").and_then(|n| n.get(&name)).and_then(|e| e.get("index")) == Some(&def) {
        return ok_json(json!({"result": "exists", "id": ddoc_id, "name": name}));
    }
    if !doc_json.get("nouveau").is_some_and(Value::is_object) {
        doc_json["nouveau"] = json!({});
    }
    doc_json["nouveau"][&name] = crate::nouveau::mango::ddoc_entry(&def);
    let doc = from_json(doc_json)?;
    save_docs(req, db, vec![doc], true).await?.pop().unwrap()?;
    ok_json(json!({"result": "created", "id": ddoc_id, "name": name}))
}

pub async fn http_delete_index(req: &Req, db: &Arc<Db>, ddoc: &str, kind: &str, name: &str) -> Result<Response> {
    if req.method != Method::DELETE {
        return not_allowed("DELETE");
    }
    let section = match kind {
        "json" => "views",
        "nouveau" if crate::nouveau::enabled(&req.server) => "nouveau",
        _ => return Err(CouchError::not_found("Index not found")),
    };
    delete_index(req, db, ddoc, section, name).await?;
    ok_json(json!({"ok": true}))
}

async fn delete_index(req: &Req, db: &Arc<Db>, ddoc: &str, section: &str, name: &str) -> Result<()> {
    let id = if ddoc.starts_with("_design/") { ddoc.to_string() } else { format!("_design/{ddoc}") };
    let snap = db.snapshot();
    let fdi = snap.get_fdi(&id)?.filter(|f| !f.deleted).ok_or_else(|| CouchError::not_found("Index not found"))?;
    let mut d = snap.winner_json(&fdi, &DocOpts::default())?;
    let removed = d.get_mut(section).and_then(|v| v.as_object_mut()).and_then(|v| v.remove(name));
    if removed.is_none() {
        return Err(CouchError::not_found("Index not found"));
    }
    if d[section].as_object().is_some_and(|v| v.is_empty()) && section == "nouveau" {
        d.as_object_mut().map(|o| o.remove("nouveau"));
    }
    let empty = |k: &str| d.get(k).and_then(Value::as_object).is_none_or(|v| v.is_empty());
    if empty("views") && empty("nouveau") {
        d = json!({"_id": id, "_rev": d["_rev"], "_deleted": true});
    }
    let doc = from_json(d)?;
    save_docs(req, db, vec![doc], true).await?.pop().unwrap()?;
    Ok(())
}

pub async fn http_index_bulk_delete(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let body = req.json_obj_body()?;
    let docids = body.get("docids").and_then(|d| d.as_array()).cloned().unwrap_or_default();
    let mut success = Vec::new();
    let mut fail = Vec::new();
    let snap = db.snapshot();
    for d in docids {
        let Some(id) = d.as_str() else { continue };
        match snap.get_fdi(id)?.filter(|f| !f.deleted) {
            Some(f) => {
                let doc = from_json(json!({"_id": id, "_rev": f.winner().rev(), "_deleted": true}))?;
                match save_docs(req, db, vec![doc], true).await?.pop().unwrap() {
                    Ok(_) => success.push(json!({"id": id, "ok": true})),
                    Err(e) => fail.push(json!({"id": id, "error": e.error})),
                }
            }
            None => fail.push(json!({"id": id, "error": "not_found"})),
        }
    }
    ok_json(json!({"success": success, "fail": fail}))
}

// ----------------------------------------------------------------- query

pub struct FindQuery {
    pub selector: Selector,
    pub fields: Option<Vec<String>>,
    pub sort: Vec<(String, bool)>,
    pub limit: usize,
    pub skip: usize,
    pub use_index: Option<(String, Option<String>)>,
    pub bookmark: Option<Value>,
    pub update: bool,
    pub conflicts: bool,
    pub execution_stats: bool,
    pub stable: bool,
    /// Set for /db/_partition/P/_find.
    pub partition: Option<String>,
}

fn parse_find(body: &Map<String, Value>) -> Result<FindQuery> {
    let selector = Selector::parse(
        body.get("selector").ok_or_else(|| CouchError::bad_request("Missing required key: selector"))?,
    )?;
    let fields = match body.get("fields") {
        Some(Value::Array(a)) => Some(a.iter().filter_map(|f| f.as_str().map(String::from)).collect()),
        Some(Value::Null) | None => None,
        Some(_) => return Err(CouchError::bad_request("fields must be an array")),
    };
    let sort = match body.get("sort") {
        Some(s) => parse_sort_fields(s)?,
        None => vec![],
    };
    if !sort.is_empty() && sort.iter().any(|(_, a)| *a != sort[0].1) {
        return Err(CouchError::new(StatusCode::BAD_REQUEST, "unsupported_mixed_sort", "Sorts currently only support a single direction for all fields."));
    }
    let use_index = match body.get("use_index") {
        Some(Value::String(s)) => {
            let s = s.trim_start_matches("_design/");
            Some((format!("_design/{s}"), None))
        }
        Some(Value::Array(a)) => {
            let d = a.first().and_then(|x| x.as_str()).unwrap_or("").trim_start_matches("_design/").to_string();
            Some((format!("_design/{d}"), a.get(1).and_then(|x| x.as_str()).map(String::from)))
        }
        _ => None,
    };
    let bookmark = match body.get("bookmark").and_then(|b| b.as_str()) {
        None | Some("nil") | Some("") => None,
        Some(b) => {
            let raw = crate::doc::B64
                .decode(b)
                .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(b))
                .map_err(|_| CouchError::bad_request("Invalid bookmark value"))?;
            Some(serde_json::from_slice(&raw).map_err(|_| CouchError::bad_request("Invalid bookmark value"))?)
        }
    };
    let limit = match body.get("limit") {
        Some(v) => v.as_u64().ok_or_else(|| CouchError::bad_request("limit must be a positive integer"))? as usize,
        None => 25,
    };
    Ok(FindQuery {
        selector,
        fields,
        sort,
        limit,
        skip: body.get("skip").and_then(|s| s.as_u64()).unwrap_or(0) as usize,
        use_index,
        bookmark,
        update: body.get("update").and_then(|u| u.as_bool()).unwrap_or(true),
        conflicts: body.get("conflicts").and_then(|u| u.as_bool()).unwrap_or(false),
        execution_stats: body.get("execution_stats").and_then(|u| u.as_bool()).unwrap_or(false),
        stable: body.get("stable").and_then(|u| u.as_bool()).unwrap_or(false),
        partition: None,
    })
}

const MAX_OBJ: &str = "\u{fff0}";

fn max_obj() -> Value {
    json!({ MAX_OBJ: 1 })
}

/// Key range for a JSON index: equality on a prefix of fields, then at
/// most one range field. Returns (start, start_inclusive, end, end_inclusive).
fn index_range(sel: &Selector, idx: &MangoIndex) -> (Vec<Value>, Option<(Value, bool)>, Option<(Value, bool)>) {
    let mut prefix = Vec::new();
    for (f, _) in &idx.fields {
        let conds = sel.top_conds(f);
        if let Some(Cond::Eq(v)) = conds.iter().find(|c| matches!(c, Cond::Eq(_))) {
            prefix.push(v.clone());
            continue;
        }
        let mut lo: Option<(Value, bool)> = None;
        let mut hi: Option<(Value, bool)> = None;
        for c in conds {
            match c {
                Cond::Gt(v) => lo = Some((v.clone(), false)),
                Cond::Gte(v) => lo = Some((v.clone(), true)),
                Cond::Lt(v) => hi = Some((v.clone(), false)),
                Cond::Lte(v) => hi = Some((v.clone(), true)),
                Cond::BeginsWith(p) => {
                    lo = Some((json!(p), true));
                    hi = Some((json!(format!("{p}\u{10ffff}")), true));
                }
                _ => {}
            }
        }
        return (prefix, lo, hi);
    }
    (prefix, None, None)
}

/// Can this index answer the query (fields required, sort compatible)?
fn usable(idx: &MangoIndex, q: &FindQuery, required: &[String]) -> bool {
    if idx.kind == "nouveau" {
        return false;
    }
    if idx.kind == "special" {
        return q.sort.is_empty() || (q.sort.len() == 1 && q.sort[0].0 == "_id");
    }
    if let Some(p) = &idx.partial {
        // Partial indexes are only chosen explicitly via use_index.
        if q.use_index.is_none() {
            return false;
        }
        let _ = p;
    } else if !idx.fields.iter().all(|(f, _)| required.contains(f)) {
        return false;
    }
    if !q.sort.is_empty() {
        // Sort fields must be a prefix of the index fields, allowing
        // leading equality-constrained fields to be skipped.
        let names: Vec<&str> = idx.fields.iter().map(|(f, _)| f.as_str()).collect();
        let sort: Vec<&str> = q.sort.iter().map(|(f, _)| f.as_str()).collect();
        let mut i = 0;
        while i < names.len() && !sort.is_empty() && names[i] != sort[0] {
            let eq = q.selector.top_conds(names[i]).iter().any(|c| matches!(c, Cond::Eq(_)));
            if !eq {
                return false;
            }
            i += 1;
        }
        if names.len() - i < sort.len() || names[i..i + sort.len()] != sort[..] {
            return false;
        }
    }
    true
}

/// mango_idx_nouveau:is_usable/3: a nouveau index serves a selector
/// whose fields it indexes (and sorts however asked).
fn nouveau_usable(idx: &MangoIndex, normalized: &Value) -> bool {
    if idx.kind != "nouveau" || normalized.as_object().is_some_and(|o| o.is_empty()) {
        return false;
    }
    let Some(def) = &idx.def else { return false };
    match crate::nouveau::mango::columns(def) {
        None => true,
        Some(cols) => crate::nouveau::mango::indexable_fields(normalized).is_ok_and(|fs| fs.iter().all(|f| cols.contains(f))),
    }
}

fn choose_index(req: &Req, snap: &Snapshot, q: &FindQuery) -> Result<(MangoIndex, Option<String>)> {
    // A partition query can use partitioned indexes only, a global one
    // global indexes only; _all_docs serves both.
    let all: Vec<MangoIndex> = list_indexes(snap, crate::nouveau::enabled(&req.server))?
        .into_iter()
        .filter(|i| i.kind == "special" || i.partitioned == q.partition.is_some())
        .collect();
    let required = q.selector.required_fields();
    let normalized = normalize_selector(&q.selector.raw)?;
    if let Some((ddoc, name)) = &q.use_index {
        let found: Vec<&MangoIndex> = all
            .iter()
            .filter(|i| i.ddoc.as_deref() == Some(ddoc.as_str()) && name.as_ref().map(|n| n == &i.name).unwrap_or(true))
            .collect();
        if let Some(i) = found.iter().find(|i| nouveau_usable(i, &normalized) || usable(i, q, &required)) {
            return Ok(((*i).clone(), None));
        }
        crate::stat!("mango.query_invalid_index").inc();
        let warn = format!("{ddoc} was not used because it does not contain a valid index for this query.");
        // Fall through to automatic selection with a warning.
        let (i, w) = auto_index(&all, q, &required)?;
        return Ok((i, Some(w.map(|w| format!("{warn}\n{w}")).unwrap_or(warn))));
    }
    // A usable nouveau index is preferred to any view (the first cursor
    // with indexes wins, mango_cursor:group_indexes_by_type/1).
    let nouveau: Vec<&MangoIndex> = all.iter().filter(|i| i.partial.is_none() && nouveau_usable(i, &normalized)).collect();
    match nouveau.as_slice() {
        [] => {}
        [i] => return Ok(((*i).clone(), None)),
        _ => {
            return Err(CouchError::new(
                StatusCode::BAD_REQUEST,
                "multiple_nouveau_indexes",
                "You must specify an index with the `use_index` parameter.",
            ));
        }
    }
    auto_index(&all, q, &required)
}

fn auto_index(all: &[MangoIndex], q: &FindQuery, required: &[String]) -> Result<(MangoIndex, Option<String>)> {
    // Prefer the json index covering the most selector fields; ties go to
    // fewer total fields (more selective per key).
    let mut best: Option<&MangoIndex> = None;
    for i in all.iter().filter(|i| i.kind == "json" && usable(i, q, required)) {
        best = match best {
            None => Some(i),
            Some(b) => {
                let score = |x: &MangoIndex| {
                    let covered = x.fields.iter().filter(|(f, _)| required.contains(f)).count();
                    (covered, std::cmp::Reverse(x.fields.len()))
                };
                if score(i) > score(b) { Some(i) } else { Some(b) }
            }
        };
    }
    if let Some(b) = best {
        return Ok((b.clone(), None));
    }
    let special = &all[0];
    if usable(special, q, required) {
        return Ok((
            special.clone(),
            Some("No matching index found, create an index to optimize query time.".into()),
        ));
    }
    Err(CouchError::new(
        StatusCode::BAD_REQUEST,
        "no_usable_index",
        "No index exists for this sort, try indexing by the sort fields.",
    ))
}

fn project(doc: &Value, fields: &Option<Vec<String>>) -> Value {
    let Some(fields) = fields else { return doc.clone() };
    let mut out = Value::Object(Map::new());
    for f in fields {
        if let Some(v) = get_field(doc, f) {
            let parts = split_path(f);
            let mut cur = &mut out;
            for (i, p) in parts.iter().enumerate() {
                if i == parts.len() - 1 {
                    cur[p.as_str()] = v.clone();
                } else {
                    if !cur.get(p.as_str()).map(|x| x.is_object()).unwrap_or(false) {
                        cur[p.as_str()] = json!({});
                    }
                    cur = cur.get_mut(p.as_str()).unwrap();
                }
            }
        }
    }
    out
}

struct Stats {
    keys: u64,
    docs: u64,
    returned: u64,
}

fn explain_json(db: &str, idx: &MangoIndex, q: &FindQuery, range: &Value) -> Value {
    json!({
        "dbname": db,
        "index": idx.to_json(),
        "partitioned": q.partition.is_some(),
        "selector": normalize_selector(&q.selector.raw).unwrap_or_else(|_| q.selector.raw.clone()),
        "opts": {
            "use_index": q.use_index.as_ref().map(|(d, n)| match n { Some(n) => json!([d, n]), None => json!([d]) }).unwrap_or(json!([])),
            "bookmark": "nil",
            "limit": q.limit,
            "skip": q.skip,
            "sort": {},
            "fields": q.fields.clone().map(|f| json!(f)).unwrap_or(json!("all_fields")),
            "partition": q.partition.clone().unwrap_or_default(),
            "r": 1,
            "conflicts": q.conflicts,
            "stale": false,
            "update": q.update,
            "stable": q.stable,
            "execution_stats": q.execution_stats
        },
        "limit": q.limit,
        "skip": q.skip,
        "fields": q.fields.clone().map(|f| json!(f)).unwrap_or(json!("all_fields")),
        "mrargs": range,
        "covering": false
    })
}

fn mrargs(idx: &MangoIndex, q: &FindQuery) -> (QueryArgs, Value) {
    let desc = q.sort.first().map(|(_, a)| !a).unwrap_or(false);
    let mut args = QueryArgs {
        reduce: Some(false),
        include_docs: true,
        descending: desc,
        ..Default::default()
    };
    if idx.kind == "special" {
        let conds = q.selector.top_conds("_id");
        let mut lo = None;
        let mut hi = None;
        for c in conds {
            match c {
                Cond::Eq(v) => {
                    lo = Some(v.clone());
                    hi = Some(v.clone());
                }
                Cond::Gt(v) | Cond::Gte(v) => lo = Some(v.clone()),
                Cond::Lt(v) | Cond::Lte(v) => hi = Some(v.clone()),
                Cond::BeginsWith(p) => {
                    lo = Some(json!(p));
                    hi = Some(json!(format!("{p}\u{10ffff}")));
                }
                _ => {}
            }
        }
        let (lo, hi) = (lo.filter(|v| v.is_string()), hi.filter(|v| v.is_string()));
        if desc {
            args.start_key = hi;
            args.end_key = lo;
        } else {
            args.start_key = lo;
            args.end_key = hi;
        }
    } else {
        let (prefix, lo, hi) = index_range(&q.selector, idx);
        let mut start = prefix.clone();
        let mut end = prefix.clone();
        match lo {
            Some((v, true)) => start.push(v),
            Some((v, false)) => {
                start.push(v);
                start.push(max_obj());
            }
            None => {}
        }
        match hi {
            Some((v, true)) => {
                end.push(v);
                end.push(max_obj());
            }
            Some((v, false)) => {
                end.push(v);
                args.inclusive_end = false;
            }
            None => end.push(max_obj()),
        }
        if desc {
            args.start_key = Some(Value::Array(end));
            args.end_key = Some(Value::Array(start));
        } else {
            args.start_key = Some(Value::Array(start));
            args.end_key = Some(Value::Array(end));
        }
    }
    let shown = json!({
        "include_docs": true, "view_type": "map", "reduce": false, "partition": null,
        "start_key": args.start_key, "end_key": args.end_key, "direction": if desc { "rev" } else { "fwd" },
        "stable": false, "update": q.update, "conflicts": "undefined"
    });
    (args, shown)
}

fn run_find(req_server: &Req, db: &Arc<Db>, q: &FindQuery) -> Result<Value> {
    let t0 = Instant::now();
    let snap = db.snapshot();
    let (idx, warning) = choose_index(req_server, &snap, q)?;
    if idx.kind == "nouveau" {
        return run_nouveau_find(req_server, db, q, &idx, t0);
    }
    let (args, _) = mrargs(&idx, q);
    let mut stats = Stats { keys: 0, docs: 0, returned: 0 };
    let mut docs = Vec::new();
    let mut skip = q.skip;
    let mut last_key: Option<Value> = None;
    let opts = DocOpts {
        conflicts: q.conflicts,
        ..Default::default()
    };
    let bookmark = q.bookmark.clone();
    if idx.kind == "special" {
        let dir = if args.descending { Dir::Rev } else { Dir::Fwd };
        let mut start = args.start_key.as_ref().and_then(|v| v.as_str().map(String::from));
        let mut after: Option<String> = None;
        if let Some(b) = &bookmark
            && let Some(id) = b.get(1).and_then(|i| i.as_str()) {
                start = Some(id.to_string());
                after = Some(id.to_string());
            }
        let mut range = Range {
            start,
            end: args.end_key.as_ref().and_then(|v| v.as_str().map(String::from)),
            inclusive_end: true,
        };
        // Within a partition: ids from "P:" up to "P;".
        if let Some(p) = &q.partition {
            let (lo, hi) = (format!("{p}:"), format!("{p};"));
            let (first, last) = if args.descending { (hi, lo) } else { (lo, hi) };
            let clamp_start = |s: Option<String>| -> String {
                match s {
                    Some(s) if (s > first) != args.descending => s,
                    _ => first.clone(),
                }
            };
            range.start = Some(clamp_start(range.start.take()));
            range.end = match range.end.take() {
                Some(e) if (e < last) != args.descending => Some(e),
                _ => {
                    range.inclusive_end = false;
                    Some(last.clone())
                }
            };
        }
        snap.fold_docs(dir, &range, |fdi| {
            if after.as_deref() == Some(fdi.id.as_str()) {
                return Ok(true);
            }
            stats.keys += 1;
            if fdi.deleted || fdi.id.starts_with("_design/") {
                return Ok(true);
            }
            stats.docs += 1;
            let doc = snap.winner_json(&fdi, &opts)?;
            if q.selector.matches(&doc) {
                if skip > 0 {
                    skip -= 1;
                    return Ok(true);
                }
                last_key = Some(json!([fdi.id, fdi.id]));
                docs.push(project(&doc, &q.fields));
            }
            Ok(docs.len() < q.limit)
        })?;
    } else {
        let ddoc_id = idx.ddoc.clone().unwrap();
        let dir = views::index_dir(req_server, db);
        let update = if q.update { "true" } else { "false" };
        let (snap2, index, st) = views::prepare(db, dir, &ddoc_id, update)?;
        let vi = index.def.view_index(&idx.name).ok_or_else(|| CouchError::not_found("missing index"))?;
        let tree = &st.views[vi];
        let fdir = if args.descending { Dir::Rev } else { Dir::Fwd };
        let start_id = if args.descending { IdBound::Max } else { IdBound::Min };
        let end_id = if args.descending { IdBound::Min } else { IdBound::Max };
        // A partitioned index stores [partition, key]: bound the range to
        // the partition and wrap the keys.
        let wrap = |k: Value| match &q.partition {
            Some(p) => json!([p, k]),
            None => k,
        };
        let (pstart, pend) = match &q.partition {
            Some(p) if args.descending => (Some(json!([p, views::key_max()])), Some(json!([p]))),
            Some(p) => (Some(json!([p])), Some(json!([p, views::key_max()]))),
            None => (None, None),
        };
        let end_inclusive = args.inclusive_end || args.end_key.is_none();
        let mut range = Range {
            start: args.start_key.clone().map(wrap).or(pstart).map(|k| (crate::collate::Json(k), start_id.clone())),
            end: args.end_key.clone().map(wrap).or(pend).map(|k| {
                let id = if end_inclusive { end_id.clone() } else if args.descending { IdBound::Max } else { IdBound::Min };
                (crate::collate::Json(k), id)
            }),
            inclusive_end: true,
        };
        let mut after: Option<(Value, String)> = None;
        if let Some(b) = &bookmark
            && let (Some(k), Some(id)) = (b.get(0), b.get(1).and_then(|i| i.as_str())) {
                range.start = Some((crate::collate::Json(k.clone()), IdBound::Id(id.to_string())));
                after = Some((k.clone(), id.to_string()));
            }
        tree.fold(fdir, &range, |(k, id), _| {
            let IdBound::Id(id) = id else { return Ok(true) };
            if let Some((ak, aid)) = &after
                && aid == id && cmp_json(ak, &k.0) == Ordering::Equal {
                    return Ok(true);
                }
            stats.keys += 1;
            let Some(fdi) = snap2.get_fdi(id)? else { return Ok(true) };
            if fdi.deleted {
                return Ok(true);
            }
            stats.docs += 1;
            let doc = snap2.winner_json(&fdi, &opts)?;
            if q.selector.matches(&doc) {
                if skip > 0 {
                    skip -= 1;
                    return Ok(true);
                }
                last_key = Some(json!([k.0, id]));
                docs.push(project(&doc, &q.fields));
            }
            Ok(docs.len() < q.limit)
        })?;
    }
    // Sort by fields not served by the index is not supported, as in CouchDB.
    stats.returned = docs.len() as u64;
    let bookmark = match last_key {
        Some(k) => crate::doc::B64.encode(serde_json::to_vec(&k)?),
        None => q
            .bookmark
            .as_ref()
            .map(|b| crate::doc::B64.encode(serde_json::to_vec(b).unwrap()))
            .unwrap_or_else(|| "nil".into()),
    };
    let mut out = json!({"docs": docs, "bookmark": bookmark});
    if idx.kind == "special" {
        crate::stat!("mango.unindexed_queries").inc();
    }
    crate::stat!("mango.keys_examined").add(stats.keys as i64);
    crate::stat!("mango.docs_examined").add(stats.docs as i64);
    crate::stat!("mango.evaluate_selector").add(stats.docs as i64);
    crate::stat!("mango.results_returned").add(stats.returned as i64);
    crate::stat!("mango.query_time").observe(crate::stats::ms_since(t0));
    crate::db::count_reads(&snap, stats.docs);
    // Many docs examined per result: an index would serve better.
    let ratio = if stats.returned == 0 { stats.docs as f64 } else { stats.docs as f64 / stats.returned as f64 };
    let threshold = req_server.server.config.get_int("mango", "index_scan_warning_threshold", 10);
    let scan_warning = if threshold > 0 && ratio > threshold as f64 {
        crate::stat!("mango.too_many_docs_scanned").inc();
        Some("The number of documents examined is high in proportion to the number of results returned. Consider adding a more specific index to improve this.")
    } else {
        None
    };
    let warnings: Vec<&str> = warning.as_deref().into_iter().chain(scan_warning).collect();
    if !warnings.is_empty() {
        out["warning"] = json!(warnings.join("\n"));
    }
    if q.execution_stats {
        out["execution_stats"] = json!({
            "total_keys_examined": stats.keys,
            "total_docs_examined": stats.docs,
            "total_quorum_docs_examined": 0,
            "results_returned": stats.returned,
            "execution_time_ms": t0.elapsed().as_secs_f64() * 1000.0
        });
    }
    Ok(out)
}

/// mango_cursor_nouveau:sort_query/1.
fn nouveau_sort(sort: &[(String, bool)]) -> Result<Option<Vec<String>>> {
    if sort.is_empty() {
        return Ok(None);
    }
    sort.iter()
        .map(|(f, asc)| {
            let e = crate::nouveau::mango::escape_user(f)?;
            Ok(if *asc { e } else { format!("-{e}") })
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

/// _find on a nouveau index (mango_cursor_nouveau): search with the
/// selector as a Lucene query, a page at a time, keeping the documents
/// that match it (the query can only narrow them down).
fn run_nouveau_find(req: &Req, db: &Arc<Db>, q: &FindQuery, idx: &MangoIndex, t0: Instant) -> Result<Value> {
    let normalized = normalize_selector(&q.selector.raw)?;
    let query = crate::nouveau::mango::selector_query(&normalized)?;
    let snap = db.snapshot();
    let ddoc_id = idx.ddoc.clone().unwrap_or_default();
    let fdi = snap.get_fdi(&ddoc_id)?.filter(|f| !f.deleted).ok_or_else(|| CouchError::not_found("missing"))?;
    let spec = crate::nouveau::design_doc_to_index(&snap.winner_json(&fdi, &DocOpts::default())?, &idx.name)?;
    let max = req.server.config.get_int("nouveau", "max_limit", 200usize);
    let mut limit = q.limit.min(max);
    let mut skip = q.skip;
    let sort = nouveau_sort(&q.sort)?;
    // A nouveau bookmark: the last hit's sort values.
    let mut after: Option<Vec<Value>> = q.bookmark.as_ref().and_then(|b| b.as_array()?.last()?.as_array().cloned());
    let opts = DocOpts { conflicts: q.conflicts, ..Default::default() };
    let mut stats = Stats { keys: 0, docs: 0, returned: 0 };
    let mut docs = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let search_error = |e: CouchError| CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, "nouveau_search_error", format!("{}: {}", e.error, e.reason));
    'pages: while !query.is_empty() && limit > 0 {
        let args = crate::nouveau::SearchArgs {
            bookmark: None,
            counts: None,
            include_docs: false,
            limit: (limit + skip).min(max) as i64,
            locale: None,
            partition: q.partition.clone(),
            query: query.clone(),
            ranges: None,
            sort: sort.clone(),
            top_n: 10,
            update: true,
        };
        let found = crate::nouveau::search_after(&req.server, db, &spec, &args, after.clone()).map_err(search_error)?;
        if found.results.hits.is_empty() {
            break;
        }
        let snap = db.snapshot();
        for h in &found.results.hits {
            after = Some(h.order.clone());
            let Some(fdi) = snap.get_fdi(&h.id)?.filter(|f| !f.deleted) else { continue };
            stats.docs += 1;
            let doc = snap.winner_json(&fdi, &opts)?;
            if q.selector.matches(&doc) && seen.insert(h.id.clone()) {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                docs.push(project(&doc, &q.fields));
                limit -= 1;
                if limit == 0 {
                    break 'pages;
                }
            }
        }
    }
    stats.returned = docs.len() as u64;
    let mut out = json!({"docs": docs, "bookmark": crate::nouveau::pack_bookmark(&after)});
    crate::stat!("mango.docs_examined").add(stats.docs as i64);
    crate::stat!("mango.results_returned").add(stats.returned as i64);
    crate::stat!("mango.query_time").observe(crate::stats::ms_since(t0));
    crate::db::count_reads(&snap, stats.docs);
    let ratio = if stats.returned == 0 { stats.docs as f64 } else { stats.docs as f64 / stats.returned as f64 };
    let threshold = req.server.config.get_int("mango", "index_scan_warning_threshold", 10);
    if threshold > 0 && ratio > threshold as f64 {
        crate::stat!("mango.too_many_docs_scanned").inc();
        out["warning"] = json!("The number of documents examined is high in proportion to the number of results returned. Consider adding a more specific index to improve this.");
    }
    if q.execution_stats {
        out["execution_stats"] = json!({
            "total_keys_examined": stats.keys,
            "total_docs_examined": stats.docs,
            "total_quorum_docs_examined": 0,
            "results_returned": stats.returned,
            "execution_time_ms": t0.elapsed().as_secs_f64() * 1000.0
        });
    }
    Ok(out)
}

pub async fn http_find(req: &Req, db: &Arc<Db>) -> Result<Response> {
    http_find_in(req, db, None).await
}

/// _find, globally or within a partition.
pub async fn http_find_in(req: &Req, db: &Arc<Db>, partition: Option<&str>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let body = req.json_obj_body()?;
    let mut q = parse_find(&body)?;
    q.partition = partition.map(String::from);
    let v = blocking(|| run_find(req, db, &q))?;
    ok_json(v)
}

pub async fn http_explain(req: &Req, db: &Arc<Db>) -> Result<Response> {
    http_explain_in(req, db, None).await
}

/// _explain, globally or within a partition.
pub async fn http_explain_in(req: &Req, db: &Arc<Db>, partition: Option<&str>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let body = req.json_obj_body()?;
    let mut q = parse_find(&body)?;
    q.partition = partition.map(String::from);
    let snap = db.snapshot();
    let (idx, _) = blocking(|| choose_index(req, &snap, &q))?;
    let (_, shown) = mrargs(&idx, &q);
    let mut out = explain_json(&db.name, &idx, &q, &shown);
    if idx.kind == "nouveau" {
        // mango_cursor_nouveau:explain/1: the Lucene query in place of
        // view arguments.
        let o = out.as_object_mut().unwrap();
        o.remove("mrargs");
        o.remove("covering");
        o.insert("query".into(), json!(crate::nouveau::mango::selector_query(&normalize_selector(&q.selector.raw)?)?));
        o.insert("partition".into(), json!(q.partition));
        o.insert("sort".into(), json!(nouveau_sort(&q.sort)?));
    }
    Ok(json_response(StatusCode::OK, &out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(sel: Value, doc: Value) -> bool {
        Selector::parse(&sel).unwrap().matches(&doc)
    }

    #[test]
    fn selectors() {
        let d = json!({"_id": "x", "name": "Bob", "age": 30, "tags": ["a", "b"], "addr": {"city": "Paris"}, "a.b": 1});
        assert!(m(json!({"name": "Bob"}), d.clone()));
        assert!(m(json!({"age": {"$gt": 20, "$lt": 40}}), d.clone()));
        assert!(!m(json!({"age": {"$gt": 30}}), d.clone()));
        assert!(m(json!({"addr": {"city": "Paris"}}), d.clone()));
        assert!(m(json!({"addr.city": {"$regex": "^Pa"}}), d.clone()));
        assert!(m(json!({"tags": {"$elemMatch": {"$eq": "b"}}}), d.clone()));
        assert!(m(json!({"tags": {"$all": ["a", "b"]}}), d.clone()));
        assert!(m(json!({"tags": {"$size": 2}}), d.clone()));
        assert!(m(json!({"$or": [{"name": "X"}, {"age": 30}]}), d.clone()));
        assert!(m(json!({"nope": {"$exists": false}}), d.clone()));
        assert!(!m(json!({"nope": {"$ne": 1}}), d.clone()));
        assert!(m(json!({"age": {"$not": {"$eq": 31}}}), d.clone()));
        assert!(m(json!({"a\\.b": 1}), d.clone()));
        assert!(m(json!({"age": {"$in": [1, 30]}}), d.clone()));
        assert!(m(json!({"tags": {"$in": ["b"]}}), d.clone()));
        assert!(m(json!({"age": {"$mod": [7, 2]}}), d.clone()));
        assert!(m(json!({"name": {"$beginsWith": "Bo"}}), d.clone()));
        assert!(m(json!({"$nor": [{"name": "Al"}]}), d));
    }
}

// ------------------------------------------------- selector normalization

fn sel_error(error: &str, reason: String) -> CouchError {
    CouchError::new(StatusCode::BAD_REQUEST, error, reason)
}

fn bad_arg(op: &str, arg: &Value) -> CouchError {
    sel_error("bad_arg", format!("Bad argument for operator {op}: {arg}"))
}

/// The single member of a one-member object.
fn single(v: &Value) -> Option<(&str, &Value)> {
    match v.as_object() {
        Some(o) if o.len() == 1 => o.iter().next().map(|(k, v)| (k.as_str(), v)),
        _ => None,
    }
}

fn one(k: &str, v: Value) -> Value {
    let mut m = Map::new();
    m.insert(k.to_string(), v);
    Value::Object(m)
}

fn each(args: &[Value], f: impl Fn(&Value) -> Result<Value>) -> Result<Value> {
    Ok(Value::Array(args.iter().map(f).collect::<Result<Vec<_>>>()?))
}

/// mango_selector:normalize/1: operators checked, implicit $eq and $and
/// made explicit, nested fields joined into paths, and $not and $nor
/// pushed inward.
pub fn normalize_selector(v: &Value) -> Result<Value> {
    if v.as_object().is_some_and(|o| o.is_empty()) {
        return Ok(v.clone());
    }
    let s = norm_negations(&norm_fields(&norm_ops(v)?, "")?)?;
    if s.as_object().is_some_and(|o| o.contains_key("")) {
        return Err(sel_error("invalid_selector", "One or more conditions is missing a field name.".into()));
    }
    Ok(s)
}

fn norm_ops(v: &Value) -> Result<Value> {
    if let Some(o) = v.as_object()
        && o.len() >= 2
    {
        return Ok(one("$and", Value::Array(o.iter().map(|(k, v)| norm_ops(&one(k, v.clone()))).collect::<Result<Vec<_>>>()?)));
    }
    let Some((k, arg)) = single(v) else { return Ok(one("$eq", v.clone())) };
    Ok(match (k, arg) {
        ("$and" | "$or" | "$nor", Value::Array(a)) => one(k, each(a, norm_ops)?),
        ("$not" | "$elemMatch" | "$allMatch" | "$keyMapMatch", Value::Object(_)) => one(k, norm_ops(arg)?),
        ("$in" | "$nin" | "$all", Value::Array(_)) => v.clone(),
        ("$exists", Value::Bool(_)) | ("$type", Value::String(_)) | ("$beginsWith", Value::String(_)) => v.clone(),
        ("$mod", Value::Array(a)) if a.len() == 2 && a.iter().all(|x| x.is_i64() || x.is_u64()) => v.clone(),
        ("$regex", Value::String(r)) => match regex::Regex::new(r) {
            Ok(_) => v.clone(),
            Err(_) => return Err(bad_arg(k, arg)),
        },
        ("$size", Value::Number(n)) if n.is_u64() => v.clone(),
        ("$text", Value::String(_) | Value::Number(_) | Value::Bool(_)) => one("$default", one("$text", arg.clone())),
        ("$default" | "$lt" | "$lte" | "$eq" | "$ne" | "$gte" | "$gt", _) => v.clone(),
        ("$where" | "$geoWithin" | "$geoIntersects" | "$near" | "$nearSphere", _) => {
            return Err(sel_error("not_supported", format!("Unsupported operator: {k}")));
        }
        (
            "$and" | "$or" | "$nor" | "$not" | "$elemMatch" | "$allMatch" | "$keyMapMatch" | "$in" | "$nin" | "$all" | "$exists"
            | "$type" | "$beginsWith" | "$mod" | "$regex" | "$size" | "$text",
            _,
        ) => return Err(bad_arg(k, arg)),
        (op, _) if op.starts_with('$') => return Err(sel_error("invalid_operator", format!("Invalid operator: {op}"))),
        (field, cond) => one(field, norm_ops(cond)?),
    })
}

fn norm_fields(v: &Value, path: &str) -> Result<Value> {
    let bad = || sel_error("bad_field", format!("Invalid field normalization on selector: {v}"));
    if v.as_object().is_some_and(|o| o.is_empty()) {
        return Ok(if path.is_empty() { v.clone() } else { one(path, v.clone()) });
    }
    let Some((k, arg)) = single(v) else { return Err(bad()) };
    Ok(match k {
        "$and" | "$or" | "$nor" => one(k, each(arg.as_array().map(Vec::as_slice).unwrap_or(&[]), |a| norm_fields(a, path))?),
        "$not" => one(k, norm_fields(arg, path)?),
        "$elemMatch" | "$allMatch" | "$keyMapMatch" => one(path, one(k, norm_fields(arg, "")?)),
        "$default" if path.is_empty() && single(arg).is_some_and(|(t, _)| t == "$text") => v.clone(),
        "$default" => return Err(bad()),
        op if op.starts_with('$') => one(path, v.clone()),
        field if path.is_empty() => norm_fields(arg, field)?,
        field => norm_fields(arg, &format!("{path}.{field}"))?,
    })
}

fn norm_negations(v: &Value) -> Result<Value> {
    let Some((k, arg)) = single(v) else { return Ok(v.clone()) };
    let list = || arg.as_array().map(Vec::as_slice).unwrap_or(&[]);
    Ok(match k {
        "$not" => negate(arg)?,
        "$nor" => one("$and", each(list(), negate)?),
        "$and" | "$or" => one(k, each(list(), norm_negations)?),
        "$elemMatch" | "$allMatch" | "$keyMapMatch" => one(k, norm_negations(arg)?),
        _ => v.clone(),
    })
}

fn negate(v: &Value) -> Result<Value> {
    let Some((k, arg)) = single(v) else { return Ok(one("$not", v.clone())) };
    let list = || arg.as_array().map(Vec::as_slice).unwrap_or(&[]);
    Ok(match k {
        "$not" => norm_negations(arg)?,
        "$nor" => one("$or", each(list(), norm_negations)?),
        "$and" => one("$or", each(list(), negate)?),
        "$or" => one("$and", each(list(), negate)?),
        "$default" => return Err(bad_arg("$not", v)),
        "$lt" => one("$gte", arg.clone()),
        "$lte" => one("$gt", arg.clone()),
        "$eq" => one("$ne", arg.clone()),
        "$ne" => one("$eq", arg.clone()),
        "$gte" => one("$lt", arg.clone()),
        "$gt" => one("$lte", arg.clone()),
        "$in" => one("$nin", arg.clone()),
        "$nin" => one("$in", arg.clone()),
        "$exists" => one("$exists", Value::Bool(!arg.as_bool().unwrap_or(false))),
        op if op.starts_with('$') => one("$not", v.clone()),
        field => one(field, negate(arg)?),
    })
}

#[cfg(test)]
mod normalize_tests {
    use super::*;

    #[test]
    fn normalizes_like_couchdb() {
        let n = |v: Value| normalize_selector(&v).unwrap();
        assert_eq!(n(json!({"a": 1})), json!({"a": {"$eq": 1}}));
        assert_eq!(n(json!({"a": {"b": 2}})), json!({"a.b": {"$eq": 2}}));
        assert_eq!(n(json!({"a": 1, "b": {"$gt": 2}})), json!({"$and": [{"a": {"$eq": 1}}, {"b": {"$gt": 2}}]}));
        assert_eq!(n(json!({"$not": {"a": {"$lt": 3}}})), json!({"a": {"$gte": 3}}));
        assert_eq!(n(json!({"$nor": [{"a": 1}]})), json!({"$and": [{"a": {"$ne": 1}}]}));
        assert_eq!(n(json!({"a": {"$elemMatch": {"b": 1}}})), json!({"a": {"$elemMatch": {"b": {"$eq": 1}}}}));
        assert_eq!(n(json!({})), json!({}));
        assert_eq!(normalize_selector(&json!({"a": {"$where": 1}})).unwrap_err().reason, "Unsupported operator: $where");
        assert_eq!(normalize_selector(&json!({"a": {"$bogus": 1}})).unwrap_err().reason, "Invalid operator: $bogus");
    }
}
