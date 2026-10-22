//! mango_idx: a database's indexes (_all_docs, then the json and nouveau
//! indexes of its query design docs), how each is shown and ordered,
//! which queries each can serve (is_usable), and adding and removing them
//! (mango_idx_view and mango_idx_nouveau's add/remove).

use super::opts::{AutoName, IdxCreateOpts, validate_json_index};
use super::selector::{cmp_raw, has_required_fields, indexable_fields, is_constant_field, is_text_search};
use super::{erl_w, error};
use crate::db::Snapshot;
use crate::doc::DocOpts;
use crate::error::Result;
use crate::replicator::rep::{Term, ejson};
use serde_json::{Map, Value, json};
use sha1::{Digest, Sha1};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Special,
    Json,
    Nouveau,
}

#[derive(Clone, Debug)]
pub struct Idx {
    /// None for _all_docs.
    pub ddoc: Option<String>,
    pub name: String,
    pub kind: Kind,
    /// json: the view's "map"; nouveau: the index definition.
    pub def: Value,
    pub partitioned: bool,
    /// json: the view's options, less "sort".
    pub opts: Value,
}

impl Idx {
    pub fn special() -> Idx {
        Idx { ddoc: None, name: "_all_docs".into(), kind: Kind::Special, def: Value::Null, partitioned: false, opts: json!([]) }
    }

    pub fn type_str(&self) -> &'static str {
        match self.kind {
            Kind::Special => "special",
            Kind::Json => "json",
            Kind::Nouveau => "nouveau",
        }
    }

    /// What tells two indexes apart.
    pub fn same(&self, other: &Idx) -> bool {
        self.ddoc == other.ddoc && self.name == other.name && self.kind == other.kind
    }

    /// The order of #idx{} records: ddoc (undefined first), name, type,
    /// def, partitioned, opts.
    pub fn term_cmp(a: &Idx, b: &Idx) -> Ordering {
        let ddoc = |i: &Idx| i.ddoc.as_ref().map(|d| d.as_bytes().to_vec());
        ddoc(a)
            .cmp(&ddoc(b))
            .then_with(|| a.name.as_bytes().cmp(b.name.as_bytes()))
            .then_with(|| a.type_str().cmp(b.type_str()))
            .then_with(|| cmp_raw(&a.def, &b.def))
            .then_with(|| a.partitioned.cmp(&b.partitioned))
            .then_with(|| cmp_raw(&a.opts, &b.opts))
    }

    pub fn to_json(&self) -> Value {
        match self.kind {
            Kind::Special => json!({"ddoc": null, "name": "_all_docs", "type": "special", "def": {"fields": [{"_id": "asc"}]}}),
            Kind::Json => json!({
                "ddoc": self.ddoc, "name": self.name, "type": "json", "partitioned": self.partitioned,
                "def": json_def_to_json(&self.def),
            }),
            Kind::Nouveau => json!({
                "ddoc": self.ddoc, "name": self.name, "type": "nouveau", "partitioned": self.partitioned,
                "def": crate::nouveau::mango::def_to_json(&self.def),
            }),
        }
    }

    /// columns/1: the fields indexed, in order (None for a nouveau index
    /// of all fields).
    pub fn columns(&self) -> Option<Vec<String>> {
        match self.kind {
            Kind::Special => Some(vec!["_id".into()]),
            Kind::Json => Some(self.def.get("fields").and_then(Value::as_object).map(|f| f.keys().cloned().collect()).unwrap_or_default()),
            Kind::Nouveau => crate::nouveau::mango::columns(&self.def),
        }
    }

    /// The width the planner compares indexes by (all fields counting as
    /// 256).
    pub fn width(&self) -> usize {
        self.columns().map(|c| c.len()).unwrap_or(256)
    }

    /// get_partial_filter_selector/1.
    pub fn partial_filter(&self) -> Option<Value> {
        let nonempty = |v: &Value| !v.as_object().is_some_and(Map::is_empty);
        match self.kind {
            Kind::Special => None,
            Kind::Json => match self.def.get("partial_filter_selector") {
                Some(p) => Some(p.clone()).filter(nonempty),
                None => self.def.get("selector").cloned().filter(nonempty),
            },
            Kind::Nouveau => crate::nouveau::mango::partial_filter(&self.def),
        }
    }

    /// is_usable/3: whether the index can serve the (normalized)
    /// selector sorted so, and if not, why.
    pub fn is_usable(&self, sel: &Value, sort: &[String]) -> Result<(bool, Vec<&'static str>)> {
        match self.kind {
            Kind::Special if sort.is_empty() => Ok((true, Vec::new())),
            Kind::Special => {
                let has = indexable_fields(sel).iter().any(|f| f == "_id");
                let can_sort = sort == ["_id"];
                let reasons = [("field_mismatch", has), ("sort_order_mismatch", can_sort)];
                Ok((has && can_sort, reasons.iter().filter(|(_, ok)| !ok).map(|(r, _)| *r).collect()))
            }
            Kind::Json => {
                let cols = self.columns().unwrap_or_default();
                let mut required = cols.clone();
                required.sort();
                required.dedup();
                required.retain(|f| !sort.contains(f) && f != "_id" && f != "_rev");
                let has = has_required_fields(sel, &required);
                let not_text = !is_text_search(sel)?;
                let can_sort = can_use_sort(&cols, sort, sel);
                let reasons = [("needs_text_search", not_text), ("field_mismatch", has), ("sort_order_mismatch", can_sort)];
                Ok((not_text && has && can_sort, reasons.iter().filter(|(_, ok)| !ok).map(|(r, _)| *r).collect()))
            }
            Kind::Nouveau if sel.as_object().is_some_and(Map::is_empty) => Ok((false, vec!["empty_selector"])),
            Kind::Nouveau => match self.columns() {
                None => Ok((true, Vec::new())),
                Some(cols) => {
                    let usable = crate::nouveau::mango::indexable_fields(sel)?.iter().all(|f| cols.contains(f));
                    Ok((usable, if usable { Vec::new() } else { vec!["field_mismatch"] }))
                }
            },
        }
    }
}

/// mango_idx_view:can_use_sort/3: the sort fields are a prefix of the
/// columns, once columns the selector holds constant are passed over.
fn can_use_sort(cols: &[String], sort: &[String], sel: &Value) -> bool {
    if sort.is_empty() {
        return true;
    }
    match cols.first() {
        None => false,
        Some(c) if *c == sort[0] => cols.starts_with(sort),
        Some(c) => is_constant_field(sel, c) && can_use_sort(&cols[1..], sort, sel),
    }
}

/// mango_idx_view:def_to_json/1: fields as a list of {field: dir}, an
/// empty partial filter left out.
fn json_def_to_json(def: &Value) -> Value {
    let mut out = Map::new();
    for (k, v) in def.as_object().into_iter().flatten() {
        match k.as_str() {
            "fields" => {
                let fields: Vec<Value> = v.as_object().into_iter().flatten().map(|(f, d)| json!({ f.clone(): d })).collect();
                out.insert(k.clone(), Value::Array(fields));
            }
            "partial_filter_selector" if v.as_object().is_some_and(Map::is_empty) => {}
            _ => {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Object(out)
}

/// A query design doc's view, as an index (validate_ddoc/1): its map an
/// object with an object of fields, its options an object.
fn view_index(view: &Value) -> Option<(Value, Value)> {
    let map = view.get("map").filter(|m| m.get("fields").is_some_and(Value::is_object))?;
    let mut opts = view.get("options")?.as_object()?.clone();
    opts.shift_remove("sort");
    Some((map.clone(), Value::Object(opts)))
}

/// list/1: _all_docs, then each query design doc's indexes (its nouveau
/// ones while nouveau is enabled, then its views).
pub fn list(snap: &Snapshot, nouveau: bool) -> Result<Vec<Idx>> {
    let mut out = vec![Idx::special()];
    for d in crate::views::all_ddocs(snap)? {
        if d.get("language").and_then(Value::as_str) != Some("query") {
            continue;
        }
        let id = d["_id"].as_str().unwrap_or("").to_string();
        let partitioned = match d.get("options").and_then(|o| o.get("partitioned")) {
            Some(Value::Bool(b)) => *b,
            _ => snap.partitioned(),
        };
        let idx = |name: &str, kind, def: Value, opts: Value| Idx { ddoc: Some(id.clone()), name: name.to_string(), kind, def, partitioned, opts };
        if nouveau {
            for (name, entry) in d.get("nouveau").and_then(Value::as_object).into_iter().flatten() {
                if let Some(def) = entry.get("index").filter(|i| crate::nouveau::mango::validate_def(i).is_ok()) {
                    out.push(idx(name, Kind::Nouveau, def.clone(), json!([])));
                }
            }
        }
        for (name, view) in d.get("views").and_then(Value::as_object).into_iter().flatten() {
            if let Some((def, opts)) = view_index(view) {
                out.push(idx(name, Kind::Json, def, opts));
            }
        }
    }
    Ok(out)
}

/// gen_name/2: the hex SHA-1 of the term {Def, usort(Opts)}.
fn gen_name(opts: &IdxCreateOpts) -> String {
    let auto = |n: &AutoName| match n {
        AutoName::Auto => Term::Atom("auto_name"),
        AutoName::Given(s) => Term::Bin(s.as_bytes().to_vec()),
    };
    let pair = |k: &'static str, v: Term| Term::Tuple(vec![Term::Atom(k), v]);
    let partitioned = match opts.partitioned {
        None => Term::Atom("db_default"),
        Some(true) => Term::Atom("true"),
        Some(false) => Term::Atom("false"),
    };
    let def = ejson(&opts.def);
    let sorted = vec![
        pair("ddoc", auto(&opts.ddoc)),
        pair("def", def.clone()),
        pair("name", auto(&opts.name)),
        pair("partitioned", partitioned),
        pair("type", Term::Bin(opts.kind.as_bytes().to_vec())),
        pair("w", Term::Int(opts.w as i128)),
    ];
    hex::encode(Sha1::digest(Term::Tuple(vec![def, Term::List(sorted)]).encode()))
}

/// The body of a design doc as mango_util:load_ddoc/3 gives it (a fresh
/// one if there's none, or it's deleted), and its revision.
fn load_ddoc(snap: &Snapshot, id: &str) -> Result<(Map<String, Value>, Option<String>)> {
    let fresh = || {
        let mut m = Map::new();
        m.insert("language".into(), json!("query"));
        m
    };
    let Some(fdi) = snap.get_fdi(id)?.filter(|f| !f.deleted) else { return Ok((fresh(), None)) };
    let Value::Object(mut body) = snap.winner_json(&fdi, &DocOpts::default())? else { return Ok((fresh(), None)) };
    body.shift_remove("_id");
    let rev = body.shift_remove("_rev").and_then(|r| r.as_str().map(String::from));
    match body.get("language") {
        Some(Value::String(l)) if l == "query" => Ok((body, rev)),
        other => {
            let lang = match other {
                Some(l) => format!("{{{},{}}}", erl_w(&json!("language")), erl_w(l)),
                None => "false".into(),
            };
            Err(error(400, "invalid_ddoc_lang", format!("Existing design doc has an invalid language: {lang}")))
        }
    }
}

/// lists:keystore/4 of an object member: replaced where it is, or added
/// at the end.
fn keystore(m: &mut Map<String, Value>, k: &str, v: Value) {
    m.insert(k.to_string(), v);
}

fn same_json(a: &Map<String, Value>, b: &Map<String, Value>) -> bool {
    serde_json::to_string(a).ok() == serde_json::to_string(b).ok()
}

/// The design document with a body and (when it exists) revision.
fn ddoc_json(id: &str, rev: Option<String>, body: Map<String, Value>) -> Value {
    let mut doc = Map::new();
    doc.insert("_id".into(), json!(id));
    if let Some(r) = rev {
        doc.insert("_rev".into(), json!(r));
    }
    doc.extend(body);
    Value::Object(doc)
}

/// new/2, validate_new/2 and add/2 of an index: its design doc and name,
/// and the design doc to save (None if the index is there already).
pub fn add(snap: &Snapshot, nouveau: bool, opts: &IdxCreateOpts) -> Result<(String, String, Option<Value>)> {
    let unavailable = |t: &str| error(503, "required index service unavailable", t.to_string());
    let kind = match opts.kind.as_str() {
        "json" => Kind::Json,
        "nouveau" if nouveau => Kind::Nouveau,
        "nouveau" | "text" => return Err(unavailable(&opts.kind)),
        other => return Err(error(400, "invalid_index", format!("Invalid type for index: {other}"))),
    };
    let generated = || gen_name(opts);
    let name = match &opts.name {
        AutoName::Given(n) => n.clone(),
        AutoName::Auto => generated(),
    };
    let ddoc = match &opts.ddoc {
        AutoName::Given(d) if d.starts_with("_design/") => d.clone(),
        AutoName::Given(d) => format!("_design/{d}"),
        AutoName::Auto => format!("_design/{}", generated()),
    };
    let (section, entry) = match kind {
        Kind::Json => {
            let Value::Object(def) = &opts.def else {
                return Err(error(400, "invalid_index", format!("JSON indexes must be an object, not: {}", erl_w(&opts.def))));
            };
            let (fields, partial) = validate_json_index(def)?;
            let fields: Map<String, Value> = fields.into_iter().map(|(f, d)| (f, Value::String(d))).collect();
            let map = json!({"fields": fields, "partial_filter_selector": partial});
            ("views", json!({"map": map, "reduce": "_count", "options": {"def": opts.def}}))
        }
        _ => {
            let def = crate::nouveau::mango::validate_def(&opts.def)?;
            ("nouveau", crate::nouveau::mango::ddoc_entry(&def))
        }
    };
    let (mut body, rev) = load_ddoc(snap, &ddoc)?;
    crate::doc::validate_docid(&ddoc)?;
    let before = body.clone();
    let mut section_map = body.get(section).and_then(Value::as_object).cloned().unwrap_or_default();
    keystore(&mut section_map, &name, entry);
    keystore(&mut body, section, Value::Object(section_map));
    // set_ddoc_partitioned/2: a new design doc takes the option; an
    // existing one must already have it.
    let old = body.get("options").and_then(Value::as_object).and_then(|o| o.get("partitioned")).and_then(Value::as_bool);
    if opts.partitioned != old {
        match (rev.is_none(), opts.partitioned) {
            (true, Some(p)) => {
                let mut options = body.get("options").and_then(Value::as_object).cloned().unwrap_or_default();
                keystore(&mut options, "partitioned", json!(p));
                keystore(&mut body, "options", Value::Object(options));
            }
            (true, None) => {}
            (false, _) => {
                return Err(error(
                    400,
                    "invalid_partitioned_option",
                    format!("Requested partitioned option does not match existing value on design document {ddoc}"),
                ));
            }
        }
    }
    if rev.is_some() && same_json(&before, &body) {
        return Ok((ddoc, name, None));
    }
    Ok((ddoc.clone(), name, Some(ddoc_json(&ddoc, rev, body))))
}

/// remove/2 (and delete/4's deleting a design doc nothing else is left
/// in): the design document to save.
pub fn remove(snap: &Snapshot, idx: &Idx) -> Result<Value> {
    let id = idx.ddoc.clone().unwrap_or_default();
    let (mut body, rev) = load_ddoc(snap, &id)?;
    let section = if idx.kind == Kind::Nouveau { "nouveau" } else { "views" };
    let not_found = || match idx.kind {
        Kind::Nouveau => super::unknown("mango_idx_nouveau", &format!("{{index_not_found,{}}}", erl_w(&json!(idx.name)))),
        _ => error(404, "invalid_index", format!("JSON index {} not found in this design doc.", idx.name)),
    };
    let mut entries = body.get(section).and_then(Value::as_object).cloned().ok_or_else(not_found)?;
    entries.shift_remove(&idx.name).ok_or_else(not_found)?;
    if entries.is_empty() {
        body.shift_remove(section);
    } else {
        keystore(&mut body, section, Value::Object(entries));
    }
    let only_language = body.len() == 1 && body.get("language") == Some(&json!("query"));
    if only_language {
        return Ok(json!({"_id": id, "_rev": rev, "_deleted": true}));
    }
    Ok(ddoc_json(&id, rev, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_as_couchdb_does() {
        let opts = |def: Value, ddoc: AutoName| IdxCreateOpts { def, kind: "json".into(), name: AutoName::Auto, ddoc, w: 2, partitioned: None };
        assert_eq!(gen_name(&opts(json!({"fields": ["a"]}), AutoName::Auto)), "7095fd2321a986309496ad2ac9c3498246175757");
        assert_eq!(gen_name(&opts(json!({"fields": ["a", "b"]}), AutoName::Given("ab".into()))), "c9960e6f9106f2e6dc9f3a2e480cd379e12dd8f9");
    }
}
