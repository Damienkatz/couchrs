//! mango_opts: the options of /_find, /_explain and /_index requests,
//! validated in mango_opts' order, with mango_sort and mango_fields.

use super::{erl_p, erl_w, error, normalize_selector, unknown};
use crate::error::Result;
use crate::server::Server;
use serde_json::{Map, Value};

/// A validated /_find (or /_explain) request.
#[derive(Clone, Debug)]
pub struct FindOpts {
    /// Normalized.
    pub selector: Value,
    /// [] or [DesignId] or [DesignId, ViewName].
    pub use_index: Vec<String>,
    /// None for nil.
    pub bookmark: Option<String>,
    pub limit: u64,
    pub skip: u64,
    /// (field, "asc" | "desc"), one direction for all.
    pub sort: Vec<(String, String)>,
    /// None for all_fields.
    pub fields: Option<Vec<Value>>,
    /// "" for none.
    pub partition: String,
    pub r: u64,
    pub conflicts: bool,
    pub stale_ok: bool,
    pub update: bool,
    pub stable: bool,
    pub execution_stats: bool,
    pub allow_fallback: bool,
}

impl FindOpts {
    pub fn sort_fields(&self) -> Vec<String> {
        self.sort.iter().map(|(f, _)| f.clone()).collect()
    }

    pub fn descending(&self) -> bool {
        self.sort.first().is_some_and(|(_, d)| d == "desc")
    }

    /// The options as _explain shows them.
    pub fn to_json(&self) -> Value {
        let sort: Map<String, Value> = self.sort.iter().map(|(f, d)| (f.clone(), Value::from(d.as_str()))).collect();
        serde_json::json!({
            "use_index": self.use_index,
            "bookmark": self.bookmark.as_deref().unwrap_or("nil"),
            "limit": self.limit,
            "skip": self.skip,
            "sort": sort,
            "fields": self.fields.clone().unwrap_or_default(),
            "partition": self.partition,
            "r": self.r,
            "conflicts": self.conflicts,
            "stale": if self.stale_ok { Value::from("ok") } else { Value::Bool(false) },
            "update": self.update,
            "stable": self.stable,
            "execution_stats": self.execution_stats,
            "allow_fallback": self.allow_fallback,
        })
    }
}

/// How an index is to be named or placed: given, or derived (auto_name).
#[derive(Clone, Debug)]
pub enum AutoName {
    Auto,
    Given(String),
}

/// A validated /_index POST.
#[derive(Clone, Debug)]
pub struct IdxCreateOpts {
    /// The index definition, as given.
    pub def: Value,
    pub kind: String,
    pub name: AutoName,
    pub ddoc: AutoName,
    pub w: u64,
    /// None for db_default.
    pub partitioned: Option<bool>,
}

/// Takes each option in turn from the request's members, as
/// mango_opts:validate/2 does, then rejects any left over.
struct Props<'a> {
    rest: Vec<(&'a String, &'a Value)>,
}

impl<'a> Props<'a> {
    fn new(body: &'a Map<String, Value>) -> Self {
        Props { rest: body.iter().collect() }
    }

    fn take(&mut self, key: &str) -> Option<&'a Value> {
        let i = self.rest.iter().position(|(k, _)| k.as_str() == key)?;
        Some(self.rest.remove(i).1)
    }

    fn required(&mut self, key: &str) -> Result<&'a Value> {
        self.take(key).ok_or_else(|| error(400, "missing_required_key", format!("Missing required key: {key}")))
    }

    fn done(self) -> Result<()> {
        match self.rest.first() {
            Some((k, _)) => Err(error(400, "invalid_key", format!("Invalid key {k} for this request."))),
            None => Ok(()),
        }
    }
}

fn is_boolean(v: Option<&Value>, default: bool) -> Result<bool> {
    match v {
        None => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(other) => Err(error(400, "invalid_boolean", format!("Invalid boolean value: {}", erl_w(other)))),
    }
}

fn is_pos_integer(v: Option<&Value>, default: u64) -> Result<u64> {
    match v {
        None => Ok(default),
        Some(Value::Number(n)) if n.as_u64().is_some_and(|n| n > 0) => Ok(n.as_u64().unwrap()),
        Some(other) => Err(error(400, "invalid_pos_integer", format!("{} is not an integer greater than zero", erl_w(other)))),
    }
}

fn is_non_neg_integer(v: Option<&Value>, default: u64) -> Result<u64> {
    match v {
        None => Ok(default),
        Some(Value::Number(n)) if n.as_u64().is_some() => Ok(n.as_u64().unwrap()),
        Some(other) => Err(error(
            400,
            "invalid_non_neg_integer",
            format!("{} is not an integer greater than or equal to zero", erl_w(other)),
        )),
    }
}

fn is_string(v: &Value) -> Result<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        other => Err(error(400, "invalid_string", format!("Invalid string: {}", erl_w(other)))),
    }
}

fn validate_non_empty_string(v: Option<&Value>) -> Result<AutoName> {
    match v {
        None => Ok(AutoName::Auto),
        Some(Value::String(s)) if s.is_empty() => Err(error(400, "invalid_empty_string", "Index name or ddoc cannot be empty string")),
        Some(other) => is_string(other).map(AutoName::Given),
    }
}

/// validate_selector/1: an object, normalized.
pub fn validate_selector(v: &Value) -> Result<Value> {
    match v {
        Value::Object(_) => normalize_selector(v),
        other => Err(error(400, "invalid_selector_json", format!("Selector must be a JSON object, not: {}", erl_w(other)))),
    }
}

fn invalid_index_name(v: &Value) -> crate::error::CouchError {
    error(400, "invalid_index_name", format!("Invalid index name: {}", erl_w(v)))
}

/// validate_use_index/1.
pub fn validate_use_index(v: &Value) -> Result<Vec<String>> {
    match v {
        Value::String(s) => {
            let parts: Vec<&str> = s.split('/').collect();
            Ok(match parts.as_slice() {
                [d] | ["_design", d] => vec![d.to_string()],
                [d, n] | ["_design", d, n] => vec![d.to_string(), n.to_string()],
                _ => return Err(invalid_index_name(v)),
            })
        }
        Value::Null => Ok(Vec::new()),
        Value::Array(a) => match a.as_slice() {
            [] => Ok(Vec::new()),
            [Value::String(d)] => Ok(vec![d.clone()]),
            [Value::String(d), Value::String(n)] => Ok(vec![d.clone(), n.clone()]),
            _ => Err(invalid_index_name(v)),
        },
        _ => Err(invalid_index_name(v)),
    }
}

/// mango_sort:new/1.
pub fn validate_sort(v: &Value) -> Result<Vec<(String, String)>> {
    let Value::Array(specs) = v else {
        return Err(error(400, "invalid_sort_json", format!("Sort must be an array of sort specs, not: {}", erl_p(v))));
    };
    let mut sort = Vec::new();
    for spec in specs {
        let bad = || error(400, "invalid_sort_field", format!("Invalid sort field: {}", erl_p(spec)));
        sort.push(match spec {
            Value::String(f) if f.is_empty() => return Err(bad()),
            Value::String(f) => (f.clone(), "asc".to_string()),
            Value::Object(o) if o.len() == 1 => match o.iter().next() {
                Some((f, Value::String(d))) if d == "asc" || d == "desc" => (f.clone(), d.clone()),
                _ => return Err(bad()),
            },
            _ => return Err(bad()),
        });
    }
    if sort.iter().any(|(_, d)| *d != sort[0].1) {
        return Err(error(400, "unsupported_mixed_sort", "Sorts currently only support a single direction for all fields."));
    }
    Ok(sort)
}

/// mango_fields:new/1: None for all_fields.
fn validate_fields(v: &Value) -> Result<Option<Vec<Value>>> {
    match v {
        Value::Array(a) if a.is_empty() => Ok(None),
        Value::Array(a) => {
            for f in a {
                if !f.is_string() && !f.is_object() {
                    return Err(error(400, "invalid_field", format!("Invalid JSON for field spec: {}", erl_w(f))));
                }
            }
            Ok(Some(a.clone()))
        }
        other => Err(error(400, "invalid_fields", format!("Fields must be an array of strings, not: {}", erl_w(other)))),
    }
}

/// couch_partition:validate_partition/1 (or "" for none).
fn validate_partition(v: &Value) -> Result<String> {
    let bad = |m: &str| error(400, "illegal_partition", m.to_string());
    match v {
        Value::String(s) if s.is_empty() => Ok(String::new()),
        Value::String(s) if s.starts_with('_') => Err(bad("Partition must not start with an underscore")),
        Value::String(s) if s.contains(':') => Err(bad("Partition must not contain a colon")),
        Value::String(s) => Ok(s.clone()),
        _ => Err(bad("Partition must be a string")),
    }
}

/// mango_opts:validate_find/1.
pub fn validate_find(body: &Map<String, Value>, server: &Server) -> Result<FindOpts> {
    let mut p = Props::new(body);
    let selector = validate_selector(p.required("selector")?)?;
    let use_index = p.take("use_index").map(validate_use_index).transpose()?.unwrap_or_default();
    let bookmark = match p.take("bookmark") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s.is_empty() => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(other) => return Err(unknown("mango_opts", &format!("{{invalid_bookmark,{}}}", erl_w(other)))),
    };
    let default_limit = server.config.get_int("mango", "default_limit", 25u64);
    let limit = is_non_neg_integer(p.take("limit"), default_limit)?;
    let skip = is_non_neg_integer(p.take("skip"), 0)?;
    let sort = p.take("sort").map(validate_sort).transpose()?.unwrap_or_default();
    let fields = p.take("fields").map(validate_fields).transpose()?.flatten();
    let partition = p.take("partition").map(validate_partition).transpose()?.unwrap_or_default();
    let r = is_pos_integer(p.take("r"), 1)?;
    let conflicts = is_boolean(p.take("conflicts"), false)?;
    let stale_ok = match p.take("stale") {
        None | Some(Value::Bool(false)) => false,
        Some(Value::String(s)) if s == "false" => false,
        Some(Value::String(s)) if s == "ok" => true,
        Some(other) => return Err(unknown("mango_opts", &format!("{{invalid_ok_or_false_value,{}}}", erl_w(other)))),
    };
    let update = is_boolean(p.take("update"), true)?;
    let stable = is_boolean(p.take("stable"), false)?;
    let execution_stats = is_boolean(p.take("execution_stats"), false)?;
    let allow_fallback = is_boolean(p.take("allow_fallback"), true)?;
    p.done()?;
    Ok(FindOpts {
        selector,
        use_index,
        bookmark,
        limit,
        skip,
        sort,
        fields,
        partition,
        r,
        conflicts,
        stale_ok,
        update,
        stable,
        execution_stats,
        allow_fallback,
    })
}

/// mango_opts:validate_idx_create/1.
pub fn validate_idx_create(body: &Map<String, Value>) -> Result<IdxCreateOpts> {
    let mut p = Props::new(body);
    let def = p.required("index")?.clone();
    let kind = p.take("type").map(is_string).transpose()?.unwrap_or_else(|| "json".into());
    let name = validate_non_empty_string(p.take("name"))?;
    let ddoc = validate_non_empty_string(p.take("ddoc"))?;
    let w = is_pos_integer(p.take("w"), 2)?;
    let partitioned = match p.take("partitioned") {
        None => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(other) => return Err(unknown("mango_opts", &format!("{{invalid_partitioned_value,{}}}", erl_w(other)))),
    };
    p.done()?;
    Ok(IdxCreateOpts { def, kind, name, ddoc, w, partitioned })
}

/// mango_opts:validate_bulk_delete/1: the design doc ids.
pub fn validate_bulk_delete(body: &Map<String, Value>) -> Result<Vec<Value>> {
    let mut p = Props::new(body);
    let docids = match p.required("docids")? {
        Value::Array(a) => {
            a.iter().try_for_each(|d| validate_use_index(d).map(|_| ()))?;
            a.clone()
        }
        other => {
            return Err(error(
                400,
                "invalid_bulk_docs",
                format!("Bulk Delete requires an array of non-null docids. Docids: {}", erl_w(other)),
            ));
        }
    };
    is_pos_integer(p.take("w"), 2)?;
    p.done()?;
    Ok(docids)
}

/// mango_opts:validate/2 for a JSON index definition
/// (mango_idx_view:opts/0): its sort fields and normalized partial
/// filter selector.
pub fn validate_json_index(def: &Map<String, Value>) -> Result<(Vec<(String, String)>, Value)> {
    let mut p = Props::new(def);
    let fields = validate_sort(p.required("fields")?)?;
    let partial = match p.take("partial_filter_selector") {
        None => Value::Object(Map::new()),
        Some(s) => validate_selector(s)?,
    };
    p.done()?;
    Ok((fields, partial))
}
