//! Mango's side of nouveau indexes: index definitions (mango_idx_nouveau),
//! what a document puts in one (mango_native_proc's nouveau_index_doc),
//! and selectors as Lucene queries over those fields
//! (mango_selector_text), quirks included: numbers written as Erlang's
//! float_to_binary writes them, and "u0x10FFFF" (an Erlang string that
//! meant \u{10FFFF}) as the top of open string ranges.

use super::index::{FType, IndexField};
use crate::error::{CouchError, Result};
use axum::http::StatusCode;
use serde_json::{Map, Value, json};

fn mango_error(status: StatusCode, error: &str, reason: impl Into<String>) -> CouchError {
    CouchError::new(status, error, reason)
}

// --------------------------------------------------------------- fields

/// mango_util:parse_field/1: a field path, split on unescaped dots.
pub fn parse_field(field: &str) -> Result<Vec<String>> {
    let bad = || mango_error(StatusCode::BAD_REQUEST, "invalid_field_name", format!("Invalid field name: {field}"));
    if !field.contains('\\') {
        let parts: Vec<String> = field.split('.').map(String::from).collect();
        if parts.iter().any(String::is_empty) {
            return Err(bad());
        }
        return Ok(parts);
    }
    let mut parts = Vec::new();
    let mut cur = String::new();
    let b: Vec<char> = field.chars().collect();
    for (i, c) in b.iter().enumerate() {
        if *c == '.' && (i == 0 || b[i - 1] != '\\') {
            parts.push(std::mem::take(&mut cur));
        } else {
            cur.push(*c);
        }
    }
    parts.push(cur);
    parts
        .into_iter()
        .map(|p| if p.is_empty() { Err(bad()) } else { Ok(p.replace('\\', "")) })
        .collect()
}

/// mango_util:lucene_escape_field/1: bytes other than letters and digits
/// as _xx.
pub fn escape_field(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() {
            out.push(b as char);
        } else {
            out.push_str(&format!("_{b:02x}"));
        }
    }
    out
}

/// mango_util:lucene_escape_user/1.
pub fn escape_user(field: &str) -> Result<String> {
    Ok(parse_field(field)?.iter().map(|p| escape_field(p)).collect::<Vec<_>>().join("."))
}

/// mango_util:lucene_escape_query_value/1.
fn escape_query_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    let mut cs = v.chars().peekable();
    while let Some(c) = cs.next() {
        if (c == '&' || c == '|') && cs.peek() == Some(&c) {
            cs.next();
            out.push('\\');
            out.push(c);
            out.push(c);
        } else if "+-(){}[]!^~*?:/\\\" \t\r\n".contains(c) {
            out.push('\\');
            out.push(c);
        } else {
            out.push(c);
        }
    }
    out
}

/// mango_util:is_number_string/1. Its pattern is unanchored, so any
/// string with a digit in it counts.
fn is_number_string(v: &str) -> bool {
    v.chars().any(char::is_numeric) || v.contains("NaN") || v.contains("Infinity")
}

/// float_to_binary/1: scientific, twenty digits after the point.
pub fn erl_float(f: f64) -> String {
    let s = format!("{f:.20e}");
    let (m, e) = s.split_once('e').unwrap_or((&s, "0"));
    let e: i32 = e.parse().unwrap_or(0);
    format!("{m}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs())
}

fn value_str(v: &Value) -> String {
    match v {
        Value::String(s) if is_number_string(s) => format!("\"{s}\""),
        Value::String(s) => format!("\"{}\"", escape_query_value(s)),
        Value::Number(n) if n.is_i64() || n.is_u64() => n.to_string(),
        Value::Number(n) => erl_float(n.as_f64().unwrap_or(0.0)),
        Value::Bool(b) => b.to_string(),
        _ => "true".into(),
    }
}

fn type_str(v: &Value) -> Result<&'static str> {
    Ok(match v {
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::String(_) => "string",
        Value::Null => "null",
        _ => return Err(CouchError::internal(format!("no text type for {v}"))),
    })
}

// ------------------------------------------------------------ selectors

/// mango_selector_text's abstract text selector.
#[derive(Debug, Clone)]
enum T {
    And(Vec<T>),
    Or(Vec<T>),
    /// Fields that exist, and (unless None) not the query.
    Not(Box<T>, Option<Box<T>>),
    Field(String, String),
    Null(String, String),
    FieldName(String, &'static str),
    Regex(String),
    Default(String),
}

/// The path (innermost part first, as convert/2 carries it) as a name.
fn path_str(path: &[String]) -> String {
    let mut acc: Vec<String> = Vec::new();
    let n = path.len();
    for (i, part) in path.iter().enumerate() {
        if i == n - 1 {
            acc.insert(0, part.clone());
        } else if !part.is_empty() {
            acc.insert(0, part.clone());
            acc.insert(0, ".".into());
        }
    }
    acc.concat()
}

fn make_field(path: &[String], v: &Value) -> Result<String> {
    Ok(format!("{}:{}", path_str(path), type_str(v)?))
}

fn length_field(path: &[String]) -> String {
    format!("{}:length", path_str(path))
}

fn with(part: &str, path: &[String]) -> Vec<String> {
    let mut p = vec![part.to_string()];
    p.extend_from_slice(path);
    p
}

fn field_exists(path: &[String]) -> T {
    let p = path_str(path);
    T::Or(vec![T::FieldName(format!("{p}:"), "*"), T::FieldName(p, ".*")])
}

fn range(op: &str, v: &Value) -> String {
    let num = v.is_number();
    let (min, max) = if num { ("*", "*") } else { ("\"\"", "u0x10FFFF") };
    let vs = value_str(v);
    match op {
        "$lt" => format!("[{min} TO {vs}}}"),
        "$lte" => format!("[{min} TO {vs}]"),
        "$gte" => format!("[{vs} TO {max}]"),
        _ => format!("{{{vs} TO {max}]"),
    }
}

fn one(k: &str, v: Value) -> Value {
    let mut m = Map::new();
    m.insert(k.to_string(), v);
    Value::Object(m)
}

fn convert(path: &[String], sel: &Value) -> Result<T> {
    let (k, arg) = match sel {
        Value::Object(o) if o.len() == 1 => o.iter().next().map(|(k, v)| (k.as_str(), v)).unwrap(),
        Value::Object(_) | Value::Array(_) => return Err(CouchError::internal(format!("unnormalized_selector {sel}"))),
        v => return Ok(T::Field(make_field(path, v)?, value_str(v))),
    };
    let comparable = |a: &Value| !(a.is_array() || a.is_object() || a.is_null());
    Ok(match k {
        "$and" => T::And(arg.as_array().map(|a| a.iter().map(|x| convert(path, x)).collect::<Result<_>>()).transpose()?.unwrap_or_default()),
        "$or" => T::Or(arg.as_array().map(|a| a.iter().map(|x| convert(path, x)).collect::<Result<_>>()).transpose()?.unwrap_or_default()),
        "$not" => T::Not(Box::new(field_exists(path)), Some(Box::new(convert(path, arg)?))),
        "$default" => match convert(path, arg)? {
            T::Field(_, q) => T::Default(q),
            _ => return Err(CouchError::internal("bad $default")),
        },
        "$text" if arg.is_string() => T::Field(make_field(path, arg)?, value_str(arg)),
        "$all" => match arg.as_array().map(Vec::as_slice) {
            Some([v @ Value::Array(_)]) => T::Or(vec![
                convert(path, &one("$eq", v.clone()))?,
                convert(&with("[]", path), &one("$eq", v.clone()))?,
            ]),
            _ => convert(&with("[]", path), &one("$and", arg.clone()))?,
        },
        "$elemMatch" | "$allMatch" => convert(&with("[]", path), arg)?,
        "$lt" | "$lte" | "$gt" | "$gte" if !comparable(arg) => field_exists(path),
        "$lt" | "$lte" | "$gt" | "$gte" => T::Field(make_field(path, arg)?, range(k, arg)),
        "$eq" => match arg {
            Value::Null => T::Null(make_field(path, arg)?, value_str(arg)),
            Value::Array(items) => {
                let p0 = with("[]", path);
                let mut parts = vec![T::Field(length_field(&p0), items.len().to_string())];
                for x in items {
                    parts.push(convert(&p0, &one("$eq", x.clone()))?);
                }
                T::And(parts)
            }
            Value::Object(_) => convert(path, arg)?,
            v => T::Field(make_field(path, v)?, value_str(v)),
        },
        "$ne" => T::Not(Box::new(field_exists(path)), Some(Box::new(convert(path, &one("$eq", arg.clone()))?))),
        "$in" => T::Or(convert_in(path, arg)?),
        "$nin" => T::Not(Box::new(field_exists(path)), Some(Box::new(convert(path, &one("$in", arg.clone()))?))),
        "$exists" => match arg {
            Value::Bool(false) => T::Not(Box::new(field_exists(path)), None),
            _ => field_exists(path),
        },
        "$beginsWith" if arg.is_string() => {
            T::Field(make_field(path, arg)?, format!("{}*", escape_query_value(arg.as_str().unwrap_or_default())))
        }
        "$type" => field_exists(path),
        "$mod" => T::FieldName(format!("{}:", path_str(path)), "number"),
        "$regex" => T::Regex(path_str(path)),
        "$size" => T::Field(length_field(&with("[]", path)), value_str(arg)),
        op if op.starts_with('$') => {
            return Err(mango_error(StatusCode::BAD_REQUEST, "invalid_operator", format!("Invalid operator: {op}")));
        }
        field => {
            let pp0 = if field.is_empty() { Vec::new() } else { parse_field(field)? };
            // Dots in parts are escaped again, for the escaping later.
            let pp1: Vec<String> = pp0.iter().map(|p| p.replace('.', "\\.")).collect();
            let is_int = |p: &str| {
                let d = p.strip_prefix(['+', '-']).unwrap_or(p);
                !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit())
            };
            let has_int = pp1.iter().any(|p| is_int(p));
            let mut new_path: Vec<String> = pp1.iter().rev().map(|p| if is_int(p) { "[]".to_string() } else { p.clone() }).collect();
            new_path.extend_from_slice(path);
            if has_int {
                let mut old_path: Vec<String> = pp1.iter().rev().cloned().collect();
                old_path.extend_from_slice(path);
                T::Or(vec![convert(&old_path, arg)?, convert(&new_path, arg)?])
            } else {
                convert(&new_path, arg)?
            }
        }
    })
}

fn convert_in(path: &[String], args: &Value) -> Result<Vec<T>> {
    let p0 = with("[]", path);
    let mut out = Vec::new();
    for a in args.as_array().into_iter().flatten() {
        match a {
            Value::Object(o) => {
                let mut parts = Vec::new();
                for (k, v) in o {
                    let sub = one(k, v.clone());
                    parts.push(T::Or(vec![convert(path, &sub)?, convert(&p0, &sub)?]));
                }
                out.push(T::Or(parts));
            }
            v => out.push(T::Or(vec![T::Field(make_field(path, v)?, value_str(v)), T::Field(make_field(&p0, v)?, value_str(v))])),
        }
    }
    Ok(out)
}

fn to_query(t: &T) -> Result<String> {
    let nested = |args: &[T], sep: &str| -> Result<String> {
        let parts: Vec<String> = args.iter().map(to_query).collect::<Result<Vec<_>>>()?.into_iter().filter(|q| !q.is_empty()).collect();
        Ok(if parts.is_empty() { String::new() } else { format!("({})", parts.join(sep)) })
    };
    Ok(match t {
        T::And(a) => nested(a, " AND ")?,
        T::Or(a) => nested(a, " OR ")?,
        T::Not(exists, Some(q)) => match to_query(q)? {
            s if s.is_empty() => format!("({})", to_query(exists)?),
            s => format!("({} AND NOT ({s}))", to_query(exists)?),
        },
        T::Not(exists, None) => format!("($fieldnames:/.*/  AND NOT ({}))", to_query(exists)?),
        T::Field(name, v) | T::Null(name, v) => format!("({}:{v})", escape_user(name)?),
        T::FieldName(name, wild) => format!("($fieldnames:{}{wild})", escape_user(name)?),
        T::Regex(name) => format!("($fieldnames:{}string)", escape_user(&format!("{name}:"))?),
        T::Default(v) => format!("($default:{v})"),
    })
}

/// mango_selector_text:convert/1, of a normalized selector.
pub fn selector_query(normalized: &Value) -> Result<String> {
    to_query(&convert(&[], normalized)?)
}

/// mango_idx_nouveau:indexable_fields/1: the "field:type" names a
/// selector needs indexed.
pub fn indexable_fields(normalized: &Value) -> Result<Vec<String>> {
    if normalized.as_object().is_some_and(Map::is_empty) {
        return Ok(Vec::new());
    }
    fn walk(t: &T, out: &mut Vec<String>) {
        match t {
            T::And(a) => a.iter().for_each(|x| walk(x, out)),
            // $in and array positions make two fields of one; only the
            // first is declared in an index.
            T::Or(a) if a.len() == 2 && matches!((&a[0], &a[1]), (T::Field(..), T::Field(..))) => {
                walk(&a[0], out);
                if let T::Field(n, _) = &a[1]
                    && !n.contains("[]") {
                        walk(&a[1], out);
                    }
            }
            T::Or(a) => a.iter().for_each(|x| walk(x, out)),
            T::Not(exists, Some(q)) => {
                walk(exists, out);
                walk(q, out);
            }
            T::Not(_, None) => out.clear(),
            T::Field(n, _) if n.ends_with(":length") && n.contains("[]") => {}
            T::Field(n, _) => out.push(n.clone()),
            T::FieldName(..) | T::Null(..) => {}
            T::Regex(n) => out.push(format!("{n}:string")),
            T::Default(_) => out.push("$default".into()),
        }
    }
    let mut out = Vec::new();
    walk(&convert(&[], normalized)?, &mut out);
    let mut seen = std::collections::HashSet::new();
    out.retain(|f| seen.insert(f.clone()));
    Ok(out)
}

// ---------------------------------------------------------- definitions

fn invalid_fields(def: &Value) -> CouchError {
    mango_error(
        StatusCode::BAD_REQUEST,
        "invalid_index_fields_definition",
        format!(
            "Text Index field definitions must be of the form\n            {{\"name\": \"non-empty fieldname\", \"type\":\n                \"boolean,number,string or text\"}}. Def: {def}"
        ),
    )
}

/// mango_idx_nouveau:validate_new/2 (mango_opts:validate with its opts):
/// the definition as stored, every option given or defaulted, in order.
pub fn validate_def(index: &Value) -> Result<Value> {
    let Value::Object(props) = index else {
        return Err(mango_error(StatusCode::BAD_REQUEST, "invalid_index", format!("Text indexes must be an object, not: {index}")));
    };
    let known = ["default_analyzer", "default_field", "partial_filter_selector", "selector", "fields", "index_array_lengths"];
    if let Some(bad) = props.keys().find(|k| !known.contains(&k.as_str())) {
        return Err(mango_error(StatusCode::BAD_REQUEST, "invalid_index", format!("Text indexes must be an object, not: {bad}")));
    }
    let mut def = Map::new();
    def.insert("default_analyzer".into(), props.get("default_analyzer").cloned().unwrap_or(json!("keyword")));
    def.insert("default_field".into(), props.get("default_field").cloned().unwrap_or(json!({})));
    for k in ["partial_filter_selector", "selector"] {
        let s = props.get(k).cloned().unwrap_or(json!({}));
        if !s.is_object() {
            return Err(mango_error(StatusCode::BAD_REQUEST, "invalid_selector_json", format!("Selector must be a JSON object, not: {s}")));
        }
        crate::mango::Selector::parse(&s)?;
        def.insert(k.into(), s);
    }
    let fields = match props.get("fields") {
        None => json!("all_fields"),
        Some(Value::String(s)) if s == "all_fields" => json!("all_fields"),
        Some(Value::Array(fs)) if fs.is_empty() => json!("all_fields"),
        Some(f @ Value::Array(fs)) => {
            for x in fs {
                let ok = x.as_object().is_some_and(|o| {
                    o.len() == 2
                        && o.get("name").and_then(Value::as_str).is_some_and(|n| !n.is_empty())
                        && o.get("type").and_then(Value::as_str).is_some_and(|t| ["text", "string", "number", "boolean"].contains(&t))
                });
                if !ok {
                    return Err(invalid_fields(f));
                }
            }
            f.clone()
        }
        Some(f) => return Err(invalid_fields(f)),
    };
    def.insert("fields".into(), fields);
    let lengths = props.get("index_array_lengths").cloned().unwrap_or(json!(true));
    if !lengths.is_boolean() {
        return Err(mango_error(StatusCode::BAD_REQUEST, "invalid_bool", format!("Invalid boolean value: {lengths}")));
    }
    def.insert("index_array_lengths".into(), lengths);
    Ok(Value::Object(def))
}

/// The ddoc entry an index definition is stored as (mango_idx_nouveau's
/// make_text/1).
pub fn ddoc_entry(def: &Value) -> Value {
    let (enabled, analyzer) = default_field(def);
    let analyzer_def = if enabled {
        json!({"name": "perfield", "default": def.get("default_analyzer").cloned().unwrap_or(json!("keyword")), "fields": {"$default": analyzer}})
    } else {
        json!("keyword")
    };
    json!({"index": def, "analyzer": analyzer_def})
}

fn default_field(def: &Value) -> (bool, Value) {
    match def.get("default_field") {
        Some(Value::Bool(b)) => (*b, json!("standard")),
        Some(Value::Object(o)) => (
            o.get("enabled").and_then(Value::as_bool).unwrap_or(true),
            o.get("analyzer").cloned().unwrap_or(json!("standard")),
        ),
        _ => (true, json!("standard")),
    }
}

/// mango_idx_nouveau:to_json's def.
pub fn def_to_json(def: &Value) -> Value {
    let mut out = Map::new();
    for (k, v) in def.as_object().into_iter().flatten() {
        match (k.as_str(), v) {
            ("fields", Value::String(_)) => {
                out.insert(k.clone(), json!([]));
            }
            ("fields", Value::Array(fs)) => {
                let fs: Vec<Value> = fs
                    .iter()
                    .map(|f| {
                        let n = f.get("name").and_then(Value::as_str).unwrap_or_default();
                        json!({ n: f.get("type").cloned().unwrap_or(Value::Null) })
                    })
                    .collect();
                out.insert(k.clone(), Value::Array(fs));
            }
            ("partial_filter_selector", Value::Object(o)) if o.is_empty() => {}
            _ => {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Object(out)
}

/// mango_idx_nouveau:columns/1: None for all fields.
pub fn columns(def: &Value) -> Option<Vec<String>> {
    let fields = def.get("fields")?.as_array().filter(|f| !f.is_empty())?;
    let mut cols = Vec::new();
    if default_field(def).0 {
        cols.push("$default".to_string());
    }
    for f in fields {
        cols.push(format!(
            "{}:{}",
            f.get("name").and_then(Value::as_str).unwrap_or_default(),
            f.get("type").and_then(Value::as_str).unwrap_or_default()
        ));
    }
    Some(cols)
}

/// The definition's partial filter (or legacy "selector"), if any.
pub fn partial_filter(def: &Value) -> Option<Value> {
    ["partial_filter_selector", "selector"].iter().find_map(|k| def.get(*k).filter(|s| s.as_object().is_some_and(|o| !o.is_empty())).cloned())
}

// ------------------------------------------------------------- indexing

/// mango_native_proc:get_nouveau_entries/2: the fields a document gives a
/// Mango nouveau index (none, if it doesn't pass the filter).
pub fn index_doc(def: &Value, doc: &Value) -> Result<Vec<IndexField>> {
    if doc.get("_id").and_then(Value::as_str).is_some_and(|id| id.starts_with("_design/")) {
        return Ok(Vec::new());
    }
    if let Some(p) = partial_filter(def) {
        let sel = crate::mango::Selector::parse(&crate::mango::normalize_selector(&p)?)?;
        if !sel.matches(doc) {
            return Ok(Vec::new());
        }
    }
    let wanted: Option<Vec<String>> = match def.get("fields") {
        Some(Value::Array(fs)) if !fs.is_empty() => Some(
            fs.iter()
                .filter_map(|f| {
                    let name = f.get("name")?.as_str()?;
                    let ty = match f.get("type").and_then(Value::as_str) {
                        Some(t @ ("number" | "boolean" | "text")) => t,
                        _ => "string",
                    };
                    escape_user(&format!("{name}:{ty}")).ok()
                })
                .collect(),
        ),
        _ => None,
    };
    let lengths = def.get("index_array_lengths").and_then(Value::as_bool).unwrap_or(true);
    let mut fields: Vec<(String, &'static str, Value)> = Vec::new();
    values(doc, &mut Vec::new(), lengths, wanted.as_deref(), &mut fields);
    if default_field(def).0 {
        let defaults: Vec<(String, &'static str, Value)> =
            fields.iter().filter(|f| f.1 == "string").map(|f| ("$default".to_string(), "text", f.2.clone())).collect();
        fields.splice(0..0, defaults.into_iter().rev());
    }
    let names: std::collections::BTreeSet<String> = fields.iter().map(|f| f.0.clone()).collect();
    let mut out: Vec<IndexField> =
        names.into_iter().map(|n| IndexField { ty: FType::String, name: "$fieldnames".into(), value: json!(n), store: false }).collect();
    for (name, ty, v) in fields {
        let (ty, value) = match (ty, v) {
            ("string", v) => (FType::String, v),
            ("text", v) => (FType::Text, v),
            ("number", v) => (FType::Double, v),
            ("boolean", Value::Bool(b)) => (FType::String, json!(b.to_string())),
            // Nouveau has no field for a null.
            _ => continue,
        };
        out.push(IndexField { ty, name, value, store: false });
    }
    Ok(out)
}

/// The name of a value's field: its path, the last part typed, each
/// part escaped.
fn text_field_name(path: &[String], ty: &str) -> String {
    let n = path.len();
    path.iter().enumerate().map(|(i, p)| if i + 1 == n { escape_field(&format!("{p}:{ty}")) } else { escape_field(p) }).collect::<Vec<_>>().join(".")
}

fn values(v: &Value, path: &mut Vec<String>, lengths: bool, wanted: Option<&[String]>, out: &mut Vec<(String, &'static str, Value)>) {
    let push = |path: &[String], ty: &'static str, val: Value, out: &mut Vec<(String, &'static str, Value)>| {
        let name = text_field_name(path, ty);
        if wanted.is_none_or(|w| w.contains(&name)) {
            out.push((name, ty, val));
        }
    };
    match v {
        Value::Object(o) => {
            for (k, x) in o {
                path.push(k.clone());
                values(x, path, lengths, wanted, out);
                path.pop();
            }
        }
        Value::Array(a) => {
            path.push("[]".into());
            if lengths {
                // The length field isn't one of the declared ones.
                out.push((text_field_name(path, "length"), "number", json!(a.len())));
            }
            for x in a {
                values(x, path, lengths, wanted, out);
            }
            path.pop();
        }
        Value::String(_) => push(path, "string", v.clone(), out),
        Value::Number(_) => push(path, "number", v.clone(), out),
        Value::Bool(_) => push(path, "boolean", v.clone(), out),
        Value::Null => push(path, "null", json!(true), out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(sel: Value) -> String {
        selector_query(&crate::mango::normalize_selector(&sel).unwrap()).unwrap()
    }

    #[test]
    fn queries() {
        assert_eq!(q(json!({"bar": {"$gt": 5}})), "(bar_3anumber:{5 TO *])");
        assert_eq!(q(json!({"foo": {"$eq": "foo"}})), "(foo_3astring:\"foo\")");
        assert_eq!(q(json!({"$text": "hello"})), "($default:\"hello\")");
        assert_eq!(q(json!({"a.b": {"$lt": "x y"}})), "(a.b_3astring:[\"\" TO \"x\\ y\"})");
        assert_eq!(q(json!({"n": 1.5})), "(n_3anumber:1.50000000000000000000e+00)");
        assert_eq!(q(json!({"f": {"$exists": true}})), "(($fieldnames:f_3a*) OR ($fieldnames:f.*))");
        assert_eq!(erl_float(12.0), "1.20000000000000000000e+01");
        assert_eq!(erl_float(0.001), "1.00000000000000002082e-03");
    }

    #[test]
    fn indexing() {
        let def = validate_def(&json!({"fields": [{"name": "foo", "type": "string"}, {"name": "bar", "type": "number"}]})).unwrap();
        let f = index_doc(&def, &json!({"_id": "d", "foo": "x", "bar": 2, "baz": "no"})).unwrap();
        let got: Vec<(String, String)> = f.iter().map(|f| (f.name.clone(), f.value.to_string())).collect();
        assert_eq!(
            got,
            [
                ("$fieldnames".into(), "\"$default\"".into()),
                ("$fieldnames".into(), "\"bar_3anumber\"".into()),
                ("$fieldnames".into(), "\"foo_3astring\"".into()),
                ("$default".into(), "\"x\"".into()),
                ("foo_3astring".into(), "\"x\"".into()),
                ("bar_3anumber".into(), "2".into()),
            ]
        );
        assert_eq!(indexable_fields(&crate::mango::normalize_selector(&json!({"$text": "hi", "bar": {"$gt": 1}})).unwrap()).unwrap(), ["$default", "bar:number"]);
    }
}
