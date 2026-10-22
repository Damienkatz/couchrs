//! mango_selector: selectors normalized, documents matched against them
//! (mango_doc's field paths, mango_json's comparisons), and what the
//! index planner reads from them (has_required_fields, is_constant_field,
//! fields, and mango_idx_view's indexable fields and ranges).

use super::{erl_w, error};
use crate::collate::cmp_json;
use crate::error::{CouchError, Result};
use serde_json::{Map, Value};
use std::cmp::Ordering;

// ---------------------------------------------------------- normalizing

fn bad_arg(op: &str, arg: &Value) -> CouchError {
    error(400, "bad_arg", format!("Bad argument for operator {op}: {}", erl_w(arg)))
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
        return Err(error(400, "invalid_selector", "One or more conditions is missing a field name."));
    }
    Ok(s)
}

fn is_int(v: &Value) -> bool {
    v.is_i64() || v.is_u64()
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
        ("$mod", Value::Array(a)) if a.len() == 2 && a.iter().all(is_int) => v.clone(),
        ("$regex", Value::String(r)) => match compile_regex(r) {
            Some(_) => v.clone(),
            None => return Err(bad_arg(k, arg)),
        },
        ("$size", Value::Number(n)) if n.is_u64() => v.clone(),
        ("$text", Value::String(_) | Value::Number(_) | Value::Bool(_)) => one("$default", one("$text", arg.clone())),
        ("$default" | "$lt" | "$lte" | "$eq" | "$ne" | "$gte" | "$gt", _) => v.clone(),
        ("$where" | "$geoWithin" | "$geoIntersects" | "$near" | "$nearSphere", _) => {
            return Err(error(400, "not_supported", format!("Unsupported operator: {k}")));
        }
        (
            "$and" | "$or" | "$nor" | "$not" | "$elemMatch" | "$allMatch" | "$keyMapMatch" | "$in" | "$nin" | "$all" | "$exists"
            | "$type" | "$beginsWith" | "$mod" | "$regex" | "$size" | "$text",
            _,
        ) => return Err(bad_arg(k, arg)),
        (op, _) if op.starts_with('$') => return Err(error(400, "invalid_operator", format!("Invalid operator: {op}"))),
        (field, cond) => one(field, norm_ops(cond)?),
    })
}

fn norm_fields(v: &Value, path: &str) -> Result<Value> {
    let bad = || error(400, "bad_field", format!("Invalid field normalization on selector: {}", erl_w(v)));
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

/// A $regex as re:run/3 runs it: over the bytes of the string, ASCII
/// classes (or Unicode ones, if that's the only way the pattern
/// compiles).
fn compile_regex(r: &str) -> Option<regex::bytes::Regex> {
    let build = |unicode| regex::bytes::RegexBuilder::new(r).unicode(unicode).build().ok();
    build(false).or_else(|| build(true))
}

// --------------------------------------------------------------- fields

/// mango_util:parse_field/1.
pub use crate::nouveau::mango::parse_field;

/// What mango_doc:get_field/2 finds.
pub enum Got<'a> {
    Found(&'a Value),
    NotFound,
    BadPath,
}

/// mango_doc:get_field/2 of a parsed path: array members by position.
pub fn get_path<'a>(v: &'a Value, path: &[String]) -> Got<'a> {
    let mut cur = v;
    for name in path {
        cur = match cur {
            Value::Object(o) => match o.get(name) {
                Some(x) => x,
                None => return Got::NotFound,
            },
            Value::Array(a) => match erl_binary_to_integer(name) {
                Some(i) if i >= 0 && (i as u128) < a.len() as u128 => &a[i as usize],
                _ => return Got::BadPath,
            },
            _ => return Got::BadPath,
        };
    }
    Got::Found(cur)
}

/// binary_to_integer/1.
fn erl_binary_to_integer(s: &str) -> Option<i128> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: i128 = digits.parse().unwrap_or(i128::MAX);
    Some(if s.starts_with('-') { -n } else { n })
}

/// A document's field, if it has it (a path that's bad or missing
/// gives none, as mango_native_proc indexes them).
pub fn get_field<'a>(doc: &'a Value, field: &str) -> Option<&'a Value> {
    match get_path(doc, &parse_field(field).ok()?) {
        Got::Found(v) => Some(v),
        _ => None,
    }
}

// ------------------------------------------------------------- matching

#[derive(Clone, Copy, Debug)]
enum Op {
    Lt,
    Lte,
    Eq,
    Ne,
    Gte,
    Gt,
}

/// A normalized selector, compiled.
#[derive(Clone, Debug)]
enum Sel {
    /// {}: everything.
    Everything,
    And(Vec<Sel>),
    Or(Vec<Sel>),
    Not(Box<Sel>),
    All(Vec<Value>),
    ElemMatch(Box<Sel>),
    AllMatch(Box<Sel>),
    KeyMapMatch(Box<Sel>),
    Cmp(Op, Value),
    In(Vec<Value>),
    Nin(Vec<Value>),
    Exists(bool),
    Type(String),
    Mod(i128, i128),
    BeginsWith(String),
    Regex(Option<regex::bytes::Regex>),
    Size(u64),
    Default,
    InvalidOp(String),
    /// A condition that matches nothing (a $mod without two integers).
    Nothing,
    /// A field ("" in $elemMatch and friends: the value itself), its
    /// path (None when it doesn't parse) and condition.
    Field(String, Option<Vec<String>>, Box<Sel>),
    Unnormalized(Value),
}

fn compile(v: &Value) -> Sel {
    let Some(o) = v.as_object() else { return Sel::Unnormalized(v.clone()) };
    if o.is_empty() {
        return Sel::Everything;
    }
    let Some((k, arg)) = single(v) else { return Sel::Unnormalized(v.clone()) };
    let list = |a: &Value| a.as_array().map(|a| a.iter().map(compile).collect()).unwrap_or_default();
    let values = |a: &Value| a.as_array().cloned().unwrap_or_default();
    let int = |x: &Value| x.as_i64().map(i128::from).or_else(|| x.as_u64().map(i128::from));
    match k {
        "$and" => Sel::And(list(arg)),
        "$or" => Sel::Or(list(arg)),
        "$not" => Sel::Not(Box::new(compile(arg))),
        "$all" if arg.is_array() => Sel::All(values(arg)),
        "$elemMatch" => Sel::ElemMatch(Box::new(compile(arg))),
        "$allMatch" => Sel::AllMatch(Box::new(compile(arg))),
        "$keyMapMatch" => Sel::KeyMapMatch(Box::new(compile(arg))),
        "$lt" => Sel::Cmp(Op::Lt, arg.clone()),
        "$lte" => Sel::Cmp(Op::Lte, arg.clone()),
        "$eq" => Sel::Cmp(Op::Eq, arg.clone()),
        "$ne" => Sel::Cmp(Op::Ne, arg.clone()),
        "$gte" => Sel::Cmp(Op::Gte, arg.clone()),
        "$gt" => Sel::Cmp(Op::Gt, arg.clone()),
        "$in" if arg.is_array() => Sel::In(values(arg)),
        "$nin" if arg.is_array() => Sel::Nin(values(arg)),
        "$exists" if arg.is_boolean() => Sel::Exists(arg.as_bool().unwrap_or(false)),
        "$type" if arg.is_string() => Sel::Type(arg.as_str().unwrap_or("").to_string()),
        "$mod" => match arg.as_array().map(Vec::as_slice) {
            Some([d, r]) if int(d).is_some() && int(r).is_some() => Sel::Mod(int(d).unwrap(), int(r).unwrap()),
            _ => Sel::Nothing,
        },
        "$beginsWith" if arg.is_string() => Sel::BeginsWith(arg.as_str().unwrap_or("").to_string()),
        "$regex" => Sel::Regex(arg.as_str().and_then(compile_regex)),
        "$size" => Sel::Size(arg.as_u64().unwrap_or(u64::MAX)),
        "$default" => Sel::Default,
        op if op.starts_with('$') => Sel::InvalidOp(op.to_string()),
        field => Sel::Field(field.to_string(), parse_field(field).ok(), Box::new(compile(arg))),
    }
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

/// Erlang's term order of decoded JSON (mango_json:cmp_raw/2): numbers,
/// then atoms (false, null, true), objects, empty lists, lists and
/// binaries (by byte).
pub fn cmp_raw(a: &Value, b: &Value) -> Ordering {
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Number(_) => 0,
            Value::Bool(_) | Value::Null => 1,
            Value::Object(_) => 2,
            Value::Array(a) if a.is_empty() => 3,
            Value::Array(_) => 4,
            Value::String(_) => 5,
        }
    }
    fn atom(v: &Value) -> &'static str {
        match v {
            Value::Bool(true) => "true",
            Value::Bool(false) => "false",
            _ => "null",
        }
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64(), x.as_u64(), y.as_u64()) {
            (Some(i), Some(j), _, _) => i.cmp(&j),
            (_, _, Some(i), Some(j)) => i.cmp(&j),
            _ => x.as_f64().unwrap_or(0.0).partial_cmp(&y.as_f64().unwrap_or(0.0)).unwrap_or(Ordering::Equal),
        },
        (Value::String(x), Value::String(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::Array(x), Value::Array(y)) => {
            for (p, q) in x.iter().zip(y) {
                let c = cmp_raw(p, q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Object(x), Value::Object(y)) => {
            for ((k1, v1), (k2, v2)) in x.iter().zip(y) {
                let c = k1.as_bytes().cmp(k2.as_bytes()).then_with(|| cmp_raw(v1, v2));
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ if rank(a) == 1 && rank(b) == 1 => atom(a).cmp(atom(b)),
        _ => rank(a).cmp(&rank(b)),
    }
}

/// Erlang's =:= (exact: an integer is never a float) when `exact`, or
/// == otherwise; objects member by member, in order.
pub fn erl_eq(a: &Value, b: &Value, exact: bool) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let ints = |n: &serde_json::Number| n.is_i64() || n.is_u64();
            if ints(x) != ints(y) {
                return !exact && x.as_f64() == y.as_f64();
            }
            if ints(x) { x == y } else { x.as_f64() == y.as_f64() }
        }
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(p, q)| erl_eq(p, q, exact)),
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|((k1, v1), (k2, v2))| k1 == k2 && erl_eq(v1, v2, exact))
        }
        _ => a == b,
    }
}

fn eval(s: &Sel, v: &Value, raw: bool) -> Result<bool> {
    let cmp = |a: &Value, b: &Value| if raw { cmp_raw(a, b) } else { cmp_json(a, b) };
    Ok(match s {
        Sel::Everything => true,
        Sel::And(args) => {
            for a in args {
                if !eval(a, v, raw)? {
                    return Ok(false);
                }
            }
            true
        }
        Sel::Or(args) if args.is_empty() => true,
        Sel::Or(args) => {
            for a in args {
                if eval(a, v, raw)? {
                    return Ok(true);
                }
            }
            false
        }
        Sel::Not(a) => !eval(a, v, raw)?,
        Sel::All(args) if args.is_empty() => false,
        Sel::All(args) => match v {
            Value::Array(vals) => {
                args.iter().all(|a| vals.iter().any(|x| erl_eq(a, x, true)))
                    || matches!(args.as_slice(), [a @ Value::Array(_)] if erl_eq(a, v, false))
            }
            _ => false,
        },
        // An error in a member (a bad field name, say) is no match.
        Sel::ElemMatch(a) => match v {
            Value::Array(vals) => vals.iter().map(|x| eval(a, x, raw)).find(|r| !matches!(r, Ok(false))).is_some_and(|r| r.is_ok()),
            _ => false,
        },
        Sel::AllMatch(a) => match v {
            Value::Array(vals) if !vals.is_empty() => vals.iter().all(|x| matches!(eval(a, x, raw), Ok(true))),
            _ => false,
        },
        Sel::KeyMapMatch(a) => match v {
            Value::Object(o) => o
                .keys()
                .map(|k| eval(a, &Value::String(k.clone()), raw))
                .find(|r| !matches!(r, Ok(false)))
                .is_some_and(|r| r.is_ok()),
            _ => false,
        },
        Sel::Cmp(op, a) => {
            let c = cmp(v, a);
            match op {
                Op::Lt => c.is_lt(),
                Op::Lte => c.is_le(),
                Op::Eq => c.is_eq(),
                Op::Ne => c.is_ne(),
                Op::Gte => c.is_ge(),
                Op::Gt => c.is_gt(),
            }
        }
        Sel::In(args) if args.is_empty() => false,
        Sel::In(args) => match v {
            Value::Array(vals) => args.iter().any(|a| vals.iter().any(|x| cmp(x, a).is_eq())),
            _ => args.iter().any(|a| cmp(v, a).is_eq()),
        },
        Sel::Nin(args) if args.is_empty() => true,
        Sel::Nin(args) => match v {
            Value::Array(vals) => !args.iter().any(|a| vals.iter().any(|x| cmp(x, a).is_eq())),
            _ => args.iter().all(|a| cmp(v, a).is_ne()),
        },
        Sel::Exists(e) => *e,
        Sel::Type(t) => type_name(v) == t,
        Sel::Mod(d, r) => match v.as_i64().map(i128::from).or_else(|| v.as_u64().map(i128::from)) {
            Some(_) if *d == 0 => return Err(error(500, "unknown_error", "badarith")),
            Some(x) => x % d == *r,
            None => false,
        },
        Sel::BeginsWith(p) => v.as_str().is_some_and(|s| s.starts_with(p.as_str())),
        Sel::Regex(re) => match (v, re) {
            (Value::String(s), Some(re)) => re.is_match(s.as_bytes()),
            _ => false,
        },
        Sel::Size(n) => v.as_array().is_some_and(|a| a.len() as u64 == *n),
        Sel::Default => true,
        Sel::Nothing => false,
        Sel::InvalidOp(op) => return Err(error(400, "invalid_operator", format!("Invalid operator: {op}"))),
        Sel::Field(name, _, cond) if name.is_empty() => eval(cond, v, raw)?,
        Sel::Field(name, path, cond) => {
            let path = path.as_ref().ok_or_else(|| error(400, "invalid_field_name", format!("Invalid field name: {name}")))?;
            match get_path(v, path) {
                Got::NotFound => matches!(**cond, Sel::Exists(false)),
                Got::BadPath => false,
                Got::Found(x) => eval(cond, x, raw || name == "_id")?,
            }
        }
        Sel::Unnormalized(sel) => return Err(error(500, "unnormalized_selector", erl_w(sel))),
    })
}

/// A selector, normalized and compiled for matching.
#[derive(Clone, Debug)]
pub struct Selector {
    /// As normalized.
    pub norm: Value,
    root: Sel,
}

impl Selector {
    /// Normalize and compile a selector (an object).
    pub fn parse(v: &Value) -> Result<Selector> {
        if !v.is_object() {
            return Err(CouchError::bad_request("Selector must be a JSON object"));
        }
        Ok(Selector::from_normalized(normalize_selector(v)?))
    }

    pub fn from_normalized(norm: Value) -> Selector {
        let root = compile(&norm);
        Selector { norm, root }
    }

    /// mango_selector:match/2.
    pub fn try_matches(&self, doc: &Value) -> Result<bool> {
        crate::stat!("mango.evaluate_selector").inc();
        eval(&self.root, doc, false)
    }

    pub fn matches(&self, doc: &Value) -> bool {
        self.try_matches(doc).unwrap_or(false)
    }
}

// ------------------------------------------------------------- analyses

/// has_required_fields/2: whether a document must have every one of the
/// fields to match.
pub fn has_required_fields(sel: &Value, required: &[String]) -> bool {
    hrf(std::slice::from_ref(sel), required.to_vec()).is_empty()
}

fn hrf(sels: &[Value], mut remainder: Vec<String>) -> Vec<String> {
    for (i, s) in sels.iter().enumerate() {
        if remainder.is_empty() {
            return remainder;
        }
        if s.as_object().is_some_and(|o| o.is_empty()) {
            continue;
        }
        let Some((k, arg)) = single(s) else { continue };
        match (k, arg) {
            ("$and", Value::Array(args)) if sels.len() == 1 => return hrf(args, remainder),
            ("$or", Value::Array(args)) => {
                let mut r: Vec<String> = args.iter().flat_map(|a| hrf(std::slice::from_ref(a), remainder.clone())).collect();
                r.sort();
                r.dedup();
                return hrf(&sels[i + 1..], r);
            }
            ("$and", Value::Array(args)) => remainder = hrf(args, remainder),
            (_, cond) if single(cond).is_some_and(|(op, a)| op == "$exists" && a == &Value::Bool(false)) => {}
            (field, _) => {
                if let Some(p) = remainder.iter().position(|f| f == field) {
                    remainder.remove(p);
                }
            }
        }
    }
    remainder
}

/// is_constant_field/2: whether the selector fixes the field to one
/// value ($eq at the top, or in a top-level $and).
pub fn is_constant_field(sel: &Value, field: &str) -> bool {
    if sel.as_object().is_some_and(|o| o.is_empty()) {
        return false;
    }
    match single(sel) {
        Some(("$and", Value::Array(args))) => args.iter().any(|a| is_constant_field(a, field)),
        Some((f, cond)) if f == field => single(cond).is_some_and(|(op, _)| op == "$eq"),
        _ => false,
    }
}

/// fields/1: the fields a selector names (through $and, $or and $nor).
pub fn fields(sel: &Value) -> Vec<String> {
    match single(sel) {
        Some((op, Value::Array(args))) if op.starts_with('$') => args.iter().flat_map(fields).collect(),
        Some((field, _)) => vec![field.to_string()],
        None => Vec::new(),
    }
}

/// mango_idx_view:indexable_fields/1: fields a JSON index could narrow
/// a scan by, sorted.
pub fn indexable_fields(sel: &Value) -> Vec<String> {
    match single(sel) {
        Some(("$and", Value::Array(args))) => {
            let mut out: Vec<String> = args.iter().flat_map(indexable_fields).collect();
            out.sort();
            out.dedup();
            out
        }
        Some((op, _)) if op.starts_with('$') => Vec::new(),
        Some((field, cond)) => match single(cond) {
            Some(("$lt" | "$lte" | "$eq" | "$gt" | "$gte" | "$beginsWith" | "$exists", _)) => vec![field.to_string()],
            _ => Vec::new(),
        },
        None => Vec::new(),
    }
}

/// A range bound: mango_json's min and max, or a value.
#[derive(Clone, Debug)]
pub enum Bound {
    Min,
    Val(Value),
    Max,
}

impl Bound {
    pub fn is_special(&self) -> bool {
        !matches!(self, Bound::Val(_))
    }

    fn cmp(&self, other: &Bound) -> Ordering {
        match (self, other) {
            (Bound::Min, Bound::Min) | (Bound::Max, Bound::Max) => Ordering::Equal,
            (Bound::Min, _) | (_, Bound::Max) => Ordering::Less,
            (_, Bound::Min) | (Bound::Max, _) => Ordering::Greater,
            (Bound::Val(a), Bound::Val(b)) => cmp_json(a, b),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RangeOp {
    Lt,
    Lte,
    Eq,
    Gte,
    Gt,
}

/// A field's range, {LowCmp, Low, HighCmp, High}, or `empty` (None).
#[derive(Clone, Debug)]
pub struct Range {
    pub lop: RangeOp,
    pub low: Bound,
    pub hop: RangeOp,
    pub high: Bound,
}

/// mango_idx_view:field_ranges/1: each indexable field's range, in
/// order; a field with an empty range is the only one given.
pub fn field_ranges(sel: &Value) -> Result<Vec<(String, Option<Range>)>> {
    let mut out = Vec::new();
    for f in indexable_fields(sel) {
        let full = Range { lop: RangeOp::Gt, low: Bound::Min, hop: RangeOp::Lt, high: Bound::Max };
        match range_of(sel, &f, Some(full))? {
            None => return Ok(vec![(f, None)]),
            r => out.push((f, r)),
        }
    }
    Ok(out)
}

fn range_of(sel: &Value, field: &str, r: Option<Range>) -> Result<Option<Range>> {
    let Some(r) = r else { return Ok(None) };
    match single(sel) {
        Some(("$and", Value::Array(args))) => {
            let mut acc = Some(r);
            for a in args {
                acc = range_of(a, field, acc)?;
            }
            Ok(acc)
        }
        Some((f, cond)) if f == field => range_cond(cond, r),
        _ => Ok(Some(r)),
    }
}

/// Where a value falls against a range (range_pos/3).
#[derive(PartialEq)]
enum Pos {
    Min,
    Low,
    Mid,
    High,
    Max,
}

fn range_pos(low: &Bound, arg: &Bound, high: &Bound) -> Pos {
    match arg.cmp(low) {
        Ordering::Less => Pos::Min,
        Ordering::Equal => Pos::Low,
        Ordering::Greater => match arg.cmp(high) {
            Ordering::Less => Pos::Mid,
            Ordering::Equal => Pos::High,
            Ordering::Greater => Pos::Max,
        },
    }
}

fn range_cond(cond: &Value, r: Range) -> Result<Option<Range>> {
    let Some((op, arg)) = single(cond) else { return Ok(Some(r)) };
    let a = Bound::Val(arg.clone());
    let eq = || Some(Range { lop: RangeOp::Eq, low: a.clone(), hop: RangeOp::Eq, high: a.clone() });
    let Range { lop, low, hop, high } = r.clone();
    let pos = range_pos(&low, &a, &high);
    let low_inclusive = matches!(lop, RangeOp::Gte | RangeOp::Eq);
    let high_inclusive = matches!(hop, RangeOp::Lte | RangeOp::Eq);
    Ok(match op {
        "$beginsWith" => {
            // U+FFFF sorts highest by the collation rules.
            let Some(r) = range_cond(&one("$gte", arg.clone()), r)? else {
                return Err(error(500, "badmatch", "empty"));
            };
            let top = format!("{}\u{ffff}", arg.as_str().unwrap_or(""));
            return range_cond(&one("$lte", Value::String(top)), r);
        }
        "$lt" => match pos {
            Pos::Min | Pos::Low => None,
            Pos::Mid | Pos::High => Some(Range { lop, low, hop: RangeOp::Lt, high: a }),
            Pos::Max => Some(r),
        },
        "$lte" => match pos {
            Pos::Min => None,
            Pos::Low if low_inclusive => eq(),
            Pos::Low => None,
            Pos::Mid => Some(Range { lop, low, hop: RangeOp::Lte, high: a }),
            Pos::High | Pos::Max => Some(r),
        },
        "$eq" => match pos {
            Pos::Min => None,
            Pos::Low if low_inclusive => eq(),
            Pos::Low => None,
            Pos::Mid => eq(),
            Pos::High if high_inclusive => eq(),
            Pos::High | Pos::Max => None,
        },
        "$gte" => match pos {
            Pos::Min | Pos::Low => Some(r),
            Pos::Mid => Some(Range { lop: RangeOp::Gte, low: a, hop, high }),
            Pos::High if high_inclusive => eq(),
            Pos::High | Pos::Max => None,
        },
        "$gt" => match pos {
            Pos::Min => Some(r),
            Pos::Low | Pos::Mid => Some(Range { lop: RangeOp::Gt, low: a, hop, high }),
            Pos::High | Pos::Max => None,
        },
        _ => Some(r),
    })
}

/// is_text_search/1 of mango_idx_view, crash included: a selector with
/// a $default (text) condition, looking through lists of conditions.
pub fn is_text_search(sel: &Value) -> Result<bool> {
    let crash = || error(500, "unknown_error", "function_clause");
    match sel {
        Value::Object(o) if o.is_empty() => Ok(false),
        Value::Object(o) if o.len() == 1 => {
            let (k, cond) = o.iter().next().unwrap();
            if k == "$default" {
                return Ok(true);
            }
            match cond {
                Value::Array(conds) => {
                    for c in conds {
                        if is_text_search(c)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                Value::Object(_) => is_text_search(cond),
                _ => Ok(false),
            }
        }
        Value::Number(_) | Value::Bool(_) | Value::String(_) => Ok(false),
        _ => Err(crash()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn m(sel: Value, doc: &Value) -> bool {
        Selector::parse(&sel).unwrap().try_matches(doc).unwrap()
    }

    #[test]
    fn matches_like_couchdb() {
        let d = json!({"_id": "x", "name": "Bob", "age": 30, "tags": ["a", "b"], "addr": {"city": "Paris"}, "a.b": 1, "n": [1, 2]});
        assert!(m(json!({"name": "Bob"}), &d));
        assert!(m(json!({"age": {"$gt": 20, "$lt": 40}}), &d));
        assert!(!m(json!({"age": {"$gt": 30}}), &d));
        assert!(m(json!({"addr": {"city": "Paris"}}), &d));
        assert!(m(json!({"addr.city": {"$regex": "^Pa"}}), &d));
        assert!(m(json!({"tags": {"$elemMatch": {"$eq": "b"}}}), &d));
        assert!(m(json!({"tags": {"$all": ["a", "b"]}}), &d));
        assert!(m(json!({"tags": {"$all": [["a", "b"]]}}), &d));
        assert!(!m(json!({"n": {"$all": [1.0]}}), &d));
        assert!(m(json!({"tags": {"$size": 2}}), &d));
        assert!(m(json!({"$or": [{"name": "X"}, {"age": 30}]}), &d));
        assert!(m(json!({"nope": {"$exists": false}}), &d));
        assert!(!m(json!({"name.x": {"$exists": false}}), &d));
        assert!(!m(json!({"nope": {"$ne": 1}}), &d));
        assert!(m(json!({"age": {"$not": {"$eq": 31}}}), &d));
        assert!(m(json!({"a\\.b": 1}), &d));
        assert!(m(json!({"age": {"$in": [1, 30]}}), &d));
        assert!(m(json!({"tags": {"$in": ["b"]}}), &d));
        assert!(!m(json!({"tags": {"$in": [["a", "b"]]}}), &d));
        assert!(m(json!({"age": {"$mod": [7, 2]}}), &d));
        assert!(m(json!({"name": {"$beginsWith": "Bo"}}), &d));
        assert!(m(json!({"$nor": [{"name": "Al"}]}), &d));
        assert!(m(json!({"tags.1": "b"}), &d));
        assert!(!m(json!({"$and": [{"$eq": 1}]}), &d));
        // _id compares as Erlang terms: binaries after numbers.
        assert!(m(json!({"_id": {"$gt": 5}}), &d));
        let e = Selector::parse(&json!({"a..b": 1})).unwrap().try_matches(&d).unwrap_err();
        assert_eq!(e.error, "invalid_field_name");
        assert!(!m(json!({"tags": {"$elemMatch": {"a..b": 1}}}), &d));
    }

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
        assert_eq!(normalize_selector(&json!({"a": {"$in": {"x": 1}}})).unwrap_err().reason, "Bad argument for operator $in: {[{<<120>>,1}]}");
    }

    #[test]
    fn analyses() {
        let n = |v: Value| normalize_selector(&v).unwrap();
        let s = n(json!({"a": {"$gt": 1}, "b": 2, "c": {"$ne": 3}, "d": {"$exists": false}}));
        assert_eq!(indexable_fields(&s), ["a", "b", "d"]);
        assert!(has_required_fields(&s, &["a".into(), "c".into()]));
        assert!(!has_required_fields(&s, &["d".into()]));
        assert!(is_constant_field(&s, "b"));
        assert!(!is_constant_field(&s, "a"));
        assert_eq!(fields(&s), ["a", "b", "c", "d"]);
        let or = n(json!({"$or": [{"a": 1, "b": 2}, {"a": 3}]}));
        assert!(has_required_fields(&or, &["a".into()]));
        assert!(!has_required_fields(&or, &["b".into()]));
        let r = field_ranges(&n(json!({"a": {"$gt": 1, "$lte": 5}}))).unwrap();
        let Range { lop, low, hop, high } = r[0].1.clone().unwrap();
        assert!(lop == RangeOp::Gt && hop == RangeOp::Lte && matches!(low, Bound::Val(_)) && matches!(high, Bound::Val(_)));
        assert!(field_ranges(&n(json!({"a": {"$gt": 3, "$lt": 2}}))).unwrap()[0].1.is_none());
    }
}
