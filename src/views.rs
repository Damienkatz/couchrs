//! Map/reduce view indexes, a port of couch_mrview / couch_index.
//!
//! Each design doc's views form an index group stored in its own file named
//! by the group signature, so editing a ddoc builds a new index while an
//! identical ddoc (even under a different id) shares one. Indexes update
//! incrementally from the database's by-seq tree.

use crate::api::{QueryArgs, blocking};
use crate::btree::{Action, Btree, Dir, NodePtr, Range, Spec};
use crate::collate::{Json, cmp_json, cmp_json_raw};
use crate::db::{Db, PurgeInfo, Snapshot};
use crate::doc::{DocOpts, FullDocInfo};
use crate::error::{CouchError, Result};
use crate::file::CouchFile;
use crate::http::{Req, json_response, not_allowed, ok_json};
use crate::js::JsFuns;
use crate::extsort::Sorter;
use crate::mango::Selector;
use crate::server::Server;
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
    /// _top_N and _bottom_N: the N greatest (or least) distinct values.
    Top(usize),
    Bottom(usize),
    /// _first and _last: the value of the least (or greatest) key and id,
    /// kept as [key, id, value] until finalized.
    First,
    Last,
    /// A JavaScript reduce: the functions, its index, and its source.
    Js(JsFuns, usize, Arc<str>),
}

pub struct ViewSpec {
    pub raw: bool,
    pub reducer: Reducer,
}

/// A reduction that failed inside the index (where couch_btree's reduce
/// throws): the update fails, as CouchDB's index updater does.
fn check_failures(st: &IndexState) -> Result<()> {
    for v in &st.views {
        if let Some(VRed { red: Some(j), .. }) = v.full_reduce()
            && let Some((e, r)) = as_failure(&j.0)
        {
            return Err(CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, &e, r).with_ref());
        }
    }
    Ok(())
}

/// A reduction couchjs refused as not reducing ({reduce_overflow_error, Msg}).
fn is_overflow(v: &Value) -> bool {
    v.as_object().is_some_and(|o| o.len() == 1 && o.contains_key("reduce_overflow_error"))
}

fn js_len(s: &str) -> usize {
    s.encode_utf16().count()
}

/// couchjs' reduce_limit (runReduce/5): a reduction longer than
/// reduce_limit_threshold, and more than 1/reduce_limit_ratio of the
/// input line less the function's source, is an overflow error (only
/// logged with "log").
fn reduce_limit(op: &str, payload: &Value, ctx: &str, src: &str, out: Value) -> Value {
    let config = crate::stats::config();
    let get = |k: &str| config.and_then(|c| c.get("query_server_config", k));
    let limit = get("reduce_limit").unwrap_or_else(|| "true".into());
    if limit != "true" && limit != "log" {
        return out;
    }
    let threshold: f64 = get("reduce_limit_threshold").and_then(|t| t.parse().ok()).unwrap_or(5000.0);
    let ratio: f64 = get("reduce_limit_ratio").and_then(|r| r.parse().ok()).unwrap_or(2.0);
    let len = js_len(&serde_json::to_string(&[&out]).unwrap_or_default());
    if (len as f64) <= threshold {
        return out;
    }
    // The line couchjs read: [op, [src], payload, ctx].
    let line = js_len(&crate::json::to_string(&json!([op, [src], payload, ctx])));
    let input = line - js_len(src);
    if (len as f64) * ratio <= input as f64 {
        return out;
    }
    let msg = format!("Reduce output must shrink more rapidly: input size: {input} output size: {len} context: {ctx}");
    if limit == "log" {
        tracing::warn!("reduce_overflow_error: {msg}");
        return out;
    }
    json!({ "reduce_overflow_error": msg })
}

impl ViewSpec {
    /// os_rereduce/4: an overflow below is the answer; otherwise the
    /// function's, limited.
    fn js_rereduce(&self, f: &JsFuns, i: usize, src: &str, vals: &[Value], ctx: &str) -> Value {
        if let Some(o) = vals.iter().find(|v| is_overflow(v)) {
            return o.clone();
        }
        let out = f.rereduce(i, vals).unwrap_or(Value::Null);
        reduce_limit("rereduce", &Value::Array(vals.to_vec()), ctx, src, out)
    }

    /// What fabric's coordinator makes of a reduction: rereduced once more
    /// (with the view as context), and finalized.
    fn coordinator(&self, v: Value, ctx: &str) -> Value {
        match &self.reducer {
            Reducer::Js(f, i, src) => self.js_rereduce(f, *i, src, std::slice::from_ref(&v), ctx),
            _ => v,
        }
    }

    pub fn cmp_key(&self, a: &Value, b: &Value) -> Ordering {
        if self.raw { cmp_json_raw(a, b) } else { cmp_json(a, b) }
    }

    fn reduce_values(&self, kvs: &[(Value, Value)]) -> Option<Value> {
        Some(match &self.reducer {
            Reducer::None => return None,
            Reducer::Count => json!(kvs.len()),
            Reducer::Sum => builtin_sum(kvs.iter().map(|(_, v)| v)),
            Reducer::Stats => builtin_stats(kvs.iter().map(|(_, v)| v.clone()).collect()),
            Reducer::ApproxCountDistinct => {
                let mut h = Hll::new();
                for (k, _) in kvs {
                    let key = k.get(0).cloned().unwrap_or(Value::Null);
                    h.add(&key);
                }
                h.to_json()
            }
            Reducer::Top(n) => self.ranked(kvs.iter().map(|(_, v)| v.clone()).collect(), *n, true),
            Reducer::Bottom(n) => self.ranked(kvs.iter().map(|(_, v)| v.clone()).collect(), *n, false),
            Reducer::First | Reducer::Last => {
                let rows = kvs.iter().map(|(k, v)| json!([k.get(0).cloned().unwrap_or(Value::Null), k.get(1).cloned().unwrap_or(Value::Null), v]));
                self.first_last(rows.collect(), matches!(self.reducer, Reducer::First))
            }
            // couchjs' runReduce: a function that throws reduces to null.
            Reducer::Js(f, i, src) => {
                let out = f.reduce(*i, kvs).unwrap_or(Value::Null);
                let kvs = Value::Array(kvs.iter().map(|(k, v)| json!([k, v])).collect());
                reduce_limit("reduce", &kvs, "", src, out)
            }
        })
    }

    /// builtin_rank_n/4: values merged in rank order, equal ones once.
    fn ranked(&self, mut vals: Vec<Value>, n: usize, top: bool) -> Value {
        vals.sort_by(|a, b| if top { self.cmp_key(b, a) } else { self.cmp_key(a, b) });
        vals.dedup_by(|a, b| self.cmp_key(a, b) == Ordering::Equal);
        vals.truncate(n);
        Value::Array(vals)
    }

    /// builtin_first_last/3, over [key, id, value] rows.
    fn first_last(&self, rows: Vec<Value>, first: bool) -> Value {
        let cmp = |a: &Value, b: &Value| {
            self.cmp_key(&a[0], &b[0]).then_with(|| a[1].as_str().unwrap_or("").cmp(b[1].as_str().unwrap_or("")))
        };
        let best = rows.into_iter().reduce(|acc, r| {
            let o = cmp(&r, &acc);
            if (first && o == Ordering::Less) || (!first && o == Ordering::Greater) { r } else { acc }
        });
        best.unwrap_or(json!([]))
    }

    pub fn rereduce_values(&self, vals: &[Value]) -> Option<Value> {
        // A failure below fails the whole.
        if let Some(f) = vals.iter().find(|v| as_failure(v).is_some()) {
            return Some(f.clone());
        }
        Some(match &self.reducer {
            Reducer::None => return None,
            Reducer::Count => builtin_sum(vals.iter()),
            Reducer::Sum => builtin_sum(vals.iter()),
            Reducer::Stats => builtin_stats(vals.to_vec()),
            Reducer::ApproxCountDistinct => {
                let mut h = Hll::new();
                for v in vals {
                    h.merge(&Hll::from_json(v));
                }
                h.to_json()
            }
            Reducer::Top(n) | Reducer::Bottom(n) => {
                let all = vals.iter().flat_map(|v| v.as_array().cloned().unwrap_or_default()).collect();
                self.ranked(all, *n, matches!(self.reducer, Reducer::Top(_)))
            }
            Reducer::First | Reducer::Last => {
                let rows = vals.iter().filter(|v| v.as_array().is_some_and(|a| a.len() == 3)).cloned().collect();
                self.first_last(rows, matches!(self.reducer, Reducer::First))
            }
            // os_rereduce/4: an overflow below is the answer.
            Reducer::Js(f, i, src) => self.js_rereduce(f, *i, src, vals, ""),
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

// ------------------------------------------------- builtin reductions
//
// couch_query_servers' _sum and _stats. Where CouchDB throws inside the
// B-tree a reduction is a failure, and the index update fails with it;
// _sum's own errors are values, as CouchDB catches them.

const SUM_ERROR: &str = "The _sum function requires that map values be numbers, arrays of numbers, or objects. Objects cannot be mixed with other data structures. Objects can be arbitrarily nested, provided that the values for all fields are themselves numbers, arrays of numbers, or objects.";

/// The member a failed reduction is kept under.
const FAILURE: &str = "\u{0}couchdb_rs_reduce_failure";

fn failure(error: &str, reason: String) -> Value {
    json!({ FAILURE: [error, reason] })
}

/// A failed reduction's error and reason.
fn as_failure(v: &Value) -> Option<(String, String)> {
    let f = v.as_object().filter(|o| o.len() == 1)?.get(FAILURE)?.as_array()?;
    Some((f.first()?.as_str()?.to_string(), f.get(1)?.as_str()?.to_string()))
}

/// A number mid-reduction, kept native so a long _sum or _stats doesn't
/// reparse and reformat its accumulator at each step.
#[derive(Clone)]
enum Num {
    Int(i128),
    Float(f64),
    /// An integer beyond 128 bits, as written.
    Big(serde_json::Number),
}

impl Num {
    fn of(n: &serde_json::Number) -> Num {
        match crate::json::int128(n) {
            Some(i) => Num::Int(i),
            None if crate::json::is_integer(n) => Num::Big(n.clone()),
            None => Num::Float(n.as_f64().unwrap_or(0.0)),
        }
    }

    fn f64(&self) -> f64 {
        match self {
            Num::Int(i) => *i as f64,
            Num::Float(f) => *f,
            // Beyond doubles it's infinite, and so badarith, as in Erlang.
            Num::Big(n) => n.as_str().parse().unwrap_or(0.0),
        }
    }

    /// Erlang arithmetic: integers stay integers.
    fn arith(&self, o: &Num, op: fn(i128, i128) -> Option<i128>, fop: fn(f64, f64) -> f64) -> Result<Num> {
        if let (Num::Int(x), Num::Int(y)) = (self, o)
            && let Some(v) = op(*x, *y)
        {
            return Ok(Num::Int(v));
        }
        let f = fop(self.f64(), o.f64());
        if !f.is_finite() {
            return Err(CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, "unknown_error", "badarith"));
        }
        Ok(Num::Float(f))
    }

    fn add(&self, o: &Num) -> Result<Num> {
        self.arith(o, i128::checked_add, |x, y| x + y)
    }

    fn cmp(&self, o: &Num) -> Ordering {
        // Integers this small are exact as doubles.
        const EXACT: u128 = 1 << 53;
        match (self, o) {
            (Num::Int(x), Num::Int(y)) => x.cmp(y),
            (Num::Float(x), Num::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
            (Num::Int(x), Num::Float(y)) if x.unsigned_abs() <= EXACT => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
            (Num::Float(x), Num::Int(y)) if y.unsigned_abs() <= EXACT => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
            _ => crate::json::cmp_numbers(&self.number(), &o.number()),
        }
    }

    fn number(&self) -> serde_json::Number {
        match self {
            Num::Int(i) => serde_json::Number::from_string_unchecked(i.to_string()),
            Num::Float(f) => serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0)),
            Num::Big(n) => n.clone(),
        }
    }

    fn value(&self) -> Value {
        Value::Number(self.number())
    }
}

fn add(a: &serde_json::Number, b: &serde_json::Number) -> Result<Value> {
    Num::of(a).add(&Num::of(b)).map(|n| n.value())
}

/// Why a _sum stopped: an error object to return, or a crash.
enum SumStop {
    /// {builtin_reduce_error, Obj} and {invalid_value, Reason, Cause}.
    Value(Value),
    Crash(String, String),
}

fn sum_invalid(cause: &Value) -> SumStop {
    SumStop::Value(json!({"error": "builtin_reduce_error", "reason": SUM_ERROR, "caused_by": cause}))
}

fn sum_crash(e: CouchError) -> SumStop {
    SumStop::Crash(e.error, e.reason)
}

/// sum_values/2: the next value added to the sum so far.
fn sum_values(v: &Value, acc: &Value) -> std::result::Result<Value, SumStop> {
    match (v, acc) {
        (Value::Number(x), Value::Number(y)) => add(y, x).map_err(sum_crash),
        (Value::Array(x), Value::Array(y)) => sum_arrays(y, x),
        (Value::Number(_), Value::Array(y)) => sum_arrays(y, std::slice::from_ref(v)),
        (Value::Array(x), Value::Number(_)) => sum_arrays(std::slice::from_ref(acc), x),
        (Value::Object(props), _) => {
            match props.get("error") {
                Some(Value::String(e)) if e == "builtin_reduce_error" => return Err(SumStop::Value(v.clone())),
                None => {}
                Some(other) => {
                    let e = format!("{{case_clause,{{{},{}}}}}", crate::mango::erl_w(&json!("error")), crate::mango::erl_w(other));
                    return Err(SumStop::Crash(e, String::new()));
                }
            }
            match acc {
                Value::Number(n) if crate::json::int128(n) == Some(0) => Ok(v.clone()),
                Value::Object(acc_props) => sum_objects(props, acc_props),
                other => Err(SumStop::Crash(format!("{{case_clause,{}}}", crate::mango::erl_w(other)), String::new())),
            }
        }
        _ => Err(sum_invalid(v)),
    }
}

/// sum_objects/2: two objects' members merged in key order, those in both
/// summed.
fn sum_objects(a: &Map<String, Value>, b: &Map<String, Value>) -> std::result::Result<Value, SumStop> {
    let mut x: Vec<(&String, &Value)> = a.iter().collect();
    let mut y: Vec<(&String, &Value)> = b.iter().collect();
    x.sort_by(|p, q| p.0.as_bytes().cmp(q.0.as_bytes()));
    y.sort_by(|p, q| p.0.as_bytes().cmp(q.0.as_bytes()));
    let (mut i, mut j) = (0, 0);
    let mut out = Map::new();
    while i < x.len() && j < y.len() {
        match x[i].0.as_bytes().cmp(y[j].0.as_bytes()) {
            Ordering::Equal => {
                out.insert(x[i].0.clone(), sum_values(x[i].1, y[j].1)?);
                i += 1;
                j += 1;
            }
            Ordering::Less => {
                out.insert(x[i].0.clone(), x[i].1.clone());
                i += 1;
            }
            Ordering::Greater => {
                out.insert(y[j].0.clone(), y[j].1.clone());
                j += 1;
            }
        }
    }
    for (k, v) in x[i..].iter().chain(&y[j..]) {
        out.insert((*k).clone(), (*v).clone());
    }
    Ok(Value::Object(out))
}

/// sum_arrays/2: element by element, the longer's tail kept.
fn sum_arrays(xs: &[Value], ys: &[Value]) -> std::result::Result<Value, SumStop> {
    let mut out = Vec::with_capacity(xs.len().max(ys.len()));
    let mut k = 0;
    while k < xs.len() && k < ys.len() {
        match (&xs[k], &ys[k]) {
            (Value::Number(a), Value::Number(b)) => out.push(add(a, b).map_err(sum_crash)?),
            _ => return Err(sum_invalid(&Value::Array(xs[k..].to_vec()))),
        }
        k += 1;
    }
    out.extend(xs[k..].iter().chain(&ys[k..]).cloned());
    Ok(Value::Array(out))
}

/// builtin_sum_rows/2: the values summed, or the error that stopped it.
fn builtin_sum<'a>(vals: impl Iterator<Item = &'a Value>) -> Value {
    let mut acc = json!(0);
    // A run of numbers is summed natively, `acc` being stale meanwhile.
    let mut num = Some(Num::Int(0));
    for v in vals {
        if let (Some(n), Value::Number(x)) = (&num, v) {
            match n.add(&Num::of(x)) {
                Ok(sum) => num = Some(sum),
                Err(e) => return failure(&e.error, e.reason),
            }
            continue;
        }
        if let Some(n) = num.take() {
            acc = n.value();
        }
        acc = match sum_values(v, &acc) {
            Ok(a) => a,
            Err(SumStop::Value(e)) => return e,
            Err(SumStop::Crash(e, r)) => return failure(&e, r),
        };
    }
    num.map(|n| n.value()).unwrap_or(acc)
}

/// One _stats accumulator: sum, count, min, max and sum of squares.
#[derive(Clone)]
struct Stat([Num; 5]);

enum StatAcc {
    One(Box<Stat>),
    Many(Vec<StatAcc>),
}

type StatResult<T> = std::result::Result<T, (String, String)>;

fn invalid_value(reason: String) -> (String, String) {
    ("invalid_value".into(), reason)
}

fn stat_of(n: &serde_json::Number) -> StatResult<Stat> {
    let n = Num::of(n);
    let sqr = n.arith(&n, i128::checked_mul, |x, y| x * y).map_err(|e| (e.error, e.reason))?;
    Ok(Stat([n.clone(), Num::Int(1), n.clone(), n, sqr]))
}

/// An object's members as ~p shows a proplist.
fn proplist_p(o: &Map<String, Value>) -> String {
    let p = crate::mango::erl_p(&Value::Object(o.clone()));
    format!("[{}]", &p[2..p.len() - 2])
}

/// unpack_stats/1: a pre-reduced {sum, count, min, max, sumsqr}.
fn unpack_stats(o: &Map<String, Value>) -> StatResult<Stat> {
    let mut out: [Num; 5] = std::array::from_fn(|_| Num::Int(0));
    for (i, k) in ["sum", "count", "min", "max", "sumsqr"].iter().enumerate() {
        out[i] = match o.get(*k) {
            Some(Value::Number(n)) => Num::of(n),
            None => return Err(invalid_value(format!("user _stats input missing required field {k} ({})", proplist_p(o)))),
            Some(other) => {
                return Err(invalid_value(format!("non-numeric _stats input received for {k}: {}", crate::mango::erl_w(other))));
            }
        };
    }
    Ok(Stat(out))
}

/// build_initial_accumulator/1.
fn initial_stats(v: &Value) -> StatResult<StatAcc> {
    match v {
        Value::Array(l) => Ok(StatAcc::Many(l.iter().map(initial_stats).collect::<StatResult<_>>()?)),
        Value::Number(n) => Ok(StatAcc::One(Box::new(stat_of(n)?))),
        Value::Object(o) => Ok(StatAcc::One(Box::new(unpack_stats(o)?))),
        other => Err(invalid_value(format!("non-numeric _stats input: {}", crate::mango::erl_w(other)))),
    }
}

/// stat_values/2.
fn stat_values(v: &Value, acc: StatAcc) -> StatResult<StatAcc> {
    match (v, acc) {
        (Value::Array(l), StatAcc::Many(accs)) => {
            if l.len() != accs.len() {
                return Err(("unknown_error".into(), "function_clause".into()));
            }
            Ok(StatAcc::Many(l.iter().zip(accs).map(|(x, a)| stat_values(x, a)).collect::<StatResult<_>>()?))
        }
        (Value::Object(o), StatAcc::One(a)) => Ok(StatAcc::One(Box::new(combine_stats(unpack_stats(o)?, *a)?))),
        (Value::Number(n), StatAcc::One(a)) => Ok(StatAcc::One(Box::new(combine_stats(stat_of(n)?, *a)?))),
        (other, _) => Err(invalid_value(format!(
            "The _stats function requires that map values be numbers or arrays of numbers, not '{}'",
            crate::mango::erl_p(other)
        ))),
    }
}

fn combine_stats(new: Stat, acc: Stat) -> StatResult<Stat> {
    let sum = |i: usize| new.0[i].add(&acc.0[i]).map_err(|e| (e.error, e.reason));
    // min/2 and max/2 give the first of equals.
    let min = if new.0[2].cmp(&acc.0[2]) == Ordering::Greater { acc.0[2].clone() } else { new.0[2].clone() };
    let max = if new.0[3].cmp(&acc.0[3]) == Ordering::Less { acc.0[3].clone() } else { new.0[3].clone() };
    Ok(Stat([sum(0)?, sum(1)?, min, max, sum(4)?]))
}

fn pack_stats(a: StatAcc) -> Value {
    match a {
        StatAcc::One(stat) => {
            let [sum, count, min, max, sumsqr] = stat.0.each_ref().map(Num::value);
            json!({"sum": sum, "count": count, "min": min, "max": max, "sumsqr": sumsqr})
        }
        StatAcc::Many(l) => Value::Array(l.into_iter().map(pack_stats).collect()),
    }
}

/// builtin_stats/2, reducing or rereducing (the reductions are packed).
fn builtin_stats(vals: Vec<Value>) -> Value {
    let mut it = vals.iter();
    let Some(first) = it.next() else { return json!({"sum": 0, "count": 0, "min": 0, "max": 0, "sumsqr": 0}) };
    let res = initial_stats(first).and_then(|mut acc| {
        for v in it {
            acc = stat_values(v, acc)?;
        }
        Ok(acc)
    });
    match res {
        Ok(acc) => pack_stats(acc),
        Err((e, r)) => failure(&e, r),
    }
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
        Reducer::First | Reducer::Last => match v {
            Value::Array(mut a) if a.len() == 3 => a.pop().unwrap_or(Value::Null),
            other => other,
        },
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
    /// Rows are stored keyed [partition, key] and queried per partition.
    pub partitioned: bool,
}

fn parse_reducer(src: &str, js_red: &mut Vec<String>, ddoc: &Value) -> Result<Reducer> {
    let _ = ddoc;
    let s = src.trim();
    let bad = |m: String| CouchError::new(StatusCode::BAD_REQUEST, "invalid_design_doc", m);
    // As couch_query_servers matches them: by prefix.
    let rank = |n: &str| -> Result<usize> {
        match n.parse::<i64>() {
            Ok(v) if (1..=100).contains(&v) => Ok(v as usize),
            Ok(_) => Err(bad("rank value must be between 1 and 100".into())),
            Err(_) => Err(bad("invalid rank reducer".into())),
        }
    };
    Ok(match s {
        s if s.starts_with("_count") => Reducer::Count,
        s if s.starts_with("_sum") => Reducer::Sum,
        s if s.starts_with("_stats") => Reducer::Stats,
        s if s.starts_with("_approx_count_distinct") => Reducer::ApproxCountDistinct,
        s if s.starts_with("_top_") => Reducer::Top(rank(&s[5..])?),
        s if s.starts_with("_bottom_") => Reducer::Bottom(rank(&s[8..])?),
        s if s.starts_with("_first") => Reducer::First,
        s if s.starts_with("_last") => Reducer::Last,
        s if s.starts_with('_') => return Err(bad(format!("`{s}` is not a supported reduce function."))),
        s => {
            js_red.push(s.to_string());
            // placeholder index fixed up once JsFuns exists
            Reducer::Js(JsFuns::new(&Value::Null, &[], &[]), js_red.len() - 1, Arc::from(s))
        }
    })
}

/// couch_mrview:validate/2: a design document's fields of the right
/// types, a map function in every view, builtin reduces known, and the
/// views' functions compiling.
pub fn validate_ddoc(ddoc: &Value, db_partitioned: bool) -> Result<()> {
    let invalid = |m: String| CouchError::new(StatusCode::BAD_REQUEST, "invalid_design_doc", m);
    let query = ddoc.get("language").and_then(Value::as_str) == Some("query");
    let map_type: &[&str] = if query { &["object"] } else { &["string"] };
    type Path<'a> = Vec<(&'a str, &'a [&'a str])>;
    let specs: Vec<Path> = vec![
        vec![("filters", &["object"]), ("*", &["object", "string"])],
        vec![("language", &["string"])],
        vec![("lists", &["object"]), ("*", &["object", "string"])],
        vec![("options", &["object"])],
        vec![("options", &["object"]), ("include_design", &["boolean"])],
        vec![("options", &["object"]), ("local_seq", &["boolean"])],
        vec![("options", &["object"]), ("partitioned", &["boolean"])],
        vec![("rewrites", &["string", "array"])],
        vec![("shows", &["object"]), ("*", &["object", "string"])],
        vec![("updates", &["object"]), ("*", &["object", "string"])],
        vec![("validate_doc_update", &["string"])],
        vec![("views", &["object"]), ("lib", &["object"])],
        vec![("views", &["object"]), ("*", &["object"]), ("map", map_type)],
        vec![("views", &["object"]), ("*", &["object"]), ("reduce", &["string"])],
    ];
    fn is(v: &Value, types: &[&str]) -> bool {
        types.iter().any(|t| match *t {
            "string" => v.is_string(),
            "array" => v.is_array(),
            "object" => v.is_object(),
            "boolean" => v.is_boolean(),
            _ => false,
        })
    }
    fn check(v: &Value, path: &[(&str, &[&str])], at: &mut Vec<String>) -> std::result::Result<(), (Vec<String>, Vec<String>)> {
        let Some(((key, types), rest)) = path.split_first() else { return Ok(()) };
        let Some(o) = v.as_object() else { return Ok(()) };
        let keys: Vec<&String> = if *key == "*" { o.keys().collect() } else { o.keys().filter(|k| k == key).collect() };
        for k in keys {
            let x = &o[k];
            at.push(k.clone());
            if !is(x, types) {
                return Err((at.clone(), types.iter().map(|t| t.to_string()).collect()));
            }
            check(x, rest, at)?;
            at.pop();
        }
        Ok(())
    }
    for spec in &specs {
        if let Err((path, types)) = check(ddoc, spec, &mut Vec::new()) {
            return Err(invalid(format!("`{}` field must have {} type", path.join("."), types.join(" or "))));
        }
    }
    if let Some(views) = ddoc.get("views").and_then(Value::as_object) {
        for (name, v) in views {
            if name != "lib" && v.get("map").is_none() {
                return Err(invalid(format!("View `{name}` must contain map function")));
            }
        }
    }
    let def = IndexDef::from_ddoc(ddoc, db_partitioned)?;
    if def.language == "javascript" {
        let views = ddoc.get("views").and_then(Value::as_object);
        for d in &def.views {
            let v = views.and_then(|vs| vs.get(&d.name));
            if let Some(src) = v.and_then(|v| v.get("map")).and_then(Value::as_str) {
                JsFuns::try_compile(ddoc, "map", &d.name, src)?;
            }
            if let (Reducer::Js(..), Some(src)) = (&d.reducer, &d.reduce_src) {
                JsFuns::try_compile(ddoc, "reduce", &d.name, src)?;
            }
        }
    }
    Ok(())
}

/// couch_mrview_util:ddoc_to_mrst/2's signature: the MD5 of the term
/// {Views, Language, DesignOpts, SortedLib}, the views as #mrview{}
/// records, one per map function and options, in order of those.
fn view_signature(ddoc: &Value) -> String {
    use crate::replicator::rep::{Term, ejson};
    let props = |o: &Map<String, Value>| Term::List(o.iter().map(|(k, v)| Term::Tuple(vec![Term::Bin(k.as_bytes().to_vec()), ejson(v)])).collect());
    let empty = Map::new();
    let views = ddoc.get("views").and_then(Value::as_object).unwrap_or(&empty);
    // ((map, options), map names, reduce funs), each list newest first.
    type ByMap = ((Value, Value), Vec<String>, Vec<(String, Value)>);
    let mut by_src: Vec<ByMap> = Vec::new();
    for (name, v) in views {
        let Some(map) = v.as_object().and_then(|o| o.get("map")) else { continue };
        let opts = v.get("options").filter(|o| o.is_object()).cloned().unwrap_or_else(|| json!({}));
        let key = (map.clone(), opts);
        let i = match by_src.iter().position(|(k, ..)| k == &key) {
            Some(i) => i,
            None => {
                by_src.push((key, Vec::new(), Vec::new()));
                by_src.len() - 1
            }
        };
        match v.get("reduce") {
            None | Some(Value::Null) => by_src[i].1.insert(0, name.clone()),
            Some(r) => by_src[i].2.insert(0, (name.clone(), r.clone())),
        }
    }
    let cmp = crate::mango::cmp_raw;
    by_src.sort_by(|a, b| cmp(&a.0.0, &b.0.0).then_with(|| cmp(&a.0.1, &b.0.1)));
    let records = by_src
        .into_iter()
        .enumerate()
        .map(|(n, ((map, opts), map_names, reduce_funs))| {
            Term::Tuple(vec![
                Term::Atom("mrview"),
                Term::Int(n as i128),
                Term::Int(0),
                Term::Int(0),
                Term::List(map_names.iter().map(|m| Term::Bin(m.as_bytes().to_vec())).collect()),
                Term::List(reduce_funs.iter().map(|(n, r)| Term::Tuple(vec![Term::Bin(n.as_bytes().to_vec()), ejson(r)])).collect()),
                ejson(&map),
                Term::Atom("nil"),
                props(opts.as_object().unwrap_or(&empty)),
            ])
        })
        .collect();
    let language = ddoc.get("language").map(ejson).unwrap_or_else(|| Term::Bin(b"javascript".to_vec()));
    let design_opts = props(ddoc.get("options").and_then(Value::as_object).unwrap_or(&empty));
    // couch_index_util:sort_lib/1: objects as lists, sorted by name.
    fn sort_lib(o: &Map<String, Value>) -> Term {
        let mut items: Vec<(&String, &Value)> = o.iter().collect();
        items.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        Term::List(
            items
                .into_iter()
                .map(|(k, v)| {
                    let v = match v {
                        Value::Object(inner) => sort_lib(inner),
                        other => ejson(other),
                    };
                    Term::Tuple(vec![Term::Bin(k.as_bytes().to_vec()), v])
                })
                .collect(),
        )
    }
    let lib = sort_lib(views.get("lib").and_then(Value::as_object).unwrap_or(&empty));
    hex::encode(Md5::digest(Term::Tuple(vec![Term::List(records), language, design_opts, lib]).encode()))
}

impl IndexDef {
    /// `db_partitioned` is the default for options.partitioned.
    pub fn from_ddoc(ddoc: &Value, db_partitioned: bool) -> Result<IndexDef> {
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
                if let Reducer::Js(_, i, src) = &d.reducer {
                    d.reducer = Reducer::Js(j.clone(), *i, src.clone());
                }
            }
        }
        let partitioned = match opts.get("partitioned") {
            Some(Value::Bool(b)) => *b,
            _ => db_partitioned,
        };
        if partitioned && !db_partitioned {
            return Err(CouchError::new(
                StatusCode::BAD_REQUEST,
                "invalid_design_doc",
                "partitioned option cannot be true in a non-partitioned database.",
            ));
        }
        let sig = view_signature(ddoc);
        Ok(IndexDef {
            sig,
            ddoc_id: id,
            language,
            views: defs,
            js,
            include_design: opts.get("include_design").and_then(|b| b.as_bool()).unwrap_or(false),
            local_seq: opts.get("local_seq").and_then(|b| b.as_bool()).unwrap_or(false),
            partitioned,
        })
    }

    pub fn view_index(&self, name: &str) -> Option<usize> {
        self.views.iter().position(|v| v.name == name)
    }
}

/// Build empty indexes with the sort-based bulk path (default on;
/// COUCHDB_RS_VIEW_BULK=0 forces incremental batches, for comparison).
fn bulk_enabled() -> bool {
    static B: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *B.get_or_init(|| std::env::var("COUCHDB_RS_VIEW_BULK").map(|v| v != "0").unwrap_or(true))
}

type SortCmp<T> = Box<dyn Fn(&T, &T) -> Ordering>;
type ViewSorter = Sorter<(VKey, VVal), SortCmp<(VKey, VVal)>>;
type IdSorter = Sorter<(String, IdEntry), SortCmp<(String, IdEntry)>>;
/// Rows mapped from a batch of docs: (doc id, rows per view, id-tree entry).
type Mapped = Vec<(String, Vec<Vec<(Json, VVal)>>, IdEntry)>;

/// Hand a mapped batch to the external sorters.
/// About how long a value's JSON is, without writing it.
fn approx_len(v: &Value) -> usize {
    match v {
        Value::Null | Value::Bool(_) => 5,
        Value::Number(n) => n.as_str().len(),
        Value::String(s) => s.len() + 2,
        Value::Array(a) => a.iter().map(|x| approx_len(x) + 1).sum::<usize>() + 2,
        Value::Object(o) => o.iter().map(|(k, x)| k.len() + 4 + approx_len(x)).sum::<usize>() + 2,
    }
}

fn feed_sorters(mapped: Mapped, views: &mut [ViewSorter], ids: &mut IdSorter) -> std::io::Result<()> {
    for (id, per_view, entry) in mapped {
        for (vi, rows) in per_view.into_iter().enumerate() {
            for (k, val) in rows {
                // Rough size for the memory budget; exactness doesn't matter.
                let approx = 64 + id.len() + approx_len(&k.0);
                views[vi].push(((k, IdBound::Id(id.clone())), val), approx)?;
            }
        }
        if !entry.keys.is_empty() {
            let approx = 64 + id.len() * 2;
            ids.push((id, entry), approx)?;
        }
    }
    Ok(())
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

/// Docs per view-index write batch. Experiment knob (COUCHDB_RS_VIEW_BATCH).
fn view_batch_docs() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::env::var("COUCHDB_RS_VIEW_BATCH").ok().and_then(|v| v.parse().ok()).unwrap_or(500))
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
    compact_running: std::sync::atomic::AtomicBool,
    /// How the last bulk build ran, reported in _info.
    last_build: Mutex<Option<Value>>,
    /// The database's name, for _active_tasks.
    db_name: String,
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
        let snap = db.snapshot();
        let db_uuid = snap.header.uuid.clone();
        let file = Arc::new(CouchFile::open(&path, true)?);
        let mut state = fresh_state(&file, &def);
        // A new index has seen every purge so far.
        state.purge_seq = snap.header.purge_seq;
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
            compact_running: std::sync::atomic::AtomicBool::new(false),
            last_build: Mutex::new(None),
            db_name: db.name.clone(),
        })
    }

    pub fn state(&self) -> IndexState {
        self.state.read().clone()
    }

    /// Put a new, empty file in place of the index's, for a rebuild. The
    /// old one, unlinked, stays open for queries still reading the state
    /// it holds (update=false or lazy), which truncating it would break.
    fn replace_file(&self) -> Result<Arc<CouchFile>> {
        let tmp = self.path.with_extension("new.view");
        let _ = std::fs::remove_file(&tmp);
        let file = Arc::new(CouchFile::open(&tmp, true)?);
        std::fs::rename(&tmp, &self.path)?;
        *self.file.write() = file.clone();
        Ok(file)
    }

    /// A new, empty index that has seen purges up to `purge_seq`.
    fn reset(&self, purge_seq: u64) -> Result<IndexState> {
        let file = self.replace_file()?;
        let mut st = fresh_state(&file, &self.def);
        st.purge_seq = purge_seq;
        Ok(st)
    }

    /// couch_mrview_updater:purge/4: drop the rows of purged docs that are
    /// gone, or have changed since the index saw them (a purge gives a
    /// doc's remaining revisions new seqs), which the update then maps
    /// again. Docs that didn't change keep theirs.
    fn apply_purges(&self, snap: &Snapshot, st: &mut IndexState, infos: &[PurgeInfo]) -> Result<()> {
        let mut ids: Vec<&str> = infos.iter().map(|i| i.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        let mut view_acts: Vec<Vec<Action<VKey, VVal>>> = (0..self.def.views.len()).map(|_| Vec::new()).collect();
        let mut id_acts = Vec::new();
        for id in ids {
            if snap.get_fdi(id)?.is_some_and(|f| f.update_seq <= st.seq) {
                continue;
            }
            let id = id.to_string();
            let Some(old) = st.ids.lookup(&id)? else { continue };
            for (vi, keys) in &old.keys {
                for k in keys {
                    view_acts[*vi as usize].push(Action::Remove((k.clone(), IdBound::Id(id.clone()))));
                }
            }
            id_acts.push(Action::Remove(id));
        }
        st.ids = st.ids.modify(id_acts)?;
        for (vi, acts) in view_acts.into_iter().enumerate() {
            st.views[vi] = st.views[vi].modify(acts)?;
        }
        st.purge_seq = infos.last().map_or(st.purge_seq, |i| i.seq);
        Ok(())
    }

    /// couch_mrview_index:ensure_local_purge_doc/2: an index with views
    /// has a purge checkpoint, starting at the database's purge seq.
    fn ensure_purge_checkpoint(&self, db: &Db) -> Result<()> {
        let snap = db.snapshot();
        let id = purge_checkpoint_id(&self.def.sig);
        match (!self.def.views.is_empty(), snap.get_local(&id)?.is_some()) {
            (true, false) => self.save_purge_checkpoint(db, snap.header.purge_seq),
            (false, true) => db.put_local_blocking(&id, None),
            _ => Ok(()),
        }
    }

    /// couch_mrview_index:update_local_purge_doc/3: the purge seq this
    /// index has seen, which compaction keeps purge infos after.
    fn save_purge_checkpoint(&self, db: &Db, purge_seq: u64) -> Result<()> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let body = json!({
            "type": "mrview",
            "purge_seq": purge_seq,
            "updated_on": now,
            "ddoc_id": self.def.ddoc_id,
            "signature": self.def.sig,
        });
        db.put_local_blocking(&purge_checkpoint_id(&self.def.sig), body.as_object().cloned())
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
        // couch_index_updater opens documents with their conflicts.
        let opts = DocOpts {
            conflicts: true,
            deleted_conflicts: true,
            local_seq: self.def.local_seq,
            ..Default::default()
        };
        // Only Mango views need the doc parsed; JS gets its JSON as text.
        let doc = match self.def.views.iter().any(|v| matches!(v.map, MapKind::Mango { .. })) {
            true => Some(snap.winner_json(fdi, &opts)?),
            false => None,
        };
        crate::stat!("couchdb.mrview.map_doc").inc();
        let js_rows = match &self.def.js {
            Some(f) if self.def.views.iter().any(|v| matches!(v.map, MapKind::Js(_))) => Some(match &doc {
                Some(d) => f.map(d)?,
                None => f.map_text(snap.winner_json_text(fdi, &opts)?)?,
            }),
            _ => None,
        };
        let mut out = Vec::with_capacity(n);
        // Partitioned rows are keyed [partition, key].
        let part = if self.def.partitioned { crate::doc::partition_of(&fdi.id).map(|p| json!(p)) } else { None };
        if self.def.partitioned && part.is_none() {
            return Ok(vec![Vec::new(); n]);
        }
        let wrap = |rows: Vec<(Value, Value)>| -> Vec<(Value, Value)> {
            match &part {
                Some(p) => rows.into_iter().map(|(k, v)| (json!([p, k]), v)).collect(),
                None => rows,
            }
        };
        for v in &self.def.views {
            match &v.map {
                MapKind::Js(i) => out.push(wrap(js_rows.as_ref().and_then(|r| r.get(*i).cloned()).unwrap_or_default())),
                MapKind::Mango { fields, partial } => {
                    let doc = doc.as_ref().expect("parsed for Mango views");
                    if let Some(p) = partial
                        && !p.matches(doc) {
                            out.push(Vec::new());
                            continue;
                        }
                    let mut key = Vec::with_capacity(fields.len());
                    for f in fields {
                        match crate::mango::get_field(doc, f) {
                            Some(val) => key.push(val.clone()),
                            None => break,
                        }
                    }
                    if key.len() == fields.len() {
                        out.push(wrap(vec![(Value::Array(key), Value::Null)]));
                    } else {
                        out.push(Vec::new());
                    }
                }
            }
        }
        crate::stat!("couchdb.mrview.emits").add(out.iter().map(|r| r.len() as i64).sum());
        Ok(out)
    }

    /// Bring the index up to date with the database.
    pub fn update(&self, db: &Db) -> Result<IndexState> {
        let _g = self.update_lock.lock();
        let snap = db.snapshot();
        let mut st = self.state();
        let db_purge = snap.header.purge_seq;
        let pending_purges = db_purge.saturating_sub(st.purge_seq);
        let purged = st.purge_seq != db_purge;
        if st.seq > snap.update_seq() || st.purge_seq > db_purge {
            // The database was recreated: rebuild.
            st = self.reset(db_purge)?;
        } else if purged {
            match snap.purges_since(st.purge_seq)? {
                Some(infos) => self.apply_purges(&snap, &mut st, &infos)?,
                // Purges it hasn't seen are gone: rebuild.
                None => st = self.reset(db_purge)?,
            }
        }
        if st.seq >= snap.update_seq() && !purged {
            return Ok(st);
        }
        self.updater_running.store(true, std::sync::atomic::Ordering::Relaxed);
        // Docs changed since the index's seq (all of them, for a new
        // index), and purges, as couch_index_updater counts them.
        let docs = if st.seq == 0 { snap.header.doc_count + snap.header.del_count } else { snap.pending_after(st.seq) };
        let total = docs + pending_purges;
        let task = crate::tasks::add(json!({
            "type": "indexer",
            "database": self.db_name,
            "design_document": self.def.ddoc_id,
            "indexer_pid": crate::tasks::new_pid(),
            "progress": 0,
            "changes_done": 0,
            "total_changes": total,
        }));
        let res = (|| -> Result<IndexState> {
            let empty = st.seq == 0 && st.ids.root.is_none() && st.views.iter().all(|v| v.root.is_none());
            if empty && bulk_enabled() {
                self.bulk_build(&snap, &mut st, &task)?;
                check_failures(&st)?;
                self.commit(&st)?;
                *self.state.write() = st.clone();
                return Ok(st);
            }
            let mut batch: Vec<FullDocInfo> = Vec::new();
            let mut err = None;
            snap.fold_changes(st.seq, Dir::Fwd, |fdi| {
                batch.push(fdi.clone());
                if batch.len() >= view_batch_docs() {
                    let n = batch.len() as u64;
                    if let Err(e) = self.apply_batch(&snap, &mut st, std::mem::take(&mut batch)) {
                        err = Some(e);
                        return Ok(false);
                    }
                    task.add_changes(n);
                }
                Ok(true)
            })?;
            if let Some(e) = err {
                return Err(e);
            }
            let n = batch.len() as u64;
            self.apply_batch(&snap, &mut st, batch)?;
            task.add_changes(n);
            st.seq = snap.update_seq();
            self.commit(&st)?;
            *self.state.write() = st.clone();
            Ok(st)
        })()
        .and_then(|st| {
            if purged {
                self.save_purge_checkpoint(db, st.purge_seq)?;
            }
            Ok(st)
        });
        self.updater_running.store(false, std::sync::atomic::Ordering::Relaxed);
        if res.is_ok() {
            crate::db::notify(&self.db_name, crate::db::DbEvent::IndexCommit(self.def.ddoc_id.clone()));
        }
        res
    }

    /// The rows one doc contributes: per view, (key, value) with duplicate
    /// keys from the same doc merged, plus its doc-id tree entry.
    fn doc_entries(&self, snap: &Snapshot, st: &IndexState, fdi: &FullDocInfo) -> Result<(Vec<Vec<(Json, VVal)>>, IdEntry)> {
        let rows = self.map_doc(snap, fdi)?;
        let mut entry = IdEntry { keys: Vec::new() };
        let mut per_view = Vec::with_capacity(rows.len());
        for (vi, vrows) in rows.into_iter().enumerate() {
            let mut out = Vec::new();
            if !vrows.is_empty() {
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
                    out.push((Json(k), val));
                }
                entry.keys.push((vi as u32, keys));
            }
            per_view.push(out);
        }
        Ok((per_view, entry))
    }

    /// Build a fresh index in one pass: map every doc, spill rows to
    /// external sorters, then build each B-tree bottom-up from the sorted
    /// stream. Each node is written once, contiguously, with no lookups of
    /// old keys (there are none) and no copy-on-write rewrites.
    fn bulk_build(&self, snap: &Snapshot, st: &mut IndexState, task: &crate::tasks::TaskHandle) -> Result<()> {
        type Cmp<T> = SortCmp<T>;
        let budget = crate::extsort::budget_bytes();
        let scratch = |name: &str| self.path.with_extension(format!("{name}.sort"));
        let mut view_sorters: Vec<ViewSorter> = st
            .views
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let spec = t.spec.clone();
                let cmp: Cmp<(VKey, VVal)> = Box::new(move |a, b| spec.cmp(&a.0, &b.0));
                Sorter::new(scratch(&format!("v{i}")), budget, cmp).with_prefix(|kv| crate::collate::sort_prefix(&kv.0.0.0))
            })
            .collect();
        let id_cmp: Cmp<(String, IdEntry)> = Box::new(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let mut id_sorter = Sorter::new(scratch("ids"), budget, id_cmp);
        // Pipeline: one thread walks the by-seq tree, JS map workers load and
        // map docs, and this thread feeds the sorters. Workers are added while
        // this (serial) stage sits idle waiting for them, i.e. until it keeps
        // a full core busy, up to the machine's core count.
        let max_workers = map_threads_max();
        let (work_tx, work_rx) = std::sync::mpsc::sync_channel::<Vec<FullDocInfo>>(max_workers * 4);
        let work_rx = Arc::new(Mutex::new(work_rx));
        let (res_tx, res_rx) = std::sync::mpsc::sync_channel::<Result<Mapped>>(max_workers * 4);
        let producer_done = std::sync::atomic::AtomicBool::new(false);
        let alive = std::sync::atomic::AtomicUsize::new(0);
        // Nanoseconds the reader spent blocked handing out work (high means
        // the map workers are the bottleneck, low means the reader is).
        let reader_blocked = std::sync::atomic::AtomicU64::new(0);
        let st_ref: &IndexState = st;
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
                        let out: Result<Mapped> = batch
                            .iter()
                            .map(|f| self.doc_entries(snap, st_ref, f).map(|(pv, e)| (f.id.clone(), pv, e)))
                            .collect();
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
                        task.add_changes(mapped.len() as u64);
                        if let Err(e) = feed_sorters(mapped, &mut view_sorters, &mut id_sorter) {
                            err = Some(e.into());
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
                    // Drain anything already sent, then finish.
                    while let Ok(r) = res_rx.try_recv() {
                        if let Ok(m) = &r {
                            task.add_changes(m.len() as u64);
                        }
                        match r.and_then(|m| Ok(feed_sorters(m, &mut view_sorters, &mut id_sorter)?)) {
                            Ok(()) => {}
                            Err(e) => {
                                err = Some(e);
                                break;
                            }
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
            // Stops the producer and workers early if we bailed out.
            drop(res_rx);
            drop(res_tx);
        });
        if let Some(e) = err {
            return Err(e);
        }
        let map_secs = t_start.elapsed().as_secs_f64();
        let t_build = Instant::now();
        for (vi, sorter) in view_sorters.into_iter().enumerate() {
            let mut sorted = sorter.finish()?;
            let mut b = st.views[vi].bulk_builder();
            while let Some((k, v)) = sorted.next_item()? {
                b.push(k, v)?;
            }
            st.views[vi] = b.finish()?;
        }
        let mut sorted = id_sorter.finish()?;
        let mut b = st.ids.bulk_builder();
        while let Some((k, v)) = sorted.next_item()? {
            b.push(k, v)?;
        }
        st.ids = b.finish()?;
        st.seq = snap.update_seq();
        *self.last_build.lock() = Some(json!({
            "method": "bulk",
            "map_threads": workers,
            "map_threads_max": max_workers,
            "map_phase_secs": map_secs,
            "collector_busy": busy.as_secs_f64() / map_secs.max(1e-9),
            "reader_blocked": reader_blocked.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e9 / map_secs.max(1e-9),
            "sort_and_build_secs": t_build.elapsed().as_secs_f64(),
        }));
        Ok(())
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
            let (per_view, entry) = self.doc_entries(snap, st, &fdi)?;
            for (vi, rows) in per_view.into_iter().enumerate() {
                for (k, val) in rows {
                    view_acts[vi].push(Action::Insert((k, IdBound::Id(fdi.id.clone())), val));
                }
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
        check_failures(st)?;
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
                // In fabric_group_info's order.
                "updates_pending": {"minimum": 0, "preferred": 0, "total": 0},
                "waiting_commit": false,
                "waiting_clients": 0,
                "updater_running": self.updater_running.load(std::sync::atomic::Ordering::Relaxed),
                "update_seq": st.seq,
                "sizes": {"file": file.eof(), "external": 0, "active": active},
                "signature": self.def.sig,
                "purge_seq": st.purge_seq,
                "language": self.def.language,
                "compact_running": self.compact_running.load(std::sync::atomic::Ordering::Relaxed),
                "collator_versions": ["153.136"]
            },
            "couchdb_rs_last_build": self.last_build.lock().clone()
        })
    }

    /// Rewrite the index file with only live data. The live trees are
    /// already in key order, so each streams straight into a bottom-up
    /// builder: one sequential pass, every node written once.
    pub fn compact(&self) -> Result<()> {
        let _g = self.update_lock.lock();
        self.compact_running.store(true, std::sync::atomic::Ordering::Relaxed);
        let res = self.compact_locked();
        self.compact_running.store(false, std::sync::atomic::Ordering::Relaxed);
        res
    }

    fn compact_locked(&self) -> Result<()> {
        let st = self.state();
        let tmp = self.path.with_extension("compact.view");
        let _ = std::fs::remove_file(&tmp);
        let nf = Arc::new(CouchFile::open(&tmp, true)?);
        let mut ns = fresh_state(&nf, &self.def);
        ns.seq = st.seq;
        ns.purge_seq = st.purge_seq;
        // Every doc id entry and view row is copied once.
        let total = st.ids.full_reduce().unwrap_or(0) + st.views.iter().map(|v| v.full_reduce().map(|r| r.count).unwrap_or(0)).sum::<u64>();
        let task = crate::tasks::add(json!({
            "type": "view_compaction",
            "database": self.db_name,
            "design_document": self.def.ddoc_id,
            "progress": 0,
            "changes_done": 0,
            "total_changes": total,
        }));
        task.update(json!({"phase": "ids", "view": null}));
        let mut b = ns.ids.bulk_builder();
        let mut n = 0u64;
        st.ids.fold(Dir::Fwd, &Range::default(), |k, v| {
            b.push(k.clone(), v.clone())?;
            n += 1;
            if n % 10_000 == 0 {
                task.add_changes(10_000);
            }
            Ok(true)
        })?;
        task.add_changes(n % 10_000);
        ns.ids = b.finish()?;
        for (i, v) in st.views.iter().enumerate() {
            task.update(json!({"phase": "view", "view": i}));
            let mut b = ns.views[i].bulk_builder();
            let mut n = 0u64;
            v.fold(Dir::Fwd, &Range::default(), |k, val| {
                b.push(k.clone(), val.clone())?;
                n += 1;
                if n % 10_000 == 0 {
                    task.add_changes(10_000);
                }
                Ok(true)
            })?;
            task.add_changes(n % 10_000);
            ns.views[i] = b.finish()?;
        }
        *self.file.write() = nf;
        self.commit(&ns)?;
        std::fs::rename(&tmp, &self.path)?;
        *self.state.write() = ns;
        Ok(())
    }
}

fn purge_checkpoint_id(sig: &str) -> String {
    format!("_local/purge-mrview-{sig}")
}

/// couch_mrview_index:verify_index_exists/2: whether a purge checkpoint's
/// index still exists, its design doc there with the same signature. One
/// that can't be checked is assumed to.
pub fn purge_client_exists(snap: &Snapshot, props: &Map<String, Value>) -> bool {
    if props.get("type").and_then(Value::as_str) != Some("mrview") {
        return false;
    }
    let Some(ddoc_id) = props.get("ddoc_id").and_then(Value::as_str) else { return true };
    let ddoc = match crate::changes::load_ddoc(snap, ddoc_id) {
        Ok(d) => d,
        Err(e) if e.error == "not_found" => return false,
        Err(_) => return true,
    };
    match IndexDef::from_ddoc(&ddoc, snap.partitioned()) {
        Ok(def) => props.get("signature").and_then(Value::as_str) == Some(def.sig.as_str()),
        Err(_) => true,
    }
}

/// Find or open the index for a design doc.
pub fn get_index(db: &Arc<Db>, ddoc: &Value, index_dir: PathBuf) -> Result<Arc<Index>> {
    let def = IndexDef::from_ddoc(ddoc, db.snapshot().partitioned())?;
    let key = format!("mrview:{}", def.sig);
    if let Some(i) = db.ext.lock().get(&key).cloned()
        && let Ok(idx) = i.downcast::<Index>() {
            return Ok(idx);
        }
    let idx = Arc::new(Index::open(db, def, index_dir)?);
    db.ext.lock().insert(key, idx.clone());
    if let Err(e) = idx.ensure_purge_checkpoint(db) {
        tracing::warn!("purge checkpoint for {}/{}: {}", db.name, idx.def.ddoc_id, e.reason);
    }
    Ok(idx)
}

pub fn index_dir(req: &Req, db: &Db) -> PathBuf {
    index_dir_of(&req.server, &db.name)
}

pub fn index_dir_of(server: &Server, db_name: &str) -> PathBuf {
    server.index_dir(db_name).join("mrview")
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
    if args.group_level == Some(0) {
        return Value::Null;
    }
    if let Some(l) = args.group_level {
        if let Value::Array(a) = key {
            return Value::Array(a.iter().take(l).cloned().collect());
        }
        return key.clone();
    }
    if args.group { key.clone() } else { Value::Null }
}

fn same_group(args: &QueryArgs, spec: &ViewSpec, a: &Value, b: &Value) -> bool {
    if args.group_level == Some(0) {
        return true;
    }
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
    crate::db::count_reads(snap, 1);
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
    // fabric_view's reduce context: the view, as a path.
    let ctx = format!("{}/{}/_view/{}", snap.name, def.ddoc_id, vdef.name);
    let has_reduce = !matches!(vdef.reducer, Reducer::None);
    let reduce = args.reduce.unwrap_or(has_reduce);
    if reduce && !has_reduce
        && args.reduce == Some(true) {
            return Err(CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", "Reduce is invalid for map-only views."));
        }
    // A map view, or a reduce view read with reduce=false, can't group
    // (group_level=0 is no grouping).
    if !(reduce && has_reduce) && (args.group || args.group_level.is_some_and(|l| l > 0)) {
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
    // Within a partition: wrapped keys. Like CouchDB, total_rows and
    // offset still count the whole view.
    let part = args.partition.clone();
    let pargs;
    let args = match &part {
        Some(p) => {
            pargs = partition_args(args, p);
            &pargs
        }
        None => args,
    };
    // Keys as the caller knows them (without the partition).
    let user_key = |k: &Value| -> Value {
        match (&part, k) {
            (Some(_), Value::Array(a)) if a.len() == 2 => a[1].clone(),
            _ => k.clone(),
        }
    };
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
                |x, y| same_group(args, &spec, &user_key(&x.0.0), &user_key(&y.0.0)),
                |k, red| {
                    if skip > 0 {
                        skip -= 1;
                        return Ok(true);
                    }
                    if rows.len() >= args.limit {
                        return Ok(false);
                    }
                    let v = red.red.map(|j| j.0).unwrap_or(Value::Null);
                    let v = finalize_red(&vdef.reducer, spec.coordinator(v, &ctx));
                    if let Some((e, r)) = as_failure(&v) {
                        err = Some(CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, &e, r).with_ref());
                        return Ok(false);
                    }
                    // fabric_view_row:transform/1 of an overflow.
                    if is_overflow(&v) {
                        rows.push(json!({"key": null, "error": "reduce_overflow_error", "reason": v["reduce_overflow_error"]}));
                        return Ok(true);
                    }
                    rows.push(json!({"key": group_key(args, &user_key(&k.0.0)), "value": v}));
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
                let mut row = json!({"id": id, "key": user_key(&k.0), "value": val});
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
    let mut out = Map::new();
    if !res.reduced {
        out.insert("total_rows".into(), json!(res.total_rows));
        out.insert("offset".into(), json!(res.offset));
    }
    // update_seq comes before the rows.
    if args.update_seq {
        out.insert("update_seq".into(), json!(res.update_seq));
    }
    out.insert("rows".into(), Value::Array(res.rows));
    Value::Object(out)
}

/// Query a view: {total_rows, offset, rows} for map results, {rows} for
/// reduced ones (plus update_seq when asked).
pub fn view_json(req: &Req, db: &Arc<Db>, ddoc_id: &str, view: &str, args: &QueryArgs) -> Result<Value> {
    let dir = index_dir(req, db);
    blocking(|| -> Result<Value> {
        let (snap, idx, st) = prepare(db, dir, ddoc_id, &args.update)?;
        let vi = idx.def.view_index(view).ok_or_else(|| CouchError::not_found("missing_named_view"))?;
        check_partition(&idx.def, args)?;
        Ok(view_response(query_view(&snap, &st, &idx.def, vi, args)?, args))
    })
}

/// Partitioned views are queried within a partition, global ones not.
fn check_partition(def: &IndexDef, args: &QueryArgs) -> Result<()> {
    let err = |m: &str| Err(CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", m));
    match (def.partitioned, args.partition.is_some()) {
        (true, false) => err("`partition` parameter is mandatory for queries to this view."),
        (false, true) => err("`partition` parameter is not supported in this design doc"),
        _ => Ok(()),
    }
}

/// Above every key a view can emit, to end a partition's range.
pub fn key_max() -> Value {
    json!({"\u{10ffff}\u{10ffff}": null})
}

/// Translate a query within a partition to the [partition, key] keys the
/// index stores: bound the range to the partition and wrap keys.
fn partition_args(args: &QueryArgs, p: &str) -> QueryArgs {
    let mut a = args.clone();
    let wrap = |k: &Value| json!([p, k]);
    let (lo, hi) = (json!([p]), json!([p, key_max()]));
    a.keys = args.keys.as_ref().map(|ks| ks.iter().map(wrap).collect());
    let (first, last) = if args.descending { (hi.clone(), lo.clone()) } else { (lo, hi) };
    a.start_key = Some(args.start_key.as_ref().map(wrap).unwrap_or(first));
    a.end_key = Some(args.end_key.as_ref().map(wrap).unwrap_or(last));
    if args.end_key.is_none() {
        a.inclusive_end = true;
    }
    a
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
    crate::stat!("couchdb.httpd.view_reads").inc();
    Ok(crate::http::rows_response(&view_json(req, db, ddoc_id, view, &args)?))
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
            let args = QueryArgs::from_query(obj, base.clone())?;
            check_partition(&idx.def, &args)?;
            results.push(view_response(query_view(&snap, &st, &idx.def, vi, &args)?, &args));
        }
        Ok(json!({"results": results}))
    }).map_err(|mut e| {
        // multi_query_view/4 sends its errors without a stack.
        e.stack_ref = None;
        e
    })?;
    Ok(crate::http::rows_response(&v))
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
    crate::api::require_json_ctype(req)?;
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
    cleanup_index_files(&req.server, db)?;
    // Search indexes too, as fabric_index_cleanup does.
    crate::nouveau::cleanup(&req.server, db)?;
    Ok(json_response(StatusCode::ACCEPTED, &json!({"ok": true})))
}

/// The ids of a database's live design docs.
pub fn design_doc_ids(snap: &Snapshot) -> Result<Vec<String>> {
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
    Ok(ddocs)
}

/// Remove index files no design doc uses any more.
pub fn cleanup_index_files(server: &Server, db: &Arc<Db>) -> Result<()> {
    let snap = db.snapshot();
    let mut live = std::collections::HashSet::new();
    for id in design_doc_ids(&snap)? {
        if let Ok(d) = crate::changes::load_ddoc(&snap, &id)
            && let Ok(def) = IndexDef::from_ddoc(&d, snap.partitioned()) {
                live.insert(def.sig);
            }
    }
    // couch_index_util:cleanup_purges/3: their purge checkpoints too.
    let prefix = purge_checkpoint_id("");
    let range = Range { start: Some(prefix.clone()), end: Some("_local/purge-mrview.".into()), inclusive_end: false };
    let mut dead = Vec::new();
    snap.fold_locals(&range, Dir::Fwd, |id, _| {
        if id.strip_prefix(&prefix).is_some_and(|sig| !live.contains(sig)) {
            dead.push(id.to_string());
        }
        true
    })?;
    for id in dead {
        db.put_local_blocking(&id, None)?;
    }
    let dir = index_dir_of(server, &db.name);
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            // SIG.view, and SIG.compact.view or SIG.new.view while a live
            // index is compacting or rebuilding.
            if let Some(sig) = name.strip_suffix(".view").and_then(|n| n.split('.').next())
                && !live.contains(sig) {
                    let _ = std::fs::remove_file(e.path());
                    db.ext.lock().remove(&format!("mrview:{sig}"));
                }
        }
    }
    Ok(())
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

#[cfg(test)]
mod signature_tests {
    use super::*;

    #[test]
    fn signatures_as_couchdb_computes_them() {
        let ddoc = json!({"_id": "_design/v", "views": {"m": {"map": "function(d){emit(1,1)}"}}});
        assert_eq!(view_signature(&ddoc), "41363480d8c47748b9cde303b08b598e");
    }
}
