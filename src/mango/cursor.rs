//! mango_cursor: choosing a query's index, and saying why (explain's
//! index candidates and selector hints), then running the query over a
//! view (mango_cursor_view), over _all_docs (mango_cursor_special) or
//! over a nouveau index (mango_cursor_nouveau), with mango_json_bookmark's
//! bookmarks and mango_execution_stats' accounting.
//!
//! The accounting follows CouchDB's split between the shard, which walks
//! the index, matches documents and streams those that match with the
//! statistics gathered since the last, and the coordinator, which skips
//! and limits them: rows the coordinator drops (design docs from
//! _all_docs, the bookmarked row) take their statistics with them, and
//! the scan stops at the first match past the limit.

use super::idx::{self, Idx, Kind};
use super::opts::FindOpts;
use super::selector::{self, Bound, Got, Range, RangeOp, Selector, get_path, parse_field};
use super::error;
use crate::btree::{Dir, Range as KeyRange};
use crate::collate::Json;
use crate::db::{Db, Snapshot};
use crate::doc::DocOpts;
use crate::error::{CouchError, Result};
use crate::http::Req;
use crate::replicator::rep::{Term, ejson};
use crate::views::{self, IdBound};
use base64::Engine;
use serde_json::{Map, Value, json};
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq)]
enum Purpose {
    Explain,
    Find,
}

/// What get_usable_indexes/4 and the cursors found, for explain.
#[derive(Default)]
struct Trace {
    all: Vec<Idx>,
    global: Vec<Idx>,
    partition: Vec<Idx>,
    usable: Vec<Idx>,
    usability: Vec<(Idx, bool, Vec<&'static str>)>,
    filtered: Vec<Idx>,
    of_type: Vec<Idx>,
    /// The cursor's candidates in order of preference, with how far
    /// each falls short of the selector's indexable fields.
    sorted: Vec<(Idx, i64)>,
}

struct Cursor {
    /// None when no index is usable (explain only).
    index: Option<Idx>,
    /// A view's or _all_docs' ranges, of its leading columns; [None] when
    /// they can't be met.
    ranges: Vec<Option<Range>>,
    trace: Trace,
    selector: Selector,
    opts: FindOpts,
    limit: u64,
}

fn contains(set: &[Idx], i: &Idx) -> bool {
    set.iter().any(|x| x.same(i))
}

fn minus(a: &[Idx], b: &[Idx]) -> Vec<Idx> {
    a.iter().filter(|i| !contains(b, i)).cloned().collect()
}

fn ddoc_name(d: &str) -> &str {
    d.strip_prefix("_design/").unwrap_or(d)
}

/// maybe_filter_indexes_by_ddoc/2: the indexes `use_index` names.
fn filter_by_ddoc(indexes: &[Idx], use_index: &[String]) -> Vec<Idx> {
    let (ddoc, name) = match use_index {
        [d] => (format!("_design/{}", ddoc_name(d)), None),
        [d, n] => (format!("_design/{}", ddoc_name(d)), Some(n)),
        _ => return Vec::new(),
    };
    indexes.iter().filter(|i| i.ddoc.as_deref() == Some(ddoc.as_str()) && name.is_none_or(|n| *n == i.name)).cloned().collect()
}

/// mango_cursor:create/4.
fn create(req: &Req, snap: &Snapshot, opts: FindOpts, purpose: Purpose) -> Result<Cursor> {
    let selector = Selector::from_normalized(selector::normalize_selector(&opts.selector)?);
    let sel = &selector.norm;
    // mango_idx:get_usable_indexes/4
    let all = idx::list(snap, crate::nouveau::enabled(&req.server))?;
    let global: Vec<Idx> = all.iter().filter(|i| i.partial_filter().is_none()).cloned().collect();
    let mut usable0: Vec<Idx> = global.iter().cloned().chain(filter_by_ddoc(&all, &opts.use_index)).collect();
    usable0.sort_by(Idx::term_cmp);
    usable0.dedup_by(|a, b| a.same(b));
    let partitioned = !opts.partition.is_empty();
    let partition: Vec<Idx> = usable0.into_iter().filter(|i| i.kind == Kind::Special || i.partitioned == partitioned).collect();
    let sort = opts.sort_fields();
    let mut usability = Vec::new();
    for i in &partition {
        let (ok, why) = i.is_usable(sel, &sort)?;
        usability.push((i.clone(), ok, why));
    }
    let usable: Vec<Idx> = usability.iter().filter(|u| u.1).map(|u| u.0.clone()).collect();
    if usable.is_empty() && purpose == Purpose::Find {
        let reason = match (snap.partitioned(), partitioned) {
            (false, _) => "No index exists for this sort, try indexing by the sort fields.",
            (true, true) => "No partitioned index exists for this sort, try indexing by the sort fields.",
            (true, false) => "No global index exists for this sort, try indexing by the sort fields.",
        };
        return Err(error(400, "no_usable_index", reason));
    }
    let trace = Trace { all, global, partition, usable: usable.clone(), usability, ..Default::default() };
    let chosen = filter_by_ddoc(&usable, &opts.use_index);
    let indexes = if !chosen.is_empty() {
        chosen
    } else if opts.allow_fallback || (opts.use_index.is_empty() && usable.len() > 1) {
        usable
    } else {
        let reason = match opts.use_index.as_slice() {
            [d] => format!("_design/{} specified by `use_index` could not be found or it is not suitable.", ddoc_name(d)),
            [d, n] => format!("_design/{}, {n} specified by `use_index` could not be found or it is not suitable.", ddoc_name(d)),
            _ => "You must specify an index with the `use_index` parameter.".into(),
        };
        return Err(error(400, "invalid_index", reason));
    };
    create_cursor(req, trace, indexes, selector, opts)
}

/// create_cursor/4: the first kind of cursor (view, nouveau, special)
/// that has indexes picks among them.
fn create_cursor(req: &Req, mut trace: Trace, indexes: Vec<Idx>, selector: Selector, opts: FindOpts) -> Result<Cursor> {
    let limit = opts.limit;
    if indexes.is_empty() {
        return Ok(Cursor { index: None, ranges: Vec::new(), trace, selector, opts, limit });
    }
    trace.filtered = indexes.clone();
    let of_type: Vec<Idx> = [Kind::Json, Kind::Nouveau, Kind::Special]
        .iter()
        .map(|k| indexes.iter().filter(|i| i.kind == *k).cloned().collect::<Vec<_>>())
        .find(|g| !g.is_empty())
        .unwrap_or_default();
    trace.of_type = of_type.clone();
    if of_type[0].kind == Kind::Nouveau {
        if of_type.len() > 1 {
            return Err(error(400, "multiple_nouveau_indexes", "You must specify an index with the `use_index` parameter."));
        }
        let max = req.server.config.get_int("nouveau", "max_limit", 200u64);
        let index = Some(of_type[0].clone());
        return Ok(Cursor { index, ranges: Vec::new(), trace, selector, limit: limit.min(max), opts });
    }
    let mut field_ranges = selector::field_ranges(&selector.norm)?;
    if of_type[0].kind == Kind::Special {
        // The catch-all range, used only when nothing better is.
        let all = Range { lop: RangeOp::Gt, low: Bound::Val(Value::Null), hop: RangeOp::Lt, high: Bound::Max };
        field_ranges.push(("_id".into(), Some(all)));
    }
    // composite_indexes/2 and choose_best_index/1
    let mut composited: Vec<(Idx, Vec<Option<Range>>, i64)> = of_type
        .iter()
        .map(|i| {
            let mut prefix = Vec::new();
            for c in i.columns().unwrap_or_default() {
                match field_ranges.iter().find(|(f, _)| *f == c) {
                    Some((_, r)) => prefix.push(r.clone()),
                    None => break,
                }
            }
            let diff = field_ranges.len() as i64 - prefix.len() as i64;
            (i.clone(), prefix, diff)
        })
        .collect();
    let ddoc = |i: &Idx| i.ddoc.as_ref().map(|d| d.as_bytes().to_vec());
    composited.sort_by(|a, b| {
        a.2.cmp(&b.2).then_with(|| a.0.width().cmp(&b.0.width())).then_with(|| ddoc(&a.0).cmp(&ddoc(&b.0))).then_with(|| a.0.name.cmp(&b.0.name))
    });
    trace.sorted = composited.iter().map(|(i, _, d)| (i.clone(), *d)).collect();
    let (index, prefix, _) = composited.swap_remove(0);
    // maybe_noop_range/2: an empty $and, $or, $in or $all matches nothing.
    let noop = selector.norm.as_object().filter(|o| o.len() == 1).and_then(|o| o.iter().next()).is_some_and(|(op, arg)| {
        ["$all", "$and", "$or", "$in"].contains(&op.as_str()) && arg.as_array().is_some_and(Vec::is_empty)
    });
    let ranges = if noop { vec![None] } else { prefix };
    Ok(Cursor { index: Some(index), ranges, trace, selector, opts, limit })
}

// ----------------------------------------------------------- view args

/// A view key bound: values, with None for the greatest object.
#[derive(Clone, Debug)]
enum Key {
    Null,
    View(Vec<Option<Value>>),
    /// An _all_docs key; None is the greatest string.
    Doc(Option<Value>),
}

impl Key {
    /// maybe_replace_max_json/1.
    fn to_json(&self) -> Value {
        match self {
            Key::Null => Value::Null,
            Key::View(parts) => Value::Array(parts.iter().map(|p| p.clone().unwrap_or_else(|| json!("<MAX>"))).collect()),
            Key::Doc(None) => json!("<MAX>"),
            Key::Doc(Some(v)) => v.clone(),
        }
    }
}

fn bound_value(b: &Bound) -> Value {
    match b {
        Bound::Val(v) => v.clone(),
        _ => Value::Null,
    }
}

/// mango_idx_view:start_key/1.
fn view_start(ranges: &[Option<Range>]) -> Vec<Option<Value>> {
    let mut out = Vec::new();
    for r in ranges.iter().flatten() {
        if r.lop == RangeOp::Gt && r.low.is_special() {
            break;
        }
        out.push(Some(bound_value(&r.low)));
    }
    out
}

/// mango_idx_view:end_key/1.
fn view_end(ranges: &[Option<Range>]) -> Vec<Option<Value>> {
    let mut out = Vec::new();
    for r in ranges.iter().flatten() {
        if r.hop == RangeOp::Lt && r.high.is_special() {
            out.push(None);
            return out;
        }
        out.push(Some(bound_value(&r.high)));
    }
    out.push(None);
    out
}

/// The view arguments of a cursor (apply_cursor_opts/1).
struct MrArgs {
    start: Key,
    end: Key,
    include_docs: bool,
    rev: bool,
    stable: bool,
    update: bool,
    conflicts: bool,
    partition: Option<String>,
    covering: bool,
}

impl Cursor {
    fn index(&self) -> &Idx {
        self.index.as_ref().expect("an index")
    }

    fn empty(&self) -> bool {
        matches!(self.ranges.as_slice(), [None])
    }

    /// required_fields/1: what a covering index must have.
    fn required_fields(&self) -> Option<Vec<Value>> {
        let mut f = self.opts.fields.clone()?;
        f.extend(selector::fields(&self.selector.norm).into_iter().map(Value::String));
        Some(f)
    }

    fn mrargs(&self) -> MrArgs {
        let idx = self.index();
        let (start, end) = match (self.empty(), idx.kind) {
            (true, _) => (Key::Null, Key::Null),
            (false, Kind::Special) => {
                let r = self.ranges.first().cloned().flatten();
                let start = match &r {
                    Some(r) if r.lop == RangeOp::Gt && r.low.is_special() => Some(json!("")),
                    Some(r) => Some(bound_value(&r.low)),
                    None => Some(json!("")),
                };
                let end = match &r {
                    Some(r) if r.hop == RangeOp::Lt && r.high.is_special() => None,
                    Some(r) => Some(bound_value(&r.high)),
                    None => None,
                };
                (Key::Doc(start), Key::Doc(end))
            }
            (false, _) => (Key::View(view_start(&self.ranges)), Key::View(view_end(&self.ranges))),
        };
        let o = &self.opts;
        let rev = o.descending();
        let (start, end) = if rev { (end, start) } else { (start, end) };
        let covering = covers(idx, self.required_fields().as_deref());
        MrArgs {
            start,
            end,
            include_docs: o.r == 1 && !covering,
            rev,
            stable: o.stale_ok || o.stable,
            update: o.update && !o.stale_ok,
            conflicts: o.conflicts,
            partition: Some(o.partition.clone()).filter(|p| !p.is_empty()),
            covering,
        }
    }
}

/// mango_idx_view:covers/2: whether the index has every field wanted.
fn covers(idx: &Idx, fields: Option<&[Value]>) -> bool {
    let Some(fields) = fields else { return false };
    if idx.kind != Kind::Json {
        return false;
    }
    let mut available = vec!["_id".to_string()];
    available.extend(idx.columns().unwrap_or_default());
    fields.iter().all(|f| f.as_str().is_some_and(|f| available.iter().any(|a| a == f)))
}

// -------------------------------------------------------------- explain

fn candidates(c: &Cursor) -> Vec<Value> {
    let t = &c.trace;
    let partial = minus(&t.all, &t.global);
    let out_of_scope = minus(&t.global, &t.partition);
    let not_usable = minus(&t.partition, &t.usable);
    let excluded = minus(&t.usable, &t.filtered);
    let unfavored = minus(&t.filtered, &t.of_type);
    let some = |s: &[Idx]| !s.is_empty() as i64;
    let unfavored_rank = (t.sorted.len() as i64).max(1);
    let excluded_rank = unfavored_rank + some(&unfavored);
    let not_usable_rank = excluded_rank + some(&excluded);
    let out_of_scope_rank = not_usable_rank + some(&not_usable);
    let partial_rank = out_of_scope_rank + some(&out_of_scope);
    // Each index's usability (anded), reasons (appended) and ranking
    // (summed), as enhance_candidates/2 merges them.
    #[derive(Default)]
    struct Tags {
        usable: Option<bool>,
        reasons: Vec<&'static str>,
        ranking: Option<i64>,
    }
    let mut tags: Vec<(Idx, Tags)> = t.all.iter().map(|i| (i.clone(), Tags::default())).collect();
    let mut tag = |set: &[Idx], usable: Option<bool>, reasons: &[&'static str], ranking: Option<i64>| {
        for (_, t) in tags.iter_mut().filter(|(i, _)| contains(set, i)) {
            t.usable = match (t.usable, usable) {
                (Some(a), Some(b)) => Some(a && b),
                (a, b) => a.or(b),
            };
            t.reasons.extend_from_slice(reasons);
            t.ranking = match (t.ranking, ranking) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            };
        }
    };
    tag(&partial, Some(false), &["is_partial"], Some(partial_rank));
    tag(&out_of_scope, Some(false), &["scope_mismatch"], Some(out_of_scope_rank));
    tag(&not_usable, Some(false), &[], Some(not_usable_rank));
    tag(&excluded, Some(true), &["excluded_by_user"], Some(excluded_rank));
    tag(&unfavored, Some(true), &["unfavored_type"], Some(unfavored_rank));
    tag(&t.of_type, Some(true), &[], None);
    for (i, ok, why) in &t.usability {
        if !ok {
            tag(std::slice::from_ref(i), None, why, None);
        }
    }
    let winner_width = c.index.as_ref().map(Idx::width).unwrap_or(0);
    for (n, (i, distance)) in t.sorted.iter().enumerate() {
        let reason = if *distance > 0 {
            "less_overlap"
        } else if i.width() > winner_width {
            "too_many_fields"
        } else {
            "alphabetically_comes_after"
        };
        tag(std::slice::from_ref(i), None, &[reason], Some(n as i64));
    }
    if let Some(w) = &c.index {
        tags.retain(|(i, _)| !i.same(w));
    }
    tags.sort_by(|a, b| Idx::term_cmp(&b.0, &a.0));
    tags.into_iter()
        .map(|(i, Tags { usable, reasons, ranking })| {
            let covering = match i.kind {
                Kind::Json => json!(covers(&i, c.opts.fields.as_deref())),
                _ => Value::Null,
            };
            json!({
                "index": i.to_json(),
                "analysis": {
                    "usable": usable.unwrap_or(false),
                    "reasons": reasons.first().map(|r| vec![json!({"name": r})]).unwrap_or_default(),
                    "ranking": ranking.unwrap_or(0),
                    "covering": covering,
                }
            })
        })
        .collect()
}

/// extract_selector_hints/1.
fn selector_hints(req: &Req, sel: &Value) -> Result<Vec<Value>> {
    let all: Vec<String> = selector::fields(sel);
    let mut kinds: Vec<(&str, Vec<String>)> = vec![("json", selector::indexable_fields(sel))];
    if crate::nouveau::enabled(&req.server) {
        let fields = crate::nouveau::mango::indexable_fields(sel)?;
        kinds.push(("nouveau", fields.iter().map(|f| f.split(':').next().unwrap_or("").to_string()).collect()));
    }
    Ok(kinds
        .into_iter()
        .map(|(kind, mut indexable)| {
            indexable.sort();
            indexable.dedup();
            let mut unindexable: Vec<String> = all.iter().filter(|f| !indexable.contains(f)).cloned().collect();
            unindexable.sort();
            unindexable.dedup();
            json!({"type": kind, "indexable_fields": indexable, "unindexable_fields": unindexable})
        })
        .collect())
}

/// mango_cursor_nouveau:sort_query/1.
fn nouveau_sort(sort: &[(String, String)]) -> Result<Option<Vec<String>>> {
    if sort.is_empty() {
        return Ok(None);
    }
    sort.iter()
        .map(|(f, d)| {
            let e = crate::nouveau::mango::escape_user(f)?;
            Ok(if d == "asc" { e } else { format!("-{e}") })
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

/// mango_cursor:explain/1.
pub fn explain(req: &Req, db: &Arc<Db>, opts: FindOpts) -> Result<Value> {
    let snap = db.snapshot();
    let c = create(req, &snap, opts, Purpose::Explain)?;
    let mut out = Map::new();
    out.insert("dbname".into(), json!(db.name));
    out.insert("index".into(), c.index.as_ref().map(Idx::to_json).unwrap_or(Value::Null));
    out.insert("partitioned".into(), json!(snap.partitioned()));
    out.insert("selector".into(), c.selector.norm.clone());
    out.insert("opts".into(), c.opts.to_json());
    out.insert("limit".into(), json!(c.limit));
    out.insert("skip".into(), json!(c.opts.skip));
    out.insert("fields".into(), json!(c.opts.fields.clone().unwrap_or_default()));
    out.insert("index_candidates".into(), Value::Array(candidates(&c)));
    out.insert("selector_hints".into(), Value::Array(selector_hints(req, &c.selector.norm)?));
    match c.index.as_ref().map(|i| i.kind) {
        None => {}
        Some(Kind::Nouveau) => {
            out.insert("query".into(), json!(crate::nouveau::mango::selector_query(&c.selector.norm)?));
            out.insert("partition".into(), json!(Some(&c.opts.partition).filter(|p| !p.is_empty())));
            out.insert("sort".into(), json!(nouveau_sort(&c.opts.sort)?));
        }
        Some(_) => {
            let a = c.mrargs();
            out.insert(
                "mrargs".into(),
                json!({
                    "include_docs": a.include_docs,
                    "view_type": "map",
                    "reduce": false,
                    "partition": a.partition,
                    "start_key": a.start.to_json(),
                    "end_key": a.end.to_json(),
                    "direction": if a.rev { "rev" } else { "fwd" },
                    "stable": a.stable,
                    "update": a.update,
                    "conflicts": if a.conflicts { json!(true) } else { json!("undefined") },
                }),
            );
            out.insert("covering".into(), json!(a.covering));
        }
    }
    Ok(Value::Object(out))
}

// ------------------------------------------------------------ bookmarks

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.trim_end_matches('=');
    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s).ok()
}

/// mango_json_bookmark:create/1: term_to_binary([{startkey_docid, Id},
/// {startkey, Key}], [compressed, {minor_version, 1}]), base64url'd.
fn pack_bookmark(key: &Value, docid: &str) -> String {
    let pair = |k: &'static str, v: Term| Term::Tuple(vec![Term::Atom(k), v]);
    let term = Term::List(vec![pair("startkey_docid", Term::Bin(docid.as_bytes().to_vec())), pair("startkey", ejson(key))]);
    let raw = term.encode();
    let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
    let compressed = z.write_all(&raw[1..]).and_then(|_| z.finish()).ok();
    let bin = match compressed {
        // Compressed only when that's no bigger.
        Some(c) if 6 + c.len() <= raw.len() => {
            let mut b = vec![131, 80];
            b.extend_from_slice(&((raw.len() - 1) as u32).to_be_bytes());
            b.extend_from_slice(&c);
            b
        }
        _ => raw,
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bin)
}

/// A term as binary_to_term/2 reads one.
#[derive(Debug)]
enum T {
    Atom(String),
    Int(i128),
    /// An integer beyond 128 bits, in decimal.
    Big(String),
    Float(f64),
    Bin(Vec<u8>),
    List(Vec<T>),
    Tuple(Vec<T>),
}

struct TermReader<'a> {
    b: &'a [u8],
}

impl TermReader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.b.len() < n {
            return None;
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Some(h)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u16(&mut self) -> Option<usize> {
        self.take(2).map(|b| u16::from_be_bytes([b[0], b[1]]) as usize)
    }

    fn u32(&mut self) -> Option<usize> {
        self.take(4).map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize)
    }

    fn term(&mut self, depth: usize) -> Option<T> {
        if depth > 512 {
            return None;
        }
        Some(match self.u8()? {
            97 => T::Int(self.u8()? as i128),
            98 => T::Int(i32::from_be_bytes(self.take(4)?.try_into().ok()?) as i128),
            70 => T::Float(f64::from_be_bytes(self.take(8)?.try_into().ok()?)),
            100 | 118 => {
                let n = self.u16()?;
                T::Atom(String::from_utf8(self.take(n)?.to_vec()).ok()?)
            }
            115 | 119 => {
                let n = self.u8()? as usize;
                T::Atom(String::from_utf8(self.take(n)?.to_vec()).ok()?)
            }
            104 => {
                let n = self.u8()? as usize;
                T::Tuple((0..n).map(|_| self.term(depth + 1)).collect::<Option<_>>()?)
            }
            105 => {
                let n = self.u32()?;
                T::Tuple((0..n).map(|_| self.term(depth + 1)).collect::<Option<_>>()?)
            }
            106 => T::List(Vec::new()),
            107 => {
                let n = self.u16()?;
                T::List(self.take(n)?.iter().map(|b| T::Int(*b as i128)).collect())
            }
            108 => {
                let n = self.u32()?;
                let items = (0..n).map(|_| self.term(depth + 1)).collect::<Option<Vec<_>>>()?;
                match self.term(depth + 1)? {
                    T::List(tail) if tail.is_empty() => T::List(items),
                    _ => return None,
                }
            }
            109 => {
                let n = self.u32()?;
                T::Bin(self.take(n)?.to_vec())
            }
            tag @ (110 | 111) => {
                let n = if tag == 110 { self.u8()? as usize } else { self.u32()? };
                let sign = self.u8()?;
                let digits = self.take(n)?;
                if n <= 15 {
                    let mag = digits.iter().rev().fold(0i128, |acc, d| (acc << 8) | *d as i128);
                    T::Int(if sign == 1 { -mag } else { mag })
                } else {
                    T::Big(format!("{}{}", if sign == 1 { "-" } else { "" }, decimal(digits)))
                }
            }
            _ => return None,
        })
    }
}

/// A magnitude's bytes (least significant first) in decimal.
fn decimal(le: &[u8]) -> String {
    let mut dec: Vec<u8> = vec![0];
    for b in le.iter().rev() {
        let mut carry = *b as u32;
        for d in dec.iter_mut() {
            let x = *d as u32 * 256 + carry;
            *d = (x % 10) as u8;
            carry = x / 10;
        }
        while carry > 0 {
            dec.push((carry % 10) as u8);
            carry /= 10;
        }
    }
    dec.iter().rev().map(|d| (b'0' + d) as char).collect()
}

/// A key as JSON, the way jiffy would have decoded it.
fn term_json(t: &T) -> Option<Value> {
    Some(match t {
        T::Atom(a) if a == "null" => Value::Null,
        T::Atom(a) if a == "true" => Value::Bool(true),
        T::Atom(a) if a == "false" => Value::Bool(false),
        T::Atom(_) => return None,
        T::Int(i) => crate::json::from_int128(*i),
        T::Big(s) => Value::Number(serde_json::Number::from_string_unchecked(s.clone())),
        T::Float(f) => Value::from(*f),
        T::Bin(b) => Value::String(String::from_utf8(b.clone()).ok()?),
        T::List(l) => Value::Array(l.iter().map(term_json).collect::<Option<_>>()?),
        T::Tuple(t) => match t.as_slice() {
            [T::List(pairs)] => {
                let mut m = Map::new();
                for p in pairs {
                    let T::Tuple(kv) = p else { return None };
                    let [T::Bin(k), v] = kv.as_slice() else { return None };
                    m.insert(String::from_utf8(k.clone()).ok()?, term_json(v)?);
                }
                Value::Object(m)
            }
            _ => return None,
        },
    })
}

/// mango_json_bookmark:unpack/1: a bookmark's start key and doc id.
fn unpack_bookmark(packed: &str) -> Result<(Value, Value)> {
    let bad = || error(400, "invalid_bookmark", format!("Invalid bookmark value: {}", Value::String(packed.to_string())));
    let bin = b64url_decode(packed).ok_or_else(bad)?;
    let body = match bin.as_slice() {
        [131, 80, rest @ ..] if rest.len() >= 4 => {
            let size = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
            let mut out = Vec::with_capacity(size.min(1 << 20));
            flate2::read::ZlibDecoder::new(&rest[4..]).take(size as u64 + 1).read_to_end(&mut out).map_err(|_| bad())?;
            out
        }
        [131, rest @ ..] => rest.to_vec(),
        _ => return Err(bad()),
    };
    let mut r = TermReader { b: &body };
    let T::List(props) = r.term(0).ok_or_else(bad)? else { return Err(bad()) };
    let get = |name: &str| {
        props.iter().find_map(|p| match p {
            T::Tuple(kv) => match kv.as_slice() {
                [T::Atom(a), v] if a == name => Some(v),
                _ => None,
            },
            _ => None,
        })
    };
    let (key, docid) = (get("startkey").ok_or_else(bad)?, get("startkey_docid").ok_or_else(bad)?);
    Ok((term_json(key).ok_or_else(bad)?, term_json(docid).ok_or_else(bad)?))
}

// ------------------------------------------------------------ execution

#[derive(Default, Clone, Copy)]
struct Stats {
    keys: u64,
    docs: u64,
    quorum: u64,
    results: u64,
}

/// A row's document, as the shard has it.
enum RowDoc {
    /// Loaded (include_docs).
    Doc(Value),
    /// Made from the index's key (a covering index).
    Covered(Value),
    /// Not loaded: the coordinator reads it (r > 1).
    Deferred,
    /// Gone.
    Missing,
}

struct Exec<'a> {
    selector: &'a Selector,
    fields: Option<&'a [Value]>,
    limit: u64,
    skip: u64,
    /// Rows the coordinator skips first (the bookmarked row).
    bookmark_skip: u64,
    /// Dropping design documents (from _all_docs).
    all_docs: bool,
    /// Statistics gathered at the shard since its last row sent.
    pending: Stats,
    stats: Stats,
    docs: Vec<Value>,
    last: Option<(Value, String)>,
    load: &'a dyn Fn(&str) -> Result<Option<Value>>,
}

impl Exec<'_> {
    /// One row of the index; false to stop.
    fn row(&mut self, key: &Value, id: &str, doc: RowDoc) -> Result<bool> {
        self.pending.keys += 1;
        crate::stat!("mango.keys_examined").inc();
        let sent = match doc {
            RowDoc::Missing => return Ok(true),
            RowDoc::Doc(d) => {
                self.pending.docs += 1;
                crate::stat!("mango.docs_examined").inc();
                if !self.selector.try_matches(&d)? {
                    return Ok(true);
                }
                Some(extract(&d, self.fields)?)
            }
            RowDoc::Covered(d) => {
                if !self.selector.try_matches(&d)? {
                    return Ok(true);
                }
                Some(extract(&d, self.fields)?)
            }
            RowDoc::Deferred => None,
        };
        // At the coordinator.
        if self.bookmark_skip > 0 {
            self.bookmark_skip -= 1;
            self.pending = Stats::default();
            return Ok(true);
        }
        if self.all_docs && id.starts_with("_design/") {
            self.pending = Stats::default();
            return Ok(true);
        }
        self.take_pending();
        let doc = match sent {
            Some(d) => d,
            None => {
                self.stats.quorum += 1;
                crate::stat!("mango.quorum_docs_examined").inc();
                let Some(d) = (self.load)(id)? else { return Ok(true) };
                if !self.selector.try_matches(&d)? {
                    return Ok(true);
                }
                extract(&d, self.fields)?
            }
        };
        if self.limit > 0 {
            self.last = Some((key.clone(), id.to_string()));
        }
        if self.skip > 0 {
            self.skip -= 1;
            return Ok(true);
        }
        if self.limit == 0 {
            return Ok(false);
        }
        self.limit -= 1;
        self.stats.results += 1;
        crate::stat!("mango.results_returned").inc();
        self.docs.push(doc);
        Ok(true)
    }

    fn take_pending(&mut self) {
        self.stats.keys += self.pending.keys;
        self.stats.docs += self.pending.docs;
        self.pending = Stats::default();
    }
}

/// mango_doc:set_field/3 on an object: missing objects along the path
/// made.
fn set_field(doc: &mut Value, path: &[String], v: Value) {
    let Value::Object(o) = doc else { return };
    match path {
        [] => {}
        [name] => {
            o.insert(name.clone(), v);
        }
        [name, rest @ ..] => match o.get_mut(name) {
            Some(elem) => set_field(elem, rest, v),
            None => {
                let mut nested = v;
                for p in rest.iter().rev() {
                    let mut m = Map::new();
                    m.insert(p.clone(), nested);
                    nested = Value::Object(m);
                }
                o.insert(name.clone(), nested);
            }
        },
    }
}

/// mango_fields:extract/2.
fn extract(doc: &Value, fields: Option<&[Value]>) -> Result<Value> {
    let Some(fields) = fields else { return Ok(doc.clone()) };
    let mut out = Value::Object(Map::new());
    for f in fields {
        let Some(f) = f.as_str() else { return Err(error(500, "unknown_error", "badarg")) };
        let path = parse_field(f)?;
        if let Got::Found(v) = get_path(doc, &path) {
            set_field(&mut out, &path, v.clone());
        }
    }
    Ok(out)
}

/// derive_doc_from_index/2: a covering index's row as a document.
fn derive_doc(columns: &[String], key: &Value, id: &str) -> Result<Value> {
    let mut doc = json!({"_id": id});
    let keys = key.as_array().map(Vec::as_slice).unwrap_or_default();
    for (c, k) in columns.iter().zip(keys).rev() {
        set_field(&mut doc, &parse_field(c)?, k.clone());
    }
    Ok(doc)
}

/// An _all_docs bound in term order: below every id, an id, or above.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Id {
    Below,
    Str(String),
    Above,
}

impl Id {
    fn of(k: &Key) -> Id {
        match k {
            Key::Doc(None) => Id::Above,
            Key::Doc(Some(Value::String(s))) => Id::Str(s.clone()),
            _ => Id::Below,
        }
    }
}

fn scan_all_docs(c: &Cursor, a: &MrArgs, snap: &Snapshot, bookmark: Option<&Value>, ex: &mut Exec) -> Result<bool> {
    let (mut start, mut end) = (Id::of(&a.start), Id::of(&a.end));
    if let Some(k) = bookmark {
        start = Id::of(&Key::Doc(Some(k.clone())));
    }
    if let Some(p) = &a.partition {
        let (lo, hi) = (Id::Str(format!("{p}:")), Id::Str(format!("{p};")));
        if a.rev {
            start = start.min(hi);
            end = end.max(lo);
        } else {
            start = start.max(lo);
            end = end.min(hi);
        }
    }
    let empty = if a.rev { start < end } else { start > end };
    let (first, last) = if a.rev { (Id::Above, Id::Below) } else { (Id::Below, Id::Above) };
    if empty || start == last || end == first {
        return Ok(false);
    }
    let range = KeyRange {
        start: match start {
            Id::Str(s) => Some(s),
            _ => None,
        },
        end: match end {
            Id::Str(s) => Some(s),
            _ => None,
        },
        inclusive_end: true,
    };
    let opts = DocOpts { conflicts: a.conflicts, ..Default::default() };
    let mut failed = None;
    let mut stopped = false;
    snap.fold_docs(if a.rev { Dir::Rev } else { Dir::Fwd }, &range, |fdi| {
        if fdi.deleted {
            return Ok(true);
        }
        let doc = if a.include_docs { RowDoc::Doc(snap.winner_json(&fdi, &opts)?) } else { RowDoc::Deferred };
        match ex.row(&json!(fdi.id), &fdi.id, doc) {
            Ok(go) => {
                stopped = !go;
                Ok(go)
            }
            Err(e) => {
                failed = Some(e);
                Ok(false)
            }
        }
    })?;
    let _ = c;
    match failed {
        Some(e) => Err(e),
        None => Ok(stopped),
    }
}

fn scan_view(req: &Req, db: &Arc<Db>, c: &Cursor, a: &MrArgs, bookmark: Option<(&Value, &Value)>, ex: &mut Exec) -> Result<bool> {
    let idx = c.index();
    let ddoc = idx.ddoc.clone().unwrap_or_default();
    let update = if a.update { "true" } else { "false" };
    let (vsnap, index, st) = views::prepare(db, views::index_dir(req, db), &ddoc, update)?;
    let vi = index.def.view_index(&idx.name).ok_or_else(CouchError::missing)?;
    let tree = &st.views[vi];
    let to_value = |k: &Key| match k {
        Key::View(parts) => Value::Array(parts.iter().map(|p| p.clone().unwrap_or_else(views::key_max)).collect()),
        _ => Value::Null,
    };
    let wrap = |k: Value| match &a.partition {
        Some(p) => json!([p, k]),
        None => k,
    };
    let (start_id, end_id) = if a.rev { (IdBound::Max, IdBound::Min) } else { (IdBound::Min, IdBound::Max) };
    let mut start = (Json(wrap(to_value(&a.start))), start_id);
    if let Some((k, id)) = bookmark {
        start = (Json(wrap(k.clone())), IdBound::Id(id.as_str().unwrap_or("").to_string()));
    }
    let range = KeyRange { start: Some(start), end: Some((Json(wrap(to_value(&a.end))), end_id)), inclusive_end: true };
    let columns = idx.columns().unwrap_or_default();
    let opts = DocOpts { conflicts: a.conflicts, ..Default::default() };
    let mut failed = None;
    let mut stopped = false;
    tree.fold(if a.rev { Dir::Rev } else { Dir::Fwd }, &range, |(k, id), _| {
        let IdBound::Id(id) = id else { return Ok(true) };
        let key = match &a.partition {
            Some(_) => k.0.get(1).cloned().unwrap_or(Value::Null),
            None => k.0.clone(),
        };
        let doc = if a.covering {
            match derive_doc(&columns, &key, id) {
                Ok(d) => RowDoc::Covered(d),
                Err(e) => {
                    failed = Some(e);
                    return Ok(false);
                }
            }
        } else if a.include_docs {
            match vsnap.get_fdi(id)?.filter(|f| !f.deleted) {
                Some(fdi) => RowDoc::Doc(vsnap.winner_json(&fdi, &opts)?),
                None => RowDoc::Missing,
            }
        } else {
            RowDoc::Deferred
        };
        match ex.row(&key, id, doc) {
            Ok(go) => {
                stopped = !go;
                Ok(go)
            }
            Err(e) => {
                failed = Some(e);
                Ok(false)
            }
        }
    })?;
    match failed {
        Some(e) => Err(e),
        None => Ok(stopped),
    }
}

/// A _find response as mango_httpd streams it: the docs, then each key
/// added (bookmark, execution_stats, warning).
fn find_body(docs: &[Value], kvs: &[(&str, Value)]) -> Vec<u8> {
    let mut out = b"{\"docs\":[".to_vec();
    for (i, d) in docs.iter().enumerate() {
        out.extend_from_slice(if i == 0 { b"\r\n" } else { b",\r\n" });
        out.extend(crate::json::to_vec(d));
    }
    out.extend_from_slice(b"\r\n]");
    for (k, v) in kvs {
        out.extend_from_slice(b",\r\n");
        out.extend(crate::json::to_vec(k));
        out.extend_from_slice(b": ");
        out.extend(crate::json::to_vec(v));
    }
    // end_delayed_json_response/1 ends it with a newline of its own.
    out.extend_from_slice(b"}\r\n\n");
    out
}

/// maybe_add_stats/4 and maybe_add_warning/4.
fn finish(req: &Req, snap: &Snapshot, c: &Cursor, stats: Stats, t0: Instant, kvs: &mut Vec<(&str, Value)>) {
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    crate::stat!("mango.query_time").observe(ms);
    crate::db::count_reads(snap, stats.docs);
    if c.opts.execution_stats {
        kvs.push((
            "execution_stats",
            json!({
                "total_keys_examined": stats.keys,
                "total_docs_examined": stats.docs,
                "total_quorum_docs_examined": stats.quorum,
                "results_returned": stats.results,
                "execution_time_ms": ms,
            }),
        ));
    }
    let idx = c.index();
    let mut warnings = Vec::new();
    let used = !filter_by_ddoc(std::slice::from_ref(idx), &c.opts.use_index).is_empty();
    match c.opts.use_index.as_slice() {
        [d] if !used => warnings.push(format!("_design/{} was not used because it does not contain a valid index for this query.", ddoc_name(d))),
        [d, n] if !used => warnings.push(format!("_design/{}, {n} was not used because it is not a valid index for this query.", ddoc_name(d))),
        _ => {}
    }
    if !warnings.is_empty() {
        crate::stat!("mango.query_invalid_index").inc();
    }
    if idx.kind == Kind::Special {
        crate::stat!("mango.unindexed_queries").inc();
        warnings.push("No matching index found, create an index to optimize query time.".into());
    }
    let scanned = (stats.docs + stats.quorum) as f64;
    let ratio = if stats.results == 0 { scanned } else { scanned / stats.results as f64 };
    let threshold = req.server.config.get_int("mango", "index_scan_warning_threshold", 10i64);
    if threshold > 0 && ratio > threshold as f64 {
        crate::stat!("mango.too_many_docs_scanned").inc();
        warnings.push("The number of documents examined is high in proportion to the number of results returned. Consider adding a more specific index to improve this.".into());
    }
    if !warnings.is_empty() {
        kvs.push(("warning", json!(warnings.join("\n"))));
    }
}

/// mango_crud:find/5: the response body.
pub fn find(req: &Req, db: &Arc<Db>, opts: FindOpts) -> Result<Vec<u8>> {
    let t0 = Instant::now();
    let snap = db.snapshot();
    let c = create(req, &snap, opts, Purpose::Find)?;
    if c.index().kind == Kind::Nouveau {
        return find_nouveau(req, db, &snap, &c, t0);
    }
    if c.empty() {
        return Ok(find_body(&[], &[]));
    }
    let a = c.mrargs();
    if a.partition.is_some() && !snap.partitioned() {
        return Err(error(400, "query_parse_error", "`partition` parameter is not supported on this db"));
    }
    let bookmark = c.opts.bookmark.as_deref().map(unpack_bookmark).transpose()?;
    let doc_opts = DocOpts { conflicts: a.conflicts, ..Default::default() };
    let load = |id: &str| -> Result<Option<Value>> {
        let snap = db.snapshot();
        match snap.get_fdi(id)?.filter(|f| !f.deleted) {
            Some(fdi) => Ok(Some(snap.winner_json(&fdi, &doc_opts)?)),
            None => Ok(None),
        }
    };
    let mut ex = Exec {
        selector: &c.selector,
        fields: c.opts.fields.as_deref(),
        limit: c.limit,
        skip: c.opts.skip,
        bookmark_skip: bookmark.is_some() as u64,
        all_docs: c.index().kind == Kind::Special,
        pending: Stats::default(),
        stats: Stats::default(),
        docs: Vec::new(),
        last: None,
        load: &load,
    };
    let stopped = match c.index().kind {
        Kind::Special => scan_all_docs(&c, &a, &snap, bookmark.as_ref().map(|b| &b.0), &mut ex)?,
        _ => scan_view(req, db, &c, &a, bookmark.as_ref().map(|b| (&b.0, &b.1)), &mut ex)?,
    };
    if !stopped {
        ex.take_pending();
    }
    let bookmark = match &ex.last {
        Some((k, id)) => pack_bookmark(k, id),
        None => c.opts.bookmark.clone().unwrap_or_else(|| "nil".into()),
    };
    let mut kvs = vec![("bookmark", json!(bookmark))];
    finish(req, &snap, &c, ex.stats, t0, &mut kvs);
    Ok(find_body(&ex.docs, &kvs))
}

/// mango_cursor_nouveau:execute/3: a page of hits at a time, keeping the
/// documents that match the selector (the query only narrows them).
fn find_nouveau(req: &Req, db: &Arc<Db>, snap: &Snapshot, c: &Cursor, t0: Instant) -> Result<Vec<u8>> {
    let idx = c.index();
    let query = crate::nouveau::mango::selector_query(&c.selector.norm)?;
    let ddoc_id = idx.ddoc.clone().unwrap_or_default();
    let fdi = snap.get_fdi(&ddoc_id)?.filter(|f| !f.deleted).ok_or_else(CouchError::missing)?;
    let spec = crate::nouveau::design_doc_to_index(&snap.winner_json(&fdi, &DocOpts::default())?, &idx.name)?;
    let max = req.server.config.get_int("nouveau", "max_limit", 200u64);
    let (mut limit, mut skip) = (c.limit, c.opts.skip);
    let sort = nouveau_sort(&c.opts.sort)?;
    let partition = Some(c.opts.partition.clone()).filter(|p| !p.is_empty());
    let search_error = |e: CouchError| error(500, "nouveau_search_error", format!("{}: {}", e.error, e.reason));
    let mut stats = Stats::default();
    let mut docs = Vec::new();
    let mut seen = std::collections::HashSet::new();
    // The user's bookmark goes unused (get_bookmark/1 wants a list); the
    // bookmark is the last hit's order.
    let mut after: Option<Vec<Value>> = None;
    'pages: while !query.is_empty() {
        let args = crate::nouveau::SearchArgs {
            bookmark: None,
            counts: None,
            include_docs: false,
            limit: (limit + skip).min(max) as i64,
            locale: None,
            partition: partition.clone(),
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
            let before = after.replace(h.order.clone());
            let Some(fdi) = snap.get_fdi(&h.id)?.filter(|f| !f.deleted) else { continue };
            let doc = snap.winner_json(&fdi, &DocOpts::default())?;
            if c.selector.try_matches(&doc)? && !seen.contains(&h.id) {
                if skip == 0 && limit == 0 {
                    // Stopped at the bookmark from before this match.
                    after = before;
                    break 'pages;
                }
                seen.insert(h.id.clone());
                stats.docs += 1;
                crate::stat!("mango.docs_examined").inc();
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                docs.push(extract(&doc, c.opts.fields.as_deref())?);
                stats.results += 1;
                crate::stat!("mango.results_returned").inc();
                limit -= 1;
                if limit == 0 {
                    break 'pages;
                }
            } else {
                stats.docs += 1;
                crate::stat!("mango.docs_examined").inc();
            }
        }
    }
    let packed = if query.is_empty() { Value::Null } else { json!(crate::nouveau::pack_bookmark(&after)) };
    let mut kvs = vec![("bookmark", packed)];
    finish(req, snap, c, stats, t0, &mut kvs);
    Ok(find_body(&docs, &kvs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bookmarks_as_couchdb_makes_them() {
        assert_eq!(pack_bookmark(&json!([3]), "d3"), "g2wAAAACaAJkAA5zdGFydGtleV9kb2NpZG0AAAACZDNoAmQACHN0YXJ0a2V5awABA2o");
        assert_eq!(pack_bookmark(&json!("d1"), "d1"), "g1AAAAA0eJzLYWBgYMpgSmHgKy5JLCrJTq2MT8lPzkzJBYmnGIJkOGAyULEsAGkGDeM");
        let (k, id) = unpack_bookmark("g1AAAAA0eJzLYWBgYMpgSmHgKy5JLCrJTq2MT8lPzkzJBYmnGIJkOGAyULEsAGkGDeM").unwrap();
        assert_eq!((k, id), (json!("d1"), json!("d1")));
        let (k, _) = unpack_bookmark(&pack_bookmark(&json!([1.5, {"a": [null, true]}, "x"]), "i")).unwrap();
        assert_eq!(k, json!([1.5, {"a": [null, true]}, "x"]));
        assert_eq!(unpack_bookmark("xyz").unwrap_err().reason, "Invalid bookmark value: \"xyz\"");
    }
}
