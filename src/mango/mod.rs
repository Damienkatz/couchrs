//! Mango (/_find, /_explain, /_index), ported from CouchDB 3.5's mango
//! application: selectors (mango_selector), indexes (mango_idx with its
//! json, special and nouveau kinds), cursors (mango_cursor and its view,
//! special and nouveau kinds), options (mango_opts), and the messages of
//! mango_error, Erlang term formatting included. JSON indexes are stored
//! as CouchDB stores them, as design docs with `"language": "query"`, and
//! built by the view engine.

mod cursor;
mod idx;
mod opts;
mod selector;

pub use selector::{Selector, get_field, normalize_selector};

use crate::api::{blocking, require_json_ctype};
use crate::db::Db;
use crate::error::{CouchError, Result};
use crate::http::{Req, not_allowed, ok_json};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use axum::response::Response;
use serde_json::{Map, Number, Value, json};
use std::sync::Arc;

// ---------------------------------------------------------------- errors

/// A mango_error: its status, error and reason.
fn error(status: u16, error: &str, reason: impl Into<String>) -> CouchError {
    let e = CouchError::new(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), error, reason);
    // A 500 is sent with its stack's ref.
    if status == 500 { e.with_ref() } else { e }
}

/// What mango_error:info/2 makes of a reason it doesn't know.
fn unknown(module: &str, reason: &str) -> CouchError {
    error(500, "unknown_error", format!("Unknown Error: {module} :: {reason}"))
}

// ------------------------------------------------------ Erlang formatting

/// A decoded JSON value as io_lib:format's ~w prints it: binaries as
/// their bytes, objects as {[{K,V}]}.
pub fn erl_w(v: &Value) -> String {
    let mut out = String::new();
    write_term(v, false, &mut out);
    out
}

/// ... and as ~p prints it: printable binaries and lists as strings.
pub fn erl_p(v: &Value) -> String {
    let mut out = String::new();
    write_term(v, true, &mut out);
    out
}

fn printable_latin1(c: u64) -> bool {
    (32..=126).contains(&c) || (160..=255).contains(&c) || [8, 9, 10, 11, 12, 13, 27].contains(&c)
}

/// A string as io_lib quotes it, of Latin-1 characters: the bytes given.
fn write_quoted(chars: impl Iterator<Item = u8>, out: &mut String) {
    let mut q = vec![b'"'];
    for c in chars {
        match c {
            b'"' => q.extend_from_slice(b"\\\""),
            b'\\' => q.extend_from_slice(b"\\\\"),
            b'\n' => q.extend_from_slice(b"\\n"),
            b'\r' => q.extend_from_slice(b"\\r"),
            b'\t' => q.extend_from_slice(b"\\t"),
            11 => q.extend_from_slice(b"\\v"),
            8 => q.extend_from_slice(b"\\b"),
            12 => q.extend_from_slice(b"\\f"),
            27 => q.extend_from_slice(b"\\e"),
            c => q.push(c),
        }
    }
    q.push(b'"');
    out.push_str(&String::from_utf8_lossy(&q));
}

fn write_bin(b: &[u8], pretty: bool, out: &mut String) {
    out.push_str("<<");
    if pretty && !b.is_empty() && b.iter().all(|c| printable_latin1(*c as u64)) {
        write_quoted(b.iter().copied(), out);
    } else {
        out.push_str(&b.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(","));
    }
    out.push_str(">>");
}

fn write_term(v: &Value, pretty: bool, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&erl_number(n)),
        Value::String(s) => write_bin(s.as_bytes(), pretty, out),
        Value::Array(a) if pretty && !a.is_empty() && a.iter().all(|x| x.as_u64().is_some_and(printable_latin1)) => {
            write_quoted(a.iter().map(|x| x.as_u64().unwrap_or(0) as u8), out);
        }
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_term(x, pretty, out);
            }
            out.push(']');
        }
        Value::Object(o) => {
            out.push_str("{[");
            for (i, (k, x)) in o.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push('{');
                write_bin(k.as_bytes(), pretty, out);
                out.push(',');
                write_term(x, pretty, out);
                out.push('}');
            }
            out.push_str("]}");
        }
    }
}

fn erl_number(n: &Number) -> String {
    match n.as_f64() {
        Some(f) if !n.is_i64() && !n.is_u64() => erl_short_float(f),
        _ => n.to_string(),
    }
}

fn erl_short_float(f: f64) -> String {
    crate::json::short_float(f)
}

/// list_to_integer/1 of a query string value.
fn erl_list_to_integer(s: &str) -> Option<i128> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: i128 = digits.parse().ok()?;
    Some(if s.starts_with('-') { -n } else { n })
}

// ------------------------------------------------------------------ http

/// /db/_find, /db/_explain and /db/_index/..., as mango_httpd handles
/// them; `partition` is set for /db/_partition/P/_find and _explain.
pub async fn handle(req: &Req, db: &Arc<Db>, path: &[&str], partition: Option<&str>) -> Result<Response> {
    match path {
        ["_index", rest @ ..] => index_req(req, db, rest).await,
        ["_explain", ..] => explain_req(req, db, partition).await,
        ["_find", ..] => find_req(req, db, partition).await,
        _ => Err(CouchError::missing()),
    }
}

async fn index_req(req: &Req, db: &Arc<Db>, rest: &[&str]) -> Result<Response> {
    match rest {
        [] => match req.method {
            Method::GET => list_req(req, db),
            Method::POST => create_req(req, db).await,
            _ => not_allowed("GET,POST"),
        },
        ["_bulk_delete"] => match req.method {
            Method::POST => bulk_delete_req(req, db).await,
            _ => not_allowed("POST"),
        },
        ["_design", ddoc, kind, name] => delete_req(req, db, &format!("_design/{ddoc}"), kind, name).await,
        [ddoc, kind, name] => delete_req(req, db, ddoc, kind, name).await,
        _ => Err(CouchError::missing()),
    }
}

fn nouveau_enabled(req: &Req) -> bool {
    crate::nouveau::enabled(&req.server)
}

fn list_req(req: &Req, db: &Arc<Db>) -> Result<Response> {
    let bad = || error(500, "invalid_list_index_params", "Index parameter ranges: limit > 1, skip > 0");
    let (mut limit, mut skip) = (None, None);
    for (k, v) in &req.query {
        let slot = match k.as_str() {
            "limit" => &mut limit,
            "skip" => &mut skip,
            _ => continue,
        };
        let n = erl_list_to_integer(v).ok_or_else(bad)?;
        slot.get_or_insert(n);
    }
    let snap = db.snapshot();
    let mut idxs = blocking(|| idx::list(&snap, nouveau_enabled(req)))?;
    idxs.sort_by(idx::Idx::term_cmp);
    let total = idxs.len() as i128;
    let limit = limit.unwrap_or(total);
    let skip = skip.unwrap_or(0);
    if limit < 1 || skip < 0 {
        return Err(bad());
    }
    let list: Vec<Value> = idxs.iter().skip(skip.min(total) as usize).take(limit as usize).map(idx::Idx::to_json).collect();
    ok_json(json!({"total_rows": total, "indexes": list}))
}

/// mango_crud:insert/3 of a design document; its error as mango_httpd
/// reports it.
async fn save_ddoc(req: &Req, db: &Arc<Db>, body: Value) -> Result<()> {
    let doc = crate::doc::from_json(body)?;
    match crate::api::save_docs(req, db, vec![doc], true).await?.pop() {
        Some(Ok(_)) => Ok(()),
        Some(Err(e)) if e.error == "conflict" => Err(error(500, "error_saving_ddoc", "Encountered a conflict while saving the design document.")),
        Some(Err(e)) => Err(error(500, "error_saving_ddoc", format!("Unknown error while saving the design document: {}", e.error))),
        None => Err(error(500, "error_saving_ddoc", "Unknown error while saving the design document.")),
    }
}

async fn create_req(req: &Req, db: &Arc<Db>) -> Result<Response> {
    require_json_ctype(req)?;
    let body = req.json_obj_body()?;
    let opts = opts::validate_idx_create(&body)?;
    let snap = db.snapshot();
    let (ddoc, name, changed) = blocking(|| idx::add(&snap, nouveau_enabled(req), &opts))?;
    let result = match changed {
        None => "exists",
        Some(body) => {
            save_ddoc(req, db, body).await?;
            "created"
        }
    };
    ok_json(json!({"result": result, "id": ddoc, "name": name}))
}

fn design_id(ddoc: &str) -> String {
    if ddoc.starts_with("_design/") { ddoc.to_string() } else { format!("_design/{ddoc}") }
}

/// mango_idx:delete/4: the one index the filter picks out removed from
/// its design document (deleted with it, if nothing else is left).
async fn delete(req: &Req, db: &Arc<Db>, filt: impl Fn(&idx::Idx) -> bool) -> Result<std::result::Result<(), &'static str>> {
    let snap = db.snapshot();
    let found: Vec<idx::Idx> = blocking(|| idx::list(&snap, nouveau_enabled(req)))?.into_iter().filter(|i| filt(i)).collect();
    match found.as_slice() {
        [] => Ok(Err("not_found")),
        [i] => {
            let body = blocking(|| idx::remove(&snap, i))?;
            save_ddoc(req, db, body).await?;
            Ok(Ok(()))
        }
        _ => Err(error(500, "case_clause", "more than one index in the design document")),
    }
}

async fn delete_req(req: &Req, db: &Arc<Db>, ddoc: &str, kind: &str, name: &str) -> Result<Response> {
    if req.method != Method::DELETE {
        return not_allowed("DELETE");
    }
    let id = design_id(ddoc);
    match delete(req, db, |i| i.ddoc.as_deref() == Some(id.as_str()) && i.type_str() == kind && i.name == name).await? {
        Ok(()) => ok_json(json!({"ok": true})),
        Err(_) => Err(CouchError::missing()),
    }
}

async fn bulk_delete_req(req: &Req, db: &Arc<Db>) -> Result<Response> {
    require_json_ctype(req)?;
    let body = req.json_obj_body()?;
    let docids = opts::validate_bulk_delete(&body)?;
    let (mut success, mut fail) = (Vec::new(), Vec::new());
    for d in docids {
        let Some(d) = d.as_str() else { return Err(error(500, "unknown_error", "badarg")) };
        let id = design_id(d);
        match delete(req, db, |i| i.ddoc.as_deref() == Some(id.as_str())).await? {
            Ok(()) => success.insert(0, json!({"id": id, "ok": true})),
            Err(e) => fail.insert(0, json!({"id": id, "error": e})),
        }
    }
    ok_json(json!({"success": success, "fail": fail}))
}

/// mango_httpd:maybe_set_partition/1: the body, with the partition of
/// the path or query string.
fn find_body(req: &Req, partition: Option<&str>) -> Result<Map<String, Value>> {
    let mut body = req.json_obj_body()?;
    let partition = partition.or_else(|| req.query.iter().find(|(k, _)| k == "partition").map(|(_, v)| v.as_str()));
    if let Some(p) = partition {
        match body.get("partition") {
            None => {
                body.insert("partition".into(), json!(p));
            }
            Some(Value::String(s)) if s == p => {}
            Some(other) => return Err(unknown("mango_httpd", &format!("{{bad_partition,{}}}", erl_w(other)))),
        }
    }
    Ok(body)
}

async fn explain_req(req: &Req, db: &Arc<Db>, partition: Option<&str>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    require_json_ctype(req)?;
    let opts = opts::validate_find(&find_body(req, partition)?, &req.server)?;
    let out = blocking(|| cursor::explain(req, db, opts))?;
    ok_json(out)
}

async fn find_req(req: &Req, db: &Arc<Db>, partition: Option<&str>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    require_json_ctype(req)?;
    let opts = opts::validate_find(&find_body(req, partition)?, &req.server)?;
    let body = blocking(|| cursor::find(req, db, opts))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "must-revalidate")
        .body(Body::from(body))
        .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn erlang_formatting() {
        let cases = [(2.5, "2.5"), (1.0, "1.0"), (1e10, "1.0e10"), (1e21, "1.0e21"), (123456789012345.0, "123456789012345.0"),
            (1.5e-7, "1.5e-7"), (0.0001, "0.0001"), (0.001, "0.001"), (1e-5, "1.0e-5"), (100.0, "100.0"), (1e15, "1.0e15"),
            (-0.0, "-0.0"), (12345678901234567890.0, "1.2345678901234567e19"), (5e-324, "5.0e-324")];
        for (f, s) in cases {
            assert_eq!(erl_short_float(f), s);
        }
        assert_eq!(erl_w(&json!({"a": [1, "s", null, 2.5]})), "{[{<<97>>,[1,<<115>>,null,2.5]}]}");
        assert_eq!(erl_p(&json!({"a": [1, "s", null]})), "{[{<<\"a\">>,[1,<<\"s\">>,null]}]}");
        assert_eq!(erl_p(&json!("é")), "<<\"é\">>");
        assert_eq!(erl_p(&json!("")), "<<>>");
        assert_eq!(erl_p(&json!([104, 105])), "\"hi\"");
        assert_eq!(erl_w(&json!("a/b")), "<<97,47,98>>");
    }
}
