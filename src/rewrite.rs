//! _rewrite, a port of chttpd_rewrite. A design document's `rewrites`
//! either lists rules — {from, to, method, query, formats}, `:name` and
//! `*` binding path segments — or is a JavaScript function of the request
//! returning the new path (or {path, query, method, headers, body}, or
//! {code, headers, body} to answer directly). Paths are relative to the
//! design document; the rewritten request is dispatched like a new one.

use crate::error::{CouchError, Result};
use crate::http::Req;
use axum::body::Body;
use axum::http::{HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use serde_json::{Value, json};

/// Keys whose values travel JSON-encoded in the query string.
const JSON_KEYS: &[&str] = &["key", "startkey", "start_key", "endkey", "end_key", "keys"];

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Lit(String),
    Bind(String),
    All,
}

fn rewrite_error(status: StatusCode, reason: impl Into<String>) -> CouchError {
    CouchError::new(status, "rewrite_error", reason)
}

/// Split a rule path; more than two `..` segments are refused unless
/// [chttpd] secure_rewrites is false.
fn parse_path(path: &str, secure: bool) -> Result<Vec<Tok>> {
    let mut out = Vec::new();
    let mut dotdots = 0;
    for seg in path.split('/').filter(|s| !s.is_empty()) {
        out.push(match seg {
            "*" => Tok::All,
            ".." => {
                if dotdots == 2 && secure {
                    return Err(CouchError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "insecure_rewrite_rule",
                        "too many ../.. segments",
                    ));
                }
                dotdots += 1;
                Tok::Lit("..".into())
            }
            s => match s.strip_prefix(':') {
                Some(var) => Tok::Bind(var.to_string()),
                None => Tok::Lit(s.to_string()),
            },
        });
    }
    Ok(out)
}

struct Rule {
    from: Vec<Tok>,
    method: Option<String>,
    to: Vec<Tok>,
    query: Vec<(String, Value)>,
    formats: serde_json::Map<String, Value>,
}

fn make_rule(rule: &serde_json::Map<String, Value>, secure: bool) -> Result<Rule> {
    let to = rule
        .get("to")
        .and_then(|t| t.as_str())
        .ok_or_else(|| CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, "error", "invalid_rewrite_target"))?;
    Ok(Rule {
        from: match rule.get("from").and_then(|f| f.as_str()) {
            Some(f) => parse_path(f, secure)?,
            None => vec![Tok::All],
        },
        method: rule.get("method").and_then(|m| m.as_str()).map(String::from),
        to: parse_path(to, secure)?,
        query: rule
            .get("query")
            .and_then(|q| q.as_object())
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default(),
        formats: rule.get("formats").and_then(|f| f.as_object()).cloned().unwrap_or_default(),
    })
}

type Bindings = Vec<(String, Value)>;

fn lookup<'a>(b: &'a Bindings, k: &str) -> Option<&'a Value> {
    b.iter().find(|(key, _)| key == k).map(|(_, v)| v)
}

/// Match a rule's `from` against the path; Some((rest, bindings)).
fn bind_path(from: &[Tok], parts: &[String]) -> Option<(Vec<String>, Bindings)> {
    let mut bindings: Bindings = Vec::new();
    let mut i = 0;
    for (n, tok) in from.iter().enumerate() {
        if *tok == Tok::All && n == from.len() - 1 {
            return Some((parts[i..].to_vec(), bindings));
        }
        let part = parts.get(i)?;
        match tok {
            Tok::Bind(var) => bindings.insert(0, (var.clone(), json!(part))),
            Tok::Lit(l) if l == part => {}
            _ => return None,
        }
        i += 1;
    }
    (i == parts.len()).then(|| (Vec::new(), bindings))
}

fn format_var(var: &str, v: Value, formats: &serde_json::Map<String, Value>) -> Value {
    let as_str = v.as_str().map(String::from);
    match (formats.get(var).and_then(|f| f.as_str()), as_str) {
        (Some("int"), Some(s)) => s.parse::<i64>().map(|n| json!(n)).unwrap_or(v),
        (Some("bool"), Some(s)) => match s.to_lowercase().as_str() {
            "true" => json!(true),
            "false" => json!(false),
            _ => v,
        },
        _ => v,
    }
}

fn get_var(var: &str, b: &Bindings, default: &Value, formats: &serde_json::Map<String, Value>) -> Value {
    format_var(var, lookup(b, var).cloned().unwrap_or_else(|| default.clone()), formats)
}

fn replace_var(v: &Value, b: &Bindings, formats: &serde_json::Map<String, Value>) -> Value {
    match v {
        Value::String(s) if s == "*" => get_var("*", b, v, formats),
        Value::String(s) if s.starts_with(':') => get_var(&s[1..], b, v, formats),
        Value::Array(a) => Value::Array(
            a.iter()
                .map(|x| match x.as_str().and_then(|s| s.strip_prefix(':')) {
                    Some(var) => get_var(var, b, x, formats),
                    None => x.clone(),
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

fn value_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The request's query, with key-like values JSON-decoded.
fn query_bindings(req: &Req) -> Result<Bindings> {
    req.query
        .iter()
        .map(|(k, v)| {
            let val = if JSON_KEYS.contains(&k.as_str()) {
                serde_json::from_str(v).map_err(|_| CouchError::bad_request(format!("Invalid JSON for {k}")))?
            } else {
                json!(v)
            };
            Ok((k.clone(), val))
        })
        .collect()
}

/// Resolve `.` and `..` (normalize_path).
fn normalize(parts: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for p in parts {
        match p.as_str() {
            "." | "" => {}
            ".." => match out.last().map(|s| s.as_str()) {
                None | Some("..") => out.push(p),
                Some(_) => {
                    out.pop();
                }
            },
            _ => out.push(p),
        }
    }
    out
}

/// The design document's path: [db, _design, name].
fn prefix(req: &Req) -> Vec<String> {
    req.path[..3].to_vec()
}

fn rewrite_rules(req: &Req, rules: &[Value]) -> Result<Req> {
    let secure = req.server.config.get_bool("chttpd", "secure_rewrites", true);
    let rules: Vec<Rule> = rules
        .iter()
        .filter_map(|r| r.as_object())
        .map(|r| make_rule(r, secure))
        .collect::<Result<_>>()?;
    let parts = &req.path[4..];
    let query = query_bindings(req)?;
    for rule in &rules {
        if rule.method.as_deref().is_some_and(|m| m != "*" && m != req.method.as_str()) {
            continue;
        }
        let Some((rest, path_bindings)) = bind_path(&rule.from, parts) else { continue };
        let bindings1: Bindings = path_bindings.into_iter().chain(query.iter().cloned()).collect();
        let mut query_args: Bindings = Vec::new();
        for (k, v) in &rule.query {
            query_args.insert(0, (k.clone(), replace_var(v, &bindings1, &rule.formats)));
        }
        let mut final_bindings: Bindings =
            bindings1.into_iter().filter(|(k, _)| lookup(&query_args, k).is_none()).collect();
        final_bindings.extend(query_args);
        let mut new_parts = prefix(req);
        for tok in &rule.to {
            match tok {
                Tok::All => {
                    new_parts.extend(rest.iter().cloned());
                    break;
                }
                Tok::Bind(var) => {
                    new_parts.push(lookup(&final_bindings, var).map(value_text).unwrap_or_else(|| "undefined".into()))
                }
                Tok::Lit(l) => new_parts.push(l.clone()),
            }
        }
        // The bindings become the query string, latest first, key-like
        // values JSON-encoded (maybe_encode_bindings).
        let new_query: Vec<(String, String)> = final_bindings
            .iter()
            .rev()
            .filter(|(k, _)| k != "*")
            .map(|(k, v)| {
                let text = if JSON_KEYS.contains(&k.as_str()) { v.to_string() } else { value_text(v) };
                (k.clone(), text)
            })
            .collect();
        let mut r = req.clone();
        r.path = normalize(new_parts);
        r.query = new_query;
        return Ok(r);
    }
    Err(CouchError::not_found("missing"))
}

fn check_strings(v: &Value, what: &str) -> Result<()> {
    let Some(o) = v.as_object() else {
        return Err(rewrite_error(StatusCode::INTERNAL_SERVER_ERROR, format!("bad {what}")));
    };
    for (k, val) in o {
        if !val.is_string() {
            return Err(rewrite_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("object key/value must be strings {k}={val}"),
            ));
        }
    }
    Ok(())
}

/// What a rewrite function produced: a new request, or a response.
enum Outcome {
    Request(Req),
    Respond(Response),
}

fn rewrite_function(req: &Req, result: Value) -> Result<Outcome> {
    let result = match result {
        Value::Null | Value::Bool(false) => return Err(rewrite_error(StatusCode::NOT_FOUND, "Invalid path.")),
        Value::String(s) if s.is_empty() => return Err(rewrite_error(StatusCode::NOT_FOUND, "Invalid path.")),
        Value::String(s) => json!({"path": s, "method": req.method.as_str()}),
        Value::Object(o) => Value::Object(o),
        _ => return Err(rewrite_error(StatusCode::INTERNAL_SERVER_ERROR, "bad rewrite")),
    };
    let bad = |what: &str| Err(rewrite_error(StatusCode::INTERNAL_SERVER_ERROR, format!("bad {what}")));
    for (k, v) in result.as_object().unwrap() {
        match k.as_str() {
            "method" if !v.is_string() => return bad("method"),
            "path" if !v.is_string() => return bad("path"),
            "body" if !v.is_string() => return bad("body"),
            "headers" => check_strings(v, "headers")?,
            "query" => check_strings(v, "query")?,
            "code" if !v.as_u64().is_some_and(|c| (200..600).contains(&c)) => return bad("code"),
            _ => {}
        }
    }
    let headers: Vec<(String, String)> = result
        .get("headers")
        .and_then(|h| h.as_object())
        .map(|o| o.iter().map(|(k, v)| (k.clone(), value_text(v))).collect())
        .unwrap_or_default();
    if let Some(code) = result.get("code").and_then(|c| c.as_u64()) {
        let mut b = Response::builder().status(StatusCode::from_u16(code as u16).unwrap_or(StatusCode::OK));
        for (k, v) in &headers {
            b = b.header(k.as_str(), v.as_str());
        }
        let body = result.get("body").and_then(|b| b.as_str()).unwrap_or("").to_string();
        return Ok(Outcome::Respond(b.body(Body::from(body)).unwrap()));
    }
    let Some(path) = result.get("path").and_then(|p| p.as_str()) else {
        return Err(rewrite_error(StatusCode::INTERNAL_SERVER_ERROR, "Rewrite result must produce a new path."));
    };
    let mut r = req.clone();
    if let Some(m) = result.get("method").and_then(|m| m.as_str()) {
        r.method = Method::from_bytes(m.as_bytes()).map_err(|_| rewrite_error(StatusCode::INTERNAL_SERVER_ERROR, "bad method"))?;
    }
    for (k, v) in &headers {
        if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v)) {
            r.headers.insert(name, value);
        }
    }
    if let Some(b) = result.get("body").and_then(|b| b.as_str()) {
        r.body = bytes::Bytes::from(b.to_string());
    }
    // The path may carry its own query string.
    let (path, inline_query) = path.split_once('?').unwrap_or((path, ""));
    let mut parts = prefix(req);
    parts.extend(path.split('/').map(|s| percent_encoding::percent_decode_str(s).decode_utf8_lossy().to_string()));
    r.path = normalize(parts);
    if let Some(q) = result.get("query").and_then(|q| q.as_object()) {
        r.query = q.iter().map(|(k, v)| (k.clone(), value_text(v))).collect();
    } else if !inline_query.is_empty() {
        r.query = form_urlencoded::parse(inline_query.as_bytes()).into_owned().collect();
    }
    Ok(Outcome::Request(r))
}

/// Handle /db/_design/name/_rewrite/...: rewrite, then dispatch the new
/// request (at most [chttpd] rewrite_limit times).
pub async fn handle(req: &Req, ddoc: &Value) -> Result<Response> {
    let limit: u32 = req.server.config.get_int("chttpd", "rewrite_limit", 100);
    if req.rewrites >= limit {
        return Err(CouchError::bad_request("Exceeded rewrite recursion limit"));
    }
    let outcome = match ddoc.get("rewrites") {
        Some(Value::Array(rules)) => Outcome::Request(rewrite_rules(req, rules)?),
        Some(Value::String(src)) => {
            let result = crate::js::run_rewrite(req, ddoc, src)?;
            rewrite_function(req, result)?
        }
        _ => return Err(rewrite_error(StatusCode::NOT_FOUND, "Invalid path.")),
    };
    match outcome {
        Outcome::Respond(r) => Ok(r),
        Outcome::Request(mut r) => {
            r.rewrites = req.rewrites + 1;
            r.trailing_slash = false;
            crate::http::dispatch(r).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_paths() {
        let from = parse_path("doc/:id/*", true).unwrap();
        let parts: Vec<String> = ["doc", "a", "x", "y"].iter().map(|s| s.to_string()).collect();
        let (rest, b) = bind_path(&from, &parts).unwrap();
        assert_eq!(rest, vec!["x".to_string(), "y".to_string()]);
        assert_eq!(b, vec![("id".to_string(), json!("a"))]);
        assert!(bind_path(&parse_path("doc/:id", true).unwrap(), &parts).is_none());
        assert!(bind_path(&parse_path("", true).unwrap(), &[]).is_some());
        assert!(parse_path("../../../x", true).is_err());
        assert!(parse_path("../../../x", false).is_ok());
        let n = |v: &[&str]| normalize(v.iter().map(|s| s.to_string()).collect());
        assert_eq!(n(&["db", "_design", "d", "..", "..", "a"]), vec!["db".to_string(), "a".to_string()]);
    }
}
