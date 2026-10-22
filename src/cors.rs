//! CORS, as chttpd_cors does it: off unless [chttpd] enable_cors, and
//! then only for the [cors] origins (or [cors:Host], by Host header).
//! Preflight requests are answered before authentication; other
//! responses, errors included, get the allow and expose headers.

use crate::server::Server;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;

const SUPPORTED_HEADERS: &[&str] = &[
    "accept",
    "accept-language",
    "authorization",
    "content-length",
    "content-range",
    "content-type",
    "destination",
    "expires",
    "if-match",
    "last-modified",
    "origin",
    "pragma",
    "x-couch-full-commit",
    "x-couch-id",
    "x-couch-persist",
    "x-couchdb-www-authenticate",
    "x-http-method-override",
    "x-requested-with",
    "x-couchdb-vhost-path",
];
const SUPPORTED_METHODS: &[&str] = &["CONNECT", "COPY", "DELETE", "GET", "HEAD", "OPTIONS", "POST", "PUT", "TRACE"];
const SIMPLE_HEADERS: &[&str] = &["cache-control", "content-language", "content-type", "expires", "last-modified", "pragma"];
const COUCH_HEADERS: &[&str] = &["accept-ranges", "etag", "server", "x-couch-request-id", "x-couch-update-newrev", "x-couchdb-body-time"];

struct Cors {
    credentials: bool,
    methods: Vec<String>,
    headers: Vec<String>,
    exposed: Vec<String>,
    max_age: String,
    origins: Vec<String>,
}

/// re:split(S, "\\s*,\\s*", [trim]).
fn split_list(s: &str) -> Vec<String> {
    let mut v: Vec<String> = s.split(',').map(|p| p.trim().to_string()).collect();
    while v.last().is_some_and(String::is_empty) {
        v.pop();
    }
    v
}

/// The configuration for a request (get_cors_config/1); None when off.
fn config(server: &Server, headers: &HeaderMap) -> Option<Cors> {
    if !server.config.get_bool("chttpd", "enable_cors", false) {
        return None;
    }
    let host = headers.get("host").and_then(|h| h.to_str().ok()).unwrap_or("");
    let host = host.split_once("://").map(|(_, h)| h).unwrap_or(host);
    let section = format!("cors:{}", host.split(':').next().unwrap_or(""));
    let get = |k: &str| server.config.get(&section, k).or_else(|| server.config.get("cors", k));
    let lower = |s: String| split_list(&s).into_iter().map(|h| h.to_lowercase()).collect::<Vec<_>>();
    let words = |l: &[&str]| l.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    Some(Cors {
        credentials: get("credentials").as_deref() == Some("true"),
        methods: get("methods").map(|m| split_list(&m)).unwrap_or_else(|| words(SUPPORTED_METHODS)),
        headers: get("headers").map(lower).unwrap_or_else(|| words(SUPPORTED_HEADERS)),
        exposed: get("exposed_headers").map(lower).unwrap_or_else(|| words(COUCH_HEADERS)),
        max_age: get("max_age").unwrap_or_else(|| "600".into()),
        origins: get("origins").map(|o| split_list(&o)).unwrap_or_default(),
    })
}

impl Cors {
    fn accepts(&self, origin: &str) -> bool {
        self.origins.iter().any(|o| o == "*" || o == origin)
    }

    fn credentials_for(&self, origin: &str) -> bool {
        origin != "*" && self.credentials
    }
}

fn set(h: &mut HeaderMap, k: &'static str, v: &str) {
    if let Ok(v) = HeaderValue::from_str(v) {
        h.insert(HeaderName::from_static(k), v);
    }
}

/// The answer to a preflight request, if this is one CORS accepts
/// (maybe_handle_preflight_request/1).
pub fn preflight(server: &Server, method: &Method, headers: &HeaderMap) -> Option<Response> {
    if *method != Method::OPTIONS {
        return None;
    }
    let cors = config(server, headers)?;
    let origin = headers.get("origin")?.to_str().ok()?;
    if !cors.accepts(origin) {
        return None;
    }
    let wanted = headers.get("access-control-request-method")?.to_str().ok()?;
    if !cors.methods.iter().any(|m| m == wanted) {
        return None;
    }
    let req_headers = headers.get("access-control-request-headers").and_then(|h| h.to_str().ok()).unwrap_or("");
    let asked: Vec<String> = if req_headers.is_empty() {
        Vec::new()
    } else {
        req_headers.split(',').map(|h| h.trim_start().to_lowercase()).filter(|h| !h.is_empty()).collect()
    };
    if asked.iter().any(|h| !cors.headers.contains(h)) {
        return None;
    }
    let mut resp = Response::builder().status(StatusCode::NO_CONTENT).header("content-length", "0").body(Body::empty()).unwrap();
    let h = resp.headers_mut();
    set(h, "access-control-allow-origin", origin);
    set(h, "access-control-max-age", &cors.max_age);
    set(h, "access-control-allow-methods", &cors.methods.join(", "));
    if cors.credentials_for(origin) {
        set(h, "access-control-allow-credentials", "true");
    }
    set(h, "access-control-allow-headers", req_headers);
    Some(resp)
}

/// A response's CORS headers (headers/2): the origin allowed, and the
/// response's simple headers and CouchDB's own exposed.
pub fn apply(server: &Server, req_headers: &HeaderMap, resp: &mut Response) {
    let Some(origin) = req_headers.get("origin").and_then(|o| o.to_str().ok()) else { return };
    let Some(cors) = config(server, req_headers) else { return };
    if !cors.accepts(origin) {
        return;
    }
    // The simple headers this response has, Content-Type first.
    let mut exposed: Vec<String> = vec!["content-type".into()];
    for name in resp.headers().keys() {
        let n = name.as_str().to_lowercase();
        if SIMPLE_HEADERS.contains(&n.as_str()) && !exposed.contains(&n) {
            exposed.push(n);
        }
    }
    exposed.extend(cors.exposed.iter().cloned());
    let h = resp.headers_mut();
    set(h, "access-control-allow-origin", origin);
    if cors.credentials_for(origin) {
        set(h, "access-control-allow-credentials", "true");
    }
    set(h, "access-control-expose-headers", &exposed.join(", "));
}
