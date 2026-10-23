//! HTTP front end, the equivalent of chttpd. Paths are parsed by hand (like
//! chttpd does) so `_design/foo`, `_local/foo`, `%2F`-encoded database names
//! and attachment names containing slashes all route correctly.

use crate::error::{CouchError, Result};
use crate::server::Server;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde_json::{Value, json};
use std::sync::Arc;

pub const VERSION: &str = "3.5.3";
const SERVER_HEADER: &str = concat!("CouchDB/3.5.3 (CouchRS ", env!("CARGO_PKG_VERSION"), ")");

#[derive(Clone)]
pub struct Req {
    pub method: Method,
    pub path: Vec<String>,
    pub query: Vec<(String, String)>,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// The body, when the front end left it for the handler to read as it
    /// arrives (`body` is then empty): see `take_body`.
    pub unread: Unread,
    pub server: Arc<Server>,
    /// Whether the raw path ended in '/' (lost when splitting `path`).
    pub trailing_slash: bool,
    /// Who is asking, as authenticated.
    pub auth: crate::auth::Auth,
    /// How many times this request has been rewritten (_rewrite).
    pub rewrites: u32,
    /// Whether it came over HTTPS.
    pub tls: bool,
}

/// Marks the requests of the HTTPS listener.
#[derive(Clone, Copy)]
pub struct Tls;

/// A request body not yet read (crate::api::streams_body), taken once.
#[derive(Clone, Default)]
pub struct Unread(Arc<parking_lot::Mutex<Option<Body>>>);

/// couch_util:url_encode/1: all but letters, digits and _ . - : as %XX.
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"_.-:".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A multipart Content-Type's boundary parameter, unquoted.
pub fn multipart_boundary(ct: &str) -> Option<String> {
    ct.split(';').filter_map(|p| p.trim().strip_prefix("boundary=")).next().map(|b| b.trim_matches('"').to_string())
}

/// chttpd:absolute_uri/2: a path on this server as the client reached it,
/// through a proxy's X-Forwarded-Host and -Ssl or -Proto if there's one.
pub fn absolute_uri(req: &Req, path: &str) -> String {
    let config = &req.server.config;
    let xhost = config.get_or("chttpd", "x_forwarded_host", "X-Forwarded-Host").to_lowercase();
    let host = req.header(&xhost).or_else(|| req.header("host")).map(String::from).unwrap_or_else(|| {
        let port = if req.tls { config.get_or("ssl", "port", "6984") } else { config.get_or("chttpd", "port", "5984") };
        format!("{}:{port}", config.get_or("chttpd", "bind_address", "127.0.0.1"))
    });
    let xssl = config.get_or("chttpd", "x_forwarded_ssl", "X-Forwarded-Ssl").to_lowercase();
    let xproto = config.get_or("chttpd", "x_forwarded_proto", "X-Forwarded-Proto").to_lowercase();
    let https = req.header(&xssl) == Some("on") || req.header(&xproto) == Some("https") || req.tls;
    format!("{}://{host}{path}", if https { "https" } else { "http" })
}

impl Req {
    pub fn q(&self, k: &str) -> Option<&str> {
        self.query.iter().rev().find(|(a, _)| a == k).map(|(_, v)| v.as_str())
    }

    pub fn q_bool(&self, k: &str) -> Result<Option<bool>> {
        match self.q(k) {
            None => Ok(None),
            Some("true") | Some("") => Ok(Some(true)),
            Some("false") => Ok(Some(false)),
            Some(v) => Err(CouchError::new(
                StatusCode::BAD_REQUEST,
                "query_parse_error",
                format!("Invalid boolean parameter: \"{v}\""),
            )),
        }
    }

    pub fn flag(&self, k: &str) -> Result<bool> {
        Ok(self.q_bool(k)?.unwrap_or(false))
    }

    pub fn q_u64(&self, k: &str) -> Result<Option<u64>> {
        match self.q(k) {
            None => Ok(None),
            Some(v) => v.parse().map(Some).map_err(|_| {
                CouchError::new(
                    StatusCode::BAD_REQUEST,
                    "query_parse_error",
                    format!("Invalid value for positive integer: \"{v}\""),
                )
            }),
        }
    }

    pub fn q_json(&self, k: &str) -> Result<Option<Value>> {
        match self.q(k) {
            None => Ok(None),
            Some(v) => crate::json::parse(v.as_bytes())
                .map(Some)
                .ok_or_else(|| CouchError::bad_request(format!("Invalid JSON for query parameter {k}: {v}"))),
        }
    }

    pub fn json_body(&self) -> Result<Value> {
        if self.body.is_empty() {
            return Err(CouchError::bad_request("Request body must be a JSON object"));
        }
        let body = self.decoded_body()?;
        crate::json::parse(&body).ok_or_else(|| CouchError::new(StatusCode::BAD_REQUEST, "bad_request", "invalid UTF-8 JSON"))
    }

    /// The body, gunzipped if sent with Content-Encoding: gzip
    /// (chttpd:maybe_decompress/2).
    pub fn decoded_body(&self) -> Result<std::borrow::Cow<'_, [u8]>> {
        match self.header("content-encoding").unwrap_or("identity") {
            "identity" => Ok(std::borrow::Cow::Borrowed(&self.body)),
            "gzip" => crate::doc::gunzip(&self.body)
                .map(std::borrow::Cow::Owned)
                .map_err(|_| CouchError::bad_request("Request body is not properly gzipped.")),
            other => Err(CouchError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "bad_content_type",
                format!("{other} is not a supported content encoding."),
            )),
        }
    }

    /// The body as it arrives, if the front end left it unread, else the
    /// bytes it read.
    pub fn take_body(&self) -> axum::body::BodyDataStream {
        self.unread.0.lock().take().unwrap_or_else(|| Body::from(self.body.clone())).into_data_stream()
    }

    pub fn json_obj_body(&self) -> Result<serde_json::Map<String, Value>> {
        match self.json_body()? {
            Value::Object(o) => Ok(o),
            _ => Err(CouchError::bad_request("Request body must be a JSON object")),
        }
    }

    pub fn header(&self, k: &str) -> Option<&str> {
        self.headers.get(k).and_then(|v| v.to_str().ok())
    }

    pub fn content_type(&self) -> &str {
        self.header("content-type").unwrap_or("")
    }

    pub fn accepts(&self, ct: &str) -> bool {
        self.header("accept").map(|a| a.contains(ct)).unwrap_or(false)
    }

    /// mochiweb's accepts_content_type/1: whether the Accept header (none
    /// meaning */*) takes this type, exactly or by wildcard, at q > 0.
    pub fn accepts_content_type(&self, ct: &str) -> bool {
        let accept = self.header("accept").unwrap_or("*/*");
        let main = ct.split('/').next().unwrap_or("");
        let super_type = format!("{main}/*");
        for item in accept.split(',') {
            let mut parts = item.split(';').map(str::trim);
            let ty = parts.next().unwrap_or("").to_ascii_lowercase();
            let mut q = 1.0f64;
            for p in parts {
                if let Some(v) = p.strip_prefix("q=") {
                    match v.trim().parse::<f64>() {
                        Ok(x) => q = x,
                        Err(_) => return false,
                    }
                }
            }
            if q > 0.0 && (ty == "*/*" || ty == ct || ty == super_type) {
                return true;
            }
        }
        false
    }
}

/// mochiweb_util:parse_qs/1: pairs separated by & or ;.
pub fn parse_qs(q: &str) -> Vec<(String, String)> {
    q.split(['&', ';']).flat_map(|seg| form_urlencoded::parse(seg.as_bytes()).into_owned()).collect()
}

pub fn json_response(status: StatusCode, v: &Value) -> Response {
    let mut body = crate::json::to_vec(v);
    body.push(b'\n');
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "must-revalidate")
        .body(Body::from(body))
        .unwrap()
}

/// A response with rows, streamed as CouchDB streams them.
pub fn rows_response(v: &Value) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "must-revalidate")
        .body(Body::from(crate::json::rows_body(v)))
        .unwrap()
}

pub fn ok_json(v: Value) -> Result<Response> {
    Ok(json_response(StatusCode::OK, &v))
}

pub fn with_header(mut r: Response, k: header::HeaderName, v: &str) -> Response {
    if let Ok(v) = HeaderValue::from_str(v) {
        r.headers_mut().insert(k, v);
    }
    r
}

fn decode(seg: &str) -> String {
    percent_encoding::percent_decode_str(seg).decode_utf8_lossy().to_string()
}

pub async fn handle(State(server): State<Arc<Server>>, req: Request) -> Response {
    let t0 = std::time::Instant::now();
    count_method(req.method());
    let req_headers = req.headers().clone();
    let mut resp = match crate::cors::preflight(&server, req.method(), req.headers()) {
        Some(r) => r,
        None => {
            let mut r = handle_req(server.clone(), req).await;
            crate::cors::apply(&server, &req_headers, &mut r);
            r
        }
    };
    // What chttpd adds to every response.
    let h = resp.headers_mut();
    h.insert(header::SERVER, HeaderValue::from_static(SERVER_HEADER));
    if let Ok(id) = HeaderValue::from_str(&format!("{:010x}", rand::random::<u64>() & 0xff_ffff_ffff)) {
        h.insert(HeaderName::from_static("x-couch-request-id"), id);
    }
    h.insert(HeaderName::from_static("x-couchdb-body-time"), HeaderValue::from_static("0"));
    if let Some(m) = crate::stats::find(&format!("couchdb.httpd_status_codes.{}", resp.status().as_u16())) {
        m.inc();
    }
    crate::stat!("couchdb.request_time").observe(crate::stats::ms_since(t0));
    crate::stat!("couchdb.httpd.requests").inc();
    resp
}

fn count_method(m: &Method) {
    let stat = match *m {
        Method::GET => crate::stat!("couchdb.httpd_request_methods.GET"),
        Method::PUT => crate::stat!("couchdb.httpd_request_methods.PUT"),
        Method::POST => crate::stat!("couchdb.httpd_request_methods.POST"),
        Method::DELETE => crate::stat!("couchdb.httpd_request_methods.DELETE"),
        Method::HEAD => crate::stat!("couchdb.httpd_request_methods.HEAD"),
        Method::OPTIONS => crate::stat!("couchdb.httpd_request_methods.OPTIONS"),
        _ if m.as_str() == "COPY" => crate::stat!("couchdb.httpd_request_methods.COPY"),
        _ => return,
    };
    stat.inc();
}

async fn handle_req(server: Arc<Server>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let raw_path = parts.uri.path().to_string();
    let path: Vec<String> = raw_path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(decode)
        .collect();
    let query: Vec<(String, String)> = parts
        .uri
        .query()
        .map(parse_qs)
        .unwrap_or_default();
    let ct = parts.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let (body, unread) = if crate::api::streams_body(&parts.method, &path, ct) {
        (Bytes::new(), Unread(Arc::new(parking_lot::Mutex::new(Some(body)))))
    } else {
        // [chttpd] max_http_request_size bounds a body.
        let max = server.config.get_int("chttpd", "max_http_request_size", 4294967296usize);
        match axum::body::to_bytes(body, max).await {
            Ok(b) => (b, Unread::default()),
            Err(e) if std::error::Error::source(&e).is_some_and(|s| s.is::<http_body_util::LengthLimitError>()) => {
                return too_large().into_response();
            }
            Err(e) => return CouchError::bad_request(e.to_string()).into_response(),
        }
    };
    let method = parts.method.clone();
    let req = Req {
        method: parts.method,
        path,
        query,
        headers: parts.headers,
        body,
        unread,
        server,
        trailing_slash: raw_path.ends_with('/'),
        auth: crate::auth::Auth::default(),
        rewrites: 0,
        tls: parts.extensions.get::<Tls>().is_some(),
    };
    let is_head = method == Method::HEAD;
    let mut req = req;
    let renew = match crate::auth::authenticate(&req.server, &req.headers, &req.path) {
        Ok(a) => {
            let renew = a.renew.clone().zip(a.user.name.clone());
            req.auth = a;
            renew
        }
        Err(e) => return e.into_response(),
    };
    let server = req.server.clone();
    let mut resp = match crate::auth::authorize_request(&req.server, &req.auth.user, &req.method, &req.path) {
        Ok(()) => match route(req).await {
            Ok(r) => r,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    };
    // Renew a session cookie past 10% of its life, unless the response
    // (a login or logout) sets one itself.
    if let Some((salt, name)) = renew
        && !resp.headers().contains_key(header::SET_COOKIE)
        && let Some(v) = crate::auth::make_cookie(&server, &name, &salt, now_secs())
        && let Ok(h) = HeaderValue::from_str(&crate::auth::cookie_header(&server.config, &v, false))
    {
        resp.headers_mut().insert(header::SET_COOKIE, h);
    }
    if is_head {
        // The headers of a GET, its length included, and no body.
        let (mut parts, body) = resp.into_parts();
        match axum::body::HttpBody::size_hint(&body).exact() {
            Some(n) => {
                parts.headers.insert(header::CONTENT_LENGTH, HeaderValue::from(n));
            }
            None => {
                parts.headers.remove(header::CONTENT_LENGTH);
            }
        }
        return Response::from_parts(parts, Body::empty());
    }
    resp
}

/// A body over [chttpd] max_http_request_size.
pub fn too_large() -> CouchError {
    CouchError::new(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "the request entity is too large")
}

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn method_not_allowed(allowed: &str) -> CouchError {
    CouchError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        format!("Only {allowed} allowed"),
    )
}

pub fn not_allowed(allowed: &str) -> Result<Response> {
    Err(method_not_allowed(allowed))
}

/// Authorize and route a request made inside the server (a rewrite).
pub fn dispatch(req: Req) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response>> + Send>> {
    Box::pin(async move {
        crate::auth::authorize_request(&req.server, &req.auth.user, &req.method, &req.path)?;
        route(req).await
    })
}

async fn route(req: Req) -> Result<Response> {
    let first = req.path.first().cloned();
    match first.as_deref() {
        None => welcome(&req),
        Some("_all_dbs") if !matches!(req.method, Method::GET | Method::HEAD) => not_allowed("GET,HEAD"),
        Some("_all_dbs") => all_dbs(&req),
        Some("_dbs_info") if !matches!(req.method, Method::GET | Method::HEAD | Method::POST) => not_allowed("GET,HEAD,POST"),
        Some("_dbs_info") => dbs_info(&req),
        Some("_uuids") if req.method != Method::GET => not_allowed("GET"),
        Some("_uuids") => uuids(&req),
        Some("_up") if !matches!(req.method, Method::GET | Method::HEAD) => not_allowed("GET,HEAD"),
        Some("_up") => ok_json(json!({"status": "ok", "seeds": {}})),
        Some("_session") => session(&req),
        Some("_utils") => crate::fauxton::serve(&req),
        Some("_active_tasks") => match req.method {
            Method::GET | Method::HEAD => {
                if !req.auth.user.is_server_admin() {
                    return Err(CouchError::unauthorized("You are not a server admin."));
                }
                ok_json(json!(crate::tasks::all()))
            }
            _ => not_allowed("GET,HEAD"),
        },
        Some("_membership") if req.method != Method::GET => not_allowed("GET"),
        Some("_membership") => ok_json(json!({
            "all_nodes": ["couchdb@127.0.0.1"], "cluster_nodes": ["couchdb@127.0.0.1"]
        })),
        Some("_node") => node(&req),
        Some("_nouveau_analyze") => crate::nouveau::http_analyze(&req),
        Some("_search_analyze") => crate::nouveau::http_search_analyze(&req),
        Some("_replicate") => crate::replicator::http_replicate(&req).await,
        Some("_scheduler") => crate::replicator::http_scheduler(&req),
        Some("_db_updates") => crate::changes::db_updates(req).await,
        // Fauxton's setup page asks this; CouchRS is always one node.
        Some("_cluster_setup") => cluster_setup(&req).await,
        Some("favicon.ico") => Err(CouchError::not_found("missing")),
        // Creating one is a bad name; anything else finds no database.
        Some(s) if s.starts_with('_') && !matches!(s, "_users" | "_replicator" | "_global_changes") => {
            if req.method == Method::PUT && req.path.len() == 1 {
                Err(CouchError::new(
                    StatusCode::BAD_REQUEST,
                    "illegal_database_name",
                    format!("Name: '{s}'. Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed. Must begin with a letter."),
                ))
            } else {
                Err(CouchError::not_found("Database does not exist."))
            }
        }
        Some(_) => crate::api::db_request(req).await,
    }
}

fn welcome(req: &Req) -> Result<Response> {
    if req.method != Method::GET && req.method != Method::HEAD {
        return not_allowed("GET,HEAD");
    }
    // chttpd_misc:get_features/0: search engines, the JavaScript engine,
    // then the features CouchDB's applications enable.
    let mut features = Vec::new();
    if crate::nouveau::enabled(&req.server) {
        features.push("nouveau");
    }
    features.extend(["quickjs", "access-ready", "partitioned", "pluggable-storage-engines", "reshard", "scheduler"]);
    let mut out = json!({
        "couchdb": "Welcome",
        "version": VERSION,
        "git_sha": "rust",
        "uuid": req.server.uuid,
        "features": features,
    });
    let vendor: serde_json::Map<String, Value> = req.server.config.section("vendor").into_iter().map(|(k, v)| (k, json!(v))).collect();
    if !vendor.is_empty() {
        out["vendor"] = Value::Object(vendor);
    }
    ok_json(out)
}

fn all_dbs(req: &Req) -> Result<Response> {
    ok_json(json!(db_names(req)?))
}

/// The databases _all_dbs (and GET _dbs_info) list: all_docs arguments
/// over the database list, keys included.
fn db_names(req: &Req) -> Result<Vec<String>> {
    if let Some(keys) = req.q_json("keys")? {
        let keys = keys.as_array().cloned().ok_or_else(|| CouchError::bad_request("`keys` member must be an array."))?;
        let all: std::collections::HashSet<String> = req.server.all_dbs().into_iter().collect();
        return Ok(keys.iter().filter_map(|k| k.as_str()).filter(|k| all.contains(*k)).map(String::from).collect());
    }
    let mut dbs = req.server.all_dbs();
    if req.flag("descending")? {
        dbs.reverse();
    }
    let start = req.q_json("startkey")?.or(req.q_json("start_key")?);
    let end = req.q_json("endkey")?.or(req.q_json("end_key")?);
    let desc = req.flag("descending")?;
    dbs.retain(|d| {
        let ok_start = match start.as_ref().and_then(|s| s.as_str()) {
            Some(s) => if desc { d.as_str() <= s } else { d.as_str() >= s },
            None => true,
        };
        let ok_end = match end.as_ref().and_then(|s| s.as_str()) {
            Some(e) => if desc { d.as_str() >= e } else { d.as_str() <= e },
            None => true,
        };
        ok_start && ok_end
    });
    let skip = req.q_u64("skip")?.unwrap_or(0) as usize;
    let limit = req.q_u64("limit")?.map(|l| l as usize).unwrap_or(usize::MAX);
    Ok(dbs.into_iter().skip(skip).take(limit).collect())
}

fn dbs_info(req: &Req) -> Result<Response> {
    let get = req.method != Method::POST;
    let names: Vec<String> = if !get {
        let b = req.json_obj_body()?;
        b.get("keys")
            .and_then(|k| k.as_array())
            .ok_or_else(|| CouchError::bad_request("`keys` member must exist."))?
            .iter()
            .filter_map(|k| k.as_str().map(String::from))
            .collect()
    } else {
        db_names(req)?
    };
    let mut out = Vec::new();
    for n in names {
        match req.server.open_db(&n) {
            Ok(db) => out.push(json!({"key": n, "info": db.info()})),
            // Listed ones that went away are left out; asked-for ones are
            // reported missing.
            Err(_) if get => {}
            Err(_) => out.push(json!({"key": n, "error": "not_found"})),
        }
    }
    ok_json(json!(out))
}

fn uuids(req: &Req) -> Result<Response> {
    let n = req.q_u64("count")?.unwrap_or(1);
    if n > 1000 {
        return Err(CouchError::bad_request("count parameter too large"));
    }
    let ids: Vec<String> = (0..n).map(|_| new_uuid()).collect();
    ok_json(json!({"uuids": ids}))
}

/// Sequential-prefix uuids like CouchDB's default algorithm, which keeps
/// generated ids clustered in the B-tree.
pub fn new_uuid() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static PREFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let prefix = PREFIX.get_or_init(|| hex::encode(&uuid::Uuid::new_v4().as_bytes()[..13]));
    let n = COUNTER.fetch_add(1 + (rand::random::<u64>() % 0xffe), Ordering::Relaxed);
    format!("{prefix}{:06x}", n & 0xffffff)
}

/// _session (couch_httpd_auth:handle_session_req): who is logged in, log
/// in (JSON or form name/password; sets the AuthSession cookie), log out.
/// `next` redirects after login or logout.
fn session(req: &Req) -> Result<Response> {
    let config = &req.server.config;
    let redirect = |r: Response| -> Response {
        match req.q("next") {
            Some(next) => {
                let (mut parts, body) = r.into_parts();
                parts.status = StatusCode::FOUND;
                if let Ok(v) = HeaderValue::from_str(&absolute_uri(req, next)) {
                    parts.headers.insert(header::LOCATION, v);
                }
                Response::from_parts(parts, body)
            }
            None => r,
        }
    };
    match req.method {
        Method::GET | Method::HEAD => {
            let user = &req.auth.user;
            if req.q("basic") == Some("true") && user.name.is_none() && !user.is_server_admin() {
                return Err(CouchError::unauthorized("Authentication required."));
            }
            let mut info = json!({"authentication_handlers": crate::auth::handlers(config)});
            if let Some(m) = req.auth.method {
                info["authenticated"] = json!(m);
            }
            ok_json(json!({"ok": true, "userCtx": {"name": user.name, "roles": user.roles}, "info": info}))
        }
        Method::POST => {
            // The form, as couch_httpd_auth reads it: other content types
            // give none.
            let creds: Value = if req.content_type().starts_with("application/x-www-form-urlencoded") {
                let m: serde_json::Map<String, Value> = form_urlencoded::parse(&req.body)
                    .map(|(k, v)| (k.to_string(), json!(v)))
                    .collect();
                Value::Object(m)
            } else if req.content_type().starts_with("application/json") {
                req.json_body()?
            } else {
                json!({})
            };
            // extract_username/1: name, or username, or both the same.
            let name = match (creds.get("name").and_then(Value::as_str), creds.get("username").and_then(Value::as_str)) {
                (None, None) => return Err(CouchError::bad_request("request body must contain a username")),
                (Some(n), None) | (None, Some(n)) => n,
                (Some(n), Some(u)) if n == u => n,
                _ => return Err(CouchError::bad_request("request body contains different usernames")),
            };
            let pass = creds.get("password").and_then(|n| n.as_str()).unwrap_or("");
            let clear = crate::auth::cookie_header(config, "", false);
            match crate::auth::get_user_creds(&req.server, name).filter(|c| c.check_password(pass)) {
                Some(c) => {
                    let value = crate::auth::make_cookie(&req.server, name, &c.salt, now_secs())
                        .ok_or_else(|| CouchError::internal("cookie auth secret is not set"))?;
                    let r = json_response(StatusCode::OK, &json!({"ok": true, "name": name, "roles": c.roles}));
                    let r = with_header(r, header::SET_COOKIE, &crate::auth::cookie_header(config, &value, false));
                    Ok(redirect(r))
                }
                None => {
                    let r = CouchError::unauthorized("Name or password is incorrect.").into_response();
                    Ok(with_header(r, header::SET_COOKIE, &clear))
                }
            }
        }
        Method::DELETE => {
            let r = json_response(StatusCode::OK, &json!({"ok": true}));
            let r = with_header(r, header::SET_COOKIE, &crate::auth::cookie_header(config, "", false));
            Ok(redirect(r))
        }
        _ => not_allowed("GET,HEAD,POST,DELETE"),
    }
}

/// /_cluster_setup (setup_httpd), for the one node there is: its state,
/// and enable_single_node and finish_cluster to set it up.
async fn cluster_setup(req: &Req) -> Result<Response> {
    if !req.auth.user.is_server_admin() {
        return Err(CouchError::unauthorized("You are not a server admin."));
    }
    let config = &req.server.config;
    let dbs_exist = |dbs: &[String]| dbs.iter().all(|d| req.server.open_db(d).is_ok());
    let system_dbs = vec!["_users".to_string(), "_replicator".to_string()];
    match req.method {
        Method::GET | Method::HEAD => {
            let dbs: Vec<String> = req.q_json("ensure_dbs_exist")?.and_then(|v| serde_json::from_value(v).ok()).unwrap_or(system_dbs);
            let enabled = config.get_bool("couchdb", "single_node", false) || (!config.section("admins").is_empty() && dbs_exist(&dbs));
            ok_json(json!({"state": if enabled { "single_node_enabled" } else { "single_node_disabled" }}))
        }
        Method::POST => {
            if req.content_type().split(';').next() != Some("application/json") {
                return Err(CouchError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "bad_content_type", "Content-Type must be application/json"));
            }
            let body = req.json_obj_body().map_err(|_| CouchError::bad_request("Missing JSON body'"))?;
            let s = |k: &str| body.get(k).and_then(|v| v.as_str()).map(String::from);
            let dbs: Vec<String> = body.get("ensure_dbs_exist").and_then(|v| serde_json::from_value(v.clone()).ok()).unwrap_or(system_dbs);
            let create = |dbs: &[String]| -> Result<()> {
                for d in dbs {
                    if req.server.open_db(d).is_err() {
                        req.server.create_db(d)?;
                    }
                }
                Ok(())
            };
            match body.get("action").and_then(|a| a.as_str()).unwrap_or("missing") {
                "enable_single_node" => {
                    let user = s("username");
                    let pass = s("password_hash").or_else(|| s("password"));
                    if config.section("admins").is_empty() && user.is_none() && pass.is_none() {
                        return Err(CouchError::bad_request("Cluster setup requires admin account to be configured"));
                    }
                    if let (Some(u), Some(p)) = (&user, &pass) {
                        let hashed = if s("password_hash").is_some() { p.clone() } else { crate::passwords::hash_admin_password(config, p) };
                        config.set("admins", u, &hashed, true)?;
                    }
                    // Like CouchDB, the address and port take effect when
                    // the server next starts.
                    config.set("chttpd", "bind_address", &s("bind_address").unwrap_or_else(|| "0.0.0.0".into()), true)?;
                    if let Some(port) = body.get("port").map(|p| p.as_str().map(String::from).unwrap_or_else(|| p.to_string())) {
                        config.set("chttpd", "port", &port, true)?;
                    }
                    config.set("cluster", "n", "1", true)?;
                    if !dbs_exist(&dbs) {
                        create(&dbs)?;
                    }
                }
                "finish_cluster" => {
                    if dbs_exist(&dbs) {
                        return Err(CouchError::bad_request("Cluster is already finished"));
                    }
                    create(&dbs)?;
                }
                "remove_node" => {}
                "enable_cluster" | "add_node" | "receive_cookie" => {
                    return Err(CouchError::bad_request("CouchRS runs as a single node"));
                }
                _ => return Err(CouchError::bad_request("Invalid Action'")),
            }
            Ok(json_response(StatusCode::CREATED, &json!({"ok": true})))
        }
        _ => not_allowed("GET,POST"),
    }
}

pub const NODE_NAME: &str = "couchdb@127.0.0.1";

/// /_node/{node}/..., where {node} is _local or this node's name
/// (chttpd_node).
fn node(req: &Req) -> Result<Response> {
    let Some(name) = req.path.get(1) else { return Err(CouchError::not_found("missing")) };
    if name != "_local" && name != NODE_NAME {
        return Err(CouchError::not_found(format!("no such node: {name}")));
    }
    let rest: Vec<&str> = req.path.iter().skip(2).map(|s| s.as_str()).collect();
    let get = matches!(req.method, Method::GET | Method::HEAD);
    match rest.as_slice() {
        [] => ok_json(json!({"name": NODE_NAME})),
        ["_config", ..] => node_config(req, &rest[1..]),
        ["_stats", path @ ..] if get => ok_json(crate::stats::extract_path(crate::stats::fetch_json(), path)),
        ["_system"] if get => ok_json(crate::system::system_json(&req.server)),
        ["_prometheus"] if get => Ok(prometheus_response(&req.server)),
        ["_versions"] if get => ok_json(crate::system::versions()),
        ["_smoosh", "status"] if get => ok_json(crate::smoosh::status()),
        ["_stats", ..] | ["_system"] | ["_prometheus"] | ["_versions"] | ["_smoosh", "status"] => not_allowed("GET"),
        ["_restart"] if req.method == Method::POST => {
            restart();
            ok_json(json!({"ok": true}))
        }
        ["_restart"] => not_allowed("POST"),
        _ => Err(CouchError::not_found("missing")),
    }
}

fn prometheus_response(server: &Server) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=2.0")
        .body(Body::from(crate::system::prometheus(server)))
        .unwrap()
}

/// Restart in place, as init:restart does: once the reply is out, run
/// this binary again with the same arguments, in this process. Writes
/// are durable when acknowledged, so nothing acknowledged is lost.
fn restart() {
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(200));
        tracing::info!("restarting");
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            match std::env::current_exe() {
                Ok(exe) => {
                    let err = std::process::Command::new(exe).args(std::env::args_os().skip(1)).exec();
                    tracing::error!("restart failed: {err}");
                }
                Err(e) => tracing::error!("restart failed: {e}"),
            }
        }
        std::process::exit(1);
    });
}

/// The _config API: the whole config, a section, or one value, which can
/// be set (a JSON string; X-Couch-Persist: false keeps it out of the ini
/// file) or deleted, each returning the old value; _reload re-reads the
/// files. Admin passwords are stored hashed.
fn node_config(req: &Req, rest: &[&str]) -> Result<Response> {
    let config = &req.server.config;
    let persist = req.header("x-couch-persist") != Some("false");
    match (rest, &req.method) {
        ([], &Method::GET) => ok_json(json!(config.all())),
        ([], _) => not_allowed("GET"),
        (["_reload"], &Method::POST) => {
            config.reload().map_err(|e| CouchError::bad_request(e.reason))?;
            ok_json(json!({"ok": true}))
        }
        (["_reload"], _) => not_allowed("POST"),
        ([section], &Method::GET) => {
            let m: serde_json::Map<String, Value> =
                config.section(section).into_iter().map(|(k, v)| (k, json!(v))).collect();
            ok_json(Value::Object(m))
        }
        ([_], _) => not_allowed("GET"),
        ([section, key], &Method::GET) => match config.get(section, key) {
            Some(v) => ok_json(json!(v)),
            None => Err(CouchError::not_found("unknown_config_value")),
        },
        ([section, key], &Method::PUT) => {
            crate::config::check_blacklist(section)?;
            let Ok(Value::String(v)) = serde_json::from_slice::<Value>(&req.body) else {
                return Err(CouchError::bad_request("a JSON string expected"));
            };
            let mut v = v.trim().to_string();
            let old = config.get(section, key).unwrap_or_default();
            if *section == "admins" && !crate::passwords::is_hashed(&v) {
                v = crate::passwords::hash_admin_password(config, &v);
            }
            config.set(section, key, &v, persist)?;
            ok_json(json!(old))
        }
        ([section, key], &Method::DELETE) => {
            crate::config::check_blacklist(section)?;
            let Some(old) = config.get(section, key) else {
                return Err(CouchError::not_found("unknown_config_value"));
            };
            config.delete(section, key, persist)?;
            ok_json(json!(old))
        }
        ([_, _], _) => not_allowed("GET,PUT,DELETE"),
        _ => Err(CouchError::not_found("missing")),
    }
}

pub async fn serve(server: Arc<Server>) -> std::io::Result<()> {
    let app = axum::Router::new().fallback(handle).with_state(server.clone());
    let bind = bind_addr(&server, "chttpd", "5984");
    if let Err(e) = server.ensure_system_dbs().await {
        tracing::error!("could not set up system databases: {}", e.reason);
    }
    crate::smoosh::start(&server);
    crate::ken::start(&server);
    crate::replicator::start(&server);
    crate::changes::start_global_changes(&server);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("CouchRS listening on http://{bind}");
    crate::system::mark_start();
    if server.config.get_bool("ssl", "enable", false) {
        let cfg = crate::tls::server_config(&server.config).map_err(std::io::Error::other)?;
        let addr = server.config.get_or("chttpd", "bind_address", "127.0.0.1");
        let port = server.config.get_or("ssl", "port", "6984");
        let sbind = if addr.contains(':') { format!("[{addr}]:{port}") } else { format!("{addr}:{port}") };
        let l = crate::tls::listen(&sbind, cfg).await?;
        tracing::info!("CouchRS listening on https://{sbind}");
        let sapp = app.clone().layer(axum::Extension(Tls));
        tokio::spawn(async move { axum::serve(l, sapp).await });
    }
    if server.config.get_bool("prometheus", "additional_port", false) {
        let pbind = bind_addr(&server, "prometheus", "17986");
        let l = tokio::net::TcpListener::bind(&pbind).await?;
        tracing::info!("prometheus metrics on http://{pbind}/_node/_local/_prometheus");
        let papp = axum::Router::new().fallback(prometheus_port).with_state(server.clone());
        tokio::spawn(async move { axum::serve(crate::system::CountingListener(l), papp).await });
    }
    axum::serve(crate::system::CountingListener(listener), app).await
}

/// A section's bind_address and port as a socket address (IPv6 bracketed).
fn bind_addr(server: &Server, section: &str, default_port: &str) -> String {
    let addr = server.config.get_or(section, "bind_address", "127.0.0.1");
    let port = server.config.get_or(section, "port", default_port);
    if addr.contains(':') { format!("[{addr}]:{port}") } else { format!("{addr}:{port}") }
}

/// The [prometheus] additional_port: /_node/{node}/_prometheus without
/// authentication, and nothing else (couch_prometheus_http).
async fn prometheus_port(State(server): State<Arc<Server>>, req: Request) -> Response {
    let parts: Vec<&str> = req.uri().path().split('/').filter(|s| !s.is_empty()).collect();
    match parts.as_slice() {
        ["_node", node, "_prometheus"] if *node == "_local" || *node == NODE_NAME => prometheus_response(&server),
        ["_node", node, "_prometheus"] => CouchError::not_found(format!("no such node: {node}")).into_response(),
        _ => json_response(StatusCode::NOT_FOUND, &json!({"error": "not_found", "reason": ""})),
    }
}

pub fn empty(status: StatusCode) -> Response {
    Response::builder().status(status).body(Body::empty()).unwrap()
}
