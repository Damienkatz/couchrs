//! HTTP front end, the equivalent of chttpd. Paths are parsed by hand (like
//! chttpd does) so `_design/foo`, `_local/foo`, `%2F`-encoded database names
//! and attachment names containing slashes all route correctly.

use crate::error::{CouchError, Result};
use crate::server::Server;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use bytes::Bytes;
use serde_json::{Value, json};
use std::sync::Arc;

pub const VERSION: &str = "3.5.0";

pub struct Req {
    pub method: Method,
    pub path: Vec<String>,
    pub query: Vec<(String, String)>,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub server: Arc<Server>,
    /// Whether the raw path ended in '/' (lost when splitting `path`).
    pub trailing_slash: bool,
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
            Some(v) => serde_json::from_str(v).map(Some).map_err(|_| {
                CouchError::bad_request(format!("Invalid JSON for query parameter {k}: {v}"))
            }),
        }
    }

    pub fn json_body(&self) -> Result<Value> {
        if self.body.is_empty() {
            return Err(CouchError::bad_request("Request body must be a JSON object"));
        }
        serde_json::from_slice(&self.body).map_err(|_| {
            CouchError::new(StatusCode::BAD_REQUEST, "bad_request", "invalid UTF-8 JSON")
        })
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
}

pub fn json_response(status: StatusCode, v: &Value) -> Response {
    let mut body = serde_json::to_vec(v).unwrap();
    body.push(b'\n');
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "must-revalidate")
        .body(Body::from(body))
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
        .map(|q| form_urlencoded::parse(q.as_bytes()).into_owned().collect())
        .unwrap_or_default();
    let body = match axum::body::to_bytes(body, 4 << 30).await {
        Ok(b) => b,
        Err(e) => return CouchError::bad_request(e.to_string()).into_response(),
    };
    let method = parts.method.clone();
    let req = Req {
        method: parts.method,
        path,
        query,
        headers: parts.headers,
        body,
        server,
        trailing_slash: raw_path.ends_with('/'),
    };
    let is_head = method == Method::HEAD;
    let resp = match authorize(&req) {
        Ok(()) => match route(req).await {
            Ok(r) => r,
            Err(e) => e.into_response(),
        },
        Err(e) => e.into_response(),
    };
    if is_head {
        let (mut parts, _) = resp.into_parts();
        parts.headers.remove(header::CONTENT_LENGTH);
        return Response::from_parts(parts, Body::empty());
    }
    resp
}

/// Basic or cookie auth against the configured admin. With no admin
/// configured the server runs in "admin party" mode.
fn authorize(req: &Req) -> Result<()> {
    if admin_party(&req.server) {
        return Ok(());
    }
    // Like CouchDB, the welcome, _up, _session and the web UI's static
    // files need no credentials; Fauxton logs in through _session.
    if req.path.is_empty() || matches!(req.path[0].as_str(), "_up" | "_session" | "_utils") {
        return Ok(());
    }
    if basic_admin(req).is_some() || session_user(req).is_some() {
        return Ok(());
    }
    Err(CouchError::unauthorized("You are not authorized to access this db."))
}

/// No admins configured: everyone is an admin.
pub fn admin_party(server: &Server) -> bool {
    server.config.section("admins").is_empty()
}

/// Whether name/password match a configured admin.
fn check_admin(server: &Server, name: &str, password: &str) -> bool {
    server
        .config
        .get("admins", name)
        .is_some_and(|stored| crate::passwords::verify_admin(&stored, password))
}

/// The admin a Basic Authorization header names, if its password matches.
fn basic_admin(req: &Req) -> Option<String> {
    let h = req.header("authorization")?;
    let d = crate::doc::B64.decode(h.strip_prefix("Basic ")?.trim()).ok()?;
    let s = String::from_utf8(d).ok()?;
    let (u, p) = s.split_once(':')?;
    check_admin(&req.server, u, p).then(|| u.to_string())
}

fn session_secret(server: &Server) -> String {
    server.config.get_or("chttpd_auth", "secret", &server.uuid)
}

fn session_token(server: &Server, name: &str, ts: u64) -> String {
    use md5::{Digest, Md5};
    let mac = hex::encode(Md5::digest(format!("{}:{name}:{ts}", session_secret(server))));
    crate::doc::B64.encode(format!("{name}:{ts}:{mac}"))
}

fn session_user(req: &Req) -> Option<String> {
    let cookie = req.header("cookie")?;
    let tok = cookie
        .split(';')
        .map(|c| c.trim())
        .find_map(|c| c.strip_prefix("AuthSession="))?;
    let raw = String::from_utf8(crate::doc::B64.decode(tok).ok()?).ok()?;
    let mut it = raw.splitn(3, ':');
    let name = it.next()?;
    let ts: u64 = it.next()?.parse().ok()?;
    let timeout: u64 = req.server.config.get_int("chttpd_auth", "timeout", 600);
    // The session ends with the admin it was issued to.
    if session_token(&req.server, name, ts) == tok
        && now_secs() < ts + timeout
        && req.server.config.get("admins", name).is_some()
    {
        Some(name.to_string())
    } else {
        None
    }
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

async fn route(req: Req) -> Result<Response> {
    let first = req.path.first().cloned();
    match first.as_deref() {
        None => welcome(&req),
        Some("_all_dbs") => all_dbs(&req),
        Some("_dbs_info") => dbs_info(&req),
        Some("_uuids") => uuids(&req),
        Some("_up") => ok_json(json!({"status": "ok", "seeds": {}})),
        Some("_session") => session(&req),
        Some("_utils") => crate::fauxton::serve(&req),
        Some("_active_tasks") => ok_json(json!([])),
        Some("_membership") => ok_json(json!({
            "all_nodes": ["couchdb@127.0.0.1"], "cluster_nodes": ["couchdb@127.0.0.1"]
        })),
        Some("_node") => node(&req),
        Some("_replicate") => Err(CouchError::new(
            StatusCode::NOT_IMPLEMENTED,
            "not_implemented",
            "The replicator is not part of this server; replicate with an external CouchDB or PouchDB.",
        )),
        Some("_scheduler") => ok_json(json!({"total_rows": 0, "offset": 0, "docs": [], "jobs": []})),
        Some("_db_updates") => crate::changes::db_updates(req).await,
        // Fauxton's setup page asks this; couchdb-rs is always one node.
        Some("_cluster_setup") => match req.method {
            Method::GET | Method::HEAD => ok_json(json!({"state": "single_node_enabled"})),
            _ => Err(CouchError::new(
                StatusCode::NOT_IMPLEMENTED,
                "not_implemented",
                "couchdb-rs is a single node; set the admin and bind address on its command line.",
            )),
        },
        Some("favicon.ico") => Err(CouchError::not_found("missing")),
        Some(s) if s.starts_with('_') && !matches!(s, "_users" | "_replicator" | "_global_changes") => {
            Err(CouchError::new(
                StatusCode::BAD_REQUEST,
                "illegal_database_name",
                format!("Name: '{s}'. Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed. Must begin with a letter."),
            ))
        }
        Some(_) => crate::api::db_request(req).await,
    }
}

fn welcome(req: &Req) -> Result<Response> {
    if req.method != Method::GET && req.method != Method::HEAD {
        return not_allowed("GET,HEAD");
    }
    ok_json(json!({
        "couchdb": "Welcome",
        "version": VERSION,
        "git_sha": "rust",
        "uuid": req.server.uuid,
        "features": ["access-ready", "partitioned", "pluggable-storage-engines", "reshard", "scheduler"],
        "vendor": {"name": "couchdb-rs"}
    }))
}

fn all_dbs(req: &Req) -> Result<Response> {
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
    let out: Vec<String> = dbs.into_iter().skip(skip).take(limit).collect();
    ok_json(json!(out))
}

fn dbs_info(req: &Req) -> Result<Response> {
    let names: Vec<String> = if req.method == Method::POST {
        let b = req.json_obj_body()?;
        b.get("keys")
            .and_then(|k| k.as_array())
            .ok_or_else(|| CouchError::bad_request("`keys` member must exist."))?
            .iter()
            .filter_map(|k| k.as_str().map(String::from))
            .collect()
    } else {
        req.server.all_dbs()
    };
    let mut out = Vec::new();
    for n in names {
        match req.server.open_db(&n) {
            Ok(db) => out.push(json!({"key": n, "info": db.info()})),
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

fn session(req: &Req) -> Result<Response> {
    let party = admin_party(&req.server);
    match req.method {
        Method::GET | Method::HEAD => {
            let name = if party { None } else { session_user(req).or_else(|| basic_admin(req)) };
            let roles = if party || name.is_some() { json!(["_admin"]) } else { json!([]) };
            ok_json(json!({
                "ok": true,
                "userCtx": {"name": name, "roles": roles},
                "info": {"authentication_handlers": ["cookie", "default"], "authenticated": "default"}
            }))
        }
        Method::POST => {
            let creds: Value = if req.content_type().starts_with("application/x-www-form-urlencoded") {
                let m: serde_json::Map<String, Value> = form_urlencoded::parse(&req.body)
                    .map(|(k, v)| (k.to_string(), json!(v)))
                    .collect();
                Value::Object(m)
            } else {
                req.json_body()?
            };
            let name = creds.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let pass = creds.get("password").and_then(|n| n.as_str()).unwrap_or("");
            if party {
                return ok_json(json!({"ok": true, "name": null, "roles": ["_admin"]}));
            }
            match check_admin(&req.server, name, pass) {
                true => {
                    let tok = session_token(&req.server, name, now_secs());
                    let r = json_response(
                        StatusCode::OK,
                        &json!({"ok": true, "name": name, "roles": ["_admin"]}),
                    );
                    Ok(with_header(
                        r,
                        header::SET_COOKIE,
                        &format!("AuthSession={tok}; Version=1; Path=/; HttpOnly"),
                    ))
                }
                false => Err(CouchError::unauthorized("Name or password is incorrect.")),
            }
        }
        Method::DELETE => {
            let r = json_response(StatusCode::OK, &json!({"ok": true}));
            Ok(with_header(r, header::SET_COOKIE, "AuthSession=; Version=1; Path=/; HttpOnly; Max-Age=0"))
        }
        _ => not_allowed("GET,HEAD,POST,DELETE"),
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
    match rest.as_slice() {
        [] => ok_json(json!({"name": NODE_NAME})),
        ["_config", ..] => node_config(req, &rest[1..]),
        ["_stats", ..] => ok_json(json!({})),
        ["_system"] => ok_json(json!({"uptime": 0})),
        _ => Err(CouchError::not_found("missing")),
    }
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
    let app = axum::Router::new()
        .fallback(handle)
        .with_state(server.clone())
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(tower_http::cors::AllowOrigin::mirror_request())
                .allow_credentials(true)
                .allow_methods(tower_http::cors::AllowMethods::mirror_request())
                .allow_headers(tower_http::cors::AllowHeaders::mirror_request())
                .expose_headers([header::ETAG, header::CONTENT_TYPE]),
        );
    let addr = server.config.get_or("chttpd", "bind_address", "127.0.0.1");
    let port = server.config.get_or("chttpd", "port", "5984");
    let bind = if addr.contains(':') { format!("[{addr}]:{port}") } else { format!("{addr}:{port}") };
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("couchdb-rs listening on http://{bind}");
    axum::serve(listener, app).await
}

pub fn empty(status: StatusCode) -> Response {
    Response::builder().status(status).body(Body::empty()).unwrap()
}
