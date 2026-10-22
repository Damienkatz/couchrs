//! Replication documents: parsing and validation (couch_replicator_parse,
//! couch_replicator_filters) and replication ids (couch_replicator_ids),
//! computed exactly as CouchDB does — the MD5 of an Erlang external term —
//! so checkpoints are named the same.

use super::RepError;
use crate::server::Server;
use serde_json::{Map, Value, json};

pub const REP_ID_VERSION: u64 = 4;

/// One end of a replication (#httpdb{}).
#[derive(Clone, Debug)]
pub struct Endpoint {
    /// The database URL, without credentials, ending in '/' (unless it
    /// has a query string).
    pub url: String,
    /// Basic auth credentials, from wherever they were given.
    pub basic: Option<(String, String)>,
    /// Header names and values, sorted, the defaults included.
    pub headers: Vec<(String, String)>,
    /// The "auth" object, other than basic credentials.
    pub auth_props: Map<String, Value>,
    pub timeout_ms: u64,
    pub http_connections: u64,
    pub retries: u64,
    pub proxy: Option<String>,
    pub tls: Tls,
    /// Log in at _session and use its cookie (couch_replicator_auth_session
    /// first in [replicator] auth_plugins).
    pub session_auth: bool,
    /// Seconds between session refreshes when the cookie has no Max-Age.
    pub session_refresh_secs: u64,
}

/// TLS settings for https endpoints ([replicator] verify_ssl_certificates,
/// ssl_trusted_certificates_file, cert_file, key_file).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tls {
    pub verify: bool,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

fn tls_config(server: &Server) -> Tls {
    let c = &server.config;
    Tls {
        verify: c.get_bool("replicator", "verify_ssl_certificates", false),
        ca_file: c.get("replicator", "ssl_trusted_certificates_file"),
        cert_file: c.get("replicator", "cert_file"),
        key_file: c.get("replicator", "key_file"),
    }
}

pub fn user_agent() -> String {
    format!("CouchDB-Replicator/{}", crate::http::VERSION)
}

fn default_headers() -> Vec<(String, String)> {
    vec![("Accept".into(), "application/json".into()), ("User-Agent".into(), user_agent())]
}

impl Endpoint {
    /// The URL as reported: credentials removed.
    pub fn uri(&self) -> String {
        self.url.clone()
    }

    /// Headers the user gave (the defaults taken out), as ids use them.
    fn user_headers(&self) -> Vec<(String, String)> {
        let mut h = self.headers.clone();
        for d in default_headers() {
            if let Some(i) = h.iter().position(|x| *x == d) {
                h.remove(i);
            }
        }
        h
    }
}

/// Replication options (make_options/1), with the config's defaults.
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub cancel: bool,
    pub id: Option<(String, String)>,
    pub create_target: bool,
    pub create_target_params: Map<String, Value>,
    pub winning_revs_only: bool,
    pub continuous: bool,
    pub filter: Option<String>,
    pub query_params: Option<Map<String, Value>>,
    pub doc_ids: Option<Vec<String>>,
    pub selector: Option<Value>,
    pub worker_processes: u64,
    pub worker_batch_size: u64,
    pub http_connections: u64,
    pub connection_timeout: u64,
    pub retries: u64,
    pub since_seq: Option<Value>,
    pub use_checkpoints: bool,
    pub use_bulk_get: bool,
    pub checkpoint_interval: u64,
}

#[derive(Clone, Debug, Default)]
pub struct UserCtx {
    pub name: Option<String>,
    pub roles: Vec<String>,
}

/// How a replication filters (couch_replicator_filters:parse/1).
#[derive(Clone, Debug)]
pub enum Filter {
    None,
    /// A built-in filter (`_view`, `_doc_ids`, ...) and its query params.
    View(String, Map<String, Value>),
    /// A design doc filter: design doc name, filter name, query params.
    User(String, String, Map<String, Value>),
    DocIds(Vec<String>),
    /// A normalized, sorted selector.
    Mango(Value),
}

#[derive(Clone, Debug)]
pub struct Rep {
    pub id: (String, String),
    pub source: Endpoint,
    pub target: Endpoint,
    pub options: Options,
    pub user_ctx: UserCtx,
    /// `_view` filters: the design doc id and view name.
    pub view: Option<(String, String)>,
    /// For replications from a replicator db: the doc and the db.
    pub doc_id: Option<String>,
    pub db_name: Option<String>,
    pub start_time: std::time::SystemTime,
}

impl Rep {
    pub fn id_str(&self) -> String {
        format!("{}{}", self.id.0, self.id.1)
    }
}

// ------------------------------------------------------------- endpoints

fn bad_request(m: impl Into<String>) -> RepError {
    RepError::BadRequest(m.into())
}

/// mochiweb_util:quote_plus/1.
pub fn quote_plus(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'~' | b'_' => out.push(b as char),
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// mochiweb_util:unquote/1: %XX escapes, and '+' as a space.
fn unquote(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() && b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit() => {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap_or(b'%'));
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The parts of a URL the replicator uses: scheme, user info, host,
/// port, and the path (with any query).
pub struct UrlParts {
    pub scheme: String,
    pub userinfo: Option<String>,
    pub host: String,
    pub port: u16,
    pub path: String,
}

pub fn parse_url(url: &str) -> Option<UrlParts> {
    let (scheme, rest) = url.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => match rest.find('?') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        },
    };
    let (userinfo, hostport) = match authority.rfind('@') {
        Some(i) => (Some(authority[..i].to_string()), &authority[i + 1..]),
        None => (None, authority),
    };
    let default_port = match scheme.as_str() {
        "https" => 443,
        "http" => 80,
        "socks5" => 1080,
        _ => 80,
    };
    let (host, port) = if let Some(h) = hostport.strip_prefix('[') {
        let (h, after) = h.split_once(']')?;
        let port = match after.strip_prefix(':') {
            Some(p) => p.parse().ok()?,
            None => default_port,
        };
        (h.to_string(), port)
    } else {
        match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().ok()?),
            None => (hostport.to_string(), default_port),
        }
    };
    if host.is_empty() {
        return None;
    }
    Some(UrlParts { scheme, userinfo, host, port, path: path.to_string() })
}

/// The URL without its user info (uri_string:recompose/1 of the rest).
fn strip_userinfo(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => {
            let end = rest.find(['/', '?']).unwrap_or(rest.len());
            match rest[..end].rfind('@') {
                Some(at) => format!("{scheme}://{}", &rest[at + 1..]),
                None => url.to_string(),
            }
        }
        None => url.to_string(),
    }
}

/// couch_util:url_strip_password/1: "user:pass@" becomes "user:*****@".
pub fn url_strip_password(url: &str) -> String {
    let re = regex::Regex::new(r"(?i)^(https?://[^:/]+):[^@/]+@").expect("regex");
    re.replace(url, "$1:*****@").into_owned()
}

fn check_url(url: &str, kind: &str, server: &Server) -> Result<(), RepError> {
    let key = format!("valid_{kind}_protocols");
    let default = if kind == "proxy" { "http,https,socks5" } else { "http,https" };
    let allowed = server.config.get_or("replicator", &key, default);
    let ok = parse_url(url).is_some_and(|u| allowed.split(',').any(|p| p.trim() == u.scheme));
    if ok { Ok(()) } else { Err(RepError::Error(format!("{kind} has an invalid url"))) }
}

fn decode_basic(header: &str) -> Option<(String, String)> {
    let (scheme, b64) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD.decode(b64.trim()).ok()?;
    let s = String::from_utf8_lossy(&raw).into_owned();
    let (u, p) = s.split_once(':')?;
    Some((u.to_string(), p.to_string()))
}

/// parse_rep_db/3 for an http(s) endpoint, string or object.
fn parse_endpoint(v: &Value, proxy: &str, opts: &Options, server: &Server) -> Result<Endpoint, RepError> {
    let (url0, auth, hdrs) = match v {
        Value::String(s) if !s.starts_with("http://") && !s.starts_with("https://") => {
            return Err(RepError::Error("local_endpoints_not_supported".into()));
        }
        Value::String(s) => (s.clone(), Map::new(), Map::new()),
        Value::Object(o) => (
            o.get("url").and_then(Value::as_str).unwrap_or_default().to_string(),
            o.get("auth").and_then(Value::as_object).cloned().unwrap_or_default(),
            o.get("headers").and_then(Value::as_object).cloned().unwrap_or_default(),
        ),
        Value::Null => return Err(RepError::Error("Missing replicator database".into())),
        _ => return Err(RepError::Error("endpoint has an invalid url".into())),
    };
    check_url(&url0, "endpoint", server)?;
    let url = if url0.contains('?') || url0.ends_with('/') { url0 } else { format!("{url0}/") };
    // ukeysort the user's headers, then ukeymerge the defaults in.
    let mut user: Vec<(String, String)> =
        hdrs.iter().map(|(k, v)| (k.clone(), v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))).collect();
    user.sort_by(|a, b| a.0.cmp(&b.0));
    user.dedup_by(|a, b| a.0 == b.0);
    let mut headers = user.clone();
    for d in default_headers() {
        if !headers.iter().any(|(k, _)| *k == d.0) {
            headers.push(d);
        }
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));
    // normalize_basic_auth/1: the auth object, then the URL, then the
    // Authorization header.
    let auth_creds = auth.get("basic").and_then(Value::as_object).and_then(|b| {
        Some((b.get("username")?.as_str()?.to_string(), b.get("password")?.as_str()?.to_string()))
    });
    let url_creds = parse_url(&url).and_then(|u| u.userinfo).and_then(|ui| ui.split_once(':').map(|(a, b)| (a.to_string(), b.to_string())));
    let mut header_creds = None;
    if let Some(i) = headers.iter().position(|(k, _)| k.eq_ignore_ascii_case("authorization")) {
        // Through mochiweb_headers: lowercase-name order afterwards.
        if let Some(c) = decode_basic(&headers[i].1) {
            header_creds = Some(c);
            headers.remove(i);
        }
        headers.sort_by_key(|h| h.0.to_ascii_lowercase());
    }
    let basic = auth_creds.or(url_creds).or(header_creds);
    let mut auth_props = auth;
    auth_props.shift_remove("basic");
    let proxy_url = if proxy.is_empty() {
        None
    } else {
        check_url(proxy, "proxy", server).map_err(|_| RepError::Error("proxy has an invalid url".into()))?;
        Some(proxy.to_string())
    };
    Ok(Endpoint {
        url: strip_userinfo(&url),
        basic,
        headers,
        auth_props,
        timeout_ms: opts.connection_timeout,
        http_connections: opts.http_connections,
        retries: opts.retries,
        proxy: proxy_url,
        tls: tls_config(server),
        session_auth: server
            .config
            .get_or("replicator", "auth_plugins", "couch_replicator_auth_session,couch_replicator_auth_noop")
            .split(',')
            .next()
            .is_some_and(|p| p.trim() == "couch_replicator_auth_session"),
        session_refresh_secs: server.config.get_int("replicator", "session_refresh_interval_sec", 550u64),
    })
}

/// chttpd's "hack": a local database name becomes a URL to this server,
/// with the request's credentials.
pub fn localize(body: &mut Map<String, Value>, server: &Server, authorization: &str, cookie: &str) {
    for key in ["source", "target"] {
        let name = match body.get(key) {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Object(o)) => match o.get("url") {
                Some(Value::String(s)) => s.clone(),
                _ => continue,
            },
            _ => continue,
        };
        if name.starts_with("http://") || name.starts_with("https://") {
            continue;
        }
        let addr = server.config.get_or("chttpd", "bind_address", "127.0.0.1");
        let port = server.config.get_or("chttpd", "port", "5984");
        let host = if addr.contains(':') { format!("[{addr}]") } else { addr };
        body.insert(
            key.into(),
            json!({
                "url": format!("http://{host}:{port}/{}", quote_plus(&name)),
                "headers": {"authorization": authorization, "cookie": cookie}
            }),
        );
    }
}

// --------------------------------------------------------------- options

fn cfg_int(server: &Server, key: &str, default: u64) -> u64 {
    server.config.get_int("replicator", key, default)
}

fn to_integer(v: &Value, name: &str) -> Result<u64, RepError> {
    match v {
        Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)).ok_or_else(|| bad_request(format!("parameter `{name}` must be an integer"))),
        Value::String(s) => s.trim().parse().map_err(|_| bad_request(format!("parameter `{name}` must be an integer"))),
        _ => Err(bad_request(format!("parameter `{name}` must be an integer"))),
    }
}

/// The replication id a `_local_id`, `replication_id` or `id` names
/// (couch_replicator_ids:convert/1).
pub fn convert_id(id: &str) -> (String, String) {
    let id = id.replace(' ', "+");
    match id.find('+') {
        Some(i) => (id[..i].to_string(), id[i..].to_string()),
        None => (id, String::new()),
    }
}

fn make_options(props: &Map<String, Value>, server: &Server) -> Result<Options, RepError> {
    let boolean = |k: &str, v: &Value| -> Result<bool, RepError> {
        v.as_bool().ok_or_else(|| bad_request(format!("parameter `{k}` must be a boolean")))
    };
    let mut o = Options {
        worker_processes: cfg_int(server, "worker_processes", 4),
        worker_batch_size: cfg_int(server, "worker_batch_size", 500),
        http_connections: cfg_int(server, "http_connections", 20),
        connection_timeout: cfg_int(server, "connection_timeout", 30000),
        retries: cfg_int(server, "retries_per_request", 5),
        use_checkpoints: server.config.get_bool("replicator", "use_checkpoints", true),
        use_bulk_get: server.config.get_bool("replicator", "use_bulk_get", true),
        checkpoint_interval: cfg_int(server, "checkpoint_interval", 30000),
        ..Default::default()
    };
    // convert_options/1, in the order the doc gives them.
    for (k, v) in props {
        match k.as_str() {
            "cancel" => o.cancel = boolean(k, v)?,
            "_local_id" | "replication_id" | "id" => {
                o.id = Some(convert_id(v.as_str().unwrap_or_default()));
            }
            "create_target" => o.create_target = boolean(k, v)?,
            "create_target_params" => {
                o.create_target_params =
                    v.as_object().cloned().ok_or_else(|| bad_request("parameter `create_target_params` must be a JSON object"))?
            }
            "winning_revs_only" => o.winning_revs_only = boolean(k, v)?,
            "continuous" => o.continuous = boolean(k, v)?,
            "filter" => o.filter = Some(v.as_str().map(String::from).unwrap_or_else(|| v.to_string())),
            "query_params" => {
                o.query_params = Some(v.as_object().cloned().ok_or_else(|| bad_request("parameter `query_params` must be an object"))?)
            }
            "doc_ids" => match v {
                Value::Null => {}
                Value::Array(a) => {
                    let mut ids: Vec<String> = a.iter().map(|i| unquote(i.as_str().unwrap_or_default())).collect();
                    ids.sort();
                    ids.dedup();
                    o.doc_ids = Some(ids);
                }
                _ => return Err(bad_request("parameter `doc_ids` must be an array")),
            },
            "selector" => {
                if !v.is_object() {
                    return Err(bad_request("parameter `selector` must be a JSON object"));
                }
                o.selector = Some(v.clone());
            }
            "worker_processes" => o.worker_processes = to_integer(v, k)?,
            "worker_batch_size" => o.worker_batch_size = to_integer(v, k)?,
            "http_connections" => o.http_connections = to_integer(v, k)?,
            "connection_timeout" => o.connection_timeout = to_integer(v, k)?,
            "retries_per_request" => o.retries = to_integer(v, k)?,
            "since_seq" => o.since_seq = Some(v.clone()),
            "use_checkpoints" => o.use_checkpoints = v.as_bool().unwrap_or(true),
            "use_bulk_get" => o.use_bulk_get = boolean(k, v)?,
            "checkpoint_interval" => o.checkpoint_interval = to_integer(v, k)?,
            _ => {}
        }
    }
    let set = [o.doc_ids.is_some(), o.filter.is_some(), o.selector.is_some()].iter().filter(|b| **b).count();
    if set > 1 {
        return Err(bad_request("`doc_ids`,`filter`,`selector` are mutually exclusive"));
    }
    Ok(o)
}

/// couch_replicator_filters:parse/1.
pub fn filter(o: &Options) -> Result<Filter, RepError> {
    let qp = || o.query_params.clone().unwrap_or_default();
    Ok(match (&o.filter, &o.doc_ids, &o.selector) {
        (None, None, None) => Filter::None,
        (Some(f), None, None) if f.starts_with('_') => Filter::View(f.clone(), qp()),
        (Some(f), None, None) => match f.split_once('/') {
            Some((d, n)) => Filter::User(d.to_string(), n.to_string(), qp()),
            None => return Err(RepError::Error("Invalid filter. Must match `ddocname/filtername`.".into())),
        },
        (None, Some(ids), None) => Filter::DocIds(ids.clone()),
        (None, None, Some(s)) => Filter::Mango(ejsort(&crate::mango::normalize_selector(s).map_err(|e| bad_request(e.reason))?)),
        _ => return Err(RepError::Error("`selector`, `filter` and `doc_ids` are mutually exclusive".into())),
    })
}

/// Sort object members by key, everywhere (ejsort/1).
pub fn ejsort(v: &Value) -> Value {
    match v {
        Value::Object(o) => {
            let mut kv: Vec<(&String, &Value)> = o.iter().collect();
            kv.sort_by(|a, b| a.0.cmp(b.0));
            Value::Object(kv.into_iter().map(|(k, v)| (k.clone(), ejsort(v))).collect())
        }
        Value::Array(a) => Value::Array(a.iter().map(ejsort).collect()),
        v => v.clone(),
    }
}

fn user_ctx_of(doc: &Map<String, Value>) -> UserCtx {
    match doc.get("user_ctx").and_then(Value::as_object) {
        None => UserCtx::default(),
        Some(u) => UserCtx {
            name: u.get("name").and_then(Value::as_str).map(String::from),
            roles: u.get("roles").and_then(Value::as_array).map(|r| r.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default(),
        },
    }
}

/// parse_rep_doc/2 without the id: endpoints, options and filter checks.
/// A cancel with an id needs nothing else.
pub fn parse_without_id(doc: &Map<String, Value>, user_ctx: UserCtx, server: &Server) -> Result<Rep, RepError> {
    let get_str = |k: &str| doc.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    let (proxy, src_proxy, tgt_proxy) = (get_str("proxy"), get_str("source_proxy"), get_str("target_proxy"));
    let (sp, tp) = if !proxy.is_empty() {
        if !src_proxy.is_empty() {
            return Err(bad_request("`proxy` is mutually exclusive with `source_proxy`"));
        }
        if !tgt_proxy.is_empty() {
            return Err(bad_request("`proxy` is mutually exclusive with `target_proxy`"));
        }
        (proxy.clone(), proxy)
    } else {
        (src_proxy, tgt_proxy)
    };
    let options = make_options(doc, server)?;
    let placeholder = Endpoint {
        url: String::new(),
        basic: None,
        headers: vec![],
        auth_props: Map::new(),
        timeout_ms: options.connection_timeout,
        http_connections: options.http_connections,
        retries: options.retries,
        proxy: None,
        tls: Tls::default(),
        session_auth: false,
        session_refresh_secs: 550,
    };
    let mut rep = Rep {
        id: (String::new(), String::new()),
        source: placeholder.clone(),
        target: placeholder,
        options,
        user_ctx,
        view: None,
        doc_id: doc.get("_id").and_then(Value::as_str).map(String::from),
        db_name: None,
        start_time: std::time::SystemTime::now(),
    };
    if rep.options.cancel && rep.options.id.is_some() {
        return Ok(rep);
    }
    rep.source = parse_endpoint(doc.get("source").unwrap_or(&Value::Null), &sp, &rep.options, server)?;
    rep.target = parse_endpoint(doc.get("target").unwrap_or(&Value::Null), &tp, &rep.options, server)?;
    if doc.get("filter").and_then(Value::as_str) == Some("_view") {
        let view = rep.options.query_params.as_ref().and_then(|q| q.get("view")).and_then(Value::as_str).unwrap_or("");
        match view.split('/').collect::<Vec<_>>().as_slice() {
            [d, v] => rep.view = Some((format!("_design/{d}"), v.to_string())),
            _ => return Err(bad_request("Invalid `view` parameter.")),
        }
    }
    filter(&rep.options)?;
    Ok(rep)
}

/// parse_rep_doc/2: the doc, checked, with its replication id.
pub async fn parse_rep_doc(doc: &Map<String, Value>, user_ctx: UserCtx, server: &Server) -> Result<Rep, RepError> {
    let mut rep = parse_without_id(doc, user_ctx, server)?;
    if rep.options.cancel && rep.options.id.is_some() {
        rep.id = rep.options.id.clone().unwrap_or_default();
        return Ok(rep);
    }
    rep.id = replication_id(&rep, server).await?;
    Ok(rep)
}

/// The user a doc in a replicator db runs as (rep_user_ctx/1).
pub fn doc_user_ctx(doc: &Map<String, Value>) -> UserCtx {
    user_ctx_of(doc)
}

// ------------------------------------------------------ replication ids

/// An Erlang term, enough of them for replication ids.
#[derive(Clone, Debug)]
pub enum Term {
    Atom(&'static str),
    /// A list of bytes, as Erlang strings are.
    Str(Vec<u8>),
    Bin(Vec<u8>),
    Int(i128),
    Float(f64),
    List(Vec<Term>),
    Tuple(Vec<Term>),
    /// A map, its pairs in key order (as small maps are).
    Map(Vec<(Term, Term)>),
}

impl Term {
    /// term_to_binary(T, [{minor_version, 1}]).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![131];
        self.encode_into(&mut out);
        out
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Term::Atom(a) => {
                out.push(100);
                out.extend_from_slice(&(a.len() as u16).to_be_bytes());
                out.extend_from_slice(a.as_bytes());
            }
            Term::Str(s) if s.is_empty() => out.push(106),
            Term::Str(s) if s.len() <= 65535 => {
                out.push(107);
                out.extend_from_slice(&(s.len() as u16).to_be_bytes());
                out.extend_from_slice(s);
            }
            Term::Str(s) => Term::List(s.iter().map(|b| Term::Int(*b as i128)).collect()).encode_into(out),
            Term::Bin(b) => {
                out.push(109);
                out.extend_from_slice(&(b.len() as u32).to_be_bytes());
                out.extend_from_slice(b);
            }
            Term::Int(i) if (0..=255).contains(i) => {
                out.push(97);
                out.push(*i as u8);
            }
            Term::Int(i) if *i >= i32::MIN as i128 && *i <= i32::MAX as i128 => {
                out.push(98);
                out.extend_from_slice(&(*i as i32).to_be_bytes());
            }
            Term::Int(i) => {
                let mut mag = i.unsigned_abs();
                let mut digits = Vec::new();
                while mag > 0 {
                    digits.push((mag & 0xff) as u8);
                    mag >>= 8;
                }
                out.push(110);
                out.push(digits.len() as u8);
                out.push(if *i < 0 { 1 } else { 0 });
                out.extend_from_slice(&digits);
            }
            Term::Float(f) => {
                out.push(70);
                out.extend_from_slice(&f.to_bits().to_be_bytes());
            }
            Term::List(l) if l.is_empty() => out.push(106),
            // A list of bytes is written as a string.
            Term::List(l) if l.len() <= 65535 && l.iter().all(|t| matches!(t, Term::Int(i) if (0..=255).contains(i))) => {
                out.push(107);
                out.extend_from_slice(&(l.len() as u16).to_be_bytes());
                out.extend(l.iter().map(|t| if let Term::Int(i) = t { *i as u8 } else { 0 }));
            }
            Term::List(l) => {
                out.push(108);
                out.extend_from_slice(&(l.len() as u32).to_be_bytes());
                for t in l {
                    t.encode_into(out);
                }
                out.push(106);
            }
            Term::Map(pairs) => {
                out.push(116);
                out.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
                for (k, v) in pairs {
                    k.encode_into(out);
                    v.encode_into(out);
                }
            }
            Term::Tuple(t) => {
                if t.len() <= 255 {
                    out.push(104);
                    out.push(t.len() as u8);
                } else {
                    out.push(105);
                    out.extend_from_slice(&(t.len() as u32).to_be_bytes());
                }
                for x in t {
                    x.encode_into(out);
                }
            }
        }
    }

    fn str(s: &str) -> Term {
        Term::Str(s.as_bytes().to_vec())
    }
}

/// JSON as jiffy decodes it: objects {[{K, V}]}, strings binaries.
pub fn ejson(v: &Value) -> Term {
    match v {
        Value::Null => Term::Atom("null"),
        Value::Bool(true) => Term::Atom("true"),
        Value::Bool(false) => Term::Atom("false"),
        Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => Term::Int(i as i128),
            (None, Some(u)) => Term::Int(u as i128),
            _ => Term::Float(n.as_f64().unwrap_or(0.0)),
        },
        Value::String(s) => Term::Bin(s.as_bytes().to_vec()),
        Value::Array(a) => Term::List(a.iter().map(ejson).collect()),
        Value::Object(o) => Term::Tuple(vec![Term::List(
            o.iter().map(|(k, v)| Term::Tuple(vec![Term::Bin(k.as_bytes().to_vec()), ejson(v)])).collect(),
        )]),
    }
}

fn headers_term(h: &[(String, String)]) -> Term {
    Term::List(h.iter().map(|(k, v)| Term::Tuple(vec![Term::str(k), Term::str(v)])).collect())
}

/// get_v4_endpoint/1: {remote, User, Host, NonDefaultPort, Path, Headers, undefined}.
fn v4_endpoint(e: &Endpoint) -> Term {
    let user = match &e.basic {
        Some((u, _)) => Term::str(u),
        None => Term::Atom("undefined"),
    };
    match parse_url(&e.url) {
        Some(u) => {
            let port = match (u.scheme.as_str(), u.port) {
                ("https", 443) | ("http", 80) | ("http", 5984) => Term::Atom("default"),
                (_, p) => Term::Int(p as i128),
            };
            Term::Tuple(vec![
                Term::Atom("remote"),
                user,
                Term::str(&u.host),
                port,
                Term::str(&u.path),
                headers_term(&e.user_headers()),
                Term::Atom("undefined"),
            ])
        }
        None => Term::Tuple(vec![
            Term::Atom("remote"),
            user,
            Term::str(&e.url),
            Term::Atom("undefined"),
            Term::Atom("undefined"),
            headers_term(&e.user_headers()),
            Term::Atom("undefined"),
        ]),
    }
}

/// get_rep_endpoint/1, as version 3 ids use it: {remote, Url, Headers}.
fn v3_endpoint(e: &Endpoint) -> Term {
    Term::Tuple(vec![Term::Atom("remote"), Term::str(&e.url), headers_term(&e.user_headers())])
}

/// The filter's part of an id, which for a design doc filter means its
/// code, fetched from the source.
async fn filter_terms(rep: &Rep) -> Result<Vec<Term>, RepError> {
    Ok(match filter(&rep.options)? {
        Filter::None => vec![],
        Filter::View(f, qp) => vec![Term::Bin(f.into_bytes()), ejson(&Value::Object(qp))],
        Filter::User(ddoc, name, qp) => {
            let code = super::client::fetch_filter(&rep.source, &ddoc, &name).await?;
            vec![Term::Bin(code.into_bytes()), ejson(&Value::Object(qp))]
        }
        Filter::DocIds(ids) => vec![Term::List(ids.into_iter().map(|i| Term::Bin(i.into_bytes())).collect())],
        Filter::Mango(sel) => vec![ejson(&sel)],
    })
}

/// The base replication id at a version (4 or 3).
pub async fn replication_id_version(rep: &Rep, server: &Server, version: u64) -> Result<String, RepError> {
    let (src, tgt) = match version {
        3 => (v3_endpoint(&rep.source), v3_endpoint(&rep.target)),
        _ => (v4_endpoint(&rep.source), v4_endpoint(&rep.target)),
    };
    let mut base = vec![Term::Bin(server.uuid.as_bytes().to_vec()), src, tgt];
    base.extend(filter_terms(rep).await?);
    if rep.options.winning_revs_only {
        base.push(Term::Bin(b"winning_revs_only".to_vec()));
    }
    if let Some(s) = &rep.options.since_seq {
        base.push(ejson(s));
    }
    use md5::Digest;
    Ok(hex::encode(md5::Md5::digest(Term::List(base).encode())))
}

/// replication_id/1: the base id and its extension (+continuous,
/// +create_target).
pub async fn replication_id(rep: &Rep, server: &Server) -> Result<(String, String), RepError> {
    let base = replication_id_version(rep, server, REP_ID_VERSION).await?;
    let mut ext = String::new();
    if rep.options.continuous {
        ext.push_str("+continuous");
    }
    if rep.options.create_target {
        ext.push_str("+create_target");
    }
    Ok((base, ext))
}

/// The filter code as CouchDB keeps it: a single line loses its
/// surrounding spaces (re:replace with "^ *(.*?) *$" — "\s" in an Erlang
/// string is a space).
pub fn trim_filter_code(code: &str) -> String {
    let (body, tail) = match code.strip_suffix('\n') {
        Some(b) => (b, "\n"),
        None => (code, ""),
    };
    if body.contains('\n') {
        return code.to_string();
    }
    format!("{}{tail}", body.trim_matches(' '))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn md5hex(t: &Term) -> String {
        use md5::Digest;
        hex::encode(md5::Md5::digest(t.encode()))
    }

    #[test]
    fn term_encoding_matches_erlang() {
        // Encodings from term_to_binary(T, [{minor_version, 1}]) on OTP 27.
        assert_eq!(Term::Atom("foo").encode(), vec![131, 100, 0, 3, 102, 111, 111]);
        assert_eq!(Term::List(vec![]).encode(), vec![131, 106]);
        let t = Term::Tuple(vec![
            Term::Atom("remote"),
            Term::str("u"),
            Term::Atom("default"),
            Term::Bin(b"ab".to_vec()),
            Term::List(vec![Term::Tuple(vec![Term::str("h"), Term::str("v")])]),
            Term::Atom("undefined"),
            Term::Float(1.5),
            Term::Int(300),
            Term::Int(-5),
            Term::Int(70000000000),
        ]);
        let want: Vec<u8> = vec![
            131, 104, 10, 100, 0, 6, 114, 101, 109, 111, 116, 101, 107, 0, 1, 117, 100, 0, 7, 100, 101, 102, 97, 117, 108, 116,
            109, 0, 0, 0, 2, 97, 98, 108, 0, 0, 0, 1, 104, 2, 107, 0, 1, 104, 107, 0, 1, 118, 106, 100, 0, 9, 117, 110, 100, 101,
            102, 105, 110, 101, 100, 70, 63, 248, 0, 0, 0, 0, 0, 0, 98, 0, 0, 1, 44, 98, 255, 255, 255, 251, 110, 5, 0, 0, 60, 83,
            76, 16,
        ];
        assert_eq!(t.encode(), want);
        assert_eq!(
            ejson(&json!({"a": true})).encode(),
            vec![131, 104, 1, 108, 0, 0, 0, 1, 104, 2, 109, 0, 0, 0, 1, 97, 100, 0, 4, 116, 114, 117, 101, 106]
        );
    }

    #[test]
    fn replication_ids_match_couchdb() {
        // Ids CouchDB 3.5.3 gave for these replications: a local source and
        // target as admin, so user "admin" and an empty cookie header.
        let uuid = Term::Bin(b"6c5c5e7a1f0b4a3a9c2f0e1d2c3b4a59".to_vec());
        let ep = |db: &str| Endpoint {
            url: format!("http://127.0.0.1:15984/{db}/"),
            basic: Some(("admin".into(), "bench".into())),
            headers: vec![("Accept".into(), "application/json".into()), ("User-Agent".into(), user_agent()), ("cookie".into(), "".into())],
            auth_props: Map::new(),
            timeout_ms: 30000,
            http_connections: 20,
            retries: 5,
            proxy: None,
            tls: Tls::default(),
            session_auth: false,
            session_refresh_secs: 550,
        };
        let id = |a: &str, b: &str| md5hex(&Term::List(vec![Term::Bin(b"6c5c5e7a1f0b4a3a9c2f0e1d2c3b4a59".to_vec()), v4_endpoint(&ep(a)), v4_endpoint(&ep(b))]));
        let _ = uuid;
        assert_eq!(id("rp_a", "rp_b"), "ce14ac47895c6cd59c57e1b636c08ca8");
        assert_eq!(id("rp_a", "rp_c"), "80be3a1f83b0e40045330297d1a44780");
    }

    #[test]
    fn urls_and_ids() {
        let u = parse_url("https://bob:pw@example.com/db/x?y=1").unwrap();
        assert_eq!((u.scheme.as_str(), u.userinfo.as_deref(), u.host.as_str(), u.port, u.path.as_str()), ("https", Some("bob:pw"), "example.com", 443, "/db/x?y=1"));
        assert_eq!(strip_userinfo("http://a:b@h:1/db/"), "http://h:1/db/");
        assert_eq!(url_strip_password("http://a:b@h:1/db/"), "http://a:*****@h:1/db/");
        assert_eq!(convert_id("abc+continuous"), ("abc".into(), "+continuous".into()));
        assert_eq!(convert_id("abc continuous"), ("abc".into(), "+continuous".into()));
        assert_eq!(quote_plus("ftp://x/y z"), "ftp%3A%2F%2Fx%2Fy+z");
        assert_eq!(unquote("a%2Fb+c"), "a/b c");
        assert_eq!(trim_filter_code("  function(d){} \n"), "function(d){}\n");
        assert_eq!(trim_filter_code(" function(d){\n} "), " function(d){\n} ");
    }
}
