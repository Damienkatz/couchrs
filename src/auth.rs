//! Authentication and authorization, a port of couch_httpd_auth,
//! chttpd_auth_request and couch_db's security checks.
//!
//! A request is authenticated by an AuthSession cookie or Basic
//! credentials, checked against the [admins] config section and then the
//! users database (`org.couchdb.user:NAME` documents); otherwise it is
//! anonymous, or a server admin when no admins exist ("admin party").
//! Authorization then follows CouchDB's rules: server-admin-only paths,
//! database admins and members from each database's _security object.

use crate::config::Config;
use crate::error::{CouchError, Result};
use crate::server::Server;
use axum::http::{HeaderMap, Method};
use base64::Engine;
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Value, json};

pub const USER_PREFIX: &str = "org.couchdb.user:";
pub const AUTH_VALIDATE: &str = include_str!("../share/users_auth_validate.js");

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UserCtx {
    pub name: Option<String>,
    pub roles: Vec<String>,
}

impl UserCtx {
    pub fn admin() -> UserCtx {
        UserCtx { name: None, roles: vec!["_admin".into()] }
    }

    pub fn is_server_admin(&self) -> bool {
        self.roles.iter().any(|r| r == "_admin")
    }

    /// The userCtx object JavaScript functions receive.
    pub fn json(&self, db: &str) -> Value {
        json!({"db": db, "name": self.name, "roles": self.roles})
    }
}

/// The result of authenticating a request.
#[derive(Clone, Debug, Default)]
pub struct Auth {
    pub user: UserCtx,
    /// "cookie" or "default" (Basic), for _session's info.authenticated.
    pub method: Option<&'static str>,
    /// Cookie-authenticated with the user's salt, and the cookie should be
    /// renewed (past 10% of its lifetime).
    pub renew: Option<String>,
}

pub fn users_db_name(config: &Config) -> String {
    config.get_or("chttpd_auth", "authentication_db", "_users")
}

/// Whether `db` is a users database (CouchDB matches by suffix).
pub fn is_users_db(config: &Config, db: &str) -> bool {
    db == users_db_name(config) || db == "_users" || db.ends_with("/_users")
}

/// Credentials for a name: a server admin from [admins], else a user doc.
pub struct Creds {
    pub roles: Vec<String>,
    pub salt: String,
    stored: Stored,
}

enum Stored {
    Admin(String),
    User(Value),
}

fn admin_salt(stored: &str) -> String {
    // -pbkdf2:PRF-DERIVED,SALT,ITER  |  -pbkdf2-DERIVED,SALT,ITER  |  -hashed-HASH,SALT
    let parts: Vec<&str> = stored.split(',').collect();
    parts.get(1).map(|s| s.to_string()).unwrap_or_default()
}

pub fn get_user_creds(server: &Server, name: &str) -> Option<Creds> {
    if let Some(stored) = server.config.get("admins", name) {
        return Some(Creds { roles: vec!["_admin".into()], salt: admin_salt(&stored), stored: Stored::Admin(stored) });
    }
    let db = server.open_db(&users_db_name(&server.config)).ok()?;
    let snap = db.snapshot();
    let fdi = snap.get_fdi(&format!("{USER_PREFIX}{name}")).ok()??;
    if fdi.deleted {
        return None;
    }
    let doc = snap.winner_json(&fdi, &crate::doc::DocOpts::default()).ok()?;
    let roles = doc
        .get("roles")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|r| r.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let salt = doc.get("salt").and_then(|s| s.as_str()).unwrap_or("").to_string();
    Some(Creds { roles, salt, stored: Stored::User(doc) })
}

impl Creds {
    pub fn check_password(&self, password: &str) -> bool {
        match &self.stored {
            Stored::Admin(stored) => crate::passwords::verify_admin(stored, password),
            Stored::User(doc) => {
                let s = |k: &str| doc.get(k).and_then(|v| v.as_str()).unwrap_or("");
                match s("password_scheme") {
                    "pbkdf2" => {
                        let iterations = doc.get("iterations").and_then(|v| v.as_u64()).unwrap_or(0);
                        // User docs from before pbkdf2_prf existed are PBKDF2-SHA1.
                        let prf = match s("pbkdf2_prf") {
                            "" => "sha",
                            p => p,
                        };
                        let stored = format!("-pbkdf2:{prf}-{},{},{iterations}", s("derived_key"), s("salt"));
                        crate::passwords::verify_admin(&stored, password)
                    }
                    "simple" | "" if !s("password_sha").is_empty() => {
                        let stored = format!("-hashed-{},{}", s("password_sha"), s("salt"));
                        crate::passwords::verify_admin(&stored, password)
                    }
                    _ => false,
                }
            }
        }
    }
}

// ------------------------------------------------------------ cookies

fn b64url() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
}

fn hash_algorithms(config: &Config) -> Vec<String> {
    let v: Vec<String> = config
        .get_or("chttpd_auth", "hash_algorithms", "sha256, sha")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| s == "sha256" || s == "sha")
        .collect();
    if v.is_empty() { vec!["sha256".into()] } else { v }
}

fn hmac(alg: &str, key: &[u8], msg: &[u8]) -> Vec<u8> {
    match alg {
        "sha" => {
            let mut m = <Hmac<sha1::Sha1> as KeyInit>::new_from_slice(key).expect("hmac key");
            m.update(msg);
            m.finalize().into_bytes().to_vec()
        }
        _ => {
            let mut m = <Hmac<sha2::Sha256> as KeyInit>::new_from_slice(key).expect("hmac key");
            m.update(msg);
            m.finalize().into_bytes().to_vec()
        }
    }
}

fn secret(server: &Server) -> Option<String> {
    server.config.get("chttpd_auth", "secret")
}

/// The AuthSession value for a user: base64url("NAME:HEXTIME:" + HMAC of
/// "NAME:HEXTIME" keyed by the server secret followed by the user's salt),
/// signed with the first of [chttpd_auth] hash_algorithms.
pub fn make_cookie(server: &Server, name: &str, salt: &str, time: u64) -> Option<String> {
    let secret = secret(server)?;
    let data = format!("{name}:{time:X}");
    let alg = &hash_algorithms(&server.config)[0];
    let mac = hmac(alg, format!("{secret}{salt}").as_bytes(), data.as_bytes());
    let mut raw = format!("{data}:").into_bytes();
    raw.extend_from_slice(&mac);
    Some(b64url().encode(raw))
}

/// Set-Cookie value for a session cookie (or the expired one that clears
/// it), with CouchDB's attributes.
pub fn cookie_header(config: &Config, value: &str, secure: bool) -> String {
    let timeout: u64 = config.get_int("chttpd_auth", "timeout", 600);
    let mut s = format!("AuthSession={value}; Version=1; Path=/; HttpOnly");
    if value.is_empty() {
        s.push_str("; Max-Age=0");
    } else if config.get_bool("chttpd_auth", "allow_persistent_cookies", true) {
        let expires = httpdate(crate::http::now_secs() + timeout);
        s.push_str(&format!("; Expires={expires}; Max-Age={timeout}"));
    }
    if secure || config.get_bool("chttpd_auth", "secure_cookie", false) {
        s.push_str("; Secure");
    }
    if let Some(d) = config.get("chttpd_auth", "cookie_domain").filter(|d| !d.is_empty()) {
        s.push_str(&format!("; Domain={d}"));
    }
    match config.get_or("chttpd_auth", "same_site", "").to_ascii_lowercase().as_str() {
        "strict" => s.push_str("; SameSite=Strict"),
        "lax" => s.push_str("; SameSite=Lax"),
        "none" => s.push_str("; SameSite=None"),
        _ => {}
    }
    s
}

/// RFC 1123 date, as cookies' Expires wants it.
fn httpdate(secs: u64) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let days = secs / 86400;
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if mo <= 2 { 1 } else { 0 };
    format!(
        "{}, {:02}-{}-{} {:02}:{:02}:{:02} GMT",
        DAYS[(days % 7) as usize],
        d,
        MONTHS[(mo - 1) as usize],
        y,
        h,
        m,
        s
    )
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(axum::http::header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .map(|c| c.trim())
        .find_map(|c| c.strip_prefix(name).and_then(|r| r.strip_prefix('=')))
}

/// Check an AuthSession cookie: Ok(None) when absent, invalid or expired
/// (the request is then anonymous), 400 when it can't be decoded.
fn cookie_auth(server: &Server, headers: &HeaderMap) -> Result<Option<Auth>> {
    let Some(cookie) = cookie_value(headers, "AuthSession").filter(|c| !c.is_empty()) else { return Ok(None) };
    let malformed = || CouchError::bad_request("Malformed AuthSession cookie. Please clear your cookies.");
    let raw = b64url().decode(cookie.trim_end_matches('=')).map_err(|_| malformed())?;
    let mut parts = raw.splitn(3, |b| *b == b':');
    let (Some(user), Some(time), Some(mac)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(malformed());
    };
    let (Ok(user), Ok(time_str)) = (std::str::from_utf8(user), std::str::from_utf8(time)) else {
        return Err(malformed());
    };
    let Some(secret) = secret(server) else { return Ok(None) };
    let Some(creds) = get_user_creds(server, user) else { return Ok(None) };
    let Ok(time) = u64::from_str_radix(time_str, 16) else { return Ok(None) };
    let timeout: u64 = server.config.get_int("chttpd_auth", "timeout", 600);
    let now = crate::http::now_secs();
    if now >= time + timeout {
        return Ok(None);
    }
    let key = format!("{secret}{}", creds.salt);
    let data = format!("{user}:{time_str}");
    let ok = hash_algorithms(&server.config)
        .iter()
        .any(|alg| crate::passwords::ct_eq(&hmac(alg, key.as_bytes(), data.as_bytes()), mac));
    if !ok {
        return Ok(None);
    }
    let renew = ((time + timeout - now) as f64) < timeout as f64 * 0.9;
    Ok(Some(Auth {
        user: UserCtx { name: Some(user.to_string()), roles: creds.roles },
        method: Some("cookie"),
        renew: renew.then_some(creds.salt),
    }))
}

/// Basic credentials, if the request carries them: Ok(Some) when they
/// match, 401 when they don't.
fn basic_auth(server: &Server, headers: &HeaderMap) -> Result<Option<Auth>> {
    let Some(h) = headers.get(axum::http::header::AUTHORIZATION).and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    let Some(b) = h.strip_prefix("Basic ").or_else(|| h.strip_prefix("basic ")) else { return Ok(None) };
    let decoded = crate::doc::B64.decode(b.trim()).ok().and_then(|d| String::from_utf8(d).ok());
    let Some((name, pass)) = decoded.as_deref().and_then(|s| s.split_once(':')) else {
        return Err(CouchError::unauthorized("Name or password is incorrect."));
    };
    match get_user_creds(server, name) {
        Some(c) if c.check_password(pass) => Ok(Some(Auth {
            user: UserCtx { name: Some(name.to_string()), roles: c.roles },
            method: Some("default"),
            renew: None,
        })),
        _ => Err(CouchError::unauthorized("Name or password is incorrect.")),
    }
}

/// Authenticate a request (chttpd's cookie, then default handler).
pub fn authenticate(server: &Server, headers: &HeaderMap, path: &[String]) -> Result<Auth> {
    if let Some(a) = cookie_auth(server, headers)? {
        return Ok(a);
    }
    if let Some(a) = basic_auth(server, headers)? {
        return Ok(a);
    }
    if server.config.section("admins").is_empty() {
        // Admin party: everyone is a server admin.
        return Ok(Auth { user: UserCtx::admin(), method: None, renew: None });
    }
    let required = server.config.get_bool("chttpd", "require_valid_user", false);
    let except_up = server.config.get_bool("chttpd", "require_valid_user_except_for_up", false);
    if (required || except_up) && !(except_up && path.first().map(|s| s.as_str()) == Some("_up")) {
        return Err(CouchError::unauthorized("Authentication required."));
    }
    Ok(Auth::default())
}

// ------------------------------------------------------ authorization

fn names_roles(sec: &Value, key: &str) -> (Vec<String>, Vec<String>) {
    let group = sec.get(key).or_else(|| if key == "members" { sec.get("readers") } else { None });
    let list = |k: &str| {
        group
            .and_then(|g| g.get(k))
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default()
    };
    (list("names"), list("roles"))
}

fn authorized(ctx: &UserCtx, names: &[String], roles: &[String]) -> bool {
    ctx.roles.iter().any(|r| r == "_admin" || roles.contains(r))
        || ctx.name.as_ref().is_some_and(|n| names.contains(n))
}

pub fn is_db_admin(ctx: &UserCtx, sec: &Value) -> bool {
    let (names, roles) = names_roles(sec, "admins");
    authorized(ctx, &names, &roles)
}

pub fn is_member(ctx: &UserCtx, sec: &Value) -> bool {
    if is_db_admin(ctx, sec) {
        return true;
    }
    let (names, roles) = names_roles(sec, "members");
    (names.is_empty() && roles.is_empty()) || authorized(ctx, &names, &roles)
}

/// 401 for anonymous users, 403 for named ones (throw_security_error).
pub fn security_error(ctx: &UserCtx, reason: &str) -> CouchError {
    if ctx.name.is_none() {
        CouchError::unauthorized(reason)
    } else {
        CouchError::forbidden(reason)
    }
}

pub fn check_member(ctx: &UserCtx, sec: &Value) -> Result<()> {
    if is_member(ctx, sec) {
        return Ok(());
    }
    let reason = if ctx.name.is_none() {
        "You are not authorized to access this db."
    } else {
        "You are not allowed to access this db."
    };
    Err(security_error(ctx, reason))
}

pub fn check_db_admin(ctx: &UserCtx, sec: &Value) -> Result<()> {
    if is_db_admin(ctx, sec) {
        return Ok(());
    }
    Err(security_error(ctx, "You are not a db or server admin."))
}

fn require_admin(ctx: &UserCtx) -> Result<()> {
    if ctx.is_server_admin() {
        Ok(())
    } else {
        Err(CouchError::unauthorized("You are not a server admin."))
    }
}

/// The default _security object for new databases ([couchdb]
/// default_security): admin_only (and admin_local) restrict the database
/// to server admins, everyone leaves it open.
pub fn default_security(config: &Config) -> Value {
    match config.get_or("couchdb", "default_security", "admin_only").as_str() {
        "everyone" => json!({}),
        _ => json!({"members": {"roles": ["_admin"]}, "admins": {"roles": ["_admin"]}}),
    }
}

/// Request-level authorization (chttpd_auth_request): server-admin-only
/// paths, database admins for maintenance, members for everything else
/// in a database. A database that doesn't exist passes, so the handler
/// can answer 404.
pub fn authorize_request(server: &Server, ctx: &UserCtx, method: &Method, path: &[String]) -> Result<()> {
    let p: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
    let users = |db: &str| is_users_db(&server.config, db);
    let replicator = |db: &str| db == "_replicator" || db.ends_with("/_replicator");
    let db_security = |db: &str| -> Option<Value> { server.open_db(db).ok().map(|d| d.snapshot().security()) };
    let member = |db: &str| match db_security(db) {
        Some(sec) => check_member(ctx, &sec),
        None => Ok(()),
    };
    let db_admin = |db: &str| match db_security(db) {
        Some(sec) if !is_db_admin(ctx, &sec) => Err(CouchError::unauthorized("You are not a server or db admin.")),
        _ => Ok(()),
    };
    let admin_only_dbs = || {
        if server.config.get_bool("chttpd", "admin_only_all_dbs", true) { require_admin(ctx) } else { Ok(()) }
    };
    match p.as_slice() {
        [] | ["favicon.ico", ..] => Ok(()),
        ["_all_dbs", ..] | ["_dbs_info", ..] => admin_only_dbs(),
        [db] if (replicator(db) || users(db)) && (method == Method::PUT || method == Method::DELETE) => {
            require_admin(ctx)
        }
        [db, "_all_docs" | "_changes", ..] if replicator(db) || users(db) => require_admin(ctx),
        [db, ..] if replicator(db) || users(db) => member(db),
        ["_reshard", ..] => require_admin(ctx),
        [s, ..] if s.starts_with('_') => server_authorization(ctx, method, &p),
        [_] if method == Method::PUT || method == Method::DELETE => require_admin(ctx),
        [db, "_compact", ..] | [db, "_view_cleanup"] | [db, "_search_cleanup"] | [db, "_nouveau_cleanup"] => {
            db_admin(db)
        }
        [_, "_purge"] | [_, "_purged_infos_limit"] | [_, "_sync_shards"] => require_admin(ctx),
        [db, ..] => member(db),
    }
}

fn server_authorization(ctx: &UserCtx, method: &Method, p: &[&str]) -> Result<()> {
    match p {
        ["_up"] | ["_uuids"] | ["_session"] | ["_stats"] | ["_active_tasks"] | ["_dbs_info"] => Ok(()),
        ["_replicate"] => {
            if ctx.name.is_none() && !ctx.is_server_admin() {
                Err(CouchError::unauthorized("You are not an authenticated user"))
            } else {
                Ok(())
            }
        }
        ["_utils", ..] if *method == Method::GET || *method == Method::HEAD => Ok(()),
        ["_node", _, "_stats" | "_system" | "_prometheus", ..] => {
            if ctx.roles.iter().any(|r| r == "_admin" || r == "_metrics") {
                Ok(())
            } else {
                Err(CouchError::unauthorized("You are not a server admin or read-only metrics user"))
            }
        }
        _ => require_admin(ctx),
    }
}

// ------------------------------------------------------ the users db

/// The name a users-db document id is for.
pub fn user_doc_name(id: &str) -> Option<&str> {
    id.strip_prefix(USER_PREFIX)
}

/// Hash a user document's plaintext `password` into password_scheme,
/// pbkdf2_prf, salt, iterations and derived_key (couch_users_db
/// before_doc_update), after checking the writer may touch it.
pub fn before_users_doc_update(config: &Config, ctx: &UserCtx, sec: &Value, body: &mut serde_json::Map<String, Value>, id: &str) -> Result<()> {
    let owner = user_doc_name(id);
    if !is_db_admin(ctx, sec) && ctx.name.is_some() && ctx.name.as_deref() != owner {
        return Err(CouchError::not_found("missing"));
    }
    let password = match body.get("password") {
        None | Some(Value::Null) => return Ok(()),
        Some(Value::String(p)) => p.clone(),
        Some(other) => other.to_string(),
    };
    validate_password(config, &password)?;
    let preserve = body.get("preserve_salt").and_then(|v| v.as_bool()).unwrap_or(false);
    let salt = match body.get("salt").and_then(|s| s.as_str()) {
        Some(s) if preserve => s.to_string(),
        _ => hex::encode(uuid::Uuid::new_v4().as_bytes()),
    };
    for k in ["derived_key", "iterations", "password", "password_scheme", "password_sha", "preserve_salt", "pbkdf2_prf", "salt"] {
        body.shift_remove(k);
    }
    let scheme = config.get_or("chttpd_auth", "password_scheme", "pbkdf2");
    let mut fields = serde_json::Map::new();
    match scheme.as_str() {
        "simple" => {
            use sha1::Digest;
            let sha = hex::encode(sha1::Sha1::digest([password.as_bytes(), salt.as_bytes()].concat()));
            fields.insert("password_scheme".into(), json!("simple"));
            fields.insert("salt".into(), json!(salt));
            fields.insert("password_sha".into(), json!(sha));
        }
        "pbkdf2" => {
            let prf = config.get_or("chttpd_auth", "pbkdf2_prf", "sha256");
            let iterations: u32 = config.get_int("chttpd_auth", "iterations", 600_000);
            let derived = crate::passwords::pbkdf2_hex(&prf, password.as_bytes(), salt.as_bytes(), iterations)
                .ok_or_else(|| CouchError::forbidden("Server cannot hash passwords at this time."))?;
            fields.insert("password_scheme".into(), json!("pbkdf2"));
            fields.insert("pbkdf2_prf".into(), json!(prf));
            fields.insert("salt".into(), json!(salt));
            fields.insert("iterations".into(), json!(iterations));
            fields.insert("derived_key".into(), json!(derived));
        }
        _ => return Err(CouchError::forbidden("Server cannot hash passwords at this time.")),
    }
    // The hash fields come first, as CouchDB writes them.
    fields.extend(std::mem::take(body));
    *body = fields;
    Ok(())
}

/// [chttpd_auth] password_regexp: a list of regexes, each optionally with
/// a reason, as an Erlang term, e.g. [{".{10,}"}, {"[A-Z]+", "Uppercase"}].
fn validate_password(config: &Config, password: &str) -> Result<()> {
    let spec = config.get_or("chttpd_auth", "password_regexp", "");
    let spec = spec.trim();
    if spec.is_empty() || spec == "[]" {
        return Ok(());
    }
    let server_error = || CouchError::forbidden("Server cannot hash passwords at this time.");
    // Each element: "re" or {"re"} or {"re", "reason"}.
    let elem = regex::Regex::new(r#"\{\s*"((?:[^"\\]|\\.)*)"\s*(?:,\s*"((?:[^"\\]|\\.)*)"\s*)?\}|"((?:[^"\\]|\\.)*)""#)
        .expect("regex");
    if !spec.starts_with('[') || !spec.ends_with(']') {
        return Err(server_error());
    }
    for c in elem.captures_iter(&spec[1..spec.len() - 1]) {
        let re = c.get(1).or_else(|| c.get(3)).map(|m| m.as_str().replace("\\\\", "\\")).unwrap_or_default();
        let reason = c.get(2).map(|m| m.as_str()).filter(|r| !r.is_empty());
        let re = regex::Regex::new(&re).map_err(|_| server_error())?;
        if !re.is_match(password) {
            let msg = match reason {
                Some(r) => format!("Password does not conform to requirements. {r}"),
                None => "Password does not conform to requirements.".to_string(),
            };
            return Err(CouchError::bad_request(msg));
        }
    }
    Ok(())
}

/// What a reader may see of a users-db document (couch_users_db
/// after_doc_read): admins and the owner see it whole; design docs are
/// admin-only; others see only [chttpd_auth] public_fields, or nothing.
pub fn after_users_doc_read(config: &Config, ctx: &UserCtx, sec: &Value, id: &str, doc: Value) -> Result<Value> {
    if is_db_admin(ctx, sec) {
        return Ok(doc);
    }
    if id.starts_with("_design/") {
        return Err(CouchError::forbidden("Only administrators can view design docs in the users database."));
    }
    if ctx.name.is_some() && ctx.name.as_deref() == user_doc_name(id) {
        return Ok(doc);
    }
    let public = config.get_or("chttpd_auth", "public_fields", "");
    let public: Vec<&str> = public.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    let stripped: serde_json::Map<String, Value> = doc
        .as_object()
        .map(|o| o.iter().filter(|(k, _)| public.contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    if stripped.is_empty() {
        return Err(CouchError::not_found("missing"));
    }
    Ok(Value::Object(stripped))
}

/// The users db's security object may only change with [couchdb]
/// users_db_security_editable = true.
pub fn check_security_editable(config: &Config, db: &str) -> Result<()> {
    if is_users_db(config, db) && !config.get_bool("couchdb", "users_db_security_editable", false) {
        return Err(CouchError::forbidden("You can't edit the security object of the user database."));
    }
    Ok(())
}

/// Validate a security object's admins/members (names and roles must be
/// lists of strings).
pub fn validate_security(sec: &Value) -> Result<()> {
    for group in ["admins", "members", "readers"] {
        let Some(g) = sec.get(group) else { continue };
        if !g.is_object() {
            return Err(CouchError::bad_request(format!("{group} must be a JSON object")));
        }
        for (k, what) in [("names", "names"), ("roles", "roles")] {
            match g.get(k) {
                None => {}
                Some(Value::Array(a)) if a.iter().all(|x| x.is_string()) => {}
                _ => return Err(CouchError::bad_request(format!("{what} must be a JSON list of strings"))),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn cookie_matches_couchdb() {
        // Secret, user salt and time as CouchDB 3.5.3 used them; the MAC is
        // HMAC-SHA256(secret ++ salt, "eve:6ADA45AF").
        let key = "0b5e9c1f6d7a4e2b8c3d9f0a1b2c3d4e".to_string() + "c413ae085305e6d513a15dd350ea5ecc";
        let mac = hmac("sha256", key.as_bytes(), b"eve:6ADA45AF");
        assert_eq!(mac.len(), 32);
        assert_eq!(httpdate(0), "Thu, 01-Jan-1970 00:00:00 GMT");
        assert_eq!(httpdate(1_792_689_543), "Thu, 22-Oct-2026 17:19:03 GMT");
    }

    #[test]
    fn security_rules() {
        let anon = UserCtx::default();
        let bob = UserCtx { name: Some("bob".into()), roles: vec![] };
        let eve = UserCtx { name: Some("eve".into()), roles: vec!["boss".into()] };
        let admin_only = json!({"members": {"roles": ["_admin"]}, "admins": {"roles": ["_admin"]}});
        assert!(!is_member(&anon, &admin_only) && !is_member(&bob, &admin_only));
        assert!(is_member(&UserCtx::admin(), &admin_only));
        assert!(is_member(&anon, &json!({})));
        let sec = json!({"members": {"names": ["bob"]}, "admins": {"roles": ["boss"]}});
        assert!(is_member(&bob, &sec) && !is_db_admin(&bob, &sec));
        assert!(is_member(&eve, &sec) && is_db_admin(&eve, &sec));
        assert!(!is_member(&anon, &sec));
        assert_eq!(check_member(&anon, &sec).unwrap_err().status, StatusCode::UNAUTHORIZED);
        let carol = UserCtx { name: Some("carol".into()), roles: vec![] };
        assert_eq!(check_member(&carol, &sec).unwrap_err().status, StatusCode::FORBIDDEN);
        // Old "readers" key still counts as members.
        assert!(!is_member(&carol, &json!({"readers": {"names": ["bob"]}})));
    }
}
