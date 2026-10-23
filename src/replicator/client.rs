//! Talking to a replication endpoint (couch_replicator_httpc and
//! couch_replicator_api_wrap): requests with CouchDB's retry and backoff
//! rules, and the calls replication makes. Documents travel as JSON:
//! open_revs and _bulk_get with revs and latest, attachments inline (or
//! stubs, given atts_since), written back with new_edits=false.

use super::RepError;
use super::rep::{Endpoint, quote_plus, url_strip_password};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::Method;
use serde_json::{Map, Value, json};
use std::time::{Duration, Instant};

const MAX_WAIT: Duration = Duration::from_secs(5 * 60);
/// Half the scheduler's health threshold: a request failing for longer
/// gives up even with retries left.
const MAX_ERROR_TIME: Duration = Duration::from_secs(60);

/// Why a request failed for good ({http_request_failed, Method, Url, Error}).
#[derive(Debug, Clone)]
pub struct HttpFail {
    pub method: String,
    pub url: String,
    pub error: String,
    pub code: Option<u16>,
}

impl std::fmt::Display for HttpFail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            Some(c) => write!(f, "{{http_request_failed,\"{}\",\"{}\",{{error,{{code,{c}}}}}}}", self.method, self.url),
            None => write!(f, "{{http_request_failed,\"{}\",\"{}\",{{error,{}}}}}", self.method, self.url, self.error),
        }
    }
}

pub struct Resp {
    pub status: u16,
    pub body: Bytes,
}

impl Resp {
    pub fn json(&self) -> Value {
        if self.body.is_empty() { Value::Null } else { crate::json::parse(&self.body).unwrap_or(Value::Null) }
    }
}

pub struct Client {
    pub ep: Endpoint,
    http: reqwest::Client,
    /// Seconds to wait before the next request after 429 responses.
    rate: parking_lot::Mutex<Duration>,
    session: tokio::sync::Mutex<Session>,
}

/// Session cookie auth (couch_replicator_auth_session): log in at the
/// server's /_session and send its AuthSession cookie, refreshing it
/// before it expires and after auth failures; basic auth if the server
/// has no sessions.
#[derive(Default)]
struct Session {
    /// Not tried yet, in use, or given up on (basic auth instead).
    state: SessionState,
    cookie: String,
    epoch: u64,
    refreshed_at: u64,
    next_refresh: u64,
    require_valid_user: bool,
}

#[derive(Default, PartialEq, Clone, Copy)]
enum SessionState {
    #[default]
    Untried,
    Active,
    Off,
}

const MIN_SESSION_UPDATE_SECS: u64 = 5;

fn now_secs() -> u64 {
    crate::http::now_secs()
}

/// The AuthSession cookie and its Max-Age, from Set-Cookie headers.
fn auth_cookie(headers: &reqwest::header::HeaderMap) -> Option<(String, Option<u64>)> {
    for v in headers.get_all(reqwest::header::SET_COOKIE) {
        let v = v.to_str().ok()?;
        let mut parts = v.split(';').map(str::trim);
        let (k, val) = parts.next()?.split_once('=')?;
        if !k.eq_ignore_ascii_case("AuthSession") {
            continue;
        }
        let max_age = parts.filter_map(|p| p.split_once('=')).find(|(k, _)| k.eq_ignore_ascii_case("Max-Age")).and_then(|(_, v)| v.parse().ok());
        return Some((val.to_string(), max_age));
    }
    None
}

/// A URL path segment for a doc id (couch_util:encode_doc_id/1): design
/// and local prefixes stay as they are.
pub fn encode_doc_id(id: &str) -> String {
    let enc = |s: &str| percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string();
    for p in ["_design/", "_local/"] {
        if let Some(rest) = id.strip_prefix(p) {
            return format!("{p}{}", enc(rest));
        }
    }
    enc(id)
}

impl Client {
    pub fn new(ep: &Endpoint) -> Result<Client, RepError> {
        let mut b = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(ep.timeout_ms.max(1000)))
            .read_timeout(Duration::from_millis(ep.timeout_ms.max(1000) + 500))
            .pool_max_idle_per_host(ep.http_connections.max(1) as usize)
            .redirect(reqwest::redirect::Policy::limited(10));
        if let Some(p) = &ep.proxy {
            b = b.proxy(reqwest::Proxy::all(p).map_err(|_| RepError::Error("proxy has an invalid url".into()))?);
        }
        // Like CouchDB, certificates aren't checked unless asked for.
        if !ep.tls.verify {
            b = b.danger_accept_invalid_certs(true);
        }
        if let Some(f) = &ep.tls.ca_file {
            let pem = std::fs::read(f).map_err(|e| RepError::Error(format!("{f}: {e}")))?;
            for cert in reqwest::Certificate::from_pem_bundle(&pem).map_err(|e| RepError::Error(e.to_string()))? {
                b = b.add_root_certificate(cert);
            }
        }
        if let (Some(cert), Some(key)) = (&ep.tls.cert_file, &ep.tls.key_file) {
            let mut pem = std::fs::read(cert).map_err(|e| RepError::Error(format!("{cert}: {e}")))?;
            pem.extend(std::fs::read(key).map_err(|e| RepError::Error(format!("{key}: {e}")))?);
            b = b.identity(reqwest::Identity::from_pem(&pem).map_err(|e| RepError::Error(e.to_string()))?);
        }
        let http = b.build().map_err(|e| RepError::Error(e.to_string()))?;
        let session = Session { state: if ep.session_auth && ep.basic.is_some() { SessionState::Untried } else { SessionState::Off }, ..Default::default() };
        Ok(Client { ep: ep.clone(), http, rate: parking_lot::Mutex::new(Duration::ZERO), session: tokio::sync::Mutex::new(session) })
    }

    /// The server's /_session, resolved against the endpoint's URL.
    fn session_url(&self) -> String {
        match super::rep::parse_url(&self.ep.url) {
            Some(u) => {
                let host = if u.host.contains(':') { format!("[{}]", u.host) } else { u.host };
                format!("{}://{host}:{}/_session", u.scheme, u.port)
            }
            None => format!("{}_session", self.ep.url),
        }
    }

    /// Log in (refresh/1); false if the server can't do sessions.
    async fn refresh_session(&self, s: &mut Session) -> bool {
        let Some((user, pass)) = &self.ep.basic else { return false };
        for attempt in 0..2 {
            let body = form_urlencoded::Serializer::new(String::new()).append_pair("name", user).append_pair("password", pass).finish();
            let mut r = self
                .http
                .post(self.session_url())
                .header("User-Agent", super::rep::user_agent())
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(body);
            if s.require_valid_user {
                r = r.basic_auth(user, Some(pass));
            }
            let Ok(resp) = r.send().await else { return false };
            match resp.status().as_u16() {
                200 => match auth_cookie(resp.headers()) {
                    Some((c, max_age)) => {
                        self.update_cookie(s, c, max_age);
                        return true;
                    }
                    None => return false,
                },
                // A server requiring valid users wants basic auth to log in.
                401 if attempt == 0 && resp.headers().contains_key(reqwest::header::WWW_AUTHENTICATE) => s.require_valid_user = true,
                _ => return false,
            }
        }
        false
    }

    fn update_cookie(&self, s: &mut Session, cookie: String, max_age: Option<u64>) {
        if cookie == s.cookie {
            return;
        }
        let now = now_secs();
        s.cookie = cookie;
        s.epoch += 1;
        s.refreshed_at = now;
        s.state = SessionState::Active;
        let next = match max_age {
            Some(a) => now + a * 9 / 10,
            None => now + self.ep.session_refresh_secs,
        };
        s.next_refresh = if s.next_refresh == 0 { next } else { s.next_refresh.min(next) };
    }

    /// The cookie to send (logging in or refreshing first if due), and
    /// its epoch; None for basic auth.
    async fn session_cookie(&self) -> Option<(String, u64)> {
        let mut s = self.session.lock().await;
        match s.state {
            SessionState::Off => return None,
            SessionState::Untried => {
                if !self.refresh_session(&mut s).await {
                    s.state = SessionState::Off;
                    return None;
                }
            }
            SessionState::Active if now_secs() >= s.next_refresh => {
                s.next_refresh = u64::MAX;
                self.refresh_session(&mut s).await;
            }
            SessionState::Active => {}
        }
        Some((s.cookie.clone(), s.epoch))
    }

    /// After a response: pick up a renewed cookie, and on 401/403 decide
    /// whether to log in again and retry (process_response/4).
    async fn session_response(&self, epoch: u64, resp: &reqwest::Response) -> bool {
        let mut s = self.session.lock().await;
        if s.state != SessionState::Active {
            return false;
        }
        let code = resp.status().as_u16();
        if code == 401 || code == 403 {
            if s.epoch > epoch {
                return true;
            }
            let now = now_secs();
            if now.saturating_sub(s.refreshed_at) < MIN_SESSION_UPDATE_SECS {
                s.next_refresh = s.next_refresh.min(now + MIN_SESSION_UPDATE_SECS);
                return false;
            }
            s.next_refresh = now;
            return true;
        }
        if (200..300).contains(&code)
            && let Some((c, max_age)) = auth_cookie(resp.headers())
        {
            self.update_cookie(&mut s, c, max_age);
        }
        false
    }

    /// full_url/2: the base, a path, and quoted query arguments.
    pub fn url(&self, path: &str, qs: &[(String, String)]) -> String {
        let mut u = format!("{}{path}", self.ep.url);
        for (i, (k, v)) in qs.iter().enumerate() {
            u.push(if i == 0 { '?' } else { '&' });
            u.push_str(k);
            u.push('=');
            u.push_str(&quote_plus(v));
        }
        u
    }

    fn request(&self, method: &Method, url: &str, headers: &[(&str, String)], body: Option<&Bytes>, cookie: Option<&str>) -> reqwest::RequestBuilder {
        let mut r = self.http.request(method.clone(), url);
        for (k, v) in &self.ep.headers {
            if !v.is_empty() && !headers.iter().any(|(h, _)| h.eq_ignore_ascii_case(k)) {
                r = r.header(k.as_str(), v.as_str());
            }
        }
        for (k, v) in headers {
            r = r.header(*k, v.as_str());
        }
        match (cookie, &self.ep.basic) {
            (Some(c), _) => r = r.header("Cookie", format!("AuthSession={c}")),
            (None, Some((u, p))) => r = r.basic_auth(u, Some(p)),
            (None, None) => {}
        }
        if let Some(b) = body {
            r = r.body(b.clone());
        }
        r
    }

    /// Send with retries: connection errors, timeouts, 408 and 5xx are
    /// retried `retries` times (waits doubling from 250ms, up to 5
    /// minutes); 429 backs off without using up retries.
    pub async fn send(
        &self,
        method: Method,
        path: &str,
        qs: &[(String, String)],
        headers: &[(&str, String)],
        body: Option<Bytes>,
        retries: Option<u64>,
    ) -> Result<Resp, HttpFail> {
        let resp = self.send_streaming(method.clone(), path, qs, headers, body, retries).await?;
        let status = resp.status().as_u16();
        let url = url_strip_password(&self.url(path, qs));
        let body = resp.bytes().await.map_err(|e| HttpFail { method: method.to_string(), url, error: e.to_string(), code: None })?;
        Ok(Resp { status, body })
    }

    /// As `send`, but the body is the caller's to read.
    pub async fn send_streaming(
        &self,
        method: Method,
        path: &str,
        qs: &[(String, String)],
        headers: &[(&str, String)],
        body: Option<Bytes>,
        retries: Option<u64>,
    ) -> Result<reqwest::Response, HttpFail> {
        let url = self.url(path, qs);
        let mut retries = retries.unwrap_or(self.ep.retries);
        let mut wait = Duration::from_millis(250);
        let mut first_error: Option<Instant> = None;
        let mut session_retried = false;
        loop {
            crate::stat!("couch_replicator.requests").inc();
            let delay = *self.rate.lock();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let fail = |error: String, code: Option<u16>| HttpFail { method: method.to_string(), url: url_strip_password(&url), error, code };
            let session = self.session_cookie().await;
            let sent = self.request(&method, &url, headers, body.as_ref(), session.as_ref().map(|(c, _)| c.as_str())).send().await;
            if let (Ok(r), Some((_, epoch))) = (&sent, &session)
                && self.session_response(*epoch, r).await
                && !session_retried
            {
                // Logged in again: the same request, once, with the new cookie.
                session_retried = true;
                continue;
            }
            let err = match sent {
                Ok(r) if r.status().as_u16() == 429 => {
                    let mut d = self.rate.lock();
                    *d = (*d * 2).clamp(Duration::from_millis(25), Duration::from_secs(25));
                    continue;
                }
                Ok(r) if r.status().as_u16() == 408 || r.status().as_u16() >= 500 => {
                    crate::stat!("couch_replicator.responses.failure").inc();
                    fail(format!("{{code,{}}}", r.status().as_u16()), Some(r.status().as_u16()))
                }
                Ok(r) => {
                    crate::stat!("couch_replicator.responses.success").inc();
                    let mut d = self.rate.lock();
                    *d = if *d <= Duration::from_millis(25) { Duration::ZERO } else { *d / 2 };
                    return Ok(r);
                }
                Err(e) if e.is_timeout() => fail("timeout".into(), None),
                Err(e) if e.is_connect() => fail(format!("{{conn_failed,{e}}}"), None),
                Err(e) => fail(e.to_string(), None),
            };
            let started = *first_error.get_or_insert_with(Instant::now);
            if retries == 0 || started.elapsed() > MAX_ERROR_TIME {
                tracing::error!("Replicator, request {} to {:?} failed due to error {}", err.method, err.url, err.error);
                return Err(err);
            }
            tracing::info!("Retrying {} request to {} in {} seconds due to error {}", err.method, err.url, wait.as_secs_f64(), err.error);
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(MAX_WAIT);
            retries -= 1;
        }
    }

    pub async fn get(&self, path: &str, qs: &[(String, String)]) -> Result<Resp, HttpFail> {
        self.send(Method::GET, path, qs, &[], None, None).await
    }

    fn json_headers() -> [(&'static str, String); 1] {
        [("Content-Type", "application/json".to_string())]
    }

    /// db_open/3: the database exists (created first if asked), or why not.
    pub async fn db_open(&self, create: bool, params: &Map<String, Value>) -> Result<(), RepError> {
        let uri = self.ep.uri();
        let map_fail = |f: HttpFail| RepError::Http(f);
        if create {
            let qs: Vec<(String, String)> =
                params.iter().map(|(k, v)| (k.clone(), v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))).collect();
            let r = self.send(Method::PUT, "", &qs, &[], None, None).await.map_err(map_fail)?;
            match r.status {
                401 => return Err(RepError::Unauthorized(format!("unauthorized to access or create database {uri}"))),
                403 => return Err(RepError::Forbidden(uri)),
                _ => {}
            }
        }
        let r = self.get("", &[]).await.map_err(map_fail)?;
        match r.status {
            200 => {
                let v = r.json();
                if v.get("update_seq").is_none() || v.get("instance_start_time").is_none() {
                    return Err(RepError::DbNotFound(format!("could not open {uri}")));
                }
                Ok(())
            }
            401 => Err(RepError::Unauthorized(format!("unauthorized to access or create database {uri}"))),
            403 => Err(RepError::Forbidden(uri)),
            _ => Err(RepError::DbNotFound(format!("could not open {uri}"))),
        }
    }

    pub async fn db_info(&self) -> Result<Map<String, Value>, RepError> {
        let r = self.get("", &[]).await.map_err(RepError::Http)?;
        match r.json() {
            Value::Object(m) if r.status == 200 => Ok(m),
            _ => Err(RepError::DbNotFound(format!("could not open {}", self.ep.uri()))),
        }
    }

    /// Changes after a sequence (get_pending_count/2): for a number, from
    /// the db's update_seq; otherwise _changes' pending.
    pub async fn pending_count(&self, seq: &Value) -> Value {
        let r = if let Some(n) = seq.as_i64() {
            self.send(Method::GET, "", &[], &[], None, Some(3)).await.ok().map(|r| match r.json().get("update_seq") {
                Some(Value::Number(u)) => json!(u.as_i64().unwrap_or(0) - n),
                _ => Value::Null,
            })
        } else {
            let qs = vec![("since".to_string(), seq_encode(seq)), ("limit".to_string(), "0".to_string())];
            self.send(Method::GET, "_changes", &qs, &[], None, Some(3)).await.ok().map(|r| r.json().get("pending").cloned().unwrap_or(Value::Null))
        };
        r.unwrap_or(Value::Null)
    }

    /// ensure_full_commit/1: the instance_start_time, or the error.
    pub async fn ensure_full_commit(&self) -> Result<Value, String> {
        let r = self.send(Method::POST, "_ensure_full_commit", &[], &Self::json_headers(), Some(Bytes::new()), None).await.map_err(|e| e.to_string())?;
        let v = r.json();
        if r.status == 201 {
            Ok(v.get("instance_start_time").cloned().unwrap_or(Value::Null))
        } else {
            Err(v.get("error").and_then(Value::as_str).unwrap_or("unknown").to_string())
        }
    }

    /// _revs_diff: each id's missing revs and possible ancestors.
    pub async fn revs_diff(&self, id_revs: &[(String, Vec<String>)]) -> Result<Vec<(String, Vec<String>, Vec<String>)>, RepError> {
        let body: Map<String, Value> = id_revs.iter().map(|(id, revs)| (id.clone(), json!(revs))).collect();
        let r = self
            .send(Method::POST, "_revs_diff", &[], &Self::json_headers(), Some(Bytes::from(serde_json::to_vec(&body).unwrap_or_default())), None)
            .await
            .map_err(RepError::Http)?;
        if r.status != 200 {
            return Err(RepError::Other(format!("{{revs_diff_failed,{},{}}}", r.status, String::from_utf8_lossy(&r.body))));
        }
        let strs = |v: Option<&Value>| -> Vec<String> {
            v.and_then(Value::as_array).map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default()
        };
        Ok(r.json().as_object().map(|o| o.iter().map(|(id, res)| (id.clone(), strs(res.get("missing")), strs(res.get("possible_ancestors")))).collect()).unwrap_or_default())
    }

    /// _bulk_get with latest, revs and no attachments: each (id, rev)'s doc
    /// or error, in request order; None if the endpoint couldn't.
    pub async fn bulk_get(&self, reqs: &[(String, String, Vec<String>)]) -> Option<Vec<Result<Value, String>>> {
        let docs: Vec<Value> = reqs.iter().map(|(id, rev, pas)| json!({"id": id, "rev": rev, "atts_since": pas})).collect();
        let body = json!({"docs": docs, "latest": true, "revs": true, "attachments": false});
        let qs = vec![("latest".into(), "true".into()), ("revs".into(), "true".into()), ("attachments".into(), "false".into())];
        let headers = [("Content-Type", "application/json".to_string()), ("Accept", "application/json".to_string())];
        let r = self.send(Method::POST, "_bulk_get", &qs, &headers, Some(Bytes::from(body.to_string())), None).await.ok()?;
        if r.status != 200 {
            return None;
        }
        let v = r.json();
        let results = v.get("results")?.as_array()?;
        if results.len() != reqs.len() {
            return None;
        }
        Some(
            reqs.iter()
                .zip(results)
                .map(|((id, _, _), res)| match (res.get("id").and_then(Value::as_str), res.get("docs").and_then(Value::as_array)) {
                    (Some(rid), Some(d)) if rid == id && d.len() == 1 => match (d[0].get("ok"), d[0].get("error")) {
                        (Some(doc), _) if doc.is_object() => Ok(doc.clone()),
                        (_, Some(e)) => Err(e.get("error").and_then(Value::as_str).unwrap_or("error").to_string()),
                        _ => Err("unexpected_bulk_get_response".into()),
                    },
                    _ => Err("unexpected_bulk_get_response".into()),
                })
                .collect(),
        )
    }

    /// open_doc_revs/6: these revisions (or their latest descendants),
    /// with history, attachments since the given ancestors inline.
    pub async fn open_doc_revs(&self, id: &str, revs: &[String], atts_since: &[String]) -> Result<Vec<Value>, RepError> {
        let mut qs = vec![
            ("revs".to_string(), "true".to_string()),
            ("open_revs".to_string(), json!(revs).to_string()),
            ("latest".to_string(), "true".to_string()),
            ("attachments".to_string(), "true".to_string()),
        ];
        if !atts_since.is_empty() {
            qs.push(("atts_since".to_string(), json!(atts_since).to_string()));
        }
        let headers = [("Accept", "application/json".to_string())];
        let r = self.send(Method::GET, &encode_doc_id(id), &qs, &headers, None, None).await.map_err(RepError::Http)?;
        if r.status == 404 {
            return Err(RepError::MissingDoc);
        }
        if r.status != 200 {
            return Err(RepError::Other(format!("open_revs of {id} failed: {}", r.status)));
        }
        let mut out = Vec::new();
        for row in r.json().as_array().cloned().unwrap_or_default() {
            match (row.get("ok"), row.get("missing")) {
                (Some(d), _) => out.push(d.clone()),
                (_, Some(_)) => return Err(RepError::MissingDoc),
                _ => {}
            }
        }
        Ok(out)
    }

    /// open_doc/3, as JSON.
    pub async fn open_doc(&self, id: &str) -> Result<Value, Value> {
        match self.get(&encode_doc_id(id), &[]).await {
            Ok(r) if r.status == 200 => Ok(r.json()),
            Ok(r) => Err(r.json().get("error").cloned().unwrap_or(Value::Null)),
            Err(e) => Err(json!(e.to_string())),
        }
    }

    /// update_doc/4 of one replicated (or, for checkpoints, local) doc.
    pub async fn update_doc(&self, doc: &Value, replicated: bool) -> Result<String, DocWriteError> {
        let id = doc.get("_id").and_then(Value::as_str).unwrap_or_default();
        let qs = if replicated { vec![("new_edits".to_string(), "false".to_string())] } else { vec![] };
        let r = self
            .send(Method::PUT, &encode_doc_id(id), &qs, &Self::json_headers(), Some(Bytes::from(doc.to_string())), Some(0))
            .await
            .map_err(|e| DocWriteError::Other(e.to_string()))?;
        let v = r.json();
        let field = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").to_string();
        match r.status {
            200..=202 => Ok(field("rev")),
            409 => Err(DocWriteError::Conflict),
            401 if field("error") == "unauthorized" => Err(DocWriteError::Skip("unauthorized".into(), field("reason"))),
            403 if field("error") == "forbidden" => Err(DocWriteError::Skip("forbidden".into(), field("reason"))),
            412 if field("error") == "missing_stub" => Err(DocWriteError::MissingStub),
            413 => Err(DocWriteError::Skip("request_body_too_large".into(), String::new())),
            415 => Err(DocWriteError::Skip("unsupported_media_type".into(), String::new())),
            400 if field("error") == "bad_request" && field("reason").starts_with("Attachment name ") => {
                Err(DocWriteError::Skip("invalid_attachment_name".into(), field("reason")))
            }
            code => Err(DocWriteError::Other(format!("{{{},{}}}", field("error"), if field("reason").is_empty() { code.to_string() } else { field("reason") }))),
        }
    }

    /// update_docs/4 with new_edits=false: the docs that failed, as
    /// {id, rev, error, reason}; None if the body was too large.
    pub async fn bulk_docs(&self, docs: &[String]) -> Result<Option<Vec<Value>>, RepError> {
        let mut body = String::from("{\"new_edits\":false,\"docs\":[");
        body.push_str(&docs.join(","));
        body.push_str("]}");
        let r = self
            .send(Method::POST, "_bulk_docs", &[], &Self::json_headers(), Some(Bytes::from(body)), None)
            .await
            .map_err(RepError::Http)?;
        match r.status {
            201 | 417 => Ok(Some(
                r.json().as_array().cloned().unwrap_or_default().into_iter().filter(|x| x.get("error").is_some()).collect(),
            )),
            413 => Ok(None),
            code => Err(RepError::Other(format!("{{bulk_docs_failed,{code},{}}}", String::from_utf8_lossy(&r.body)))),
        }
    }

    /// Start a _changes feed request (changes_since/5's request); its
    /// errors are the caller's to retry.
    pub async fn changes(
        &self,
        continuous: bool,
        style: &str,
        since: &Value,
        filter: &super::rep::Filter,
        timeout_ms: u64,
    ) -> Result<reqwest::Response, HttpFail> {
        let mut qs = vec![
            ("feed".to_string(), if continuous { "continuous" } else { "normal" }.to_string()),
            ("style".to_string(), style.to_string()),
            ("since".to_string(), seq_encode(since)),
            ("timeout".to_string(), timeout_ms.to_string()),
        ];
        use super::rep::Filter;
        let (method, body) = match filter {
            Filter::Mango(sel) => {
                qs.insert(0, ("filter".into(), "_selector".into()));
                (Method::POST, Some(Bytes::from(json!({"selector": sel}).to_string())))
            }
            Filter::DocIds(ids) => {
                qs.insert(0, ("filter".into(), "_doc_ids".into()));
                (Method::POST, Some(Bytes::from(json!({"doc_ids": ids}).to_string())))
            }
            Filter::View(_, params) | Filter::User(_, _, params) => {
                let fname = name_of(filter);
                let mut extra = Vec::new();
                for (k, v) in params {
                    if qs.iter().any(|(q, _)| q == k) || extra.iter().any(|(q, _): &(String, String)| q == k) {
                        continue;
                    }
                    // _view filters take view arguments as JSON.
                    let s = match v {
                        Value::String(s) if !(fname == "_view" && is_view_arg(k)) => s.clone(),
                        _ if fname == "_view" && is_view_arg(k) => v.to_string(),
                        other => other.to_string(),
                    };
                    extra.push((k.clone(), s));
                }
                extra.reverse();
                qs.splice(0..0, std::iter::once(("filter".to_string(), fname)).chain(extra));
                (Method::GET, None)
            }
            Filter::None => (Method::GET, None),
        };
        let headers: Vec<(&str, String)> = if body.is_some() { vec![("Content-Type", "application/json".into())] } else { vec![] };
        // The feed reader retries on its own terms (retries = 0 here).
        let r = self.send_streaming(method, "_changes", &qs, &headers, body, Some(0)).await?;
        if r.status().as_u16() != 200 {
            let code = r.status().as_u16();
            let body = r.text().await.unwrap_or_default();
            return Err(HttpFail { method: "GET".into(), url: url_strip_password(&self.url("_changes", &qs)), error: format!("{{changes_req_failed,{code},{body}}}"), code: Some(code) });
        }
        Ok(r)
    }
}

fn name_of(f: &super::rep::Filter) -> String {
    match f {
        super::rep::Filter::View(n, _) => n.clone(),
        super::rep::Filter::User(d, n, _) => format!("{d}/{n}"),
        _ => String::new(),
    }
}

/// View query arguments (the mrargs fields and "key"), JSON-encoded when
/// passed to a _view filter.
fn is_view_arg(k: &str) -> bool {
    matches!(
        k,
        "key" | "keys" | "start_key" | "startkey" | "end_key" | "endkey" | "start_key_docid" | "end_key_docid" | "limit" | "skip"
            | "group_level" | "group" | "stable" | "update" | "descending" | "include_docs" | "inclusive_end" | "reduce"
            | "conflicts" | "sorted" | "view_type" | "direction" | "extra" | "callback" | "preflight_fun" | "update_seq"
            | "doc_options" | "multi_get" | "start_key_doc_id" | "end_key_doc_id" | "db_name" | "view_name" | "partition"
    )
}

#[derive(Debug)]
pub enum DocWriteError {
    Conflict,
    MissingStub,
    /// Skipped and counted as a write failure (unauthorized, forbidden,
    /// too large, ...).
    Skip(String, String),
    Other(String),
}

/// A sequence as sent in `since` (seq_encode/1): strings as they are,
/// anything else as JSON.
pub fn seq_encode(seq: &Value) -> String {
    match seq {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The filter's code, for the replication id (couch_replicator_filters:fetch/3).
pub async fn fetch_filter(source: &Endpoint, ddoc: &str, name: &str) -> Result<String, RepError> {
    let uri = source.uri();
    let c = Client::new(source)?;
    if let Err(e) = c.db_open(false, &Map::new()).await {
        return Err(RepError::FilterFetch(format!("Could not open source database `{uri}`: {}", e.erl_text())));
    }
    let doc = c.open_doc(&format!("_design/{ddoc}")).await.map_err(|e| {
        let e = match e {
            Value::String(s) => format!("{{error,<<\"{s}\">>}}"),
            other => format!("{{error,{other}}}"),
        };
        RepError::FilterFetch(format!("Couldn't open document `_design/{ddoc}` from source database `{uri}`: {e}"))
    })?;
    match doc.get("filters").and_then(|f| f.get(name)).and_then(Value::as_str) {
        Some(code) => Ok(super::rep::trim_filter_code(code)),
        None => Err(RepError::FilterFetch(format!(
            "Couldn't parse filter code from document {ddoc} on `{uri}`  Error: {{not_found,<<\"{name}\">>}}"
        ))),
    }
}

// ------------------------------------------------------------ the feed

/// One change from the feed (#doc_info{}), or the end of a continuous one.
#[derive(Debug, Clone)]
pub enum Change {
    Doc { id: String, seq: Value, revs: Vec<String> },
    LastSeq(Value),
}

fn to_change(v: &Value) -> Option<Change> {
    match v.get("changes").and_then(Value::as_array) {
        None => Some(Change::LastSeq(v.get("last_seq").cloned().unwrap_or(Value::Null))),
        Some(changes) => {
            let mut revs: Vec<String> = changes.iter().filter_map(|c| c.get("rev").and_then(Value::as_str).map(String::from)).collect();
            if v.get("removed") == Some(&Value::Bool(true)) && !revs.is_empty() {
                revs.remove(0);
            }
            Some(Change::Doc { id: v.get("id").and_then(Value::as_str).unwrap_or("").to_string(), seq: v.get("seq").cloned().unwrap_or(Value::Null), revs })
        }
    }
}

/// A feed's changes as they arrive: a normal feed's "results" one object
/// at a time (it can be large), or a continuous feed's lines.
pub fn feed_stream(resp: reqwest::Response, continuous: bool) -> impl futures::Stream<Item = Result<Change, String>> {
    async_stream::stream! {
        let mut stream = resp.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();
        let mut scan = ResultsScanner::default();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    yield Err(e.to_string());
                    return;
                }
            };
            buf.extend_from_slice(&chunk);
            let objs: Vec<Vec<u8>> = if continuous {
                let mut lines = Vec::new();
                while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=nl).collect();
                    if !line.iter().all(u8::is_ascii_whitespace) {
                        lines.push(line);
                    }
                }
                lines
            } else {
                scan.feed(&mut buf)
            };
            for obj in objs {
                match serde_json::from_slice::<Value>(&obj) {
                    Ok(v) => {
                        if let Some(c) = to_change(&v) {
                            yield Ok(c);
                        }
                    }
                    Err(e) => {
                        yield Err(e.to_string());
                        return;
                    }
                }
            }
        }
        if continuous && !buf.iter().all(u8::is_ascii_whitespace) {
            if let Ok(v) = serde_json::from_slice::<Value>(&buf)
                && let Some(c) = to_change(&v)
            {
                yield Ok(c);
            }
        } else if !continuous && !scan.done {
            yield Err("more_data_expected".into());
        }
    }
}

/// Picks the objects of a normal feed's "results" array out of the bytes
/// as they arrive.
#[derive(Default)]
struct ResultsScanner {
    in_array: bool,
    done: bool,
}

impl ResultsScanner {
    fn feed(&mut self, buf: &mut Vec<u8>) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        if self.done {
            buf.clear();
            return out;
        }
        if !self.in_array {
            let Some(at) = find(buf, b"\"results\"") else { return out };
            let mut i = at + 9;
            while i < buf.len() && (buf[i].is_ascii_whitespace() || buf[i] == b':') {
                i += 1;
            }
            if i >= buf.len() {
                return out;
            }
            if buf[i] != b'[' {
                self.done = true;
                return out;
            }
            buf.drain(..=i);
            self.in_array = true;
        }
        loop {
            let mut i = 0;
            while i < buf.len() && (buf[i].is_ascii_whitespace() || buf[i] == b',') {
                i += 1;
            }
            if i >= buf.len() {
                buf.drain(..i);
                return out;
            }
            if buf[i] == b']' {
                self.done = true;
                buf.clear();
                return out;
            }
            // A balanced object, minding strings.
            let (mut depth, mut in_str, mut esc, mut end) = (0i32, false, false, None);
            for (j, &c) in buf.iter().enumerate().skip(i) {
                if in_str {
                    match (esc, c) {
                        (true, _) => esc = false,
                        (false, b'\\') => esc = true,
                        (false, b'"') => in_str = false,
                        _ => {}
                    }
                    continue;
                }
                match c {
                    b'"' => in_str = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(j);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            match end {
                Some(e) => {
                    out.push(buf[i..=e].to_vec());
                    buf.drain(..=e);
                }
                None => {
                    buf.drain(..i);
                    return out;
                }
            }
        }
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_results_in_pieces() {
        let body = br#"{"results":[{"seq":"1-a","id":"x","changes":[{"rev":"1-a"}]},
{"seq":"2-b","id":"y\"}","changes":[{"rev":"1-b"}],"deleted":true}
],"last_seq":"2-b","pending":0}"#;
        let mut scan = ResultsScanner::default();
        let mut buf = Vec::new();
        let mut got = Vec::new();
        for chunk in body.chunks(7) {
            buf.extend_from_slice(chunk);
            for o in scan.feed(&mut buf) {
                got.push(serde_json::from_slice::<Value>(&o).unwrap()["id"].as_str().unwrap().to_string());
            }
        }
        assert_eq!(got, vec!["x".to_string(), "y\"}".to_string()]);
        assert!(scan.done);
    }

    #[test]
    fn doc_ids_in_urls() {
        assert_eq!(encode_doc_id("_design/a b"), "_design/a%20b");
        assert_eq!(encode_doc_id("a/b"), "a%2Fb");
        assert_eq!(encode_doc_id("_local/x"), "_local/x");
    }
}
