//! The replicator, a port of couch_replicator: replications between any
//! two CouchDB-compatible endpoints, over HTTP (a local database name is
//! this server's URL for it, with the request's credentials, as chttpd
//! arranges). POST /_replicate runs one (waiting for a one-shot to
//! finish), _replicator databases hold persistent ones, and _scheduler
//! reports on them all.

pub mod client;
pub mod docs;
pub mod job;
pub mod rep;
pub mod scheduler;

use crate::error::{CouchError, Result};
use crate::http::{Req, json_response, not_allowed, ok_json};
use axum::http::StatusCode;
use axum::response::Response;
use serde_json::{Map, Value, json};
use std::sync::Arc;

/// Replication errors, as couch_replicator throws them.
#[derive(Debug, Clone)]
pub enum RepError {
    /// {bad_request, Reason}
    BadRequest(String),
    /// {bad_request, Reason} with the reason an Erlang string, not a binary.
    BadRequestList(String),
    /// An error exception the parser runs into (badarg, function_clause).
    Crash(&'static str),
    /// {error, Reason}
    Error(String),
    DbNotFound(String),
    Unauthorized(String),
    Forbidden(String),
    /// {filter_fetch_error, Reason}
    FilterFetch(String),
    Http(client::HttpFail),
    MissingDoc,
    NotFound,
    Other(String),
}

impl RepError {
    /// How the error reads in crash reasons and messages
    /// (rep_error_to_binary/1).
    pub fn erl_text(&self) -> String {
        match self {
            RepError::BadRequest(m)
            | RepError::BadRequestList(m)
            | RepError::Error(m)
            | RepError::DbNotFound(m)
            | RepError::Unauthorized(m)
            | RepError::FilterFetch(m) => m.clone(),
            RepError::Crash(a) => a.to_string(),
            RepError::Forbidden(m) => m.clone(),
            RepError::Http(f) => f.to_string(),
            RepError::MissingDoc => "missing_doc".into(),
            RepError::NotFound => "not_found".into(),
            RepError::Other(m) => m.clone(),
        }
    }

    /// How POST /_replicate reports a replication that failed to start: a
    /// request that failed is an {error, Reason} with the reason as `~p`
    /// writes it.
    pub fn start_failure(&self) -> CouchError {
        match self {
            RepError::Http(f) => CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, "error", crate::erlpp::pretty(&f.to_string())),
            e => e.to_couch(),
        }
    }

    pub fn to_couch(&self) -> CouchError {
        let e = |code: StatusCode, err: &str, reason: &str| CouchError::new(code, err, reason.to_string());
        match self {
            RepError::BadRequest(m) | RepError::BadRequestList(m) => e(StatusCode::BAD_REQUEST, "bad_request", m),
            RepError::Crash(a) => e(StatusCode::INTERNAL_SERVER_ERROR, "unknown_error", a).with_ref(),
            RepError::Error(m) if m == "endpoint has an invalid url" || m == "proxy has an invalid url" => {
                e(StatusCode::BAD_REQUEST, "invalid_replication", m)
            }
            RepError::Error(m) => e(StatusCode::INTERNAL_SERVER_ERROR, "error", m),
            RepError::DbNotFound(m) => e(StatusCode::INTERNAL_SERVER_ERROR, "db_not_found", m),
            RepError::Unauthorized(m) => e(StatusCode::UNAUTHORIZED, "unauthorized", m),
            RepError::Forbidden(m) => e(StatusCode::FORBIDDEN, "forbidden", m),
            RepError::FilterFetch(m) => e(StatusCode::NOT_FOUND, "not_found", m),
            RepError::Http(f) => match f.code {
                Some(c) => e(StatusCode::INTERNAL_SERVER_ERROR, "http_error_code", &format!("{c}")),
                None => e(StatusCode::INTERNAL_SERVER_ERROR, "http_request_failed", &f.to_string()),
            },
            RepError::MissingDoc => e(StatusCode::INTERNAL_SERVER_ERROR, "missing_doc", "missing_doc"),
            RepError::NotFound => CouchError::not_found("missing"),
            RepError::Other(m) => e(StatusCode::INTERNAL_SERVER_ERROR, "error", m),
        }
    }
}

/// Start the scheduler and the replicator databases' doc processor.
pub fn start(server: &Arc<crate::server::Server>) {
    scheduler::start(server);
    docs::start(server);
}

fn user_ctx(req: &Req) -> rep::UserCtx {
    rep::UserCtx { name: req.auth.user.name.clone(), roles: req.auth.user.roles.clone() }
}

/// POST /_replicate.
pub async fn http_replicate(req: &Req) -> Result<Response> {
    if req.body.is_empty() {
        return Err(CouchError::bad_request("Missing request body"));
    }
    if req.method != axum::http::Method::POST {
        return not_allowed("POST");
    }
    if !req.content_type().starts_with("application/json") {
        return Err(CouchError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, "bad_content_type", "Content-Type must be application/json"));
    }
    let mut body = req.json_obj_body()?;
    let cookie = req
        .header("cookie")
        .and_then(|c| c.split(';').map(str::trim).find_map(|p| p.strip_prefix("AuthSession=")))
        .map(|s| format!("AuthSession={s}"))
        .unwrap_or_default();
    rep::localize(&mut body, &req.server, req.header("authorization").unwrap_or(""), &cookie).map_err(|e| e.to_couch())?;
    let sched = scheduler::get().ok_or_else(|| CouchError::internal("replicator not started"))?;
    let rep = rep::parse_rep_doc(&body, user_ctx(req), &req.server).await.map_err(|e| e.to_couch())?;
    let id = rep.id_str();
    if rep.options.cancel {
        return match scheduler::check_authorization(&id, &req.auth.user).map_err(|e| e.to_couch())? {
            true => {
                tracing::info!("Canceling replication '{id}' ...");
                sched.remove_job(&id);
                tracing::info!("Replication '{id}' cancelled");
                ok_json(json!({"ok": true, "_local_id": id}))
            }
            false => {
                tracing::info!("Replication '{id}' not found");
                Err(CouchError::not_found("missing"))
            }
        };
    }
    scheduler::check_authorization(&id, &req.auth.user).map_err(|e| e.to_couch())?;
    let continuous = rep.options.continuous;
    let rx = sched.add_job(rep);
    if continuous {
        return Ok(json_response(StatusCode::ACCEPTED, &json!({"ok": true, "_local_id": id})));
    }
    match rx.await {
        Ok(scheduler::Outcome::Finished(history)) => {
            let mut out = Map::new();
            out.insert("ok".into(), json!(true));
            if let Value::Object(h) = history {
                out.extend(h);
            }
            ok_json(Value::Object(out))
        }
        Ok(scheduler::Outcome::Error(job::JobError::Start(e))) => Err(e.start_failure()),
        Ok(scheduler::Outcome::Error(e @ job::JobError::Crash(_))) => Err(CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, "error", e.reason())),
        Ok(scheduler::Outcome::Stopped) | Err(_) => ok_json(json!({"ok": "stopped"})),
    }
}

fn int_param(req: &Req, name: &str, default: u64) -> Result<u64> {
    match req.q(name) {
        None => Ok(default),
        Some(v) => v.parse::<i64>().map_err(|_| CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", format!("{name} must be an integer"))).and_then(|n| {
            if n < 0 {
                Err(CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", format!("{name} not in range of [0,infinity]")))
            } else {
                Ok(n as u64)
            }
        }),
    }
}

/// /_scheduler/jobs and /_scheduler/docs (couch_replicator_httpd).
pub fn http_scheduler(req: &Req) -> Result<Response> {
    let get = matches!(req.method, axum::http::Method::GET | axum::http::Method::HEAD);
    if !get {
        return not_allowed("GET,HEAD");
    }
    let parts: Vec<&str> = req.path.iter().skip(1).map(String::as_str).collect();
    let Some(sched) = scheduler::get() else { return Err(CouchError::internal("replicator not started")) };
    match parts.as_slice() {
        ["jobs"] => {
            let limit = int_param(req, "limit", 100)?;
            let skip = int_param(req, "skip", 0)?;
            let all = sched.jobs_json();
            let total = all.len();
            let offset = (skip as usize).min(total);
            let jobs: Vec<Value> = all.into_iter().skip(offset).take(limit as usize).collect();
            ok_json(json!({"total_rows": total, "offset": offset, "jobs": jobs}))
        }
        ["jobs", id] => {
            let (base, ext) = rep::convert_id(id);
            sched.job_json_by_id(&format!("{base}{ext}")).map(ok_json).unwrap_or_else(|| Err(CouchError::not_found("missing")))
        }
        ["docs"] => docs::http_docs(req, "_replicator"),
        ["docs", rest @ ..] => {
            // A db name may arrive with its slashes unencoded.
            match rest.iter().position(|s| *s == "_replicator" || s.ends_with("/_replicator")) {
                Some(i) if i + 1 == rest.len() => docs::http_docs(req, &rest[..=i].join("/")),
                Some(i) if i + 2 == rest.len() => docs::http_doc(req, &rest[..=i].join("/"), rest[i + 1]),
                _ => Err(CouchError::bad_request("Invalid path")),
            }
        }
        _ => Ok(json_response(StatusCode::NOT_FOUND, &json!({"error": "not found"}))),
    }
}
