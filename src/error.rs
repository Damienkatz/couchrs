use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct CouchError {
    pub status: StatusCode,
    pub error: String,
    pub reason: String,
    /// The "ref" (and X-Couch-Stack-Hash) CouchDB gives an error it
    /// caught with its stack: a hash of where it came from.
    pub stack_ref: Option<u32>,
}

pub type Result<T> = std::result::Result<T, CouchError>;

impl CouchError {
    pub fn new(status: StatusCode, error: &str, reason: impl Into<String>) -> Self {
        CouchError {
            status,
            error: error.to_string(),
            reason: reason.into(),
            stack_ref: None,
        }
    }
    /// The error with a ref, as chttpd:send_error/2 sends one with a stack.
    pub fn with_ref(mut self) -> Self {
        self.stack_ref = Some(crc32fast::hash(format!("{}{}", self.error, self.reason).as_bytes()));
        self
    }
    pub fn not_found(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", reason)
    }
    pub fn missing() -> Self {
        Self::not_found("missing")
    }
    pub fn deleted() -> Self {
        Self::not_found("deleted")
    }
    pub fn conflict() -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", "Document update conflict.")
    }
    pub fn bad_request(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", reason)
    }
    pub fn forbidden(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", reason)
    }
    pub fn unauthorized(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", reason)
    }
    pub fn doc_validation(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "doc_validation", reason)
    }
    pub fn file_exists() -> Self {
        Self::new(
            StatusCode::PRECONDITION_FAILED,
            "file_exists",
            "The database could not be created, the file already exists.",
        )
    }
    pub fn precondition(error: &str, reason: impl Into<String>) -> Self {
        Self::new(StatusCode::PRECONDITION_FAILED, error, reason)
    }
    pub fn internal(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_server_error", reason)
    }
    pub fn json(&self) -> serde_json::Value {
        match self.stack_ref {
            Some(r) => json!({"error": self.error, "reason": self.reason, "ref": r}),
            None => json!({"error": self.error, "reason": self.reason}),
        }
    }
}

impl From<std::io::Error> for CouchError {
    fn from(e: std::io::Error) -> Self {
        CouchError::internal(e.to_string())
    }
}

impl From<serde_json::Error> for CouchError {
    fn from(e: serde_json::Error) -> Self {
        CouchError::new(StatusCode::BAD_REQUEST, "bad_request", format!("invalid UTF-8 JSON: {e}"))
    }
}

impl IntoResponse for CouchError {
    fn into_response(self) -> Response {
        let mut r = crate::http::json_response(self.status, &self.json());
        if let Some(h) = self.stack_ref {
            r.headers_mut().insert("x-couch-stack-hash", axum::http::HeaderValue::from(h));
        }
        // send_method_not_allowed/2 names the methods in Allow too.
        if self.status == StatusCode::METHOD_NOT_ALLOWED
            && let Some(m) = self.reason.strip_prefix("Only ").and_then(|r| r.strip_suffix(" allowed"))
            && let Ok(v) = axum::http::HeaderValue::from_str(m)
        {
            r.headers_mut().insert(axum::http::header::ALLOW, v);
        }
        r
    }
}

/// Lets CouchError propagate out of btree fold callbacks (which return
/// io::Result) and back again.
impl From<CouchError> for std::io::Error {
    fn from(e: CouchError) -> Self {
        std::io::Error::other(format!("{}: {}", e.error, e.reason))
    }
}
