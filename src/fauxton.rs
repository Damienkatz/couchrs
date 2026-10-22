//! Fauxton, the CouchDB web UI, served at /_utils/ (chttpd_misc's
//! handle_utils_dir_req). The built files from share/www are compiled into
//! the binary, so the server needs no install directory.

use crate::error::{CouchError, Result};
use crate::http::{Req, not_allowed};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use axum::response::Response;
use include_dir::{Dir, include_dir};

static WWW: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/share/www");

fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, e)| e).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "application/javascript",
        "css" => "text/css",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "gif" => "image/gif",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "eot" => "application/vnd.ms-fontobject",
        "txt" | "md" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

pub fn serve(req: &Req) -> Result<Response> {
    if req.method != Method::GET && req.method != Method::HEAD {
        return not_allowed("GET,HEAD");
    }
    // `/_utils` without the slash would break Fauxton's relative asset paths.
    if req.path.len() == 1 && !req.trailing_slash {
        return Ok(Response::builder()
            .status(StatusCode::MOVED_PERMANENTLY)
            .header(header::LOCATION, crate::http::absolute_uri(req, "/_utils/"))
            .body(Body::empty())
            .unwrap());
    }
    let rel = if req.path.len() == 1 { "index.html".to_string() } else { req.path[1..].join("/") };
    if rel.split('/').any(|s| s == "..") {
        return Err(CouchError::not_found("missing"));
    }
    let file = WWW.get_file(&rel).ok_or_else(|| CouchError::not_found("missing"))?;
    // Asset names carry a content hash, so they can be cached for good;
    // index.html names the current ones and must be revalidated.
    let cache = if rel.starts_with("dashboard.assets/") { "public, max-age=31536000, immutable" } else { "no-cache" };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type(&rel))
        .header(header::CACHE_CONTROL, cache)
        .body(Body::from(file.contents()))
        .unwrap())
}
