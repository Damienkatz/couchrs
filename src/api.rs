//! Database and document endpoints (chttpd_db).

use crate::btree::{Dir, Range};
use crate::db::{Db, Snapshot};
use crate::doc::*;
use crate::error::{CouchError, Result};
use crate::http::{Req, empty, json_response, new_uuid, not_allowed, ok_json, with_header};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use axum::response::Response;
use base64::Engine;
use bytes::Bytes;
use md5::{Digest, Md5};
use serde_json::{Map, Value, json};
use std::sync::Arc;

/// Run work that may take a while (a JavaScript function, a scan, a
/// large attachment) without stalling the other tasks on this worker.
///
/// Not for point reads (one doc, one body, one small attachment):
/// RocksDB answers those from memory in microseconds, while each
/// block_in_place hands the worker's queue to the blocking pool through
/// one shared lock. At tens of thousands of connections those handoffs
/// serialize, and the server stalls with its CPUs idle.
pub fn blocking<T>(f: impl FnOnce() -> T) -> T {
    tokio::task::block_in_place(f)
}

pub async fn db_request(req: Req) -> Result<Response> {
    let name = req.path[0].clone();
    let rest: Vec<String> = req.path[1..].to_vec();
    if rest.is_empty() {
        return db_root(&req, &name).await;
    }
    let db = req.server.open_db(&name)?;
    let r: Vec<&str> = rest.iter().map(|s| s.as_str()).collect();
    match r.as_slice() {
        ["_all_docs"] => all_docs(&req, &db, AllDocsKind::All).await,
        ["_all_docs", "queries"] => multi_query(&req, &db, AllDocsKind::All).await,
        ["_design_docs"] => all_docs(&req, &db, AllDocsKind::Design).await,
        ["_design_docs", "queries"] => multi_query(&req, &db, AllDocsKind::Design).await,
        ["_local_docs"] => all_docs(&req, &db, AllDocsKind::Local).await,
        ["_local_docs", "queries"] => multi_query(&req, &db, AllDocsKind::Local).await,
        ["_bulk_docs"] => bulk_docs(&req, &db).await,
        ["_bulk_get"] => bulk_get(&req, &db).await,
        ["_changes"] => crate::changes::changes(req, db).await,
        ["_compact"] => compact(&req, &db),
        ["_compact", ddoc] => {
            let ddoc = ddoc.to_string();
            crate::views::http_compact_view(&req, &db, &ddoc)
        }
        ["_view_cleanup"] => crate::views::http_view_cleanup(&req, &db),
        ["_nouveau_cleanup"] => crate::nouveau::http_cleanup(&req, &db),
        ["_search_cleanup"] => crate::nouveau::http_search_cleanup(&req),
        ["_ensure_full_commit"] => {
            if req.method != Method::POST {
                return not_allowed("POST");
            }
            Ok(json_response(StatusCode::CREATED, &json!({"ok": true, "instance_start_time": "0"})))
        }
        ["_revs_diff"] => revs_diff(&req, &db),
        ["_missing_revs"] => missing_revs(&req, &db),
        ["_revs_limit"] => revs_limit(&req, &db).await,
        ["_security"] => security(&req, &db).await,
        ["_purge"] => purge(&req, &db).await,
        ["_purged_infos_limit"] => match req.method {
            Method::GET => ok_json(json!(1000)),
            Method::PUT => ok_json(json!({"ok": true})),
            _ => not_allowed("GET,PUT"),
        },
        ["_find"] => crate::mango::http_find(&req, &db).await,
        ["_explain"] => crate::mango::http_explain(&req, &db).await,
        ["_index"] => crate::mango::http_index(&req, &db).await,
        ["_index", "_bulk_delete"] => crate::mango::http_index_bulk_delete(&req, &db).await,
        ["_index", ddoc, kind, iname] => {
            let (d, k, n) = (ddoc.to_string(), kind.to_string(), iname.to_string());
            crate::mango::http_delete_index(&req, &db, &d, &k, &n).await
        }
        ["_index", "_design", ddoc, kind, iname] => {
            let (d, k, n) = (format!("_design/{ddoc}"), kind.to_string(), iname.to_string());
            crate::mango::http_delete_index(&req, &db, &d, &k, &n).await
        }
        ["_partition", p, rest @ ..] => {
            let (p, rest): (String, Vec<String>) = (p.to_string(), rest.iter().map(|s| s.to_string()).collect());
            partition_request(&req, &db, &p, &rest).await
        }
        ["_design", ddoc, rest @ ..] => {
            let id = format!("_design/{ddoc}");
            match rest {
                [] => doc_request(&req, &db, &id).await,
                ["_info"] => crate::views::http_ddoc_info(&req, &db, &id).await,
                ["_view", view] => crate::views::http_view(&req, &db, &id, view).await,
                ["_view", view, "queries"] => crate::views::http_view_queries(&req, &db, &id, view).await,
                ["_nouveau", rest @ ..] => crate::nouveau::http_search(&req, &db, &id, rest, None).await,
                ["_nouveau_info", rest @ ..] => crate::nouveau::http_info(&req, &db, &id, rest),
                [kind @ ("_search" | "_search_info" | "_search_disk_size"), rest @ ..] => crate::nouveau::http_dreyfus(&req, &db, &id, kind, rest),
                ["_show", ..] | ["_list", ..] | ["_update", ..] | ["_rewrite", ..] => {
                    crate::js::http_ddoc_fun(&req, &db, &id, rest).await
                }
                att => att_request(&req, &db, &id, &att.join("/")).await,
            }
        }
        ["_local", lid] => {
            let id = format!("_local/{lid}");
            doc_request(&req, &db, &id).await
        }
        ["_local"] => Err(CouchError::bad_request("Invalid _local document id.")),
        // Not a reserved id: there is no such document to read, and none
        // may be written.
        [s, ..] if s.starts_with('_') => match req.method {
            Method::GET | Method::HEAD => Err(CouchError::not_found("missing")),
            _ => Err(CouchError::new(
                StatusCode::BAD_REQUEST,
                "illegal_docid",
                "Only reserved document ids may start with underscore.",
            )),
        },
        [docid] => {
            let id = docid.to_string();
            doc_request(&req, &db, &id).await
        }
        [docid, att @ ..] => {
            let id = docid.to_string();
            att_request(&req, &db, &id, &att.join("/")).await
        }
        [] => unreachable!(),
    }
}

async fn db_root(req: &Req, name: &str) -> Result<Response> {
    match req.method {
        Method::GET | Method::HEAD => {
            let db = req.server.open_db(name)?;
            let t = std::time::Instant::now();
            let info = db.info();
            crate::stat!("couchdb.dbinfo").observe(crate::stats::ms_since(t));
            ok_json(info)
        }
        Method::PUT => {
            if req.q("q").map(|q| q != "1").unwrap_or(false) || req.q("n").map(|q| q != "1").unwrap_or(false) {
                // Single node: accept and ignore sharding parameters.
            }
            let props = match req.q("partitioned") {
                None | Some("false") => json!({}),
                Some("true") => {
                    let sys = name.starts_with('_') || name.ends_with("/_users") || name.ends_with("/_replicator");
                    if sys {
                        return Err(CouchError::bad_request("Cannot partition a system database"));
                    }
                    json!({"partitioned": true})
                }
                Some(_) => return Err(CouchError::bad_request("Invalid `partitioned` parameter")),
            };
            let db = req.server.create_db_with(name, &props)?;
            if crate::auth::is_users_db(&req.server.config, name) {
                crate::server::ensure_auth_ddoc(&db).await?;
            }
            let r = json_response(StatusCode::CREATED, &json!({"ok": true}));
            Ok(with_header(r, header::LOCATION, &format!("/{name}")))
        }
        Method::DELETE => {
            if req.q("rev").is_some() {
                return Err(CouchError::bad_request(
                    "You tried to DELETE a database with a ?rev= parameter. Did you mean to DELETE a document instead?",
                ));
            }
            req.server.delete_db(name)?;
            ok_json(json!({"ok": true}))
        }
        Method::POST => {
            let db = req.server.open_db(name)?;
            let body = req.json_body()?;
            let mut doc = from_json(body)?;
            if doc.id.is_empty() {
                doc.id = new_uuid();
            }
            validate_docid(&doc.id)?;
            let id = doc.id.clone();
            let batch = req.q("batch") == Some("ok");
            let rev = save_docs(req, &db, vec![doc], true).await?.pop().unwrap()?;
            let status = if batch { StatusCode::ACCEPTED } else { StatusCode::CREATED };
            let r = json_response(status, &json!({"ok": true, "id": id, "rev": rev}));
            let r = with_header(r, header::LOCATION, &format!("/{name}/{id}"));
            Ok(with_header(r, header::ETAG, &format!("\"{rev}\"")))
        }
        _ => not_allowed("DELETE,GET,HEAD,POST,PUT"),
    }
}

/// Check, validate and submit docs to the writer, as `req`'s user:
/// design documents need a database admin, users-db documents get their
/// passwords hashed, and validate_doc_update functions see the user.
pub async fn save_docs(req: &Req, db: &Arc<Db>, mut docs: Vec<Doc>, new_edits: bool) -> Result<Vec<Result<String>>> {
    let snap = db.snapshot();
    let sec = snap.security();
    let user = &req.auth.user;
    let config = &req.server.config;
    let users_db = crate::auth::is_users_db(config, &db.name);
    let replicator_db = crate::replicator::docs::is_replicator_db(&db.name);
    let mut early: Vec<Option<CouchError>> = vec![None; docs.len()];
    for (d, e) in docs.iter_mut().zip(early.iter_mut()) {
        for a in d.atts.iter_mut() {
            crate::doc::maybe_compress_att(config, a);
        }
        if d.is_local() {
            continue;
        }
        if d.is_design() {
            crate::nouveau::before_doc_update(d, new_edits);
            *e = crate::auth::check_db_admin(user, &sec).err();
            // Design documents must describe valid indexes (couch_mrview
            // validate), including the partitioned option.
            if e.is_none() && !d.deleted && !d.id.starts_with("_design/_") {
                *e = crate::views::IndexDef::from_ddoc(&crate::js::doc_to_vdu_json(d), snap.partitioned()).err();
            }
        } else if users_db {
            *e = crate::auth::before_users_doc_update(config, user, &sec, &mut d.body, &d.id).err();
        } else if replicator_db {
            *e = crate::replicator::docs::before_doc_update(&req.server, user, &sec, d, !new_edits).err();
        }
    }
    if snap.partitioned() {
        for d in &docs {
            crate::doc::validate_partitioned_docid(&d.id)?;
        }
    }
    let ctx = user.json(&db.name);
    let mut pre = if crate::js::has_validators(db, &snap)? {
        let skip: Vec<bool> = early.iter().map(|e| e.is_some()).collect();
        blocking(|| crate::js::validate_docs(db, &snap, &mut docs, new_edits, &ctx, &skip))?
    } else {
        vec![None; docs.len()]
    };
    for (p, e) in pre.iter_mut().zip(early) {
        if e.is_some() {
            *p = e;
        }
    }
    let mut idx = Vec::new();
    let mut to_save = Vec::new();
    for (i, (d, err)) in docs.into_iter().zip(pre.iter()).enumerate() {
        if err.is_none() {
            idx.push(i);
            to_save.push(d);
        }
    }
    let mut results: Vec<Result<String>> = pre
        .into_iter()
        .map(|e| match e {
            Some(e) => Err(e),
            None => Ok(String::new()),
        })
        .collect();
    // Stage attachment bytes outside the writer; retry docs whose staged
    // chunks were orphaned by a concurrent compaction swap.
    let mut pending: Vec<(usize, Doc)> = idx.into_iter().zip(to_save).collect();
    for attempt in 0..3 {
        if pending.is_empty() {
            break;
        }
        let mut staged = Vec::with_capacity(pending.len());
        for (_, d) in &pending {
            let mut d = d.clone();
            if d.atts.iter().any(|a| matches!(a.data, AttData::Bytes(_))) {
                blocking(|| db.stage_atts(&mut d))?;
            }
            staged.push(d);
        }
        let saved = db.update_docs(staged, new_edits).await?;
        let mut retry = Vec::new();
        for ((i, d), r) in pending.into_iter().zip(saved) {
            match r {
                Err(e) if e.error == "att_file_moved" && attempt < 2 => retry.push((i, d)),
                r => results[i] = r,
            }
        }
        pending = retry;
    }
    Ok(results)
}

pub fn doc_opts(req: &Req) -> Result<DocOpts> {
    let meta = req.flag("meta")?;
    let atts_since = match req.q_json("atts_since")? {
        Some(Value::Array(a)) => Some(
            a.iter()
                .filter_map(|r| r.as_str().and_then(|s| parse_rev(s).ok()))
                .collect(),
        ),
        Some(_) => return Err(CouchError::bad_request("atts_since value must be array of revs.")),
        None => None,
    };
    Ok(DocOpts {
        revs: req.flag("revs")?,
        revs_info: req.flag("revs_info")? || meta,
        conflicts: req.flag("conflicts")? || meta,
        deleted_conflicts: req.flag("deleted_conflicts")? || meta,
        local_seq: req.flag("local_seq")?,
        attachments: req.flag("attachments")?,
        att_encoding_info: req.flag("att_encoding_info")?,
        atts_since,
        meta,
        follows: false,
    })
}

fn local_json(id: &str, l: &crate::db::LocalDoc) -> Value {
    let mut m = Map::new();
    m.insert("_id".into(), json!(id));
    m.insert("_rev".into(), json!(format!("0-{}", l.rev)));
    let body: Map<String, Value> = serde_json::from_slice(&l.body).unwrap_or_default();
    m.extend(body);
    Value::Object(m)
}

fn rev_from_req(req: &Req) -> Option<String> {
    req.q("rev").map(String::from).or_else(|| {
        req.header("if-match").map(|s| s.trim_matches('"').to_string())
    })
}

async fn doc_request(req: &Req, db: &Arc<Db>, id: &str) -> Result<Response> {
    match req.method {
        Method::GET | Method::HEAD => get_doc(req, db, id),
        Method::PUT => put_doc(req, db, id).await,
        Method::DELETE => {
            let rev = rev_from_req(req).ok_or_else(CouchError::conflict)?;
            let mut doc = Doc {
                id: id.to_string(),
                deleted: true,
                ..Default::default()
            };
            if id.starts_with("_local/") {
                doc.revs = (0, vec![rev.trim_start_matches("0-").to_string()]);
            } else {
                let (p, r) = parse_rev(&rev)?;
                doc.revs = (p, vec![r]);
            }
            let rev = save_docs(req, db, vec![doc], true).await?.pop().unwrap()?;
            let r = ok_json(json!({"ok": true, "id": id, "rev": rev}))?;
            Ok(with_header(r, header::ETAG, &format!("\"{rev}\"")))
        }
        Method::POST => Err(CouchError::bad_request("Referer header required.")),
        _ if req.method.as_str() == "COPY" => copy_doc(req, db, id).await,
        _ => not_allowed("COPY,DELETE,GET,HEAD,POST,PUT"),
    }
}

fn get_doc(req: &Req, db: &Arc<Db>, id: &str) -> Result<Response> {
    let snap = db.snapshot();
    if id.starts_with("_local/") {
        let l = snap.get_local(id)?.ok_or_else(CouchError::missing)?;
        return ok_json(local_json(id, &l));
    }
    let opts = doc_opts(req)?;
    if let Some(open_revs) = req.q("open_revs") {
        return open_revs_response(req, &snap, id, open_revs, &opts);
    }
    let fdi = snap.get_fdi(id)?.ok_or_else(CouchError::missing)?;
    let rev = match req.q("rev") {
        Some(r) => {
            let (p, rid) = parse_rev(r)?;
            Snapshot::find_rev(&fdi, p, &rid).ok_or_else(CouchError::missing)?
        }
        None => {
            let w = fdi.winner();
            if w.deleted {
                return Err(CouchError::deleted());
            }
            w
        }
    };
    let etag = format!("\"{}\"", rev.rev());
    if req.header("if-none-match") == Some(etag.as_str()) {
        return Ok(with_header(empty(StatusCode::NOT_MODIFIED), header::ETAG, &etag));
    }
    // Inlined attachment bodies can be large; the doc alone is a point read.
    let mut v = if opts.attachments { blocking(|| snap.doc_json(&fdi, &rev, &opts))? } else { snap.doc_json(&fdi, &rev, &opts)? };
    crate::db::count_reads(&snap, 1);
    if crate::replicator::docs::is_replicator_db(&db.name) {
        v = crate::replicator::docs::after_doc_read(&req.auth.user, &snap.security(), v);
    }
    if crate::auth::is_users_db(&req.server.config, &db.name) {
        v = crate::auth::after_users_doc_read(&req.server.config, &req.auth.user, &snap.security(), id, v)?;
    }
    Ok(with_header(json_response(StatusCode::OK, &v), header::ETAG, &etag))
}

/// Resolve `open_revs` into per-revision results.
pub fn open_revs(
    snap: &Snapshot,
    id: &str,
    spec: &Value,
    latest: bool,
    opts: &DocOpts,
) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    for r in resolve_open_revs(snap, id, spec, latest)? {
        match r {
            Ok((fdi, ri)) => {
                crate::db::count_reads(snap, 1);
                out.push(json!({"ok": snap.doc_json(&fdi, &ri, opts)?}))
            }
            Err(missing) => out.push(json!({"missing": missing})),
        }
    }
    Ok(out)
}

/// The revisions an `open_revs` request names: each found revision, or the
/// rev string that is missing.
pub type OpenRev = std::result::Result<(crate::doc::FullDocInfo, RevInfo), String>;

pub fn resolve_open_revs(snap: &Snapshot, id: &str, spec: &Value, latest: bool) -> Result<Vec<OpenRev>> {
    let fdi = snap.get_fdi(id)?;
    let mut out = Vec::new();
    match spec {
        Value::String(s) if s == "all" => {
            let fdi = fdi.ok_or_else(CouchError::missing)?;
            for l in fdi.sorted_leafs() {
                if l.val.leaf().is_some() {
                    out.push(Ok((fdi.clone(), l)));
                }
            }
        }
        Value::Array(revs) => {
            for r in revs {
                let rs = r.as_str().ok_or_else(|| CouchError::bad_request("Invalid rev format"))?;
                let (p, rid) = parse_rev(rs)?;
                let Some(fdi) = fdi.as_ref() else {
                    out.push(Err(rs.to_string()));
                    continue;
                };
                let found: Vec<RevInfo> = if latest {
                    crate::keytree::leafs_under(&fdi.tree, p, &rid)
                        .into_iter()
                        .map(|l| RevInfo {
                            pos: l.pos,
                            deleted: l.val.deleted(),
                            id: l.id,
                            val: l.val,
                            path: l.path,
                        })
                        .collect()
                } else {
                    Snapshot::find_rev(fdi, p, &rid).into_iter().collect()
                };
                let found: Vec<RevInfo> = found.into_iter().filter(|r| r.val.leaf().is_some()).collect();
                if found.is_empty() {
                    out.push(Err(rs.to_string()));
                }
                for ri in found {
                    out.push(Ok((fdi.clone(), ri)));
                }
            }
        }
        _ => return Err(CouchError::bad_request("Invalid open_revs value")),
    }
    Ok(out)
}

fn open_revs_response(req: &Req, snap: &Snapshot, id: &str, spec: &str, opts: &DocOpts) -> Result<Response> {
    let spec: Value = if spec == "all" {
        json!("all")
    } else {
        serde_json::from_str(spec).map_err(|_| CouchError::bad_request("Invalid open_revs value"))?
    };
    let latest = req.flag("latest")?;
    if !req.accepts("multipart/mixed") {
        let results = blocking(|| open_revs(snap, id, &spec, latest, opts))?;
        return ok_json(Value::Array(results));
    }
    // As CouchDB does: one part per revision; a revision with attachments
    // is a nested multipart/related whose JSON marks them "follows" and
    // whose following parts stream the raw bytes.
    // CouchDB always sends attachment bodies in multipart open_revs
    // responses (`[attachments, follows, att_encoding_info | Options]`);
    // the replicator relies on it and never asks for attachments=true.
    let mut opts = opts.clone();
    opts.follows = true;
    opts.attachments = true;
    opts.att_encoding_info = true;
    let revs = blocking(|| resolve_open_revs(snap, id, &spec, latest))?;
    let boundary = hex::encode(uuid::Uuid::new_v4().as_bytes());
    let mut pieces: Vec<Piece> = Vec::new();
    for r in revs {
        pieces.push(Piece::Data(Bytes::from(format!("--{boundary}\r\n"))));
        let (fdi, ri) = match r {
            Ok(x) => x,
            Err(missing) => {
                let j = serde_json::to_vec(&json!({"missing": missing}))?;
                pieces.push(Piece::Data(Bytes::from(format!(
                    "Content-Type: application/json; error=\"true\"\r\n\r\n{}\r\n",
                    String::from_utf8_lossy(&j)
                ))));
                continue;
            }
        };
        let doc = blocking(|| snap.doc_json(&fdi, &ri, &opts))?;
        crate::db::count_reads(&snap, 1);
        let follows: Vec<String> = doc
            .get("_attachments")
            .and_then(|a| a.as_object())
            .map(|a| {
                a.iter()
                    .filter(|(_, v)| v.get("follows").is_some())
                    .map(|(k, _)| k.clone())
                    .collect()
            })
            .unwrap_or_default();
        let doc_bytes = serde_json::to_vec(&doc)?;
        if follows.is_empty() {
            pieces.push(Piece::Data(Bytes::from(b"Content-Type: application/json\r\n\r\n".to_vec())));
            pieces.push(Piece::Data(Bytes::from(doc_bytes)));
            pieces.push(Piece::Data(Bytes::from_static(b"\r\n")));
            continue;
        }
        let body = blocking(|| snap.read_body(ri.val.leaf().unwrap().ptr))?;
        let inner = hex::encode(uuid::Uuid::new_v4().as_bytes());
        pieces.push(Piece::Data(Bytes::from(format!(
            "Content-Type: multipart/related; boundary=\"{inner}\"\r\n\r\n--{inner}\r\nContent-Type: application/json\r\n\r\n"
        ))));
        pieces.push(Piece::Data(Bytes::from(doc_bytes)));
        for name in follows {
            let Some(att) = body.atts.iter().find(|a| a.name == name) else { continue };
            pieces.push(Piece::Data(Bytes::from(format!(
                "\r\n--{inner}\r\nContent-Disposition: attachment; filename=\"{}\"\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n",
                name.replace('"', "\\\""),
                att.content_type,
                att.encoded_length
            ))));
            pieces.push(Piece::Att(att.clone()));
        }
        pieces.push(Piece::Data(Bytes::from(format!("\r\n--{inner}--\r\n"))));
    }
    pieces.push(Piece::Data(Bytes::from(format!("--{boundary}--"))));
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, format!("multipart/mixed; boundary=\"{boundary}\""))
        .body(stream_pieces(snap.clone(), pieces))
        .unwrap())
}

/// Byte ranges from a Range header (mochiweb's parse_range_request/1 and
/// parse_ranges/2): None for no usable header, or the whole body.
fn parse_ranges(h: Option<&str>, len: u64) -> Result<Option<Vec<(u64, u64)>>> {
    let Some(spec) = h.and_then(|h| h.trim().strip_prefix("bytes=")) else { return Ok(None) };
    let mut raw = Vec::new();
    for part in spec.split(',').map(str::trim) {
        let Some((a, b)) = part.split_once('-') else { return Ok(None) };
        let num = |s: &str| -> std::result::Result<Option<u64>, ()> { if s.is_empty() { Ok(None) } else { s.parse().map(Some).map_err(|_| ()) } };
        match (num(a), num(b)) {
            (Ok(None), Ok(None)) | (Err(_), _) | (_, Err(_)) => return Ok(None),
            (Ok(x), Ok(y)) => raw.push((x, y)),
        }
    }
    let mut out = Vec::new();
    for (i, r) in raw.into_iter().enumerate() {
        let (from, to) = match r {
            (Some(0), None) if i == 0 => return Ok(None),
            (Some(f), Some(t)) if t < f => {
                return Err(CouchError::new(StatusCode::RANGE_NOT_SATISFIABLE, "requested_range_not_satisfiable", "Requested range not satisfiable"));
            }
            (Some(f), Some(t)) => (f, t.min(len.saturating_sub(1))),
            (None, Some(t)) => (len.saturating_sub(t), len.saturating_sub(1)),
            (Some(f), None) => (f, len.saturating_sub(1)),
            (None, None) => return Ok(None),
        };
        out.push((from, to));
    }
    Ok(Some(out))
}

/// GET of an attachment (db_attachment_req/4): encoded when stored so and
/// the client accepts it, else in identity form; ranges of identity
/// attachments.
fn attachment_response(req: &Req, snap: &Snapshot, att: &AttInfo) -> Result<Response> {
    let etag = format!("\"{}\"", crate::doc::B64.encode(&att.md5));
    let accepts = |enc: &str| {
        req.header("accept-encoding").is_some_and(|h| {
            h.split(',').any(|p| {
                let mut it = p.split(';');
                let name = it.next().unwrap_or("").trim();
                let q0 = it.any(|q| q.trim().strip_prefix("q=").is_some_and(|v| v.trim().parse::<f64>().is_ok_and(|q| q == 0.0)));
                (name.eq_ignore_ascii_case(enc) || name == "*") && !q0
            })
        })
    };
    let enc = att.encoding.clone();
    let send_encoded = enc.as_deref().is_some_and(accepts);
    let config = &req.server.config;
    let mut b = Response::builder()
        .header(header::ETAG, &etag)
        .header(header::CACHE_CONTROL, "must-revalidate")
        .header(header::CONTENT_TYPE, &att.content_type);
    if send_encoded && let Some(e) = &enc {
        b = b.header(header::CONTENT_ENCODING, e);
    }
    b = b.header(header::ACCEPT_RANGES, if enc.is_none() { "bytes" } else { "none" });
    if config.get_bool("csp", "attachments_enable", true) {
        b = b.header("Content-Security-Policy", config.get_or("csp", "attachments_header_value", "sandbox"));
    }
    if send_encoded || enc.is_none() {
        let len = att.encoded_length;
        if enc.is_none()
            && let Some(ranges) = parse_ranges(req.header("range"), len)?
        {
            let data = blocking(|| snap.read_att(att))?;
            let slice = |(f, t): (u64, u64)| Bytes::copy_from_slice(&data[f.min(len) as usize..(t + 1).min(len) as usize]);
            if ranges.len() == 1 {
                let (f, t) = ranges[0];
                return Ok(b
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header("Content-Range", format!("bytes {f}-{t}/{len}"))
                    .header(header::CONTENT_LENGTH, t + 1 - f)
                    .body(Body::from(slice((f, t))))
                    .unwrap());
            }
            if ranges.len() < 10 {
                let boundary = hex::encode(uuid::Uuid::new_v4().as_bytes());
                let mut out = format!("--{boundary}").into_bytes();
                for (f, t) in ranges {
                    out.extend_from_slice(format!("\r\nContent-Type: {}\r\nContent-Range: bytes {f}-{t}/{len}\r\n\r\n", att.content_type).as_bytes());
                    out.extend_from_slice(&slice((f, t)));
                    out.extend_from_slice(format!("\r\n--{boundary}").as_bytes());
                }
                out.extend_from_slice(b"--");
                return Ok(Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_TYPE, format!("multipart/byteranges; boundary=\"{boundary}\""))
                    .body(Body::from(out))
                    .unwrap());
            }
        }
        return Ok(b.status(StatusCode::OK).header(header::CONTENT_LENGTH, len).body(stream_pieces(snap.clone(), vec![Piece::Att(att.clone())])).unwrap());
    }
    // Stored encoded, the client wants identity: decode it.
    let data = blocking(|| snap.read_att_decoded(att))?;
    Ok(b.status(StatusCode::OK).header(header::CONTENT_LENGTH, data.len()).body(Body::from(data)).unwrap())
}

enum Piece {
    Data(Bytes),
    Att(AttInfo),
}

/// Stream a response made of literal pieces and attachment data, reading
/// attachment chunks from disk only as the client consumes them.
fn stream_pieces(snap: Snapshot, pieces: Vec<Piece>) -> Body {
    let stream = async_stream::stream! {
        for p in pieces {
            match p {
                Piece::Data(b) => yield Ok::<Bytes, std::io::Error>(b),
                Piece::Att(att) => {
                    for group in att.chunks.chunks(16) {
                        let read = || -> std::io::Result<Vec<u8>> {
                            let mut buf = Vec::new();
                            for pos in group {
                                buf.extend_from_slice(&snap.read_chunk(*pos)?);
                            }
                            Ok(buf)
                        };
                        // A single chunk (up to 64 KB) is a point read.
                        let r = if att.chunks.len() <= 1 { read() } else { tokio::task::block_in_place(read) };
                        match r {
                            Ok(b) => yield Ok(Bytes::from(b)),
                            Err(e) => {
                                yield Err(e);
                                return;
                            }
                        }
                    }
                }
            }
        }
    };
    Body::from_stream(stream)
}

/// Parse a multipart/related document upload: JSON first, then one part
/// per attachment marked `"follows": true`, in order.
async fn parse_multipart_doc(req: &Req) -> Result<Doc> {
    let ct = req.content_type();
    // multer's parse_boundary only accepts multipart/form-data.
    let boundary = ct
        .split(';')
        .filter_map(|p| p.trim().strip_prefix("boundary="))
        .next()
        .map(|b| b.trim_matches('"').to_string())
        .ok_or_else(|| CouchError::bad_request("multipart boundary missing"))?;
    let stream = futures::stream::once(futures::future::ready(Ok::<_, std::io::Error>(req.body.clone())));
    let mut mp = multer::Multipart::new(stream, boundary);
    let first = mp
        .next_field()
        .await
        .map_err(|e| CouchError::bad_request(e.to_string()))?
        .ok_or_else(|| CouchError::bad_request("empty multipart body"))?;
    let json_bytes = first.bytes().await.map_err(|e| CouchError::bad_request(e.to_string()))?;
    let mut doc = from_json(serde_json::from_slice(&json_bytes)?)?;
    let mut parts: Vec<(Option<String>, bytes::Bytes)> = Vec::new();
    while let Some(f) = mp.next_field().await.map_err(|e| CouchError::bad_request(e.to_string()))? {
        let name = f.file_name().map(String::from);
        let data = f.bytes().await.map_err(|e| CouchError::bad_request(e.to_string()))?;
        parts.push((name, data));
    }
    let mut it = parts.into_iter();
    let mut pending: Vec<(Option<String>, bytes::Bytes)> = Vec::new();
    for a in doc.atts.iter_mut().filter(|a| matches!(a.data, AttData::Follows)) {
        let p = match pending.iter().position(|(n, _)| n.as_deref() == Some(a.name.as_str())) {
            Some(i) => pending.remove(i),
            None => loop {
                match it.next() {
                    Some((n, d)) if n.is_none() || n.as_deref() == Some(a.name.as_str()) => break (n, d),
                    Some(other) => pending.push(other),
                    None => {
                        return Err(CouchError::bad_request(format!(
                            "attachment {} data missing from multipart body",
                            a.name
                        )));
                    }
                }
            },
        };
        let bytes = p.1;
        if a.encoding.is_none() {
            a.length = bytes.len() as u64;
            a.md5 = Md5::digest(&bytes).to_vec();
        }
        a.encoded_length = bytes.len() as u64;
        a.data = AttData::Bytes(bytes);
    }
    Ok(doc)
}

async fn put_doc(req: &Req, db: &Arc<Db>, id: &str) -> Result<Response> {
    validate_docid(id)?;
    let mut doc = if req.content_type().starts_with("multipart/related") {
        parse_multipart_doc(req).await?
    } else {
        from_json(req.json_body()?)?
    };
    doc.id = id.to_string();
    if doc.revs.1.is_empty() {
        if let Some(r) = rev_from_req(req) {
            if doc.is_local() {
                doc.revs = (0, vec![r.trim_start_matches("0-").to_string()]);
            } else {
                let (p, rid) = parse_rev(&r)?;
                doc.revs = (p, vec![rid]);
            }
        }
    } else if doc.is_local() {
        doc.revs = (0, vec![doc.rev().unwrap_or_default().trim_start_matches("0-").to_string()]);
    }
    let new_edits = req.q_bool("new_edits")?.unwrap_or(true);
    let rev = save_docs(req, db, vec![doc], new_edits).await?.pop().unwrap()?;
    let status = if req.q("batch") == Some("ok") {
        StatusCode::ACCEPTED
    } else {
        StatusCode::CREATED
    };
    let r = json_response(status, &json!({"ok": true, "id": id, "rev": rev}));
    let r = with_header(r, header::ETAG, &format!("\"{rev}\""));
    Ok(with_header(r, header::LOCATION, &format!("/{}/{}", db.name, id)))
}

async fn copy_doc(req: &Req, db: &Arc<Db>, id: &str) -> Result<Response> {
    let dest = req
        .header("destination")
        .ok_or_else(|| CouchError::bad_request("Destination header is mandatory for COPY."))?;
    let (dest_id, dest_rev) = match dest.split_once("?rev=") {
        Some((d, r)) => (d.to_string(), Some(r.to_string())),
        None => (dest.to_string(), None),
    };
    let dest_id = percent_encoding::percent_decode_str(&dest_id).decode_utf8_lossy().to_string();
    let snap = db.snapshot();
    let fdi = snap.get_fdi(id)?.ok_or_else(CouchError::missing)?;
    let rev = match req.q("rev") {
        Some(r) => {
            let (p, rid) = parse_rev(r)?;
            Snapshot::find_rev(&fdi, p, &rid).ok_or_else(CouchError::missing)?
        }
        None => fdi.winner(),
    };
    let mut doc = snap.load_doc(&fdi, &rev)?;
    doc.id = dest_id.clone();
    doc.revs = match dest_rev {
        Some(r) => {
            let (p, rid) = parse_rev(&r)?;
            (p, vec![rid])
        }
        None => (0, vec![]),
    };
    let new_rev = save_docs(req, db, vec![doc], true).await?.pop().unwrap()?;
    let r = json_response(StatusCode::CREATED, &json!({"ok": true, "id": dest_id, "rev": new_rev}));
    Ok(with_header(r, header::ETAG, &format!("\"{new_rev}\"")))
}

// -------------------------------------------------------------- attachments

async fn att_request(req: &Req, db: &Arc<Db>, id: &str, name: &str) -> Result<Response> {
    match req.method {
        Method::GET | Method::HEAD => {
            let snap = db.snapshot();
            let fdi = snap.get_fdi(id)?.ok_or_else(CouchError::missing)?;
            let rev = match req.q("rev") {
                Some(r) => {
                    let (p, rid) = parse_rev(r)?;
                    Snapshot::find_rev(&fdi, p, &rid).ok_or_else(CouchError::missing)?
                }
                None => {
                    let w = fdi.winner();
                    if w.deleted {
                        return Err(CouchError::deleted());
                    }
                    w
                }
            };
            let leaf = rev.val.leaf().ok_or_else(CouchError::missing)?;
            let body = snap.read_body(leaf.ptr)?;
            let att = body
                .atts
                .iter()
                .find(|a| a.name == name)
                .ok_or_else(|| CouchError::not_found("Document is missing attachment"))?;
            let etag = format!("\"{}\"", crate::doc::B64.encode(&att.md5));
            if req.header("if-none-match") == Some(etag.as_str()) {
                return Ok(with_header(empty(StatusCode::NOT_MODIFIED), header::ETAG, &etag));
            }
            attachment_response(req, &snap, att)
        }
        Method::PUT | Method::DELETE => {
            let snap = db.snapshot();
            let fdi = snap.get_fdi(id)?;
            let rev = rev_from_req(req);
            let mut doc = match (&fdi, &rev) {
                (Some(f), Some(r)) => {
                    let (p, rid) = parse_rev(r)?;
                    let ri = Snapshot::find_rev(f, p, &rid).ok_or_else(CouchError::conflict)?;
                    snap.load_doc(f, &ri)?
                }
                (Some(f), None) if f.deleted && req.method == Method::PUT => Doc {
                    id: id.to_string(),
                    ..Default::default()
                },
                (Some(_), None) => return Err(CouchError::conflict()),
                (None, None) if req.method == Method::PUT => Doc {
                    id: id.to_string(),
                    ..Default::default()
                },
                (None, _) => {
                    return Err(if req.method == Method::DELETE {
                        CouchError::missing()
                    } else {
                        CouchError::conflict()
                    });
                }
            };
            // Load replaces the rev path; only the head is needed to edit.
            if !doc.revs.1.is_empty() {
                doc.revs.1.truncate(1);
            }
            doc.deleted = false;
            // Keep existing attachments as stubs: the writer resolves them
            // from the parent revision, so they can't go stale if the file
            // is compacted while this request is in flight.
            for a in doc.atts.iter_mut() {
                a.data = AttData::Stub;
            }
            doc.att_gen = 0;
            if req.method == Method::PUT {
                let ct = req
                    .header("content-type")
                    .unwrap_or("application/octet-stream")
                    .to_string();
                let data = req.body.clone();
                // Uploaded gzipped, it is kept gzipped (its identity length
                // unknown).
                let encoding = match req.header("content-encoding").map(|e| e.trim().to_ascii_lowercase()).as_deref() {
                    None | Some("identity") => None,
                    Some("gzip") => Some("gzip".to_string()),
                    Some(_) => {
                        return Err(CouchError::new(
                            StatusCode::UNSUPPORTED_MEDIA_TYPE,
                            "bad_content_type",
                            "Only gzip and identity content-encodings are supported",
                        ));
                    }
                };
                doc.atts.retain(|a| a.name != name);
                doc.atts.push(Att {
                    name: name.to_string(),
                    content_type: ct,
                    revpos: None,
                    length: data.len() as u64,
                    md5: Md5::digest(&data).to_vec(),
                    encoding,
                    encoded_length: data.len() as u64,
                    stored_length: 0,
                    data: AttData::Bytes(data),
                });
            } else {
                let before = doc.atts.len();
                doc.atts.retain(|a| a.name != name);
                if before == doc.atts.len() {
                    return Err(CouchError::not_found("Document is missing attachment"));
                }
            }
            let new_rev = save_docs(req, db, vec![doc], true).await?.pop().unwrap()?;
            let status = if req.method == Method::PUT { StatusCode::CREATED } else { StatusCode::OK };
            let r = json_response(status, &json!({"ok": true, "id": id, "rev": new_rev}));
            Ok(with_header(r, header::ETAG, &format!("\"{new_rev}\"")))
        }
        _ => not_allowed("DELETE,GET,HEAD,PUT"),
    }
}

// ----------------------------------------------------------------- all_docs

#[derive(Clone, Copy, PartialEq)]
pub enum AllDocsKind {
    All,
    Design,
    Local,
}

/// Arguments shared by _all_docs and views (couch_mrview_http:parse_params).
#[derive(Clone, Debug)]
pub struct QueryArgs {
    pub keys: Option<Vec<Value>>,
    pub start_key: Option<Value>,
    pub end_key: Option<Value>,
    pub start_docid: Option<String>,
    pub end_docid: Option<String>,
    pub inclusive_end: bool,
    pub descending: bool,
    pub limit: usize,
    pub skip: usize,
    pub include_docs: bool,
    pub conflicts: bool,
    pub update_seq: bool,
    pub reduce: Option<bool>,
    pub group: bool,
    pub group_level: Option<usize>,
    pub update: String,
    pub attachments: bool,
    pub att_encoding_info: bool,
    pub sorted: bool,
    /// Set for queries within a partition (/db/_partition/P/...).
    pub partition: Option<String>,
}

impl Default for QueryArgs {
    fn default() -> Self {
        QueryArgs {
            keys: None,
            start_key: None,
            end_key: None,
            start_docid: None,
            end_docid: None,
            inclusive_end: true,
            descending: false,
            limit: usize::MAX,
            skip: 0,
            include_docs: false,
            conflicts: false,
            update_seq: false,
            reduce: None,
            group: false,
            group_level: None,
            update: "true".into(),
            attachments: false,
            att_encoding_info: false,
            sorted: true,
            partition: None,
        }
    }
}

fn qerr(msg: impl Into<String>) -> CouchError {
    CouchError::new(StatusCode::BAD_REQUEST, "query_parse_error", msg)
}

impl QueryArgs {
    /// Apply one parameter from a query string (JSON-encoded value) or body.
    pub fn set(&mut self, k: &str, v: Value) -> Result<()> {
        let as_bool = |v: &Value| -> Result<bool> {
            match v {
                Value::Bool(b) => Ok(*b),
                Value::String(s) if s == "true" => Ok(true),
                Value::String(s) if s == "false" => Ok(false),
                _ => Err(qerr(format!("Invalid boolean parameter: {v}"))),
            }
        };
        let as_uint = |v: &Value| -> Result<usize> {
            match v {
                Value::Number(n) if n.as_u64().is_some() => Ok(n.as_u64().unwrap() as usize),
                Value::String(s) => s.parse().map_err(|_| qerr(format!("Invalid value for positive integer: \"{s}\""))),
                _ => Err(qerr(format!("Invalid value for positive integer: {v}"))),
            }
        };
        let as_str = |v: &Value| -> String {
            match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            }
        };
        match k {
            "key" => {
                self.start_key = Some(v.clone());
                self.end_key = Some(v);
            }
            "keys" => match v {
                Value::Array(a) => self.keys = Some(a),
                _ => return Err(qerr("`keys` member must be an array.")),
            },
            "startkey" | "start_key" => self.start_key = Some(v),
            "endkey" | "end_key" => self.end_key = Some(v),
            "startkey_docid" | "start_key_doc_id" => self.start_docid = Some(as_str(&v)),
            "endkey_docid" | "end_key_doc_id" => self.end_docid = Some(as_str(&v)),
            "inclusive_end" => self.inclusive_end = as_bool(&v)?,
            "descending" => self.descending = as_bool(&v)?,
            "limit" => self.limit = as_uint(&v)?,
            "skip" => self.skip = as_uint(&v)?,
            "include_docs" => self.include_docs = as_bool(&v)?,
            "conflicts" => self.conflicts = as_bool(&v)?,
            "update_seq" => self.update_seq = as_bool(&v)?,
            "reduce" => self.reduce = Some(as_bool(&v)?),
            "group" => self.group = as_bool(&v)?,
            "group_level" => {
                let l = as_uint(&v)?;
                self.group_level = Some(l);
            }
            "update" => self.update = as_str(&v),
            "stale" => {
                let s = as_str(&v);
                self.update = match s.as_str() {
                    "ok" => "false".into(),
                    "update_after" => "lazy".into(),
                    _ => return Err(qerr("Invalid value for `stale`.")),
                };
            }
            "stable" | "partition" | "r" | "n" | "w" | "timeout" | "callback" | "list" | "_" => {}
            "attachments" => self.attachments = as_bool(&v)?,
            "att_encoding_info" => self.att_encoding_info = as_bool(&v)?,
            "sorted" => self.sorted = as_bool(&v)?,
            "queries" => {}
            _ => {}
        }
        Ok(())
    }

    pub fn from_req(req: &Req) -> Result<QueryArgs> {
        let mut a = QueryArgs::default();
        for (k, v) in &req.query {
            let jv = match k.as_str() {
                "key" | "keys" | "startkey" | "start_key" | "endkey" | "end_key" => {
                    serde_json::from_str(v).map_err(|_| qerr(format!("Invalid JSON for {k}: {v}")))?
                }
                _ => Value::String(v.clone()),
            };
            a.set(k, jv)?;
        }
        Ok(a)
    }

    pub fn from_obj(obj: &Map<String, Value>, base: QueryArgs) -> Result<QueryArgs> {
        let mut a = base;
        for (k, v) in obj {
            a.set(k, v.clone())?;
        }
        Ok(a)
    }

    pub fn doc_opts(&self) -> DocOpts {
        DocOpts {
            conflicts: self.conflicts,
            attachments: self.attachments,
            att_encoding_info: self.att_encoding_info,
            ..Default::default()
        }
    }
}

async fn all_docs(req: &Req, db: &Arc<Db>, kind: AllDocsKind) -> Result<Response> {
    let mut args = QueryArgs::from_req(req)?;
    match req.method {
        Method::GET | Method::HEAD => {}
        Method::POST => {
            let body = req.json_obj_body()?;
            args = QueryArgs::from_obj(&body, args)?;
        }
        _ => return not_allowed("GET,HEAD,POST"),
    }
    let snap = db.snapshot();
    let v = blocking(|| run_all_docs(&snap, &args, kind))?;
    ok_json(v)
}

async fn multi_query(req: &Req, db: &Arc<Db>, kind: AllDocsKind) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let body = req.json_obj_body()?;
    let queries = body
        .get("queries")
        .and_then(|q| q.as_array())
        .ok_or_else(|| CouchError::bad_request("`queries` member must be an array"))?;
    let snap = db.snapshot();
    let mut results = Vec::new();
    for q in queries {
        let obj = q.as_object().ok_or_else(|| CouchError::bad_request("query must be an object"))?;
        let args = QueryArgs::from_obj(obj, QueryArgs::default())?;
        results.push(blocking(|| run_all_docs(&snap, &args, kind))?);
    }
    ok_json(json!({"results": results}))
}

fn key_str(v: &Value) -> Option<String> {
    v.as_str().map(String::from)
}

/// /db/_partition/P/...: the partition's info, _all_docs, views, _find
/// and _explain.
async fn partition_request(req: &Req, db: &Arc<Db>, partition: &str, rest: &[String]) -> Result<Response> {
    if !db.snapshot().partitioned() {
        return Err(CouchError::bad_request("database is not partitioned"));
    }
    crate::doc::validate_partition(partition)?;
    let r: Vec<&str> = rest.iter().map(|s| s.as_str()).collect();
    match r.as_slice() {
        ["_all_docs", ..] => crate::stat!("couchdb.httpd.partition_all_docs_requests").inc(),
        ["_design", ..] => crate::stat!("couchdb.httpd.partition_view_requests").inc(),
        ["_find", ..] => crate::stat!("couchdb.httpd.partition_find_requests").inc(),
        ["_explain", ..] => crate::stat!("couchdb.httpd.partition_explain_requests").inc(),
        _ => {}
    }
    match r.as_slice() {
        [] => match req.method {
            Method::GET | Method::HEAD => ok_json(db.snapshot().partition_info(partition)?),
            _ => not_allowed("GET,HEAD"),
        },
        ["_all_docs"] => {
            let mut args = QueryArgs::from_req(req)?;
            match req.method {
                Method::GET | Method::HEAD => {}
                Method::POST => args = QueryArgs::from_obj(&req.json_obj_body()?, args)?,
                _ => return not_allowed("GET,HEAD,POST"),
            }
            let snap = db.snapshot();
            ok_json(blocking(|| run_partition_all_docs(&snap, &args, partition))?)
        }
        ["_design", ddoc, "_view", view] => {
            let mut args = QueryArgs::from_req(req)?;
            match req.method {
                Method::GET | Method::HEAD => {}
                Method::POST => args = QueryArgs::from_obj(&req.json_obj_body()?, args)?,
                _ => return not_allowed("GET,POST,HEAD"),
            }
            args.partition = Some(partition.to_string());
            ok_json(crate::views::view_json(req, db, &format!("_design/{ddoc}"), view, &args)?)
        }
        ["_design", ddoc, "_view", view, "queries"] => {
            if req.method != Method::POST {
                return not_allowed("POST");
            }
            let body = req.json_obj_body()?;
            let queries = body
                .get("queries")
                .and_then(|q| q.as_array())
                .cloned()
                .ok_or_else(|| CouchError::bad_request("`queries` member must be an array"))?;
            let base = QueryArgs::from_req(req)?;
            let mut results = Vec::new();
            for q in &queries {
                let obj = q.as_object().ok_or_else(|| CouchError::bad_request("query must be an object"))?;
                let mut args = QueryArgs::from_obj(obj, base.clone())?;
                args.partition = Some(partition.to_string());
                results.push(crate::views::view_json(req, db, &format!("_design/{ddoc}"), view, &args)?);
            }
            ok_json(json!({"results": results}))
        }
        ["_find"] => crate::mango::http_find_in(req, db, Some(partition)).await,
        ["_explain"] => crate::mango::http_explain_in(req, db, Some(partition)).await,
        ["_design", ddoc, "_nouveau", rest @ ..] => {
            crate::nouveau::http_search(req, db, &format!("_design/{ddoc}"), rest, Some(partition)).await
        }
        ["_design", ddoc, "_nouveau_info", rest @ ..] => crate::nouveau::http_info(req, db, &format!("_design/{ddoc}"), rest),
        ["_design", ddoc, kind @ ("_search" | "_search_info" | "_search_disk_size"), rest @ ..] => {
            crate::nouveau::http_dreyfus(req, db, &format!("_design/{ddoc}"), kind, rest)
        }
        _ => Err(CouchError::not_found("missing")),
    }
}

/// _all_docs within one partition: ids from "P:" up to "P;". Like
/// CouchDB, total_rows and offset still count the whole database.
pub fn run_partition_all_docs(snap: &Snapshot, args: &QueryArgs, partition: &str) -> Result<Value> {
    let (lo, hi) = (json!(format!("{partition}:")), json!(format!("{partition};")));
    // Keys are clamped into the partition as CouchDB does, comparing by
    // Erlang term order: ids as raw bytes, any non-string below them all.
    let cmp = |x: &Value, y: &Value| match (x.as_str(), y.as_str()) {
        (Some(x), Some(y)) => x.cmp(y),
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    };
    let mut a = args.clone();
    if a.keys.is_none() {
        let (first, last) = if a.descending { (&hi, &lo) } else { (&lo, &hi) };
        let past = |k: &Value, bound: &Value| if a.descending { cmp(k, bound).is_gt() } else { cmp(k, bound).is_lt() };
        a.start_key = Some(match a.start_key.take() {
            Some(k) if !past(&k, first) => k,
            _ => first.clone(),
        });
        match a.end_key.take() {
            Some(k) if !past(last, &k) => a.end_key = Some(k),
            _ => {
                a.end_key = Some(last.clone());
                a.inclusive_end = a.descending;
            }
        }
    }
    // As in CouchDB, keys= lookups aren't limited to the partition.
    run_all_docs(snap, &a, AllDocsKind::All)
}

pub fn run_all_docs(snap: &Snapshot, args: &QueryArgs, kind: AllDocsKind) -> Result<Value> {
    let opts = args.doc_opts();
    let mut rows = Vec::new();
    if kind == AllDocsKind::Local {
        let range = Range {
            start: args.start_key.as_ref().and_then(key_str).or(Some("_local/".into())),
            end: args.end_key.as_ref().and_then(key_str).or(Some("_local0".into())),
            inclusive_end: args.inclusive_end,
        };
        let dir = if args.descending { Dir::Rev } else { Dir::Fwd };
        let range = if args.descending && args.start_key.is_none() {
            Range { start: Some("_local0".into()), end: Some("_local/".into()), ..range }
        } else {
            range
        };
        let mut skip = args.skip;
        snap.fold_locals(&range, dir, |id, l| {
            if skip > 0 {
                skip -= 1;
                return true;
            }
            let mut row = json!({"id": id, "key": id, "value": {"rev": format!("0-{}", l.rev)}});
            if args.include_docs {
                row["doc"] = local_json(id, l);
            }
            rows.push(row);
            rows.len() < args.limit
        })?;
        return Ok(json!({"total_rows": null, "offset": null, "rows": rows}));
    }
    let total = snap.doc_count();
    if let Some(keys) = &args.keys {
        let keys: Vec<&Value> = if args.descending { keys.iter().rev().collect() } else { keys.iter().collect() };
        for k in keys.into_iter().skip(args.skip).take(args.limit) {
            let Some(id) = k.as_str() else {
                rows.push(json!({"key": k, "error": "not_found"}));
                continue;
            };
            match snap.get_fdi(id)? {
                None => rows.push(json!({"key": id, "error": "not_found"})),
                Some(fdi) => {
                    let w = fdi.winner();
                    if w.deleted {
                        let mut row = json!({"id": id, "key": id, "value": {"rev": w.rev(), "deleted": true}});
                        if args.include_docs {
                            row["doc"] = Value::Null;
                        }
                        rows.push(row);
                    } else {
                        let mut row = json!({"id": id, "key": id, "value": {"rev": w.rev()}});
                        if args.include_docs {
                            row["doc"] = snap.doc_json(&fdi, &w, &opts)?;
                            crate::db::count_reads(snap, 1);
                        }
                        rows.push(row);
                    }
                }
            }
        }
        let mut out = json!({"total_rows": total, "offset": null, "rows": rows});
        if args.update_seq {
            out["update_seq"] = json!(snap.seq_str(snap.update_seq()));
        }
        return Ok(out);
    }
    let dir = if args.descending { Dir::Rev } else { Dir::Fwd };
    let mut start = args.start_key.as_ref().and_then(key_str);
    let mut end = args.end_key.as_ref().and_then(key_str);
    if kind == AllDocsKind::Design {
        let (lo, hi) = ("_design/".to_string(), "_design0".to_string());
        if args.descending {
            start = Some(start.map(|s| s.min(hi.clone())).unwrap_or(hi.clone()));
            end = Some(end.map(|e| e.max(lo.clone())).unwrap_or(lo.clone()));
        } else {
            start = Some(start.map(|s| s.max(lo.clone())).unwrap_or(lo.clone()));
            end = Some(end.map(|e| e.min(hi.clone())).unwrap_or(hi.clone()));
        }
    }
    let range = Range {
        start: start.clone(),
        end,
        inclusive_end: args.inclusive_end,
    };
    let offset = match &start {
        Some(s) => snap.count_docs_before(s, dir)?,
        None => 0,
    };
    let mut skip = args.skip;
    let limit = args.limit;
    if limit > 0 {
        snap.fold_docs(dir, &range, |fdi| {
            if fdi.deleted {
                return Ok(true);
            }
            if skip > 0 {
                skip -= 1;
                return Ok(true);
            }
            let w = fdi.winner();
            let mut row = json!({"id": fdi.id, "key": fdi.id, "value": {"rev": w.rev()}});
            if args.include_docs {
                row["doc"] = snap.doc_json(&fdi, &w, &opts)?;
                crate::db::count_reads(snap, 1);
            }
            rows.push(row);
            Ok(rows.len() < limit)
        })?;
    }
    let mut out = json!({"total_rows": total, "offset": offset + args.skip.min(total as usize) as u64, "rows": rows});
    if args.update_seq {
        out["update_seq"] = json!(snap.seq_str(snap.update_seq()));
    }
    Ok(out)
}

// --------------------------------------------------------------- bulk docs

async fn bulk_docs(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    crate::stat!("couchdb.httpd.bulk_requests").inc();
    let body = req.json_obj_body()?;
    let docs = body
        .get("docs")
        .and_then(|d| d.as_array())
        .ok_or_else(|| CouchError::bad_request("POST body must include `docs` parameter."))?;
    crate::stat!("couchdb.httpd.bulk_docs").observe(docs.len() as f64);
    let new_edits = body.get("new_edits").and_then(|v| v.as_bool()).unwrap_or(true);
    let mut parsed = Vec::with_capacity(docs.len());
    for d in docs {
        let mut doc = from_json(d.clone())?;
        if doc.id.is_empty() {
            if !new_edits {
                return Err(CouchError::bad_request("Document id must be specified with new_edits=false"));
            }
            doc.id = new_uuid();
        }
        validate_docid(&doc.id)?;
        if doc.is_local() && !doc.revs.1.is_empty() {
            let r = doc.rev().unwrap();
            doc.revs = (0, vec![r.split_once('-').map(|x| x.1.to_string()).unwrap_or(r)]);
        }
        parsed.push(doc);
    }
    let ids: Vec<String> = parsed.iter().map(|d| d.id.clone()).collect();
    let results = save_docs(req, db, parsed, new_edits).await?;
    let mut out = Vec::new();
    for (id, r) in ids.into_iter().zip(results) {
        match r {
            Ok(rev) => {
                if new_edits {
                    out.push(json!({"ok": true, "id": id, "rev": rev}));
                }
            }
            Err(e) => out.push(json!({"id": id, "error": e.error, "reason": e.reason})),
        }
    }
    Ok(json_response(StatusCode::CREATED, &Value::Array(out)))
}

async fn bulk_get(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    crate::stat!("couchdb.httpd.bulk_requests").inc();
    let body = req.json_obj_body()?;
    let docs = body
        .get("docs")
        .and_then(|d| d.as_array())
        .ok_or_else(|| CouchError::bad_request("Missing JSON list of 'docs'."))?;
    let base = doc_opts(req)?;
    let latest = req.flag("latest")?;
    let snap = db.snapshot();
    let mut results = Vec::new();
    for d in docs {
        let id = d.get("id").and_then(|i| i.as_str()).unwrap_or("");
        if id.is_empty() {
            results.push(json!({"id": null, "docs": [{"error": {"id": null, "rev": null, "error": "illegal_docid", "reason": "Document id must not be empty"}}]}));
            continue;
        }
        let mut opts = base.clone();
        if let Some(Value::Array(a)) = d.get("atts_since") {
            opts.atts_since = Some(a.iter().filter_map(|r| r.as_str().and_then(|s| parse_rev(s).ok())).collect());
        }
        let spec = match d.get("rev").and_then(|r| r.as_str()) {
            Some(r) => json!([r]),
            None => json!("all"),
        };
        let res = blocking(|| open_revs(&snap, id, &spec, latest, &opts));
        let rev = d.get("rev").cloned().unwrap_or(Value::Null);
        let entries: Vec<Value> = match res {
            Ok(list) if !list.is_empty() => list
                .into_iter()
                .map(|r| match r.get("ok") {
                    Some(doc) => json!({"ok": doc}),
                    None => json!({"error": {"id": id, "rev": r.get("missing"), "error": "not_found", "reason": "missing"}}),
                })
                .collect(),
            Ok(_) => vec![json!({"error": {"id": id, "rev": rev, "error": "not_found", "reason": "missing"}})],
            Err(e) => vec![json!({"error": {"id": id, "rev": rev, "error": e.error, "reason": e.reason}})],
        };
        results.push(json!({"id": id, "docs": entries}));
    }
    ok_json(json!({"results": results}))
}

// -------------------------------------------------------------- misc db ops

fn compact(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    if !req.content_type().starts_with("application/json") {
        return Err(CouchError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "bad_content_type",
            "Content-Type must be application/json",
        ));
    }
    db.start_compaction()?;
    Ok(json_response(StatusCode::ACCEPTED, &json!({"ok": true})))
}

fn revs_diff_map(req: &Req, db: &Arc<Db>) -> Result<Vec<(String, Vec<String>, Vec<String>)>> {
    let body = req.json_obj_body()?;
    let snap = db.snapshot();
    let mut out = Vec::new();
    for (id, revs) in body {
        let revs = revs.as_array().ok_or_else(|| CouchError::bad_request("revs must be an array"))?;
        let fdi = snap.get_fdi(&id)?;
        let mut missing = Vec::new();
        let mut min_missing = u64::MAX;
        for r in revs {
            let rs = r.as_str().ok_or_else(|| CouchError::bad_request("Invalid rev format"))?;
            let (p, rid) = parse_rev(rs)?;
            let present = fdi.as_ref().map(|f| crate::keytree::contains(&f.tree, p, &rid)).unwrap_or(false);
            if !present {
                missing.push(rs.to_string());
                min_missing = min_missing.min(p);
            }
        }
        if missing.is_empty() {
            continue;
        }
        let ancestors: Vec<String> = fdi
            .map(|f| {
                f.sorted_leafs()
                    .into_iter()
                    .filter(|l| l.pos < missing.iter().filter_map(|m| parse_rev(m).ok()).map(|x| x.0).max().unwrap_or(0))
                    .map(|l| l.rev())
                    .collect()
            })
            .unwrap_or_default();
        let _ = min_missing;
        out.push((id, missing, ancestors));
    }
    Ok(out)
}

fn revs_diff(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let mut m = Map::new();
    for (id, missing, anc) in revs_diff_map(req, db)? {
        let mut e = json!({"missing": missing});
        if !anc.is_empty() {
            e["possible_ancestors"] = json!(anc);
        }
        m.insert(id, e);
    }
    ok_json(Value::Object(m))
}

fn missing_revs(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    let mut m = Map::new();
    for (id, missing, _) in revs_diff_map(req, db)? {
        m.insert(id, json!(missing));
    }
    ok_json(json!({"missing_revs": m}))
}

async fn revs_limit(req: &Req, db: &Arc<Db>) -> Result<Response> {
    match req.method {
        Method::GET => ok_json(json!(db.snapshot().header.revs_limit)),
        Method::PUT => {
            crate::auth::check_db_admin(&req.auth.user, &db.snapshot().security())?;
            let n = req
                .json_body()?
                .as_u64()
                .filter(|n| *n > 0)
                .ok_or_else(|| CouchError::bad_request("Rev limit has to be an integer"))?;
            db.set_revs_limit(n).await?;
            ok_json(json!({"ok": true}))
        }
        _ => not_allowed("GET,PUT"),
    }
}

async fn security(req: &Req, db: &Arc<Db>) -> Result<Response> {
    match req.method {
        Method::GET => ok_json(db.snapshot().security()),
        Method::PUT => {
            crate::auth::check_security_editable(&req.server.config, &db.name)?;
            crate::auth::check_db_admin(&req.auth.user, &db.snapshot().security())?;
            let sec = req.json_body()?;
            if !sec.is_object() {
                return Err(CouchError::bad_request("Security object must be a JSON object"));
            }
            crate::auth::validate_security(&sec)?;
            db.set_security(&sec).await?;
            ok_json(json!({"ok": true}))
        }
        _ => not_allowed("GET,PUT"),
    }
}

async fn purge(req: &Req, db: &Arc<Db>) -> Result<Response> {
    if req.method != Method::POST {
        return not_allowed("POST");
    }
    crate::stat!("couchdb.httpd.purge_requests").inc();
    let body = req.json_obj_body()?;
    let mut list = Vec::new();
    for (id, revs) in body {
        let revs = revs.as_array().ok_or_else(|| CouchError::bad_request("revs must be an array"))?;
        let mut rs = Vec::new();
        for r in revs {
            rs.push(parse_rev(r.as_str().unwrap_or(""))?);
        }
        list.push((id, rs));
    }
    let n = list.len() as i64;
    crate::stat!("couchdb.document_purges.total").add(n);
    let r = db.purge(list).await?;
    crate::stat!("couchdb.document_purges.success").add(n);
    Ok(json_response(StatusCode::CREATED, &r))
}
