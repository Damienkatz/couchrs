//! The _changes feed (couch_changes / chttpd_db changes handling) and
//! _db_updates.

use crate::api::blocking;
use crate::btree::Dir;
use crate::db::{Db, Snapshot, parse_seq};
use crate::doc::{DocOpts, FullDocInfo};
use crate::error::{CouchError, Result};
use crate::http::{Req, json_response, not_allowed};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

pub enum Filter {
    None,
    DocIds(HashSet<String>),
    Design,
    Selector(crate::mango::Selector),
    Js(crate::js::ChangesFilter),
    View(crate::js::ViewFilter),
}

pub struct ChangesArgs {
    pub feed: String,
    pub since: u64,
    pub descending: bool,
    pub limit: usize,
    pub include_docs: bool,
    pub all_docs_style: bool,
    pub heartbeat: Option<u64>,
    pub timeout: u64,
    pub doc_opts: DocOpts,
    pub filter: Filter,
    pub req_json: Value,
    pub seq_interval: Option<u64>,
    /// What each row becomes, or None to leave it out (and uncounted).
    pub transform: Option<fn(&Value) -> Option<Value>>,
    /// _db_updates: no pending counts.
    pub db_updates: bool,
}

fn parse_args(req: &Req, db: &Db) -> Result<ChangesArgs> {
    let body: Option<serde_json::Map<String, Value>> =
        if req.method == Method::POST && !req.body.is_empty() {
            Some(req.json_obj_body()?)
        } else {
            None
        };
    let feed = req.q("feed").unwrap_or("normal").to_string();
    if !matches!(feed.as_str(), "normal" | "longpoll" | "continuous" | "eventsource") {
        return Err(CouchError::bad_request("Supported `feed` types: normal, continuous, live, longpoll, eventsource"));
    }
    let snap = db.snapshot();
    let since_raw = req
        .q("since")
        .map(String::from)
        .or_else(|| req.header("last-event-id").map(String::from));
    let since = match since_raw.as_deref() {
        None | Some("0") | Some("") => 0,
        Some("now") => snap.update_seq(),
        Some(s) => {
            let v: Value = serde_json::from_str(s).unwrap_or(Value::String(s.to_string()));
            parse_seq(&v).ok_or_else(|| CouchError::bad_request("Malformed sequence supplied in 'since' parameter."))?
        }
    };
    let heartbeat = match req.q("heartbeat") {
        Some("true") => Some(60_000),
        Some(h) => Some(h.parse().map_err(|_| CouchError::bad_request("Invalid heartbeat value. Expecting a positive integer value (in milliseconds)."))?),
        None => None,
    };
    let filter_name = req.q("filter").map(String::from);
    let filter = match filter_name.as_deref() {
        None => Filter::None,
        Some("_doc_ids") => {
            let ids: Value = match (req.q("doc_ids"), &body) {
                (Some(q), _) => serde_json::from_str(q)?,
                (None, Some(b)) => b.get("doc_ids").cloned().unwrap_or(json!([])),
                _ => json!([]),
            };
            let ids = ids
                .as_array()
                .ok_or_else(|| CouchError::bad_request("`doc_ids` filter parameter is not a list of doc ids."))?
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            Filter::DocIds(ids)
        }
        Some("_design") => Filter::Design,
        Some("_selector") => {
            let sel = body
                .as_ref()
                .and_then(|b| b.get("selector"))
                .ok_or_else(|| CouchError::bad_request("Selector must be specified in POST payload"))?;
            Filter::Selector(crate::mango::Selector::parse(sel)?)
        }
        Some("_view") => {
            let view = req.q("view").ok_or_else(|| CouchError::bad_request("`view` filter parameter is not provided."))?;
            let (ddoc, vname) = view
                .split_once('/')
                .ok_or_else(|| CouchError::bad_request("`view` must be of the form `designname/viewname`"))?;
            let ddoc_json = load_ddoc(&snap, &format!("_design/{ddoc}"))?;
            Filter::View(crate::js::ViewFilter::new(&ddoc_json, vname)?)
        }
        Some(f) => {
            let (ddoc, fname) = f
                .split_once('/')
                .ok_or_else(|| CouchError::bad_request("`filter` must be of the form `designname/filtername`"))?;
            let ddoc_json = load_ddoc(&snap, &format!("_design/{ddoc}"))?;
            Filter::Js(crate::js::ChangesFilter::new(&ddoc_json, fname)?)
        }
    };
    let style = req.q("style").unwrap_or("main_only");
    let mut doc_opts = DocOpts {
        conflicts: req.flag("conflicts")?,
        attachments: req.flag("attachments")?,
        att_encoding_info: req.flag("att_encoding_info")?,
        ..Default::default()
    };
    if let Some(Value::Array(a)) = req.q_json("atts_since").ok().flatten() {
        doc_opts.atts_since = Some(a.iter().filter_map(|r| r.as_str().and_then(|s| crate::doc::parse_rev(s).ok())).collect());
    }
    let req_json = json!({
        "method": req.method.as_str(),
        "query": req.query.iter().map(|(k, v)| (k.clone(), json!(v))).collect::<serde_json::Map<_, _>>(),
        "headers": {},
        "body": body.map(Value::Object).unwrap_or(json!("undefined")),
        "userCtx": req.auth.user.json(&db.name),
        "info": {"db_name": db.name},
    });
    Ok(ChangesArgs {
        feed,
        since,
        descending: req.flag("descending")?,
        limit: req.q_u64("limit")?.map(|l| l as usize).unwrap_or(usize::MAX),
        include_docs: req.flag("include_docs")?,
        all_docs_style: style == "all_docs",
        heartbeat,
        timeout: req.q_u64("timeout")?.unwrap_or(60_000),
        doc_opts,
        filter,
        req_json,
        seq_interval: req.q_u64("seq_interval")?,
        transform: None,
        db_updates: false,
    })
}

pub fn load_ddoc(snap: &Snapshot, id: &str) -> Result<Value> {
    let fdi = snap.get_fdi(id)?.ok_or_else(CouchError::missing)?;
    let w = fdi.winner();
    if w.deleted {
        return Err(CouchError::deleted());
    }
    snap.doc_json(&fdi, &w, &DocOpts::default())
}

impl Filter {
    fn matches(&mut self, snap: &Snapshot, fdi: &FullDocInfo, req: &Value) -> Result<bool> {
        Ok(match self {
            Filter::None => true,
            Filter::DocIds(ids) => ids.contains(&fdi.id),
            Filter::Design => fdi.id.starts_with("_design/"),
            Filter::Selector(sel) => {
                let doc = snap.winner_json(fdi, &DocOpts::default())?;
                sel.matches(&doc)
            }
            Filter::Js(f) => {
                let doc = snap.winner_json(fdi, &DocOpts::default())?;
                f.filter(&doc, req)?
            }
            Filter::View(f) => {
                let doc = snap.winner_json(fdi, &DocOpts::default())?;
                f.filter(&doc)?
            }
        })
    }
}

fn change_row(snap: &Snapshot, fdi: &FullDocInfo, args: &ChangesArgs) -> Result<Value> {
    let leafs = fdi.sorted_leafs();
    let changes: Vec<Value> = if args.all_docs_style {
        leafs.iter().map(|l| json!({"rev": l.rev()})).collect()
    } else {
        vec![json!({"rev": leafs[0].rev()})]
    };
    let mut row = json!({"seq": snap.seq_str(fdi.update_seq), "id": fdi.id, "changes": changes});
    if leafs[0].deleted {
        row["deleted"] = json!(true);
    }
    if args.include_docs {
        row["doc"] = snap.doc_json(fdi, &leafs[0], &args.doc_opts)?;
        crate::db::count_reads(snap, 1);
    }
    Ok(row)
}

/// Collect up to `limit` matching rows after `since`. Returns rows and the
/// last seq examined.
fn collect(snap: &Snapshot, args: &mut ChangesArgs, since: u64, limit: usize) -> Result<(Vec<Value>, u64)> {
    let mut rows = Vec::new();
    let dir = if args.descending { Dir::Rev } else { Dir::Fwd };
    let mut last = since;
    let req = args.req_json.clone();
    // Descending starts at the newest change, whatever `since` says.
    let start = if args.descending { 0 } else { since };
    snap.fold_changes(start, dir, |fdi| {
        // Check the limit before advancing `last`: a change we stop in
        // front of must not be covered by last_seq, or a client that
        // checkpoints last_seq would skip it.
        if rows.len() >= limit {
            return Ok(false);
        }
        last = fdi.update_seq;
        if args.filter.matches(snap, fdi, &req)? {
            let row = change_row(snap, fdi, args)?;
            match args.transform {
                Some(t) => rows.extend(t(&row)),
                None => rows.push(row),
            }
            if rows.len() >= limit {
                return Ok(false);
            }
        }
        Ok(true)
    })?;
    if rows.len() < limit && !args.descending {
        last = last.max(snap.update_seq());
    }
    Ok((rows, last))
}

fn pending(snap: &Snapshot, last: u64) -> u64 {
    snap.pending_after(last)
}

/// A client waiting on a longpoll, continuous or eventsource feed, counted
/// while it is (clients_requesting_changes).
struct FeedClient;

impl FeedClient {
    fn new() -> FeedClient {
        crate::stat!("couchdb.httpd.clients_requesting_changes").inc();
        FeedClient
    }
}

impl Drop for FeedClient {
    fn drop(&mut self) {
        crate::stat!("couchdb.httpd.clients_requesting_changes").dec();
    }
}

pub async fn changes(req: Req, db: Arc<Db>) -> Result<Response> {
    if req.method != Method::GET && req.method != Method::POST && req.method != Method::HEAD {
        return not_allowed("GET,POST,HEAD");
    }
    let args = parse_args(&req, &db)?;
    run_feed(db, args).await
}

/// A feed of a database's changes, of any kind.
async fn run_feed(db: Arc<Db>, mut args: ChangesArgs) -> Result<Response> {
    match args.feed.as_str() {
        "normal" => {
            let snap = db.snapshot();
            let since = args.since;
            let limit = args.limit;
            let (rows, last) = blocking(|| collect(&snap, &mut args, since, limit))?;
            let last_seq = if args.descending {
                rows.last().map(|r| r["seq"].clone()).unwrap_or(json!(snap.seq_str(0)))
            } else {
                json!(snap.seq_str(last))
            };
            // Descending, what's left are the changes before the last row.
            let pend = if args.descending {
                let lowest = rows.last().and_then(|r| parse_seq(&r["seq"])).unwrap_or(u64::MAX);
                snap.pending_before(lowest)
            } else {
                pending(&snap, last)
            };
            let mut out = json!({"results": rows, "last_seq": last_seq, "pending": pend});
            if args.db_updates {
                out.as_object_mut().map(|o| o.shift_remove("pending"));
                return Ok(json_response(StatusCode::OK, &out));
            }
            Ok(changes_response(&out, false))
        }
        "longpoll" => {
            let _client = FeedClient::new();
            let mut rx = db.subscribe();
            let deadline = tokio::time::Instant::now() + Duration::from_millis(args.timeout);
            loop {
                let snap = db.snapshot();
                let since = args.since;
                let limit = args.limit;
                let (rows, last) = blocking(|| collect(&snap, &mut args, since, limit))?;
                if !rows.is_empty() || tokio::time::Instant::now() >= deadline {
                    let pend = pending(&snap, last);
                    let mut out = json!({"results": rows, "last_seq": snap.seq_str(last), "pending": pend});
                    if args.db_updates {
                        out.as_object_mut().map(|o| o.shift_remove("pending"));
                        return Ok(json_response(StatusCode::OK, &out));
                    }
                    return Ok(changes_response(&out, false));
                }
                args.since = last;
                let wait = tokio::time::timeout_at(deadline, rx.changed()).await;
                if wait.is_err() {
                    let snap = db.snapshot();
                    let mut out = json!({"results": [], "last_seq": snap.seq_str(args.since), "pending": 0});
                    if args.db_updates {
                        out.as_object_mut().map(|o| o.shift_remove("pending"));
                        return Ok(json_response(StatusCode::OK, &out));
                    }
                    return Ok(changes_response(&out, true));
                }
            }
        }
        _ => {
            let eventsource = args.feed == "eventsource";
            let stream = continuous_stream(db, args, eventsource, FeedClient::new());
            let ct = if eventsource { "text/event-stream" } else { "application/json" };
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, ct)
                .header(header::CACHE_CONTROL, "no-cache")
                .body(Body::from_stream(stream))
                .unwrap())
        }
    }
}

fn continuous_stream(
    db: Arc<Db>,
    mut args: ChangesArgs,
    eventsource: bool,
    client: FeedClient,
) -> impl futures::Stream<Item = std::result::Result<Bytes, Infallible>> {
    async_stream::stream! {
        let _client = client;
        let mut rx = db.subscribe();
        let mut since = args.since;
        let mut remaining = args.limit;
        let timeout = Duration::from_millis(args.timeout);
        let beat = args.heartbeat.map(Duration::from_millis);
        let mut idle_deadline = tokio::time::Instant::now() + timeout;
        loop {
            rx.borrow_and_update();
            let snap = db.snapshot();
            let res = tokio::task::block_in_place(|| collect(&snap, &mut args, since, remaining));
            let (rows, last) = match res {
                Ok(r) => r,
                Err(e) => {
                    yield Ok(Bytes::from(format!("{}\n", crate::json::to_string(&e.json()))));
                    break;
                }
            };
            if !rows.is_empty() {
                idle_deadline = tokio::time::Instant::now() + timeout;
            }
            for row in rows {
                remaining -= 1;
                if eventsource {
                    let id = row["seq"].as_str().unwrap_or("").to_string();
                    yield Ok(Bytes::from(format!("data: {}\nid: {id}\n\n", crate::json::to_string(&row))));
                } else {
                    yield Ok(Bytes::from(format!("{}\n", crate::json::to_string(&row))));
                }
            }
            since = last;
            if remaining == 0 {
                if !eventsource {
                    let end = if args.db_updates { json!({"last_seq": snap.seq_str(since)}) } else { json!({"last_seq": snap.seq_str(since), "pending": pending(&snap, since)}) };
                    yield Ok(Bytes::from(format!("{end}\n")));
                }
                break;
            }
            // Wait for a change, sending heartbeats while idle.
            let mut done = false;
            loop {
                let wait_until = match beat {
                    Some(b) => (tokio::time::Instant::now() + b).min(idle_deadline),
                    None => idle_deadline,
                };
                match tokio::time::timeout_at(wait_until, rx.changed()).await {
                    Ok(Ok(())) => break,
                    Ok(Err(_)) => { done = true; break; }
                    Err(_) => {
                        if tokio::time::Instant::now() >= idle_deadline && beat.is_none() {
                            done = true;
                            break;
                        }
                        if beat.is_some() {
                            if eventsource {
                                yield Ok(Bytes::from_static(b"event: heartbeat\ndata: \n\n"));
                            } else {
                                yield Ok(Bytes::from_static(b"\n"));
                            }
                        }
                    }
                }
            }
            if done {
                if !eventsource {
                    let snap = db.snapshot();
                    let end = if args.db_updates { json!({"last_seq": snap.seq_str(since)}) } else { json!({"last_seq": snap.seq_str(since), "pending": 0}) };
                    yield Ok(Bytes::from(format!("{end}\n")));
                }
                break;
            }
        }
    }
}

fn global_changes_db(server: &crate::server::Server) -> String {
    server.config.get_or("global_changes", "db_name", "_global_changes")
}

/// global_changes_httpd:default_transform_change/2: "type:db" ids as
/// database events.
fn db_update_row(row: &Value) -> Option<Value> {
    let (kind, db) = row.get("id")?.as_str()?.split_once(':')?;
    Some(json!({"db_name": db, "type": kind, "seq": row.get("seq").cloned().unwrap_or(Value::Null)}))
}

/// GET /_db_updates: the changes of the _global_changes database, each a
/// database's creation, update or deletion (global_changes_httpd).
pub async fn db_updates(req: Req) -> Result<Response> {
    if req.method != Method::GET {
        return not_allowed("GET");
    }
    let feed = req.q("feed").unwrap_or("normal").to_string();
    let non_neg = |k: &str| -> Result<Option<u64>> {
        match req.q(k) {
            None => Ok(None),
            Some(v) => v.parse::<u64>().map(Some).map_err(|_| CouchError::bad_request("invalid_integer")),
        }
    };
    let limit = non_neg("limit")?;
    let timeout = non_neg("timeout")?;
    let heartbeat = match req.q("heartbeat") {
        Some("true") => Some(60_000),
        Some("false") | None => None,
        Some(_) => non_neg("heartbeat")?,
    };
    if !req.auth.user.is_server_admin() {
        return Err(CouchError::unauthorized("You are not a server admin."));
    }
    let name = global_changes_db(&req.server);
    let db = req.server.open_db(&name).map_err(|_| CouchError::not_found("Database does not exist."))?;
    if !matches!(feed.as_str(), "normal" | "longpoll" | "continuous" | "eventsource") {
        return Err(CouchError::bad_request("Supported `feed` types: normal, continuous, longpoll, eventsource"));
    }
    let snap = db.snapshot();
    let since = match req.q("since") {
        None | Some("0") | Some("") => 0,
        Some("now") => snap.update_seq(),
        Some(s) => {
            let v: Value = serde_json::from_str(s).unwrap_or(Value::String(s.to_string()));
            parse_seq(&v).ok_or_else(|| CouchError::bad_request("Malformed sequence supplied in 'since' parameter."))?
        }
    };
    let args = ChangesArgs {
        feed: feed.clone(),
        since,
        descending: req.q("descending") == Some("true"),
        limit: limit.map(|l| l as usize).unwrap_or(usize::MAX),
        include_docs: false,
        all_docs_style: false,
        heartbeat,
        timeout: timeout.unwrap_or(60_000),
        doc_opts: DocOpts::default(),
        filter: Filter::None,
        req_json: Value::Null,
        seq_interval: None,
        transform: Some(db_update_row),
        db_updates: true,
    };
    run_feed(db, args).await
}

/// The global_changes server: database events, gathered for
/// [global_changes] max_write_delay ms, then written as new revisions of
/// "type:db" documents in the _global_changes database (while it exists;
/// until then they wait).
pub fn start_global_changes(server: &Arc<crate::server::Server>) {
    let weak = Arc::downgrade(server);
    let mut rx = crate::db::events().subscribe();
    tokio::spawn(async move {
        let mut pending: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let delay = weak.upgrade().map(|s| s.config.get_int("global_changes", "max_write_delay", 500u64)).unwrap_or(500);
        let mut tick = tokio::time::interval(Duration::from_millis(delay.max(1)));
        loop {
            tokio::select! {
                ev = rx.recv() => {
                    let (db, kind) = match ev {
                        Ok((db, crate::db::DbEvent::Created)) => (db, "created"),
                        Ok((db, crate::db::DbEvent::Updated)) => (db, "updated"),
                        Ok((db, crate::db::DbEvent::Deleted)) => (db, "deleted"),
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => return,
                    };
                    let Some(server) = weak.upgrade() else { return };
                    if server.config.get("global_changes", "update_db").as_deref() == Some("false") || db == global_changes_db(&server) {
                        continue;
                    }
                    pending.insert(format!("{kind}:{db}"));
                }
                _ = tick.tick() => {
                    if pending.is_empty() {
                        continue;
                    }
                    let Some(server) = weak.upgrade() else { return };
                    let Ok(db) = server.open_db(&global_changes_db(&server)) else { continue };
                    let snap = db.snapshot();
                    let mut docs = Vec::new();
                    for id in std::mem::take(&mut pending) {
                        let mut d = json!({"_id": id});
                        if let Ok(Some(fdi)) = snap.get_fdi(&id) {
                            d["_rev"] = json!(fdi.winner().rev());
                        }
                        if let Ok(doc) = crate::doc::from_json(d) {
                            docs.push(doc);
                        }
                    }
                    crate::stat!("global_changes.db_writes").add(docs.len() as i64);
                    if let Err(e) = db.update_docs(docs, true).await {
                        tracing::warn!("global_changes: {}", e.reason);
                    }
                }
            }
        }
    });
}

/// A normal or longpoll feed's response, laid out as chttpd_db sends it.
fn changes_response(v: &Value, timed_out: bool) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "must-revalidate")
        .body(Body::from(crate::json::changes_body(v, timed_out)))
        .unwrap()
}
