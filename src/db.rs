//! A single database, stored in RocksDB: the RocksDB-experiment
//! replacement for couch_db + couch_db_updater + couch_bt_engine.
//!
//! Column families:
//!   by_id   doc id            -> FdiVal (rev tree)
//!   by_seq  seq (big-endian)  -> FullDocInfo
//!   local   _local doc id     -> LocalDoc
//!   body    body id (BE)      -> DiskBody, LZ4 value encoding
//!   att     chunk id (BE)     -> attachment chunk, LZ4 chunk encoding (blob files)
//!   meta    "header"          -> Header (seq, counters, settings)
//!
//! Readers take RocksDB snapshots. All writes go through one writer thread
//! per database, which drains its queue and commits everything it found as
//! one WriteBatch with a synced WAL (group commit). RocksDB keeps no
//! reductions, so doc counts and sizes are counters kept in the header.

use crate::btree::{Dir, Range};
use crate::doc::*;
use crate::error::{CouchError, Result};
use crate::file::Pos;
use crate::keytree::{self, Path};
use crate::rstore::{Store, View, rerr, successor};
use md5::{Digest, Md5};
use parking_lot::{Mutex, RwLock};
use rocksdb::{WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::{Arc, Weak};
use tokio::sync::{oneshot, watch};

const DISK_VERSION: u32 = 2;
const ATT_CHUNK: usize = 64 * 1024;
const SEQ_PLACEHOLDER: u64 = u64::MAX;

const CF_ID: &str = "by_id";
const CF_SEQ: &str = "by_seq";
const CF_LOCAL: &str = "local";
const CF_BODY: &str = "body";
const CF_ATT: &str = "att";
const CF_META: &str = "meta";
const CFS: &[(&str, bool)] = &[(CF_ID, false), (CF_SEQ, false), (CF_LOCAL, false), (CF_BODY, false), (CF_ATT, true), (CF_META, false)];
const HEADER_KEY: &[u8] = b"header";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FdiVal {
    pub update_seq: u64,
    pub deleted: bool,
    pub tree: keytree::RevTree<RevVal>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LocalDoc {
    pub rev: u64,
    pub body: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Header {
    pub version: u32,
    pub uuid: String,
    pub update_seq: u64,
    pub purge_seq: u64,
    pub security: Vec<u8>,
    pub revs_limit: u64,
    /// Live (non-deleted) and deleted document counts.
    pub doc_count: u64,
    pub del_count: u64,
    /// Inflated bytes of live leaf revisions (sizes.external).
    pub external: u64,
    /// The same, as stored after LZ4 (body part of sizes.active).
    pub stored_live: u64,
}

fn be(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

/// Metadata is stored uncompressed; only bodies and attachments use LZ4.
fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    crate::file::encode_term_raw(v)
}

fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    Ok(crate::file::decode_term(b)?)
}

fn add_signed(base: u64, delta: i128) -> u64 {
    (base as i128 + delta).max(0) as u64
}

// --------------------------------------------------------------- snapshot

#[derive(Clone)]
pub struct Snapshot {
    pub name: String,
    pub header: Header,
    view: Arc<View>,
}

pub fn fmt_seq(seq: u64, uuid: &str) -> String {
    format!("{seq}-{}", &uuid[..uuid.len().min(12)])
}

/// Parse an opaque seq (string "N-..." or number) back to its integer.
pub fn parse_seq(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => {
            let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
            digits.parse().ok()
        }
        Value::Array(a) => a.first().and_then(parse_seq),
        _ => None,
    }
}

fn fdi_from(id: &str, v: FdiVal) -> FullDocInfo {
    FullDocInfo { id: id.to_string(), update_seq: v.update_seq, deleted: v.deleted, tree: v.tree }
}

impl Snapshot {
    pub fn seq_str(&self, seq: u64) -> String {
        fmt_seq(seq, &self.header.uuid)
    }

    pub fn update_seq(&self) -> u64 {
        self.header.update_seq
    }

    pub fn doc_count(&self) -> u64 {
        self.header.doc_count
    }

    pub fn security(&self) -> Value {
        serde_json::from_slice(&self.header.security).unwrap_or_else(|_| json!({}))
    }

    pub fn get_fdi(&self, id: &str) -> Result<Option<FullDocInfo>> {
        match self.view.get(CF_ID, id.as_bytes())? {
            Some(b) => Ok(Some(fdi_from(id, dec(&b)?))),
            None => Ok(None),
        }
    }

    pub fn get_local(&self, id: &str) -> Result<Option<LocalDoc>> {
        match self.view.get(CF_LOCAL, id.as_bytes())? {
            Some(b) => Ok(Some(dec(&b)?)),
            None => Ok(None),
        }
    }

    pub fn read_body(&self, ptr: Pos) -> Result<DiskBody> {
        let raw = self.view.get(CF_BODY, &be(ptr))?.ok_or_else(|| CouchError::internal(format!("missing body {ptr}")))?;
        Ok(crate::file::decode_value(&raw)?)
    }

    /// Stored (deflated) bytes of every live leaf revision.
    pub fn stored_live(&self) -> u64 {
        self.header.stored_live
    }

    pub fn tagged_chunks(&self) -> bool {
        true
    }

    /// Read one attachment chunk, inflated.
    pub fn read_chunk(&self, pos: Pos) -> Result<Vec<u8>> {
        let stored = self.view.get(CF_ATT, &be(pos))?.ok_or_else(|| CouchError::internal(format!("missing chunk {pos}")))?;
        Ok(crate::file::decode_chunk(stored)?)
    }

    pub fn read_att(&self, a: &AttInfo) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(a.encoded_length as usize);
        for p in &a.chunks {
            out.extend_from_slice(&self.read_chunk(*p)?);
        }
        Ok(out)
    }

    /// `sizes.external` counts inflated body and attachment bytes.
    /// `sizes.active` counts live data as stored: deflated bodies and
    /// attachments plus RocksDB's estimate of live index data. `sizes.file`
    /// is every file in the database's RocksDB directory.
    pub fn info(&self, compacting: bool) -> Value {
        let store = self.view.store();
        let meta: u64 = [CF_ID, CF_SEQ, CF_LOCAL].iter().map(|cf| store.prop(cf, "rocksdb.estimate-live-data-size")).sum();
        json!({
            "db_name": self.name,
            "purge_seq": self.seq_str(self.header.purge_seq),
            "update_seq": self.seq_str(self.header.update_seq),
            "sizes": {"file": store.disk_bytes(), "external": self.header.external, "active": self.header.stored_live + meta},
            "props": {},
            "doc_del_count": self.header.del_count,
            "doc_count": self.header.doc_count,
            "disk_format_version": 8,
            "compact_running": compacting,
            "cluster": {"q": 1, "n": 1, "w": 1, "r": 1},
            "instance_start_time": "0"
        })
    }

    /// Find a revision in the doc's tree; None if absent or body missing.
    pub fn find_rev(fdi: &FullDocInfo, pos: u64, id: &str) -> Option<RevInfo> {
        keytree::get(&fdi.tree, pos, id).map(|(val, path)| RevInfo { pos, id: id.to_string(), deleted: val.deleted(), val, path })
    }

    /// Load a revision as a Doc (body + attachment refs).
    pub fn load_doc(&self, fdi: &FullDocInfo, rev: &RevInfo) -> Result<Doc> {
        let leaf = rev.val.leaf().ok_or_else(CouchError::missing)?;
        let body = self.read_body(leaf.ptr)?;
        let map: Map<String, Value> = serde_json::from_slice(&body.json).unwrap_or_default();
        Ok(Doc {
            id: fdi.id.clone(),
            revs: (rev.pos, rev.path.clone()),
            deleted: leaf.deleted,
            body: map,
            atts: body.atts.iter().map(Att::from_info).collect(),
            // Chunks never move in RocksDB, so there is no file generation.
            att_gen: 0,
        })
    }

    /// Render a revision to JSON with the given options.
    pub fn doc_json(&self, fdi: &FullDocInfo, rev: &RevInfo, opts: &DocOpts) -> Result<Value> {
        let leaf = rev.val.leaf().ok_or_else(CouchError::missing)?;
        let body = self.read_body(leaf.ptr)?;
        let mut m = Map::new();
        m.insert("_id".into(), json!(fdi.id));
        m.insert("_rev".into(), json!(rev.rev()));
        if leaf.deleted {
            m.insert("_deleted".into(), json!(true));
        }
        let fields: Map<String, Value> = serde_json::from_slice(&body.json).unwrap_or_default();
        m.extend(fields);
        if !body.atts.is_empty() {
            let since_pos = opts.atts_since.as_ref().map(|revs| {
                revs.iter()
                    .filter(|(p, id)| {
                        let idx = rev.pos.checked_sub(*p).map(|d| d as usize);
                        idx.and_then(|i| rev.path.get(i)).map(|x| x == id).unwrap_or(false)
                    })
                    .map(|(p, _)| *p)
                    .max()
                    .unwrap_or(0)
            });
            let mut atts = Map::new();
            for a in &body.atts {
                let want = match since_pos {
                    Some(sp) => a.revpos > sp,
                    None => opts.attachments,
                };
                let stub = if want && opts.follows {
                    let mut j = att_stub_json(a, opts, None);
                    let o = j.as_object_mut().unwrap();
                    o.remove("stub");
                    o.insert("follows".into(), json!(true));
                    j
                } else {
                    let data = if want { Some(self.read_att(a)?) } else { None };
                    att_stub_json(a, opts, data)
                };
                atts.insert(a.name.clone(), stub);
            }
            m.insert("_attachments".into(), Value::Object(atts));
        }
        if opts.revs {
            m.insert("_revisions".into(), json!({"start": rev.pos, "ids": rev.path}));
        }
        if opts.revs_info {
            let vals = keytree::get_path_vals(&fdi.tree, rev.pos, &rev.id).unwrap_or_default();
            let info: Vec<Value> = vals
                .iter()
                .map(|(p, id, v)| {
                    let status = match v {
                        RevVal::Missing => "missing",
                        RevVal::Avail(l) if l.deleted => "deleted",
                        _ => "available",
                    };
                    json!({"rev": format!("{p}-{id}"), "status": status})
                })
                .collect();
            m.insert("_revs_info".into(), Value::Array(info));
        }
        if opts.conflicts {
            let c = fdi.conflicts();
            if !c.is_empty() {
                m.insert("_conflicts".into(), json!(c));
            }
        }
        if opts.deleted_conflicts {
            let c = fdi.deleted_conflicts();
            if !c.is_empty() {
                m.insert("_deleted_conflicts".into(), json!(c));
            }
        }
        if opts.local_seq {
            m.insert("_local_seq".into(), json!(leaf.seq));
        }
        Ok(Value::Object(m))
    }

    /// Render the winning revision.
    pub fn winner_json(&self, fdi: &FullDocInfo, opts: &DocOpts) -> Result<Value> {
        self.doc_json(fdi, &fdi.winner(), opts)
    }

    /// RocksDB [lower, upper) bounds for a string-keyed range. Btree ranges
    /// start where iteration starts, so reverse ranges swap ends.
    fn str_bounds(range: &Range<String>, dir: Dir) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let start = range.start.as_ref().map(|s| s.as_bytes().to_vec());
        let end = range.end.as_ref().map(|e| e.as_bytes().to_vec());
        match dir {
            Dir::Fwd => (start, end.map(|e| if range.inclusive_end { successor(&e) } else { e })),
            Dir::Rev => (end.map(|e| if range.inclusive_end { e } else { successor(&e) }), start.map(|s| successor(&s))),
        }
    }

    pub fn fold_docs<F>(&self, dir: Dir, range: &Range<String>, mut f: F) -> Result<()>
    where
        F: FnMut(FullDocInfo) -> Result<bool>,
    {
        let (lo, hi) = Self::str_bounds(range, dir);
        self.view.scan(CF_ID, lo.as_deref(), hi.as_deref(), dir == Dir::Rev, |k, v| {
            let id = std::str::from_utf8(k).map_err(|_| CouchError::internal("bad doc id"))?;
            f(fdi_from(id, dec(v)?))
        })
    }

    pub fn fold_changes<F>(&self, since: u64, dir: Dir, mut f: F) -> Result<()>
    where
        F: FnMut(&FullDocInfo) -> Result<bool>,
    {
        let (lo, hi) = match dir {
            Dir::Fwd => (Some(be(since + 1).to_vec()), None),
            Dir::Rev => (None, if since == 0 { None } else { Some(be(since + 1).to_vec()) }),
        };
        self.view.scan(CF_SEQ, lo.as_deref(), hi.as_deref(), dir == Dir::Rev, |_, v| f(&dec::<FullDocInfo>(v)?))
    }

    pub fn fold_locals<F>(&self, range: &Range<String>, dir: Dir, mut f: F) -> Result<()>
    where
        F: FnMut(&str, &LocalDoc) -> bool,
    {
        let (lo, hi) = Self::str_bounds(range, dir);
        self.view.scan(CF_LOCAL, lo.as_deref(), hi.as_deref(), dir == Dir::Rev, |k, v| {
            let id = String::from_utf8_lossy(k);
            Ok(f(&id, &dec::<LocalDoc>(v)?))
        })
    }

    /// Live docs sorting before `key` in `dir` order (_all_docs offset).
    /// RocksDB has no order statistics, so this counts by scanning.
    pub fn count_docs_before(&self, key: &str, dir: Dir) -> Result<u64> {
        let mut n = 0u64;
        let (lo, hi) = match dir {
            Dir::Fwd => (None, Some(key.as_bytes().to_vec())),
            Dir::Rev => (Some(successor(key.as_bytes())), None),
        };
        self.view.scan(CF_ID, lo.as_deref(), hi.as_deref(), false, |_, v| {
            if !dec::<FdiVal>(v)?.deleted {
                n += 1;
            }
            Ok(true)
        })?;
        Ok(n)
    }

    /// Changes after `seq` (the `pending` count of _changes), by scanning.
    pub fn pending_after(&self, seq: u64) -> u64 {
        let mut n = 0u64;
        let _ = self.view.scan(CF_SEQ, Some(&be(seq + 1)), None, false, |_, _| {
            n += 1;
            Ok(true)
        });
        n
    }
}

// ----------------------------------------------------------------- writer

pub type UpdateResult = Result<String>;

enum Msg {
    Update { docs: Vec<Doc>, new_edits: bool, reply: oneshot::Sender<Vec<UpdateResult>> },
    SetSecurity(Vec<u8>, oneshot::Sender<()>),
    SetRevsLimit(u64, oneshot::Sender<()>),
    Purge(Vec<(String, Vec<(u64, String)>)>, oneshot::Sender<Result<Value>>),
    /// Compaction: forget bodies of non-leaf revisions (ptr < limit) for
    /// these docs and any doc changed after `since`.
    DropOldBodies { ids: Vec<String>, since: u64, limit: u64, reply: oneshot::Sender<Result<()>> },
}

// ------------------------------------------------------ directory layout
//
// A database directory (`{name}.rocks`) holds one RocksDB instance per
// incarnation, in `g-<uuid>/`, and a LIVE file naming the current one.
// Deleting a database removes LIVE and leaves the incarnation's files to
// whoever still has it open; they are removed once it closes. So a
// database recreated under the same name never shares files (or
// RocksDB's lock) with the one it replaced, and requests still running
// on the old one finish against intact files. Directories from before
// this layout hold RocksDB's files directly; they stay live until a
// DELETED marker appears.

const LIVE_FILE: &str = "LIVE";
const DELETED_MARKER: &str = "DELETED";

/// Incarnation directories open in this process. Its lock also orders
/// every layout change (create, delete, cleanup on close).
static OPEN_GENS: Mutex<Vec<PathBuf>> = parking_lot::const_mutex(Vec::new());

fn live_gen(dir: &FsPath) -> Option<PathBuf> {
    if let Ok(g) = std::fs::read_to_string(dir.join(LIVE_FILE))
        && !g.trim().is_empty()
    {
        return Some(dir.join(g.trim()));
    }
    (dir.join("CURRENT").exists() && !dir.join(DELETED_MARKER).exists()).then(|| dir.to_path_buf())
}

/// Whether `dir` holds a live (not deleted) database.
pub fn exists(dir: &FsPath) -> bool {
    live_gen(dir).is_some()
}

/// Remove an incarnation that is neither live nor open, then the
/// database directory itself once nothing is left in it. Called with
/// OPEN_GENS locked.
fn remove_gen(dir: &FsPath, generation: &FsPath) {
    if generation == dir {
        // Old layout: RocksDB's files sit directly in `dir`, next to any
        // newer incarnation's directory and LIVE file.
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                if e.file_name() != LIVE_FILE && e.path().is_file() {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
    } else {
        let _ = std::fs::remove_dir_all(generation);
    }
    let empty = std::fs::read_dir(dir).map(|rd| rd.flatten().all(|e| e.file_name() == DELETED_MARKER)).unwrap_or(false);
    if empty {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Remove incarnations left behind by a crash. Called with OPEN_GENS locked.
fn remove_stale_gens(dir: &FsPath, open: &[PathBuf]) {
    let live = live_gen(dir);
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        if e.file_name().to_string_lossy().starts_with("g-") && Some(&p) != live.as_ref() && !open.contains(&p) {
            let _ = std::fs::remove_dir_all(&p);
        }
    }
}

pub struct Db {
    pub name: String,
    /// The database directory; `generation` is this incarnation's RocksDB.
    pub path: PathBuf,
    generation: PathBuf,
    deleted: Arc<AtomicBool>,
    store: Arc<Store>,
    state: RwLock<Snapshot>,
    tx: Mutex<mpsc::Sender<Msg>>,
    seq_tx: watch::Sender<u64>,
    compacting: AtomicBool,
    /// Next id for a body or attachment chunk.
    next_ptr: AtomicU64,
    /// Per-database extension state (view indexes), keyed by signature.
    pub ext: Mutex<HashMap<String, Arc<dyn std::any::Any + Send + Sync>>>,
}

/// Highest big-endian id in a column family, plus one.
fn next_id_after(view: &View, cf: &str) -> Result<u64> {
    let mut next = 1u64;
    view.scan(cf, None, None, true, |k, _| {
        if k.len() == 8 {
            next = u64::from_be_bytes(k.try_into().unwrap()) + 1;
        }
        Ok(false)
    })?;
    Ok(next)
}

impl Db {
    /// Open an existing database, or create one when `create` is set.
    pub fn open(path: &FsPath, name: &str, create: bool) -> Result<Arc<Db>> {
        Self::open_with(path, name, create.then_some(b"{}".as_slice()))
    }

    /// Create a database whose _security object starts as `security`.
    pub fn create(path: &FsPath, name: &str, security: &Value) -> Result<Arc<Db>> {
        Self::open_with(path, name, Some(&serde_json::to_vec(security)?))
    }

    fn open_with(path: &FsPath, name: &str, new_security: Option<&[u8]>) -> Result<Arc<Db>> {
        let create = new_security.is_some();
        let mut open = OPEN_GENS.lock();
        let generation = if create {
            if exists(path) {
                return Err(CouchError::file_exists());
            }
            std::fs::create_dir_all(path)?;
            path.join(format!("g-{}", uuid::Uuid::new_v4().simple()))
        } else {
            live_gen(path).ok_or_else(|| CouchError::not_found("Database does not exist."))?
        };
        let store = Store::open(&generation, CFS, create)?;
        if create {
            let tmp = path.join("LIVE.tmp");
            std::fs::write(&tmp, generation.file_name().unwrap().as_encoded_bytes())?;
            std::fs::rename(&tmp, path.join(LIVE_FILE))?;
        }
        remove_stale_gens(path, &open);
        open.push(generation.clone());
        drop(open);
        let deleted = Arc::new(AtomicBool::new(false));
        let (dir, g, del) = (path.to_path_buf(), generation.clone(), deleted.clone());
        store.on_close(move || {
            let mut open = OPEN_GENS.lock();
            open.retain(|p| p != &g);
            if del.load(AtomicOrdering::SeqCst) {
                remove_gen(&dir, &g);
            }
        });
        let header: Header = match store.db.get_cf(&store.cf(CF_META), HEADER_KEY).map_err(rerr)? {
            Some(b) => dec(&b)?,
            None => {
                let h = Header {
                    version: DISK_VERSION,
                    uuid: uuid::Uuid::new_v4().simple().to_string(),
                    update_seq: 0,
                    purge_seq: 0,
                    security: new_security.unwrap_or(b"{}").to_vec(),
                    revs_limit: 1000,
                    doc_count: 0,
                    del_count: 0,
                    external: 0,
                    stored_live: 0,
                };
                let mut wo = WriteOptions::default();
                wo.set_sync(true);
                store.db.put_cf_opt(&store.cf(CF_META), HEADER_KEY, enc(&h), &wo).map_err(rerr)?;
                h
            }
        };
        let view = View::new(store.clone());
        // Chunk ids are allocated before their doc commits; reusing one an
        // uncommitted upload took is harmless (it was never referenced).
        let next_ptr = next_id_after(&view, CF_BODY)?.max(next_id_after(&view, CF_ATT)?);
        let snap = Snapshot { name: name.to_string(), header, view };
        let (tx, rx) = mpsc::channel();
        let (seq_tx, _) = watch::channel(snap.header.update_seq);
        let db = Arc::new(Db {
            name: name.to_string(),
            path: path.to_path_buf(),
            generation,
            deleted,
            store,
            state: RwLock::new(snap),
            tx: Mutex::new(tx),
            seq_tx,
            compacting: AtomicBool::new(false),
            next_ptr: AtomicU64::new(next_ptr),
            ext: Mutex::new(HashMap::new()),
        });
        let weak = Arc::downgrade(&db);
        std::thread::Builder::new()
            .name(format!("db-writer:{name}"))
            .spawn(move || writer_loop(weak, rx))
            .map_err(|e| CouchError::internal(e.to_string()))?;
        Ok(db)
    }

    pub fn snapshot(&self) -> Snapshot {
        self.state.read().clone()
    }

    pub fn info(&self) -> Value {
        self.snapshot().info(self.compacting.load(AtomicOrdering::Relaxed))
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.seq_tx.subscribe()
    }

    fn send(&self, m: Msg) -> Result<()> {
        self.tx.lock().send(m).map_err(|_| CouchError::internal("database writer stopped"))
    }

    pub async fn update_docs(&self, docs: Vec<Doc>, new_edits: bool) -> Result<Vec<UpdateResult>> {
        let (reply, rx) = oneshot::channel();
        self.send(Msg::Update { docs, new_edits, reply })?;
        rx.await.map_err(|_| CouchError::internal("update failed"))
    }

    pub async fn update_doc(&self, doc: Doc) -> Result<String> {
        self.update_docs(vec![doc], true).await?.pop().unwrap()
    }

    pub async fn set_security(&self, sec: &Value) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(Msg::SetSecurity(serde_json::to_vec(sec)?, reply))?;
        rx.await.map_err(|_| CouchError::internal("update failed"))
    }

    pub async fn set_revs_limit(&self, n: u64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(Msg::SetRevsLimit(n, reply))?;
        rx.await.map_err(|_| CouchError::internal("update failed"))
    }

    pub async fn purge(&self, req: Vec<(String, Vec<(u64, String)>)>) -> Result<Value> {
        let (reply, rx) = oneshot::channel();
        self.send(Msg::Purge(req, reply))?;
        rx.await.map_err(|_| CouchError::internal("purge failed"))?
    }

    fn alloc_ptrs(&self, n: u64) -> u64 {
        self.next_ptr.fetch_add(n, AtomicOrdering::Relaxed)
    }

    /// Write a doc's new attachment bytes before it reaches the writer
    /// thread, so large uploads don't stall every other write. They become
    /// durable with the doc's own synced commit, which follows them in the
    /// WAL.
    pub fn stage_atts(&self, doc: &mut Doc) -> Result<()> {
        for a in doc.atts.iter_mut() {
            if let AttData::Bytes(bytes) = &a.data {
                let (chunks, stored) = write_att_chunks(self, bytes)?;
                a.stored_length = stored;
                if a.md5.is_empty() || a.encoding.is_none() {
                    a.md5 = Md5::digest(bytes).to_vec();
                }
                if a.encoding.is_none() {
                    a.length = bytes.len() as u64;
                }
                a.encoded_length = bytes.len() as u64;
                a.data = AttData::Stored(chunks);
            }
        }
        Ok(())
    }

    pub fn is_compacting(&self) -> bool {
        self.compacting.load(AtomicOrdering::Relaxed)
    }

    /// Start background compaction. Returns immediately.
    pub fn start_compaction(self: &Arc<Self>) -> Result<()> {
        if self.compacting.swap(true, AtomicOrdering::SeqCst) {
            return Ok(());
        }
        let db = self.clone();
        std::thread::spawn(move || {
            if let Err(e) = db.compact_blocking() {
                tracing::error!("compaction of {} failed: {}", db.name, e.reason);
            }
            db.compacting.store(false, AtomicOrdering::SeqCst);
        });
        Ok(())
    }

    /// Compaction for the RocksDB engine:
    /// 1. mark every body and chunk referenced by a live leaf revision,
    /// 2. have the writer drop non-leaf revision bodies from rev trees,
    /// 3. delete unreferenced bodies and chunks allocated before the mark,
    /// 4. compact every column family so RocksDB reclaims the space,
    ///    including garbage collection of blob files.
    fn compact_blocking(&self) -> Result<()> {
        let snap = self.snapshot();
        let limit = self.next_ptr.load(AtomicOrdering::SeqCst);
        let mut live_bodies: Vec<u64> = Vec::new();
        let mut live_chunks: Vec<u64> = Vec::new();
        let mut with_old: Vec<String> = Vec::new();
        snap.fold_docs(Dir::Fwd, &Range::default(), |fdi| {
            let mut has_old = false;
            keytree::map_vals(&fdi.tree, &mut |_, _, v, is_leaf| {
                if let RevVal::Avail(l) = v {
                    if is_leaf {
                        live_bodies.push(l.ptr);
                    } else {
                        has_old = true;
                    }
                }
            });
            for l in keytree::leafs(&fdi.tree) {
                if let Some(lv) = l.val.leaf() {
                    for a in snap.read_body(lv.ptr)?.atts {
                        live_chunks.extend(a.chunks);
                    }
                }
            }
            if has_old {
                with_old.push(fdi.id.clone());
            }
            Ok(true)
        })?;
        live_bodies.sort_unstable();
        live_chunks.sort_unstable();
        live_chunks.dedup();

        let (reply, rx) = oneshot::channel();
        self.send(Msg::DropOldBodies { ids: with_old, since: snap.update_seq(), limit, reply })?;
        rx.blocking_recv().map_err(|_| CouchError::internal("compaction failed"))??;

        sweep(&self.store, CF_BODY, &live_bodies, limit)?;
        sweep(&self.store, CF_ATT, &live_chunks, limit)?;

        let mut co = rocksdb::CompactOptions::default();
        co.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
        for (cf, _) in CFS {
            self.store.db.compact_range_cf_opt(&self.store.cf(cf), None::<&[u8]>, None::<&[u8]>, &co);
        }
        Ok(())
    }

    /// Delete this database: it stops being live at once, and its files
    /// are removed when the last handle to this incarnation closes.
    pub fn delete_files(&self) {
        let _open = OPEN_GENS.lock();
        if live_gen(&self.path).as_ref() == Some(&self.generation) {
            if self.generation == self.path {
                let _ = std::fs::write(self.path.join(DELETED_MARKER), b"");
            } else {
                let _ = std::fs::remove_file(self.path.join(LIVE_FILE));
            }
        }
        self.deleted.store(true, AtomicOrdering::SeqCst);
    }
}

/// Delete ids below `limit` that aren't in `live` (sorted), as ranges.
/// Walks keys only, so blob-file values (attachment chunks) aren't read.
fn sweep(store: &Store, cf: &str, live: &[u64], limit: u64) -> Result<()> {
    let handle = store.cf(cf);
    let mut ro = rocksdb::ReadOptions::default();
    ro.set_iterate_upper_bound(be(limit).to_vec());
    let mut it = store.db.raw_iterator_cf_opt(&handle, ro);
    it.seek_to_first();
    let mut batch = WriteBatch::default();
    let mut run: Option<(u64, u64)> = None;
    while it.valid() {
        let id = u64::from_be_bytes(it.key().unwrap().try_into().map_err(|_| CouchError::internal("bad id key"))?);
        if live.binary_search(&id).is_ok() {
            if let Some((a, b)) = run.take() {
                batch.delete_range_cf(&handle, be(a), be(b + 1));
            }
        } else {
            run = Some(match run {
                Some((a, _)) => (a, id),
                None => (id, id),
            });
        }
        if batch.len() >= 10_000 {
            store.db.write(std::mem::take(&mut batch)).map_err(rerr)?;
        }
        it.next();
    }
    it.status().map_err(rerr)?;
    if let Some((a, b)) = run.take() {
        batch.delete_range_cf(&handle, be(a), be(b + 1));
    }
    store.db.write(batch).map_err(rerr)?;
    Ok(())
}

fn writer_loop(weak: Weak<Db>, rx: mpsc::Receiver<Msg>) {
    while let Ok(first) = rx.recv() {
        let Some(db) = weak.upgrade() else { return };
        let mut pending = vec![first];
        while let Ok(m) = rx.try_recv() {
            pending.push(m);
            if pending.len() >= 256 {
                break;
            }
        }
        let mut updates = Vec::new();
        for m in pending {
            match m {
                Msg::Update { docs, new_edits, reply } => updates.push((docs, new_edits, reply)),
                other => {
                    flush_updates(&db, std::mem::take(&mut updates));
                    handle_other(&db, other);
                }
            }
        }
        flush_updates(&db, updates);
    }
}

/// Commit a batch plus the new header atomically and durably, then
/// publish a new snapshot.
fn commit(db: &Db, mut batch: WriteBatch, header: Header) -> Result<()> {
    batch.put_cf(&db.store.cf(CF_META), HEADER_KEY, enc(&header));
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    db.store.db.write_opt(batch, &wo).map_err(rerr)?;
    let seq = header.update_seq;
    *db.state.write() = Snapshot { name: db.name.clone(), header, view: View::new(db.store.clone()) };
    db.seq_tx.send_replace(seq);
    Ok(())
}

fn handle_other(db: &Db, m: Msg) {
    let snap = db.snapshot();
    match m {
        Msg::Update { .. } => unreachable!(),
        Msg::SetSecurity(sec, reply) => {
            let mut h = snap.header.clone();
            h.security = sec;
            let _ = commit(db, WriteBatch::default(), h);
            let _ = reply.send(());
        }
        Msg::SetRevsLimit(n, reply) => {
            let mut h = snap.header.clone();
            h.revs_limit = n;
            let _ = commit(db, WriteBatch::default(), h);
            let _ = reply.send(());
        }
        Msg::Purge(req, reply) => {
            let _ = reply.send(do_purge(db, snap, req));
        }
        Msg::DropOldBodies { ids, since, limit, reply } => {
            let _ = reply.send(drop_old_bodies(db, snap, ids, since, limit));
        }
    }
}

fn drop_old_bodies(db: &Db, snap: Snapshot, mut ids: Vec<String>, since: u64, limit: u64) -> Result<()> {
    snap.fold_changes(since, Dir::Fwd, |fdi| {
        ids.push(fdi.id.clone());
        Ok(true)
    })?;
    ids.sort();
    ids.dedup();
    let mut batch = WriteBatch::default();
    for id in ids {
        let Some(fdi) = snap.get_fdi(&id)? else { continue };
        let mut changed = false;
        let tree = keytree::map_vals(&fdi.tree, &mut |_, _, v, is_leaf| match v {
            RevVal::Avail(l) if !is_leaf && l.ptr < limit => {
                changed = true;
                RevVal::Missing
            }
            other => other.clone(),
        });
        if !changed {
            continue;
        }
        let fdi = FullDocInfo { tree, ..fdi };
        put_fdi(db, &mut batch, &fdi);
        batch.put_cf(&db.store.cf(CF_SEQ), be(fdi.update_seq), enc(&fdi));
    }
    commit(db, batch, snap.header.clone())
}

fn put_fdi(db: &Db, batch: &mut WriteBatch, fdi: &FullDocInfo) {
    let v = FdiVal { update_seq: fdi.update_seq, deleted: fdi.deleted, tree: fdi.tree.clone() };
    batch.put_cf(&db.store.cf(CF_ID), fdi.id.as_bytes(), enc(&v));
}

type UpdateBatch = Vec<(Vec<Doc>, bool, oneshot::Sender<Vec<UpdateResult>>)>;

fn flush_updates(db: &Db, batch: UpdateBatch) {
    if batch.is_empty() {
        return;
    }
    let snap = db.snapshot();
    match apply_updates(db, &snap, &batch) {
        Ok((wb, header, results)) => {
            let ok = commit(db, wb, header);
            for ((_, _, reply), res) in batch.into_iter().zip(results) {
                let res = match &ok {
                    Ok(()) => res,
                    Err(e) => res.into_iter().map(|_| Err(e.clone())).collect(),
                };
                let _ = reply.send(res);
            }
        }
        Err(e) => {
            for (docs, _, reply) in batch {
                let _ = reply.send(docs.iter().map(|_| Err(e.clone())).collect());
            }
        }
    }
}

/// Header counters change by the difference between a doc's old and new
/// state; `sign` is -1 to remove a doc's contribution and +1 to add it.
fn count_doc(h: &mut Header, fdi: Option<&FullDocInfo>, sign: i128) {
    let Some(f) = fdi else { return };
    if f.deleted {
        h.del_count = add_signed(h.del_count, sign);
    } else {
        h.doc_count = add_signed(h.doc_count, sign);
    }
    h.external = add_signed(h.external, sign * f.leaf_size() as i128);
    h.stored_live = add_signed(h.stored_live, sign * f.leaf_stored() as i128);
}

/// Writes collected while applying a group of updates.
struct Writes<'a> {
    db: &'a Db,
    batch: WriteBatch,
    /// Bodies written earlier in this batch (not yet visible to readers), so
    /// a second update to the same doc can find its parent's body.
    pending_bodies: HashMap<u64, DiskBody>,
}

fn apply_updates(db: &Db, snap: &Snapshot, batch: &UpdateBatch) -> Result<(WriteBatch, Header, Vec<Vec<UpdateResult>>)> {
    let mut w = Writes { db, batch: WriteBatch::default(), pending_bodies: HashMap::new() };
    let mut fdis: HashMap<String, Option<FullDocInfo>> = HashMap::new();
    let mut originals: HashMap<String, Option<FullDocInfo>> = HashMap::new();
    let mut locals: HashMap<String, Option<LocalDoc>> = HashMap::new();
    let mut changed: Vec<String> = Vec::new();
    let mut local_changed: Vec<String> = Vec::new();
    let mut results = Vec::with_capacity(batch.len());
    let revs_limit = snap.header.revs_limit;

    for (docs, new_edits, _) in batch {
        let mut res = Vec::with_capacity(docs.len());
        for doc in docs {
            if doc.is_local() {
                if !locals.contains_key(&doc.id) {
                    locals.insert(doc.id.clone(), snap.get_local(&doc.id)?);
                }
                let slot = locals.get_mut(&doc.id).unwrap();
                let r = update_local(slot, doc);
                if r.is_ok() && !local_changed.contains(&doc.id) {
                    local_changed.push(doc.id.clone());
                }
                res.push(r);
                continue;
            }
            if !fdis.contains_key(&doc.id) {
                let f = snap.get_fdi(&doc.id)?;
                originals.insert(doc.id.clone(), f.clone());
                fdis.insert(doc.id.clone(), f);
            }
            let slot = fdis.get_mut(&doc.id).unwrap();
            let r = if *new_edits {
                update_interactive(&mut w, snap, slot, doc, revs_limit)
            } else {
                update_replicated(&mut w, snap, slot, doc, revs_limit)
            };
            match r {
                Ok((rev, did_change)) => {
                    if did_change && !changed.contains(&doc.id) {
                        changed.push(doc.id.clone());
                    }
                    res.push(Ok(rev));
                }
                Err(e) => res.push(Err(e)),
            }
        }
        results.push(res);
    }

    let mut header = snap.header.clone();
    let mut seq = header.update_seq;
    for id in changed {
        let mut fdi = fdis.remove(&id).unwrap().unwrap();
        seq += 1;
        fdi.update_seq = seq;
        fdi.tree = keytree::map_vals(&fdi.tree, &mut |_, _, v, _| match v {
            RevVal::Avail(l) if l.seq == SEQ_PLACEHOLDER => RevVal::Avail(LeafVal { seq, ..l.clone() }),
            other => other.clone(),
        });
        fdi.deleted = fdi.winner().deleted;
        let old = originals.get(&id).cloned().flatten();
        if let Some(o) = &old {
            w.batch.delete_cf(&db.store.cf(CF_SEQ), be(o.update_seq));
        }
        count_doc(&mut header, old.as_ref(), -1);
        count_doc(&mut header, Some(&fdi), 1);
        put_fdi(db, &mut w.batch, &fdi);
        w.batch.put_cf(&db.store.cf(CF_SEQ), be(seq), enc(&fdi));
    }
    for id in local_changed {
        match locals.remove(&id).unwrap() {
            Some(l) => w.batch.put_cf(&db.store.cf(CF_LOCAL), id.as_bytes(), enc(&l)),
            None => w.batch.delete_cf(&db.store.cf(CF_LOCAL), id.as_bytes()),
        }
    }
    header.update_seq = seq;
    Ok((w.batch, header, results))
}

fn update_local(slot: &mut Option<LocalDoc>, doc: &Doc) -> UpdateResult {
    let given: u64 = match doc.revs.1.first() {
        None => 0,
        Some(r) => r.parse().map_err(|_| CouchError::conflict())?,
    };
    let cur = slot.as_ref().map(|l| l.rev).unwrap_or(0);
    if given != cur {
        return Err(CouchError::conflict());
    }
    if doc.deleted {
        if slot.is_none() {
            return Err(CouchError::not_found("missing"));
        }
        *slot = None;
        return Ok("0-0".into());
    }
    let rev = cur + 1;
    *slot = Some(LocalDoc { rev, body: serde_json::to_vec(&doc.body)? });
    Ok(format!("0-{rev}"))
}

fn missing_stub(doc: &Doc, name: &str) -> CouchError {
    CouchError::precondition("missing_stub", format!("Invalid attachment stub in {} for {}", doc.id, name))
}

/// Resolve stubs and write new attachment data, returning stored infos.
fn prepare_atts(w: &Writes, doc: &Doc, prev: Option<&DiskBody>, new_pos: u64) -> Result<Vec<AttInfo>> {
    let mut out = Vec::with_capacity(doc.atts.len());
    for a in &doc.atts {
        let info = match &a.data {
            AttData::Stub => {
                let p = prev.and_then(|b| b.atts.iter().find(|x| x.name == a.name)).ok_or_else(|| missing_stub(doc, &a.name))?;
                if let Some(rp) = a.revpos {
                    if rp != p.revpos && rp > new_pos {
                        return Err(missing_stub(doc, &a.name));
                    }
                }
                p.clone()
            }
            AttData::Stored(chunks) => AttInfo {
                name: a.name.clone(),
                content_type: a.content_type.clone(),
                revpos: a.revpos.unwrap_or(new_pos),
                length: a.length,
                md5: a.md5.clone(),
                encoding: a.encoding.clone(),
                encoded_length: a.encoded_length,
                chunks: chunks.clone(),
                stored_length: a.stored_length,
            },
            AttData::Bytes(bytes) => {
                let (chunks, stored_length) = write_att_chunks(w.db, bytes)?;
                let md5 = if a.md5.is_empty() || a.encoding.is_none() { Md5::digest(bytes).to_vec() } else { a.md5.clone() };
                AttInfo {
                    name: a.name.clone(),
                    content_type: a.content_type.clone(),
                    revpos: a.revpos.unwrap_or(new_pos),
                    length: if a.encoding.is_some() { a.length } else { bytes.len() as u64 },
                    md5,
                    encoding: a.encoding.clone(),
                    encoded_length: bytes.len() as u64,
                    chunks,
                    stored_length,
                }
            }
            AttData::Follows => {
                return Err(CouchError::bad_request(format!("attachment {} data was not supplied", a.name)));
            }
        };
        out.push(info);
    }
    Ok(out)
}

/// Write attachment data as LZ4-encoded 64 KiB chunks under fresh ids.
/// Returns the chunk ids and the bytes they occupy as stored.
fn write_att_chunks(db: &Db, bytes: &[u8]) -> Result<(Vec<Pos>, u64)> {
    let pieces: Vec<&[u8]> = bytes.chunks(ATT_CHUNK).collect();
    let first = db.alloc_ptrs(pieces.len() as u64);
    let cf = db.store.cf(CF_ATT);
    let mut stored = 0u64;
    let mut chunks = Vec::with_capacity(pieces.len());
    for (gi, group) in pieces.chunks(256).enumerate() {
        let mut batch = WriteBatch::default();
        for (i, c) in group.iter().enumerate() {
            let id = first + (gi * 256 + i) as u64;
            let e = crate::file::encode_chunk(c);
            stored += e.len() as u64;
            batch.put_cf(&cf, be(id), e);
            chunks.push(id);
        }
        db.store.db.write(batch).map_err(rerr)?;
    }
    Ok((chunks, stored))
}

fn write_rev(w: &mut Writes, doc: &Doc, atts: Vec<AttInfo>, body_json: Vec<u8>) -> Result<LeafVal> {
    let size = body_json.len() as u64 + atts.iter().map(|a| a.encoded_length).sum::<u64>();
    let att_stored: u64 = atts.iter().map(|a| a.stored_length).sum();
    let body = DiskBody { json: body_json, atts };
    let encoded = crate::file::encode_value(&body);
    let stored = encoded.len() as u64 + att_stored;
    let ptr = w.db.alloc_ptrs(1);
    w.batch.put_cf(&w.db.store.cf(CF_BODY), be(ptr), encoded);
    w.pending_bodies.insert(ptr, body);
    Ok(LeafVal { deleted: doc.deleted, ptr, seq: SEQ_PLACEHOLDER, size, stored })
}

fn read_prev_body(w: &Writes, snap: &Snapshot, ptr: Pos) -> Result<DiskBody> {
    match w.pending_bodies.get(&ptr) {
        Some(b) => Ok(b.clone()),
        None => snap.read_body(ptr),
    }
}

fn update_interactive(w: &mut Writes, snap: &Snapshot, slot: &mut Option<FullDocInfo>, doc: &Doc, revs_limit: u64) -> Result<(String, bool)> {
    let parent: Option<RevInfo> = match (slot.as_ref(), doc.revs.1.first()) {
        (None, None) => None,
        (None, Some(_)) => return Err(CouchError::conflict()),
        (Some(f), None) => {
            let wn = f.winner();
            if wn.deleted {
                Some(wn)
            } else {
                return Err(CouchError::conflict());
            }
        }
        (Some(f), Some(id)) => {
            let pos = doc.revs.0;
            match f.sorted_leafs().into_iter().find(|l| l.pos == pos && &l.id == id) {
                Some(l) => Some(l),
                None => return Err(CouchError::conflict()),
            }
        }
    };
    let prev_body = match parent.as_ref().and_then(|p| p.val.leaf()) {
        Some(l) => Some(read_prev_body(w, snap, l.ptr)?),
        None => None,
    };
    let new_pos = parent.as_ref().map(|p| p.pos + 1).unwrap_or(1);
    let atts = prepare_atts(w, doc, prev_body.as_ref(), new_pos)?;
    let body_json = serde_json::to_vec(&doc.body)?;
    let mut hash_doc = doc.clone();
    hash_doc.atts = atts.iter().map(Att::from_info).collect();
    let new_id = new_rev_id(&hash_doc, parent.as_ref().map(|p| (p.pos, p.id.as_str())), &body_json);
    let leaf = write_rev(w, doc, atts, body_json)?;
    let path = match &parent {
        Some(p) => Path { start: p.pos, revs: vec![(p.id.clone(), RevVal::Missing), (new_id.clone(), RevVal::Avail(leaf))] },
        None => Path { start: 1, revs: vec![(new_id.clone(), RevVal::Avail(leaf))] },
    };
    let fdi = slot.get_or_insert_with(|| FullDocInfo { id: doc.id.clone(), update_seq: 0, deleted: false, tree: Vec::new() });
    keytree::merge(&mut fdi.tree, path, revs_limit);
    Ok((format!("{new_pos}-{new_id}"), true))
}

fn update_replicated(w: &mut Writes, snap: &Snapshot, slot: &mut Option<FullDocInfo>, doc: &Doc, revs_limit: u64) -> Result<(String, bool)> {
    let (pos, ids) = (&doc.revs.0, &doc.revs.1);
    if ids.is_empty() {
        return Err(CouchError::bad_request("Document rev and _revisions are required with new_edits=false"));
    }
    let rev_str = format!("{}-{}", pos, ids[0]);
    if let Some(f) = slot.as_ref() {
        if let Some((RevVal::Avail(_), _)) = keytree::get(&f.tree, *pos, &ids[0]) {
            return Ok((rev_str, false));
        }
    }
    // Stubs refer to the nearest ancestor we have a body for.
    let mut prev_body = None;
    if let Some(f) = slot.as_ref() {
        for (i, id) in ids.iter().enumerate().skip(1) {
            if let Some((RevVal::Avail(l), _)) = keytree::get(&f.tree, pos - i as u64, id) {
                prev_body = Some(read_prev_body(w, snap, l.ptr)?);
                break;
            }
        }
        if prev_body.is_none() && doc.atts.iter().any(|a| matches!(a.data, AttData::Stub)) {
            for l in f.sorted_leafs() {
                if let Some(lv) = l.val.leaf() {
                    let b = read_prev_body(w, snap, lv.ptr)?;
                    if doc.atts.iter().filter(|a| matches!(a.data, AttData::Stub)).all(|a| b.atts.iter().any(|x| x.name == a.name)) {
                        prev_body = Some(b);
                        break;
                    }
                }
            }
        }
    }
    let atts = prepare_atts(w, doc, prev_body.as_ref(), *pos)?;
    let body_json = serde_json::to_vec(&doc.body)?;
    let leaf = write_rev(w, doc, atts, body_json)?;
    let n = ids.len();
    let start = pos + 1 - n as u64;
    let revs: Vec<(String, RevVal)> = ids
        .iter()
        .rev()
        .enumerate()
        .map(|(i, id)| (id.clone(), if i == n - 1 { RevVal::Avail(leaf.clone()) } else { RevVal::Missing }))
        .collect();
    let fdi = slot.get_or_insert_with(|| FullDocInfo { id: doc.id.clone(), update_seq: 0, deleted: false, tree: Vec::new() });
    keytree::merge(&mut fdi.tree, Path { start, revs }, revs_limit);
    Ok((rev_str, true))
}

fn do_purge(db: &Db, snap: Snapshot, req: Vec<(String, Vec<(u64, String)>)>) -> Result<Value> {
    let mut purged = Map::new();
    let mut header = snap.header.clone();
    let mut seq = header.update_seq;
    let mut batch = WriteBatch::default();
    for (id, revs) in req {
        let Some(mut fdi) = snap.get_fdi(&id)? else {
            purged.insert(id, json!([]));
            continue;
        };
        let before = fdi.clone();
        let removed = keytree::remove_leafs(&mut fdi.tree, &revs);
        purged.insert(id.clone(), json!(removed.iter().map(|(p, r)| format!("{p}-{r}")).collect::<Vec<_>>()));
        if removed.is_empty() {
            continue;
        }
        count_doc(&mut header, Some(&before), -1);
        batch.delete_cf(&db.store.cf(CF_SEQ), be(before.update_seq));
        if fdi.tree.is_empty() {
            batch.delete_cf(&db.store.cf(CF_ID), id.as_bytes());
        } else {
            seq += 1;
            fdi.update_seq = seq;
            fdi.deleted = fdi.winner().deleted;
            count_doc(&mut header, Some(&fdi), 1);
            put_fdi(db, &mut batch, &fdi);
            batch.put_cf(&db.store.cf(CF_SEQ), be(seq), enc(&fdi));
        }
    }
    header.update_seq = seq;
    header.purge_seq += 1;
    let purge_seq = header.purge_seq;
    commit(db, batch, header)?;
    Ok(json!({"purge_seq": purge_seq, "purged": purged}))
}
