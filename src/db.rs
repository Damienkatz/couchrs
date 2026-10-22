//! A single database file: a port of couch_db + couch_db_updater +
//! couch_bt_engine.
//!
//! Readers take cheap immutable snapshots (the btree roots from the last
//! committed header). All writes go through one writer thread per database
//! which drains its queue and commits everything it found in one fsync'd
//! header write (group commit).

use crate::btree::{Action, Btree, Dir, NodePtr, Range, Spec};
use crate::doc::*;
use crate::error::{CouchError, Result};
use crate::file::{CouchFile, Pos};
use crate::keytree::{self, Path};
use md5::{Digest, Md5};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::{Arc, Weak};
use tokio::sync::{oneshot, watch};

/// Format 2 stores doc bodies LZ4-compressed (when that helps) and tags
/// every attachment chunk with its encoding. Format 1 chunks are raw bytes;
/// such files stay format 1 until compaction rewrites them.
const DISK_VERSION: u32 = 2;
const TAGGED_CHUNKS_SINCE: u32 = 2;
const ATT_CHUNK: usize = 64 * 1024;
const SEQ_PLACEHOLDER: u64 = u64::MAX;

// ------------------------------------------------------------------ btrees

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FdiVal {
    pub update_seq: u64,
    pub deleted: bool,
    pub tree: keytree::RevTree<RevVal>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct IdRed {
    pub doc_count: u64,
    pub del_count: u64,
    pub size: u64,
}

pub struct IdSpec;
impl Spec for IdSpec {
    type K = String;
    type V = FdiVal;
    type R = IdRed;
    fn cmp(&self, a: &String, b: &String) -> Ordering {
        a.as_bytes().cmp(b.as_bytes())
    }
    fn reduce(&self, kvs: &[(String, FdiVal)]) -> IdRed {
        let mut r = IdRed::default();
        for (id, v) in kvs {
            if v.deleted {
                r.del_count += 1;
            } else {
                r.doc_count += 1;
            }
            let _ = id;
            r.size += keytree::leafs(&v.tree)
                .iter()
                .filter_map(|l| l.val.leaf().map(|lv| lv.size))
                .sum::<u64>();
        }
        r
    }
    fn rereduce(&self, reds: &[IdRed]) -> IdRed {
        let mut r = IdRed::default();
        for x in reds {
            r.doc_count += x.doc_count;
            r.del_count += x.del_count;
            r.size += x.size;
        }
        r
    }
}

pub struct SeqSpec;
impl Spec for SeqSpec {
    type K = u64;
    type V = FullDocInfo;
    type R = u64;
    fn cmp(&self, a: &u64, b: &u64) -> Ordering {
        a.cmp(b)
    }
    fn reduce(&self, kvs: &[(u64, FullDocInfo)]) -> u64 {
        kvs.len() as u64
    }
    fn rereduce(&self, reds: &[u64]) -> u64 {
        reds.iter().sum()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LocalDoc {
    pub rev: u64,
    pub body: Vec<u8>,
}

pub struct LocalSpec;
impl Spec for LocalSpec {
    type K = String;
    type V = LocalDoc;
    type R = u64;
    fn cmp(&self, a: &String, b: &String) -> Ordering {
        a.as_bytes().cmp(b.as_bytes())
    }
    fn reduce(&self, kvs: &[(String, LocalDoc)]) -> u64 {
        kvs.len() as u64
    }
    fn rereduce(&self, reds: &[u64]) -> u64 {
        reds.iter().sum()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Header {
    pub version: u32,
    pub uuid: String,
    pub update_seq: u64,
    pub purge_seq: u64,
    pub id_root: Option<NodePtr<IdRed>>,
    pub seq_root: Option<NodePtr<u64>>,
    pub local_root: Option<NodePtr<u64>>,
    pub security: Vec<u8>,
    pub revs_limit: u64,
    pub props: Vec<u8>,
}

impl Header {
    fn prop_u64(&self, key: &str) -> Option<u64> {
        serde_json::from_slice::<Value>(&self.props).ok()?.get(key)?.as_u64()
    }
    fn set_prop_u64(&mut self, key: &str, v: u64) {
        let mut m: Map<String, Value> = serde_json::from_slice(&self.props).unwrap_or_default();
        m.insert(key.into(), json!(v));
        self.props = serde_json::to_vec(&m).unwrap();
    }
}

/// Header property holding the stored (deflated) bytes of all live leaf
/// revisions: the body part of sizes.active.
const STORED_LIVE: &str = "stored_live";

fn add_signed(base: u64, delta: i128) -> u64 {
    (base as i128 + delta).max(0) as u64
}

// --------------------------------------------------------------- snapshot

#[derive(Clone)]
pub struct Snapshot {
    pub name: String,
    pub header: Header,
    pub file: Arc<CouchFile>,
    pub ids: Btree<IdSpec>,
    pub seqs: Btree<SeqSpec>,
    pub locals: Btree<LocalSpec>,
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

impl Snapshot {
    pub fn seq_str(&self, seq: u64) -> String {
        fmt_seq(seq, &self.header.uuid)
    }

    pub fn update_seq(&self) -> u64 {
        self.header.update_seq
    }

    pub fn doc_count(&self) -> u64 {
        self.ids.full_reduce().map(|r| r.doc_count).unwrap_or(0)
    }

    pub fn security(&self) -> Value {
        serde_json::from_slice(&self.header.security).unwrap_or_else(|_| json!({}))
    }

    pub fn get_fdi(&self, id: &str) -> Result<Option<FullDocInfo>> {
        Ok(self.ids.lookup(&id.to_string())?.map(|v| FullDocInfo {
            id: id.to_string(),
            update_seq: v.update_seq,
            deleted: v.deleted,
            tree: v.tree,
        }))
    }

    pub fn get_local(&self, id: &str) -> Result<Option<LocalDoc>> {
        Ok(self.locals.lookup(&id.to_string())?)
    }

    pub fn read_body(&self, ptr: Pos) -> Result<DiskBody> {
        let raw = self.file.read_raw(ptr)?;
        if crate::file::is_current_value(&raw) {
            Ok(crate::file::decode_value(&raw)?)
        } else {
            Ok(crate::file::decode_term::<DiskBodyV1>(&raw)?.into())
        }
    }

    /// Stored (deflated) bytes of every live leaf revision.
    pub fn stored_live(&self) -> u64 {
        self.header.prop_u64(STORED_LIVE).unwrap_or(0)
    }

    /// True if this file tags (and may LZ4-compress) attachment chunks.
    pub fn tagged_chunks(&self) -> bool {
        self.header.version >= TAGGED_CHUNKS_SINCE
    }

    /// Read one attachment chunk, inflated.
    pub fn read_chunk(&self, pos: Pos) -> Result<Vec<u8>> {
        let stored = self.file.read_raw(pos)?;
        Ok(if self.tagged_chunks() { crate::file::decode_chunk(stored)? } else { stored })
    }

    pub fn read_att(&self, a: &AttInfo) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(a.encoded_length as usize);
        for p in &a.chunks {
            out.extend_from_slice(&self.read_chunk(*p)?);
        }
        Ok(out)
    }

    /// `sizes.external` counts inflated (uncompressed) body and attachment
    /// bytes. `sizes.active`, which compaction is judged by, counts what is
    /// live as stored on disk: deflated bodies and attachments plus index
    /// nodes. `sizes.file` is the file's length.
    pub fn info(&self, compacting: bool) -> Value {
        let red = self.ids.full_reduce().unwrap_or_default();
        let active = self.stored_live() + self.ids.size() + self.seqs.size() + self.locals.size();
        json!({
            "db_name": self.name,
            "purge_seq": self.seq_str(self.header.purge_seq),
            "update_seq": self.seq_str(self.header.update_seq),
            "sizes": {"file": self.file.eof(), "external": red.size, "active": active},
            "props": {},
            "doc_del_count": red.del_count,
            "doc_count": red.doc_count,
            "disk_format_version": 8,
            "compact_running": compacting,
            "cluster": {"q": 1, "n": 1, "w": 1, "r": 1},
            "instance_start_time": "0"
        })
    }

    /// Find a revision in the doc's tree; None if absent or body missing.
    pub fn find_rev(fdi: &FullDocInfo, pos: u64, id: &str) -> Option<RevInfo> {
        keytree::get(&fdi.tree, pos, id).map(|(val, path)| RevInfo {
            pos,
            id: id.to_string(),
            deleted: val.deleted(),
            val,
            path,
        })
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
            // Stored chunk pointers are only valid in this file.
            att_gen: if body.atts.is_empty() { 0 } else { self.file.generation },
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

    pub fn fold_docs<F>(&self, dir: Dir, range: &Range<String>, mut f: F) -> Result<()>
    where
        F: FnMut(FullDocInfo) -> Result<bool>,
    {
        let mut err = None;
        self.ids.fold(dir, range, |k, v| {
            let fdi = FullDocInfo {
                id: k.clone(),
                update_seq: v.update_seq,
                deleted: v.deleted,
                tree: v.tree.clone(),
            };
            match f(fdi) {
                Ok(c) => Ok(c),
                Err(e) => {
                    err = Some(e);
                    Ok(false)
                }
            }
        })?;
        err.map_or(Ok(()), Err)
    }

    pub fn fold_changes<F>(&self, since: u64, dir: Dir, mut f: F) -> Result<()>
    where
        F: FnMut(&FullDocInfo) -> Result<bool>,
    {
        let range = match dir {
            Dir::Fwd => Range {
                start: Some(since + 1),
                end: None,
                inclusive_end: true,
            },
            Dir::Rev => Range {
                start: if since == 0 { None } else { Some(since) },
                end: None,
                inclusive_end: true,
            },
        };
        let mut err = None;
        self.seqs.fold(dir, &range, |_, v| match f(v) {
            Ok(c) => Ok(c),
            Err(e) => {
                err = Some(e);
                Ok(false)
            }
        })?;
        err.map_or(Ok(()), Err)
    }

    pub fn fold_locals<F>(&self, range: &Range<String>, dir: Dir, mut f: F) -> Result<()>
    where
        F: FnMut(&str, &LocalDoc) -> bool,
    {
        self.locals.fold(dir, range, |k, v| Ok(f(k, v)))?;
        Ok(())
    }
}

// ----------------------------------------------------------------- writer

pub type UpdateResult = Result<String>;

enum Msg {
    Update {
        docs: Vec<Doc>,
        new_edits: bool,
        reply: oneshot::Sender<Vec<UpdateResult>>,
    },
    SetSecurity(Vec<u8>, oneshot::Sender<()>),
    SetRevsLimit(u64, oneshot::Sender<()>),
    Purge(Vec<(String, Vec<(u64, String)>)>, oneshot::Sender<Result<Value>>),
    CompactDone(Box<Snapshot>, PathBuf, oneshot::Sender<Result<()>>),
}

pub struct Db {
    pub name: String,
    pub path: PathBuf,
    state: RwLock<Snapshot>,
    tx: Mutex<mpsc::Sender<Msg>>,
    seq_tx: watch::Sender<u64>,
    compacting: AtomicBool,
    /// Per-database extension state (view indexes), keyed by signature.
    pub ext: Mutex<HashMap<String, Arc<dyn std::any::Any + Send + Sync>>>,
}

impl Db {
    pub fn open(path: &FsPath, name: &str, create: bool) -> Result<Arc<Db>> {
        if create
            && let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
        let file = Arc::new(CouchFile::open(path, create)?);
        let header: Header = match file.read_header()? {
            Some(h) => h,
            None => {
                let h = Header {
                    version: DISK_VERSION,
                    uuid: uuid::Uuid::new_v4().simple().to_string(),
                    update_seq: 0,
                    purge_seq: 0,
                    id_root: None,
                    seq_root: None,
                    local_root: None,
                    security: b"{}".to_vec(),
                    revs_limit: 1000,
                    props: b"{}".to_vec(),
                };
                file.write_header(&h)?;
                file.sync()?;
                h
            }
        };
        let mut snap = Snapshot {
            name: name.to_string(),
            ids: Btree::new(file.clone(), header.id_root.clone(), Arc::new(IdSpec)),
            seqs: Btree::new(file.clone(), header.seq_root.clone(), Arc::new(SeqSpec)),
            locals: Btree::new(file.clone(), header.local_root.clone(), Arc::new(LocalSpec)),
            file,
            header,
        };
        if snap.header.prop_u64(STORED_LIVE).is_none() {
            // Older file: total the stored size of live leaves once; it is
            // persisted with the next commit and maintained from then on.
            let mut total = 0u64;
            snap.fold_docs(Dir::Fwd, &Range::default(), |fdi| {
                total += fdi.leaf_stored();
                Ok(true)
            })?;
            snap.header.set_prop_u64(STORED_LIVE, total);
        }
        let (tx, rx) = mpsc::channel();
        let (seq_tx, _) = watch::channel(snap.header.update_seq);
        let db = Arc::new(Db {
            name: name.to_string(),
            path: path.to_path_buf(),
            state: RwLock::new(snap),
            tx: Mutex::new(tx),
            seq_tx,
            compacting: AtomicBool::new(false),
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
        self.tx
            .lock()
            .send(m)
            .map_err(|_| CouchError::internal("database writer stopped"))
    }

    pub async fn update_docs(&self, docs: Vec<Doc>, new_edits: bool) -> Result<Vec<UpdateResult>> {
        let (reply, rx) = oneshot::channel();
        self.send(Msg::Update {
            docs,
            new_edits,
            reply,
        })?;
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

    /// Write a doc's new attachment bytes to the file before it reaches the
    /// writer thread (CouchDB streams attachments the same way), so large
    /// uploads don't stall every other write. The writer rejects the doc if
    /// compaction swapped the file in between.
    pub fn stage_atts(&self, doc: &mut Doc) -> Result<()> {
        if !doc.atts.iter().any(|a| matches!(a.data, AttData::Bytes(_))) {
            return Ok(());
        }
        let snap = self.snapshot();
        let file = snap.file.clone();
        for a in doc.atts.iter_mut() {
            if let AttData::Bytes(bytes) = &a.data {
                let (chunks, stored) = write_att_chunks(&file, bytes, snap.tagged_chunks())?;
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
        doc.att_gen = file.generation;
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
            let r = db.compact_blocking();
            if let Err(e) = r {
                tracing::error!("compaction of {} failed: {}", db.name, e.reason);
            }
            db.compacting.store(false, AtomicOrdering::SeqCst);
        });
        Ok(())
    }

    fn compact_blocking(&self) -> Result<()> {
        let snap = self.snapshot();
        let tmp = self.path.with_extension("couch.compact");
        let _ = std::fs::remove_file(&tmp);
        let file = Arc::new(CouchFile::open(&tmp, true)?);
        let mut header = snap.header.clone();
        header.version = DISK_VERSION;
        header.id_root = None;
        header.seq_root = None;
        header.local_root = None;
        let mut new = Snapshot {
            name: snap.name.clone(),
            ids: Btree::new(file.clone(), None, Arc::new(IdSpec)),
            seqs: Btree::new(file.clone(), None, Arc::new(SeqSpec)),
            locals: Btree::new(file.clone(), None, Arc::new(LocalSpec)),
            file: file.clone(),
            header,
        };
        // Copy bodies in seq order (sequential appends) while building the
        // indexes without copy-on-write: by-seq keys already arrive sorted
        // and stream into a bottom-up builder; by-id entries go through an
        // external sort first. Every index node is written exactly once.
        let mut seqs = new.seqs.bulk_builder();
        type IdCmp = Box<dyn Fn(&(String, FdiVal), &(String, FdiVal)) -> Ordering>;
        let id_cmp: IdCmp = Box::new(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let mut ids = crate::extsort::Sorter::new(
            self.path.with_extension("couch.compact.idsort"),
            crate::extsort::budget_bytes(),
            id_cmp,
        );
        let mut stored_total = 0u64;
        snap.fold_changes(0, Dir::Fwd, |fdi| {
            let copied = copy_fdi(&snap, &file, fdi)?;
            stored_total += copied.leaf_stored();
            let approx = 64 + copied.id.len() + 48 * keytree::leafs(&copied.tree).len();
            ids.push(
                (copied.id.clone(), FdiVal { update_seq: copied.update_seq, deleted: copied.deleted, tree: copied.tree.clone() }),
                approx,
            )?;
            seqs.push(copied.update_seq, copied)?;
            Ok(true)
        })?;
        new.seqs = seqs.finish()?;
        let mut sorted = ids.finish()?;
        let mut idb = new.ids.bulk_builder();
        while let Some((k, v)) = sorted.next_item()? {
            idb.push(k, v)?;
        }
        new.ids = idb.finish()?;
        let mut locals = new.locals.bulk_builder();
        snap.locals.fold(Dir::Fwd, &Range::default(), |k, v| {
            locals.push(k.clone(), v.clone())?;
            Ok(true)
        })?;
        new.locals = locals.finish()?;
        new.header.set_prop_u64(STORED_LIVE, stored_total);
        commit_header(&mut new)?;
        let (reply, rx) = oneshot::channel();
        self.send(Msg::CompactDone(Box::new(new), tmp, reply))?;
        rx.blocking_recv().map_err(|_| CouchError::internal("compaction swap failed"))?
    }

    pub fn delete_files(&self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(self.path.with_extension("couch.compact"));
        let _ = std::fs::remove_file(self.path.with_extension("couch.compact.idsort"));
    }
}

/// Copy docs changed since the bulk copy into a compaction target,
/// replacing the entries copied earlier.
/// Returns the change in stored live bytes.
fn copy_docs(src: &Snapshot, dst: &mut Snapshot, fdis: Vec<FullDocInfo>) -> Result<i128> {
    let mut delta = 0i128;
    if fdis.is_empty() {
        return Ok(0);
    }
    let mut id_acts = Vec::new();
    let mut seq_acts = Vec::new();
    for fdi in fdis {
        if let Some(old) = dst.ids.lookup(&fdi.id)? {
            seq_acts.push(Action::Remove(old.update_seq));
            let old = FullDocInfo { id: fdi.id.clone(), update_seq: old.update_seq, deleted: old.deleted, tree: old.tree };
            delta -= old.leaf_stored() as i128;
        }
        let new = copy_fdi(src, &dst.file, &fdi)?;
        delta += new.leaf_stored() as i128;
        id_acts.push(Action::Insert(
            new.id.clone(),
            FdiVal {
                update_seq: new.update_seq,
                deleted: new.deleted,
                tree: new.tree.clone(),
            },
        ));
        seq_acts.push(Action::Insert(new.update_seq, new));
    }
    dst.ids = dst.ids.modify(id_acts)?;
    dst.seqs = dst.seqs.modify(seq_acts)?;
    Ok(delta)
}

/// Copy a doc's leaf bodies (and their attachments) into `dst`. Non-leaf
/// revisions keep their place in the tree but lose their bodies.
fn copy_fdi(src: &Snapshot, dst: &CouchFile, fdi: &FullDocInfo) -> Result<FullDocInfo> {
    let mut err = None;
    let tree = keytree::map_vals(&fdi.tree, &mut |_, _, v, is_leaf| match v {
        RevVal::Avail(l) if is_leaf => match copy_body(src, dst, l.ptr) {
            Ok((ptr, stored)) => RevVal::Avail(LeafVal { ptr, stored, ..l.clone() }),
            Err(e) => {
                err = Some(e);
                RevVal::Missing
            }
        },
        _ => RevVal::Missing,
    });
    match err {
        Some(e) => Err(e),
        None => Ok(FullDocInfo { tree, ..fdi.clone() }),
    }
}

/// Compaction always writes format 2: chunks already stored tagged are
/// copied as is (no recompression); format-1 raw chunks get encoded.
/// Returns the new body position and its stored size with attachments.
fn copy_body(src: &Snapshot, dst: &CouchFile, ptr: Pos) -> Result<(Pos, u64)> {
    let mut body = src.read_body(ptr)?;
    for a in body.atts.iter_mut() {
        let mut chunks = Vec::with_capacity(a.chunks.len());
        let mut stored_len = 0u64;
        for p in &a.chunks {
            let stored = src.file.read_raw(*p)?;
            let out = if src.tagged_chunks() { stored } else { crate::file::encode_chunk(&stored) };
            stored_len += out.len() as u64;
            chunks.push(dst.append_raw(&out)?.0);
        }
        a.chunks = chunks;
        a.stored_length = stored_len;
    }
    let att_stored: u64 = body.atts.iter().map(|a| a.stored_length).sum();
    let encoded = crate::file::encode_value(&body);
    let stored = encoded.len() as u64 + att_stored;
    Ok((dst.append_raw(&encoded)?.0, stored))
}

fn commit_header(s: &mut Snapshot) -> Result<()> {
    s.header.id_root = s.ids.root.clone();
    s.header.seq_root = s.seqs.root.clone();
    s.header.local_root = s.locals.root.clone();
    s.file.sync()?;
    s.file.write_header(&s.header)?;
    s.file.sync()?;
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
                Msg::Update {
                    docs,
                    new_edits,
                    reply,
                } => updates.push((docs, new_edits, reply)),
                other => {
                    flush_updates(&db, std::mem::take(&mut updates));
                    handle_other(&db, other);
                }
            }
        }
        flush_updates(&db, updates);
    }
}

fn handle_other(db: &Db, m: Msg) {
    let mut snap = db.snapshot();
    match m {
        Msg::Update { .. } => unreachable!(),
        Msg::SetSecurity(sec, reply) => {
            snap.header.security = sec;
            let _ = commit_and_swap(db, snap);
            let _ = reply.send(());
        }
        Msg::SetRevsLimit(n, reply) => {
            snap.header.revs_limit = n;
            let _ = commit_and_swap(db, snap);
            let _ = reply.send(());
        }
        Msg::Purge(req, reply) => {
            let r = do_purge(db, snap, req);
            let _ = reply.send(r);
        }
        Msg::CompactDone(new, tmp, reply) => {
            let r = finish_compaction(db, snap, *new, &tmp);
            let _ = reply.send(r);
        }
    }
}

fn finish_compaction(db: &Db, cur: Snapshot, mut new: Snapshot, tmp: &FsPath) -> Result<()> {
    let compacted = new.header.update_seq;
    let mut changed = Vec::new();
    cur.fold_changes(compacted, Dir::Fwd, |fdi| {
        changed.push(fdi.clone());
        Ok(true)
    })?;
    let delta = copy_docs(&cur, &mut new, changed)?;
    let stored = add_signed(new.header.prop_u64(STORED_LIVE).unwrap_or(0), delta);
    let mut locals = Vec::new();
    cur.locals.fold(Dir::Fwd, &Range::default(), |k, v| {
        locals.push(Action::Insert(k.clone(), v.clone()));
        Ok(true)
    })?;
    // Replace local docs wholesale; there are few of them.
    new.locals = Btree::new(new.file.clone(), None, Arc::new(LocalSpec)).modify(locals)?;
    new.header = Header {
        version: DISK_VERSION,
        id_root: None,
        seq_root: None,
        local_root: None,
        ..cur.header.clone()
    };
    new.header.set_prop_u64(STORED_LIVE, stored);
    commit_header(&mut new)?;
    std::fs::rename(tmp, &db.path)?;
    *db.state.write() = new;
    Ok(())
}

fn commit_and_swap(db: &Db, mut snap: Snapshot) -> Result<()> {
    commit_header(&mut snap)?;
    let seq = snap.header.update_seq;
    *db.state.write() = snap;
    db.seq_tx.send_replace(seq);
    Ok(())
}

type UpdateBatch = Vec<(Vec<Doc>, bool, oneshot::Sender<Vec<UpdateResult>>)>;

fn flush_updates(db: &Db, batch: UpdateBatch) {
    if batch.is_empty() {
        return;
    }
    let snap = db.snapshot();
    match apply_updates(&snap, &batch) {
        Ok((new, results)) => {
            let ok = commit_and_swap(db, new);
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

fn apply_updates(snap: &Snapshot, batch: &UpdateBatch) -> Result<(Snapshot, Vec<Vec<UpdateResult>>)> {
    let mut fdis: HashMap<String, Option<FullDocInfo>> = HashMap::new();
    let mut locals: HashMap<String, Option<LocalDoc>> = HashMap::new();
    let mut original_seq: HashMap<String, Option<u64>> = HashMap::new();
    let mut original_stored: HashMap<String, u64> = HashMap::new();
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
            if doc.att_gen != 0 && doc.att_gen != snap.file.generation {
                res.push(Err(att_file_moved()));
                continue;
            }
            if !fdis.contains_key(&doc.id) {
                let f = snap.get_fdi(&doc.id)?;
                original_seq.insert(doc.id.clone(), f.as_ref().map(|f| f.update_seq));
                original_stored.insert(doc.id.clone(), f.as_ref().map(|f| f.leaf_stored()).unwrap_or(0));
                fdis.insert(doc.id.clone(), f);
            }
            let slot = fdis.get_mut(&doc.id).unwrap();
            let r = if *new_edits {
                update_interactive(snap, slot, doc, revs_limit)
            } else {
                update_replicated(snap, slot, doc, revs_limit)
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

    let mut new = snap.clone();
    let mut seq = snap.header.update_seq;
    let mut id_acts = Vec::new();
    let mut seq_acts = Vec::new();
    let mut stored_delta = 0i128;
    for id in changed {
        let mut fdi = fdis.remove(&id).unwrap().unwrap();
        stored_delta += fdi.leaf_stored() as i128 - original_stored.get(&id).copied().unwrap_or(0) as i128;
        seq += 1;
        fdi.update_seq = seq;
        fdi.tree = keytree::map_vals(&fdi.tree, &mut |_, _, v, _| match v {
            RevVal::Avail(l) if l.seq == SEQ_PLACEHOLDER => RevVal::Avail(LeafVal { seq, ..l.clone() }),
            other => other.clone(),
        });
        fdi.deleted = fdi.winner().deleted;
        if let Some(Some(old)) = original_seq.get(&id) {
            seq_acts.push(Action::Remove(*old));
        }
        id_acts.push(Action::Insert(
            id.clone(),
            FdiVal {
                update_seq: seq,
                deleted: fdi.deleted,
                tree: fdi.tree.clone(),
            },
        ));
        seq_acts.push(Action::Insert(seq, fdi));
    }
    let mut local_acts = Vec::new();
    for id in local_changed {
        match locals.remove(&id).unwrap() {
            Some(l) => local_acts.push(Action::Insert(id, l)),
            None => local_acts.push(Action::Remove(id)),
        }
    }
    new.ids = new.ids.modify(id_acts)?;
    new.seqs = new.seqs.modify(seq_acts)?;
    new.locals = new.locals.modify(local_acts)?;
    new.header.update_seq = seq;
    let stored = add_signed(snap.stored_live(), stored_delta);
    new.header.set_prop_u64(STORED_LIVE, stored);
    Ok((new, results))
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
    *slot = Some(LocalDoc {
        rev,
        body: serde_json::to_vec(&doc.body)?,
    });
    Ok(format!("0-{rev}"))
}

fn missing_stub(doc: &Doc, name: &str) -> CouchError {
    CouchError::precondition(
        "missing_stub",
        format!("Invalid attachment stub in {} for {}", doc.id, name),
    )
}

/// Resolve stubs and write new attachment data, returning stored infos.
fn prepare_atts(
    snap: &Snapshot,
    doc: &Doc,
    prev: Option<&DiskBody>,
    new_pos: u64,
) -> Result<Vec<AttInfo>> {
    let mut out = Vec::with_capacity(doc.atts.len());
    for a in &doc.atts {
        let info = match &a.data {
            AttData::Stub => {
                let p = prev
                    .and_then(|b| b.atts.iter().find(|x| x.name == a.name))
                    .ok_or_else(|| missing_stub(doc, &a.name))?;
                if let Some(rp) = a.revpos
                    && rp != p.revpos && rp > new_pos {
                        return Err(missing_stub(doc, &a.name));
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
                let (chunks, stored_length) = write_att_chunks(&snap.file, bytes, snap.tagged_chunks())?;
                let md5 = if a.md5.is_empty() || a.encoding.is_none() {
                    Md5::digest(bytes).to_vec()
                } else {
                    a.md5.clone()
                };
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
                return Err(CouchError::bad_request(format!(
                    "attachment {} data was not supplied",
                    a.name
                )));
            }
        };
        out.push(info);
    }
    Ok(out)
}

/// Append attachment data as 64 KiB chunks, 16 MiB per write. In tagged
/// (format 2) files each chunk is LZ4-compressed when that shrinks it.
/// Returns the chunk positions and the bytes they occupy as stored.
fn write_att_chunks(file: &CouchFile, bytes: &[u8], tagged: bool) -> Result<(Vec<Pos>, u64)> {
    let mut chunks = Vec::with_capacity(bytes.len() / ATT_CHUNK + 1);
    let mut stored = 0u64;
    let pieces: Vec<&[u8]> = bytes.chunks(ATT_CHUNK).collect();
    for group in pieces.chunks(256) {
        let written = if tagged {
            let encoded: Vec<Vec<u8>> = group.iter().map(|c| crate::file::encode_chunk(c)).collect();
            stored += encoded.iter().map(|e| e.len() as u64).sum::<u64>();
            file.append_raw_many(&encoded)?
        } else {
            stored += group.iter().map(|c| c.len() as u64).sum::<u64>();
            file.append_raw_many(group)?
        };
        chunks.extend(written.into_iter().map(|(p, _)| p));
    }
    Ok((chunks, stored))
}

fn att_file_moved() -> CouchError {
    CouchError::new(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "att_file_moved",
        "database file changed while attachments were being written; retry",
    )
}

fn write_rev(snap: &Snapshot, doc: &Doc, atts: Vec<AttInfo>, body_json: Vec<u8>) -> Result<LeafVal> {
    let size = body_json.len() as u64 + atts.iter().map(|a| a.encoded_length).sum::<u64>();
    let att_stored: u64 = atts.iter().map(|a| a.stored_length).sum();
    let encoded = crate::file::encode_value(&DiskBody { json: body_json, atts });
    let stored = encoded.len() as u64 + att_stored;
    let (ptr, _) = snap.file.append_raw(&encoded)?;
    Ok(LeafVal {
        deleted: doc.deleted,
        ptr,
        seq: SEQ_PLACEHOLDER,
        size,
        stored,
    })
}

fn update_interactive(
    snap: &Snapshot,
    slot: &mut Option<FullDocInfo>,
    doc: &Doc,
    revs_limit: u64,
) -> Result<(String, bool)> {
    let parent: Option<RevInfo> = match (slot.as_ref(), doc.revs.1.first()) {
        (None, None) => None,
        (None, Some(_)) => return Err(CouchError::conflict()),
        (Some(f), None) => {
            let w = f.winner();
            if w.deleted {
                Some(w)
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
        Some(l) => Some(snap.read_body(l.ptr)?),
        None => None,
    };
    let new_pos = parent.as_ref().map(|p| p.pos + 1).unwrap_or(1);
    let atts = prepare_atts(snap, doc, prev_body.as_ref(), new_pos)?;
    let body_json = serde_json::to_vec(&doc.body)?;
    let mut hash_doc = doc.clone();
    hash_doc.atts = atts.iter().map(Att::from_info).collect();
    let new_id = new_rev_id(&hash_doc, parent.as_ref().map(|p| (p.pos, p.id.as_str())), &body_json);
    let leaf = write_rev(snap, doc, atts, body_json)?;
    let path = match &parent {
        Some(p) => Path {
            start: p.pos,
            revs: vec![(p.id.clone(), RevVal::Missing), (new_id.clone(), RevVal::Avail(leaf))],
        },
        None => Path {
            start: 1,
            revs: vec![(new_id.clone(), RevVal::Avail(leaf))],
        },
    };
    let fdi = slot.get_or_insert_with(|| FullDocInfo {
        id: doc.id.clone(),
        update_seq: 0,
        deleted: false,
        tree: Vec::new(),
    });
    keytree::merge(&mut fdi.tree, path, revs_limit);
    Ok((format!("{new_pos}-{new_id}"), true))
}

fn update_replicated(
    snap: &Snapshot,
    slot: &mut Option<FullDocInfo>,
    doc: &Doc,
    revs_limit: u64,
) -> Result<(String, bool)> {
    let (pos, ids) = (&doc.revs.0, &doc.revs.1);
    if ids.is_empty() {
        return Err(CouchError::bad_request("Document rev and _revisions are required with new_edits=false"));
    }
    let rev_str = format!("{}-{}", pos, ids[0]);
    if let Some(f) = slot.as_ref()
        && let Some((RevVal::Avail(_), _)) = keytree::get(&f.tree, *pos, &ids[0]) {
            return Ok((rev_str, false));
        }
    // Stubs refer to the nearest ancestor we have a body for.
    let mut prev_body = None;
    if let Some(f) = slot.as_ref() {
        for (i, id) in ids.iter().enumerate().skip(1) {
            if let Some((RevVal::Avail(l), _)) = keytree::get(&f.tree, pos - i as u64, id) {
                prev_body = Some(snap.read_body(l.ptr)?);
                break;
            }
        }
        if prev_body.is_none() && doc.atts.iter().any(|a| matches!(a.data, AttData::Stub)) {
            // Fall back to any leaf holding the stubbed attachments.
            for l in f.sorted_leafs() {
                if let Some(lv) = l.val.leaf() {
                    let b = snap.read_body(lv.ptr)?;
                    if doc
                        .atts
                        .iter()
                        .filter(|a| matches!(a.data, AttData::Stub))
                        .all(|a| b.atts.iter().any(|x| x.name == a.name))
                    {
                        prev_body = Some(b);
                        break;
                    }
                }
            }
        }
    }
    let atts = prepare_atts(snap, doc, prev_body.as_ref(), *pos)?;
    let body_json = serde_json::to_vec(&doc.body)?;
    let leaf = write_rev(snap, doc, atts, body_json)?;
    let n = ids.len();
    let start = pos + 1 - n as u64;
    let revs: Vec<(String, RevVal)> = ids
        .iter()
        .rev()
        .enumerate()
        .map(|(i, id)| {
            let v = if i == n - 1 {
                RevVal::Avail(leaf.clone())
            } else {
                RevVal::Missing
            };
            (id.clone(), v)
        })
        .collect();
    let fdi = slot.get_or_insert_with(|| FullDocInfo {
        id: doc.id.clone(),
        update_seq: 0,
        deleted: false,
        tree: Vec::new(),
    });
    keytree::merge(&mut fdi.tree, Path { start, revs }, revs_limit);
    Ok((rev_str, true))
}

fn do_purge(db: &Db, snap: Snapshot, req: Vec<(String, Vec<(u64, String)>)>) -> Result<Value> {
    let mut purged = Map::new();
    let mut stored_delta = 0i128;
    let mut new = snap.clone();
    let mut seq = snap.header.update_seq;
    let mut id_acts = Vec::new();
    let mut seq_acts = Vec::new();
    for (id, revs) in req {
        let Some(mut fdi) = snap.get_fdi(&id)? else {
            purged.insert(id, json!([]));
            continue;
        };
        let before = fdi.leaf_stored();
        let removed = keytree::remove_leafs(&mut fdi.tree, &revs);
        stored_delta += fdi.leaf_stored() as i128 - before as i128;
        purged.insert(
            id.clone(),
            json!(removed.iter().map(|(p, r)| format!("{p}-{r}")).collect::<Vec<_>>()),
        );
        if removed.is_empty() {
            continue;
        }
        seq_acts.push(Action::Remove(fdi.update_seq));
        if fdi.tree.is_empty() {
            id_acts.push(Action::Remove(id));
        } else {
            seq += 1;
            fdi.update_seq = seq;
            fdi.deleted = fdi.winner().deleted;
            id_acts.push(Action::Insert(
                id,
                FdiVal {
                    update_seq: seq,
                    deleted: fdi.deleted,
                    tree: fdi.tree.clone(),
                },
            ));
            seq_acts.push(Action::Insert(seq, fdi));
        }
    }
    new.ids = new.ids.modify(id_acts)?;
    new.seqs = new.seqs.modify(seq_acts)?;
    new.header.update_seq = seq;
    new.header.purge_seq += 1;
    let stored = add_signed(snap.stored_live(), stored_delta);
    new.header.set_prop_u64(STORED_LIVE, stored);
    let purge_seq = new.header.purge_seq;
    commit_and_swap(db, new)?;
    Ok(json!({"purge_seq": purge_seq, "purged": purged}))
}
