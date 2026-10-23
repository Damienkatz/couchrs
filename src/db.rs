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
/// The database's properties ({"partitioned": true}), fixed at creation.
const PROPS_KEY: &[u8] = b"props";
/// When the database was created, in seconds (its instance_start_time,
/// as a CouchDB shard's file name records it).
const CREATED_KEY: &[u8] = b"created";
/// Purge infos, each under this prefix and its purge seq.
const PURGE_INFO_PREFIX: &[u8] = b"purge_info/";
/// The first key after them.
const PURGE_INFO_END: &[u8] = b"purge_info0";
/// How many purge infos compaction keeps (once indexes have seen them).
const PURGE_LIMIT_KEY: &[u8] = b"purge_infos_limit";
const DEFAULT_PURGE_INFOS_LIMIT: u64 = 1000;

fn purge_info_key(seq: u64) -> Vec<u8> {
    [PURGE_INFO_PREFIX, &be(seq)].concat()
}

/// One purge request item, recorded whether or not it removed anything:
/// the doc id and the revisions asked for, sorted and deduplicated.
#[derive(Clone, Debug)]
pub struct PurgeInfo {
    pub seq: u64,
    pub id: String,
    pub revs: Vec<(u64, String)>,
}

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
    pub props: Arc<Value>,
    /// instance_start_time.
    pub created: Arc<str>,
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
    pub fn partitioned(&self) -> bool {
        self.props.get("partitioned").and_then(|v| v.as_bool()).unwrap_or(false)
    }

    /// Document counts and sizes of one partition (/db/_partition/P).
    pub fn partition_info(&self, partition: &str) -> Result<Value> {
        let range = Range { start: Some(format!("{partition}:")), end: Some(format!("{partition};")), inclusive_end: false };
        let (mut docs, mut deleted, mut active, mut external) = (0u64, 0u64, 0u64, 0u64);
        self.fold_docs(Dir::Fwd, &range, |fdi| {
            if fdi.deleted {
                deleted += 1;
            } else {
                docs += 1;
            }
            for l in keytree::leafs(&fdi.tree) {
                if let RevVal::Avail(v) = &l.val {
                    active += v.stored;
                    external += v.size;
                }
            }
            Ok(true)
        })?;
        Ok(json!({
            "db_name": self.name,
            "sizes": {"active": active, "external": external},
            "partition": partition,
            "doc_count": docs,
            "doc_del_count": deleted,
        }))
    }

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

    /// The purge infos after `seq`, oldest first.
    pub fn purge_infos_after(&self, seq: u64) -> Result<Vec<PurgeInfo>> {
        let (lo, hi) = (purge_info_key(seq.saturating_add(1)), PURGE_INFO_END);
        let mut out = Vec::new();
        self.view.scan(CF_META, Some(&lo), Some(hi), false, |k, v| {
            let seq = u64::from_be_bytes(k[PURGE_INFO_PREFIX.len()..].try_into().map_err(|_| CouchError::internal("bad purge info key"))?);
            let (id, revs): (String, Vec<(u64, String)>) = dec(v)?;
            out.push(PurgeInfo { seq, id, revs });
            Ok(true)
        })?;
        Ok(out)
    }

    /// The purges an index at purge seq `seq` hasn't seen, or None when
    /// some are gone (pruned by compaction, or from before infos were
    /// kept) and the index has to be rebuilt.
    pub fn purges_since(&self, seq: u64) -> Result<Option<Vec<PurgeInfo>>> {
        let infos = self.purge_infos_after(seq)?;
        let complete = infos.len() as u64 == self.header.purge_seq.saturating_sub(seq) && infos.first().is_none_or(|i| i.seq == seq + 1);
        Ok(complete.then_some(infos))
    }

    /// The oldest purge info kept, or 0 for none.
    pub fn oldest_purge_seq(&self) -> Result<u64> {
        let mut oldest = 0;
        self.view.scan(CF_META, Some(PURGE_INFO_PREFIX), Some(PURGE_INFO_END), false, |k, _| {
            oldest = k[PURGE_INFO_PREFIX.len()..].try_into().map(u64::from_be_bytes).unwrap_or(0);
            Ok(false)
        })?;
        Ok(oldest)
    }

    pub fn purge_infos_limit(&self) -> Result<u64> {
        match self.view.get(CF_META, PURGE_LIMIT_KEY)? {
            Some(b) => dec(&b),
            None => Ok(DEFAULT_PURGE_INFOS_LIMIT),
        }
    }

    /// couch_db:get_minimum_purge_seq/1: the purge seq compaction may
    /// drop infos up to. Infos beyond the limit are kept while a purge
    /// checkpoint (`_local/purge-*`) of a client that still exists needs
    /// them, and every one kept while a checkpoint has no purge seq.
    pub fn minimum_purge_seq(&self, client_exists: impl Fn(&Map<String, Value>) -> bool) -> Result<u64> {
        let purge_seq = self.header.purge_seq as i128;
        let floor = purge_seq - self.purge_infos_limit()? as i128;
        let oldest = self.oldest_purge_seq()? as i128;
        let mut min = floor;
        let range = Range { start: Some("_local/purge-".into()), end: Some("_local/purge.".into()), inclusive_end: false };
        self.fold_locals(&range, Dir::Fwd, |_, l| {
            let props: Map<String, Value> = serde_json::from_slice(&l.body).unwrap_or_default();
            match props.get("purge_seq").and_then(|v| v.as_number()).and_then(crate::json::int128) {
                Some(cs) if cs >= floor => {}
                Some(cs) if client_exists(&props) => min = min.min(cs),
                Some(_) => {}
                None => min = min.min(oldest),
            }
            true
        })?;
        let min = if min < floor { min } else { floor };
        Ok(min.max(0) as u64)
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

    /// An attachment's data in identity form.
    pub fn read_att_decoded(&self, a: &AttInfo) -> Result<Vec<u8>> {
        let raw = self.read_att(a)?;
        match a.encoding.as_deref() {
            Some("gzip") => crate::doc::gunzip(&raw).map_err(|e| CouchError::internal(format!("bad gzip attachment: {e}"))),
            _ => Ok(raw),
        }
    }

    /// `sizes.external` counts inflated body and attachment bytes.
    /// `sizes.active` counts live data as stored: deflated bodies and
    /// attachments plus RocksDB's estimate of live index data. `sizes.file`
    /// is every file in the database's RocksDB directory.
    pub fn info(&self, compacting: bool) -> Value {
        let store = self.view.store();
        let meta: u64 = [CF_ID, CF_SEQ, CF_LOCAL].iter().map(|cf| store.prop(cf, "rocksdb.estimate-live-data-size")).sum();
        json!({
            "instance_start_time": &*self.created,
            "db_name": self.name,
            "purge_seq": self.seq_str(self.header.purge_seq),
            "update_seq": self.seq_str(self.header.update_seq),
            "sizes": {"file": store.disk_bytes(), "external": self.header.external, "active": self.header.stored_live + meta},
            "props": *self.props,
            "doc_del_count": self.header.del_count,
            "doc_count": self.header.doc_count,
            "disk_format_version": 8,
            "compact_running": compacting,
            "cluster": {"q": 1, "n": 1, "w": 1, "r": 1}
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
        let (mut m, body, tail) = self.doc_parts(fdi, rev, opts)?;
        let fields: Map<String, Value> = serde_json::from_slice(&body.json).unwrap_or_default();
        m.extend(fields);
        m.extend(tail);
        Ok(Value::Object(m))
    }

    /// A revision's JSON as doc_json renders it, as text, with the stored
    /// body spliced in unparsed (bodies hold none of the members added).
    pub fn doc_json_text(&self, fdi: &FullDocInfo, rev: &RevInfo, opts: &DocOpts) -> Result<String> {
        let (head, body, tail) = self.doc_parts(fdi, rev, opts)?;
        let Some(fields) = std::str::from_utf8(&body.json).ok().and_then(json_members) else {
            return Ok(self.doc_json(fdi, rev, opts)?.to_string());
        };
        let tail = Value::Object(tail).to_string();
        let mut out = Value::Object(head).to_string();
        out.pop();
        for part in [fields, json_members(&tail).unwrap_or("")] {
            if !part.is_empty() {
                out.push(',');
                out.push_str(part);
            }
        }
        out.push('}');
        Ok(out)
    }

    /// A revision's stored body and the members rendered before it (_id,
    /// _rev) and after it: _deleted, _revisions, the meta members, then the
    /// attachments, as couch_doc:to_json_obj/2 orders them.
    fn doc_parts(&self, fdi: &FullDocInfo, rev: &RevInfo, opts: &DocOpts) -> Result<(Map<String, Value>, DiskBody, Map<String, Value>)> {
        let leaf = rev.val.leaf().ok_or_else(CouchError::missing)?;
        let body = self.read_body(leaf.ptr)?;
        let mut head = Map::new();
        head.insert("_id".into(), json!(fdi.id));
        head.insert("_rev".into(), json!(rev.rev()));
        let mut m = Map::new();
        if leaf.deleted {
            m.insert("_deleted".into(), json!(true));
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
        // The doc's latest update seq, whichever revision this is.
        if opts.local_seq {
            m.insert("_local_seq".into(), json!(fdi.update_seq));
        }
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
                let stub = match (want, opts.follows) {
                    (true, true) => att_json(a, opts, AttBody::Follows),
                    (true, false) => att_json(a, opts, AttBody::Data(self.read_att_decoded(a)?)),
                    (false, _) => att_json(a, opts, AttBody::Stub),
                };
                atts.insert(a.name.clone(), stub);
            }
            m.insert("_attachments".into(), Value::Object(atts));
        }
        Ok((head, body, m))
    }

    /// Render the winning revision.
    pub fn winner_json(&self, fdi: &FullDocInfo, opts: &DocOpts) -> Result<Value> {
        self.doc_json(fdi, &fdi.winner(), opts)
    }

    /// The winning revision as doc_json_text renders it.
    pub fn winner_json_text(&self, fdi: &FullDocInfo, opts: &DocOpts) -> Result<String> {
        self.doc_json_text(fdi, &fdi.winner(), opts)
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

    /// Changes after `seq` (count_changes_since/2), by scanning.
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
    SetPurgeInfosLimit(u64, oneshot::Sender<()>),
    Purge(Vec<(String, Vec<(u64, String)>)>, oneshot::Sender<Result<Value>>),
    /// Compaction: forget bodies of non-leaf revisions (ptr < limit) for
    /// these docs and any doc changed after `since`.
    DropOldBodies { ids: Vec<String>, since: u64, limit: u64, reply: oneshot::Sender<Result<()>> },
    /// Publish a new snapshot of the same state.
    Refresh(oneshot::Sender<()>),
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
    /// Messages waiting for the writer.
    queued: Arc<AtomicU64>,
    /// The update seq the last compaction (since opening) started from.
    compacted_seq: AtomicU64,
}

impl Drop for Db {
    fn drop(&mut self) {
        crate::stat!("couchdb.open_databases").dec();
    }
}

/// What happened to a database (couch_event's events).
#[derive(Clone, Debug, PartialEq)]
pub enum DbEvent {
    Created,
    Deleted,
    /// Documents changed.
    Updated,
    /// _local documents changed.
    LocalUpdated,
    /// A design document changed.
    DdocUpdated,
    /// A view index committed, by design doc id.
    IndexCommit(String),
}

/// Every database's events, for whoever listens (_db_updates, smoosh, ken).
pub fn events() -> &'static tokio::sync::broadcast::Sender<(String, DbEvent)> {
    static EVENTS: std::sync::OnceLock<tokio::sync::broadcast::Sender<(String, DbEvent)>> = std::sync::OnceLock::new();
    EVENTS.get_or_init(|| tokio::sync::broadcast::channel(4096).0)
}

pub fn notify(db: &str, ev: DbEvent) {
    let _ = events().send((db.to_string(), ev));
}

/// Whether a database is one CouchDB keeps out of its read and write
/// counts (couch_server's sys_db option): users and replicator databases.
pub fn is_system_db(name: &str) -> bool {
    let base = name.rsplit('/').next().unwrap_or(name);
    matches!(base, "_users" | "_replicator") || name == "_global_changes"
}

/// Count `n` documents read on a client's behalf (database_reads).
pub fn count_reads(snap: &Snapshot, n: u64) {
    if n > 0 && !is_system_db(&snap.name) {
        crate::stat!("couchdb.database_reads").add(n as i64);
    }
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
        Self::open_with(path, name, create.then(|| (b"{}".to_vec(), json!({}))))
    }

    /// Create a database whose _security object starts as `security`, with
    /// properties `props` ({"partitioned": true} or {}).
    pub fn create(path: &FsPath, name: &str, security: &Value, props: &Value) -> Result<Arc<Db>> {
        Self::open_with(path, name, Some((serde_json::to_vec(security)?, props.clone())))
    }

    fn open_with(path: &FsPath, name: &str, new: Option<(Vec<u8>, Value)>) -> Result<Arc<Db>> {
        let create = new.is_some();
        let new_security = new.as_ref().map(|(s, _)| s.as_slice());
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
        let props: Value = match store.db.get_cf(&store.cf(CF_META), PROPS_KEY).map_err(rerr)? {
            Some(b) => serde_json::from_slice(&b).unwrap_or_else(|_| json!({})),
            None => {
                let p = new.as_ref().map(|(_, p)| p.clone()).unwrap_or_else(|| json!({}));
                if create {
                    let mut wo = WriteOptions::default();
                    wo.set_sync(true);
                    store.db.put_cf_opt(&store.cf(CF_META), PROPS_KEY, serde_json::to_vec(&p)?, &wo).map_err(rerr)?;
                }
                p
            }
        };
        let props = Arc::new(props);
        let created: Arc<str> = match store.db.get_cf(&store.cf(CF_META), CREATED_KEY).map_err(rerr)? {
            Some(b) => String::from_utf8_lossy(&b).into(),
            None if create => {
                let t = crate::http::now_secs().to_string();
                store.db.put_cf(&store.cf(CF_META), CREATED_KEY, t.as_bytes()).map_err(rerr)?;
                t.into()
            }
            // Made before creation times were kept: when its files were.
            None => std::fs::metadata(&generation)
                .and_then(|m| m.created().or_else(|_| m.modified()))
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs().to_string())
                .unwrap_or_else(|| "0".into())
                .into(),
        };
        let view = View::new(store.clone());
        // Chunk ids are allocated before their doc commits; reusing one an
        // uncommitted upload took is harmless (it was never referenced).
        let next_ptr = next_id_after(&view, CF_BODY)?.max(next_id_after(&view, CF_ATT)?);
        let snap = Snapshot { name: name.to_string(), header, props: props.clone(), created, view };
        let (tx, rx) = mpsc::channel();
        let (seq_tx, _) = watch::channel(snap.header.update_seq);
        let queued = Arc::new(AtomicU64::new(0));
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
            queued: queued.clone(),
            compacted_seq: AtomicU64::new(u64::MAX),
        });
        crate::stat!("couchdb.open_databases").inc();
        let weak = Arc::downgrade(&db);
        std::thread::Builder::new()
            .name(format!("db-writer:{name}"))
            .spawn(move || writer_loop(weak, rx, queued))
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
        self.queued.fetch_add(1, AtomicOrdering::Relaxed);
        self.tx.lock().send(m).map_err(|_| {
            self.queued.fetch_sub(1, AtomicOrdering::Relaxed);
            CouchError::internal("database writer stopped")
        })
    }

    /// Messages waiting for the writer.
    pub fn queue_len(&self) -> u64 {
        self.queued.load(AtomicOrdering::Relaxed)
    }

    pub async fn update_docs(&self, docs: Vec<Doc>, new_edits: bool) -> Result<Vec<UpdateResult>> {
        if !is_system_db(&self.name) {
            crate::stat!("couchdb.database_writes").inc();
        }
        let (reply, rx) = oneshot::channel();
        self.send(Msg::Update { docs, new_edits, reply })?;
        let t = std::time::Instant::now();
        let r = rx.await.map_err(|_| CouchError::internal("update failed"));
        crate::stat!("couchdb.collect_results_time").observe(crate::stats::ms_since(t));
        r
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

    pub async fn set_purge_infos_limit(&self, n: u64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(Msg::SetPurgeInfosLimit(n, reply))?;
        rx.await.map_err(|_| CouchError::internal("update failed"))
    }

    /// Write a _local doc as revision `rev` + 1 (0-1 without one), or
    /// with no body delete it, from code that may be running async or
    /// blocking, and wait for it: purge checkpoints.
    pub fn put_local_blocking(&self, id: &str, rev: Option<u64>, body: Option<Map<String, Value>>) -> Result<()> {
        let doc = Doc {
            id: id.to_string(),
            revs: (0, rev.map(|r| r.to_string()).into_iter().collect()),
            deleted: body.is_none(),
            body: body.unwrap_or_default(),
            ..Default::default()
        };
        let (reply, rx) = oneshot::channel();
        self.send(Msg::Update { docs: vec![doc], new_edits: true, reply })?;
        let mut res = tokio::task::block_in_place(|| rx.blocking_recv()).map_err(|_| CouchError::internal("update failed"))?;
        res.pop().unwrap_or_else(|| Err(CouchError::internal("update failed"))).map(|_| ())
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
                a.data = AttData::Staged(chunks);
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
            if let Err(e) = db.run_compaction() {
                tracing::error!("compaction of {} failed: {}", db.name, e.reason);
            }
        });
        Ok(())
    }

    /// Compact now, on this thread; if a compaction is already running,
    /// wait for it instead.
    pub fn compact_and_wait(&self) -> Result<()> {
        if self.compacting.swap(true, AtomicOrdering::SeqCst) {
            while self.is_compacting() {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            return Ok(());
        }
        self.run_compaction()
    }

    /// Compact, with `compacting` already claimed; releases it.
    fn run_compaction(&self) -> Result<()> {
        self.compacted_seq.store(self.snapshot().update_seq(), AtomicOrdering::Relaxed);
        let r = self.compact_blocking();
        self.compacting.store(false, AtomicOrdering::SeqCst);
        r
    }

    /// The update seq the last compaction started from, if any.
    pub fn compacted_seq(&self) -> Option<u64> {
        Some(self.compacted_seq.load(AtomicOrdering::Relaxed)).filter(|&s| s != u64::MAX)
    }

    /// Compaction for the RocksDB engine:
    /// 1. mark every body and chunk referenced by a live leaf revision,
    /// 2. have the writer drop non-leaf revision bodies from rev trees,
    /// 3. delete unreferenced bodies and chunks allocated before the mark,
    /// 4. compact every column family so RocksDB reclaims the space,
    ///    including garbage collection of blob files.
    fn compact_blocking(&self) -> Result<()> {
        // couch_db_engine:trigger_on_compact/1: indexes' purge checkpoints
        // exist before the purge infos they protect are pruned.
        for r in [crate::views::ensure_purge_checkpoints(self), crate::nouveau::ensure_purge_checkpoints(self)] {
            if let Err(e) = r {
                tracing::warn!("purge checkpoints of {}: {}", self.name, e.reason);
            }
        }
        let snap = self.snapshot();
        let limit = self.next_ptr.load(AtomicOrdering::SeqCst);
        let mut live_bodies: Vec<u64> = Vec::new();
        let mut live_chunks: Vec<u64> = Vec::new();
        let mut with_old: Vec<String> = Vec::new();
        // Phases: document_copy walks every doc (as CouchDB's copy does)
        // marking what is live, then sweep and store_compaction reclaim.
        let task = crate::tasks::add(json!({
            "type": "database_compaction",
            "database": self.name,
            "phase": "document_copy",
            "progress": 0,
            "changes_done": 0,
            "total_changes": snap.header.doc_count + snap.header.del_count,
        }));
        let mut n = 0u64;
        snap.fold_docs(Dir::Fwd, &Range::default(), |fdi| {
            n += 1;
            if n % 1000 == 0 {
                task.add_changes(1000);
            }
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
        task.add_changes(n % 1000);
        live_bodies.sort_unstable();
        live_chunks.sort_unstable();
        live_chunks.dedup();
        task.update(json!({"phase": "sweep", "progress": 0, "changes_done": 0, "total_changes": 0}));

        let (reply, rx) = oneshot::channel();
        self.send(Msg::DropOldBodies { ids: with_old, since: snap.update_seq(), limit, reply })?;
        rx.blocking_recv().map_err(|_| CouchError::internal("compaction failed"))??;

        sweep(&self.store, CF_BODY, &live_bodies, limit)?;
        sweep(&self.store, CF_ATT, &live_chunks, limit)?;
        // Purge infos up to the minimum purge seq go, as
        // couch_bt_engine_compactor copies only those after it.
        let min_purge =
            snap.minimum_purge_seq(|props| crate::views::purge_client_exists(&snap, props) || crate::nouveau::purge_client_exists(&snap, props))?;
        if min_purge > 0 {
            let mut batch = WriteBatch::default();
            batch.delete_range_cf(&self.store.cf(CF_META), purge_info_key(0), purge_info_key(min_purge + 1));
            self.store.db.write(batch).map_err(rerr)?;
        }
        // RocksDB keeps what any snapshot older than the sweep can see,
        // so let go of ours and have readers move to a new one first.
        drop(snap);
        let (reply, rx) = oneshot::channel();
        self.send(Msg::Refresh(reply))?;
        rx.blocking_recv().map_err(|_| CouchError::internal("compaction failed"))?;
        task.update(json!({"phase": "store_compaction", "total_changes": CFS.len()}));

        let mut co = rocksdb::CompactOptions::default();
        co.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
        for (cf, _) in CFS {
            self.store.db.compact_range_cf_opt(&self.store.cf(cf), None::<&[u8]>, None::<&[u8]>, &co);
            task.add_changes(1);
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

fn writer_loop(weak: Weak<Db>, rx: mpsc::Receiver<Msg>, queued: Arc<AtomicU64>) {
    while let Ok(first) = rx.recv() {
        let Some(db) = weak.upgrade() else { return };
        let mut pending = vec![first];
        while let Ok(m) = rx.try_recv() {
            pending.push(m);
            if pending.len() >= 256 {
                break;
            }
        }
        queued.fetch_sub(pending.len() as u64, AtomicOrdering::Relaxed);
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
    // A synced write is RocksDB's fsync of its log.
    let t = std::time::Instant::now();
    db.store.db.write_opt(batch, &wo).map_err(rerr)?;
    crate::stat!("fsync.time").observe(crate::stats::ms_since(t));
    crate::stat!("fsync.count").inc();
    crate::stat!("couchdb.commits").inc();
    let seq = header.update_seq;
    let (props, created) = {
        let st = db.state.read();
        (st.props.clone(), st.created.clone())
    };
    *db.state.write() = Snapshot { name: db.name.clone(), header, props, created, view: View::new(db.store.clone()) };
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
        Msg::SetPurgeInfosLimit(n, reply) => {
            let mut batch = WriteBatch::default();
            batch.put_cf(&db.store.cf(CF_META), PURGE_LIMIT_KEY, enc(&n));
            let _ = commit(db, batch, snap.header.clone());
            let _ = reply.send(());
        }
        Msg::Purge(req, reply) => {
            let res = do_purge(db, snap, req);
            if res.is_ok() {
                // Indexes (ken) catch up with purges as with updates.
                notify(&db.name, DbEvent::Updated);
            }
            let _ = reply.send(res);
        }
        Msg::DropOldBodies { ids, since, limit, reply } => {
            let _ = reply.send(drop_old_bodies(db, snap, ids, since, limit));
        }
        Msg::Refresh(reply) => {
            *db.state.write() = Snapshot { view: View::new(db.store.clone()), ..snap };
            let _ = reply.send(());
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
    // Requests after the first were coalesced into this commit.
    for (_, new_edits, _) in batch.iter().skip(1) {
        if *new_edits {
            crate::stat!("couchdb.coalesced_updates.interactive").inc();
        } else {
            crate::stat!("couchdb.coalesced_updates.replicated").inc();
        }
    }
    let snap = db.snapshot();
    match apply_updates(db, &snap, &batch) {
        Ok((wb, header, results, counts)) => {
            let ok = commit(db, wb, header);
            if ok.is_ok() {
                crate::stat!("couchdb.document_inserts").add(counts.inserts as i64);
                crate::stat!("couchdb.document_writes").add(counts.writes as i64);
                crate::stat!("couchdb.local_document_writes").add(counts.local_writes as i64);
                if counts.writes > 0 {
                    notify(&db.name, DbEvent::Updated);
                }
                if counts.ddocs > 0 {
                    notify(&db.name, DbEvent::DdocUpdated);
                }
                if counts.local_writes > 0 {
                    notify(&db.name, DbEvent::LocalUpdated);
                }
            }
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

/// Documents an update batch wrote: new ones, all, design docs, and
/// local ones.
#[derive(Default)]
struct WriteCounts {
    inserts: u64,
    writes: u64,
    ddocs: u64,
    local_writes: u64,
}

type Applied = (WriteBatch, Header, Vec<Vec<UpdateResult>>, WriteCounts);

fn apply_updates(db: &Db, snap: &Snapshot, batch: &UpdateBatch) -> Result<Applied> {
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
    let mut counts = WriteCounts { local_writes: local_changed.len() as u64, ..Default::default() };
    // Seqs go to a batch's docs in id order, as couch_db_updater sorts
    // them (group_alike_docs), whatever order they came in.
    changed.sort_unstable();
    for id in changed {
        counts.writes += 1;
        if id.starts_with("_design/") {
            counts.ddocs += 1;
        }
        let mut fdi = fdis.remove(&id).unwrap().unwrap();
        seq += 1;
        fdi.update_seq = seq;
        fdi.tree = keytree::map_vals(&fdi.tree, &mut |_, _, v, _| match v {
            RevVal::Avail(l) if l.seq == SEQ_PLACEHOLDER => RevVal::Avail(LeafVal { seq, ..l.clone() }),
            other => other.clone(),
        });
        fdi.deleted = fdi.winner().deleted;
        let old = originals.get(&id).cloned().flatten();
        match &old {
            Some(o) => w.batch.delete_cf(&db.store.cf(CF_SEQ), be(o.update_seq)),
            None => counts.inserts += 1,
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
    Ok((w.batch, header, results, counts))
}

/// couch_db_updater:increment_local_doc_revs/1: a local document's new
/// revision is the one given plus one (0-1 without one, 0-0 deleted),
/// with no checking against what's there.
fn update_local(slot: &mut Option<LocalDoc>, doc: &Doc) -> UpdateResult {
    if doc.deleted {
        *slot = None;
        return Ok("0-0".into());
    }
    let invalid = || CouchError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "error", "Invalid rev format");
    let rev = match (doc.revs.0, doc.revs.1.first()) {
        (0, None) => 1,
        (0, Some(r)) => {
            // binary_to_integer/1
            let digits = r.strip_prefix(['+', '-']).unwrap_or(r);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || r.starts_with('-') {
                return Err(invalid());
            }
            digits.parse::<u64>().ok().and_then(|n| n.checked_add(1)).ok_or_else(invalid)?
        }
        _ => return Err(invalid()),
    };
    *slot = Some(LocalDoc { rev, body: serde_json::to_vec(&doc.body)? });
    Ok(format!("0-{rev}"))
}

fn missing_stub(doc: &Doc, name: &str) -> CouchError {
    CouchError::precondition("missing_stub", format!("Invalid attachment stub in {} for {}", doc.id, name))
}

/// Resolve stubs and write new attachment data, returning stored infos.
/// New data takes the new revision's position in an interactive edit
/// (set_new_att_revpos/1); a replicated one keeps the revpos it came with,
/// or 0.
fn prepare_atts(w: &Writes, doc: &Doc, prev: Option<&DiskBody>, new_pos: u64, replicated: bool) -> Result<Vec<AttInfo>> {
    let new_revpos = |a: &Att| if replicated { a.revpos.unwrap_or(0) } else { new_pos };
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
            AttData::Stored(chunks) | AttData::Staged(chunks) => AttInfo {
                name: a.name.clone(),
                content_type: a.content_type.clone(),
                revpos: match a.data {
                    AttData::Staged(_) => new_revpos(a),
                    _ => a.revpos.unwrap_or(new_pos),
                },
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
                    revpos: new_revpos(a),
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

/// The members of a JSON object's text, between its braces.
fn json_members(json: &str) -> Option<&str> {
    json.trim().strip_prefix('{')?.strip_suffix('}').map(str::trim)
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
    let atts = prepare_atts(w, doc, prev_body.as_ref(), new_pos, false)?;
    let body_json = serde_json::to_vec(&doc.body)?;
    let mut hash_doc = doc.clone();
    hash_doc.atts = atts.iter().map(Att::from_info).collect();
    let new_id = new_rev_id(&hash_doc, parent.as_ref().map(|p| (p.pos, p.id.as_str())));
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
    let atts = prepare_atts(w, doc, prev_body.as_ref(), *pos, true)?;
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
    // Answered in request order, but applied (and given purge seqs) last
    // to first, as fabric_doc_purge hands a shard its requests.
    let mut purged: Map<String, Value> = req.iter().map(|(id, _)| (id.clone(), Value::Null)).collect();
    let mut header = snap.header.clone();
    let mut seq = header.update_seq;
    let mut batch = WriteBatch::default();
    for (id, mut revs) in req.into_iter().rev() {
        revs.sort();
        revs.dedup();
        header.purge_seq += 1;
        batch.put_cf(&db.store.cf(CF_META), purge_info_key(header.purge_seq), enc(&(&id, &revs)));
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
            // Every remaining leaf gets a new seq, as couch_db_updater
            // relabels them, so the doc is re-indexed.
            fdi.tree = keytree::map_vals(&fdi.tree, &mut |_, _, v, leaf| {
                if !leaf {
                    return v.clone();
                }
                seq += 1;
                match v {
                    RevVal::Avail(l) => RevVal::Avail(LeafVal { seq, ..l.clone() }),
                    RevVal::Missing => RevVal::Missing,
                }
            });
            fdi.update_seq = seq;
            fdi.deleted = fdi.winner().deleted;
            count_doc(&mut header, Some(&fdi), 1);
            put_fdi(db, &mut batch, &fdi);
            batch.put_cf(&db.store.cf(CF_SEQ), be(seq), enc(&fdi));
        }
    }
    header.update_seq = seq;
    let purge_seq = header.purge_seq;
    commit(db, batch, header)?;
    Ok(json!({"purge_seq": purge_seq, "purged": purged}))
}
