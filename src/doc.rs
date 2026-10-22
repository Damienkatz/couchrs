//! Documents, a port of couch_doc: parsing, validation, JSON rendering and
//! revision id generation.

use crate::error::{CouchError, Result};
use crate::file::Pos;
use crate::keytree::{self, RevTree, RevValue};
use base64::Engine;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

pub const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Value stored on each revision tree node.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(from = "RevValWire", into = "RevValWire")]
pub enum RevVal {
    Missing,
    Avail(LeafVal),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LeafVal {
    pub deleted: bool,
    pub ptr: Pos,
    pub seq: u64,
    /// Inflated body plus attachment bytes (sizes.external).
    pub size: u64,
    /// The same bytes as stored on disk, after LZ4 (sizes.active).
    pub stored: u64,
}

/// On-disk form of RevVal. New values are written as `AvailV2`; `Avail`
/// is the original layout without a stored size, still read from older
/// files (where nothing was compressed beyond the body, so stored = size).
#[derive(Serialize, Deserialize, Clone)]
enum RevValWire {
    Missing,
    Avail(LeafValV1),
    AvailV2(LeafVal),
}

#[derive(Serialize, Deserialize, Clone)]
struct LeafValV1 {
    deleted: bool,
    ptr: Pos,
    seq: u64,
    size: u64,
}

impl From<RevValWire> for RevVal {
    fn from(w: RevValWire) -> RevVal {
        match w {
            RevValWire::Missing => RevVal::Missing,
            RevValWire::Avail(l) => RevVal::Avail(LeafVal { deleted: l.deleted, ptr: l.ptr, seq: l.seq, size: l.size, stored: l.size }),
            RevValWire::AvailV2(l) => RevVal::Avail(l),
        }
    }
}

impl From<RevVal> for RevValWire {
    fn from(v: RevVal) -> RevValWire {
        match v {
            RevVal::Missing => RevValWire::Missing,
            RevVal::Avail(l) => RevValWire::AvailV2(l),
        }
    }
}

impl RevValue for RevVal {
    fn is_missing(&self) -> bool {
        matches!(self, RevVal::Missing)
    }
}

impl RevVal {
    pub fn leaf(&self) -> Option<&LeafVal> {
        match self {
            RevVal::Avail(l) => Some(l),
            RevVal::Missing => None,
        }
    }
    pub fn deleted(&self) -> bool {
        self.leaf().map(|l| l.deleted).unwrap_or(false)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FullDocInfo {
    pub id: String,
    pub update_seq: u64,
    pub deleted: bool,
    pub tree: RevTree<RevVal>,
}

#[derive(Clone, Debug)]
pub struct RevInfo {
    pub pos: u64,
    pub id: String,
    pub deleted: bool,
    pub val: RevVal,
    pub path: Vec<String>,
}

impl RevInfo {
    pub fn rev(&self) -> String {
        format!("{}-{}", self.pos, self.id)
    }
}

impl FullDocInfo {
    /// Leaves sorted winner-first: non-deleted, then highest pos, then id.
    pub fn sorted_leafs(&self) -> Vec<RevInfo> {
        let mut ls: Vec<RevInfo> = keytree::leafs(&self.tree)
            .into_iter()
            .map(|l| RevInfo {
                pos: l.pos,
                deleted: l.val.deleted(),
                id: l.id,
                val: l.val,
                path: l.path,
            })
            .collect();
        ls.sort_by(|a, b| {
            (!b.deleted, b.pos, &b.id).cmp(&(!a.deleted, a.pos, &a.id))
        });
        ls
    }

    pub fn winner(&self) -> RevInfo {
        self.sorted_leafs().into_iter().next().expect("doc with no leafs")
    }

    pub fn conflicts(&self) -> Vec<String> {
        self.sorted_leafs()
            .into_iter()
            .skip(1)
            .filter(|l| !l.deleted)
            .map(|l| l.rev())
            .collect()
    }

    pub fn deleted_conflicts(&self) -> Vec<String> {
        self.sorted_leafs()
            .into_iter()
            .skip(1)
            .filter(|l| l.deleted)
            .map(|l| l.rev())
            .collect()
    }

    pub fn leaf_size(&self) -> u64 {
        keytree::leafs(&self.tree)
            .iter()
            .filter_map(|l| l.val.leaf().map(|v| v.size))
            .sum()
    }
}

/// Attachment metadata as stored with a document body.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AttInfo {
    pub name: String,
    pub content_type: String,
    pub revpos: u64,
    pub length: u64,
    pub md5: Vec<u8>,
    pub encoding: Option<String>,
    pub encoded_length: u64,
    pub chunks: Vec<Pos>,
    /// Bytes the chunks occupy on disk (after LZ4).
    pub stored_length: u64,
}

impl AttInfo {
    pub fn digest(&self) -> String {
        format!("md5-{}", B64.encode(&self.md5))
    }
}

/// On-disk body of one revision.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DiskBody {
    pub json: Vec<u8>,
    pub atts: Vec<AttInfo>,
}

/// Body layout written before attachment stored lengths existed.
#[derive(Serialize, Deserialize)]
pub struct DiskBodyV1 {
    pub json: Vec<u8>,
    pub atts: Vec<AttInfoV1>,
}

#[derive(Serialize, Deserialize)]
pub struct AttInfoV1 {
    pub name: String,
    pub content_type: String,
    pub revpos: u64,
    pub length: u64,
    pub md5: Vec<u8>,
    pub encoding: Option<String>,
    pub encoded_length: u64,
    pub chunks: Vec<Pos>,
}

impl From<DiskBodyV1> for DiskBody {
    fn from(b: DiskBodyV1) -> DiskBody {
        DiskBody {
            json: b.json,
            atts: b
                .atts
                .into_iter()
                .map(|a| AttInfo {
                    // Format-1 chunks are raw, so stored equals encoded.
                    stored_length: a.encoded_length,
                    name: a.name,
                    content_type: a.content_type,
                    revpos: a.revpos,
                    length: a.length,
                    md5: a.md5,
                    encoding: a.encoding,
                    encoded_length: a.encoded_length,
                    chunks: a.chunks,
                })
                .collect(),
        }
    }
}

impl FullDocInfo {
    /// Stored (deflated) bytes of this doc's leaf revisions.
    pub fn leaf_stored(&self) -> u64 {
        keytree::leafs(&self.tree).iter().filter_map(|l| l.val.leaf().map(|v| v.stored)).sum()
    }
}

#[derive(Clone, Debug)]
pub enum AttData {
    /// Refers to the attachment of the same name on the previous revision.
    Stub,
    /// Bytes supplied in the request (base64 inline, multipart, or PUT).
    /// Refcounted so retries and clones never copy large payloads.
    Bytes(bytes::Bytes),
    /// Already written to the file.
    Stored(Vec<Pos>),
    /// Will arrive in a later multipart part.
    Follows,
}

#[derive(Clone, Debug)]
pub struct Att {
    pub name: String,
    pub content_type: String,
    pub revpos: Option<u64>,
    pub length: u64,
    pub md5: Vec<u8>,
    pub encoding: Option<String>,
    pub encoded_length: u64,
    /// Stored (deflated) bytes once written; 0 until then.
    pub stored_length: u64,
    pub data: AttData,
}

impl Att {
    pub fn from_info(i: &AttInfo) -> Att {
        Att {
            name: i.name.clone(),
            content_type: i.content_type.clone(),
            revpos: Some(i.revpos),
            length: i.length,
            md5: i.md5.clone(),
            encoding: i.encoding.clone(),
            encoded_length: i.encoded_length,
            stored_length: i.stored_length,
            data: AttData::Stored(i.chunks.clone()),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Doc {
    pub id: String,
    /// Generation of the newest rev and ids newest-first. Empty for new docs.
    pub revs: (u64, Vec<String>),
    pub deleted: bool,
    pub body: Map<String, Value>,
    pub atts: Vec<Att>,
    /// Generation of the file that `Stored` attachment chunks were staged
    /// into before the update reached the writer (0 = none staged).
    pub att_gen: u64,
}

impl Doc {
    pub fn rev(&self) -> Option<String> {
        self.revs.1.first().map(|id| format!("{}-{}", self.revs.0, id))
    }
    pub fn is_local(&self) -> bool {
        self.id.starts_with("_local/")
    }
    pub fn is_design(&self) -> bool {
        self.id.starts_with("_design/")
    }
}

pub fn parse_rev(s: &str) -> Result<(u64, String)> {
    let bad = || CouchError::bad_request("Invalid rev format");
    let (p, id) = s.split_once('-').ok_or_else(bad)?;
    let pos: u64 = p.parse().map_err(|_| bad())?;
    if id.is_empty() {
        return Err(bad());
    }
    Ok((pos, id.to_string()))
}

pub fn validate_docid(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(CouchError::bad_request("Document id must not be empty"));
    }
    if id.starts_with('_') && !id.starts_with("_design/") && !id.starts_with("_local/") {
        return Err(CouchError::bad_request("Only reserved document ids may start with underscore."));
    }
    if id == "_design/" || id == "_local/" {
        return Err(CouchError::bad_request("Document id must not be empty"));
    }
    Ok(())
}

/// Parse a JSON object into a Doc (couch_doc:from_json_obj_validate).
pub fn from_json(v: Value) -> Result<Doc> {
    let obj = match v {
        Value::Object(o) => o,
        _ => return Err(CouchError::bad_request("Document must be a JSON object")),
    };
    let mut doc = Doc::default();
    let mut rev: Option<(u64, String)> = None;
    let mut revisions: Option<(u64, Vec<String>)> = None;
    for (k, v) in obj {
        if !k.starts_with('_') {
            doc.body.insert(k, v);
            continue;
        }
        match k.as_str() {
            "_id" => match v {
                Value::String(s) => doc.id = s,
                _ => return Err(CouchError::bad_request("Document id must be a string")),
            },
            "_rev" => match v {
                Value::String(s) => rev = Some(parse_rev(&s)?),
                Value::Null => {}
                _ => return Err(CouchError::bad_request("Invalid rev format")),
            },
            "_deleted" => doc.deleted = v.as_bool().unwrap_or(false),
            "_revisions" => {
                let start = v.get("start").and_then(|s| s.as_u64());
                let ids = v.get("ids").and_then(|s| s.as_array());
                match (start, ids) {
                    (Some(s), Some(ids)) => {
                        let ids: Option<Vec<String>> =
                            ids.iter().map(|i| i.as_str().map(String::from)).collect();
                        let ids = ids.ok_or_else(|| CouchError::doc_validation("RevId isn't a string"))?;
                        // start 0 is the `0-N` rev scheme of _local docs (replicator checkpoints).
                        if ids.is_empty() || (s != 0 && (s as usize) < ids.len()) {
                            return Err(CouchError::doc_validation("RevId isn't a valid revision list"));
                        }
                        revisions = Some((s, ids));
                    }
                    _ => return Err(CouchError::doc_validation("_revisions must have start and ids")),
                }
            }
            "_attachments" => doc.atts = parse_atts(&v)?,
            // Accepted on input and ignored, like CouchDB.
            "_revs_info" | "_conflicts" | "_deleted_conflicts" | "_local_seq" => {}
            other => {
                return Err(CouchError::doc_validation(format!(
                    "Bad special document member: {other}"
                )));
            }
        }
    }
    match (rev, revisions) {
        (_, Some(r)) => doc.revs = r,
        (Some((p, id)), None) => doc.revs = (p, vec![id]),
        (None, None) => {}
    }
    Ok(doc)
}

fn parse_atts(v: &Value) -> Result<Vec<Att>> {
    let obj = v
        .as_object()
        .ok_or_else(|| CouchError::bad_request("_attachments must be an object"))?;
    let mut atts = Vec::new();
    for (name, a) in obj {
        if name.starts_with('_') {
            return Err(CouchError::bad_request(
                "Attachment name '_' prefix is reserved for internal use",
            ));
        }
        let content_type = a
            .get("content_type")
            .and_then(|c| c.as_str())
            .unwrap_or("application/octet-stream")
            .to_string();
        let md5 = a
            .get("digest")
            .and_then(|d| d.as_str())
            .and_then(|d| d.strip_prefix("md5-"))
            .and_then(|d| B64.decode(d).ok())
            .unwrap_or_default();
        let revpos = a.get("revpos").and_then(|r| r.as_u64());
        let encoding = a.get("encoding").and_then(|e| e.as_str()).map(String::from);
        if a.get("stub").and_then(|s| s.as_bool()) == Some(true) {
            let length = a.get("length").and_then(|l| l.as_u64()).unwrap_or(0);
            atts.push(Att {
                name: name.clone(),
                content_type,
                revpos,
                length,
                md5,
                encoded_length: a.get("encoded_length").and_then(|l| l.as_u64()).unwrap_or(length),
                encoding,
                stored_length: 0,
                data: AttData::Stub,
            });
        } else if a.get("follows").and_then(|s| s.as_bool()) == Some(true) {
            let length = a.get("length").and_then(|l| l.as_u64()).unwrap_or(0);
            atts.push(Att {
                name: name.clone(),
                content_type,
                revpos,
                length,
                md5,
                encoded_length: a.get("encoded_length").and_then(|l| l.as_u64()).unwrap_or(length),
                encoding,
                stored_length: 0,
                data: AttData::Follows,
            });
        } else if let Some(d) = a.get("data") {
            let s = d
                .as_str()
                .ok_or_else(|| CouchError::bad_request("Attachment data must be a string"))?;
            let bytes = B64
                .decode(s.as_bytes())
                .map_err(|_| CouchError::bad_request("Invalid attachment data"))?;
            atts.push(Att {
                name: name.clone(),
                content_type,
                revpos,
                length: bytes.len() as u64,
                md5: Md5::digest(&bytes).to_vec(),
                encoding: None,
                encoded_length: bytes.len() as u64,
                stored_length: 0,
                data: AttData::Bytes(bytes::Bytes::from(bytes)),
            });
        } else {
            return Err(CouchError::bad_request(format!(
                "Invalid attachment stub in {name}"
            )));
        }
    }
    Ok(atts)
}

/// Deterministic rev id: md5 over the edit's content and its parent.
pub fn new_rev_id(doc: &Doc, parent: Option<(u64, &str)>, body_json: &[u8]) -> String {
    let mut h = Md5::new();
    h.update([doc.deleted as u8]);
    if let Some((p, id)) = parent {
        h.update(p.to_be_bytes());
        h.update(id.as_bytes());
    } else {
        h.update(0u64.to_be_bytes());
    }
    h.update(body_json);
    for a in &doc.atts {
        h.update(a.name.as_bytes());
        h.update([0]);
        h.update(&a.md5);
    }
    hex::encode(h.finalize())
}

/// Options controlling how a document is rendered.
#[derive(Default, Clone, Debug)]
pub struct DocOpts {
    pub revs: bool,
    pub revs_info: bool,
    pub conflicts: bool,
    pub deleted_conflicts: bool,
    pub local_seq: bool,
    pub attachments: bool,
    pub att_encoding_info: bool,
    pub atts_since: Option<Vec<(u64, String)>>,
    pub meta: bool,
    /// Mark included attachments `"follows": true` instead of inlining
    /// base64 data; the bytes are sent as multipart parts.
    pub follows: bool,
}

pub fn att_stub_json(a: &AttInfo, opts: &DocOpts, data: Option<Vec<u8>>) -> Value {
    let mut m = Map::new();
    m.insert("content_type".into(), json!(a.content_type));
    m.insert("revpos".into(), json!(a.revpos));
    m.insert("digest".into(), json!(a.digest()));
    m.insert("length".into(), json!(a.length));
    if opts.att_encoding_info
        && let Some(e) = &a.encoding {
            m.insert("encoding".into(), json!(e));
            m.insert("encoded_length".into(), json!(a.encoded_length));
        }
    match data {
        Some(d) => {
            m.insert("data".into(), json!(B64.encode(d)));
        }
        None => {
            m.insert("stub".into(), json!(true));
        }
    }
    Value::Object(m)
}

pub fn is_valid_db_name(name: &str) -> bool {
    const SYSTEM: &[&str] = &["_users", "_replicator", "_global_changes"];
    if SYSTEM.contains(&name) {
        return true;
    }
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    name.len() <= 238
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "_$()+-/".contains(c))
}
