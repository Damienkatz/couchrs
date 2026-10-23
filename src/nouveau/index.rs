//! One search index on disk: what the Nouveau server keeps per index
//! (a Lucene index), on tantivy.
//!
//! Lucene takes any field name at any time; a tantivy schema is fixed when
//! the index is created. So the schema has numbered slots per kind (text,
//! string, double), and the names documents use are given slots as they
//! first appear, the mapping kept with each commit. Text fields with
//! their own analyzer (field_analyzers) have their own fields. When a
//! kind's slots run out the index is rebuilt with twice as many.
//!
//! Scores follow Lucene's BM25 (tantivy's, without its (k1 + 1) factor).

use super::analysis::{Analyzer, NouveauTokenizer};
use super::query::{self, Kind};
use crate::error::{CouchError, Result};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::{Column, StrColumn};
use tantivy::query::{BooleanQuery, BoostQuery, Occur, Query, TermQuery};
use tantivy::schema::{
    BytesOptions, Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions, Value as _,
};
use tantivy::tokenizer::TextAnalyzer;
use tantivy::{DocAddress, DocId, Index, IndexReader, IndexWriter, ReloadPolicy, Score, SegmentOrdinal, SegmentReader, TantivyDocument, Term};

/// Lucene's BM25 leaves out tantivy's (k1 + 1) factor.
pub const LUCENE_SCORE: f32 = 1.0 / 2.2;
pub const INITIAL_SLOTS: usize = 8;
const DEFINITION_FILE: &str = "index_definition.json";

/// An index's files, kept as MmapDirectory keeps them but synced, on
/// Apple platforms, without flushing the drive's cache (F_FULLFSYNC, what
/// std's sync_data does there, several milliseconds each and a dozen per
/// commit). There a plain fsync hands a file's data to the drive, and the
/// commit point, meta.json, is written behind a write barrier
/// (F_BARRIERFSYNC): no commit reaches the disk before the files it
/// names, so after a power loss an index is at an earlier commit and
/// catches up from its update seq, as it's only derived data (and the
/// database's own files aren't synced harder). Elsewhere files are synced
/// as MmapDirectory syncs them.
#[derive(Clone, Debug)]
struct IndexDirectory {
    inner: tantivy::directory::MmapDirectory,
    root: PathBuf,
}

/// Sync a file's data, behind a barrier when later writes depend on it.
fn sync_file(f: &std::fs::File, barrier: bool) -> std::io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: fcntl and fsync on a file descriptor the File owns.
        let r = unsafe { if barrier { libc::fcntl(f.as_raw_fd(), libc::F_BARRIERFSYNC) } else { libc::fsync(f.as_raw_fd()) } };
        if r == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        let _ = barrier;
        f.sync_data()
    }
}

impl IndexDirectory {
    fn open(root: &Path) -> Result<IndexDirectory> {
        let inner = tantivy::directory::MmapDirectory::open(root).map_err(|e| CouchError::internal(e.to_string()))?;
        Ok(IndexDirectory { inner, root: root.to_path_buf() })
    }
}

/// A file being written, synced when tantivy is done with it.
struct SyncedFile(std::io::BufWriter<std::fs::File>);

impl std::io::Write for SyncedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    // As MmapDirectory's writer: only terminating flushes (and syncs).
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl tantivy::directory::TerminatingWrite for SyncedFile {
    fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> std::io::Result<()> {
        use std::io::Write;
        self.0.flush()?;
        sync_file(self.0.get_ref(), false)
    }
}

impl tantivy::Directory for IndexDirectory {
    fn get_file_handle(&self, path: &Path) -> std::result::Result<std::sync::Arc<dyn tantivy::directory::FileHandle>, tantivy::directory::error::OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> std::result::Result<(), tantivy::directory::error::DeleteError> {
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, tantivy::directory::error::OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> std::result::Result<tantivy::directory::WritePtr, tantivy::directory::error::OpenWriteError> {
        use tantivy::directory::error::OpenWriteError;
        let file = std::fs::OpenOptions::new().write(true).create_new(true).open(self.root.join(path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                OpenWriteError::FileAlreadyExists(path.to_path_buf())
            } else {
                OpenWriteError::wrap_io_error(e, path.to_path_buf())
            }
        })?;
        Ok(std::io::BufWriter::new(Box::new(SyncedFile(std::io::BufWriter::new(file)))))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, tantivy::directory::error::OpenReadError> {
        self.inner.atomic_read(path)
    }

    /// As MmapDirectory writes atomically: a synced temporary file in the
    /// same directory, renamed over the file; meta.json, the commit point,
    /// behind a barrier.
    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        let tmp = self.root.join(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
        let res = (|| {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(data)?;
            sync_file(&f, path == Path::new("meta.json"))?;
            std::fs::rename(&tmp, self.root.join(path))
        })();
        if res.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        res
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        sync_file(&std::fs::File::open(&self.root)?, true)
    }

    fn acquire_lock(&self, lock: &tantivy::directory::Lock) -> std::result::Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
        self.inner.acquire_lock(lock)
    }

    fn watch(&self, callback: tantivy::directory::WatchCallback) -> tantivy::Result<tantivy::directory::WatchHandle> {
        self.inner.watch(callback)
    }
}

/// The index definition (Nouveau's IndexDefinition), with our slot count.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Definition {
    pub lucene_version: i64,
    pub default_analyzer: String,
    pub field_analyzers: BTreeMap<String, String>,
    pub slots: usize,
}

/// Nouveau's field types (LuceneIndexSchema.Type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FType {
    #[serde(rename = "STRING")]
    String,
    #[serde(rename = "TEXT")]
    Text,
    #[serde(rename = "DOUBLE")]
    Double,
    #[serde(rename = "STORED")]
    Stored,
}

impl FType {
    fn name(self) -> &'static str {
        match self {
            FType::String => "STRING",
            FType::Text => "TEXT",
            FType::Double => "DOUBLE",
            FType::Stored => "STORED",
        }
    }
}

/// A value from an index function, as index() recorded it.
#[derive(Debug, Clone)]
pub struct IndexField {
    pub ty: FType,
    pub name: String,
    pub value: Value,
    pub store: bool,
}

#[derive(Debug, Clone)]
pub enum Update {
    Upsert { id: String, partition: Option<String>, fields: Vec<IndexField> },
    Delete { id: String },
}

/// Why updates were refused.
#[derive(Debug)]
pub enum UpdateError {
    /// The schema needs more slots of a kind than it has.
    SlotsFull,
    Bad(String),
    Other(CouchError),
}

impl From<tantivy::TantivyError> for UpdateError {
    fn from(e: tantivy::TantivyError) -> Self {
        UpdateError::Other(tantivy_err(e))
    }
}

pub fn tantivy_err(e: tantivy::TantivyError) -> CouchError {
    CouchError::internal(e.to_string())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Payload {
    update_seq: u64,
    purge_seq: u64,
    /// Field name: its type and slot.
    schema: BTreeMap<String, (FType, usize)>,
}

struct Fields {
    id: Field,
    partition: Field,
    stored: Field,
    text: Vec<Field>,
    string: Vec<Field>,
    double: Vec<Field>,
    /// The fields with their own analyzer.
    analyzed: HashMap<String, Field>,
}

pub struct SearchIndex {
    pub dir: PathBuf,
    pub def: Definition,
    index: Index,
    reader: IndexReader,
    f: Fields,
    default_analyzer: Analyzer,
    field_analyzers: HashMap<String, Analyzer>,
    state: RwLock<Payload>,
    writer: Mutex<Option<IndexWriter>>,
}

fn analyzer_named(name: &str) -> Result<Analyzer> {
    Analyzer::from_name(name).ok_or_else(|| CouchError::bad_request(format!("{name} is not a valid analyzer name")))
}

fn schema_for(def: &Definition) -> Result<Schema> {
    let mut b = Schema::builder();
    let raw = TextFieldIndexing::default().set_tokenizer("raw").set_index_option(IndexRecordOption::Basic).set_fieldnorms(false);
    b.add_text_field("_id", TextOptions::default().set_indexing_options(raw.clone()).set_stored().set_fast("raw"));
    b.add_text_field("_partition", TextOptions::default().set_indexing_options(raw.clone()));
    b.add_bytes_field("_stored", BytesOptions::default().set_stored());
    let text = |analyzer: &str| -> Result<TextOptions> {
        let tokenizer = analyzer_named(analyzer)?.tokenizer_name();
        Ok(TextOptions::default().set_indexing_options(
            TextFieldIndexing::default().set_tokenizer(&tokenizer).set_index_option(IndexRecordOption::WithFreqsAndPositions),
        ))
    };
    for i in 0..def.slots {
        b.add_text_field(&format!("t{i}"), text(&def.default_analyzer)?);
        b.add_text_field(&format!("s{i}"), TextOptions::default().set_indexing_options(raw.clone()).set_fast("raw"));
        b.add_f64_field(&format!("d{i}"), NumericOptions::default().set_indexed().set_fast());
    }
    for (i, (_, analyzer)) in def.field_analyzers.iter().enumerate() {
        b.add_text_field(&format!("a{i}"), text(analyzer)?);
    }
    Ok(b.build())
}

fn register_tokenizers(index: &Index, def: &Definition) -> Result<()> {
    for name in std::iter::once(&def.default_analyzer).chain(def.field_analyzers.values()) {
        let a = analyzer_named(name)?;
        index.tokenizers().register(&a.tokenizer_name(), TextAnalyzer::builder(NouveauTokenizer(a)).build());
    }
    Ok(())
}

impl SearchIndex {
    pub fn exists(dir: &Path) -> bool {
        dir.join(DEFINITION_FILE).exists()
    }

    pub fn load_definition(dir: &Path) -> Option<Definition> {
        serde_json::from_slice(&std::fs::read(dir.join(DEFINITION_FILE)).ok()?).ok()
    }

    /// A new, empty index (replacing any there).
    pub fn create(dir: &Path, def: Definition, initial_purge_seq: u64) -> Result<SearchIndex> {
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        std::fs::create_dir_all(dir)?;
        let schema = schema_for(&def)?;
        let index = Index::create(IndexDirectory::open(dir)?, schema, tantivy::IndexSettings::default()).map_err(tantivy_err)?;
        register_tokenizers(&index, &def)?;
        let tmp = dir.join(format!("{DEFINITION_FILE}.tmp"));
        std::fs::write(&tmp, serde_json::to_vec(&def)?)?;
        std::fs::rename(&tmp, dir.join(DEFINITION_FILE))?;
        let idx = Self::from_index(dir, def, index)?;
        idx.state.write().purge_seq = initial_purge_seq;
        Ok(idx)
    }

    pub fn open(dir: &Path) -> Result<Option<SearchIndex>> {
        let Some(def) = Self::load_definition(dir) else { return Ok(None) };
        let index = match IndexDirectory::open(dir).and_then(|d| Index::open(d).map_err(tantivy_err)) {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!("search index {} unreadable, rebuilding: {}", dir.display(), e.reason);
                return Ok(None);
            }
        };
        register_tokenizers(&index, &def)?;
        Self::from_index(dir, def, index).map(Some)
    }

    fn from_index(dir: &Path, def: Definition, index: Index) -> Result<SearchIndex> {
        let schema = index.schema();
        let get = |n: &str| schema.get_field(n).map_err(|e| CouchError::internal(e.to_string()));
        let f = Fields {
            id: get("_id")?,
            partition: get("_partition")?,
            stored: get("_stored")?,
            text: (0..def.slots).map(|i| get(&format!("t{i}"))).collect::<Result<_>>()?,
            string: (0..def.slots).map(|i| get(&format!("s{i}"))).collect::<Result<_>>()?,
            double: (0..def.slots).map(|i| get(&format!("d{i}"))).collect::<Result<_>>()?,
            analyzed: def.field_analyzers.keys().enumerate().map(|(i, k)| Ok((k.clone(), get(&format!("a{i}"))?))).collect::<Result<_>>()?,
        };
        let payload: Payload = index
            .load_metas()
            .map_err(tantivy_err)?
            .payload
            .and_then(|p| serde_json::from_str(&p).ok())
            .unwrap_or_default();
        let reader = index.reader_builder().reload_policy(ReloadPolicy::Manual).try_into().map_err(tantivy_err)?;
        Ok(SearchIndex {
            dir: dir.to_path_buf(),
            default_analyzer: analyzer_named(&def.default_analyzer)?,
            field_analyzers: def.field_analyzers.iter().map(|(k, v)| Ok((k.clone(), analyzer_named(v)?))).collect::<Result<_>>()?,
            def,
            index,
            reader,
            f,
            state: RwLock::new(payload),
            writer: Mutex::new(None),
        })
    }

    pub fn seqs(&self) -> (u64, u64) {
        let s = self.state.read();
        (s.update_seq, s.purge_seq)
    }

    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    pub fn disk_size(&self) -> u64 {
        std::fs::read_dir(&self.dir)
            .map(|d| d.filter_map(|e| e.ok()?.metadata().ok()).filter(|m| m.is_file()).map(|m| m.len()).sum())
            .unwrap_or(0)
    }

    fn analyzer_for(&self, name: &str) -> Analyzer {
        self.field_analyzers.get(name).copied().unwrap_or(self.default_analyzer)
    }

    /// Apply documents' new fields (or removal), Lucene's updateDocument
    /// and deleteDocuments, checking each field's type against the
    /// schema as LuceneIndexSchema does. Nothing is visible until commit.
    pub fn apply(&self, updates: &[Update]) -> std::result::Result<(), UpdateError> {
        let mut guard = self.writer.lock();
        if guard.is_none() {
            *guard = Some(self.index.writer_with_num_threads(1, 64 << 20)?);
        }
        let writer = guard.as_mut().unwrap();
        let mut state = self.state.write();
        for u in updates {
            match u {
                Update::Delete { id } => {
                    writer.delete_term(Term::from_field_text(self.f.id, id));
                }
                Update::Upsert { id, partition, fields } => {
                    let doc = self.document(&mut state, id, partition.as_deref(), fields)?;
                    writer.delete_term(Term::from_field_text(self.f.id, id));
                    writer.add_document(doc)?;
                }
            }
        }
        Ok(())
    }

    fn document(&self, state: &mut Payload, id: &str, partition: Option<&str>, fields: &[IndexField]) -> std::result::Result<TantivyDocument, UpdateError> {
        let mut doc = TantivyDocument::new();
        doc.add_text(self.f.id, id);
        if let Some(p) = partition {
            doc.add_text(self.f.partition, p);
        }
        let mut stored: Vec<(String, Value)> = Vec::new();
        for fld in fields {
            // (The Nouveau server's constraints on names and values don't
            // reach fields: Mango's names have colons and dollars.)
            // Underscore-prefixed names are reserved.
            if fld.name.starts_with('_') {
                continue;
            }
            let slot = match state.schema.get(&fld.name) {
                Some((ty, slot)) if *ty == fld.ty => *slot,
                Some((ty, _)) => {
                    return Err(UpdateError::Bad(format!("field {} is of type {} not {}", fld.name, fld.ty.name(), ty.name())));
                }
                None => {
                    let used = state.schema.values().filter(|(t, _)| *t == fld.ty).count();
                    let in_own_field = fld.ty == FType::Text && self.f.analyzed.contains_key(&fld.name);
                    if fld.ty != FType::Stored && !in_own_field && used >= self.def.slots {
                        return Err(UpdateError::SlotsFull);
                    }
                    state.schema.insert(fld.name.clone(), (fld.ty, used));
                    used
                }
            };
            match fld.ty {
                FType::Text => {
                    let s = fld.value.as_str().unwrap_or_default();
                    let field = self.f.analyzed.get(&fld.name).copied().unwrap_or(self.f.text[slot]);
                    doc.add_text(field, s);
                }
                FType::String => doc.add_text(self.f.string[slot], fld.value.as_str().unwrap_or_default()),
                FType::Double => {
                    let v = fld.value.as_f64().ok_or_else(|| UpdateError::Bad(format!("field {} value must be a number", fld.name)))?;
                    doc.add_f64(self.f.double[slot], v);
                }
                FType::Stored => {}
            }
            if fld.store || fld.ty == FType::Stored {
                let v = match (&fld.value, fld.ty) {
                    (Value::Number(n), _) => json!(n.as_f64().unwrap_or(0.0)),
                    (Value::String(s), FType::Double) => json!(s.parse::<f64>().unwrap_or(0.0)),
                    (v, _) => v.clone(),
                };
                stored.push((fld.name.clone(), v));
            }
        }
        doc.add_bytes(self.f.stored, &serde_json::to_vec(&stored).unwrap_or_default());
        Ok(doc)
    }

    pub fn set_seqs(&self, update_seq: u64, purge_seq: u64) {
        let mut s = self.state.write();
        s.update_seq = update_seq;
        s.purge_seq = purge_seq;
    }

    /// Make the applied updates durable and visible, with the seqs and schema.
    pub fn commit(&self) -> Result<()> {
        let mut guard = self.writer.lock();
        if guard.is_none() {
            *guard = Some(self.index.writer_with_num_threads(1, 64 << 20).map_err(tantivy_err)?);
        }
        let payload = serde_json::to_string(&*self.state.read())?;
        let writer = guard.as_mut().unwrap();
        let mut prepared = writer.prepare_commit().map_err(tantivy_err)?;
        prepared.set_payload(&payload);
        prepared.commit().map_err(tantivy_err)?;
        // Let merges finish before the writer (and its threads) go.
        if let Some(w) = guard.take() {
            w.wait_merging_threads().map_err(tantivy_err)?;
        }
        self.reader.reload().map_err(tantivy_err)?;
        Ok(())
    }

    /// Throw away updates applied since the last commit.
    pub fn rollback(&self) {
        if let Some(mut w) = self.writer.lock().take() {
            let _ = w.rollback();
        }
        if let Ok(Some(p)) = self.index.load_metas().map(|m| m.payload)
            && let Ok(p) = serde_json::from_str(&p) {
                *self.state.write() = p;
            }
    }

    // ------------------------------------------------------------ search

    fn kind_of(&self, schema: &BTreeMap<String, (FType, usize)>, name: &str) -> Option<(Kind, Field)> {
        match name {
            "_id" => return Some((Kind::Keyword, self.f.id)),
            "_partition" => return Some((Kind::Keyword, self.f.partition)),
            _ => {}
        }
        let (ty, slot) = *schema.get(name)?;
        match ty {
            FType::Text => Some((Kind::Text, self.f.analyzed.get(name).copied().unwrap_or(self.f.text[slot]))),
            FType::String => Some((Kind::Keyword, self.f.string[slot])),
            FType::Double => Some((Kind::Double, self.f.double[slot])),
            FType::Stored => None,
        }
    }

    fn type_of(&self, schema: &BTreeMap<String, (FType, usize)>, name: &str) -> Option<FType> {
        if name == "_id" { Some(FType::String) } else { schema.get(name).map(|(t, _)| *t) }
    }

    fn fast_name(&self, schema: &BTreeMap<String, (FType, usize)>, name: &str) -> Option<String> {
        match name {
            "_id" => Some("_id".into()),
            _ => match schema.get(name)? {
                (FType::String, s) => Some(format!("s{s}")),
                (FType::Double, s) => Some(format!("d{s}")),
                _ => None,
            },
        }
    }

    pub fn search(&self, req: &SearchRequest) -> Result<SearchResults> {
        let schema = self.state.read().schema.clone();
        let parsed = query::parse(&req.query, "default")?;
        let ctx = query::Ctx {
            field: &|n| self.kind_of(&schema, n),
            analyzer: &|n| self.analyzer_for(n),
            locale: req.locale.as_deref(),
            query: &req.query,
        };
        let mut q = ctx.build(&parsed)?;
        if let Some(p) = &req.partition {
            let pq: Box<dyn Query> = Box::new(BoostQuery::new(
                Box::new(TermQuery::new(Term::from_field_text(self.f.partition, p), IndexRecordOption::Basic)),
                LUCENE_SCORE,
            ));
            q = Box::new(BooleanQuery::new(vec![(Occur::Must, pq), (Occur::Must, q)]));
        }

        // Sort: the request's, _id last (to page by), or relevance then _id.
        let mut sort: Vec<SortKey> = Vec::new();
        match &req.sort {
            None => {
                sort.push(SortKey::Score { asc: false });
                sort.push(SortKey::Str { col: "_id".into(), desc: false });
            }
            Some(list) => {
                let mut list = list.clone();
                if !matches!(list.last().map(String::as_str), Some("_id" | "-_id")) {
                    list.push("_id".into());
                }
                for s in &list {
                    sort.push(self.sort_key(&schema, s)?);
                }
            }
        }
        let after = match &req.after {
            None => None,
            Some(a) => {
                if a.len() != sort.len() {
                    return Err(CouchError::bad_request(format!("after.fields has {} values but sort has {}", a.len(), sort.len())));
                }
                Some(a.iter().zip(&sort).map(|(v, k)| after_value(v, k)).collect::<Result<Vec<_>>>()?)
            }
        };

        let mut counts = Vec::new();
        for name in req.counts.iter().flatten() {
            let col = match self.type_of(&schema, name) {
                None => None,
                Some(FType::String) => self.fast_name(&schema, name),
                Some(t) => return Err(CouchError::bad_request(docvalues_error(t, name, "[SORTED, SORTED_SET]"))),
            };
            counts.push((name.clone(), col));
        }
        let mut ranges = Vec::new();
        for (name, rs) in req.ranges.iter().flatten() {
            let col = match self.type_of(&schema, name) {
                None => None,
                Some(FType::Double) => self.fast_name(&schema, name),
                Some(t) => {
                    let t = if name == "_id" { "SORTED" } else { docvalues_type(t) };
                    return Err(CouchError::bad_request(format!(
                        "unexpected docvalues type {t} for field '{name}' (expected one of [SORTED_NUMERIC, NUMERIC]). Re-index with correct docvalues type."
                    )));
                }
            };
            ranges.push((name.clone(), col, rs.clone()));
        }

        let scoring = sort.iter().any(|k| matches!(k, SortKey::Score { .. }));
        let collector = HitCollector { sort: sort.clone(), after, limit: req.limit, scoring, counts: counts.clone(), ranges: ranges.clone() };
        let searcher = self.reader.searcher();
        let fruit = searcher.search(&*q, &collector).map_err(|e| CouchError::bad_request(e.to_string()))?;

        let mut hits = Vec::with_capacity(fruit.hits.len());
        for (key, addr) in fruit.hits {
            let doc: TantivyDocument = searcher.doc(addr).map_err(tantivy_err)?;
            let id = doc.get_first(self.f.id).and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let stored: Vec<(String, Value)> =
                doc.get_first(self.f.stored).and_then(|v| v.as_bytes()).and_then(|b| serde_json::from_slice(b).ok()).unwrap_or_default();
            let order = key.iter().map(Resolved::to_json).collect();
            hits.push(Hit { id, order, fields: stored });
        }
        let top = |m: HashMap<String, u64>| -> Map<String, Value> {
            let mut v: Vec<(String, u64)> = m.into_iter().filter(|(_, c)| *c > 0).collect();
            v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            v.truncate(req.top_n);
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v.into_iter().map(|(k, c)| (k, json!(c))).collect()
        };
        let counts = req.counts.as_ref().map(|_| {
            let mut out: BTreeMap<String, Value> = BTreeMap::new();
            for (name, m) in fruit.counts {
                out.insert(name, Value::Object(top(m)));
            }
            out.into_iter().collect::<Map<_, _>>()
        });
        let ranges = req.ranges.as_ref().map(|_| {
            let mut out: BTreeMap<String, Value> = BTreeMap::new();
            for (name, m) in fruit.ranges {
                out.insert(name, Value::Object(top(m)));
            }
            out.into_iter().collect::<Map<_, _>>()
        });
        Ok(SearchResults { total_hits: fruit.total, hits, counts, ranges })
    }

    fn sort_key(&self, schema: &BTreeMap<String, (FType, usize)>, s: &str) -> Result<SortKey> {
        match s {
            "-<score>" => return Ok(SortKey::Score { asc: false }),
            "<score>" => return Ok(SortKey::Score { asc: true }),
            _ => {}
        }
        let (desc, name) = match s.strip_prefix('-') {
            Some(n) => (true, n),
            None => (false, s.strip_prefix('+').unwrap_or(s)),
        };
        if name.is_empty() || !name.chars().all(|c| c == '.' || c == '_' || c.is_ascii_alphanumeric()) {
            return Err(CouchError::bad_request(format!("{s} is not a valid sort parameter")));
        }
        match self.type_of(schema, name) {
            None => Ok(SortKey::Unknown),
            Some(FType::String) => Ok(SortKey::Str { col: self.fast_name(schema, name).unwrap_or_default(), desc }),
            Some(FType::Double) => Ok(SortKey::Dbl { col: self.fast_name(schema, name).unwrap_or_default(), desc }),
            Some(_) => Err(CouchError::bad_request(format!("can't sort on field {name}"))),
        }
    }
}

fn docvalues_type(t: FType) -> &'static str {
    match t {
        FType::String => "SORTED_SET",
        FType::Double => "SORTED_NUMERIC",
        FType::Text | FType::Stored => "NONE",
    }
}

fn docvalues_error(t: FType, name: &str, expected: &str) -> String {
    format!("unexpected docvalues type {} for field '{name}' (expected one of {expected}). Re-index with correct docvalues type.", docvalues_type(t))
}

/// A search as the Nouveau server takes it (SearchRequest).
#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub query: String,
    pub locale: Option<String>,
    pub partition: Option<String>,
    pub limit: usize,
    pub sort: Option<Vec<String>>,
    pub counts: Option<Vec<String>>,
    pub ranges: Option<Vec<(String, Vec<RangeSpec>)>>,
    pub after: Option<Vec<Value>>,
    pub top_n: usize,
}

#[derive(Debug, Clone)]
pub struct RangeSpec {
    pub label: String,
    pub min: f64,
    pub min_inclusive: bool,
    pub max: f64,
    pub max_inclusive: bool,
}

impl RangeSpec {
    fn contains(&self, v: f64) -> bool {
        let lo = if self.min_inclusive { v >= self.min } else { v > self.min };
        let hi = if self.max_inclusive { v <= self.max } else { v < self.max };
        lo && hi
    }
}

pub struct Hit {
    pub id: String,
    pub order: Vec<Value>,
    pub fields: Vec<(String, Value)>,
}

pub struct SearchResults {
    pub total_hits: u64,
    pub hits: Vec<Hit>,
    pub counts: Option<Map<String, Value>>,
    pub ranges: Option<Map<String, Value>>,
}

// --------------------------------------------------------------- sorting

#[derive(Debug, Clone)]
enum SortKey {
    /// Relevance, highest first (or lowest, for <score>).
    Score { asc: bool },
    Str { col: String, desc: bool },
    Dbl { col: String, desc: bool },
    /// A field this index has never seen: every hit ties.
    Unknown,
}

/// A hit's sort value within its segment (strings by ordinal).
#[derive(Debug, Clone, Copy)]
enum Local {
    Score(Score),
    Ord(Option<u64>),
    Num(f64),
    Null,
}

/// A sort value comparable across segments.
#[derive(Debug, Clone)]
enum Resolved {
    Score(Score),
    Str(Option<String>),
    Num(f64),
    Null,
}

impl Resolved {
    fn to_json(&self) -> Value {
        match self {
            // Through f32's shortest form, as Jackson writes a float.
            Resolved::Score(s) => json!({"@type": "float", "value": s.to_string().parse::<f64>().unwrap_or(0.0)}),
            Resolved::Str(Some(s)) => json!({"@type": "string", "value": s}),
            Resolved::Str(None) | Resolved::Null => json!({"@type": "null", "value": null}),
            Resolved::Num(n) => json!({"@type": "double", "value": n}),
        }
    }
}

/// A bookmark's value for one sort key.
#[derive(Debug, Clone)]
enum After {
    Score(Score),
    Str(Option<String>),
    Num(f64),
    Null,
}

fn after_value(v: &Value, k: &SortKey) -> Result<After> {
    let ty = v.get("@type").and_then(Value::as_str).unwrap_or("");
    let val = v.get("value");
    let bad = || CouchError::bad_request(format!("{v} is not valid for sorting on {k:?}"));
    Ok(match (k, ty) {
        (SortKey::Unknown, _) => After::Null,
        (_, "null") => match k {
            SortKey::Str { .. } => After::Str(None),
            _ => After::Null,
        },
        (SortKey::Score { .. }, "float" | "double" | "int" | "long") => After::Score(val.and_then(Value::as_f64).ok_or_else(bad)? as f32),
        (SortKey::Dbl { .. }, "double" | "float" | "int" | "long") => After::Num(val.and_then(Value::as_f64).ok_or_else(bad)?),
        (SortKey::Str { .. }, "string") => After::Str(Some(val.and_then(Value::as_str).ok_or_else(bad)?.to_string())),
        (SortKey::Str { .. }, "bytes") => {
            use base64::Engine;
            let b = base64::engine::general_purpose::STANDARD.decode(val.and_then(Value::as_str).unwrap_or("")).map_err(|_| bad())?;
            After::Str(Some(String::from_utf8_lossy(&b).into_owned()))
        }
        _ => return Err(bad()),
    })
}

fn cmp_f(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn cmp_resolved(sort: &[SortKey], a: &[Resolved], b: &[Resolved]) -> Ordering {
    for (k, (x, y)) in sort.iter().zip(a.iter().zip(b)) {
        let o = match (k, x, y) {
            (SortKey::Score { asc }, Resolved::Score(x), Resolved::Score(y)) => {
                let o = cmp_f(*x as f64, *y as f64);
                if *asc { o } else { o.reverse() }
            }
            (SortKey::Str { desc, .. }, Resolved::Str(x), Resolved::Str(y)) => {
                let o = x.cmp(y);
                if *desc { o.reverse() } else { o }
            }
            (SortKey::Dbl { desc, .. }, Resolved::Num(x), Resolved::Num(y)) => {
                let o = cmp_f(*x, *y);
                if *desc { o.reverse() } else { o }
            }
            _ => Ordering::Equal,
        };
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

/// Per segment, the bookmark's string values as positions in the
/// segment's dictionary: (terms before it, whether it is one).
#[derive(Debug, Clone, Copy)]
enum LocalAfter {
    Score(Score),
    Str(Option<(u64, bool)>),
    Num(f64),
    Null,
}

fn cmp_local(sort: &[SortKey], a: &[Local], b: &[Local]) -> Ordering {
    for (k, (x, y)) in sort.iter().zip(a.iter().zip(b)) {
        let o = match (k, x, y) {
            (SortKey::Score { asc }, Local::Score(x), Local::Score(y)) => {
                let o = cmp_f(*x as f64, *y as f64);
                if *asc { o } else { o.reverse() }
            }
            (SortKey::Str { desc, .. }, Local::Ord(x), Local::Ord(y)) => {
                let o = x.cmp(y);
                if *desc { o.reverse() } else { o }
            }
            (SortKey::Dbl { desc, .. }, Local::Num(x), Local::Num(y)) => {
                let o = cmp_f(*x, *y);
                if *desc { o.reverse() } else { o }
            }
            _ => Ordering::Equal,
        };
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

/// Whether a hit sorts after the bookmark.
fn after_bookmark(sort: &[SortKey], a: &[Local], after: &[LocalAfter]) -> bool {
    for (k, (x, y)) in sort.iter().zip(a.iter().zip(after)) {
        let o = match (k, x, y) {
            (SortKey::Score { asc }, Local::Score(x), LocalAfter::Score(y)) => {
                let o = cmp_f(*x as f64, *y as f64);
                if *asc { o } else { o.reverse() }
            }
            (SortKey::Str { desc, .. }, Local::Ord(x), LocalAfter::Str(y)) => {
                let o = match (x, y) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Less,
                    (Some(_), None) => Ordering::Greater,
                    (Some(o), Some((before, exact))) => match o.cmp(before) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Equal if *exact => Ordering::Equal,
                        _ => Ordering::Greater,
                    },
                };
                if *desc { o.reverse() } else { o }
            }
            (SortKey::Dbl { desc, .. }, Local::Num(x), LocalAfter::Num(y)) => {
                let o = cmp_f(*x, *y);
                if *desc { o.reverse() } else { o }
            }
            _ => Ordering::Equal,
        };
        if o != Ordering::Equal {
            return o == Ordering::Greater;
        }
    }
    false
}

// ------------------------------------------------------------- collector

struct HitCollector {
    sort: Vec<SortKey>,
    after: Option<Vec<After>>,
    limit: usize,
    scoring: bool,
    counts: Vec<(String, Option<String>)>,
    ranges: Vec<(String, Option<String>, Vec<RangeSpec>)>,
}

enum SortCol {
    Score,
    Str(Option<StrColumn>),
    Num(Option<Column<f64>>),
    Null,
}

struct SegHits {
    seg: SegmentOrdinal,
    sort: Vec<SortKey>,
    cols: Vec<SortCol>,
    after: Option<Vec<LocalAfter>>,
    limit: usize,
    total: u64,
    top: Vec<(Vec<Local>, DocId)>,
    counts: Vec<(String, Option<StrColumn>, HashMap<u64, u64>)>,
    ranges: Vec<(String, Option<Column<f64>>, Vec<RangeSpec>, Vec<u64>)>,
}

struct SegFruit {
    total: u64,
    top: Vec<(Vec<Resolved>, DocAddress)>,
    counts: Vec<(String, HashMap<String, u64>)>,
    ranges: Vec<(String, HashMap<String, u64>)>,
}

pub struct Fruit {
    total: u64,
    hits: Vec<(Vec<Resolved>, DocAddress)>,
    counts: Vec<(String, HashMap<String, u64>)>,
    ranges: Vec<(String, HashMap<String, u64>)>,
}

fn str_col(seg: &SegmentReader, name: &str) -> tantivy::Result<Option<StrColumn>> {
    seg.fast_fields().str(name)
}

fn num_col(seg: &SegmentReader, name: &str) -> tantivy::Result<Option<Column<f64>>> {
    seg.fast_fields().column_opt::<f64>(name)
}

impl Collector for HitCollector {
    type Fruit = Fruit;
    type Child = SegHits;

    fn for_segment(&self, seg: SegmentOrdinal, reader: &SegmentReader) -> tantivy::Result<SegHits> {
        let mut cols = Vec::new();
        for k in &self.sort {
            cols.push(match k {
                SortKey::Score { .. } => SortCol::Score,
                SortKey::Str { col, .. } => SortCol::Str(str_col(reader, col)?),
                SortKey::Dbl { col, .. } => SortCol::Num(num_col(reader, col)?),
                SortKey::Unknown => SortCol::Null,
            });
        }
        let after = match &self.after {
            None => None,
            Some(a) => {
                let mut out = Vec::new();
                for (v, c) in a.iter().zip(&cols) {
                    out.push(match (v, c) {
                        (After::Score(s), _) => LocalAfter::Score(*s),
                        (After::Num(n), _) => LocalAfter::Num(*n),
                        (After::Str(None), _) => LocalAfter::Str(None),
                        (After::Str(Some(s)), SortCol::Str(Some(col))) => LocalAfter::Str(Some(match col.dictionary().term_ord_or_next(s)? {
                            tantivy::columnar::TermOrdHit::Exact(o) => (o, true),
                            tantivy::columnar::TermOrdHit::Next(o) => (o.min(col.num_terms() as u64), false),
                        })),
                        // No values in this segment: every hit is missing,
                        // which sorts before any string.
                        (After::Str(Some(_)), _) => LocalAfter::Str(Some((0, false))),
                        (After::Null, _) => LocalAfter::Null,
                    });
                }
                Some(out)
            }
        };
        let mut counts = Vec::new();
        for (name, col) in &self.counts {
            let c = match col {
                Some(c) => str_col(reader, c)?,
                None => None,
            };
            counts.push((name.clone(), c, HashMap::new()));
        }
        let mut ranges = Vec::new();
        for (name, col, rs) in &self.ranges {
            let c = match col {
                Some(c) => num_col(reader, c)?,
                None => None,
            };
            ranges.push((name.clone(), c, rs.clone(), vec![0; rs.len()]));
        }
        Ok(SegHits { seg, sort: self.sort.clone(), cols, after, limit: self.limit, total: 0, top: Vec::new(), counts, ranges })
    }

    fn requires_scoring(&self) -> bool {
        self.scoring
    }

    fn merge_fruits(&self, fruits: Vec<SegFruit>) -> tantivy::Result<Fruit> {
        let mut total = 0;
        let mut hits = Vec::new();
        let mut counts: Vec<(String, HashMap<String, u64>)> = self.counts.iter().map(|(n, _)| (n.clone(), HashMap::new())).collect();
        let mut ranges: Vec<(String, HashMap<String, u64>)> = self.ranges.iter().map(|(n, _, _)| (n.clone(), HashMap::new())).collect();
        for f in fruits {
            total += f.total;
            hits.extend(f.top);
            for (i, (_, m)) in f.counts.into_iter().enumerate() {
                for (k, v) in m {
                    *counts[i].1.entry(k).or_default() += v;
                }
            }
            for (i, (_, m)) in f.ranges.into_iter().enumerate() {
                for (k, v) in m {
                    *ranges[i].1.entry(k).or_default() += v;
                }
            }
        }
        hits.sort_by(|a, b| cmp_resolved(&self.sort, &a.0, &b.0));
        hits.truncate(self.limit);
        Ok(Fruit { total, hits, counts, ranges })
    }
}

impl SegHits {
    fn key(&self, doc: DocId, score: Score) -> Vec<Local> {
        self.cols
            .iter()
            .map(|c| match c {
                SortCol::Score => Local::Score(score),
                // The lowest of a document's values sorts it, as Lucene's
                // SortedSetSelector.MIN and SortedNumericSelector.MIN do.
                SortCol::Str(Some(col)) => Local::Ord(col.term_ords(doc).min()),
                SortCol::Str(None) => Local::Ord(None),
                SortCol::Num(Some(col)) => Local::Num(col.values_for_doc(doc).fold(None, |m: Option<f64>, v| Some(m.map_or(v, |m| m.min(v)))).unwrap_or(0.0)),
                SortCol::Num(None) => Local::Num(0.0),
                SortCol::Null => Local::Null,
            })
            .collect()
    }

    fn trim(&mut self) {
        let sort = &self.sort;
        self.top.sort_by(|a, b| cmp_local(sort, &a.0, &b.0));
        self.top.truncate(self.limit);
    }
}

impl SegmentCollector for SegHits {
    type Fruit = SegFruit;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.total += 1;
        for (_, col, m) in &mut self.counts {
            if let Some(col) = col {
                let mut ords: Vec<u64> = col.term_ords(doc).collect();
                ords.dedup();
                for o in ords {
                    *m.entry(o).or_default() += 1;
                }
            }
        }
        for (_, col, rs, n) in &mut self.ranges {
            if let Some(col) = col {
                for (i, r) in rs.iter().enumerate() {
                    if col.values_for_doc(doc).any(|v| r.contains(v)) {
                        n[i] += 1;
                    }
                }
            }
        }
        if self.limit == 0 {
            return;
        }
        let key = self.key(doc, score);
        if let Some(a) = &self.after
            && !after_bookmark(&self.sort, &key, a) {
                return;
            }
        self.top.push((key, doc));
        if self.top.len() >= self.limit * 2 + 64 {
            self.trim();
        }
    }

    fn harvest(mut self) -> SegFruit {
        self.trim();
        let mut buf = String::new();
        let top = self
            .top
            .iter()
            .map(|(key, doc)| {
                let resolved = key
                    .iter()
                    .zip(&self.cols)
                    .map(|(k, c)| match (k, c) {
                        (Local::Score(s), _) => Resolved::Score(*s),
                        (Local::Ord(Some(o)), SortCol::Str(Some(col))) => {
                            buf.clear();
                            let _ = col.ord_to_str(*o, &mut buf);
                            Resolved::Str(Some(buf.clone()))
                        }
                        (Local::Ord(_), _) => Resolved::Str(None),
                        (Local::Num(n), _) => Resolved::Num(*n),
                        (Local::Null, _) => Resolved::Null,
                    })
                    .collect();
                (resolved, DocAddress::new(self.seg, *doc))
            })
            .collect();
        let counts = self
            .counts
            .into_iter()
            .map(|(name, col, m)| {
                let mut out = HashMap::new();
                if let Some(col) = col {
                    for (o, n) in m {
                        let mut s = String::new();
                        if col.ord_to_str(o, &mut s).unwrap_or(false) {
                            out.insert(s, n);
                        }
                    }
                }
                (name, out)
            })
            .collect();
        let ranges = self
            .ranges
            .into_iter()
            .map(|(name, _, rs, n)| {
                let mut out = HashMap::new();
                for (r, c) in rs.iter().zip(n) {
                    out.insert(r.label.clone(), c);
                }
                (name, out)
            })
            .collect();
        SegFruit { total: self.total, top, counts, ranges }
    }
}
