//! RocksDB plumbing shared by the database and view engines.
//!
//! RocksDB's own compression is off everywhere (data blocks, bottommost
//! level, blob files): values are LZ4-compressed by couchdb-rs before they
//! reach RocksDB, so nothing is compressed twice.

use crate::error::{CouchError, Result};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBCompressionType, IteratorMode, MultiThreaded, Options, ReadOptions,
    SnapshotWithThreadMode,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub type Rdb = rocksdb::DBWithThreadMode<MultiThreaded>;

pub fn rerr(e: rocksdb::Error) -> CouchError {
    CouchError::internal(format!("rocksdb: {e}"))
}

/// Column family options. `blob` keeps large values (attachment chunks)
/// in blob files so LSM compaction doesn't rewrite them.
pub fn cf_options(blob: bool) -> Options {
    let mut o = Options::default();
    o.set_compression_type(DBCompressionType::None);
    o.set_bottommost_compression_type(DBCompressionType::None);
    if blob {
        o.set_enable_blob_files(true);
        o.set_min_blob_size(4096);
        o.set_blob_compression_type(DBCompressionType::None);
        o.set_enable_blob_gc(true);
        o.set_blob_gc_age_cutoff(1.0);
    }
    o
}

pub struct Store {
    pub db: Rdb,
    pub path: PathBuf,
}

impl Store {
    /// Open (or create) a store with the given column families: (name, blob).
    pub fn open(path: &Path, cfs: &[(&str, bool)], create: bool) -> Result<Arc<Store>> {
        let mut opts = cf_options(false);
        opts.create_if_missing(create);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(8);
        // RocksDB's diagnostic LOG otherwise grows with every flush/compaction.
        opts.set_log_level(rocksdb::LogLevel::Warn);
        let descs: Vec<ColumnFamilyDescriptor> =
            cfs.iter().map(|(n, blob)| ColumnFamilyDescriptor::new(*n, cf_options(*blob))).collect();
        let db = Rdb::open_cf_descriptors(&opts, path, descs).map_err(rerr)?;
        Ok(Arc::new(Store { db, path: path.to_path_buf() }))
    }

    pub fn cf(&self, name: &str) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(name).expect("column family")
    }

    /// Bytes of the files holding data: SST tables, blob files, the WAL and
    /// the manifest. RocksDB's diagnostic LOG, OPTIONS, LOCK, IDENTITY and
    /// CURRENT files are bookkeeping and not counted.
    pub fn disk_bytes(&self) -> u64 {
        let Ok(rd) = std::fs::read_dir(&self.path) else { return 0 };
        rd.flatten()
            .filter(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.ends_with(".sst") || n.ends_with(".blob") || (n.ends_with(".log") && n != "LOG") || n.starts_with("MANIFEST-")
            })
            .filter_map(|e| e.metadata().ok().map(|m| m.len()))
            .sum()
    }

    pub fn prop(&self, cf: &str, name: &str) -> u64 {
        self.db.property_int_value_cf(&self.cf(cf), name).ok().flatten().unwrap_or(0)
    }
}

/// A point-in-time read view. It owns its store so the snapshot can never
/// outlive the database it refers to.
pub struct View {
    // Declared first so it is dropped before `store`.
    snap: Option<SnapshotWithThreadMode<'static, Rdb>>,
    store: Arc<Store>,
}

impl View {
    pub fn new(store: Arc<Store>) -> Arc<View> {
        let snap = store.db.snapshot();
        // SAFETY: the snapshot borrows `store.db`, which lives in the Arc'd
        // Store this View keeps alive, and Drop releases the snapshot first.
        let snap: SnapshotWithThreadMode<'static, Rdb> = unsafe { std::mem::transmute(snap) };
        Arc::new(View { snap: Some(snap), store })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    pub fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snap = self.snap.as_ref().unwrap();
        snap.get_cf_opt(&self.store.cf(cf), key, ReadOptions::default()).map_err(rerr)
    }

    /// Visit keys in [lower, upper) in order (or reverse); `f` returns
    /// false to stop.
    pub fn scan<F>(&self, cf: &str, lower: Option<&[u8]>, upper: Option<&[u8]>, reverse: bool, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let snap = self.snap.as_ref().unwrap();
        let mut ro = ReadOptions::default();
        if let Some(l) = lower {
            ro.set_iterate_lower_bound(l.to_vec());
        }
        if let Some(u) = upper {
            ro.set_iterate_upper_bound(u.to_vec());
        }
        let mode = if reverse { IteratorMode::End } else { IteratorMode::Start };
        for item in snap.iterator_cf_opt(&self.store.cf(cf), ro, mode) {
            let (k, v) = item.map_err(rerr)?;
            if !f(&k, &v)? {
                break;
            }
        }
        Ok(())
    }
}

impl Drop for View {
    fn drop(&mut self) {
        self.snap.take();
    }
}

/// The smallest key strictly greater than `k`, turning an inclusive upper
/// bound into the exclusive one RocksDB iterators take.
pub fn successor(k: &[u8]) -> Vec<u8> {
    let mut s = k.to_vec();
    s.push(0);
    s
}
