//! External merge sort for records too numerous to sort in memory.
//!
//! Records accumulate in memory up to a byte budget, are sorted with the
//! caller's comparator and spilled as a run to a scratch file; `finish`
//! streams the k-way merge of all runs back in order. Used to bulk-build
//! B-trees from rows produced in arbitrary order.

use serde::{Serialize, de::DeserializeOwned};
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// In-memory budget per sorter before a run spills to disk
/// (COUCHDB_RS_SORT_MB, default 256).
pub fn budget_bytes() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("COUCHDB_RS_SORT_MB").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(256) << 20
    })
}

/// A record's leading sort bits: heads order records consistently with
/// the full comparison (a smaller head means a smaller record), and tails,
/// present when the head pins down everything it covers, order records
/// whose heads are equal.
pub type Prefix = (u64, Option<u64>);

/// What two prefixes say of their records' order: Equal when they can't
/// tell.
pub fn cmp_prefix(a: &Prefix, b: &Prefix) -> Ordering {
    a.0.cmp(&b.0).then_with(|| match (a.1, b.1) {
        (Some(x), Some(y)) => x.cmp(&y),
        _ => Ordering::Equal,
    })
}

pub struct Sorter<T, F: Fn(&T, &T) -> Ordering> {
    cmp: F,
    /// Optional cheap key computed once per record so most comparisons in
    /// a sort skip `cmp`; see `cmp_prefix`.
    prefix: Option<fn(&T) -> Prefix>,
    buf: Vec<T>,
    buf_bytes: usize,
    limit: usize,
    path: PathBuf,
    file: Option<BufWriter<File>>,
    /// Byte offset where each spilled run starts.
    runs: Vec<u64>,
    written: u64,
}

impl<T: Serialize + DeserializeOwned, F: Fn(&T, &T) -> Ordering> Sorter<T, F> {
    /// `path` is a scratch file, created only if the data spills; it is
    /// removed when the sorter's output is dropped.
    pub fn new(path: PathBuf, limit_bytes: usize, cmp: F) -> Self {
        Sorter { cmp, prefix: None, buf: Vec::new(), buf_bytes: 0, limit: limit_bytes, path, file: None, runs: Vec::new(), written: 0 }
    }

    /// Sorts by `prefix` first, then `cmp` where prefixes can't tell;
    /// prefixes must never order two records against `cmp`.
    pub fn with_prefix(mut self, prefix: fn(&T) -> Prefix) -> Self {
        self.prefix = Some(prefix);
        self
    }

    pub fn push(&mut self, item: T, approx_bytes: usize) -> io::Result<()> {
        self.buf_bytes += approx_bytes + std::mem::size_of::<T>();
        self.buf.push(item);
        if self.buf_bytes >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    fn sort_buf(&mut self) {
        let cmp = &self.cmp;
        match self.prefix {
            None => self.buf.sort_unstable_by(|a, b| cmp(a, b)),
            Some(prefix) => {
                let mut keyed: Vec<(Prefix, T)> = self.buf.drain(..).map(|t| (prefix(&t), t)).collect();
                keyed.sort_unstable_by(|a, b| cmp_prefix(&a.0, &b.0).then_with(|| cmp(&a.1, &b.1)));
                self.buf.extend(keyed.into_iter().map(|(_, t)| t));
            }
        }
    }

    fn spill(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.sort_buf();
        if self.file.is_none() {
            let f = OpenOptions::new().create(true).truncate(true).read(true).write(true).open(&self.path)?;
            self.file = Some(BufWriter::with_capacity(1 << 20, f));
        }
        let w = self.file.as_mut().unwrap();
        let start = self.written;
        for item in self.buf.drain(..) {
            let bytes = bincode::serialize(&item).map_err(io::Error::other)?;
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            w.write_all(&bytes)?;
            self.written += 4 + bytes.len() as u64;
        }
        self.runs.push(start);
        self.buf_bytes = 0;
        Ok(())
    }

    /// Finish and return every record in sorted order.
    pub fn finish(mut self) -> io::Result<Sorted<T, F>> {
        if self.runs.is_empty() {
            self.sort_buf();
            let items = std::mem::take(&mut self.buf);
            return Ok(Sorted { inner: SortedInner::Mem(items.into_iter()), cmp: self.cmp, path: None });
        }
        self.spill()?;
        let mut w = self.file.take().unwrap();
        w.flush()?;
        drop(w);
        let mut readers = Vec::with_capacity(self.runs.len());
        let ends: Vec<u64> = self.runs.iter().skip(1).copied().chain(std::iter::once(self.written)).collect();
        for (i, start) in self.runs.iter().enumerate() {
            let mut f = File::open(&self.path)?;
            f.seek(SeekFrom::Start(*start))?;
            let mut r = RunReader { rd: BufReader::with_capacity(1 << 20, f), left: ends[i] - start, head: None };
            r.advance()?;
            readers.push(r);
        }
        Ok(Sorted { inner: SortedInner::Merge(readers), cmp: self.cmp, path: Some(self.path) })
    }
}

struct RunReader<T> {
    rd: BufReader<File>,
    left: u64,
    head: Option<T>,
}

impl<T: DeserializeOwned> RunReader<T> {
    fn advance(&mut self) -> io::Result<()> {
        if self.left == 0 {
            self.head = None;
            return Ok(());
        }
        let mut len = [0u8; 4];
        self.rd.read_exact(&mut len)?;
        let n = u32::from_le_bytes(len) as usize;
        let mut buf = vec![0u8; n];
        self.rd.read_exact(&mut buf)?;
        self.left -= 4 + n as u64;
        self.head = Some(bincode::deserialize(&buf).map_err(io::Error::other)?);
        Ok(())
    }
}

enum SortedInner<T> {
    Mem(std::vec::IntoIter<T>),
    Merge(Vec<RunReader<T>>),
}

pub struct Sorted<T, F> {
    inner: SortedInner<T>,
    cmp: F,
    path: Option<PathBuf>,
}

impl<T: DeserializeOwned, F: Fn(&T, &T) -> Ordering> Sorted<T, F> {
    pub fn next_item(&mut self) -> io::Result<Option<T>> {
        match &mut self.inner {
            SortedInner::Mem(it) => Ok(it.next()),
            SortedInner::Merge(runs) => {
                // Few runs, so a linear scan for the minimum head is cheap.
                let mut best: Option<usize> = None;
                for (i, r) in runs.iter().enumerate() {
                    if let Some(h) = &r.head {
                        best = match best {
                            Some(b) if (self.cmp)(runs[b].head.as_ref().unwrap(), h) != Ordering::Greater => Some(b),
                            _ => Some(i),
                        };
                    }
                }
                let Some(b) = best else { return Ok(None) };
                let item = runs[b].head.take();
                runs[b].advance()?;
                Ok(item)
            }
        }
    }
}

impl<T, F> Drop for Sorted<T, F> {
    fn drop(&mut self) {
        if let Some(p) = &self.path {
            let _ = std::fs::remove_file(p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spills_and_merges_in_order() {
        let path = std::env::temp_dir().join(format!("sort-{}", uuid::Uuid::new_v4()));
        // Tiny budget forces many runs.
        let mut s = Sorter::new(path.clone(), 4096, |a: &(u64, String), b: &(u64, String)| a.cmp(b));
        let mut x: u64 = 12345;
        let mut expect = Vec::new();
        for i in 0..20_000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let item = (x % 5000, format!("v{i}"));
            expect.push(item.clone());
            s.push(item, 16).unwrap();
        }
        expect.sort();
        let mut out = s.finish().unwrap();
        assert!(path.exists(), "should have spilled");
        let mut got = Vec::new();
        while let Some(i) = out.next_item().unwrap() {
            got.push(i);
        }
        assert_eq!(got, expect);
        drop(out);
        assert!(!path.exists(), "scratch file removed");
    }
}
