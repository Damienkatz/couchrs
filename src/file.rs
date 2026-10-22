//! Append-only file, a port of couch_file.
//!
//! The file is divided into 4096-byte blocks. The first byte of every block
//! is a prefix: 0 for a data block, 1 for a block that starts a header. That
//! lets us find the most recent valid header by scanning backwards from EOF
//! after a crash, exactly as couch_file:find_header does.
//!
//! Chunks are laid out as `[len: u32 BE][crc32: u32 BE][payload]`, with block
//! prefix bytes transparently inserted wherever a chunk crosses a boundary.

use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub const BLOCK: u64 = 4096;

/// When set, commits use the platform's strongest sync (F_FULLFSYNC on
/// macOS) instead of plain fsync. Set from `--fsync-mode full`.
pub static FULL_FSYNC: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub type Pos = u64;

pub struct CouchFile {
    /// Unique per opened file, so callers can tell when compaction has
    /// swapped the file out from under data they already wrote.
    pub generation: u64,
    path: PathBuf,
    file: File,
    eof: Mutex<u64>,
}

fn corrupt(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Insert block prefix bytes into `data` assuming it will be written at
/// physical position `pos`.
fn add_prefixes(pos: u64, data: &[u8], out: &mut Vec<u8>) {
    let mut pos = pos;
    let mut rest = data;
    while !rest.is_empty() {
        let in_block = pos % BLOCK;
        if in_block == 0 {
            out.push(0);
            pos += 1;
            continue;
        }
        let room = (BLOCK - in_block) as usize;
        let n = room.min(rest.len());
        out.extend_from_slice(&rest[..n]);
        rest = &rest[n..];
        pos += n as u64;
    }
}

impl CouchFile {
    pub fn open(path: &Path, create: bool) -> io::Result<CouchFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(path)?;
        let eof = file.metadata()?.len();
        static NEXT_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Ok(CouchFile {
            generation: NEXT_GEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            path: path.to_path_buf(),
            file,
            eof: Mutex::new(eof),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn eof(&self) -> u64 {
        *self.eof.lock()
    }

    /// Append a raw chunk, returning its position and its physical size.
    pub fn append_raw(&self, payload: &[u8]) -> io::Result<(Pos, u64)> {
        let mut logical = Vec::with_capacity(payload.len() + 8);
        logical.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        logical.extend_from_slice(&crc32fast::hash(payload).to_be_bytes());
        logical.extend_from_slice(payload);
        let mut eof = self.eof.lock();
        let pos = *eof;
        let mut phys = Vec::with_capacity(logical.len() + logical.len() / 4096 + 2);
        add_prefixes(pos, &logical, &mut phys);
        self.file.write_all_at(&phys, pos)?;
        *eof += phys.len() as u64;
        Ok((pos, phys.len() as u64))
    }

    /// Append many chunks in a single write. Returns their positions.
    pub fn append_raw_many<T: AsRef<[u8]>>(&self, payloads: &[T]) -> io::Result<Vec<(Pos, u64)>> {
        let mut eof = self.eof.lock();
        let start = *eof;
        let mut pos = start;
        let mut phys = Vec::new();
        let mut out = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let payload = payload.as_ref();
            let mut logical = Vec::with_capacity(payload.len() + 8);
            logical.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            logical.extend_from_slice(&crc32fast::hash(payload).to_be_bytes());
            logical.extend_from_slice(payload);
            let before = phys.len();
            add_prefixes(pos, &logical, &mut phys);
            let sz = (phys.len() - before) as u64;
            out.push((pos, sz));
            pos += sz;
        }
        self.file.write_all_at(&phys, start)?;
        *eof = pos;
        Ok(out)
    }

    /// Read `len` logical bytes starting at physical `pos`, skipping block
    /// prefixes. Returns the bytes and the next physical position.
    fn read_logical(&self, pos: u64, len: usize) -> io::Result<(Vec<u8>, u64)> {
        // Over-read enough to cover prefix bytes, then strip them.
        let start_in_block = pos % BLOCK;
        let extra = (start_in_block as usize + len) / (BLOCK as usize - 1) + 2;
        let phys_len = len + extra;
        let mut buf = vec![0u8; phys_len];
        let n = read_at_most(&self.file, &mut buf, pos)?;
        buf.truncate(n);
        let mut out = Vec::with_capacity(len);
        let mut p = pos;
        let mut i = 0usize;
        while out.len() < len {
            if i >= buf.len() {
                return Err(corrupt("unexpected eof reading chunk"));
            }
            if p.is_multiple_of(BLOCK) {
                i += 1;
                p += 1;
                continue;
            }
            let room = (BLOCK - p % BLOCK) as usize;
            let n = room.min(len - out.len()).min(buf.len() - i);
            out.extend_from_slice(&buf[i..i + n]);
            i += n;
            p += n as u64;
        }
        Ok((out, p))
    }

    pub fn read_raw(&self, pos: Pos) -> io::Result<Vec<u8>> {
        let (hdr, next) = self.read_logical(pos, 8)?;
        let len = u32::from_be_bytes(hdr[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_be_bytes(hdr[4..8].try_into().unwrap());
        if len as u64 > self.eof() {
            return Err(corrupt(format!("bad chunk length at {pos}")));
        }
        let (data, _) = self.read_logical(next, len)?;
        if crc32fast::hash(&data) != crc {
            return Err(corrupt(format!("checksum mismatch at {pos}")));
        }
        Ok(data)
    }

    pub fn append_term<T: Serialize>(&self, term: &T) -> io::Result<(Pos, u64)> {
        self.append_raw(&encode_term(term))
    }

    /// Append a stored value (doc body), LZ4-compressed when that helps.
    pub fn append_value<T: Serialize>(&self, value: &T) -> io::Result<(Pos, u64)> {
        self.append_raw(&encode_value(value))
    }

    pub fn read_term<T: DeserializeOwned>(&self, pos: Pos) -> io::Result<T> {
        decode_term(&self.read_raw(pos)?)
    }

    pub fn write_header<T: Serialize>(&self, header: &T) -> io::Result<()> {
        let payload = encode_term(header);
        let mut logical = Vec::with_capacity(payload.len() + 8);
        logical.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        logical.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        logical.extend_from_slice(&payload);
        let mut eof = self.eof.lock();
        let pad = (BLOCK - *eof % BLOCK) % BLOCK;
        let start = *eof + pad;
        let mut phys = vec![0u8; pad as usize];
        phys.push(1);
        add_prefixes(start + 1, &logical, &mut phys);
        self.file.write_all_at(&phys, *eof)?;
        *eof += phys.len() as u64;
        Ok(())
    }

    /// Scan backwards for the newest valid header.
    pub fn read_header<T: DeserializeOwned>(&self) -> io::Result<Option<T>> {
        let eof = self.eof();
        if eof == 0 {
            return Ok(None);
        }
        let mut block = (eof - 1) / BLOCK;
        loop {
            let pos = block * BLOCK;
            let mut b = [0u8; 1];
            if read_at_most(&self.file, &mut b, pos)? == 1 && b[0] == 1
                && let Ok(raw) = self.read_raw(pos + 1)
                    && let Ok(h) = decode_term::<T>(&raw) {
                        return Ok(Some(h));
                    }
            if block == 0 {
                return Ok(None);
            }
            block -= 1;
        }
    }

    pub fn sync(&self) -> io::Result<()> {
        if FULL_FSYNC.load(std::sync::atomic::Ordering::Relaxed) {
            // On macOS this is F_FULLFSYNC, which also flushes the drive cache.
            return self.file.sync_data();
        }
        // Plain fsync, the level Erlang's file:datasync (and so CouchDB) uses.
        use std::os::unix::io::AsRawFd;
        if unsafe { libc::fsync(self.file.as_raw_fd()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn truncate(&self, len: u64) -> io::Result<()> {
        let mut eof = self.eof.lock();
        self.file.set_len(len)?;
        *eof = len;
        Ok(())
    }
}

fn read_at_most(f: &File, buf: &mut [u8], pos: u64) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match f.read_at(&mut buf[n..], pos + n as u64) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

const ENC_RAW: u8 = 0;
const ENC_SNAPPY: u8 = 1;
const ENC_LZ4: u8 = 2;
/// Uncompressed, in the current value layout (vs ENC_RAW, the old one).
const ENC_RAW2: u8 = 3;

/// Serialize with bincode, snappy-compressing larger payloads.
pub fn encode_term<T: Serialize>(term: &T) -> Vec<u8> {
    let raw = bincode::serialize(term).expect("serialize");
    if raw.len() > 128 {
        let mut enc = snap::raw::Encoder::new();
        if let Ok(c) = enc.compress_vec(&raw)
            && c.len() < raw.len() {
                let mut out = Vec::with_capacity(c.len() + 1);
                out.push(ENC_SNAPPY);
                out.extend_from_slice(&c);
                return out;
            }
    }
    let mut out = Vec::with_capacity(raw.len() + 1);
    out.push(ENC_RAW);
    out.extend_from_slice(&raw);
    out
}

/// Serialize a stored value (doc body) LZ4-compressed, or raw when LZ4
/// doesn't make it smaller. Keys and B-tree nodes keep `encode_term`.
/// Values are tagged ENC_LZ4/ENC_RAW2 so readers can tell them from
/// values written in the previous layout (ENC_RAW/ENC_SNAPPY).
pub fn encode_value<T: Serialize>(value: &T) -> Vec<u8> {
    let raw = bincode::serialize(value).expect("serialize");
    let mut out = tag_lz4_or_raw(&raw);
    if out[0] == ENC_RAW {
        out[0] = ENC_RAW2;
    }
    out
}

/// True if `data` was written by `encode_value` (current layout).
pub fn is_current_value(data: &[u8]) -> bool {
    matches!(data.first(), Some(&ENC_LZ4) | Some(&ENC_RAW2))
}

pub fn decode_value<T: DeserializeOwned>(data: &[u8]) -> io::Result<T> {
    match data.split_first() {
        Some((&ENC_LZ4, body)) => {
            let raw = lz4_flex::block::decompress_size_prepended(body).map_err(|e| corrupt(e.to_string()))?;
            bincode::deserialize(&raw).map_err(|e| corrupt(e.to_string()))
        }
        Some((&ENC_RAW2, body)) => bincode::deserialize(body).map_err(|e| corrupt(e.to_string())),
        _ => Err(corrupt("not a current-layout value")),
    }
}

fn tag_lz4_or_raw(raw: &[u8]) -> Vec<u8> {
    if raw.len() > 64 {
        let c = lz4_flex::block::compress_prepend_size(raw);
        if c.len() < raw.len() {
            let mut out = Vec::with_capacity(c.len() + 1);
            out.push(ENC_LZ4);
            out.extend_from_slice(&c);
            return out;
        }
    }
    let mut out = Vec::with_capacity(raw.len() + 1);
    out.push(ENC_RAW);
    out.extend_from_slice(raw);
    out
}

/// Encode an attachment chunk for a format-2 file: tagged, LZ4 when that
/// shrinks it.
pub fn encode_chunk(data: &[u8]) -> Vec<u8> {
    tag_lz4_or_raw(data)
}

/// Decode an attachment chunk stored by `encode_chunk`.
pub fn decode_chunk(stored: Vec<u8>) -> io::Result<Vec<u8>> {
    match stored.first() {
        Some(&ENC_RAW) => Ok(stored[1..].to_vec()),
        Some(&ENC_LZ4) => lz4_flex::block::decompress_size_prepended(&stored[1..]).map_err(|e| corrupt(e.to_string())),
        _ => Err(corrupt("unknown chunk encoding")),
    }
}

pub fn decode_term<T: DeserializeOwned>(data: &[u8]) -> io::Result<T> {
    let (tag, body) = data.split_first().ok_or_else(|| corrupt("empty term"))?;
    let res = match *tag {
        ENC_RAW => bincode::deserialize(body),
        ENC_SNAPPY => {
            let raw = snap::raw::Decoder::new()
                .decompress_vec(body)
                .map_err(|e| corrupt(e.to_string()))?;
            bincode::deserialize(&raw)
        }
        _ => return Err(corrupt("unknown term encoding")),
    };
    res.map_err(|e| corrupt(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lz4_values_and_chunks_round_trip() {
        let text = "the quick brown fox ".repeat(500);
        let enc = encode_value(&text);
        assert_eq!(enc[0], ENC_LZ4);
        assert!(enc.len() < text.len() / 4);
        assert!(is_current_value(&enc));
        assert_eq!(decode_value::<String>(&enc).unwrap(), text);
        let small = encode_value(&"hi".to_string());
        assert_eq!(small[0], ENC_RAW2);
        assert_eq!(decode_value::<String>(&small).unwrap(), "hi");
        // Old-layout terms are not mistaken for current values.
        assert!(!is_current_value(&encode_term(&text)));
        // Incompressible data is stored raw rather than grown.
        let mut x = 7u64;
        let random: Vec<u8> = (0..65536).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x as u8 }).collect();
        let c = encode_chunk(&random);
        assert_eq!((c[0], c.len()), (ENC_RAW, random.len() + 1));
        assert_eq!(decode_chunk(c).unwrap(), random);
        let c = encode_chunk(text.as_bytes());
        assert_eq!(c[0], ENC_LZ4);
        assert_eq!(decode_chunk(c).unwrap(), text.as_bytes());
    }

    #[test]
    fn chunks_cross_blocks_and_header_recovers() {
        let dir = std::env::temp_dir().join(format!("cf-{}", uuid::Uuid::new_v4()));
        let f = CouchFile::open(&dir, true).unwrap();
        let big = vec![7u8; 10_000];
        let (p1, _) = f.append_raw(b"hello").unwrap();
        let (p2, _) = f.append_raw(&big).unwrap();
        f.write_header(&(42u64, "hdr".to_string())).unwrap();
        let (p3, _) = f.append_raw(b"after").unwrap();
        assert_eq!(f.read_raw(p1).unwrap(), b"hello");
        assert_eq!(f.read_raw(p2).unwrap(), big);
        assert_eq!(f.read_raw(p3).unwrap(), b"after");
        let h: (u64, String) = f.read_header().unwrap().unwrap();
        assert_eq!(h, (42, "hdr".to_string()));
        // A torn write after the header must not hide it.
        let eof = f.eof();
        f.file.write_all_at(&[1, 0, 0, 0xff, 0xff], eof).unwrap();
        let f2 = CouchFile::open(&dir, false).unwrap();
        let h: (u64, String) = f2.read_header().unwrap().unwrap();
        assert_eq!(h.0, 42);
        std::fs::remove_file(dir).ok();
    }
}
