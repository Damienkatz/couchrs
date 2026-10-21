//! Append-only B+tree, a port of couch_btree.
//!
//! Inner (kp) nodes hold `(last_key_of_child, NodePtr)`; leaf (kv) nodes hold
//! `(key, value)`. Every pointer carries the reduction of its subtree, so
//! counts, offsets and view reductions are answered without visiting leaves.
//! Nodes are never modified in place: updates write new nodes up to a new
//! root, leaving old roots valid for concurrent readers (MVCC snapshots).

use crate::file::{CouchFile, Pos};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// Target serialized size of a node, matching couch's chunk_threshold.
const CHUNK_THRESHOLD: usize = 1279;

pub trait Spec: Send + Sync + 'static {
    type K: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    type V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    type R: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    fn cmp(&self, a: &Self::K, b: &Self::K) -> Ordering;
    fn reduce(&self, kvs: &[(Self::K, Self::V)]) -> Self::R;
    fn rereduce(&self, reds: &[Self::R]) -> Self::R;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodePtr<R> {
    pub pos: Pos,
    pub red: R,
    /// Total bytes of this subtree on disk (nodes only).
    pub size: u64,
}

#[derive(Serialize, Deserialize, Clone)]
enum Node<K, V, R> {
    Kp(Vec<(K, NodePtr<R>)>),
    Kv(Vec<(K, V)>),
}

type NodeOf<S> = Node<<S as Spec>::K, <S as Spec>::V, <S as Spec>::R>;

/// Small shared cache of decoded inner nodes, which are hot on every lookup.
struct NodeCache<S: Spec> {
    map: Mutex<HashMap<Pos, Arc<NodeOf<S>>>>,
}

pub struct Btree<S: Spec> {
    pub file: Arc<CouchFile>,
    pub root: Option<NodePtr<S::R>>,
    pub spec: Arc<S>,
    cache: Arc<NodeCache<S>>,
}

impl<S: Spec> Clone for Btree<S> {
    fn clone(&self) -> Self {
        Btree {
            file: self.file.clone(),
            root: self.root.clone(),
            spec: self.spec.clone(),
            cache: self.cache.clone(),
        }
    }
}

pub enum Action<K, V> {
    Insert(K, V),
    Remove(K),
}

impl<K, V> Action<K, V> {
    fn key(&self) -> &K {
        match self {
            Action::Insert(k, _) | Action::Remove(k) => k,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Dir {
    Fwd,
    Rev,
}

/// Key range for folds. `start` is where iteration begins in the chosen
/// direction; `end` is where it stops.
pub struct Range<K> {
    pub start: Option<K>,
    pub end: Option<K>,
    pub inclusive_end: bool,
}

impl<K> Default for Range<K> {
    fn default() -> Self {
        Range {
            start: None,
            end: None,
            inclusive_end: true,
        }
    }
}

impl<S: Spec> Btree<S> {
    pub fn new(file: Arc<CouchFile>, root: Option<NodePtr<S::R>>, spec: Arc<S>) -> Self {
        Btree {
            file,
            root,
            spec,
            cache: Arc::new(NodeCache {
                map: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn with_root(&self, root: Option<NodePtr<S::R>>) -> Self {
        let mut t = self.clone();
        t.root = root;
        t
    }

    fn read_node(&self, pos: Pos) -> io::Result<Arc<NodeOf<S>>> {
        if let Some(n) = self.cache.map.lock().get(&pos) {
            return Ok(n.clone());
        }
        let n: NodeOf<S> = self.file.read_term(pos)?;
        let n = Arc::new(n);
        if matches!(*n, Node::Kp(_)) {
            let mut m = self.cache.map.lock();
            if m.len() > 4096 {
                m.clear();
            }
            m.insert(pos, n.clone());
        }
        Ok(n)
    }

    pub fn full_reduce(&self) -> Option<S::R> {
        self.root.as_ref().map(|r| r.red.clone())
    }

    pub fn size(&self) -> u64 {
        self.root.as_ref().map(|r| r.size).unwrap_or(0)
    }

    // ---------------------------------------------------------------- lookup

    pub fn lookup(&self, key: &S::K) -> io::Result<Option<S::V>> {
        let mut ptr = match &self.root {
            None => return Ok(None),
            Some(r) => r.pos,
        };
        loop {
            let node = self.read_node(ptr)?;
            match &*node {
                Node::Kp(entries) => {
                    let i = entries.partition_point(|(k, _)| self.spec.cmp(k, key) == Ordering::Less);
                    if i == entries.len() {
                        return Ok(None);
                    }
                    ptr = entries[i].1.pos;
                }
                Node::Kv(entries) => {
                    return Ok(entries
                        .binary_search_by(|(k, _)| self.spec.cmp(k, key))
                        .ok()
                        .map(|i| entries[i].1.clone()));
                }
            }
        }
    }

    pub fn lookup_many(&self, keys: &[S::K]) -> io::Result<Vec<Option<S::V>>> {
        keys.iter().map(|k| self.lookup(k)).collect()
    }

    // ---------------------------------------------------------------- modify

    /// Apply inserts/removes and return the new tree. Actions need not be
    /// sorted; later actions on the same key win.
    pub fn modify(&self, mut actions: Vec<Action<S::K, S::V>>) -> io::Result<Self> {
        if actions.is_empty() {
            return Ok(self.clone());
        }
        // Stable sort keeps the last action per key last; dedupe to it.
        actions.sort_by(|a, b| self.spec.cmp(a.key(), b.key()));
        let mut deduped: Vec<Action<S::K, S::V>> = Vec::with_capacity(actions.len());
        for a in actions {
            if let Some(last) = deduped.last()
                && self.spec.cmp(last.key(), a.key()) == Ordering::Equal {
                    *deduped.last_mut().unwrap() = a;
                    continue;
                }
            deduped.push(a);
        }
        let mut level = self.modify_node(self.root.as_ref().map(|r| r.pos), deduped)?;
        let root = loop {
            match level.len() {
                0 => break None,
                1 => break Some(level.pop().unwrap().1),
                _ => level = self.write_kp(level)?,
            }
        };
        Ok(self.with_root(root))
    }

    fn modify_node(
        &self,
        pos: Option<Pos>,
        actions: Vec<Action<S::K, S::V>>,
    ) -> io::Result<Vec<(S::K, NodePtr<S::R>)>> {
        let node = match pos {
            None => Arc::new(Node::Kv(Vec::new())),
            Some(p) => self.read_node(p)?,
        };
        match &*node {
            Node::Kv(entries) => {
                let mut out: Vec<(S::K, S::V)> = Vec::with_capacity(entries.len() + actions.len());
                let mut ei = entries.iter().peekable();
                for a in actions {
                    while let Some((k, _)) = ei.peek() {
                        if self.spec.cmp(k, a.key()) == Ordering::Less {
                            out.push(ei.next().unwrap().clone());
                        } else {
                            break;
                        }
                    }
                    if let Some((k, _)) = ei.peek()
                        && self.spec.cmp(k, a.key()) == Ordering::Equal {
                            ei.next();
                        }
                    if let Action::Insert(k, v) = a {
                        out.push((k, v));
                    }
                }
                out.extend(ei.cloned());
                self.write_kv(out)
            }
            Node::Kp(entries) => {
                let mut out: Vec<(S::K, NodePtr<S::R>)> = Vec::with_capacity(entries.len() + 4);
                let mut actions = actions.into_iter().peekable();
                let n = entries.len();
                for (i, (k, ptr)) in entries.iter().enumerate() {
                    let mut mine = Vec::new();
                    while let Some(a) = actions.peek() {
                        if i == n - 1 || self.spec.cmp(a.key(), k) != Ordering::Greater {
                            mine.push(actions.next().unwrap());
                        } else {
                            break;
                        }
                    }
                    if mine.is_empty() {
                        out.push((k.clone(), ptr.clone()));
                    } else {
                        out.extend(self.modify_node(Some(ptr.pos), mine)?);
                    }
                }
                if out.is_empty() {
                    Ok(out)
                } else {
                    self.write_kp(out)
                }
            }
        }
    }

    fn chunkify<T: Serialize>(items: &[T]) -> Vec<std::ops::Range<usize>> {
        let mut ranges = Vec::new();
        let mut start = 0;
        let mut size = 0usize;
        for (i, it) in items.iter().enumerate() {
            let sz = bincode::serialized_size(it).unwrap_or(64) as usize;
            // At least two entries per node, so kp levels always shrink.
            if i - start >= 2 && size + sz > CHUNK_THRESHOLD {
                ranges.push(start..i);
                start = i;
                size = 0;
            }
            size += sz;
        }
        if start < items.len() {
            ranges.push(start..items.len());
        }
        // Avoid a tiny trailing node by merging it into its neighbour.
        if ranges.len() >= 2 {
            let last = ranges.last().unwrap().clone();
            if last.len() * 4 < ranges[ranges.len() - 2].len() {
                ranges.pop();
                ranges.last_mut().unwrap().end = last.end;
            }
        }
        ranges
    }

    fn write_kv(&self, kvs: Vec<(S::K, S::V)>) -> io::Result<Vec<(S::K, NodePtr<S::R>)>> {
        let ranges = Self::chunkify(&kvs);
        let mut payloads = Vec::with_capacity(ranges.len());
        let mut meta = Vec::with_capacity(ranges.len());
        for r in ranges {
            let chunk = &kvs[r];
            let red = self.spec.reduce(chunk);
            let node: NodeOf<S> = Node::Kv(chunk.to_vec());
            payloads.push(crate::file::encode_term(&node));
            meta.push((chunk.last().unwrap().0.clone(), red));
        }
        let ptrs = self.file.append_raw_many(&payloads)?;
        Ok(meta
            .into_iter()
            .zip(ptrs)
            .map(|((k, red), (pos, size))| (k, NodePtr { pos, red, size }))
            .collect())
    }

    fn write_kp(
        &self,
        kps: Vec<(S::K, NodePtr<S::R>)>,
    ) -> io::Result<Vec<(S::K, NodePtr<S::R>)>> {
        let ranges = Self::chunkify(&kps);
        let mut payloads = Vec::with_capacity(ranges.len());
        let mut meta = Vec::with_capacity(ranges.len());
        for r in ranges {
            let chunk = &kps[r];
            let reds: Vec<S::R> = chunk.iter().map(|(_, p)| p.red.clone()).collect();
            let red = self.spec.rereduce(&reds);
            let child_size: u64 = chunk.iter().map(|(_, p)| p.size).sum();
            let node: NodeOf<S> = Node::Kp(chunk.to_vec());
            payloads.push(crate::file::encode_term(&node));
            meta.push((chunk.last().unwrap().0.clone(), red, child_size));
        }
        let ptrs = self.file.append_raw_many(&payloads)?;
        Ok(meta
            .into_iter()
            .zip(ptrs)
            .map(|((k, red, cs), (pos, size))| {
                (
                    k,
                    NodePtr {
                        pos,
                        red,
                        size: size + cs,
                    },
                )
            })
            .collect())
    }

    // ------------------------------------------------------------------ fold

    fn before_start(&self, k: &S::K, start: &Option<S::K>, dir: Dir) -> bool {
        match start {
            None => false,
            Some(s) => match dir {
                Dir::Fwd => self.spec.cmp(k, s) == Ordering::Less,
                Dir::Rev => self.spec.cmp(k, s) == Ordering::Greater,
            },
        }
    }

    fn past_end(&self, k: &S::K, range: &Range<S::K>, dir: Dir) -> bool {
        match &range.end {
            None => false,
            Some(e) => {
                let c = self.spec.cmp(k, e);
                match (dir, range.inclusive_end) {
                    (Dir::Fwd, true) => c == Ordering::Greater,
                    (Dir::Fwd, false) => c != Ordering::Less,
                    (Dir::Rev, true) => c == Ordering::Less,
                    (Dir::Rev, false) => c != Ordering::Greater,
                }
            }
        }
    }

    /// Visit kvs in order. The callback returns false to stop.
    pub fn fold<F>(&self, dir: Dir, range: &Range<S::K>, mut f: F) -> io::Result<()>
    where
        F: FnMut(&S::K, &S::V) -> io::Result<bool>,
    {
        if let Some(r) = &self.root {
            self.fold_node(r.pos, dir, range, &mut f)?;
        }
        Ok(())
    }

    fn fold_node<F>(&self, pos: Pos, dir: Dir, range: &Range<S::K>, f: &mut F) -> io::Result<bool>
    where
        F: FnMut(&S::K, &S::V) -> io::Result<bool>,
    {
        let node = self.read_node(pos)?;
        match &*node {
            Node::Kp(entries) => {
                let n = entries.len();
                let idxs: Box<dyn Iterator<Item = usize>> = match dir {
                    Dir::Fwd => Box::new(0..n),
                    Dir::Rev => Box::new((0..n).rev()),
                };
                for i in idxs {
                    let (k, ptr) = &entries[i];
                    match dir {
                        // Child i holds keys <= k: skip if all are before start.
                        Dir::Fwd => {
                            if self.before_start(k, &range.start, dir) {
                                continue;
                            }
                        }
                        // Child i holds keys > entries[i-1].k: if that lower
                        // bound is already past the end, everything is.
                        Dir::Rev => {
                            if let Some(s) = &range.start
                                && i > 0
                                    && self.spec.cmp(&entries[i - 1].0, s) != Ordering::Less
                                {
                                    continue;
                                }
                        }
                    }
                    if !self.fold_node(ptr.pos, dir, range, f)? {
                        return Ok(false);
                    }
                    if dir == Dir::Fwd && self.past_end(k, range, dir) {
                        return Ok(false);
                    }
                    if dir == Dir::Rev && i > 0 && self.past_end(&entries[i - 1].0, range, dir) {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Node::Kv(entries) => {
                let n = entries.len();
                let idxs: Box<dyn Iterator<Item = usize>> = match dir {
                    Dir::Fwd => {
                        let s = match &range.start {
                            None => 0,
                            Some(s) => entries
                                .partition_point(|(k, _)| self.spec.cmp(k, s) == Ordering::Less),
                        };
                        Box::new(s..n)
                    }
                    Dir::Rev => {
                        let e = match &range.start {
                            None => n,
                            Some(s) => entries
                                .partition_point(|(k, _)| self.spec.cmp(k, s) != Ordering::Greater),
                        };
                        Box::new((0..e).rev())
                    }
                };
                for i in idxs {
                    let (k, v) = &entries[i];
                    if self.past_end(k, range, dir) {
                        return Ok(false);
                    }
                    if !f(k, v)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }
    }

    /// Reduction of every key that sorts strictly before `key` in `dir`
    /// order. Used to compute `offset` for _all_docs and views.
    pub fn reduce_before(&self, key: &S::K, dir: Dir) -> io::Result<Option<S::R>> {
        let mut reds = Vec::new();
        let mut kvs_acc = Vec::new();
        let mut ptr = match &self.root {
            None => return Ok(None),
            Some(r) => r.pos,
        };
        loop {
            let node = self.read_node(ptr)?;
            match &*node {
                Node::Kp(entries) => {
                    let i = entries.partition_point(|(k, _)| self.spec.cmp(k, key) == Ordering::Less);
                    match dir {
                        Dir::Fwd => {
                            reds.extend(entries[..i].iter().map(|(_, p)| p.red.clone()));
                            if i == entries.len() {
                                break;
                            }
                            ptr = entries[i].1.pos;
                        }
                        Dir::Rev => {
                            if i == entries.len() {
                                break;
                            }
                            reds.extend(entries[i + 1..].iter().map(|(_, p)| p.red.clone()));
                            ptr = entries[i].1.pos;
                        }
                    }
                }
                Node::Kv(entries) => {
                    match dir {
                        Dir::Fwd => {
                            let i = entries
                                .partition_point(|(k, _)| self.spec.cmp(k, key) == Ordering::Less);
                            kvs_acc.extend_from_slice(&entries[..i]);
                        }
                        Dir::Rev => {
                            let i = entries.partition_point(|(k, _)| {
                                self.spec.cmp(k, key) != Ordering::Greater
                            });
                            kvs_acc.extend_from_slice(&entries[i..]);
                        }
                    }
                    break;
                }
            }
        }
        if !kvs_acc.is_empty() {
            reds.push(self.spec.reduce(&kvs_acc));
        }
        if reds.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.spec.rereduce(&reds)))
    }

    /// Grouped reduce over a range, a port of couch_btree:fold_reduce.
    /// `same_group(a, b)` says whether two keys belong to the same group;
    /// groups must be contiguous in key order. The callback receives the
    /// first key seen of each group (in fold order) and its reduction.
    pub fn fold_reduce<G, F>(
        &self,
        dir: Dir,
        range: &Range<S::K>,
        same_group: G,
        mut f: F,
    ) -> io::Result<()>
    where
        G: Fn(&S::K, &S::K) -> bool,
        F: FnMut(&S::K, S::R) -> io::Result<bool>,
    {
        let mut acc = GroupAcc::<S> {
            key: None,
            reds: Vec::new(),
            kvs: Vec::new(),
            halted: false,
        };
        if let Some(r) = &self.root {
            self.fold_reduce_node(r.pos, None, None, dir, range, &same_group, &mut acc, &mut f)?;
            if acc.halted {
                return Ok(());
            }
        }
        if let Some(k) = acc.key.take() {
            let red = self.finish_group(&mut acc);
            f(&k, red)?;
        }
        Ok(())
    }

    fn finish_group(&self, acc: &mut GroupAcc<S>) -> S::R {
        let mut reds = std::mem::take(&mut acc.reds);
        if !acc.kvs.is_empty() {
            let kvs = std::mem::take(&mut acc.kvs);
            reds.push(self.spec.reduce(&kvs));
        }
        if reds.len() == 1 {
            reds.pop().unwrap()
        } else {
            self.spec.rereduce(&reds)
        }
    }

    /// True if every key in (lo, hi] lies inside the range.
    fn span_inside(&self, lo: Option<&S::K>, hi: &S::K, dir: Dir, range: &Range<S::K>) -> bool {
        // In key order the span is (lo, hi]. Map range bounds to key order.
        let (min, max, min_incl, max_incl) = match dir {
            Dir::Fwd => (&range.start, &range.end, true, range.inclusive_end),
            Dir::Rev => (&range.end, &range.start, range.inclusive_end, true),
        };
        if let Some(m) = min {
            let lo = match lo {
                None => return false,
                Some(l) => l,
            };
            // Keys in the span are strictly greater than lo, so lo >= m
            // keeps them in range whether the bound is inclusive or not.
            let _ = min_incl;
            if self.spec.cmp(lo, m) == Ordering::Less {
                return false;
            }
        }
        if let Some(m) = max {
            let c = self.spec.cmp(hi, m);
            let ok = if max_incl { c != Ordering::Greater } else { c == Ordering::Less };
            if !ok {
                return false;
            }
        }
        true
    }

    fn in_range(&self, k: &S::K, dir: Dir, range: &Range<S::K>) -> bool {
        !self.before_start(k, &range.start, dir) && !self.past_end(k, range, dir)
    }

    #[allow(clippy::too_many_arguments)]
    fn fold_reduce_node<G, F>(
        &self,
        pos: Pos,
        lo: Option<&S::K>,
        _hi: Option<&S::K>,
        dir: Dir,
        range: &Range<S::K>,
        same_group: &G,
        acc: &mut GroupAcc<S>,
        f: &mut F,
    ) -> io::Result<bool>
    where
        G: Fn(&S::K, &S::K) -> bool,
        F: FnMut(&S::K, S::R) -> io::Result<bool>,
    {
        let node = self.read_node(pos)?;
        match &*node {
            Node::Kp(entries) => {
                let n = entries.len();
                let idxs: Vec<usize> = match dir {
                    Dir::Fwd => (0..n).collect(),
                    Dir::Rev => (0..n).rev().collect(),
                };
                for i in idxs {
                    let (k, ptr) = &entries[i];
                    let child_lo = if i == 0 { lo } else { Some(&entries[i - 1].0) };
                    // Prune children entirely outside the range.
                    match dir {
                        Dir::Fwd => {
                            if self.before_start(k, &range.start, dir) {
                                continue;
                            }
                            // Keys here are > lo; if lo is past the end, so is the rest.
                            if let Some(l) = child_lo
                                && self.past_end(l, range, dir) {
                                    return Ok(false);
                                }
                        }
                        Dir::Rev => {
                            if let (Some(l), Some(s)) = (child_lo, &range.start)
                                && self.spec.cmp(l, s) != Ordering::Less {
                                    continue;
                                }
                            // Keys here are <= k; if k is past the end, so is the rest.
                            if self.past_end(k, range, dir) {
                                return Ok(false);
                            }
                        }
                    }
                    // Whole child can be taken as one reduction if it is in
                    // range and both its bounds share the current group.
                    let whole = match child_lo {
                        Some(l) if self.span_inside(Some(l), k, dir, range) => {
                            same_group(l, k)
                                && match &acc.key {
                                    Some(g) => same_group(g, k),
                                    None => false,
                                }
                        }
                        _ => false,
                    };
                    if whole {
                        acc.reds.push(ptr.red.clone());
                    } else if !self.fold_reduce_node(ptr.pos, child_lo, Some(k), dir, range, same_group, acc, f)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Node::Kv(entries) => {
                let n = entries.len();
                let idxs: Box<dyn Iterator<Item = usize>> = match dir {
                    Dir::Fwd => Box::new(0..n),
                    Dir::Rev => Box::new((0..n).rev()),
                };
                for i in idxs {
                    let (k, v) = &entries[i];
                    if self.before_start(k, &range.start, dir) {
                        continue;
                    }
                    if self.past_end(k, range, dir) {
                        return Ok(false);
                    }
                    debug_assert!(self.in_range(k, dir, range));
                    match &acc.key {
                        Some(g) if same_group(g, k) => {}
                        Some(_) => {
                            let gk = acc.key.take().unwrap();
                            let red = self.finish_group(acc);
                            if !f(&gk, red)? {
                                acc.halted = true;
                                return Ok(false);
                            }
                            acc.key = Some(k.clone());
                        }
                        None => acc.key = Some(k.clone()),
                    }
                    acc.kvs.push((k.clone(), v.clone()));
                }
                Ok(true)
            }
        }
    }
}

struct GroupAcc<S: Spec> {
    key: Option<S::K>,
    reds: Vec<S::R>,
    kvs: Vec<(S::K, S::V)>,
    halted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CountSpec;
    impl Spec for CountSpec {
        type K = u64;
        type V = u64;
        type R = (u64, u64);
        fn cmp(&self, a: &u64, b: &u64) -> Ordering {
            a.cmp(b)
        }
        fn reduce(&self, kvs: &[(u64, u64)]) -> (u64, u64) {
            (kvs.len() as u64, kvs.iter().map(|(_, v)| v).sum())
        }
        fn rereduce(&self, reds: &[(u64, u64)]) -> (u64, u64) {
            reds.iter().fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1))
        }
    }

    fn tree() -> Btree<CountSpec> {
        let p = std::env::temp_dir().join(format!("bt-{}", uuid::Uuid::new_v4()));
        let f = Arc::new(CouchFile::open(&p, true).unwrap());
        Btree::new(f, None, Arc::new(CountSpec))
    }

    #[test]
    fn insert_lookup_remove_fold() {
        let mut t = tree();
        // insert in several batches to exercise splits
        for batch in 0..10u64 {
            let acts = (0..1000u64)
                .map(|i| Action::Insert(i * 10 + batch, i))
                .collect();
            t = t.modify(acts).unwrap();
        }
        assert_eq!(t.full_reduce().unwrap().0, 10_000);
        assert_eq!(t.lookup(&5003).unwrap(), Some(500));
        assert_eq!(t.lookup(&100_000).unwrap(), None);
        t = t
            .modify((0..10_000u64).filter(|i| i % 2 == 0).map(Action::Remove).collect())
            .unwrap();
        assert_eq!(t.full_reduce().unwrap().0, 5000);
        let mut seen = Vec::new();
        let r = Range {
            start: Some(101),
            end: Some(111),
            inclusive_end: true,
        };
        t.fold(Dir::Fwd, &r, |k, _| {
            seen.push(*k);
            Ok(true)
        })
        .unwrap();
        assert_eq!(seen, vec![101, 103, 105, 107, 109, 111]);
        seen.clear();
        let r = Range {
            start: Some(111),
            end: Some(101),
            inclusive_end: false,
        };
        t.fold(Dir::Rev, &r, |k, _| {
            seen.push(*k);
            Ok(true)
        })
        .unwrap();
        assert_eq!(seen, vec![111, 109, 107, 105, 103]);
        assert_eq!(t.reduce_before(&101, Dir::Fwd).unwrap().unwrap().0, 50);
        assert_eq!(t.reduce_before(&9999, Dir::Rev).unwrap(), None);
        // grouped reduce: group by key / 1000
        let mut groups = Vec::new();
        t.fold_reduce(Dir::Fwd, &Range::default(), |a, b| a / 1000 == b / 1000, |k, r| {
            groups.push((k / 1000, r.0));
            Ok(true)
        })
        .unwrap();
        assert_eq!(groups.len(), 10);
        assert!(groups.iter().all(|(_, c)| *c == 500));
        let mut groups = Vec::new();
        let r = Range {
            start: Some(8500),
            end: Some(1500),
            inclusive_end: true,
        };
        t.fold_reduce(Dir::Rev, &r, |a, b| a / 1000 == b / 1000, |k, r| {
            groups.push((k / 1000, r.0));
            Ok(true)
        })
        .unwrap();
        assert_eq!(groups, vec![(8, 250), (7, 500), (6, 500), (5, 500), (4, 500), (3, 500), (2, 500), (1, 250)]);
        // full reduce over a sub-range
        let mut total = 0;
        let r = Range {
            start: Some(1000),
            end: Some(2000),
            inclusive_end: false,
        };
        t.fold_reduce(Dir::Fwd, &r, |_, _| true, |_, r| {
            total += r.0;
            Ok(true)
        })
        .unwrap();
        assert_eq!(total, 500);
    }
}
