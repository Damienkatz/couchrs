//! Revision trees, a port of couch_key_tree.
//!
//! A document's history is a forest: a list of `(start_pos, root)` where
//! `start_pos` is the generation number of the root revision. Most docs have
//! a single tree; disjoint trees appear when stemming or replication leaves
//! histories that no longer share an ancestor.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RevNode<V> {
    pub id: String,
    pub val: V,
    pub kids: Vec<RevNode<V>>,
}

pub type RevTree<V> = Vec<(u64, RevNode<V>)>;

/// A linear path: generation of its first (oldest) element, then ids from
/// oldest to newest, each carrying a value.
pub struct Path<V> {
    pub start: u64,
    pub revs: Vec<(String, V)>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MergeResult {
    /// The path extended an existing leaf.
    NewLeaf,
    /// The path created a new branch (a conflict) or a new tree.
    NewBranch,
    /// Every revision in the path was already present.
    InternalNode,
}

/// Rev ids in CouchDB's order: 32 hex digits compare as the 16 bytes they
/// spell (parse_revid/2 keeps them so), any other id as its own bytes.
pub fn cmp_rev_ids(a: &str, b: &str) -> std::cmp::Ordering {
    let (mut ba, mut bb) = ([0u8; 16], [0u8; 16]);
    let key = |id: &'_ str, buf: &mut [u8; 16]| -> bool { id.len() == 32 && hex::decode_to_slice(id, buf).is_ok() };
    let ka: &[u8] = if key(a, &mut ba) { &ba } else { a.as_bytes() };
    let kb: &[u8] = if key(b, &mut bb) { &bb } else { b.as_bytes() };
    ka.cmp(kb)
}

pub trait RevValue: Clone {
    /// True if this value has no data (stemmed / compacted away).
    fn is_missing(&self) -> bool;
}

fn path_to_node<V: Clone>(revs: &[(String, V)]) -> RevNode<V> {
    let (id, val) = &revs[0];
    RevNode {
        id: id.clone(),
        val: val.clone(),
        kids: if revs.len() > 1 {
            vec![path_to_node(&revs[1..])]
        } else {
            vec![]
        },
    }
}

/// Merge `revs` (starting at this node's depth, revs[0].id == node.id) into
/// the node. Returns what happened.
fn merge_into<V: RevValue>(node: &mut RevNode<V>, revs: &[(String, V)]) -> MergeResult {
    debug_assert_eq!(node.id, revs[0].0);
    if node.val.is_missing() && !revs[0].1.is_missing() {
        node.val = revs[0].1.clone();
    }
    if revs.len() == 1 {
        return MergeResult::InternalNode;
    }
    let next = &revs[1..];
    if let Some(kid) = node.kids.iter_mut().find(|k| k.id == next[0].0) {
        return merge_into(kid, next);
    }
    let was_leaf = node.kids.is_empty();
    node.kids.push(path_to_node(next));
    // Keep siblings in a deterministic order.
    node.kids.sort_by(|a, b| cmp_rev_ids(&a.id, &b.id));
    if was_leaf {
        MergeResult::NewLeaf
    } else {
        MergeResult::NewBranch
    }
}

/// Find the node at `depth` (relative to `node_depth`) with the given id.
fn find_at_depth<'a, V>(
    node: &'a mut RevNode<V>,
    node_depth: u64,
    depth: u64,
    id: &str,
) -> Option<&'a mut RevNode<V>> {
    if node_depth == depth {
        return if node.id == id { Some(node) } else { None };
    }
    for kid in node.kids.iter_mut() {
        if let Some(n) = find_at_depth(kid, node_depth + 1, depth, id) {
            return Some(n);
        }
    }
    None
}

fn try_merge<V: RevValue>(tree: &mut (u64, RevNode<V>), path: &Path<V>) -> Option<MergeResult> {
    let (start, root) = tree;
    let pstart = path.start;
    let pend = pstart + path.revs.len() as u64 - 1;
    if pstart >= *start {
        // Path begins inside (or at the root of) this tree.
        let first = &path.revs[0].0;
        let node = find_at_depth(root, *start, pstart, first)?;
        Some(merge_into(node, &path.revs))
    } else {
        // Path begins above this tree's root: it must pass through the root.
        if pend < *start {
            return None;
        }
        let idx = (*start - pstart) as usize;
        if path.revs[idx].0 != root.id {
            return None;
        }
        let res = merge_into(root, &path.revs[idx..]);
        // Graft the older prefix on top.
        let old_root = std::mem::replace(root, path_to_node(&path.revs[..idx + 1]));
        let mut slot = &mut *root;
        while !slot.kids.is_empty() {
            slot = &mut slot.kids[0];
        }
        *slot = old_root;
        *start = pstart;
        Some(res)
    }
}

pub fn merge_path<V: RevValue>(tree: &mut RevTree<V>, path: Path<V>) -> MergeResult {
    assert!(!path.revs.is_empty());
    for t in tree.iter_mut() {
        if let Some(r) = try_merge(t, &path) {
            return r;
        }
    }
    tree.push((path.start, path_to_node(&path.revs)));
    tree.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| cmp_rev_ids(&a.1.id, &b.1.id)));
    MergeResult::NewBranch
}

/// Merge a path and then stem to `limit` revisions per branch.
pub fn merge<V: RevValue>(tree: &mut RevTree<V>, path: Path<V>, limit: u64) -> MergeResult {
    let r = merge_path(tree, path);
    stem(tree, limit);
    r
}

pub struct Leaf<V> {
    pub pos: u64,
    pub id: String,
    pub val: V,
    /// Ids from this leaf back to the root of its tree.
    pub path: Vec<String>,
}

fn collect_leafs<V: Clone>(node: &RevNode<V>, depth: u64, stack: &mut Vec<String>, out: &mut Vec<Leaf<V>>) {
    stack.push(node.id.clone());
    if node.kids.is_empty() {
        out.push(Leaf {
            pos: depth,
            id: node.id.clone(),
            val: node.val.clone(),
            path: stack.iter().rev().cloned().collect(),
        });
    } else {
        for k in &node.kids {
            collect_leafs(k, depth + 1, stack, out);
        }
    }
    stack.pop();
}

pub fn leafs<V: Clone>(tree: &RevTree<V>) -> Vec<Leaf<V>> {
    let mut out = Vec::new();
    for (start, root) in tree {
        collect_leafs(root, *start, &mut Vec::new(), &mut out);
    }
    out
}

/// Path from the root through every leaf truncated to `limit`, then rebuilt.
pub fn stem<V: RevValue>(tree: &mut RevTree<V>, limit: u64) {
    let limit = limit.max(1) as usize;
    let needs = leafs(tree).iter().any(|l| l.path.len() > limit);
    if !needs {
        return;
    }
    let mut paths: Vec<Path<V>> = Vec::new();
    for (start, root) in tree.iter() {
        let mut stack: Vec<(String, V)> = Vec::new();
        collect_paths(root, *start, &mut stack, &mut paths, limit);
    }
    // Merge longest-first so shared prefixes coalesce into one tree.
    paths.sort_by_key(|a| a.start);
    let mut new: RevTree<V> = Vec::new();
    for p in paths {
        merge_path(&mut new, p);
    }
    *tree = new;
}

fn collect_paths<V: Clone>(
    node: &RevNode<V>,
    depth: u64,
    stack: &mut Vec<(String, V)>,
    out: &mut Vec<Path<V>>,
    limit: usize,
) {
    stack.push((node.id.clone(), node.val.clone()));
    if node.kids.is_empty() {
        let skip = stack.len().saturating_sub(limit);
        out.push(Path {
            start: depth + 1 - (stack.len() - skip) as u64,
            revs: stack[skip..].to_vec(),
        });
    } else {
        for k in &node.kids {
            collect_paths(k, depth + 1, stack, out, limit);
        }
    }
    stack.pop();
}

/// Look up a specific revision. Returns its value and its path to root.
pub fn get<V: Clone>(tree: &RevTree<V>, pos: u64, id: &str) -> Option<(V, Vec<String>)> {
    fn walk<V: Clone>(
        n: &RevNode<V>,
        depth: u64,
        pos: u64,
        id: &str,
        stack: &mut Vec<String>,
    ) -> Option<(V, Vec<String>)> {
        stack.push(n.id.clone());
        let r = if depth == pos {
            if n.id == id {
                Some((n.val.clone(), stack.iter().rev().cloned().collect()))
            } else {
                None
            }
        } else {
            n.kids.iter().find_map(|k| walk(k, depth + 1, pos, id, stack))
        };
        stack.pop();
        r
    }
    tree.iter()
        .find_map(|(s, root)| if pos >= *s { walk(root, *s, pos, id, &mut Vec::new()) } else { None })
}

/// Full path (to root) of values for revision, for `revs_info`.
pub fn get_path_vals<V: Clone>(tree: &RevTree<V>, pos: u64, id: &str) -> Option<Vec<(u64, String, V)>> {
    fn walk<V: Clone>(
        n: &RevNode<V>,
        depth: u64,
        pos: u64,
        id: &str,
        stack: &mut Vec<(u64, String, V)>,
    ) -> Option<Vec<(u64, String, V)>> {
        stack.push((depth, n.id.clone(), n.val.clone()));
        let r = if depth == pos {
            if n.id == id {
                Some(stack.iter().rev().cloned().collect())
            } else {
                None
            }
        } else {
            n.kids.iter().find_map(|k| walk(k, depth + 1, pos, id, stack))
        };
        stack.pop();
        r
    }
    tree.iter()
        .find_map(|(s, root)| if pos >= *s { walk(root, *s, pos, id, &mut Vec::new()) } else { None })
}

/// Leafs descending from the given revision (for `latest=true`).
pub fn leafs_under<V: Clone>(tree: &RevTree<V>, pos: u64, id: &str) -> Vec<Leaf<V>> {
    fn find<'a, V>(n: &'a RevNode<V>, depth: u64, pos: u64, id: &str) -> Option<&'a RevNode<V>> {
        if depth == pos {
            return if n.id == id { Some(n) } else { None };
        }
        n.kids.iter().find_map(|k| find(k, depth + 1, pos, id))
    }
    let mut out = Vec::new();
    for (s, root) in tree {
        if pos < *s {
            continue;
        }
        if let Some(n) = find(root, *s, pos, id) {
            // Paths returned from here are relative to n; fix up with the
            // ancestry so callers can build _revisions.
            let anc = get(tree, pos, id).map(|(_, p)| p).unwrap_or_default();
            let mut sub = Vec::new();
            collect_leafs(n, pos, &mut Vec::new(), &mut sub);
            for mut l in sub {
                l.path.extend(anc.iter().skip(1).cloned());
                out.push(l);
            }
        }
    }
    out
}

pub fn contains<V: Clone>(tree: &RevTree<V>, pos: u64, id: &str) -> bool {
    get(tree, pos, id).is_some()
}

/// Map over every value in the tree.
pub fn map_vals<V: Clone, W: Clone>(tree: &RevTree<V>, f: &mut impl FnMut(u64, &str, &V, bool) -> W) -> RevTree<W> {
    fn walk<V, W>(n: &RevNode<V>, depth: u64, f: &mut impl FnMut(u64, &str, &V, bool) -> W) -> RevNode<W> {
        RevNode {
            id: n.id.clone(),
            val: f(depth, &n.id, &n.val, n.kids.is_empty()),
            kids: n.kids.iter().map(|k| walk(k, depth + 1, f)).collect(),
        }
    }
    tree.iter().map(|(s, r)| (*s, walk(r, *s, f))).collect()
}

/// Remove specific leaf revisions (purge). Returns the removed revs.
pub fn remove_leafs<V: RevValue>(tree: &mut RevTree<V>, revs: &[(u64, String)]) -> Vec<(u64, String)> {
    let all = leafs(tree);
    let mut removed = Vec::new();
    let mut keep: Vec<Path<V>> = Vec::new();
    for (start, root) in tree.iter() {
        let mut stack = Vec::new();
        let mut paths = Vec::new();
        collect_paths(root, *start, &mut stack, &mut paths, usize::MAX);
        for p in paths {
            let leaf_pos = p.start + p.revs.len() as u64 - 1;
            let leaf_id = &p.revs.last().unwrap().0;
            if revs.iter().any(|(pos, id)| *pos == leaf_pos && id == leaf_id) {
                removed.push((leaf_pos, leaf_id.clone()));
            } else {
                keep.push(p);
            }
        }
    }
    let _ = all;
    let mut new = Vec::new();
    for p in keep {
        merge_path(&mut new, p);
    }
    *tree = new;
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    impl RevValue for i32 {
        fn is_missing(&self) -> bool {
            *self == 0
        }
    }

    fn p(start: u64, ids: &[&str]) -> Path<i32> {
        Path {
            start,
            revs: ids.iter().map(|s| (s.to_string(), 1)).collect(),
        }
    }

    #[test]
    fn merges_and_conflicts() {
        let mut t: RevTree<i32> = Vec::new();
        assert_eq!(merge(&mut t, p(1, &["a"]), 1000), MergeResult::NewBranch);
        assert_eq!(merge(&mut t, p(1, &["a", "b"]), 1000), MergeResult::NewLeaf);
        assert_eq!(merge(&mut t, p(2, &["b", "c"]), 1000), MergeResult::NewLeaf);
        assert_eq!(merge(&mut t, p(2, &["b", "x"]), 1000), MergeResult::NewBranch);
        assert_eq!(merge(&mut t, p(1, &["a", "b"]), 1000), MergeResult::InternalNode);
        let ls = leafs(&t);
        assert_eq!(ls.len(), 2);
        assert_eq!(ls[0].path, vec!["c", "b", "a"]);
        assert_eq!(ls[0].pos, 3);
        // a path that starts above the root grafts on
        let mut t2: RevTree<i32> = Vec::new();
        merge(&mut t2, p(3, &["c", "d"]), 1000);
        assert_eq!(merge(&mut t2, p(1, &["a", "b", "c"]), 1000), MergeResult::InternalNode);
        assert_eq!(t2[0].0, 1);
        assert_eq!(leafs(&t2)[0].path, vec!["d", "c", "b", "a"]);
    }

    #[test]
    fn stemming() {
        let mut t: RevTree<i32> = Vec::new();
        merge(&mut t, p(1, &["a", "b", "c", "d", "e"]), 3);
        assert_eq!(t[0].0, 3);
        assert_eq!(leafs(&t)[0].path, vec!["e", "d", "c"]);
        // a branch from c keeps sharing c..
        merge(&mut t, p(3, &["c", "z"]), 3);
        assert_eq!(t.len(), 1);
        assert_eq!(leafs(&t).len(), 2);
        // stemming an older branch that no longer overlaps makes two trees
        merge(&mut t, p(4, &["d", "e", "f", "g"]), 3);
        assert_eq!(leafs(&t).len(), 2);
    }

    #[test]
    fn remove() {
        let mut t: RevTree<i32> = Vec::new();
        merge(&mut t, p(1, &["a", "b"]), 10);
        merge(&mut t, p(1, &["a", "c"]), 10);
        let r = remove_leafs(&mut t, &[(2, "c".into())]);
        assert_eq!(r.len(), 1);
        assert_eq!(leafs(&t).len(), 1);
    }

    #[test]
    fn rev_id_order() {
        use std::cmp::Ordering::*;
        // 32 hex digits compare as bytes: 0xc7 sorts after "z" (0x7a).
        assert_eq!(cmp_rev_ids("c7babbc20892ba5e4ba940b588585336", "zzz"), Greater);
        assert_eq!(cmp_rev_ids("0785e9eb543380151003dc452c3a001a", "zzz"), Less);
        assert_eq!(cmp_rev_ids("C7BABBC20892BA5E4BA940B588585336", "c7babbc20892ba5e4ba940b588585336"), Equal);
        assert_eq!(cmp_rev_ids("aaa", "bbb"), Less);
        assert_eq!(cmp_rev_ids("ab", "abc"), Less);
    }
}
