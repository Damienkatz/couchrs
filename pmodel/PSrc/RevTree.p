// Revision trees, flattened. P has no recursive types, so a document's
// rev tree (couch_key_tree) is a map from each revision to its parent edge.
// Stemming is not modelled (revs_limit is effectively infinite).

type tRev = (pos: int, id: int);
type tRevInfo = (parent: tRev, hasParent: bool, deleted: bool, hasBody: bool);
type tTree = map[tRev, tRevInfo];

// A revision and its ancestry, leaf first (what _revisions carries).
type tPathElem = (rev: tRev, info: tRevInfo);
type tPath = seq[tPathElem];

type tDocPath = (doc: int, path: tPath);
type tMergeResult = (tree: tTree, changed: bool, added: set[tRev]);

fun Leaves(t: tTree): set[tRev] {
  var r: tRev;
  var parents: set[tRev];
  var res: set[tRev];
  foreach (r in keys(t)) {
    if (t[r].hasParent) {
      parents += (t[r].parent);
    }
  }
  foreach (r in keys(t)) {
    if (!(r in parents)) {
      res += (r);
    }
  }
  return res;
}

// couch_doc:to_doc_info ordering: live beats deleted, then higher
// generation, then higher rev id.
fun Beats(t: tTree, a: tRev, b: tRev): bool {
  if (t[a].deleted != t[b].deleted) {
    return !t[a].deleted;
  }
  if (a.pos != b.pos) {
    return a.pos > b.pos;
  }
  return a.id > b.id;
}

fun Winner(t: tTree): tRev {
  var r: tRev;
  var best: tRev;
  var have: bool;
  foreach (r in Leaves(t)) {
    if (!have || Beats(t, r, best)) {
      best = r;
      have = true;
    }
  }
  assert have, "Winner of an empty tree";
  return best;
}

// Ancestry of a revision, leaf first.
fun PathOf(t: tTree, r: tRev): tPath {
  var p: tPath;
  var cur: tRev;
  cur = r;
  while (true) {
    p += (sizeof(p), (rev = cur, info = t[cur]));
    if (!t[cur].hasParent) {
      return p;
    }
    cur = t[cur].parent;
  }
  return p;
}

// couch_key_tree:merge of one path. Only the leaf (path[0]) carries a body:
// _revisions sends ancestor ids, not their contents, so new ancestors are
// stored without a body (RevVal::Missing in CouchRS). A body never gets
// overwritten, and an arriving leaf body fills in a missing one.
fun MergePath(t: tTree, path: tPath): tMergeResult {
  var i: int;
  var e: tPathElem;
  var changed: bool;
  var added: set[tRev];
  var info: tRevInfo;
  while (i < sizeof(path)) {
    e = path[i];
    if (i > 0) {
      e.info.hasBody = false;
      e.info.deleted = false;
    }
    if (!(e.rev in t)) {
      t[e.rev] = e.info;
      added += (e.rev);
      changed = true;
    } else if (!t[e.rev].hasBody && e.info.hasBody) {
      info = t[e.rev];
      info.hasBody = true;
      info.deleted = e.info.deleted;
      t[e.rev] = info;
      changed = true;
    }
    i = i + 1;
  }
  return (tree = t, changed = changed, added = added);
}
