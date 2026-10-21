// CheckpointSafety: when a replicator records checkpoint S for src -> tgt,
// every revision that was a leaf of some doc at any src seq <= S must
// already exist in tgt's tree (with or without a body). If this fails, a
// replicator crash after the checkpoint loses those revisions for good.
type tDocRev = (doc: int, rev: tRev);
type tSeqEntry = (doc: int, sq: int, leaves: set[tRev]);

spec CheckpointSafety observes eSpecRevStored, eSpecDocSeq, eSpecCheckpoint {
  var present: map[int, set[tDocRev]];
  var history: map[int, seq[tSeqEntry]];

  start state Watching {
    on eSpecRevStored do (p: (node: int, doc: int, rev: tRev)) {
      if (!(p.node in present)) {
        present[p.node] = default(set[tDocRev]);
      }
      present[p.node] += ((doc = p.doc, rev = p.rev));
    }

    on eSpecDocSeq do (p: (node: int, doc: int, sq: int, leaves: set[tRev])) {
      if (!(p.node in history)) {
        history[p.node] = default(seq[tSeqEntry]);
      }
      history[p.node] += (sizeof(history[p.node]), (doc = p.doc, sq = p.sq, leaves = p.leaves));
    }

    on eSpecCheckpoint do (p: (src: int, tgt: int, sq: int)) {
      var h: tSeqEntry;
      var r: tRev;
      if (!(p.src in history)) {
        return;
      }
      foreach (h in history[p.src]) {
        if (h.sq <= p.sq) {
          foreach (r in h.leaves) {
            assert p.tgt in present && (doc = h.doc, rev = r) in present[p.tgt],
              format("checkpoint {0} for node {1} -> node {2} skips doc {3} rev {4} (src seq {5})",
                     p.sq, p.src, p.tgt, h.doc, r, h.sq);
          }
        }
      }
    }
  }
}

// Convergence + durability, checked after the system quiesces and a final
// bidirectional replication has run:
//  - both nodes hold identical rev trees (same revs, parents, deleted flags)
//  - both pick the same winning revision for every doc (deterministic
//    conflict resolution, no coordination needed)
//  - every leaf has its body on every node
//  - no acknowledged client edit is missing anywhere
spec Convergence observes eSpecEditAcked, eSpecFinal {
  var acked: set[tDocRev];
  var finals: map[int, map[int, tTree]];

  start state Watching {
    on eSpecEditAcked do (p: (doc: int, rev: tRev)) {
      acked += ((doc = p.doc, rev = p.rev));
    }

    on eSpecFinal do (p: (node: int, trees: map[int, tTree])) {
      finals[p.node] = p.trees;
      if (sizeof(finals) == 2) {
        CheckConverged(finals[0], finals[1]);
      }
    }
  }

  fun CheckConverged(a: map[int, tTree], b: map[int, tTree]) {
    var d: int;
    var r: tRev;
    var ta: tTree;
    var tb: tTree;
    var k: tDocRev;
    assert sizeof(keys(a)) == sizeof(keys(b)), format("doc sets differ: {0} vs {1}", keys(a), keys(b));
    foreach (d in keys(a)) {
      assert d in b, format("doc {0} missing on node 1", d);
      ta = a[d];
      tb = b[d];
      assert sizeof(ta) == sizeof(tb), format("doc {0}: rev trees differ: {1} vs {2}", d, keys(ta), keys(tb));
      foreach (r in keys(ta)) {
        assert r in tb, format("doc {0}: rev {1} missing on node 1", d, r);
        assert ta[r].parent == tb[r].parent && ta[r].hasParent == tb[r].hasParent,
          format("doc {0}: rev {1} has different parents", d, r);
      }
      foreach (r in Leaves(ta)) {
        assert ta[r].hasBody && tb[r].hasBody, format("doc {0}: leaf {1} lacks a body", d, r);
        assert ta[r].deleted == tb[r].deleted, format("doc {0}: leaf {1} deleted flag differs", d, r);
      }
      assert Winner(ta) == Winner(tb),
        format("doc {0}: winners differ: {1} vs {2}", d, Winner(ta), Winner(tb));
    }
    foreach (k in acked) {
      assert k.doc in a && k.rev in a[k.doc], format("acked edit {0} lost on node 0", k);
      assert k.doc in b && k.rev in b[k.doc], format("acked edit {0} lost on node 1", k);
    }
  }
}

// Not a correctness property: used by tcConflictsAreReachable, which is
// expected to fail, to show that conflicts actually occur in the model.
spec ConflictsNeverHappen observes eSpecFinal {
  start state Watching {
    on eSpecFinal do (p: (node: int, trees: map[int, tTree])) {
      var d: int;
      foreach (d in keys(p.trees)) {
        assert sizeof(Leaves(p.trees[d])) == 1,
          format("reached: doc {0} on node {1} has conflicting leaves {2}", d, p.node, Leaves(p.trees[d]));
      }
    }
  }
}
