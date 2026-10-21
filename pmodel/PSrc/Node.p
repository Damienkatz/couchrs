// A database replica (couch_db + couch_db_updater). Keeps a rev tree per
// doc and a by-seq index in which an updated doc moves to the newest seq.

machine Node {
  var id: int;
  var trees: map[int, tTree];
  var updateSeq: int;
  var docSeq: map[int, int];
  var seqDoc: map[int, int];
  var checkpoints: map[int, int];

  start state Init {
    entry (nodeId: int) {
      id = nodeId;
      goto Serving;
    }
  }

  state Serving {
    on eGetDoc do (req: tGetDocReq) {
      var t: tTree;
      var w: tRev;
      if (!(req.doc in trees)) {
        send req.client, eGetDocResp, (found = false, winner = default(tRev), winnerDeleted = false, leaves = default(set[tRev]));
        return;
      }
      t = trees[req.doc];
      w = Winner(t);
      send req.client, eGetDocResp, (found = true, winner = w, winnerDeleted = t[w].deleted, leaves = Leaves(t));
    }

    on eEdit do (req: tEditReq) {
      var t: tTree;
      var found: bool;
      found = req.doc in trees;
      if (found) {
        t = trees[req.doc];
      }
      // An edit must name a current leaf; a create must target a missing doc.
      if (req.hasParent) {
        if (!found || !(req.parent in Leaves(t))) {
          send req.client, eEditResp, (ok = false,);
          return;
        }
        assert req.rev.pos == req.parent.pos + 1, "edit must extend its parent by one generation";
      } else if (found) {
        send req.client, eEditResp, (ok = false,);
        return;
      }
      assert !(req.rev in t), "rev ids are unique";
      t[req.rev] = (parent = req.parent, hasParent = req.hasParent, deleted = req.deleted, hasBody = true);
      trees[req.doc] = t;
      announce eSpecRevStored, (node = id, doc = req.doc, rev = req.rev);
      Bump(req.doc);
      send req.client, eEditResp, (ok = true,);
    }

    on eChanges do (req: (rep: machine, since: int)) {
      var rows: seq[tChangeRow];
      var s: int;
      var d: int;
      s = req.since + 1;
      while (s <= updateSeq) {
        if (s in seqDoc) {
          d = seqDoc[s];
          rows += (sizeof(rows), (doc = d, sq = s, leaves = Leaves(trees[d])));
        }
        s = s + 1;
      }
      // last_seq comes from the same snapshot as the rows.
      send req.rep, eChangesResp, (rows = rows, lastSeq = updateSeq);
    }

    on eRevsDiff do (req: (rep: machine, revs: map[int, set[tRev]])) {
      var missing: map[int, set[tRev]];
      var d: int;
      var r: tRev;
      var m: set[tRev];
      foreach (d in keys(req.revs)) {
        m = default(set[tRev]);
        foreach (r in req.revs[d]) {
          if (!(d in trees) || !(r in trees[d])) {
            m += (r);
          }
        }
        if (sizeof(m) > 0) {
          missing[d] = m;
        }
      }
      send req.rep, eRevsDiffResp, (missing = missing,);
    }

    on eBulkGet do (req: (rep: machine, wanted: map[int, set[tRev]])) {
      var docs: seq[tDocPath];
      var d: int;
      var r: tRev;
      foreach (d in keys(req.wanted)) {
        foreach (r in req.wanted[d]) {
          if (d in trees && r in trees[d]) {
            docs += (sizeof(docs), (doc = d, path = PathOf(trees[d], r)));
          }
        }
      }
      send req.rep, eBulkGetResp, (docs = docs,);
    }

    on eBulkDocs do (req: (rep: machine, docs: seq[tDocPath])) {
      var i: int;
      var dp: tDocPath;
      var t: tTree;
      var res: tMergeResult;
      var r: tRev;
      while (i < sizeof(req.docs)) {
        dp = req.docs[i];
        t = default(tTree);
        if (dp.doc in trees) {
          t = trees[dp.doc];
        }
        res = MergePath(t, dp.path);
        if (res.changed) {
          trees[dp.doc] = res.tree;
          foreach (r in res.added) {
            announce eSpecRevStored, (node = id, doc = dp.doc, rev = r);
          }
          Bump(dp.doc);
        }
        i = i + 1;
      }
      send req.rep, eBulkDocsResp;
    }

    on eGetCheckpoint do (req: (rep: machine, repId: int)) {
      var s: int;
      if (req.repId in checkpoints) {
        s = checkpoints[req.repId];
      }
      send req.rep, eCheckpointResp, (sq = s,);
    }

    on ePutCheckpoint do (req: (rep: machine, repId: int, sq: int)) {
      checkpoints[req.repId] = req.sq;
      send req.rep, ePutCheckpointResp;
    }

    on eDump do (req: (requester: machine)) {
      send req.requester, eDumpResp, (node = id, trees = trees);
    }
  }

  // Move the doc to the newest seq in the by-seq index.
  fun Bump(doc: int) {
    if (doc in docSeq) {
      seqDoc -= (docSeq[doc]);
    }
    updateSeq = updateSeq + 1;
    docSeq[doc] = updateSeq;
    seqDoc[updateSeq] = doc;
    announce eSpecDocSeq, (node = id, doc = doc, sq = updateSeq, leaves = Leaves(trees[doc]));
  }
}
