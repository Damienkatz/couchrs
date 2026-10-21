// One-way replication, following couch_replicator's protocol:
//   1. read the checkpoint from _local/<rep id> on the target
//   2. GET source/_changes?style=all_docs&since=checkpoint
//   3. POST target/_revs_diff with every leaf rev seen
//   4. POST source/_bulk_get?revs=true for the missing revs
//   5. POST target/_bulk_docs new_edits=false
//   6. PUT the checkpoint (last_seq from step 2) on the target
// The replicator can crash after any step and restart from its checkpoint.

type tRepConfig = (
  src: machine, tgt: machine, srcId: int, tgtId: int,
  rounds: int, mayCrash: bool,
  // Seeded bug: write the checkpoint before the documents.
  checkpointFirst: bool,
  // Seeded bug: _bulk_get without revs=true, so no ancestry is sent.
  noRevsHistory: bool,
  driver: machine
);

machine Replicator {
  var cfg: tRepConfig;
  var transferred: int;

  start state Init {
    entry (c: tRepConfig) {
      cfg = c;
      goto Running;
    }
  }

  state Running {
    entry {
      var round: int;
      while (round < cfg.rounds) {
        RunOnce();
        round = round + 1;
      }
      send cfg.driver, eRepDone, (rep = this, transferred = transferred);
    }
  }

  fun Crashes(): bool {
    return cfg.mayCrash && choose(5) == 0;
  }

  fun RunOnce() {
    var repId: int;
    var since: int;
    var changes: tChangesResp;
    var revs: map[int, set[tRev]];
    var missing: map[int, set[tRev]];
    var docs: seq[tDocPath];
    var row: tChangeRow;

    repId = cfg.srcId * 10 + cfg.tgtId;
    send cfg.tgt, eGetCheckpoint, (rep = this, repId = repId);
    receive {
      case eCheckpointResp: (r: (sq: int)) { since = r.sq; }
    }
    if (Crashes()) { return; }

    send cfg.src, eChanges, (rep = this, since = since);
    receive {
      case eChangesResp: (r: tChangesResp) { changes = r; }
    }
    if (sizeof(changes.rows) == 0) { return; }
    if (Crashes()) { return; }

    foreach (row in changes.rows) {
      revs[row.doc] = row.leaves;
    }
    send cfg.tgt, eRevsDiff, (rep = this, revs = revs);
    receive {
      case eRevsDiffResp: (r: (missing: map[int, set[tRev]])) { missing = r.missing; }
    }
    if (Crashes()) { return; }

    if (sizeof(missing) > 0) {
      send cfg.src, eBulkGet, (rep = this, wanted = missing);
      receive {
        case eBulkGetResp: (r: (docs: seq[tDocPath])) { docs = r.docs; }
      }
      if (cfg.noRevsHistory) {
        docs = LeafOnly(docs);
      }
      if (Crashes()) { return; }
    }

    if (cfg.checkpointFirst) {
      Checkpoint(repId, changes.lastSeq);
      if (Crashes()) { return; }
      WriteDocs(docs);
    } else {
      WriteDocs(docs);
      if (Crashes()) { return; }
      Checkpoint(repId, changes.lastSeq);
    }
    transferred = transferred + sizeof(docs);
  }

  fun LeafOnly(docs: seq[tDocPath]): seq[tDocPath] {
    var out: seq[tDocPath];
    var dp: tDocPath;
    var p: tPath;
    foreach (dp in docs) {
      p = default(tPath);
      p += (0, dp.path[0]);
      out += (sizeof(out), (doc = dp.doc, path = p));
    }
    return out;
  }

  fun WriteDocs(docs: seq[tDocPath]) {
    if (sizeof(docs) == 0) { return; }
    send cfg.tgt, eBulkDocs, (rep = this, docs = docs);
    receive {
      case eBulkDocsResp: { }
    }
  }

  fun Checkpoint(repId: int, s: int) {
    send cfg.tgt, ePutCheckpoint, (rep = this, repId = repId, sq = s);
    receive {
      case ePutCheckpointResp: { }
    }
    announce eSpecCheckpoint, (src = cfg.srcId, tgt = cfg.tgtId, sq = s);
  }
}
