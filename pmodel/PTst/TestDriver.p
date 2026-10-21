// Two nodes, one client per node editing a shared set of docs, and a
// crash-prone replicator in each direction running concurrently with the
// clients. Once everything stops, a final crash-free A->B then B->A pass
// runs and both nodes are dumped for the Convergence spec.

type tScenario = (docs: int, ops: int, rounds: int, checkpointFirst: bool, noRevsHistory: bool);

fun RunScenario(driver: machine, sc: tScenario) {
  var a: machine;
  var b: machine;
  var pending: int;
  var dumps: int;

  a = new Node(0);
  b = new Node(1);
  new Client((node = a, cid = 1, ops = sc.ops, docs = sc.docs, driver = driver));
  new Client((node = b, cid = 2, ops = sc.ops, docs = sc.docs, driver = driver));
  new Replicator((src = a, tgt = b, srcId = 0, tgtId = 1, rounds = sc.rounds, mayCrash = true,
                  checkpointFirst = sc.checkpointFirst, noRevsHistory = sc.noRevsHistory, driver = driver));
  new Replicator((src = b, tgt = a, srcId = 1, tgtId = 0, rounds = sc.rounds, mayCrash = true,
                  checkpointFirst = sc.checkpointFirst, noRevsHistory = sc.noRevsHistory, driver = driver));
  pending = 4;
  while (pending > 0) {
    receive {
      case eClientDone: { pending = pending - 1; }
      case eRepDone: (r: (rep: machine, transferred: int)) { pending = pending - 1; }
    }
  }

  // Quiesced: one reliable pass each way must make the replicas identical.
  FinalPass(driver, a, b, 0, 1, sc);
  FinalPass(driver, b, a, 1, 0, sc);

  send a, eDump, (requester = driver,);
  send b, eDump, (requester = driver,);
  while (dumps < 2) {
    receive {
      case eDumpResp: (r: (node: int, trees: map[int, tTree])) {
        announce eSpecFinal, r;
        dumps = dumps + 1;
      }
    }
  }
}

fun FinalPass(driver: machine, src: machine, tgt: machine, srcId: int, tgtId: int, sc: tScenario) {
  new Replicator((src = src, tgt = tgt, srcId = srcId, tgtId = tgtId, rounds = 1, mayCrash = false,
                  checkpointFirst = sc.checkpointFirst, noRevsHistory = sc.noRevsHistory, driver = driver));
  receive {
    case eRepDone: (r: (rep: machine, transferred: int)) { }
  }
}

machine TestSmall {
  start state Init {
    entry { RunScenario(this, (docs = 1, ops = 2, rounds = 2, checkpointFirst = false, noRevsHistory = false)); }
  }
}

machine TestConflicts {
  start state Init {
    entry { RunScenario(this, (docs = 2, ops = 4, rounds = 3, checkpointFirst = false, noRevsHistory = false)); }
  }
}

// Seeded bug: the checkpoint is written before the documents. The checker
// should find a schedule where a crash between the two loses revisions.
machine TestCheckpointFirstBug {
  start state Init {
    entry { RunScenario(this, (docs = 2, ops = 3, rounds = 3, checkpointFirst = true, noRevsHistory = false)); }
  }
}

// Seeded bug: replicating leaves without their _revisions history.
machine TestNoRevsHistoryBug {
  start state Init {
    entry { RunScenario(this, (docs = 1, ops = 3, rounds = 2, checkpointFirst = false, noRevsHistory = true)); }
  }
}

module System = { Node, Client, Replicator };

test tcSmall [main = TestSmall]:
  assert CheckpointSafety, Convergence in (union System, { TestSmall });

test tcConflicts [main = TestConflicts]:
  assert CheckpointSafety, Convergence in (union System, { TestConflicts });

test tcCheckpointFirstBug [main = TestCheckpointFirstBug]:
  assert CheckpointSafety, Convergence in (union System, { TestCheckpointFirstBug });

// Same seeded checkpoint bug, judged only by end-to-end convergence: shows
// it causes real data loss, not just a spec technicality.
test tcCheckpointFirstDataLoss [main = TestCheckpointFirstBug]:
  assert Convergence in (union System, { TestCheckpointFirstBug });

test tcNoRevsHistoryBug [main = TestNoRevsHistoryBug]:
  assert Convergence in (union System, { TestNoRevsHistoryBug });

// Reachability: this property is EXPECTED TO FAIL. A counterexample proves
// the checker really explores schedules where conflicts arise and replicate.
test tcReachConflicts [main = TestConflicts]:
  assert ConflictsNeverHappen in (union System, { TestConflicts });
