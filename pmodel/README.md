# P model of CouchDB replication

A [P](https://p-org.github.io/P/) model of the CouchDB replication protocol
over MVCC revision trees, as implemented by CouchRS (and CouchDB). The
P checker explores interleavings of concurrent client edits, two-way
replication, and replicator crashes at every step.

```bash
./check.sh            # compile and run every test (needs `dotnet tool install -g P`)
p check -tc tcConflicts -s 20000   # one test, more schedules
```

## What is modelled

| Machine | Models | Notes |
|---------|--------|-------|
| `Node` (`PSrc/Node.p`) | `couch_db` + `couch_db_updater` | Each request is atomic, matching the single writer thread and snapshot reads. Rev trees are flattened to `rev -> parent` maps. The by-seq index moves an updated doc to the newest seq. |
| `Client` (`PSrc/Client.p`) | An application | GET then PUT as separate requests, so concurrent edits produce 409s and edits on different nodes produce conflicts. Edits sometimes target a non-winning leaf, and about one in four edits is a delete. |
| `Replicator` (`PSrc/Replicator.p`) | `couch_replicator` | Reads the checkpoint, then `_changes?style=all_docs`, `_revs_diff`, `_bulk_get?revs=true`, `_bulk_docs new_edits=false`, and finally writes the checkpoint. It can crash after any step and restart from its checkpoint. |
| Rev tree helpers (`PSrc/RevTree.p`) | `couch_key_tree`, `couch_doc` | Leaves, the deterministic winner (live beats deleted, then higher pos, then higher rev id), ancestry paths, and path merge. |

## Properties (`PSpec/Specs.p`)

- **CheckpointSafety**: when a replicator records checkpoint S, every
  revision that was a leaf on the source at any seq ≤ S already exists on
  the target. Otherwise a crash right after the checkpoint loses data
  permanently.
- **Convergence**: after the system quiesces and one crash-free pass runs
  in each direction:
  - both nodes have identical rev trees, deleted flags and winners, with
    no coordination between them;
  - every leaf has its body on both nodes;
  - no acknowledged client edit is missing from either node.

## Tests (`PTst/TestDriver.p`)

| Test | Expectation | What it shows |
|------|-------------|---------------|
| `tcSmall` | 0 bugs | One doc, few operations. |
| `tcConflicts` | 0 bugs | Two docs, four edits per client, three crash-prone replication rounds in each direction. |
| `tcCheckpointFirstBug` | **finds a bug** | Seeded bug: the checkpoint is written before `_bulk_docs`. CheckpointSafety catches it. |
| `tcCheckpointFirstDataLoss` | **finds a bug** | The same seeded bug judged only by Convergence, which shows it loses acknowledged edits end to end. |
| `tcNoRevsHistoryBug` | **finds a bug** | Seeded bug: `_bulk_get` without `revs=true`, so histories never join up. |
| `tcReachConflicts` | **finds a bug** | A reachability check, not a correctness check. Its counterexample proves the checker explores schedules where conflicts actually occur and replicate. |

The seeded-bug and reachability tests show the specs have teeth: a passing
`tcConflicts` is not vacuous.

## Abstractions and limits

- Two nodes, bounded edits and docs. The checker samples random schedules;
  it does not exhaustively prove all of them.
- No stemming (`revs_limit`), compaction, purge, or attachments.
- Node crashes are not modelled; `_bulk_docs` is acknowledged only once
  durable, which CouchRS guarantees by fsyncing before it replies.
- Rev ids are fresh unique integers, so two nodes never independently
  produce the same rev. In real CouchDB identical edits can share a rev id,
  which only makes merging easier.
- `_changes` returns `last_seq` from the same snapshot as its rows, which
  CouchRS does. A server that took `last_seq` from a later snapshot
  could checkpoint past changes it never sent. Modelling that would mean
  splitting the node's changes handler into two steps.
