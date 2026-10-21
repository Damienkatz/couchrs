// Requests and responses, mirroring the HTTP endpoints the replicator and
// clients use. Each request is handled atomically by a Node, which matches
// the single writer thread + snapshot reads of couch_db_updater.

// GET /db/doc
type tGetDocReq = (client: machine, doc: int);
type tGetDocResp = (found: bool, winner: tRev, winnerDeleted: bool, leaves: set[tRev]);
event eGetDoc: tGetDocReq;
event eGetDocResp: tGetDocResp;

// PUT /db/doc (interactive edit, new_edits=true)
type tEditReq = (client: machine, doc: int, hasParent: bool, parent: tRev, rev: tRev, deleted: bool);
event eEdit: tEditReq;
event eEditResp: (ok: bool);

// GET /db/_changes?style=all_docs&since=N
type tChangeRow = (doc: int, sq: int, leaves: set[tRev]);
type tChangesResp = (rows: seq[tChangeRow], lastSeq: int);
event eChanges: (rep: machine, since: int);
event eChangesResp: tChangesResp;

// POST /db/_revs_diff
event eRevsDiff: (rep: machine, revs: map[int, set[tRev]]);
event eRevsDiffResp: (missing: map[int, set[tRev]]);

// POST /db/_bulk_get?revs=true
event eBulkGet: (rep: machine, wanted: map[int, set[tRev]]);
event eBulkGetResp: (docs: seq[tDocPath]);

// POST /db/_bulk_docs {"new_edits": false}; acknowledged only once durable
event eBulkDocs: (rep: machine, docs: seq[tDocPath]);
event eBulkDocsResp;

// GET/PUT /db/_local/<replication id>
event eGetCheckpoint: (rep: machine, repId: int);
event eCheckpointResp: (sq: int);
event ePutCheckpoint: (rep: machine, repId: int, sq: int);
event ePutCheckpointResp;

// Test harness
event eDump: (requester: machine);
event eDumpResp: (node: int, trees: map[int, tTree]);
event eClientDone;
event eRepDone: (rep: machine, transferred: int);

// Observations for the specs
event eSpecRevStored: (node: int, doc: int, rev: tRev);
event eSpecDocSeq: (node: int, doc: int, sq: int, leaves: set[tRev]);
event eSpecCheckpoint: (src: int, tgt: int, sq: int);
event eSpecEditAcked: (doc: int, rev: tRev);
event eSpecFinal: (node: int, trees: map[int, tTree]);
