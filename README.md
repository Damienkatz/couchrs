# CouchRS

CouchRS is Apache CouchDB 3.5 rebuilt in Rust as a single-node server. It
speaks CouchDB's HTTP API and aims to answer every request the way CouchDB
3.5.3 does, down to the bytes of its responses and error messages. Each
feature is checked by sending the same requests to CouchDB 3.5.3 and to
CouchRS and comparing what comes back.

```bash
cargo build --release
./target/release/couchrs --data-dir ./data --admin admin:secret
```

Then open Fauxton, CouchDB's web UI, at http://127.0.0.1:5984/_utils/.

## Running

| Option | Environment | Default |
|---|---|---|
| `--data-dir DIR` | `COUCHDB_DATA_DIR` | `./data` |
| `--bind ADDR:PORT` | `COUCHDB_BIND` | `[chttpd] bind_address` and `port`: `127.0.0.1:5984` |
| `--admin USER:PASS` | `COUCHDB_USER`, `COUCHDB_PASSWORD` | none |
| `--ini FILE\|DIR` (repeatable) | | `DATA_DIR/local.ini` |
| `--fsync-mode fsync\|full` | | `fsync` |

Configuration is layered as in CouchDB: the built-in
`share/default.ini`, then each `--ini` file or directory in order, then
`--bind` and `--admin`, which are never saved. Changes made over the
`_config` API are written to the last ini file. With no admins configured
the server runs in "admin party" mode, as CouchDB does.

`--fsync-mode full` makes view index commits use the platform's strongest
sync (`F_FULLFSYNC` on macOS).

## What it does

- **Documents:**
  - Revision trees, conflicts, `_bulk_docs`, `_bulk_get`, `_revs_diff`, `open_revs`, `_local` docs, `_purge`.
  - Partitioned databases.
  - Integers of any size kept exact.
- **Attachments:** inline, stubs, `atts_since`, multipart/related in and
  out, gzip encoding, `Range` requests, and uploads streamed to storage as
  they arrive.
- **Views:** map/reduce in JavaScript (embedded QuickJS), the built-in
  reduces, `_show`, `_list`, `_update` and `_rewrite`. Indexes update
  incrementally, purges included.
- **Mango:** `_find`, `_index` and `_explain`, with JSON indexes and
  nouveau text indexes.
- **Full-text search:** nouveau (`_nouveau`), with the index server built
  in on tantivy instead of a separate Java server. It's on by default, set
  by `[nouveau] enable`.
- **`_changes`:** normal, longpoll, continuous and eventsource feeds, with
  `_doc_ids`, `_selector`, `_design`, `_view` and JavaScript filters, and
  `_db_updates`.
- **Replication:** `POST /_replicate`, `_replicator` databases and
  `_scheduler`, and acting as a source or target for CouchDB, PouchDB or
  any other replicator.
- **Security:** the `_users` database, Basic, cookie (`_session`), JWT and
  proxy authentication, per-database `_security`, and
  `require_valid_user`.
- **Operations:**
  - `_config`, `_active_tasks`, `_stats`, `_prometheus` and `_system`.
  - Background indexing (ken) and automatic compaction (smoosh).
  - CORS, HTTPS, and Fauxton.

## Storage

- **Databases:** each database is a RocksDB instance.
  - Column families hold the by-id and by-seq indexes, `_local` docs,
    document bodies and attachment chunks.
  - Values are LZ4-compressed by CouchRS, not by RocksDB.
  - All writes go through one writer thread per database. It commits each
    batch with a synced write-ahead log (group commit), and readers work
    from RocksDB snapshots.
  - Compaction drops the bodies of non-leaf revisions and unreferenced
    attachment chunks, then has RocksDB reclaim the space.
- **View indexes:** each design doc signature gets its own append-only
  file of copy-on-write B+trees with stored reductions, as couch_file and
  couch_btree lay them out.
- **Nouveau indexes:** tantivy indexes.

## Layout, mapped to the Erlang modules it replaces

| Rust module | CouchDB equivalent | Notes |
|---|---|---|
| `http.rs`, `reqline.rs` | `chttpd`, mochiweb | Hand-rolled routing, so `_design/x`, `_local/x`, `%2F` names and attachment paths parse as chttpd parses them. Raw quotes in request lines are accepted as mochiweb accepts them. |
| `api.rs` | `chttpd_db`, `chttpd_misc`, `chttpd_show` | Database and document endpoints, and dispatch of `_show`, `_list`, `_update` and `_rewrite`. |
| `server.rs` | `couch_server` | Database registry. `/` in database names maps to subdirectories. |
| `db.rs`, `rstore.rs` | `couch_db`, `couch_db_updater`, `couch_bt_engine` | The RocksDB storage engine. |
| `doc.rs`, `keytree.rs` | `couch_doc`, `couch_key_tree`, `couch_att` | Parsing, validation, JSON rendering, revision ids, revision trees and stemming. |
| `multipart.rs` | `couch_httpd:parse_multipart_request` | Multipart bodies, read as they arrive. |
| `changes.rs` | `couch_changes`, `fabric_view_changes`, `global_changes` | `_changes` and `_db_updates`. |
| `file.rs`, `btree.rs`, `extsort.rs` | `couch_file`, `couch_btree` | View index files: 4 KiB blocks with header prefixes and per-chunk CRC32, copy-on-write B+trees, and external sort for bulk builds. |
| `views.rs` | `couch_mrview`, `couch_index` | Map/reduce indexes and view queries. |
| `rewrite.rs` | `chttpd_rewrite` | `_rewrite` rules and functions. |
| `collate.rs` | `couch_ejson_compare` | View collation with the Unicode Collation Algorithm (ICU4X root locale), plus raw collation. |
| `js.rs` | `couchjs` | Embedded QuickJS for map, reduce, validate_doc_update, filters, shows, lists and updates. 5 s timeout per call. |
| `json.rs` | jiffy | JSON as CouchDB writes it: exact integers, Erlang's shortest floats. |
| `mango/` | `mango` | Selectors, indexes, cursors, options and error messages. |
| `nouveau/` | `nouveau` and its Lucene server | Index definitions, updates, queries and analyzers, on tantivy. |
| `replicator/` | `couch_replicator` | Replication jobs, the scheduler, `_replicator` docs and the HTTP client. |
| `auth.rs`, `passwords.rs`, `jwt.rs` | `couch_httpd_auth`, `chttpd_auth_request`, `couch_passwords`, `jwtf` | Authentication and authorization. |
| `config.rs` | `config` | Layered ini files and the `_config` API. |
| `cors.rs`, `tls.rs` | `chttpd_cors`, chttpd's https listener | CORS and HTTPS. |
| `fauxton.rs` | `chttpd_misc` (`/_utils`) | Fauxton, compiled into the binary. |
| `stats.rs`, `system.rs`, `tasks.rs` | `couch_stats`, `couch_prometheus`, `couch_task_status` | Metrics, `_system`, `_active_tasks`. |
| `ken.rs`, `smoosh.rs` | `ken`, `smoosh` | Background indexing and automatic compaction. |
| `erlpp.rs` | `io_lib_pretty` | Erlang's `~p` layout, for error reasons CouchDB prints as terms. |

## Tests

```bash
cargo test
```

```bash
COUCHDB_CREDS=admin:secret python3 tests/integration.py http://127.0.0.1:5984
```

`cargo test` runs the unit tests. `tests/integration.py` is the end-to-end
suite, and it runs against any server: CouchRS, or a real CouchDB 3.5 to
confirm that a test describes CouchDB's behaviour. It needs only the
Python standard library. `COUCHDB_CREDS` is needed for a server that has
admins. Each test uses its own databases and deletes them afterwards, so
it can run against a shared server.

## Differences from CouchDB

- **Clustering:** none (no `mem3`, `fabric` or `rexi`). A database is one
  shard on one node. `q` and `n` are accepted and ignored, and
  `_membership` lists only this node.
- **Clouseau search:** `_search` answers as CouchDB does when Clouseau
  isn't installed. Use nouveau for full-text search.
- **Sequence numbers:** a seq is `N-` followed by part of the database's
  uuid, rather than CouchDB's encoded clustered seq. Clients that only
  echo seqs back (PouchDB, the replicator) are unaffected. A plain
  integer `since` is also accepted, where CouchDB rejects it.
- **Erlang VM statistics:** in `_system` and `_prometheus`, these are Rust
  stand-ins where one exists (memory, context switches, run queue) and
  zero otherwise.
- **Malformed multipart bodies:** where CouchDB crashes with a 500 and a
  stack trace, CouchRS answers 400. A body with only the JSON part, which
  CouchDB never answers, is accepted.
