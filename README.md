# couchdb-rs

A single-node CouchDB server in Rust, wire-compatible with the CouchDB 3.x
HTTP API: documents with MVCC revision trees, attachments, `_changes`,
map/reduce views (JavaScript via embedded QuickJS), and Mango
(`_find` / `_index` / `_explain`). It can act as a replication source or
target using the standard replication protocol.

```bash
cargo build --release
./target/release/couchdb-rs --data-dir ./data --bind 127.0.0.1:5984
```

Optional admin credentials (`--admin user:pass`, or `COUCHDB_USER` /
`COUCHDB_PASSWORD`) turn on Basic and cookie (`/_session`) auth. Without
them the server runs in "admin party" mode.

## Layout, mapped to the Erlang modules it replaces

| Rust module   | CouchDB equivalent | Notes |
|---------------|--------------------|-------|
| `file.rs`     | `couch_file`       | Append-only file, 4 KiB block prefixes so the latest valid header is found by scanning back from EOF after a crash. CRC32 per chunk, snappy compression. |
| `btree.rs`    | `couch_btree`      | Copy-on-write B+tree with stored reductions, `fold`, `reduce_before` (offsets), grouped `fold_reduce`. |
| `keytree.rs`  | `couch_key_tree`   | Revision forests: merge, stemming, leafs, `latest`, purge. |
| `doc.rs`      | `couch_doc`        | JSON parsing/validation, attachments, deterministic rev ids. |
| `db.rs`       | `couch_db`, `couch_db_updater`, `couch_bt_engine` | by-id, by-seq and local-doc trees; one writer thread per DB with group commit; MVCC snapshots for readers; online compaction with catch-up. |
| `server.rs`   | `couch_server`     | DB registry; `/` in DB names maps to subdirectories. |
| `http.rs`, `api.rs` | `chttpd`, `chttpd_db` | Hand-rolled routing like chttpd, so `_design/x`, `_local/x`, `%2F` names and attachment paths parse exactly. |
| `changes.rs`  | `couch_changes`    | normal / longpoll / continuous / eventsource; `_doc_ids`, `_selector`, `_design`, `_view` and JS filters; `style=all_docs`. |
| `collate.rs`  | `couch_ejson_compare` | View collation with the Unicode Collation Algorithm (ICU4X root locale), plus raw collation. |
| `js.rs`       | `couchjs`          | QuickJS: map, reduce/rereduce, `validate_doc_update`, filters, `_show`, `_update`, CommonJS `require`. 5 s timeout per call. |
| `views.rs`    | `couch_mrview`, `couch_index` | Signature-named index files, incremental updates, dup keys, `_sum`/`_count`/`_stats`/`_approx_count_distinct`, `group`/`group_level`, `keys`, linked docs in `include_docs`, `update=false/lazy`. |
| `mango.rs`    | `mango_*`          | Selector parse/normalize/match, JSON indexes stored as `language: "query"` design docs (same format as CouchDB), index selection, sort, bookmarks, `execution_stats`, partial indexes. |

## Tests

```bash
cargo test                                   # unit tests
python3 tests/integration.py http://127.0.0.1:5984   # end-to-end, needs a running server
```

The integration suite covers CRUD and conflicts, replication-style writes
(`new_edits=false`, `_revs_diff`, `_bulk_get`, `open_revs`), a full
replication round trip using the protocol, attachments (inline, stubs,
`atts_since`, multipart/related), `_changes` feeds, views and collation,
`validate_doc_update`, Mango, compaction, show/update functions and
concurrent writers.

## Not implemented

- Clustering (`mem3`/`fabric`): `q`/`n` are accepted and ignored.
- The replicator itself (`/_replicate`, `_replicator` jobs). Use an external
  CouchDB or PouchDB to replicate to or from this server.
- `_users` database authentication and per-DB `_security` enforcement
  (`_security` is stored and returned, but only the server admin is checked).
- `_list` and `_rewrite` (deprecated in CouchDB 3), text/search indexes,
  partitioned databases, attachment gzip encoding, `Range` requests.
- Seqs are opaque strings of the form `N-<db uuid prefix>`; clients that only
  echo seqs back (PouchDB, the CouchDB replicator) are unaffected.
