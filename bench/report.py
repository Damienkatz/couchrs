#!/usr/bin/env python3
"""Build the comparison report: report.py [results.json] [report.html]

Embeds the benchmark results into report_template.html.
"""
import datetime
import json
import os
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(here, "results.json")
dst = sys.argv[2] if len(sys.argv) > 2 else os.path.join(here, "report.html")


def sysctl(name):
    try:
        return subprocess.check_output(["sysctl", "-n", name], text=True).strip()
    except Exception:
        return ""


results = json.load(open(src))
ram = sysctl("hw.memsize")
data = {
    "results": results,
    "generated": datetime.date.fromtimestamp(os.path.getmtime(src)).isoformat(),
    "machine": {
        "cpu": f"{sysctl('machdep.cpu.brand_string')}, {sysctl('hw.ncpu')} cores",
        "ram": f"{int(ram) / 2**30:.0f} GB" if ram else "",
    },
    "method": [
        "Servers: Apache CouchDB 3.5.3 built from source on Erlang/OTP 27 with the QuickJS engine, "
        "and couchdb-rs (release build). Each ran alone while being measured.",
        "Same durability level: CouchDB on macOS syncs with fsync(), so couchdb-rs ran with plain fsync too. "
        "couchdb-rs can instead use F_FULLFSYNC (--fsync-mode full), which also flushes the drive cache and is slower.",
        "CouchDB was configured as a single node with q=1 (one shard, like couchdb-rs) and with automatic "
        "compaction (smoosh) disabled, since couchdb-rs has none. Both required admin credentials.",
        "Dataset: deterministic from a fixed seed, so both servers received identical documents and ids. "
        "Sizes are log-uniform within each power-of-ten band from 100 B to 1 GB, with the bands getting "
        "1/4/10/15/20/25/25% of the volume. Payloads and attachments are generated text (words drawn at random "
        "from a fixed vocabulary), so they compress roughly the way real documents do.",
        "Compression: couchdb-rs stores document bodies and attachment chunks LZ4-compressed (raw when that "
        "doesn't help). CouchDB compresses bodies with snappy and, because the attachments are text/plain, "
        "gzips attachments at its default level 8.",
        "Sizes: compaction figures use deflated (as stored) bytes for both servers. couchdb-rs's sizes.external "
        "counts uncompressed bytes; CouchDB's counts gzipped attachments, so the report compares file sizes and "
        "the dataset's known uncompressed size instead.",
        "Operation tests use 16 concurrent clients for small docs, fewer for very large ones (8 at 10 MB, 4 at 100 MB, 2 at 1 GB).",
        "Replication used CouchDB's replicator with default settings for both servers, copying each server's "
        "database to a new database on that same server.",
        "The load generator (bench/couchbench) is written in Rust and generates data on the fly, so it never "
        "holds the dataset in memory.",
        "CouchDB with q=1 serializes all reads of the database through one Erlang file process, which is likely "
        "why its large-document reads fall far behind; more shards (q>1) would parallelize them.",
        "couchdb-rs builds a new view index by running the JavaScript map function on up to 12 threads in parallel, "
        "then an external sort and a bottom-up B-tree build, so each node is written once. CouchDB updates its index incrementally even on a first build; on an append-only file every "
        "batch rewrites the B-tree nodes it touches, leaving its 100 GB index file about 91% dead space.",
        "CouchDB's first view request for the 100 GB build hit its request timeout (500 timeout) while indexing "
        "continued; the client kept asking, and build time runs until the index first answered.",
    ],
}
if "rocksdb" in results:
    data["method"] += [
        "couchdb-rs (RocksDB) is the same server with its storage and view-index engines replaced by RocksDB "
        "(librocksdb 11.8 via the rocksdb crate), from the rocksdb-experiment branch. HTTP, document, revision, "
        "replication and query logic are unchanged.",
        "RocksDB's own compression is off everywhere (data blocks, bottommost level, blob files). Bodies and "
        "attachments are LZ4-compressed by couchdb-rs before reaching RocksDB; metadata is stored raw. "
        "Attachment chunks live in RocksDB blob files so LSM compaction does not rewrite them.",
        "Each group commit is one RocksDB WriteBatch with a synced WAL. This RocksDB build syncs with plain "
        "fsync on macOS (no F_FULLFSYNC), the same durability level as the other two servers.",
        "View rows are keyed by an order-preserving byte encoding of the JSON key (Unicode collation sort keys "
        "for strings), so RocksDB's byte order equals CouchDB's view collation. Fresh view builds map in "
        "parallel, sort externally, and ingest SST files.",
        "RocksDB keeps no reductions: doc and row counts are counters, while view offsets and reduce queries "
        "scan rows. couchdb-rs's B-tree stores a reduction in every node instead.",
        "RocksDB compaction (_compact): mark the bodies and chunks of live leaf revisions, delete the rest, then "
        "compact every column family, garbage-collecting blob files.",
    ]
if "hybrid" in results:
    data["method"] += [
        "couchdb-rs (RocksDB docs + B-tree views) keeps the RocksDB document engine and replaces the RocksDB "
        "view index with couchdb-rs's B-tree view engine (append-only index file, a reduction in every node). "
        "Only the view phases were rerun for it, on the RocksDB server's loaded database; its document results "
        "in the summary are the RocksDB engine's, since the document storage code is identical.",
        "View compaction streams each live B-tree, already in key order, into the bottom-up builder, so every "
        "node is written once. couchdb-rs (B-tree) ran main's earlier view compaction, which collects every "
        "live row in memory and inserts them with one B-tree update.",
        "View compaction memory is macOS physical footprint (what Activity Monitor shows), sampled every 0.1 s; "
        "RSS on macOS keeps counting memory malloc has freed but the kernel has not yet reclaimed.",
    ]
if any("scale" in v for v in results.values() if isinstance(v, dict)):
    data["method"] += [
        "Connection scaling (bench/couchbench couchscale): each simulated host is a loopback alias "
        "(127.0.0.2-127.0.0.7), because macOS gives one source address only about 16K ephemeral ports. Every "
        "client holds one keep-alive HTTP/1.1 connection. CouchDB's connection cap (mochiweb max, default "
        "65,536) was raised to 200,000 and its Erlang port limit to 262,144 for this test.",
        "Server memory in the scaling test is macOS physical footprint; CPU is the server's CPU time over the "
        "measured window. The load generator shares the machine's 14 cores with the server.",
        "Why couchdb-rs collapses above ~15K clients: every request runs its database work inside tokio's "
        "block_in_place, and a PUT blocks there until its group commit is durable. Each blocked call hands its "
        "worker's runtime to a thread from tokio's blocking pool, capped at 512; once that is exhausted, sampled "
        "stacks show requests queued on the pool's spawn lock while the server's CPUs sit idle. Neither the "
        "load generator (under 1 core) nor the storage engine was the limit.",
        "CouchDB's step-1 I/O errors are connections it closed during that window, which clients reopened. "
        "At 75K and 90K clients no CouchDB PUT succeeded, which accounts for its HTTP errors there; the test "
        "did not record their status codes.",
    ]
html = open(os.path.join(here, "report_template.html")).read()
payload = json.dumps(data).replace("</", "<\\/")
open(dst, "w").write(html.replace("__DATA__", payload))
print(dst)
