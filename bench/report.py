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
        "Why couchdb-rs falls off above ~15K clients is not settled. Two suspects were ruled out: taking point "
        "reads off tokio's block_in_place (the hybrid, point reads inline series) changed nothing, and the "
        "numbers were the same with a 29 GB virtual machine running and with it suspended (this run).",
        "What the tests do show: with PUTs only, throughput holds (16.8K req/s at 15K clients, 16.4K at 30K); "
        "with GETs only it falls (22K to 9.5K), while the server stays under one core and answers a fresh "
        "request in about 2 ms. Sampled stacks of both the server and the load generator show their busy "
        "threads inside the kernel's recvfrom and writev with everything else idle, which points at the "
        "macOS network stack carrying tens of thousands of loopback connections. Confirming that needs "
        "clients on separate machines.",
        "CouchDB's HTTP errors from 60K clients up are PUTs that timed out inside CouchDB "
        "(fabric_worker_timeout on update_docs in its log); at 75K and 90K no PUT succeeded. Its memory and "
        "CPU were not captured in this run (the sampler read the previous CouchDB process while it exited).",
    ]
if os.environ.get("REPORT_ENV") == "aws":
    # Concurrent run on AWS: one server node and one load generator per
    # configuration, all in one availability zone.
    data["machine"] = {"cpu": "per pair: i4i.4xlarge server (16 vCPU, local NVMe) + c7i.4xlarge load generator",
                       "ram": "128 GB per server"}
    data["setting"] = ("ran at the same time on AWS (us-east-1a, one cluster placement group), each on its own "
                       "i4i.4xlarge server with its own c7i.4xlarge load generator,")
    data["configs"] = ("All four configurations (CouchDB; couchdb-rs with its B-tree engine; the hybrid, RocksDB "
                       "documents with B-tree views; and the hybrid with point reads run inline)")
    data["lg_note"] = ("Each load generator is its own c7i.4xlarge in the server's availability zone; its six "
                       "private addresses are the six hosts.")
    data["method"] = [
        "Servers: Apache CouchDB 3.5.3 (official apache/couchdb:3.5.3 image, Erlang/OTP 27, QuickJS) and "
        "three couchdb-rs builds, each on its own i4i.4xlarge (16 vCPU, 128 GB RAM, data on the instance's "
        "local NVMe, XFS). Each server had its own c7i.4xlarge load generator; all eight nodes were in "
        "us-east-1a in one cluster placement group, and the four pairs ran at the same time.",
        "Durability: Linux fdatasync for every server (CouchDB's file:datasync, couchdb-rs's sync_data, "
        "RocksDB's WAL sync).",
        "CouchDB: single node, q=1 (one shard, like couchdb-rs), automatic compaction (smoosh) off, connection "
        "cap raised to 200,000, Erlang port limit 262,144, listen backlog 1,024 (tokio's default, which "
        "couchdb-rs uses). All nodes: 2M open files, somaxconn 65,535, local ports 1024-65535.",
        "Dataset, phases and the load generator (bench/couchbench) are the same as the Mac runs: 100 GB, "
        "deterministic from a fixed seed, sizes 100 B to 1 GB, generated text.",
        "Replication used a CouchDB replicator: CouchDB's own for CouchDB, and a separate CouchDB container on "
        "each couchdb-rs server node (port 15984) for the couchdb-rs builds, copying each database to a new "
        "database on the same server.",
        "View queries and the per-size GET/PUT/DELETE tests use the same 16 (or fewer) concurrent clients as "
        "on the Mac, so across a network their rates are bounded by round trips (about 0.5 ms here) as well "
        "as by the server; compare engines within this report rather than against the Mac numbers.",
        "Server memory and CPU come from a monitor on the server node (resident set and, on Linux, "
        "proportional set size; CPU from /proc), matched to each measured window by wall-clock time.",
    ] + [m for m in data["method"] if m.startswith(("couchdb-rs (RocksDB)", "couchdb-rs (RocksDB docs",
                                                     "View compaction streams", "hybrid, point reads",
                                                     "RocksDB's own compression", "View rows are keyed",
                                                     "RocksDB keeps no reductions", "RocksDB compaction"))]
html = open(os.path.join(here, "report_template.html")).read()
payload = json.dumps(data).replace("</", "<\\/")
open(dst, "w").write(html.replace("__DATA__", payload))
print(dst)
