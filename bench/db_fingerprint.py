#!/usr/bin/env python3
"""Fingerprint a database's logical contents, independent of file layout.

usage: db_fingerprint.py http://user:pass@host:port/db [samples]

Hashes every (id, rev, deleted) from _all_docs, every (seq, id, revs) from
_changes in order, the doc counts, and the full bodies and attachments of a
deterministic sample of docs. Two databases with the same fingerprint hold
the same data; compaction must never change it.
"""
import base64
import hashlib
import json
import random
import sys
import urllib.request

url = sys.argv[1].rstrip("/")
samples = int(sys.argv[2]) if len(sys.argv) > 2 else 300
scheme, rest = url.split("://", 1)
creds, hostpath = rest.split("@", 1) if "@" in rest else ("", rest)
base = f"{scheme}://{hostpath}"
auth = {"Authorization": "Basic " + base64.b64encode(creds.encode()).decode()} if creds else {}


def get(path, raw=False):
    with urllib.request.urlopen(urllib.request.Request(base + path, headers=auth), timeout=3600) as r:
        data = r.read()
    return data if raw else json.loads(data)


info = get("")
h = hashlib.sha256()
all_docs = get("/_all_docs")["rows"]
for row in all_docs:
    h.update(f"{row['id']}\0{row['value']['rev']}\n".encode())
ids_digest = h.hexdigest()

h = hashlib.sha256()
changes = get("/_changes?style=all_docs")["results"]
for c in changes:
    seq = str(c["seq"]).split("-")[0]
    revs = ",".join(sorted(x["rev"] for x in c["changes"]))
    h.update(f"{seq}\0{c['id']}\0{revs}\0{c.get('deleted', False)}\n".encode())
changes_digest = h.hexdigest()

rng = random.Random(7)
pick = rng.sample(all_docs, min(samples, len(all_docs)))
h = hashlib.sha256()
att_bytes = 0
for row in sorted(pick, key=lambda r: r["id"]):
    doc_id = urllib.request.quote(row["id"], safe="")
    doc = get(f"/{doc_id}")
    h.update(json.dumps(doc, sort_keys=True).encode())
    for name in sorted(doc.get("_attachments", {})):
        data = get(f"/{doc_id}/{urllib.request.quote(name, safe='')}", raw=True)
        att_bytes += len(data)
        h.update(hashlib.md5(data).digest())
sample_digest = h.hexdigest()

print(json.dumps({
    "doc_count": info["doc_count"],
    "doc_del_count": info["doc_del_count"],
    "update_seq": str(info["update_seq"]).split("-")[0],
    "all_docs": ids_digest[:16],
    "changes": changes_digest[:16],
    "changes_rows": len(changes),
    "samples": f"{len(pick)} docs, {att_bytes} attachment bytes, {sample_digest[:16]}",
}))
