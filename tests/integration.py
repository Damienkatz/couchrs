#!/usr/bin/env python3
"""End-to-end checks of CouchDB API behaviour against a running server.

usage: python3 tests/integration.py [http://127.0.0.1:5984]
Stdlib only. Each test uses a fresh database. COUCHDB_CREDS=user:pass
sends those credentials with every request (for a server with admins).
"""
import base64
import json
import os
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5984"
CREDS = os.environ.get("COUCHDB_CREDS")


def db_url(name):
    """A database's URL, with the credentials in it, for replications."""
    if not CREDS:
        return f"{BASE}/{name}"
    scheme, rest = BASE.split("://", 1)
    return f"{scheme}://{CREDS}@{rest}/{name}"


def req(method, path, body=None, headers=None, raw=False):
    data = None
    hdrs = {"Accept": "application/json"}
    if CREDS:
        hdrs["Authorization"] = "Basic " + base64.b64encode(CREDS.encode()).decode()
    if headers:
        hdrs.update(headers)
    if body is not None:
        if isinstance(body, (bytes, bytearray)):
            data = bytes(body)
        else:
            data = json.dumps(body).encode()
            hdrs.setdefault("Content-Type", "application/json")
    path = urllib.parse.quote(path, safe="/?&=%:@$+,-_.~")
    r = urllib.request.Request(BASE + path, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            content = resp.read()
            return resp.status, (content if raw else _json(content)), resp.headers
    except urllib.error.HTTPError as e:
        content = e.read()
        return e.code, (content if raw else _json(content)), e.headers


def _json(b):
    try:
        return json.loads(b) if b else None
    except ValueError:
        return b


# Databases the tests made, deleted at the end (whether or not a test
# deleted its own), so a shared server is left as it was.
CREATED = []


def new_db():
    name = "t" + uuid.uuid4().hex[:12]
    s, _, _ = req("PUT", "/" + name)
    assert s == 201, s
    CREATED.append(name)
    return name


def eq(a, b, msg=""):
    assert a == b, f"{msg} expected {b!r}, got {a!r}"


TESTS = []


def test(f):
    TESTS.append(f)
    return f


@test
def server_basics():
    s, b, _ = req("GET", "/")
    eq(s, 200)
    eq(b["couchdb"], "Welcome")
    s, b, _ = req("GET", "/_uuids?count=3")
    eq(len(b["uuids"]), 3)
    s, b, _ = req("PUT", "/BadName")
    eq(s, 400)
    db = new_db()
    s, b, _ = req("PUT", "/" + db)
    eq(s, 412)
    eq(b["error"], "file_exists")
    s, b, _ = req("GET", "/_all_dbs")
    assert db in b
    s, b, _ = req("DELETE", "/" + db)
    eq(s, 200)
    s, b, _ = req("GET", "/" + db)
    eq(s, 404)


@test
def doc_crud_and_conflicts():
    db = new_db()
    s, b, h = req("PUT", f"/{db}/doc1", {"a": 1})
    eq(s, 201)
    rev1 = b["rev"]
    assert rev1.startswith("1-") and len(rev1) == 34
    eq(h["ETag"], f'"{rev1}"')
    s, b, _ = req("PUT", f"/{db}/doc1", {"a": 2})
    eq(s, 409)
    eq(b["error"], "conflict")
    s, b, _ = req("PUT", f"/{db}/doc1", {"_rev": rev1, "a": 2})
    eq(s, 201)
    rev2 = b["rev"]
    s, b, _ = req("GET", f"/{db}/doc1")
    eq(b, {"_id": "doc1", "_rev": rev2, "a": 2})
    s, b, _ = req("GET", f"/{db}/doc1?rev={rev1}")
    eq(b["a"], 1)
    s, b, _ = req("GET", f"/{db}/doc1?revs=true&revs_info=true")
    eq(b["_revisions"]["start"], 2)
    eq(len(b["_revisions"]["ids"]), 2)
    eq([r["status"] for r in b["_revs_info"]], ["available", "available"])
    # deterministic revs: same edit from same parent gives same rev
    db2 = new_db()
    s, b2, _ = req("PUT", f"/{db2}/doc1", {"a": 1})
    eq(b2["rev"], rev1)
    # delete then recreate
    s, b, _ = req("DELETE", f"/{db}/doc1?rev={rev2}")
    eq(s, 200)
    rev3 = b["rev"]
    s, b, _ = req("GET", f"/{db}/doc1")
    eq((s, b["reason"]), (404, "deleted"))
    s, b, _ = req("GET", f"/{db}/doc1?rev={rev3}")
    eq(b["_deleted"], True)
    s, b, _ = req("PUT", f"/{db}/doc1", {"again": True})
    eq(s, 201)
    assert b["rev"].startswith("4-")
    s, b, _ = req("GET", f"/{db}")
    eq(b["doc_count"], 1)
    eq(b["doc_del_count"], 0)
    # POST generates ids
    s, b, _ = req("POST", f"/{db}", {"x": 1})
    eq(s, 201)
    assert len(b["id"]) == 32
    # bad special field
    s, b, _ = req("PUT", f"/{db}/bad", {"_foo": 1})
    eq((s, b["error"]), (400, "doc_validation"))
    s, b, _ = req("PUT", f"/{db}/_bad", {})
    eq(s, 400)


@test
def replication_style_writes():
    db = new_db()
    docs = [
        {"_id": "r", "_rev": "3-c", "_revisions": {"start": 3, "ids": ["c", "b", "a"]}, "v": "c"},
        {"_id": "r", "_rev": "3-z", "_revisions": {"start": 3, "ids": ["z", "y", "a"]}, "v": "z"},
    ]
    s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": docs, "new_edits": False})
    eq((s, b), (201, []))
    s, b, _ = req("GET", f"/{db}/r?conflicts=true")
    eq(b["_rev"], "3-z")  # highest rev id wins
    eq(b["_conflicts"], ["3-c"])
    s, b, _ = req("GET", f"/{db}/r?open_revs=all")
    eq(sorted(x["ok"]["_rev"] for x in b), ["3-c", "3-z"])
    s, b, _ = req("GET", f'/{db}/r?open_revs=["2-b","9-q"]&latest=true')
    eq([x.get("ok", {}).get("_rev") or x.get("missing") for x in b], ["3-c", "9-q"])
    s, b, _ = req("POST", f"/{db}/_revs_diff", {"r": ["3-c", "4-d", "1-a"], "other": ["1-x"]})
    eq(b["r"]["missing"], ["4-d"])
    eq(sorted(b["r"]["possible_ancestors"]), ["3-c", "3-z"])
    eq(b["other"], {"missing": ["1-x"]})
    s, b, _ = req("POST", f"/{db}/_bulk_get?revs=true", {"docs": [{"id": "r", "rev": "3-c"}, {"id": "nope"}]})
    eq(b["results"][0]["docs"][0]["ok"]["_revisions"]["ids"], ["c", "b", "a"])
    assert "error" in b["results"][1]["docs"][0]
    # resolving a conflict by deleting the loser
    s, b, _ = req("DELETE", f"/{db}/r?rev=3-c")
    eq(s, 200)
    s, b, _ = req("GET", f"/{db}/r?conflicts=true&deleted_conflicts=true")
    assert "_conflicts" not in b
    eq(len(b["_deleted_conflicts"]), 1)
    # re-sending an existing rev is a no-op
    s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": docs[:1], "new_edits": False})
    s, info, _ = req("GET", f"/{db}")
    eq(info["update_seq"].split("-")[0], "2")


@test
def bulk_docs_and_all_docs():
    db = new_db()
    docs = [{"_id": f"d{i:03}", "n": i} for i in range(100)]
    s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": docs})
    eq(s, 201)
    eq(len(b), 100)
    assert all(r["ok"] for r in b)
    s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "d000"}, {"_id": "new"}]})
    eq(b[0]["error"], "conflict")
    eq(b[1]["ok"], True)
    s, b, _ = req("GET", f'/{db}/_all_docs?startkey="d010"&endkey="d014"')
    eq([r["id"] for r in b["rows"]], ["d010", "d011", "d012", "d013", "d014"])
    eq(b["total_rows"], 101)
    eq(b["offset"], 10)
    s, b, _ = req("GET", f'/{db}/_all_docs?startkey="d010"&endkey="d014"&inclusive_end=false&descending=false&limit=2&skip=1&include_docs=true')
    eq([r["id"] for r in b["rows"]], ["d011", "d012"])
    eq(b["rows"][0]["doc"]["n"], 11)
    s, b, _ = req("GET", f'/{db}/_all_docs?descending=true&limit=3')
    eq([r["id"] for r in b["rows"]], ["new", "d099", "d098"])
    s, b, _ = req("POST", f"/{db}/_all_docs", {"keys": ["d005", "missing", "d001"]})
    eq(b["rows"][0]["id"], "d005")
    eq(b["rows"][1], {"key": "missing", "error": "not_found"})
    # many updates to force btree splits and still read back correctly
    big = [{"_id": f"big{i:05}", "payload": "x" * 100} for i in range(3000)]
    s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": big})
    s, b, _ = req("GET", f'/{db}/_all_docs?startkey="big01500"&limit=1')
    eq(b["rows"][0]["id"], "big01500")
    eq(b["offset"], 1500)
    s, info, _ = req("GET", f"/{db}")
    eq(info["doc_count"], 3101)


@test
def local_docs():
    db = new_db()
    s, b, _ = req("PUT", f"/{db}/_local/cp", {"seq": 1})
    eq((s, b["rev"]), (201, "0-1"))
    s, b, _ = req("PUT", f"/{db}/_local/cp", {"_rev": "0-1", "seq": 2})
    eq(b["rev"], "0-2")
    # No conflicts: the revision given, plus one (increment_local_doc_revs/1).
    s, b, _ = req("PUT", f"/{db}/_local/cp", {"_rev": "0-1", "seq": 3})
    eq((s, b["rev"]), (201, "0-2"))
    s, b, _ = req("GET", f"/{db}/_local/cp")
    eq(b, {"_id": "_local/cp", "_rev": "0-2", "seq": 3})
    eq(req("PUT", f"/{db}/_local/x", {"_rev": "0-9"})[1]["rev"], "0-10")
    eq(req("PUT", f"/{db}/_local/x", {"_rev": "1-abc"})[:2], (500, {"error": "error", "reason": "Invalid rev format"}))
    eq(req("DELETE", f"/{db}/_local/x")[:2], (200, {"ok": True, "id": "_local/x", "rev": "0-0"}))
    eq(req("DELETE", f"/{db}/_local/x")[:2], (404, {"error": "not_found", "reason": "missing"}))
    eq(req("DELETE", f"/{db}/nope")[:2], (404, {"error": "not_found", "reason": "missing"}))
    s, b, _ = req("GET", f"/{db}/_local_docs")
    eq([r["id"] for r in b["rows"]], ["_local/cp"])
    s, b, _ = req("GET", f"/{db}/_changes")
    eq(b["results"], [])
    s, b, _ = req("DELETE", f"/{db}/_local/cp?rev=0-2")
    eq(s, 200)
    s, b, _ = req("GET", f"/{db}/_local/cp")
    eq(s, 404)


@test
def attachments():
    db = new_db()
    data = b"hello world" * 1000
    s, b, _ = req("PUT", f"/{db}/a/file.txt", data, {"Content-Type": "text/plain"})
    eq(s, 201)
    rev = b["rev"]
    s, body, h = req("GET", f"/{db}/a/file.txt", raw=True)
    eq(body, data)
    eq(h["Content-Type"], "text/plain")
    s, b, _ = req("GET", f"/{db}/a")
    att = b["_attachments"]["file.txt"]
    eq((att["stub"], att["length"], att["revpos"]), (True, len(data), 1))
    # inline attachment + stub preservation on update
    s, b, _ = req("PUT", f"/{db}/a", {"_rev": rev, "k": 1, "_attachments": {
        "file.txt": {"stub": True},
        "inline.bin": {"content_type": "application/octet-stream", "data": base64.b64encode(b"\x00\x01\x02").decode()},
    }})
    eq(s, 201)
    rev2 = b["rev"]
    s, b, _ = req("GET", f"/{db}/a?attachments=true")
    eq(base64.b64decode(b["_attachments"]["inline.bin"]["data"]), b"\x00\x01\x02")
    eq(b["_attachments"]["file.txt"]["revpos"], 1)
    s, b, _ = req("GET", f'/{db}/a?atts_since=["{rev}"]')
    assert "data" in b["_attachments"]["inline.bin"] and "stub" in b["_attachments"]["file.txt"]
    s, b, _ = req("PUT", f"/{db}/a", {"_rev": rev2, "_attachments": {"nope": {"stub": True}}})
    eq((s, b["error"]), (412, "missing_stub"))
    s, b, _ = req("DELETE", f"/{db}/a/file.txt?rev={rev2}")
    eq(s, 200)
    s, b, _ = req("GET", f"/{db}/a/file.txt")
    eq(s, 404)
    # multipart/related upload, as the replicator sends it
    boundary = "abc123"
    doc = {"_id": "m", "_attachments": {"x.txt": {"follows": True, "content_type": "text/plain", "length": 5}}}
    mp = (f"--{boundary}\r\nContent-Type: application/json\r\n\r\n{json.dumps(doc)}\r\n"
          f"--{boundary}\r\nContent-Disposition: attachment; filename=\"x.txt\"\r\n\r\nhello\r\n--{boundary}--").encode()
    s, b, _ = req("PUT", f"/{db}/m", mp, {"Content-Type": f'multipart/related; boundary="{boundary}"'})
    eq(s, 201)
    s, body, _ = req("GET", f"/{db}/m/x.txt", raw=True)
    eq(body, b"hello")


@test
def changes_feed():
    db = new_db()
    for i in range(5):
        req("PUT", f"/{db}/c{i}", {"i": i})
    s, b, _ = req("GET", f"/{db}/_changes")
    eq([r["id"] for r in b["results"]], [f"c{i}" for i in range(5)])
    last = b["last_seq"]
    s, b, _ = req("GET", f"/{db}/_changes?since={b['results'][2]['seq']}&include_docs=true")
    eq([r["id"] for r in b["results"]], ["c3", "c4"])
    eq(b["results"][0]["doc"]["i"], 3)
    s, b, _ = req("GET", f"/{db}/_changes?limit=2")
    eq(len(b["results"]), 2)
    eq(b["pending"], 3)
    # limit=0 must not advance last_seq past changes it didn't return
    s, b0, _ = req("GET", f"/{db}/_changes?limit=0")
    eq((b0["results"], b0["last_seq"].split("-")[0]), ([], "0"))
    s, b0, _ = req("GET", f"/{db}/_changes?limit=0&since={b['last_seq']}")
    eq(b0["last_seq"].split("-")[0], b["last_seq"].split("-")[0])
    s, b, _ = req("GET", f"/{db}/_changes?descending=true&limit=1")
    eq(b["results"][0]["id"], "c4")
    s, b, _ = req("POST", f"/{db}/_changes?filter=_doc_ids", {"doc_ids": ["c1", "c3"]})
    eq([r["id"] for r in b["results"]], ["c1", "c3"])
    s, b, _ = req("POST", f"/{db}/_changes?filter=_selector", {"selector": {"i": {"$gte": 3}}})
    eq([r["id"] for r in b["results"]], ["c3", "c4"])
    # an update moves the doc to the end of the feed
    _, d, _ = req("GET", f"/{db}/c0")
    req("PUT", f"/{db}/c0", {"_rev": d["_rev"], "i": 0})
    s, b, _ = req("GET", f"/{db}/_changes")
    eq([r["id"] for r in b["results"]], ["c1", "c2", "c3", "c4", "c0"])
    # longpoll wakes on a write
    result = {}

    def poll():
        result["r"] = req("GET", f"/{db}/_changes?feed=longpoll&since=now&timeout=10000")

    t = threading.Thread(target=poll)
    t.start()
    time.sleep(0.3)
    req("PUT", f"/{db}/late", {})
    t.join(15)
    eq([r["id"] for r in result["r"][1]["results"]], ["late"])
    # continuous with timeout ends with last_seq
    creds = {"Authorization": "Basic " + base64.b64encode(CREDS.encode()).decode()} if CREDS else {}
    r = urllib.request.urlopen(urllib.request.Request(BASE + f"/{db}/_changes?feed=continuous&timeout=300", headers=creds), timeout=10)
    lines = [json.loads(l) for l in r.read().decode().splitlines() if l.strip()]
    eq(lines[-1].keys() >= {"last_seq"}, True)
    eq(len(lines), 7)
    # JS filter
    req("PUT", f"/{db}/_design/f", {"filters": {"odd": "function(doc, req) { return doc.i % 2 == 1; }"}})
    s, b, _ = req("GET", f"/{db}/_changes?filter=f/odd")
    eq(sorted(r["id"] for r in b["results"]), ["c1", "c3"])


@test
def views():
    db = new_db()
    docs = [{"_id": f"p{i}", "type": "post", "author": a, "words": w, "tags": t}
            for i, (a, w, t) in enumerate([("bob", 10, ["a"]), ("alice", 20, ["a", "b"]), ("bob", 5, []),
                                          ("carol", 7, ["b"]), ("alice", 3, ["c"])])]
    req("POST", f"/{db}/_bulk_docs", {"docs": docs})
    ddoc = {"views": {
        "by_author": {"map": "function(doc) { if (doc.type == 'post') emit(doc.author, doc.words); }", "reduce": "_sum"},
        "by_tag": {"map": "function(doc) { (doc.tags||[]).forEach(function(t) { emit([t, doc.author], 1); }); }", "reduce": "_count"},
        "stats": {"map": "function(doc) { if (doc.words !== undefined) emit(null, doc.words); }", "reduce": "_stats"},
        "js_red": {"map": "function(doc) { emit(doc.author, doc.words); }",
                   "reduce": "function(k, v, re) { return Math.max.apply(null, v); }"},
        "dup": {"map": "function(doc) { emit(1, 1); emit(1, 2); }"},
        "collate": {"map": "function(doc) { if (doc.k !== undefined) emit(doc.k, null); }"},
        "lib": {"x": "exports.y = 2;"},
        "req": {"map": "function(doc) { if (doc.type) emit(require('views/lib/x').y, null); }"},
    }}
    s, b, _ = req("PUT", f"/{db}/_design/v", ddoc)
    eq(s, 201)
    s, b, _ = req("GET", f"/{db}/_design/v/_view/by_author?reduce=false")
    eq([(r["key"], r["value"]) for r in b["rows"]],
       [("alice", 20), ("alice", 3), ("bob", 10), ("bob", 5), ("carol", 7)])
    eq(b["total_rows"], 5)
    s, b, _ = req("GET", f"/{db}/_design/v/_view/by_author")
    eq(b["rows"], [{"key": None, "value": 45}])
    s, b, _ = req("GET", f"/{db}/_design/v/_view/by_author?group=true")
    eq([(r["key"], r["value"]) for r in b["rows"]], [("alice", 23), ("bob", 15), ("carol", 7)])
    s, b, _ = req("GET", f'/{db}/_design/v/_view/by_author?key="bob"&reduce=false&include_docs=true')
    eq([r["doc"]["_id"] for r in b["rows"]], ["p0", "p2"])
    s, b, _ = req("GET", f"/{db}/_design/v/_view/by_tag?group_level=1")
    eq([(r["key"], r["value"]) for r in b["rows"]], [(["a"], 2), (["b"], 2), (["c"], 1)])
    s, b, _ = req("GET", f'/{db}/_design/v/_view/by_tag?reduce=false&startkey=["b"]&endkey=["b",{{}}]')
    eq([r["key"] for r in b["rows"]], [["b", "alice"], ["b", "carol"]])
    s, b, _ = req("GET", f"/{db}/_design/v/_view/stats")
    eq(b["rows"][0]["value"], {"sum": 45, "count": 5, "min": 3, "max": 20, "sumsqr": 583})
    s, b, _ = req("GET", f"/{db}/_design/v/_view/js_red?group=true")
    eq([(r["key"], r["value"]) for r in b["rows"]], [("alice", 20), ("bob", 10), ("carol", 7)])
    s, b, _ = req("GET", f"/{db}/_design/v/_view/dup?limit=4")
    eq([r["value"] for r in b["rows"]], [1, 2, 1, 2])
    s, b, _ = req("GET", f"/{db}/_design/v/_view/req?limit=1")
    eq(b["rows"][0]["key"], 2)
    s, b, _ = req("POST", f"/{db}/_design/v/_view/by_author", {"keys": ["carol", "alice"], "reduce": False})
    eq([r["id"] for r in b["rows"]], ["p3", "p1", "p4"])
    s, b, _ = req("GET", f"/{db}/_design/v/_view/by_author?descending=true&reduce=false&limit=2")
    eq([r["key"] for r in b["rows"]], ["carol", "bob"])
    # incremental update after edits and deletes
    _, d, _ = req("GET", f"/{db}/p3")
    req("DELETE", f"/{db}/p3?rev={d['_rev']}")
    req("PUT", f"/{db}/p9", {"type": "post", "author": "dave", "words": 1})
    s, b, _ = req("GET", f"/{db}/_design/v/_view/by_author?group=true")
    eq([(r["key"], r["value"]) for r in b["rows"]], [("alice", 23), ("bob", 15), ("dave", 1)])
    # stale/update=false does not update
    req("PUT", f"/{db}/p10", {"type": "post", "author": "eve", "words": 1})
    s, b, _ = req("GET", f"/{db}/_design/v/_view/by_author?group=true&update=false")
    eq(len(b["rows"]), 3)
    # CouchDB collation
    keys = [None, False, True, 1, 2, "a", "A", "aa", "b", "B", ["a"], ["b"], {"a": 1}]
    req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": f"k{i}", "k": k} for i, k in enumerate(reversed(keys))]})
    s, b, _ = req("GET", f"/{db}/_design/v/_view/collate")
    eq([r["key"] for r in b["rows"]], keys)
    s, b, _ = req("GET", f"/{db}/_design/v/_info")
    eq(b["name"], "v")
    s, b, _ = req("GET", f"/{db}/_design/v/_view/missing")
    eq(s, 404)
    # A value _stats can't take fails the index, every view of it (the
    # B-tree's reduce throws); _sum's errors are values.
    req("PUT", f"/{db}/_design/bad", {"views": {"s": {"map": "function(doc) { if (doc.k !== undefined) emit(doc._id, doc.k); }", "reduce": "_stats"},
                                                "m": {"map": "function(doc) { emit(1, 1); }"}}})
    for path in ["s", "m", "s?reduce=false"]:
        s, b, _ = req("GET", f"/{db}/_design/bad/_view/{path}")
        eq((s, b["error"]), (500, "invalid_value"), path)
    req("PUT", f"/{db}/_design/sum", {"views": {"s": {"map": "function(doc) { if (doc.k !== undefined) emit(doc._id, doc.k); }", "reduce": "_sum"}}})
    s, b, _ = req("GET", f"/{db}/_design/sum/_view/s")
    eq((s, b["rows"][0]["value"]["error"]), (200, "builtin_reduce_error"))
    s, b, _ = req("GET", f"/{db}/_design/v/_view/dup?group=true")
    eq(s, 400)
    # view survives a ddoc-unrelated restart of the index (same signature)
    s, b, _ = req("POST", f"/{db}/_view_cleanup", {})
    eq(s, 202)


@test
def validate_doc_update():
    db = new_db()
    req("PUT", f"/{db}/_design/auth", {"validate_doc_update":
        "function(n, o, ctx, sec) { if (!n.name) throw({forbidden: 'name required'}); "
        "if (o && o.locked) throw({unauthorized: 'locked'}); }"})
    s, b, _ = req("PUT", f"/{db}/x", {"a": 1})
    eq((s, b["error"], b["reason"]), (403, "forbidden", "name required"))
    s, b, _ = req("PUT", f"/{db}/x", {"name": "ok", "locked": True})
    eq(s, 201)
    s, b, _ = req("PUT", f"/{db}/x", {"_rev": b["rev"], "name": "again"})
    eq(s, 401)
    s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "y", "name": "y"}, {"_id": "z"}]})
    eq(b[0]["ok"], True)
    eq(b[1]["error"], "forbidden")


@test
def mango():
    db = new_db()
    try:
        people = [{"_id": f"u{i}", "name": n, "age": a, "city": c, "pets": p}
                  for i, (n, a, c, p) in enumerate([("ann", 31, "nyc", ["cat"]), ("bob", 25, "sf", []),
                                                    ("cat", 42, "nyc", ["dog", "cat"]), ("dan", 19, "la", ["fish"]),
                                                    ("eve", 25, "sf", ["dog"])])]
        req("POST", f"/{db}/_bulk_docs", {"docs": people})
        s, b, _ = req("POST", f"/{db}/_find", {"selector": {"age": {"$gt": 24}}, "fields": ["_id"]})
        eq(sorted(d["_id"] for d in b["docs"]), ["u0", "u1", "u2", "u4"])
        assert "warning" in b
        s, b, _ = req("POST", f"/{db}/_index", {"index": {"fields": ["age"]}, "name": "age-idx", "ddoc": "ages"})
        eq((b["result"], b["id"]), ("created", "_design/ages"))
        s, b, _ = req("POST", f"/{db}/_index", {"index": {"fields": ["age"]}, "name": "age-idx", "ddoc": "ages"})
        eq(b["result"], "exists")
        s, b, _ = req("POST", f"/{db}/_find", {"selector": {"age": {"$gte": 25}}, "sort": [{"age": "desc"}],
                                               "fields": ["name", "age"], "execution_stats": True})
        eq([d["name"] for d in b["docs"]][:2], ["cat", "ann"])
        eq(len(b["docs"]), 4)
        assert "warning" not in b
        eq(b["execution_stats"]["results_returned"], 4)
        s, b, _ = req("POST", f"/{db}/_explain", {"selector": {"age": {"$gte": 25}}})
        eq(b["index"]["name"], "age-idx")
        s, b, _ = req("POST", f"/{db}/_find", {"selector": {"pets": {"$elemMatch": {"$eq": "dog"}}, "city": "nyc"}})
        eq([d["_id"] for d in b["docs"]], ["u2"])
        s, b, _ = req("POST", f"/{db}/_find", {"selector": {"name": {"$gt": None}}, "sort": ["name"]})
        eq(s, 400)
        eq(b["error"], "no_usable_index")
        # pagination with bookmarks
        s, b1, _ = req("POST", f"/{db}/_find", {"selector": {"age": {"$gt": 0}}, "sort": [{"age": "asc"}], "limit": 2})
        s, b2, _ = req("POST", f"/{db}/_find", {"selector": {"age": {"$gt": 0}}, "sort": [{"age": "asc"}], "limit": 2,
                                                "bookmark": b1["bookmark"]})
        s, b3, _ = req("POST", f"/{db}/_find", {"selector": {"age": {"$gt": 0}}, "sort": [{"age": "asc"}], "limit": 2,
                                                "bookmark": b2["bookmark"]})
        ages = [d["age"] for d in b1["docs"] + b2["docs"] + b3["docs"]]
        eq(ages, [19, 25, 25, 31, 42])
        s, b, _ = req("GET", f"/{db}/_index")
        eq([i["name"] for i in b["indexes"]], ["_all_docs", "age-idx"])
        s, b, _ = req("DELETE", f"/{db}/_index/_design/ages/json/age-idx")
        eq(s, 200)
        s, b, _ = req("GET", f"/{db}/_index")
        eq(b["total_rows"], 1)
        # _id range on the special index
        s, b, _ = req("POST", f"/{db}/_find", {"selector": {"_id": {"$gt": "u2"}}, "fields": ["_id"]})
        eq([d["_id"] for d in b["docs"]], ["u3", "u4"])
    finally:
        req("DELETE", f"/{db}")


@test
def mango_35():
    """Mango as CouchDB 3.5 has it: the index chosen and why (explain),
    fallback, covering indexes, bookmarks as term_to_binary writes them,
    statistics as shard and coordinator count them, and mango_error's
    messages, Erlang term formatting and all."""
    db = new_db()
    find = lambda body: req("POST", f"/{db}/_find", body)
    explain = lambda body: req("POST", f"/{db}/_explain", body)[1]
    mids = lambda b: [d["_id"] for d in b["docs"]]
    try:
        docs = [{"_id": f"d{i:02}", "a": i, "b": i % 3, "c": {"d": i % 4}} for i in range(12)]
        eq(req("POST", f"/{db}/_bulk_docs", {"docs": docs})[0], 201)
        # Names derive from the definition and options (gen_name/2).
        eq(req("POST", f"/{db}/_index", {"index": {"fields": ["a"]}})[1],
           {"result": "created", "id": "_design/7095fd2321a986309496ad2ac9c3498246175757", "name": "7095fd2321a986309496ad2ac9c3498246175757"})
        eq(req("POST", f"/{db}/_index", {"index": {"fields": ["a", "b"]}, "ddoc": "ab"})[1]["name"], "c9960e6f9106f2e6dc9f3a2e480cd379e12dd8f9")
        eq(req("POST", f"/{db}/_index", {"index": {"fields": ["b"]}, "name": "bi", "ddoc": "bdd"})[1]["result"], "created")
        pidx = {"index": {"fields": ["c.d"], "partial_filter_selector": {"a": {"$gt": 1}}}, "name": "cp", "ddoc": "cp"}
        eq(req("POST", f"/{db}/_index", pidx)[1]["result"], "created")
        view = req("GET", f"/{db}/_design/cp")[1]["views"]["cp"]
        eq(view, {"map": {"fields": {"c.d": "asc"}, "partial_filter_selector": {"a": {"$gt": 1}}}, "reduce": "_count",
                  "options": {"def": {"fields": ["c.d"], "partial_filter_selector": {"a": {"$gt": 1}}}}})

        ex = explain({"selector": {"a": {"$gt": 1}, "b": 2}, "fields": ["a", "_id"], "sort": [{"a": "desc"}], "r": 2, "stale": "ok"})
        eq(ex["index"]["ddoc"], "_design/ab")
        eq(ex["opts"], {"use_index": [], "bookmark": "nil", "limit": 25, "skip": 0, "sort": {"a": "desc"}, "fields": ["a", "_id"],
                        "partition": "", "r": 2, "conflicts": False, "stale": "ok", "update": True, "stable": False,
                        "execution_stats": False, "allow_fallback": True})
        eq(ex["mrargs"], {"include_docs": False, "view_type": "map", "reduce": False, "partition": None, "start_key": ["<MAX>"],
                          "end_key": [1, 2], "direction": "rev", "stable": True, "update": False, "conflicts": "undefined"})
        eq(ex["covering"], True)
        eq([(c["index"]["name"], c["analysis"]) for c in ex["index_candidates"]], [
            ("cp", {"usable": False, "reasons": [{"name": "is_partial"}], "ranking": 3, "covering": False}),
            ("bi", {"usable": False, "reasons": [{"name": "sort_order_mismatch"}], "ranking": 2, "covering": False}),
            ("7095fd2321a986309496ad2ac9c3498246175757", {"usable": True, "reasons": [{"name": "less_overlap"}], "ranking": 1, "covering": True}),
            ("_all_docs", {"usable": False, "reasons": [{"name": "field_mismatch"}], "ranking": 2, "covering": None})])
        eq([h for h in ex["selector_hints"] if h["type"] == "json"], [{"type": "json", "indexable_fields": ["a", "b"], "unindexable_fields": []}])
        ex = explain({"selector": {"z": 1}})
        eq((ex["index"]["name"], ex["mrargs"]["start_key"], ex["mrargs"]["end_key"]), ("_all_docs", None, "<MAX>"))
        eq(explain({"selector": {"$and": []}})["mrargs"]["start_key"], None)

        # use_index that can't serve: a warning, or without fallback, an error.
        b = find({"selector": {"b": 1}, "use_index": "cp"})[1]
        eq((mids(b), b["warning"]), (["d01", "d04", "d07", "d10"], "_design/cp was not used because it does not contain a valid index for this query."))
        eq(find({"selector": {"b": 1}, "use_index": ["cp", "x"], "allow_fallback": False})[:2], (400, {"error": "invalid_index",
           "reason": "_design/cp, x specified by `use_index` could not be found or it is not suitable."}))
        eq(find({"selector": {"z": 1}, "allow_fallback": False})[:2],
           (400, {"error": "invalid_index", "reason": "You must specify an index with the `use_index` parameter."}))
        eq(find({"selector": {"s": 1}, "sort": ["s"]})[:2],
           (400, {"error": "no_usable_index", "reason": "No index exists for this sort, try indexing by the sort fields."}))

        # The scan stops at the first match past the limit; a covering
        # index reads no documents.
        b = find({"selector": {"a": {"$gt": 1}}, "limit": 2, "execution_stats": True, "fields": ["_id", "a"]})[1]
        eq((b["docs"], b["execution_stats"]["total_keys_examined"], b["execution_stats"]["total_docs_examined"]),
           ([{"_id": "d02", "a": 2}, {"_id": "d03", "a": 3}], 4, 0))
        b = find({"selector": {"a": {"$gt": 1}}, "r": 2, "limit": 1, "execution_stats": True})[1]
        eq((b["execution_stats"]["total_docs_examined"], b["execution_stats"]["total_quorum_docs_examined"]), (0, 3))
        # _all_docs drops the design docs that match, and what was counted for them.
        b = find({"selector": {"_id": {"$gt": None}}, "execution_stats": True, "fields": ["_id"]})[1]
        eq((len(b["docs"]), b["execution_stats"]["total_keys_examined"]), (12, 12))

        # Paging: the bookmark is the last row's key and id.
        b1 = find({"selector": {"a": {"$gte": 3}}, "limit": 3})[1]
        eq((mids(b1), b1["bookmark"]), (["d03", "d04", "d05"], "g2wAAAACaAJkAA5zdGFydGtleV9kb2NpZG0AAAADZDA1aAJkAAhzdGFydGtleWsAAQVq"))
        b2 = find({"selector": {"a": {"$gte": 3}}, "limit": 3, "bookmark": b1["bookmark"], "execution_stats": True})[1]
        eq((mids(b2), b2["execution_stats"]["total_keys_examined"]), (["d06", "d07", "d08"], 4))
        b = find({"selector": {"_id": {"$gt": "d05"}}, "limit": 2})[1]
        eq(b["bookmark"], "g1AAAAA2eJzLYWBgYMpgSmHgKy5JLCrJTq2MT8lPzkzJBYozpxiYg6Q4YFIwwSwAhAoOUQ")
        eq(mids(find({"selector": {"_id": {"$gt": "d05"}}, "limit": 2, "bookmark": b["bookmark"]})[1]), ["d08", "d09"])

        # The response as mango_httpd streams it.
        eq(req("POST", f"/{db}/_find", {"selector": {"a": {"$gt": 100}}}, raw=True)[1], b'{"docs":[\r\n],\r\n"bookmark": "nil"}\r\n\n')
        eq(req("POST", f"/{db}/_find", {"selector": {"$or": []}}, raw=True)[1], b'{"docs":[\r\n]}\r\n\n')
        raw = req("POST", f"/{db}/_find", {"selector": {"a": 3}, "fields": ["a"]}, raw=True)[1]
        assert raw.startswith(b'{"docs":[\r\n{"a":3}\r\n],\r\n"bookmark": "g2wAAAACaAJk') and raw.endswith(b'"}\r\n\n'), raw

        errors = [
            ({"selector": {"b": 1}, "bogus": 1}, 400, "invalid_key", "Invalid key bogus for this request."),
            ({"selector": [1]}, 400, "invalid_selector_json", "Selector must be a JSON object, not: [1]"),
            ({"selector": {"b": 1}, "skip": "x"}, 400, "invalid_non_neg_integer", "<<120>> is not an integer greater than or equal to zero"),
            ({"selector": {"b": 1}, "limit": 1.5}, 400, "invalid_non_neg_integer", "1.5 is not an integer greater than or equal to zero"),
            ({"selector": {"b": 1}, "sort": [{"a": "up"}]}, 400, "invalid_sort_field", 'Invalid sort field: {[{<<"a">>,<<"up">>}]}'),
            ({"selector": {"b": 1}, "sort": "x"}, 400, "invalid_sort_json", 'Sort must be an array of sort specs, not: <<"x">>'),
            ({"selector": {"b": 1}, "fields": [1]}, 400, "invalid_field", "Invalid JSON for field spec: 1"),
            ({"selector": {"b": 1}, "use_index": "a/b/c/d"}, 400, "invalid_index_name", "Invalid index name: <<97,47,98,47,99,47,100>>"),
            ({"selector": {"a": {"$in": {"x": 1}}}}, 400, "bad_arg", "Bad argument for operator $in: {[{<<120>>,1}]}"),
            ({"selector": {"a": {"$size": 1.0}}}, 400, "bad_arg", "Bad argument for operator $size: 1.0"),
            ({"selector": {"a": {"$default": 1}}}, 400, "bad_field", "Invalid field normalization on selector: {[{<<36,100,101,102,97,117,108,116>>,1}]}"),
            ({"selector": {"$eq": 1}}, 400, "invalid_selector", "One or more conditions is missing a field name."),
            ({"selector": {"a..b": 1}}, 400, "invalid_field_name", "Invalid field name: a..b"),
            ({"selector": {"b": 1}, "bookmark": "xyz"}, 400, "invalid_bookmark", 'Invalid bookmark value: "xyz"'),
            ({"selector": {"b": 1}, "partition": "x"}, 400, "query_parse_error", "`partition` parameter is not supported on this db"),
            ({"selector": {"b": 1}, "stale": "x"}, 500, "unknown_error", "Unknown Error: mango_opts :: {invalid_ok_or_false_value,<<120>>}"),
            ({"selector": {"b": 1}, "bookmark": 5}, 500, "unknown_error", "Unknown Error: mango_opts :: {invalid_bookmark,5}"),
            ({"selector": {"a": {"$mod": [0, 1]}}}, 500, "unknown_error", "badarith"),
            # mango_idx_view:is_text_search/1 has no clause for null.
            ({"selector": {"a": {"$in": [None]}}}, 500, "unknown_error", "function_clause"),
        ]
        for body, status, error, reason in errors:
            s, b, _ = find(body)
            eq((s, b["error"], b["reason"], "ref" in b), (status, error, reason, status == 500), json.dumps(body))

        # Creating and deleting indexes.
        eq(req("POST", f"/{db}/_index", dict(pidx, name="cp2"))[1]["result"], "created")
        eq(req("POST", f"/{db}/_index", dict(pidx, name="cp2"))[1]["result"], "exists")
        eq(req("POST", f"/{db}/_index", {"index": {"fields": ["z"]}, "ddoc": "cp", "name": "z", "partitioned": False})[:2],
           (400, {"error": "invalid_partitioned_option", "reason": "Requested partitioned option does not match existing value on design document _design/cp"}))
        eq(req("PUT", f"/{db}/_design/js", {"views": {"v": {"map": "function(d){}"}}})[0], 201)
        eq(req("POST", f"/{db}/_index", {"index": {"fields": ["a"]}, "ddoc": "js"})[:2],
           (400, {"error": "invalid_ddoc_lang", "reason": "Existing design doc has an invalid language: false"}))
        for body, error, reason in [
            ({"index": [1]}, "invalid_index", "JSON indexes must be an object, not: [1]"),
            ({"index": {"fields": ["a"], "selector": {}}}, "invalid_key", "Invalid key selector for this request."),
            ({"index": {}}, "missing_required_key", "Missing required key: fields"),
            ({"index": {"fields": ["a"]}, "type": 5}, "invalid_string", "Invalid string: 5"),
            ({"index": {"fields": ["a"]}, "type": "geo"}, "invalid_index", "Invalid type for index: geo"),
            ({"index": {"fields": ["a"]}, "name": ""}, "invalid_empty_string", "Index name or ddoc cannot be empty string"),
            ({"index": {"fields": ["a"]}, "ddoc": "_design/"}, "illegal_docid", "Illegal document id `_design/`"),
        ]:
            eq(req("POST", f"/{db}/_index", body)[:2], (400, {"error": error, "reason": reason}), json.dumps(body))
        # mochiweb separates query parameters by ";" too.
        r = urllib.request.Request(f"{BASE}/{db}/_index?limit=2;skip=1", headers={"Authorization": "Basic " + base64.b64encode(CREDS.encode()).decode()} if CREDS else {})
        with urllib.request.urlopen(r, timeout=30) as resp:
            listed = json.loads(resp.read())["indexes"]
        eq([i["name"] for i in listed], ["7095fd2321a986309496ad2ac9c3498246175757", "c9960e6f9106f2e6dc9f3a2e480cd379e12dd8f9"])
        s, b, _ = req("GET", f"/{db}/_index?limit=0")
        eq((s, b["error"], b["reason"]), (500, "invalid_list_index_params", "Index parameter ranges: limit > 1, skip > 0"))
        eq(req("DELETE", f"/{db}/_index/cp/json/cp")[:2], (200, {"ok": True}))
        # Each design doc's one index, the results in reverse.
        eq(req("POST", f"/{db}/_index/_bulk_delete", {"docids": ["bdd", "nope", "cp"]})[1],
           {"success": [{"id": "_design/cp", "ok": True}, {"id": "_design/bdd", "ok": True}], "fail": [{"id": "_design/nope", "error": "not_found"}]})
        eq((req("GET", f"/{db}/_design/bdd")[0], req("GET", f"/{db}/_design/cp")[0]), (404, 404))
        for path in ["a", "a/b", "a/b/c/d"]:
            eq(req("PUT", f"/{db}/_index/{path}", {})[:2], (404, {"error": "not_found", "reason": "missing"}))
        eq(req("GET", f"/{db}/_index/x/json/y")[0], 405)
    finally:
        req("DELETE", f"/{db}")


@test
def encoding_parity():
    """JSON and terms as CouchDB writes them: floats in Erlang's shortest
    form, lists of bytes as strings in revision hashes; and the documents
    a view's map function sees have their conflicts."""
    db = new_db()
    try:
        s, b, _ = req("PUT", f"/{db}/f", {"v": [1000.0, 100.0, 1e21, 0.00001, 120000.0, 0.002, 1.5], "a": [1, 2, 3], "b": [300, "s"]})
        eq(b["rev"], "1-aa6405c3ac2f779bee1b86d51c1876c1")
        raw = req("GET", f"/{db}/f", raw=True)[1]
        assert b'"v":[1.0e3,100.0,1.0e21,1.0e-5,1.2e5,0.002,1.5]' in raw, raw
        eq(req("POST", f"/{db}", {"_id": "_design/"})[:2], (400, {"error": "illegal_docid", "reason": "Illegal document id `_design/`"}))
        eq(req("PUT", f"/{db}/_design/", {})[:2], (400, {"error": "illegal_docid", "reason": "Only reserved document ids may start with underscore."}))
        s, b, _ = req("PUT", f"/{db}/_revs_limit", "x")
        eq((s, b["error"], b["reason"]), (500, "unknown_error", "function_clause"))
        req("PUT", f"/{db}/c", {"x": 1})
        req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "c", "_rev": "1-23202479633c2b380f79507a776743d5", "x": 2}], "new_edits": False})
        ddoc = {"views": {"conf": {"map": "function(doc) { if (doc._conflicts) emit(doc._id, doc._conflicts.length); }"}}}
        eq(req("PUT", f"/{db}/_design/v", ddoc)[0], 201)
        eq(req("GET", f"/{db}/_design/v/_view/conf")[1]["rows"], [{"id": "c", "key": "c", "value": 1}])
    finally:
        req("DELETE", f"/{db}")


@test
def exact_numbers():
    """Integers exact whatever their size, compared with floats by value
    as Erlang does (not as doubles), in Mango and in view collation."""
    db = new_db()
    try:
        big = 123456789012345678901234567890123456789
        req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "a", "n": 9007199254740993}, {"_id": "b", "n": 9007199254740992},
                                                    {"_id": "c", "n": 1000000000000000000001}, {"_id": "d", "n": 0.30000000000000004},
                                                    {"_id": "e", "n": big}]})
        assert b'"n":123456789012345678901234567890123456789' in req("GET", f"/{db}/e", raw=True)[1]
        def ids(sel):
            return [d["_id"] for d in req("POST", f"/{db}/_find", {"selector": {"n": sel}, "fields": ["_id"]})[1]["docs"]]
        eq(ids({"$gt": 9007199254740992.0}), ["a", "c", "e"])
        eq(ids({"$eq": 9007199254740992.0}), ["b"])
        eq(ids({"$gt": 1e21}), ["c", "e"])
        eq(ids({"$lt": 1e21}), ["a", "b", "d"])
        eq(ids({"$eq": big}), ["e"])
        eq(ids({"$gt": 0.3, "$lt": 0.31}), ["d"])
        # JS prints 1e21 as 1e+21, a float to jiffy; the integer key just
        # above it is excluded.
        req("PUT", f"/{db}/_design/v", {"views": {"k": {"map": "function(d) { if (d._id == 'a') { emit(1e21, 1); emit(2e21, 2); } }"}}})
        rows = req("GET", f"/{db}/_design/v/_view/k?startkey=1000000000000000000001")[1]["rows"]
        eq([r["value"] for r in rows], [2])
        rows = req("GET", f"/{db}/_design/v/_view/k?key=1000000000000000000000")[1]["rows"]
        eq([r["value"] for r in rows], [1])
    finally:
        req("DELETE", f"/{db}")


@test
def purges_update_views_incrementally():
    """Purged docs leave view indexes without a rebuild: rows of docs gone
    are dropped, a doc that keeps revisions is mapped again, and each
    index keeps a purge checkpoint that compaction respects."""
    # One shard, so CouchDB's seqs are the database's.
    db = "t" + uuid.uuid4().hex[:12]
    eq(req("PUT", f"/{db}?q=1")[0], 201)
    CREATED.append(db)
    try:
        _, res, _ = req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": f"d{i:04}", "n": i} for i in range(1000)] + [{"_id": "c", "n": -1}]})
        revs = {r["id"]: r["rev"] for r in res}
        req("POST", f"/{db}/_bulk_docs", {"new_edits": False, "docs": [{"_id": "c", "_rev": "1-zzzz", "n": -2}]})
        req("PUT", f"/{db}/_design/v", {"views": {"n": {"map": "function(d) { emit(d.n, d._rev); }", "reduce": "_count"}}})
        sig = req("GET", f"/{db}/_design/v/_info")[1]["view_index"]["signature"]
        cp = f"/{db}/_local/purge-mrview-{sig}"
        s, b, _ = req("GET", cp)
        eq((s, b["_rev"], b["type"], b["purge_seq"], b["ddoc_id"], b["signature"]), (200, "0-1", "mrview", 0, "_design/v", sig))
        eq(req("GET", f"/{db}/_design/v/_view/n?reduce=false&limit=1")[1]["total_rows"], 1001)
        stat = lambda: req("GET", "/_node/_local/_stats/couchdb/mrview/map_doc")[1]["value"]
        mapped = stat()
        _, b, _ = req("POST", f"/{db}/_purge", {"d0005": [revs["d0005"]], "c": ["1-zzzz"], "nope": ["1-x"]})
        eq(b["purged"], {"d0005": [revs["d0005"]], "c": ["1-zzzz"], "nope": []})
        info = req("GET", f"/{db}")[1]
        eq((info["purge_seq"].split("-")[0], info["update_seq"].split("-")[0]), ("3", "1004"))
        eq(req("GET", f"/{db}/c?local_seq=true")[1]["_local_seq"], 1004)
        b = req("GET", f"/{db}/_design/v/_view/n?reduce=false&limit=2")[1]
        eq((b["total_rows"], [(r["id"], r["key"]) for r in b["rows"]]), (1000, [("c", -1), ("d0000", 0)]))
        assert stat() - mapped < 10, "purge rebuilt the index"
        s, b, _ = req("GET", cp)
        eq((b["_rev"], b["purge_seq"]), ("0-2", 3))
        eq(req("GET", f"/{db}/_purged_infos")[1], {"purged_infos": [{"id": "c", "revs": ["1-zzzz"]}, {"id": "d0005", "revs": [revs["d0005"]]}, {"id": "nope", "revs": ["1-x"]}]})
        eq(req("GET", f"/{db}/_local_docs")[1]["rows"], [])
        eq([r["id"] for r in req("GET", f"/{db}/_local_docs?include_system=true")[1]["rows"]], [cp.split("/", 2)[2]])
        # Compaction keeps the infos past the limit an index still needs.
        eq(req("PUT", f"/{db}/_purged_infos_limit", "0")[:2], (400, {"error": "bad_request", "reason": "`purged_infos_limit` must be positive integer"}))
        eq(req("PUT", f"/{db}/_purged_infos_limit", 2)[0], 200)
        eq(req("GET", f"/{db}/_purged_infos_limit")[1], 2)
        req("POST", f"/{db}/_purge", {"x1": ["1-a"], "x2": ["1-b"], "x3": ["1-c"]})
        eq(req("GET", f"/{db}/_design/v/_view/n")[1]["rows"], [{"key": None, "value": 1000}])
        # Compaction gives every design doc's view index its checkpoint,
        # opened or not.
        req("PUT", f"/{db}/_design/u", {"autoupdate": False, "views": {"m": {"map": "function(d) { emit(d._id, null); }"}}})
        eq(len(req("GET", f"/{db}/_local_docs?include_system=true")[1]["rows"]), 1)
        req("POST", f"/{db}/_compact", {})
        for _ in range(100):
            if not req("GET", f"/{db}")[1]["compact_running"]:
                break
            time.sleep(0.1)
        time.sleep(0.5)
        eq([i["id"] for i in req("GET", f"/{db}/_purged_infos")[1]["purged_infos"]], ["x1", "x2"])
        eq([r["value"]["rev"] for r in req("GET", f"/{db}/_local_docs?include_system=true")[1]["rows"]], ["0-1", "0-3"])
        # A deleted design doc's checkpoint goes with its index.
        req("DELETE", f"/{db}/_design/v?rev={req('GET', f'/{db}/_design/v')[1]['_rev']}")
        req("POST", f"/{db}/_view_cleanup", {})
        time.sleep(0.5)
        eq(req("GET", cp)[0], 404)
    finally:
        req("DELETE", f"/{db}")


@test
def view_reads_while_rebuilding():
    """Queries that don't wait for an index update (update=false, and
    update=lazy, which starts one) keep reading the index as it was while
    purges are applied."""
    db = new_db()
    try:
        _, res, _ = req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": f"d{i:04}", "n": i, "pad": "x" * 40} for i in range(3000)]})
        revs = {r["id"]: r["rev"] for r in res}
        req("PUT", f"/{db}/_design/v", {"views": {"by_n": {"map": "function(d) { emit(d.n, d.pad); }"}}})
        eq(req("GET", f"/{db}/_design/v/_view/by_n?limit=1")[1]["total_rows"], 3000)
        for i in range(8):
            eq(req("POST", f"/{db}/_purge", {f"d{i:04}": [revs[f"d{i:04}"]]})[0], 201)
            for q in ("stable=true&update=lazy&limit=1", "update=false&skip=2000&limit=2"):
                s, b, _ = req("GET", f"/{db}/_design/v/_view/by_n?{q}")
                eq(s, 200, f"{q}: {b}")
                assert b["rows"], b
        b = req("GET", f"/{db}/_design/v/_view/by_n?limit=1")[1]
        eq((b["total_rows"], b["rows"][0]["key"]), (2992, 8))
    finally:
        req("DELETE", f"/{db}")


@test
def all_docs_keys():
    """keys= on _local_docs, _design_docs and _all_docs (GET, POST and
    /queries) as fabric_view_all_docs looks them up, and keys with a
    range as couch_mrview validates them."""
    db = new_db()
    try:
        for i, v in (("_local/a", 1), ("_local/b", 2), ("_local/gone", 3)):
            req("PUT", f"/{db}/{i}", {"v": v})
        req("DELETE", f"/{db}/_local/gone")
        req("PUT", f"/{db}/d1", {"n": 1})
        req("PUT", f"/{db}/_design/x", {"views": {"n": {"map": "function(d) { if (d.n) emit(d.n, 1); }", "reduce": "_count"}}})
        keys = ["_local/a", "_local/missing", "d1", 1, None, "_local/b", "_local/gone", "_design/x"]
        q = lambda v: urllib.parse.quote(json.dumps(v))
        row = lambda i, v: {"id": i, "key": i, "value": {"rev": "0-1"}, "doc": {"_id": i, "_rev": "0-1", "v": v}}
        missing = lambda k: {"key": k, "error": "not_found"}
        want = [row("_local/a", 1), missing("_local/missing"), row("_local/b", 2), missing("_local/gone")]
        b = req("GET", f"/{db}/_local_docs?keys={q(keys)}&include_docs=true&update_seq=true")[1]
        eq(b, {"total_rows": None, "offset": None, "update_seq": None, "rows": want})
        eq(req("POST", f"/{db}/_local_docs", {"keys": keys, "include_docs": True})[1]["rows"], want)
        # Reversed, skipped and limited before other namespaces' keys go.
        eq([r["key"] for r in req("GET", f"/{db}/_local_docs?keys={q(keys)}&descending=true&skip=1&limit=4")[1]["rows"]], ["_local/gone", "_local/b"])
        b = req("POST", f"/{db}/_local_docs/queries", {"queries": [{"keys": ["_local/b", "_local/a"]}, {"keys": ["_local/a"]}]})[1]
        eq([[r["id"] for r in res["rows"]] for res in b["results"]], [["_local/b", "_local/a"], ["_local/a"]])
        b = req("GET", f"/{db}/_design_docs?keys={q(keys)}")[1]
        eq((b["total_rows"], [r["id"] for r in b["rows"]]), (1, ["_design/x"]))
        b = req("GET", f"/{db}/_all_docs?keys={q(['_local/a', 'd1'])}")[1]
        eq([r["value"]["rev"][:2] for r in b["rows"]], ["0-", "1-"])
        # Errors.
        eq(req("GET", f"/{db}/_local_docs?keys={q('x')}")[:2], (400, {"error": "bad_request", "reason": "`keys` parameter must be an array."}))
        eq(req("POST", f"/{db}/_all_docs", {"keys": "x"})[:2], (400, {"error": "bad_request", "reason": "`keys` body member must be an array."}))
        incompatible = (400, {"error": "query_parse_error", "reason": "`keys` is incompatible with `key`, `start_key` and `end_key`"})
        eq(req("GET", f"/{db}/_local_docs?keys={q(['_local/a'])}&startkey={q('_local/z')}")[:2], incompatible)
        eq(req("GET", f"/{db}/_all_docs?keys={q(['d1'])}&key={q('d1')}")[0], 200)
        eq(req("GET", f"/{db}/_design/x/_view/n?keys={q([1, 2])}&key=1&reduce=false")[:2], incompatible)
        eq(req("GET", f"/{db}/_design/x/_view/n?keys={q([1, 2])}&group_level=1")[1]["reason"], "Multi-key fetches for reduce views must use `group=true`")
        # A view's single key is a range of it, so it reduces without group.
        eq(req("GET", f"/{db}/_design/x/_view/n?keys={q([1])}")[1]["rows"], [{"key": None, "value": 1}])
        eq(req("GET", f"/{db}/_design/x/_view/n?keys={q([1])}&reduce=false&inclusive_end=false")[1]["rows"], [])
    finally:
        req("DELETE", f"/{db}")


@test
def compaction_preserves_data():
    db = new_db()
    for i in range(20):
        s, b, _ = req("PUT", f"/{db}/doc", {"i": i, "_rev": b["rev"]} if i else {"i": i})
    req("PUT", f"/{db}/att/f", b"x" * 100000, {"Content-Type": "application/octet-stream"})
    req("PUT", f"/{db}/_local/l", {"keep": True})
    _, before, _ = req("GET", f"/{db}")
    s, b, _ = req("POST", f"/{db}/_compact", {})
    eq(s, 202)
    for _ in range(100):
        _, info, _ = req("GET", f"/{db}")
        if not info["compact_running"]:
            break
        time.sleep(0.05)
    assert info["sizes"]["file"] < before["sizes"]["file"], (info["sizes"], before["sizes"])
    s, b, _ = req("GET", f"/{db}/doc?revs_info=true")
    eq(b["i"], 19)
    eq(b["_revs_info"][1]["status"], "missing")
    s, body, _ = req("GET", f"/{db}/att/f", raw=True)
    eq(len(body), 100000)
    s, b, _ = req("GET", f"/{db}/_local/l")
    eq(b["keep"], True)
    s, b, _ = req("PUT", f"/{db}/after", {})
    eq(s, 201)


@test
def show_and_update_functions():
    db = new_db()
    req("PUT", f"/{db}/_design/fn", {
        "shows": {"hello": "function(doc, req) { return {body: 'Hi ' + (doc ? doc.name : req.query.n)}; }"},
        "updates": {"bump": "function(doc, req) { if (!doc) doc = {_id: req.id, n: 0}; doc.n++; return [doc, 'n=' + doc.n]; }"},
    })
    req("PUT", f"/{db}/x", {"name": "X"})
    s, body, _ = req("GET", f"/{db}/_design/fn/_show/hello/x", raw=True)
    eq(body, b"Hi X")
    s, body, _ = req("POST", f"/{db}/_design/fn/_update/bump/ctr", raw=True)
    s, body, _ = req("POST", f"/{db}/_design/fn/_update/bump/ctr", raw=True)
    eq(body, b"n=2")


@test
def config_api():
    sec = "test_" + uuid.uuid4().hex[:8]
    c = "/_node/_local/_config"
    s, b, _ = req("GET", "/_node/_local")
    eq((s, b["name"]), (200, "couchdb@127.0.0.1"))
    s, b, _ = req("GET", "/_node/nobody@nowhere/_config")
    eq((s, b), (404, {"error": "not_found", "reason": "no such node: nobody@nowhere"}))
    s, all_cfg, _ = req("GET", c)
    eq(s, 200)
    assert "chttpd" in all_cfg, sorted(all_cfg)
    eq(req("GET", f"{c}/{sec}")[:2], (200, {}))
    eq(req("GET", f"{c}/{sec}/k")[:2], (404, {"error": "not_found", "reason": "unknown_config_value"}))
    # Values are JSON strings, trimmed; PUT and DELETE return the old value.
    eq(req("PUT", f"{c}/{sec}/k", b"123")[:2], (400, {"error": "bad_request", "reason": "a JSON string expected"}))
    eq(req("PUT", f"{c}/{sec}/k", "  v1  ")[:2], (200, ""))
    eq(req("GET", f"{c}/{sec}/k")[:2], (200, "v1"))
    eq(req("PUT", f"{c}/{sec}/k", "v2")[:2], (200, "v1"))
    eq(req("GET", f"{c}/{sec}")[:2], (200, {"k": "v2"}))
    eq(req("GET", c)[1][sec], {"k": "v2"})
    eq(req("PUT", f"{c}/{sec}/a%5Bb", "x")[:2], (400, {"error": "bad_request", "reason": "Invalid configuration key"}))
    eq(req("PUT", f"{c}/{sec}/k3", "a\nb")[:2], (400, {"error": "bad_request", "reason": "Invalid configuration value"}))
    eq(req("PUT", f"{c}/query_servers/js", "x")[:2],
       (403, {"error": "forbidden", "reason": "Config section blacklisted for modification over HTTP API."}))
    # An unsaved value is gone after _reload; a saved one isn't.
    eq(req("PUT", f"{c}/{sec}/temp", "x", headers={"X-Couch-Persist": "false"})[:2], (200, ""))
    eq(req("GET", f"{c}/{sec}/temp")[:2], (200, "x"))
    eq(req("POST", f"{c}/_reload")[:2], (200, {"ok": True}))
    eq(req("GET", f"{c}/{sec}/temp")[0], 404)
    eq(req("GET", f"{c}/{sec}/k")[:2], (200, "v2"))
    eq(req("DELETE", f"{c}/{sec}/k")[:2], (200, "v2"))
    eq(req("DELETE", f"{c}/{sec}/k")[:2], (404, {"error": "not_found", "reason": "unknown_config_value"}))
    eq(req("GET", f"{c}/{sec}")[:2], (200, {}))
    for path, method, allowed in [(c, "POST", "GET"), (f"{c}/{sec}", "POST", "GET"),
                                  (f"{c}/{sec}/k", "POST", "GET,PUT,DELETE"), (f"{c}/_reload", "GET", "POST")]:
        eq(req(method, path)[:2], (405, {"error": "method_not_allowed", "reason": f"Only {allowed} allowed"}), path)
    eq(req("GET", f"{c}/a/b/c")[:2], (404, {"error": "not_found", "reason": "missing"}))

    # A new admin's password is stored hashed and works for login.
    name = "admin_" + uuid.uuid4().hex[:8]
    auth = {"Authorization": "Basic " + base64.b64encode(f"{name}:pw-{name}".encode()).decode()}
    eq(req("PUT", f"{c}/admins/{name}", f"pw-{name}")[:2], (200, ""))
    try:
        # The first admin ends admin party, so ask as the new admin. CouchDB
        # hashes the password in the background and refuses the login until
        # it has.
        stored = None
        for _ in range(50):
            s, stored, _ = req("GET", f"{c}/admins/{name}", headers=auth)
            if s == 200 and stored.startswith("-pbkdf2:"):
                break
            time.sleep(0.1)
        assert isinstance(stored, str) and stored.startswith("-pbkdf2:sha256-") and stored.endswith(",600000"), stored
        derived, salt, iters = stored[len("-pbkdf2:sha256-"):].split(",")
        import hashlib
        eq(hashlib.pbkdf2_hmac("sha256", f"pw-{name}".encode(), salt.encode(), int(iters)).hex(), derived)
        eq(req("GET", "/_all_dbs", headers=auth)[0], 200)
    finally:
        eq(req("DELETE", f"{c}/admins/{name}", headers=auth)[0], 200)


def basic(user, pw):
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()}


ANON = {"Authorization": ""}


@test
def users_and_security():
    x = uuid.uuid4().hex[:8]
    c = "/_node/_local/_config"
    sa, sapw = f"sa_{x}", f"pw_{x}"
    ADMIN = basic(sa, sapw)
    eq(req("PUT", f"{c}/admins/{sa}", sapw)[0], 200)
    try:
        for _ in range(50):  # CouchDB hashes new admins in the background
            if req("GET", "/_session", headers=ADMIN)[1].get("userCtx", {}).get("name") == sa:
                break
            time.sleep(0.1)
        _users_and_security(x, ADMIN)
    finally:
        req("DELETE", f"{c}/admins/{sa}", headers=ADMIN)


def _users_and_security(x, ADMIN):
    db, bob, eve = f"sec_{x}", f"bob_{x}", f"eve_{x}"
    BOB, EVE = basic(bob, "pw"), basic(eve, "pw")

    def r(method, path, body=None, who=ADMIN):
        s, b, h = req(method, path, body, headers=who)
        return s, b

    not_admin = {"error": "unauthorized", "reason": "You are not a server admin."}
    eq(r("PUT", f"/{db}"), (201, {"ok": True}))
    try:
        eq(r("GET", f"/{db}/_security"), (200, {"members": {"roles": ["_admin"]}, "admins": {"roles": ["_admin"]}}))
        eq(r("GET", f"/{db}", who=ANON), (401, {"error": "unauthorized", "reason": "You are not authorized to access this db."}))
        for name, roles in [(bob, []), (eve, ["boss"])]:
            s, b = r("PUT", f"/_users/org.couchdb.user:{name}",
                     {"name": name, "password": "pw", "roles": roles, "type": "user"})
            eq(s, 201, name)
        # Passwords are stored hashed.
        s, doc = r("GET", f"/_users/org.couchdb.user:{bob}")
        assert "password" not in doc and doc["password_scheme"] == "pbkdf2", doc
        assert doc["iterations"] == 600000 and len(doc["derived_key"]) == 64 and doc["pbkdf2_prf"] == "sha256", doc
        # The users db is admin-only by default, even for one's own doc.
        eq(r("GET", f"/_users/org.couchdb.user:{bob}", who=BOB),
           (403, {"error": "forbidden", "reason": "You are not allowed to access this db."}))
        eq(r("GET", "/_users/_all_docs", who=BOB), (401, not_admin))
        eq(r("GET", "/_all_dbs", who=BOB), (401, not_admin))
        s, b = r("GET", "/_session", who=BOB)
        eq((b["userCtx"], b["info"]["authenticated"]), ({"name": bob, "roles": []}, "default"))
        eq(r("GET", "/_session", who=EVE)[1]["userCtx"], {"name": eve, "roles": ["boss"]})
        eq(r("GET", "/_session", who=basic(bob, "nope")), (401, {"error": "unauthorized", "reason": "Name or password is incorrect."}))
        eq(r("GET", f"/{db}", who=BOB), (403, {"error": "forbidden", "reason": "You are not allowed to access this db."}))

        # Members by name, db admins by role.
        sec = {"members": {"names": [bob]}, "admins": {"roles": ["boss"]}}
        eq(r("PUT", f"/{db}/_security", sec), (200, {"ok": True}))
        eq(r("GET", f"/{db}", who=BOB)[0], 200)
        eq(r("PUT", f"/{db}/d1", {"a": 1}, who=BOB)[0], 201)
        eq(r("PUT", f"/{db}/_design/x", {"views": {}}, who=BOB),
           (403, {"error": "forbidden", "reason": "You are not a db or server admin."}))
        eq(r("GET", f"/{db}/_security", who=BOB), (200, sec))
        eq(r("POST", f"/{db}/_compact", {}, who=BOB),
           (401, {"error": "unauthorized", "reason": "You are not a server or db admin."}))
        eq(r("GET", f"/{db}", who=EVE)[0], 200)
        eq(r("POST", f"/{db}/_compact", {}, who=EVE), (202, {"ok": True}))
        eq(r("POST", f"/{db}/_purge", {}, who=EVE), (401, not_admin))
        eq(r("DELETE", f"/{db}", who=EVE), (401, not_admin))
        eq(r("GET", f"/{db}", who=ANON), (401, {"error": "unauthorized", "reason": "You are not authorized to access this db."}))

        # validate_doc_update sees the real user.
        vdu = "function(n, o, u) { if (u.name !== '%s' && u.roles.indexOf('_admin') === -1) throw({forbidden: 'only bob'}); }" % bob
        eq(r("PUT", f"/{db}/_design/v", {"validate_doc_update": vdu}, who=EVE)[0], 201)
        eq(r("PUT", f"/{db}/d2", {"a": 1}, who=EVE), (403, {"error": "forbidden", "reason": "only bob"}))
        eq(r("PUT", f"/{db}/d2", {"a": 1}, who=BOB)[0], 201)
        vrev = r("GET", f"/{db}/_design/v", who=EVE)[1]["_rev"]
        eq(r("DELETE", f"/{db}/_design/v?rev={vrev}", who=EVE)[0], 200)

        # An open database: anyone may read and write documents, but design
        # documents still need a db admin.
        eq(r("PUT", f"/{db}/_security", {}), (200, {"ok": True}))
        eq(r("GET", f"/{db}", who=ANON)[0], 200)
        eq(r("PUT", f"/{db}/d3", {"a": 1}, who=ANON)[0], 201)
        eq(r("PUT", f"/{db}/_design/y", {"views": {}}, who=ANON),
           (401, {"error": "unauthorized", "reason": "You are not a db or server admin."}))

        # The users db validator and security.
        eq(r("PUT", "/_users/org.couchdb.user:" + bob + "x", {"name": bob + "x", "password": "p", "roles": []}),
           (403, {"error": "forbidden", "reason": "doc.type must be user"}))
        eq(r("PUT", "/_users/org.couchdb.user:" + bob + "x", {"name": "other", "password": "p", "roles": [], "type": "user"}),
           (403, {"error": "forbidden", "reason": "Doc ID must be of the form org.couchdb.user:name"}))
        eq(r("PUT", "/_users/_security", {}),
           (403, {"error": "forbidden", "reason": "You can't edit the security object of the user database."}))

        # Server-level rules for anonymous users.
        eq(r("GET", "/_active_tasks", who=ANON), (401, not_admin))
        eq(r("GET", "/_uuids", who=ANON)[0], 200)
        eq(r("PUT", f"/{db}_anon", who=ANON), (401, not_admin))
        eq(r("POST", "/_replicate", {}, who=ANON), (401, {"error": "unauthorized", "reason": "You are not an authenticated user"}))
        s, b = r("GET", "/_session", who=ANON)
        eq((s, b["userCtx"], "authenticated" in b["info"]), (200, {"name": None, "roles": []}, False))

        # Cookie login.
        s, b, h = req("POST", "/_session", {"name": bob, "password": "pw"}, headers=ANON)
        eq((s, b), (200, {"ok": True, "name": bob, "roles": []}))
        cookie = h["Set-Cookie"].split(";")[0]
        assert cookie.startswith("AuthSession=") and len(cookie) > 20, h["Set-Cookie"]
        assert "HttpOnly" in h["Set-Cookie"] and "Path=/" in h["Set-Cookie"], h["Set-Cookie"]
        s, b, _ = req("GET", "/_session", headers={"Authorization": "", "Cookie": cookie})
        eq((b["userCtx"]["name"], b["info"]["authenticated"]), (bob, "cookie"))
        eq(req("GET", f"/{db}", headers={"Authorization": "", "Cookie": cookie})[0], 200)
        s, b, h = req("POST", "/_session", {"name": bob, "password": "wrong"}, headers=ANON)
        eq((s, b["reason"]), (401, "Name or password is incorrect."))
        assert h["Set-Cookie"].startswith("AuthSession=;"), h["Set-Cookie"]
        s, b, h = req("DELETE", "/_session", headers={"Authorization": "", "Cookie": cookie})
        eq((s, b), (200, {"ok": True}))
        assert h["Set-Cookie"].startswith("AuthSession=;"), h["Set-Cookie"]
    finally:
        r("DELETE", f"/{db}")
        for name in (bob, eve):
            s, d = r("GET", f"/_users/org.couchdb.user:{name}")
            if s == 200:
                r("DELETE", f"/_users/org.couchdb.user:{name}?rev={d['_rev']}")


def _b64url(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _jwt(header, claims, sign):
    msg = _b64url(json.dumps(header).encode()) + "." + _b64url(json.dumps(claims).encode())
    return msg + "." + _b64url(sign(msg.encode()))


@test
def proxy_and_jwt_auth():
    import hashlib, hmac as hmac_mod
    c = "/_node/_local/_config"
    x = uuid.uuid4().hex[:8]
    sa, sapw = f"sa_{x}", f"pw_{x}"
    ADMIN = basic(sa, sapw)
    nopersist = {"X-Couch-Persist": "false"}
    eq(req("PUT", f"{c}/admins/{sa}", sapw)[0], 200)
    changed = []

    def setc(sec, key, val):
        eq(req("PUT", f"{c}/{sec}/{key}", val, headers={**ADMIN, **nopersist})[0], 200, (sec, key))
        changed.append((sec, key))

    try:
        for _ in range(50):
            if req("GET", "/_session", headers=ADMIN)[1].get("userCtx", {}).get("name") == sa:
                break
            time.sleep(0.1)
        setc("chttpd", "authentication_handlers", "{chttpd_auth, proxy_authentication_handler}, "
             "{chttpd_auth, jwt_authentication_handler}, {chttpd_auth, cookie_authentication_handler}, "
             "{chttpd_auth, default_authentication_handler}")
        time.sleep(0.5)
        # CouchDB reads authentication_handlers only when chttpd starts.
        if "proxy" not in req("GET", "/_session", headers=ADMIN)[1]["info"]["authentication_handlers"]:
            print("     (authentication_handlers not changed at runtime: skipped)")
            return

        def session(headers):
            s, b, _ = req("GET", "/_session", headers={"Authorization": "", **headers})
            return s, b

        # Proxy authentication, without and then with the shared secret.
        s, b = session({"X-Auth-CouchDB-UserName": f"px_{x}", "X-Auth-CouchDB-Roles": "r1, r2"})
        eq((s, b["userCtx"], b["info"]["authenticated"]), (200, {"name": f"px_{x}", "roles": ["r1", "r2"]}, "proxy"))
        eq(b["info"]["authentication_handlers"], ["proxy", "jwt", "cookie", "default"])
        setc("chttpd_auth", "proxy_use_secret", "true")
        secret = req("GET", f"{c}/chttpd_auth/secret", headers=ADMIN)[1]
        token = hmac_mod.new(secret.encode(), f"px_{x}".encode(), hashlib.sha256).hexdigest()
        eq(session({"X-Auth-CouchDB-UserName": f"px_{x}", "X-Auth-CouchDB-Token": token})[1]["userCtx"]["name"], f"px_{x}")
        eq(session({"X-Auth-CouchDB-UserName": f"px_{x}", "X-Auth-CouchDB-Token": "00" * 32})[1]["userCtx"]["name"], None)

        # JWT with an HMAC key.
        key = b"jwt-secret-" + x.encode()
        setc("jwt_keys", "hmac:_default", base64.b64encode(key).decode())
        hs = lambda m: hmac_mod.new(key, m, hashlib.sha256).digest()
        now = int(time.time())
        good = _jwt({"alg": "HS256", "typ": "JWT"}, {"sub": f"jw_{x}", "exp": now + 60, "_couchdb.roles": ["a", "b"]}, hs)
        s, b = session({"Authorization": "Bearer " + good})
        eq((s, b["userCtx"], b["info"]["authenticated"]), (200, {"name": f"jw_{x}", "roles": ["a", "b"]}, "jwt"))
        expired = _jwt({"alg": "HS256"}, {"sub": "j", "exp": now - 10}, hs)
        eq(session({"Authorization": "Bearer " + expired}), (401, {"error": "unauthorized", "reason": "exp not in future"}))
        badsig = _jwt({"alg": "HS256"}, {"sub": "j", "exp": now + 60}, lambda m: b"x" * 32)
        eq(session({"Authorization": "Bearer " + badsig}), (400, {"error": "bad_request", "reason": "Bad HMAC in authentication token"}))
        nosub = _jwt({"alg": "HS256"}, {"exp": now + 60}, hs)
        eq(session({"Authorization": "Bearer " + nosub}), (401, {"error": "unauthorized", "reason": "Token missing sub claim."}))
        unknown = _jwt({"alg": "HS256", "kid": "nokey"}, {"sub": "j", "exp": now + 60}, hs)
        eq(session({"Authorization": "Bearer " + unknown}), (400, {"error": "bad_request", "reason": "Unknown kid"}))
        eq(session({"Authorization": "Bearer a.b"}), (400, {"error": "bad_request", "reason": "Malformed authentication token"}))

        # RSA and EC keys.
        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
            from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
        except ImportError:
            return

        def pem(pub):
            p = pub.public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
            return p.decode().replace("\n", "\\n")

        rk = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        setc("jwt_keys", "rsa:rk", pem(rk.public_key()))
        rs = _jwt({"alg": "RS256", "kid": "rk"}, {"sub": f"rs_{x}", "exp": now + 60},
                  lambda m: rk.sign(m, padding.PKCS1v15(), hashes.SHA256()))
        eq(session({"Authorization": "Bearer " + rs})[1]["userCtx"]["name"], f"rs_{x}")
        for alg, curve, h, size in [("ES256", ec.SECP256R1(), hashes.SHA256(), 32), ("ES512", ec.SECP521R1(), hashes.SHA512(), 66)]:
            k = ec.generate_private_key(curve)
            setc("jwt_keys", f"ec:{alg}", pem(k.public_key()))

            def es(m, k=k, h=h, size=size):
                r, s_ = decode_dss_signature(k.sign(m, ec.ECDSA(h)))
                return r.to_bytes(size, "big") + s_.to_bytes(size, "big")

            t = _jwt({"alg": alg, "kid": alg}, {"sub": f"{alg}_{x}", "exp": now + 60}, es)
            eq(session({"Authorization": "Bearer " + t})[1]["userCtx"]["name"], f"{alg}_{x}", alg)
            wrong = _jwt({"alg": alg, "kid": alg}, {"sub": "j", "exp": now + 60}, lambda m: b"\x01" * (2 * size))
            eq(session({"Authorization": "Bearer " + wrong}), (400, {"error": "bad_request", "reason": "Bad signature in authentication token"}), alg)
    finally:
        for sec, key in reversed(changed):
            req("DELETE", f"{c}/{sec}/{key}", headers={**ADMIN, **nopersist})
        req("DELETE", f"{c}/admins/{sa}", headers=ADMIN)


@test
def list_functions():
    db = new_db()
    req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "a", "n": 1}, {"_id": "b", "n": 2}, {"_id": "c", "n": 3}]})
    req("PUT", f"/{db}/_design/l", {
        "views": {"by_n": {"map": "function(d){ emit(d.n, d._id); }", "reduce": "_count"}},
        "lists": {
            "plain": "function(head, req){ var r; send('H:' + JSON.stringify(head) + '\\n'); while (r = getRow()) { send(r.key + '=' + r.value + '\\n'); } return 'TAIL'; }",
            "started": "function(head, req){ start({code: 201, headers: {'Content-Type': 'text/plain', 'X-Foo': 'bar'}}); var r; while (r = getRow()) send(r.id + ','); return 'end'; }",
            "prov": "function(head, req){ provides('json', function(){ var a=[], r; while (r = getRow()) a.push(r.key); send(JSON.stringify(a)); }); provides('html', function(){ var r; send('<ul>'); while (r = getRow()) send('<li>' + r.key + '</li>'); return '</ul>'; }); }",
            "err": "function(head, req){ throw({forbidden: 'nope'}); }",
        },
        "shows": {"s": "function(doc, req){ send('a'); send('b'); return 'c'; }",
                  "missing": "function(doc, req){ return doc.x; }"},
    })
    p = f"/{db}/_design/l/_list"

    def get(path, **kw):
        s, b, h = req(kw.pop("method", "GET"), path, kw.pop("body", None), headers=kw.pop("headers", {}), raw=True)
        return s, b.decode(), h.get("Content-Type")

    eq(get(f"{p}/plain/by_n?reduce=false"),
       (200, 'H:{"total_rows":3,"offset":0}\n1=a\n2=b\n3=c\nTAIL', "application/json"))
    eq(get(f"{p}/plain/by_n")[1], "H:{}\nnull=3\nTAIL")
    eq(get(f"{p}/plain/by_n?group=true")[1], "H:{}\n1=1\n2=1\n3=1\nTAIL")
    s, b, h = req("GET", f"{p}/started/by_n?reduce=false", raw=True)
    eq((s, b, h["Content-Type"], h["X-Foo"]), (201, b"a,b,c,end", "text/plain", "bar"))
    eq(get(f"{p}/prov/by_n?reduce=false", headers={"Accept": "application/json"}), (200, "[1,2,3]", "application/json"))
    eq(get(f"{p}/prov/by_n?reduce=false", headers={"Accept": "text/html"}),
       (200, "<ul><li>1</li><li>2</li><li>3</li></ul>", "text/html; charset=utf-8"))
    s, b, _ = get(f"{p}/prov/by_n?reduce=false", headers={"Accept": "image/png"})
    eq((s, json.loads(b)["reason"]),
       (406, "Content-Type image/png not supported, try one of: text/html; charset=utf-8, application/json, text/x-json"))
    s, b, _ = get(f"{p}/err/by_n?reduce=false")
    eq((s, json.loads(b)), (500, {"error": "render_error", "reason": "function raised error: [object Object] \nstacktrace: undefined"}))
    # Another design doc's view, _all_docs, and keys posted to the view.
    eq(get(f"{p}/plain/l/by_n?reduce=false&limit=1")[1], 'H:{"total_rows":3,"offset":0}\n1=a\nTAIL')
    eq(get(f"{p}/plain/_all_docs?limit=1")[1], 'H:{"total_rows":4,"offset":0}\n_design/l=[object Object]\nTAIL')
    eq(get(f"{p}/plain/by_n?reduce=false", method="POST", body={"keys": [2]})[1], 'H:{"total_rows":3,"offset":1}\n2=b\nTAIL')
    s, b, _ = get(f"{p}/nolist/by_n")
    eq((s, json.loads(b)), (404, {"error": "not_found", "reason": "missing json key: nolist"}))
    s, b, _ = get(f"{p}/plain/noview")
    eq((s, json.loads(b)), (404, {"error": "not_found", "reason": "missing_named_view"}))
    # Shows: send() chunks come before the return value; a show that fails
    # on a missing document named in the path is a 404.
    eq(get(f"/{db}/_design/l/_show/s")[:2], (200, "abc"))
    s, b, _ = get(f"/{db}/_design/l/_show/missing/zzz")
    eq((s, json.loads(b)), (404, {"error": "not_found", "reason": "document not found"}))
    eq(get(f"/{db}/_design/l/_show/missing")[0], 500)


@test
def rewrites():
    db = new_db()
    req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "a", "n": 1}, {"_id": "b", "n": 2}, {"_id": "c", "n": 3}]})
    view = {"v": {"map": "function(d){ if (d.n) emit(d.n, d._id); }"}}
    req("PUT", f"/{db}/_design/r", {"views": view, "rewrites": [
        {"from": "doc/:id", "to": "../../:id"},
        {"from": "view/:k", "to": "_view/v", "query": {"key": ":k"}, "formats": {"k": "int"}},
        {"from": "views/:k", "to": "_view/v", "query": {"key": ":k"}},
        {"from": "all", "to": "../../_all_docs", "query": {"limit": "2"}},
        {"from": "star/*", "to": "../../*"},
        {"from": "m", "to": "../../a", "method": "GET"},
        {"from": "info", "to": "../.."},
        {"from": "pass", "to": "_view/v"},
    ]})
    req("PUT", f"/{db}/_design/rx", {"rewrites": [{"from": "esc", "to": "../../../_all_dbs"}]})
    req("PUT", f"/{db}/_design/rf", {"views": view, "rewrites":
        "function(req) { var p = req.path; if (p[4] == 'doc') return '../../' + p[5]; "
        "if (p[4] == 'code') return {code: 418, body: 'teapot', headers: {'X-T': '1'}}; "
        "if (p[4] == 'q') return {path: '_view/v', query: {key: '2'}}; "
        "if (p[4] == 'forbid') throw({forbidden: 'no'}); if (p[4] == 'bad') return {path: 1}; return null; }"})
    r = f"/{db}/_design/r/_rewrite"
    eq(req("GET", f"{r}/doc/a")[1]["n"], 1)
    eq([x["id"] for x in req("GET", f"{r}/view/2")[1]["rows"]], ["b"])
    eq(req("GET", f"{r}/views/2")[1]["rows"], [])  # "2", not 2, without the int format
    eq(len(req("GET", f"{r}/all")[1]["rows"]), 2)
    eq(len(req("GET", f"{r}/all?limit=1")[1]["rows"]), 2)  # the rule's query wins
    eq(req("GET", f"{r}/star/b")[1]["n"], 2)
    eq(req("GET", f"{r}/star/_design/r")[1]["_id"], "_design/r")
    eq(req("GET", f"{r}/m")[1]["n"], 1)
    eq(req("POST", f"{r}/m", {})[:2], (404, {"error": "not_found", "reason": "missing"}))
    eq(req("GET", f"{r}/info")[1]["db_name"], db)
    eq([x["id"] for x in req("GET", f"{r}/pass?key=3")[1]["rows"]], ["c"])
    eq(req("GET", f"{r}/zzz")[:2], (404, {"error": "not_found", "reason": "missing"}))
    eq(req("GET", f"/{db}/_design/rx/_rewrite/esc")[:2],
       (500, {"error": "insecure_rewrite_rule", "reason": "too many ../.. segments"}))
    f = f"/{db}/_design/rf/_rewrite"
    eq(req("GET", f"{f}/doc/a")[1]["n"], 1)
    s, b, h = req("GET", f"{f}/code", raw=True)
    eq((s, b, h["X-T"]), (418, b"teapot", "1"))
    eq([x["id"] for x in req("GET", f"{f}/q")[1]["rows"]], ["b"])
    eq(req("GET", f"{f}/zzz")[:2], (404, {"error": "rewrite_error", "reason": "Invalid path."}))
    eq(req("GET", f"{f}/bad")[:2], (500, {"error": "rewrite_error", "reason": "bad path"}))
    eq(req("GET", f"{f}/forbid")[:2],
       (500, {"error": "rewrite_error", "reason": "function raised error: [object Object] \nstacktrace: undefined"}))
    eq(req("GET", f"/{db}/_design/rx/_rewrite")[0], 500)


@test
def partitioned_databases():
    db = "p" + uuid.uuid4().hex[:12]
    illegal = lambda reason: {"error": "illegal_docid", "reason": reason}
    eq(req("PUT", f"/{db}?partitioned=maybe")[:2], (400, {"error": "bad_request", "reason": "Invalid `partitioned` parameter"}))
    eq(req("PUT", f"/{db}?partitioned=true")[0], 201)
    try:
        eq(req("GET", f"/{db}")[1]["props"], {"partitioned": True})
        eq(req("PUT", f"/{db}/nopart", {"a": 1})[:2], (400, illegal("Doc id must be of form partition:id")))
        eq(req("PUT", f"/{db}/:x", {"a": 1})[:2], (400, {"error": "illegal_partition", "reason": "Partition must not be empty"}))
        eq(req("PUT", f"/{db}/_p:x", {"a": 1})[:2], (400, illegal("Only reserved document ids may start with underscore.")))
        eq(req("POST", f"/{db}", {"n": 5})[:2], (400, illegal("Doc id must be of form partition:id")))
        # One bad id rejects the whole batch.
        eq(req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "p1:z"}, {"_id": "bad"}]})[:2],
           (400, illegal("Doc id must be of form partition:id")))
        eq(req("GET", f"/{db}/p1:z")[0], 404)
        s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": [
            {"_id": "p1:a", "n": 1, "t": "x"}, {"_id": "p1:b", "n": 2, "t": "y"}, {"_id": "p2:a", "n": 3, "t": "x"}]})
        eq(s, 201)
        eq(req("PUT", f"/{db}/_local/x", {"a": 1})[0], 201)

        s, info, _ = req("GET", f"/{db}/_partition/p1")
        eq({k: info[k] for k in ("db_name", "partition", "doc_count", "doc_del_count")},
           {"db_name": db, "partition": "p1", "doc_count": 2, "doc_del_count": 0})
        assert info["sizes"]["active"] > 0 and info["sizes"]["external"] > 0, info
        eq(req("GET", f"/{db}/_partition/zz")[1]["doc_count"], 0)
        eq(req("GET", f"/{db}/_partition/_bad")[:2],
           (400, {"error": "illegal_partition", "reason": "Partition must not start with an underscore"}))
        # total_rows and offset count the whole database, as in CouchDB.
        s, b, _ = req("GET", f"/{db}/_partition/p1/_all_docs")
        eq((b["total_rows"], b["offset"], [r["id"] for r in b["rows"]]), (3, 0, ["p1:a", "p1:b"]))
        eq(req("GET", f"/{db}/_partition/p2/_all_docs")[1]["offset"], 2)
        eq([r["id"] for r in req("GET", f"/{db}/_partition/p2/_all_docs?include_docs=true")[1]["rows"]], ["p2:a"])
        # keys= looks docs up by id, in any partition; key= stays inside it.
        rows = req("POST", f"/{db}/_partition/p1/_all_docs", {"keys": ["p1:b", "p2:a", "p3:z"]})[1]["rows"]
        eq(([r.get("id") for r in rows], rows[2]), (["p1:b", "p2:a", None], {"key": "p3:z", "error": "not_found"}))
        eq(req("GET", f"/{db}/_partition/p1/_all_docs?key=%22p2:a%22")[1]["rows"], [])
        # Start and end keys outside the partition are clamped to it.
        for q, want in [("startkey=%22p2:a%22", (2, [])), ("endkey=%22p2:z%22", (0, ["p1:a", "p1:b"])),
                        ("startkey=%22p0%22", (0, ["p1:a", "p1:b"])),
                        ("startkey=%22p1:b%22&descending=true", (1, ["p1:b", "p1:a"])),
                        ("endkey=%22a%22&descending=true", (1, ["p1:b", "p1:a"]))]:
            b = req("GET", f"/{db}/_partition/p1/_all_docs?{q}")[1]
            eq((b["offset"], [r["id"] for r in b["rows"]]), want)
        eq(req("GET", f"/{db}/_partition/p1/p1:a")[0], 404)

        # Views: partitioned by default, global with options.partitioned false.
        eq(req("PUT", f"/{db}/_design/pv", {"views": {"v": {"map": "function(d){ emit(d.n, d.t); }", "reduce": "_count"}}})[0], 201)
        eq(req("PUT", f"/{db}/_design/gv", {"options": {"partitioned": False},
                                            "views": {"v": {"map": "function(d){ emit(d.n, d.t); }"}}})[0], 201)
        s, b, _ = req("GET", f"/{db}/_partition/p1/_design/pv/_view/v?reduce=false")
        eq(b, {"total_rows": 3, "offset": 0, "rows": [{"id": "p1:a", "key": 1, "value": "x"}, {"id": "p1:b", "key": 2, "value": "y"}]})
        eq(req("GET", f"/{db}/_partition/p1/_design/pv/_view/v?reduce=false&startkey=2")[1]["offset"], 1)
        eq(req("GET", f"/{db}/_partition/p2/_design/pv/_view/v?reduce=false")[1]["offset"], 2)
        eq(req("GET", f"/{db}/_partition/p2/_design/pv/_view/v")[1]["rows"], [{"key": None, "value": 1}])
        eq(req("GET", f"/{db}/_partition/p1/_design/pv/_view/v?group=true")[1]["rows"],
           [{"key": 1, "value": 1}, {"key": 2, "value": 1}])
        eq(req("GET", f"/{db}/_partition/p1/_design/pv/_view/v?reduce=false&include_docs=true&limit=1")[1]["rows"][0]["doc"]["t"], "x")
        eq(req("GET", f"/{db}/_design/pv/_view/v?reduce=false")[:2],
           (400, {"error": "query_parse_error", "reason": "`partition` parameter is mandatory for queries to this view."}))
        eq(req("GET", f"/{db}/_design/gv/_view/v")[1]["total_rows"], 3)
        eq(req("GET", f"/{db}/_partition/p1/_design/gv/_view/v")[:2],
           (400, {"error": "query_parse_error", "reason": "`partition` parameter is not supported in this design doc"}))

        # Mango: indexes are partitioned by default.
        eq(req("POST", f"/{db}/_index", {"index": {"fields": ["n"]}, "name": "pn"})[1]["result"], "created")
        eq(req("POST", f"/{db}/_index", {"index": {"fields": ["t"]}, "name": "gt", "partitioned": False})[1]["result"], "created")
        idx = {i["name"]: i for i in req("GET", f"/{db}/_index")[1]["indexes"]}
        eq((idx["pn"]["partitioned"], idx["gt"]["partitioned"], "partitioned" in idx["_all_docs"]), (True, False, False))
        s, b, _ = req("POST", f"/{db}/_partition/p1/_find", {"selector": {"n": {"$gt": 0}}})
        eq(([d["_id"] for d in b["docs"]], "warning" in b), (["p1:a", "p1:b"], False))
        s, b, _ = req("POST", f"/{db}/_partition/p1/_explain", {"selector": {"n": {"$gt": 0}}})
        eq((b["index"]["name"], b["partitioned"], b["opts"]["partition"]), ("pn", True, "p1"))
        s, b, _ = req("POST", f"/{db}/_find", {"selector": {"n": {"$gt": 0}}, "use_index": "pn"})
        eq(len(b["docs"]), 3)
        assert b["warning"].startswith("_design/pn was not used because it does not contain a valid index"), b["warning"]
        eq([d["_id"] for d in req("POST", f"/{db}/_partition/p2/_find", {"selector": {"t": "x"}})[1]["docs"]], ["p2:a"])

        eq(req("GET", "/_users/_partition/x")[:2], (400, {"error": "bad_request", "reason": "database is not partitioned"}))
        np = db + "_np"
        eq(req("PUT", f"/{np}")[0], 201)
        eq(req("PUT", f"/{np}/_design/x", {"options": {"partitioned": True}, "views": {"v": {"map": "function(d){emit(1)}"}}})[:2],
           (400, {"error": "invalid_design_doc", "reason": "partitioned option cannot be true in a non-partitioned database."}))
        req("DELETE", f"/{np}")
    finally:
        req("DELETE", f"/{db}")


@test
def show_content_negotiation():
    db = new_db()
    req("PUT", f"/{db}/_design/fn", {
        "shows": {
            "fmt": """function(doc, req) {
                provides('json', function() { return JSON.stringify({name: doc.name}); });
                provides('html', function() { return '<b>' + doc.name + '</b>'; });
                registerType('vcard', 'text/vcard');
                provides('vcard', function() { return 'FN:' + doc.name; });
            }""",
            "typed": """function(doc, req) {
                return {body: 'plain', headers: {'Content-Type': 'text/plain'}};
            }""",
            "hdr": "function(doc, req) { return {json: {accept: req.headers.Accept || null}}; }",
        },
    })
    req("PUT", f"/{db}/x", {"name": "X"})
    path = f"/{db}/_design/fn/_show/fmt/x"

    def get(p, accept=None):
        s, body, h = req("GET", p, headers={"Accept": accept} if accept else {"Accept": ""}, raw=True)
        return s, body, h.get_all("Content-Type") or []

    # Accept picks the provider; each gets its registered type.
    s, body, ct = get(path, "application/json")
    eq((s, json.loads(body), ct), (200, {"name": "X"}, ["application/json"]))
    s, body, ct = get(path, "text/html")
    eq((s, body, ct), (200, b"<b>X</b>", ["text/html; charset=utf-8"]))
    s, body, ct = get(path, "text/vcard")
    eq((s, body, ct), (200, b"FN:X", ["text/vcard"]))
    # Quality values and wildcards, as browsers send them.
    s, body, _ = get(path, "text/html;q=0.5, application/json;q=0.9")
    eq(json.loads(body), {"name": "X"})
    s, body, _ = get(path, "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    eq(body, b"<b>X</b>")
    # */* and no Accept header take the first provider (CouchDB labels the
    # */* case with the last matching type, text/x-json, so only the body
    # is compared).
    s, body, _ = get(path, "*/*")
    eq(json.loads(body), {"name": "X"})
    s, body, _ = get(path)
    eq(json.loads(body), {"name": "X"})
    # ?format= overrides Accept.
    s, body, ct = get(path + "?format=html", "application/json")
    eq((body, ct), (b"<b>X</b>", ["text/html; charset=utf-8"]))
    # Nothing acceptable: 406.
    s, body, _ = get(path, "image/png")
    eq(s, 406)
    eq(json.loads(body)["error"], "not_acceptable")

    # A show's own Content-Type replaces the default instead of adding to it.
    s, body, ct = get(f"/{db}/_design/fn/_show/typed/x")
    eq((body, ct), (b"plain", ["text/plain"]))
    # Header names reach the function as clients write them.
    s, body, _ = get(f"/{db}/_design/fn/_show/hdr/x", "text/csv")
    eq(json.loads(body), {"accept": "text/csv"})


def replicate(src, tgt):
    """Minimal CouchDB replication protocol, as couch_replicator does it."""
    rep_id = f"_local/rep-{src}-{tgt}"
    _, cp, _ = req("GET", f"/{tgt}/{rep_id}")
    since = cp.get("source_last_seq", 0) if isinstance(cp, dict) and "_rev" in cp else 0
    _, ch, _ = req("GET", f"/{src}/_changes?style=all_docs&since={since}")
    if ch["results"]:
        diff_req = {r["id"]: [c["rev"] for c in r["changes"]] for r in ch["results"]}
        _, diff, _ = req("POST", f"/{tgt}/_revs_diff", diff_req)
        wanted = [{"id": i, "rev": r, "atts_since": d.get("possible_ancestors", [])}
                  for i, d in diff.items() for r in d["missing"]]
        if wanted:
            _, got, _ = req("POST", f"/{src}/_bulk_get?revs=true&attachments=true&latest=true", {"docs": wanted})
            docs = [d["ok"] for res in got["results"] for d in res["docs"] if "ok" in d]
            s, errs, _ = req("POST", f"/{tgt}/_bulk_docs", {"docs": docs, "new_edits": False})
            eq((s, errs), (201, []))
    body = {"source_last_seq": ch["last_seq"]}
    if isinstance(cp, dict) and "_rev" in cp:
        body["_rev"] = cp["_rev"]
    req("PUT", f"/{tgt}/{rep_id}", body)
    return len(ch["results"])


@test
def replication_protocol_roundtrip():
    a, b = new_db(), new_db()
    req("POST", f"/{a}/_bulk_docs", {"docs": [{"_id": f"d{i}", "v": i} for i in range(50)]})
    req("PUT", f"/{a}/withatt/f.bin", bytes(range(256)) * 10, {"Content-Type": "application/x-test"})
    eq(replicate(a, b), 51)
    eq(replicate(a, b), 0)  # checkpoint honoured
    # concurrent edits on both sides create a conflict that replicates both ways
    _, d, _ = req("GET", f"/{a}/d1")
    req("PUT", f"/{a}/d1", {"_rev": d["_rev"], "v": "from-a"})
    req("PUT", f"/{b}/d1", {"_rev": d["_rev"], "v": "from-b"})
    replicate(a, b)
    replicate(b, a)
    _, da, _ = req("GET", f"/{a}/d1?conflicts=true")
    _, db_, _ = req("GET", f"/{b}/d1?conflicts=true")
    eq(da["_rev"], db_["_rev"])  # same deterministic winner on both sides
    eq(len(da["_conflicts"]), 1)
    s, body, h = req("GET", f"/{b}/withatt/f.bin", raw=True)
    eq((body, h["Content-Type"]), (bytes(range(256)) * 10, "application/x-test"))
    _, ia, _ = req("GET", f"/{a}")
    _, ib, _ = req("GET", f"/{b}")
    eq(ia["doc_count"], ib["doc_count"])


@test
def concurrent_writers():
    db = new_db()
    errors = []

    def writer(n):
        try:
            rev = None
            for i in range(30):
                body = {"i": i}
                if rev:
                    body["_rev"] = rev
                s, b, _ = req("PUT", f"/{db}/w{n}", body)
                assert s == 201, (s, b)
                rev = b["rev"]
                req("POST", f"/{db}", {"writer": n, "i": i})
        except Exception as e:
            errors.append(e)

    ts = [threading.Thread(target=writer, args=(n,)) for n in range(16)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    eq(errors, [])
    _, info, _ = req("GET", f"/{db}")
    eq(info["doc_count"], 16 + 16 * 30)
    eq(int(info["update_seq"].split("-")[0]), 16 * 30 * 2)
    _, ch, _ = req("GET", f"/{db}/_changes")
    eq(len(ch["results"]), 16 + 16 * 30)
    _, w, _ = req("GET", f"/{db}/w7")
    assert w["_rev"].startswith("30-")


@test
def fauxton_web_ui():
    r = urllib.request.Request(BASE + "/_utils", method="GET")

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *a, **k):
            return None

    try:
        urllib.request.build_opener(NoRedirect).open(r, timeout=30)
        raise AssertionError("expected a redirect")
    except urllib.error.HTTPError as e:
        eq(e.code, 301)
        eq(e.headers["Location"], f"{BASE}/_utils/")
    s, html, h = req("GET", "/_utils/", raw=True)
    if s == 404 and req("GET", "/")[1]["vendor"]["name"] != "couchdb-rs":
        print("     (this CouchDB was built without Fauxton: skipped)")
        return
    eq(s, 200)
    assert h["Content-Type"].startswith("text/html"), h["Content-Type"]
    assert b"Project Fauxton" in html
    # Every script and stylesheet index.html names is served.
    import re
    assets = re.findall(rb'(?:src|href)="(dashboard\.assets/[^"]+)"', html)
    assert assets, html[:200]
    for a in assets:
        s, body, h = req("GET", "/_utils/" + a.decode(), raw=True)
        eq(s, 200, a)
        assert len(body) > 0
        assert h["Content-Type"] in ("application/javascript", "text/css", "image/png"), (a, h["Content-Type"])
    s, _, _ = req("GET", "/_utils/missing.js")
    eq(s, 404)
    s, _, _ = req("GET", "/_utils/../_all_dbs")
    assert s in (400, 404), s
    s, b, _ = req("GET", "/_cluster_setup")
    eq(b, {"state": "single_node_enabled"})


@test
def delete_and_recreate_while_in_use():
    db = new_db()
    # CouchDB's cluster layer answers 500s ("No DB shards could be opened",
    # internal_server_error) while its shards come and go.
    couchdb = req("GET", "/")[1]["vendor"]["name"] != "couchdb-rs"
    stop = threading.Event()
    bad = []

    def check(what, s, b):
        # Deleted underneath, a write may see 404 (or 409/412 racing the
        # recreate), never a server error.
        if s >= 500 and not couchdb:
            bad.append((what, s, b))

    def writer(n):
        i = 0
        while not stop.is_set():
            s, b, _ = req("PUT", f"/{db}/w{n}-{i}", {"n": n, "i": i})
            check("put", s, b)
            s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": f"b{n}-{i}-{k}", "k": k} for k in range(20)]})
            check("bulk_docs", s, b)
            s, b, _ = req("GET", f"/{db}/w{n}-{i}")
            check("get", s, b)
            i += 1

    def churner():
        while not stop.is_set():
            s, b, _ = req("DELETE", f"/{db}")
            check("delete", s, b)
            s, b, _ = req("PUT", f"/{db}")
            check("create", s, b)

    ts = [threading.Thread(target=writer, args=(n,)) for n in range(6)] + [threading.Thread(target=churner) for _ in range(2)]
    [t.start() for t in ts]
    time.sleep(4)
    stop.set()
    [t.join() for t in ts]
    eq(bad[:3], [])
    # A recreated database starts empty, independent of the old one.
    req("DELETE", f"/{db}")
    s, _, _ = req("PUT", f"/{db}")
    eq(s, 201)
    s, _, _ = req("PUT", f"/{db}/only", {"x": 1})
    eq(s, 201)
    _, info, _ = req("GET", f"/{db}")
    eq(info["doc_count"], 1)
    eq(int(info["update_seq"].split("-")[0]), 1)
    s, _, _ = req("GET", f"/{db}/w0-0")
    eq(s, 404)
    s, dbs, _ = req("GET", "/_all_dbs")
    eq(dbs.count(db), 1)
    req("DELETE", f"/{db}")
    s, dbs, _ = req("GET", "/_all_dbs")
    eq(db in dbs, False)


@test
def stats_system_and_active_tasks():
    n = "/_node/_local"
    def stat(path):
        return req("GET", f"{n}/_stats/{path}")[1]

    # _stats: every metric is {value, type, desc}, nested by its path.
    s, b, _ = req("GET", f"{n}/_stats")
    eq(s, 200)
    for group in ("couchdb", "couch_log", "couch_replicator", "mango", "fsync", "ddoc_cache"):
        assert group in b, group
    eq(stat("couchdb/httpd_request_methods/GET")["desc"], "number of HTTP GET requests")
    eq(stat("couchdb/httpd/requests")["type"], "counter")
    eq(stat("couchdb/query_server/process_count")["type"], "gauge")
    eq(stat("couchdb/httpd_status_codes/404")["desc"], "number of HTTP 404 Not Found responses")
    hist = stat("couchdb/request_time")
    eq((hist["type"], hist["desc"]), ("histogram", "length of a request inside CouchDB without MochiWeb"))
    eq(list(hist["value"].keys()), ["n", "min", "max", "arithmetic_mean", "geometric_mean", "harmonic_mean", "median",
                                    "variance", "standard_deviation", "skewness", "kurtosis", "percentile", "histogram"])
    eq([p[0] for p in hist["value"]["percentile"]], [50, 75, 90, 95, 99, 999])
    eq(hist["value"]["histogram"], [[0, 0]])
    eq(stat("couchdb/nope"), None)
    assert isinstance(stat("couchdb/httpd/requests/value"), int)
    eq(req("POST", f"{n}/_stats", {})[:2], (405, {"error": "method_not_allowed", "reason": "Only GET allowed"}))
    eq(req("GET", "/_node/nonode/_stats")[:2], (404, {"error": "not_found", "reason": "no such node: nonode"}))
    # Metrics need _admin or _metrics (anonymous users are admins in
    # admin party).
    if "_admin" not in req("GET", "/_session", headers=ANON)[1]["userCtx"]["roles"]:
        metrics_only = {"error": "unauthorized", "reason": "You are not a server admin or read-only metrics user"}
        for p in ("_stats", "_system", "_prometheus"):
            eq(req("GET", f"{n}/{p}", headers=ANON)[:2], (401, metrics_only))

    # Counters move with requests and documents.
    db = new_db()
    try:
        before = {k: stat(k)["value"] for k in ("couchdb/httpd_request_methods/GET", "couchdb/httpd_status_codes/404",
                                                 "couchdb/document_inserts", "couchdb/database_writes",
                                                 "couchdb/database_reads", "couchdb/httpd/bulk_requests")}
        eq(req("PUT", f"/{db}/a", {"v": 1})[0], 201)
        eq(req("GET", f"/{db}/a")[0], 200)
        eq(req("GET", f"/{db}/missing")[0], 404)
        eq(req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "b"}, {"_id": "c"}]})[0], 201)
        after = {k: stat(k)["value"] for k in before}
        assert after["couchdb/httpd_request_methods/GET"] >= before["couchdb/httpd_request_methods/GET"] + 2, (before, after)
        assert after["couchdb/httpd_status_codes/404"] >= before["couchdb/httpd_status_codes/404"] + 1, (before, after)
        assert after["couchdb/document_inserts"] >= before["couchdb/document_inserts"] + 3, (before, after)
        assert after["couchdb/database_writes"] >= before["couchdb/database_writes"] + 2, (before, after)
        assert after["couchdb/database_reads"] >= before["couchdb/database_reads"] + 1, (before, after)
        assert after["couchdb/httpd/bulk_requests"] >= before["couchdb/httpd/bulk_requests"] + 1, (before, after)
        assert stat("couchdb/request_time")["value"]["n"] > 0

        # _system: the node's process figures.
        s, b, _ = req("GET", f"{n}/_system")
        eq(s, 200)
        for k in ("uptime", "memory", "run_queue", "run_queue_dirty_cpu", "ets_table_count", "context_switches",
                  "reductions", "garbage_collection_count", "words_reclaimed", "io_input", "io_output",
                  "os_proc_count", "stale_proc_count", "process_count", "process_limit", "message_queues",
                  "internal_replication_jobs", "distribution", "distribution_events"):
            assert k in b, k
        eq(sorted(b["memory"]), sorted(["other", "atom", "atom_used", "processes", "processes_used", "binary", "code", "ets"]))
        assert b["io_input"] > 0 and b["io_output"] > 0, b
        assert isinstance(b["message_queues"]["couch_file"], dict), b["message_queues"]
        eq(req("POST", f"{n}/_system", {})[0], 405)

        # _prometheus: the same metrics as text.
        s, text, h = req("GET", f"{n}/_prometheus", raw=True)
        eq((s, h["Content-Type"]), (200, "text/plain; version=2.0"))
        text = text.decode()
        for line in ("# TYPE couchdb_httpd_requests_total counter", "# TYPE couchdb_request_time_seconds summary",
                     "# HELP couchdb_httpd_status_codes number of HTTP responses by status code",
                     "# TYPE couchdb_open_databases gauge", "# TYPE couchdb_uptime_seconds counter",
                     "# HELP couchdb_auth_cache_requests_total number of authentication cache requests"):
            assert line in text.split("\n"), line
        assert 'couchdb_httpd_request_methods{method="GET"} ' in text
        assert 'couchdb_httpd_status_codes{code="200"} ' in text
        assert 'couchdb_request_time_seconds{quantile="0.5"} ' in text
        assert 'couchdb_couch_log_requests_total{level="alert"} ' in text
        assert 'couchdb_rexi_streams_timeout_total{stage="init_stream"} 0' in text

        s, b, _ = req("GET", f"{n}/_versions")
        eq((s, sorted(b["javascript_engine"]), "name" in b["collation_driver"]), (200, ["name"], True))

        # _active_tasks: an index build in progress.
        eq(req("POST", "/_active_tasks", {})[:2], (405, {"error": "method_not_allowed", "reason": "Only GET,HEAD allowed"}))
        for i in range(0, 30000, 5000):
            eq(req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": f"d{j:06d}", "n": j} for j in range(i, i + 5000)]})[0], 201)
        eq(req("PUT", f"/{db}/_design/slow", {"views": {"v": {
            "map": "function(d){ var s = 0; for (var i = 0; i < 20000; i++) { s += i; } emit(d.n, s); }"}}})[0], 201)
        t = threading.Thread(target=lambda: req("GET", f"/{db}/_design/slow/_view/v?limit=1"))
        t.start()
        seen = None
        deadline = time.time() + 60
        while time.time() < deadline and t.is_alive():
            tasks = [x for x in req("GET", "/_active_tasks")[1] if x.get("type") == "indexer" and db in x.get("database", "")]
            if tasks:
                seen = tasks[0]
                break
            time.sleep(0.01)
        t.join()
        assert seen, "no indexer task seen"
        eq(list(seen)[:3], ["node", "pid", "process_status"])
        eq(list(seen)[3:], sorted(list(seen)[3:]))
        eq(sorted(seen), sorted(["node", "pid", "process_status", "changes_done", "database", "design_document",
                                 "indexer_pid", "progress", "started_on", "total_changes", "type", "updated_on"]))
        eq((seen["design_document"], seen["total_changes"] >= 30003), ("_design/slow", True))
        assert 0 <= seen["progress"] <= 100 and seen["started_on"] <= seen["updated_on"], seen
        assert seen["pid"].startswith("<0.") and seen["indexer_pid"].startswith("<0."), seen
        for _ in range(100):
            if not [x for x in req("GET", "/_active_tasks")[1] if db in x.get("database", "")]:
                break
            time.sleep(0.1)
        else:
            raise AssertionError("indexer task still listed after the build")
    finally:
        req("DELETE", f"/{db}")

    # Unknown _paths: creating one is a bad name, anything else 404s.
    eq(req("GET", "/_nosuch")[:2], (404, {"error": "not_found", "reason": "Database does not exist."}))
    eq(req("PUT", "/_nosuch")[0:2], (400, {"error": "illegal_database_name",
        "reason": "Name: '_nosuch'. Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed. Must begin with a letter."}))

    # _restart runs the server again in place (couchdb-rs only here, to
    # leave a shared CouchDB alone).
    if req("GET", "/")[1].get("vendor", {}).get("name") == "couchdb-rs":
        up = req("GET", f"{n}/_system")[1]["uptime"]
        time.sleep(1.1)
        eq(req("GET", f"{n}/_restart")[:2], (405, {"error": "method_not_allowed", "reason": "Only POST allowed"}))
        eq(req("POST", f"{n}/_restart", {})[:2], (200, {"ok": True}))
        time.sleep(0.5)
        for _ in range(100):
            try:
                if req("GET", "/_up")[0] == 200:
                    break
            except Exception:
                pass
            time.sleep(0.1)
        assert req("GET", f"{n}/_system")[1]["uptime"] <= up, "uptime did not restart"


@test
def background_indexing_and_auto_compaction():
    import os
    n = "/_node/_local"
    # smoosh's channels, as _smoosh/status reports them.
    s, b, _ = req("GET", f"{n}/_smoosh/status")
    eq(s, 200)
    for ch in ("index_cleanup", "ratio_dbs", "ratio_views", "slack_dbs", "slack_views", "upgrade_dbs", "upgrade_views"):
        c = b["channels"][ch]
        eq((sorted(c), sorted(c["waiting"])), (["active", "starting", "waiting"], ["max", "min", "size"]), ch)
    eq(req("POST", f"{n}/_smoosh/status", {})[:2], (405, {"error": "method_not_allowed", "reason": "Only GET allowed"}))

    db = new_db()
    try:
        def db_seq():
            return int(str(req("GET", f"/{db}")[1]["update_seq"]).split("-")[0])

        def index_seq(ddoc):
            return req("GET", f"/{db}/_design/{ddoc}/_info")[1]["view_index"]["update_seq"]

        # ken: views are built in the background, without a query, unless
        # the design doc turns that off.
        eq(req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": f"d{i:05d}", "n": i, "pad": os.urandom(1000).hex()}
                                                       for i in range(1500)]})[0], 201)
        eq(req("PUT", f"/{db}/_design/auto", {"views": {"v": {"map": "function(d){ emit(d.n, null); }"}}})[0], 201)
        eq(req("PUT", f"/{db}/_design/manual", {"autoupdate": False,
                                                "views": {"v": {"map": "function(d){ emit(d._id, 1); }"}}})[0], 201)
        deadline = time.time() + 60
        while time.time() < deadline and index_seq("auto") < 1501:
            time.sleep(0.2)
        assert index_seq("auto") >= 1501, (index_seq("auto"), db_seq())
        eq(index_seq("manual"), 0)

        # smoosh: a database mostly old revisions is compacted by itself,
        # here by a channel of the test's own. Channels are asked in name
        # order, so it goes first, and only databases that change while it
        # exists can reach it. It starts compacting after smoosh's wait.
        c, np = "/_node/_local/_config", {"X-Couch-Persist": "false"}
        eq(req("PUT", f"{c}/smoosh.parity_dbs/priority", "ratio", headers=np)[0], 200)
        eq(req("PUT", f"{c}/smoosh.parity_dbs/concurrency", "1", headers=np)[0], 200)
        old_channels = req("GET", f"{c}/smoosh/db_channels")
        eq(req("PUT", f"{c}/smoosh/db_channels", "parity_dbs", headers=np)[0], 200)
        try:
            _auto_compaction(db)
        finally:
            if old_channels[0] == 200:
                req("PUT", f"{c}/smoosh/db_channels", old_channels[1], headers=np)
            else:
                req("DELETE", f"{c}/smoosh/db_channels", headers=np)
            for k in ("priority", "concurrency"):
                req("DELETE", f"{c}/smoosh.parity_dbs/{k}", headers=np)
        s, b, _ = req("GET", f"{n}/_smoosh/status")
        assert "parity_dbs" not in b["channels"], b
    finally:
        req("DELETE", f"/{db}")


def _auto_compaction(db):
    import os
    for k in range(3):
        rows = req("GET", f"/{db}/_all_docs?include_docs=true&startkey=%22d%22&endkey=%22e%22")[1]["rows"]
        docs = [dict(r["doc"], k=k, pad=os.urandom(1000).hex()) for r in rows]
        eq(req("POST", f"/{db}/_bulk_docs", {"docs": docs})[0], 201)
    before = req("GET", f"/{db}")[1]["sizes"]
    assert before["file"] > 2 * before["active"], before
    deadline = time.time() + 120
    while time.time() < deadline:
        sizes = req("GET", f"/{db}")[1]["sizes"]
        if sizes["file"] < before["file"] / 2:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"not compacted: {before} -> {sizes}")
    eq(len(req("GET", f"/{db}/_all_docs?include_docs=true&limit=5")[1]["rows"]), 5)
    eq(req("GET", f"/{db}/d00007")[1]["k"], 2)


def _wait(fn, secs=90, step=0.25):
    """fn() until it returns something true, or None after secs."""
    deadline = time.time() + secs
    while time.time() < deadline:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return None


def _rep_fixture(src):
    """Docs replication must carry: plain, attachments, a deletion, a
    conflict, a design doc, a history, and a _local doc it must not."""
    eq(req("POST", f"/{src}/_bulk_docs", {"docs": [{"_id": f"d{i:02d}", "n": i} for i in range(20)]})[0], 201)
    blob = bytes(range(256)) * 200
    eq(req("PUT", f"/{src}/att", {"_attachments": {
        "b.bin": {"content_type": "application/octet-stream", "data": base64.b64encode(blob).decode()},
        "t.txt": {"content_type": "text/plain", "data": base64.b64encode(b"hi").decode()}}})[0], 201)
    rev = req("PUT", f"/{src}/gone", {"a": 1})[1]["rev"]
    eq(req("DELETE", f"/{src}/gone?rev={rev}")[0], 200)
    eq(req("POST", f"/{src}/_bulk_docs", {"new_edits": False, "docs": [
        {"_id": "conf", "_rev": "2-aaa", "_revisions": {"start": 2, "ids": ["aaa", "root"]}, "v": "a"},
        {"_id": "conf", "_rev": "2-bbb", "_revisions": {"start": 2, "ids": ["bbb", "root"]}, "v": "b"}]})[0], 201)
    eq(req("PUT", f"/{src}/_design/f", {"filters": {"even": "function(doc, req) { return doc.n % 2 == 0; }"}})[0], 201)
    rev = req("PUT", f"/{src}/upd", {"v": 1})[1]["rev"]
    eq(req("PUT", f"/{src}/upd", {"v": 2, "_rev": rev})[0], 201)
    eq(req("PUT", f"/{src}/_local/l", {"x": 1})[0], 201)


def _db_state(db):
    """Every doc with its history, conflicts and attachments, and the
    deleted ids."""
    out = {}
    for row in req("GET", f"/{db}/_all_docs")[1]["rows"]:
        doc_id = urllib.parse.quote(row["id"], safe="/")
        out[row["id"]] = req("GET", f"/{db}/{doc_id}?revs=true&conflicts=true&attachments=true")[1]
    deleted = sorted(r["id"] for r in req("GET", f"/{db}/_changes?style=all_docs")[1]["results"] if r.get("deleted"))
    return out, deleted


@test
def replication():
    src, tgt, cont = new_db(), "r" + uuid.uuid4().hex[:12], "r" + uuid.uuid4().hex[:12]
    try:
        _rep_fixture(src)
        # One-shot, local names: everything arrives, checkpointed.
        s, b, _ = req("POST", "/_replicate", {"source": src, "target": tgt, "create_target": True})
        eq((s, b["ok"], b["replication_id_version"]), (200, True, 4), b)
        h = b["history"][0]
        eq((h["docs_written"], h["doc_write_failures"], h["missing_found"]), (26, 0, 26), h)
        assert b["session_id"] == h["session_id"] and b["source_last_seq"] == h["recorded_seq"], b
        eq(_db_state(tgt), _db_state(src))
        eq(req("GET", f"/{tgt}/_local/l")[0], 404)
        # Again: nothing new, the same checkpoint.
        s, b2, _ = req("POST", "/_replicate", {"source": src, "target": tgt})
        eq((s, b2["no_changes"], b2["session_id"]), (200, True, b["session_id"]))
        # Filtered, doc_ids and selector replications.
        def replicated(extra):
            t = "r" + uuid.uuid4().hex[:12]
            try:
                s, b, _ = req("POST", "/_replicate", dict({"source": src, "target": t, "create_target": True}, **extra))
                eq(s, 200, b)
                return [r["id"] for r in req("GET", f"/{t}/_all_docs")[1]["rows"]]
            finally:
                req("DELETE", f"/{t}")
        eq(replicated({"filter": "f/even"}), [f"d{i:02d}" for i in range(0, 20, 2)])
        eq(replicated({"doc_ids": ["d03", "att"]}), ["att", "d03"])
        eq(replicated({"selector": {"n": {"$gte": 18}}}), ["d18", "d19"])

        # Continuous: a job, a task, and new changes follow.
        s, b, _ = req("POST", "/_replicate", {"source": src, "target": cont, "create_target": True, "continuous": True})
        eq(s, 202, b)
        rep_id = b["_local_id"]
        assert b["ok"] is True and rep_id.endswith("+continuous+create_target"), b
        job = _wait(lambda: [j for j in req("GET", "/_scheduler/jobs")[1]["jobs"] if j["id"] == rep_id])
        assert job, "no job"
        eq(sorted(job[0]), sorted(["database", "id", "pid", "source", "target", "user", "doc_id", "info", "history", "node", "start_time"]))
        eq((job[0]["database"], job[0]["doc_id"], job[0]["source"].endswith(f"/{src}/")), (None, None, True))
        eq(req("GET", f"/_scheduler/jobs/{rep_id}")[1]["id"], rep_id)
        eq(req("PUT", f"/{src}/late", {"x": 1})[0], 201)
        assert _wait(lambda: req("GET", f"/{cont}/late")[0] == 200), "continuous replication missed a change"
        task = _wait(lambda: [t for t in req("GET", "/_active_tasks")[1] if t.get("replication_id") == rep_id])
        assert task, "no replication task"
        for k in ("continuous", "docs_written", "source_seq", "through_seq", "checkpointed_source_seq", "changes_pending",
                  "revisions_checked", "missing_revisions_found", "docs_read", "doc_write_failures", "checkpoint_interval"):
            assert k in task[0], (k, task[0])
        eq((task[0]["type"], task[0]["continuous"]), ("replication", True))
        eq(req("POST", "/_replicate", {"replication_id": rep_id, "cancel": True})[:2], (200, {"ok": True, "_local_id": rep_id}))
        eq(req("POST", "/_replicate", {"replication_id": rep_id, "cancel": True})[:2], (404, {"error": "not_found", "reason": "missing"}))

        # Errors.
        missing = "r" + uuid.uuid4().hex[:12]
        s, b, _ = req("POST", "/_replicate", {"source": src, "target": missing})
        assert s == 500 and b["error"] == "db_not_found" and b["reason"].startswith("could not open http") \
            and b["reason"].endswith(f"/{missing}/"), (s, b)
        eq(req("POST", "/_replicate", {})[:2], (500, {"error": "error", "reason": "Missing replicator database"}))
        eq(req("POST", "/_replicate", {"source": src, "target": tgt, "continuous": "yes"})[:2],
           (400, {"error": "bad_request", "reason": "parameter `continuous` must be a boolean"}))
        eq(req("POST", "/_replicate", {"source": src, "target": tgt, "doc_ids": ["a"], "filter": "f/even"})[:2],
           (400, {"error": "bad_request", "reason": "`doc_ids`,`filter`,`selector` are mutually exclusive"}))
        eq(req("POST", "/_replicate", {"source": src, "target": tgt, "filter": "nofilter"})[:2],
           (500, {"error": "error", "reason": "Invalid filter. Must match `ddocname/filtername`."}))
        s, b, _ = req("POST", "/_replicate", {"source": src, "target": tgt, "filter": "nope/f"})
        assert s == 404 and b["error"] == "not_found" and b["reason"].startswith("Couldn't open document `_design/nope` from source database `http"), (s, b)
        eq(req("GET", "/_replicate")[:2], (400, {"error": "bad_request", "reason": "Missing request body"}))
        eq(req("POST", "/_replicate", b"{}", headers={"Content-Type": "text/plain"})[:2],
           (415, {"error": "bad_content_type", "reason": "Content-Type must be application/json"}))
        eq(req("GET", "/_scheduler/jobs/nope")[:2], (404, {"error": "not_found", "reason": "missing"}))
        eq(req("GET", "/_scheduler/nope")[:2], (404, {"error": "not found"}))
        eq(req("POST", "/_scheduler/jobs", {})[:2], (405, {"error": "method_not_allowed", "reason": "Only GET,HEAD allowed"}))
        eq(req("GET", "/_scheduler/jobs?limit=x")[:2], (400, {"error": "query_parse_error", "reason": "limit must be an integer"}))
    finally:
        for d in (src, tgt, cont):
            req("DELETE", f"/{d}")


@test
def replicator_database():
    src, tgt, cont = new_db(), "r" + uuid.uuid4().hex[:12], "r" + uuid.uuid4().hex[:12]
    one, run = "one_" + uuid.uuid4().hex[:8], "run_" + uuid.uuid4().hex[:8]
    try:
        eq(req("POST", f"/{src}/_bulk_docs", {"docs": [{"_id": f"d{i}"} for i in range(3)]})[0], 201)
        # Docs are checked as they are written.
        bad = {"source": db_url(src), "target": db_url(tgt)}
        eq(req("PUT", f"/_replicator/bad_{one}", dict(bad, continuous="yes"))[:2],
           (403, {"error": "forbidden", "reason": '{throw,{bad_request,<<"parameter `continuous` must be a boolean">>}}'}))
        eq(req("PUT", f"/_replicator/bad_{one}", {"source": src, "target": db_url(tgt)})[:2],
           (403, {"error": "forbidden", "reason": "local_endpoints_not_supported"}))
        eq(req("PUT", f"/_replicator/bad_{one}", {"target": db_url(tgt)})[:2],
           (403, {"error": "forbidden", "reason": "Missing replicator database"}))
        eq(req("GET", "/_scheduler/docs?states=bogus")[:2], (400, {"error": "query_parse_error",
           "reason": "States must be one or more of [initializing,error,running,pending,crashing,completed,failed]"}))

        # A one-shot doc runs and is marked completed, with its stats.
        eq(req("PUT", f"/_replicator/{one}", {"source": db_url(src), "target": db_url(tgt), "create_target": True})[0], 201)
        doc = _wait(lambda: (lambda d: d if d.get("_replication_state") == "completed" else None)(req("GET", f"/_replicator/{one}")[1]))
        assert doc, req("GET", f"/_replicator/{one}")[1]
        assert "owner" in doc and "_replication_state_time" in doc, doc
        stats = doc["_replication_stats"]
        eq((stats["docs_written"], stats["doc_write_failures"], "start_time" in stats), (3, 0, True))
        eq(req("GET", f"/{tgt}")[1]["doc_count"], 3)
        info = req("GET", f"/_scheduler/docs/_replicator/{one}")[1]
        eq((info["doc_id"], info["database"], info["state"], info["id"], info["error_count"]), (one, "_replicator", "completed", None, 0))
        assert info["source"].endswith(f"/{src}/") and "@" not in info["source"], info
        eq(info["info"]["docs_written"], 3)
        assert "start_time" not in info["info"], info

        # A continuous doc runs until the doc goes.
        eq(req("PUT", f"/_replicator/{run}", {"source": db_url(src), "target": db_url(cont), "create_target": True, "continuous": True})[0], 201)
        info = _wait(lambda: (lambda i: i if i.get("state") == "running" else None)(req("GET", f"/_scheduler/docs/_replicator/{run}")[1]))
        assert info, req("GET", f"/_scheduler/docs/_replicator/{run}")[1]
        assert info["id"].endswith("+continuous+create_target") and info["node"], info
        for k in ("source_proxy", "target_proxy", "start_time", "last_updated", "info"):
            assert k in info, (k, info)
        docs = req("GET", "/_scheduler/docs?states=running")[1]["docs"]
        assert any(d["doc_id"] == run for d in docs) and not any(d["doc_id"] == one for d in docs), docs
        jobs = [j for j in req("GET", "/_scheduler/jobs")[1]["jobs"] if j["doc_id"] == run]
        eq((len(jobs), jobs[0]["database"]), (1, "_replicator"))
        rev = req("GET", f"/_replicator/{run}")[1]["_rev"]
        eq(req("DELETE", f"/_replicator/{run}?rev={rev}")[0], 200)
        assert _wait(lambda: not [j for j in req("GET", "/_scheduler/jobs")[1]["jobs"] if j["doc_id"] == run]), "job outlived its doc"
        eq(req("GET", f"/_scheduler/docs/_replicator/{run}")[:2], (404, {"error": "not_found", "reason": "missing"}))
    finally:
        for d in (one, run):
            s, b, _ = req("GET", f"/_replicator/{d}")
            if s == 200:
                req("DELETE", f"/_replicator/{d}?rev={b['_rev']}")
        for d in (src, tgt, cont):
            req("DELETE", f"/{d}")


@test
def special_doc_fields():
    # _access (for per-document access, CouchDB's "access-ready") and the
    # replication state fields are stored as given; others are refused.
    db = new_db()
    try:
        body = {"_access": ["bob"], "_replication_state": "x", "_replication_state_time": "t", "_replication_state_reason": "r",
                "_replication_id": "i", "_replication_stats": {"a": 1}, "v": 1}
        eq(req("PUT", f"/{db}/a", body)[0], 201)
        got = req("GET", f"/{db}/a")[1]
        got.pop("_rev")
        eq(got, dict(body, _id="a"))
        eq(req("PUT", f"/{db}/b", {"_bogus": 1})[:2], (400, {"error": "doc_validation", "reason": "Bad special document member: _bogus"}))
        assert "access-ready" in req("GET", "/")[1]["features"]
    finally:
        req("DELETE", f"/{db}")


@test
def gzip_and_attachment_encoding():
    import gzip as gz
    import hashlib
    import zlib
    db = new_db()
    try:
        # Request bodies may come gzipped.
        hdr = {"Content-Type": "application/json", "Content-Encoding": "gzip"}
        eq(req("PUT", f"/{db}/z", gz.compress(b'{"a": 1}'), headers=hdr)[0], 201)
        eq(req("GET", f"/{db}/z")[1]["a"], 1)
        eq(req("POST", f"/{db}/_bulk_docs", gz.compress(b'{"docs": [{"_id": "y"}]}'), headers=hdr)[0], 201)
        eq(req("PUT", f"/{db}/w", b"not gzip", headers=hdr)[:2], (400, {"error": "bad_request", "reason": "Request body is not properly gzipped."}))
        eq(req("PUT", f"/{db}/w", b"{}", headers={"Content-Type": "application/json", "Content-Encoding": "deflate"})[:2],
           (415, {"error": "bad_content_type", "reason": "deflate is not a supported content encoding."}))

        # Compressible types are stored gzipped (digest of the gzipped
        # bytes, as zlib makes them) and served decoded unless the client
        # takes gzip.
        text = b"hello world " * 200
        rev = req("PUT", f"/{db}/d/a.txt", text, headers={"Content-Type": "text/plain"})[1]["rev"]
        c = zlib.compressobj(8, zlib.DEFLATED, 16 + 15, 8)
        stored = c.compress(text) + c.flush()
        digest = base64.b64encode(hashlib.md5(stored).digest()).decode()
        info = req("GET", f"/{db}/d?att_encoding_info=true")[1]["_attachments"]["a.txt"]
        eq(info, {"content_type": "text/plain", "revpos": 1, "digest": "md5-" + digest, "length": len(text), "stub": True,
                  "encoding": "gzip", "encoded_length": len(stored)})
        s, body, h = req("GET", f"/{db}/d/a.txt", headers={"Accept-Encoding": "gzip"}, raw=True)
        eq((s, h["Content-Encoding"], gz.decompress(body), h["ETag"], h["Accept-Ranges"]), (200, "gzip", text, f'"{digest}"', "none"))
        s, body, h = req("GET", f"/{db}/d/a.txt", raw=True)
        eq((s, h.get("Content-Encoding"), body, h["Content-Length"]), (200, None, text, str(len(text))))
        eq((h["Content-Security-Policy"], h["Cache-Control"]), ("sandbox", "must-revalidate"))
        eq(base64.b64decode(req("GET", f"/{db}/d?attachments=true")[1]["_attachments"]["a.txt"]["data"]), text)

        # Other types aren't, and take byte ranges.
        data = b"binarydata0123456789"
        rev = req("PUT", f"/{db}/d/b.bin?rev={rev}", data, headers={"Content-Type": "application/octet-stream"})[1]["rev"]
        assert "encoding" not in req("GET", f"/{db}/d?att_encoding_info=true")[1]["_attachments"]["b.bin"]
        s, body, h = req("GET", f"/{db}/d/b.bin", headers={"Range": "bytes=2-5"}, raw=True)
        eq((s, body, h["Content-Range"], h["Accept-Ranges"]), (206, b"nary", "bytes 2-5/20", "bytes"))
        eq(req("GET", f"/{db}/d/b.bin", headers={"Range": "bytes=-3"}, raw=True)[1], b"789")
        eq(req("GET", f"/{db}/d/b.bin", headers={"Range": "bytes=15-99"}, raw=True)[1], b"56789")
        eq(req("GET", f"/{db}/d/b.bin", headers={"Range": "bytes=0-"}, raw=True)[:2], (200, data))
        s, body, h = req("GET", f"/{db}/d/b.bin", headers={"Range": "bytes=0-1,4-5"}, raw=True)
        eq((s, h["Content-Type"].split(";")[0]), (206, "multipart/byteranges"))
        assert b"Content-Range: bytes 0-1/20\r\n\r\nbi" in body and b"Content-Range: bytes 4-5/20\r\n\r\nry" in body, body
        eq(req("GET", f"/{db}/d/b.bin", headers={"Range": "bytes=5-2"})[:2],
           (416, {"error": "requested_range_not_satisfiable", "reason": "Requested range not satisfiable"}))

        # Uploaded gzipped, kept so (identity length unknown) and decoded
        # on the way out.
        rev = req("PUT", f"/{db}/d/up.txt?rev={rev}", gz.compress(text),
                  headers={"Content-Type": "text/plain", "Content-Encoding": "gzip"})[1]["rev"]
        info = req("GET", f"/{db}/d?att_encoding_info=true")[1]["_attachments"]["up.txt"]
        eq((info["encoding"], info["length"] == info["encoded_length"]), ("gzip", True))
        eq(req("GET", f"/{db}/d/up.txt", raw=True)[1], text)
        eq(req("PUT", f"/{db}/d/br.txt?rev={rev}", b"x", headers={"Content-Type": "text/plain", "Content-Encoding": "br"})[:2],
           (415, {"error": "bad_content_type", "reason": "Only gzip and identity content-encodings are supported"}))
    finally:
        req("DELETE", f"/{db}")



NOUVEAU_DDOC = {
    # Built when searched (not in the background), to see it not built.
    "autoupdate": False,
    "nouveau": {
        "bar": {
            "default_analyzer": "standard",
            "index": """function (doc) {
              index("string", "foo", doc.foo, {store: true});
              index("double", "bar", doc.bar, {store: true});
              index("stored", "baz", doc.foo);
              if (doc.baz) {index("text", "txt", doc.baz);}
            }""",
        }
    }
}


def _nouveau_docs(db):
    docs = [
        {"_id": "doc4", "foo": "foo", "bar": 42, "baz": "hello hello"},
        {"_id": "doc3", "foo": "bar", "bar": 12.0, "baz": "hello there"},
        {"_id": "doc1", "foo": "baz", "bar": 0, "baz": "there"},
        {"_id": "doc2", "foo": "foobar", "bar": 100, "baz": "hi"},
    ]
    s, b, _ = req("POST", f"/{db}/_bulk_docs", {"docs": docs})
    eq(s, 201)
    return b


def _nouveau_sig(index):
    """sha256 of term_to_binary({LuceneVersion, Analyzer, #{}, Def}), as
    nouveau_util signs an index."""
    import hashlib
    def b(x):
        x = x.encode()
        return bytes([109]) + len(x).to_bytes(4, "big") + x
    term = bytes([131, 104, 4, 97, index["lucene_version"]]) + b(index["default_analyzer"]) + bytes([116, 0, 0, 0, 0]) + b(index["index"])
    return hashlib.sha256(term).hexdigest()


@test
def nouveau_search():
    s, _, _ = req("POST", "/_nouveau_analyze", {"analyzer": "standard", "text": "hello there"})
    if s != 404:
        _nouveau_enabled()
        req("PUT", "/_node/_local/_config/nouveau/enable", "false", headers={"X-Couch-Persist": "false"})
    try:
        _nouveau_disabled()
    finally:
        if s != 404:
            req("PUT", "/_node/_local/_config/nouveau/enable", "true", headers={"X-Couch-Persist": "false"})


def _nouveau_disabled():
    # As CouchDB ships ([nouveau] enable = false): nothing to be found,
    # though new design documents still get a lucene_version.
    missing = {"error": "not_found", "reason": "missing"}
    db = new_db()
    try:
        eq(req("PUT", f"/{db}/_design/foo", NOUVEAU_DDOC)[0], 201)
        eq(req("GET", f"/{db}/_design/foo")[1]["nouveau"]["bar"]["lucene_version"], 10)
        eq(req("POST", "/_nouveau_analyze", {"analyzer": "standard", "text": "hi"})[:2], (404, missing))
        eq(req("GET", "/_nouveau_analyze")[:2], (405, {"error": "method_not_allowed", "reason": "Only POST allowed"}))
        eq(req("GET", f"/{db}/_design/foo/_nouveau/bar?q=*:*")[:2], (404, missing))
        eq(req("POST", f"/{db}/_design/foo/_nouveau/bar", {"q": "*:*"})[:2], (404, missing))
        eq(req("GET", f"/{db}/_design/foo/_nouveau_info/bar")[:2], (404, missing))
        eq(req("POST", f"/{db}/_nouveau_cleanup", {})[:2], (202, {"ok": True}))
        eq(req("POST", f"/{db}/_nouveau_cleanup", b"{}", headers={"Content-Type": "text/plain"})[:2],
           (415, {"error": "bad_content_type", "reason": "Content-Type must be application/json"}))
        # Nor the older text search (dreyfus), without its Clouseau.
        nosearch = (503, {"error": "service unavailable", "reason": "Search is not available"})
        eq(req("PUT", f"/{db}/_design/s", {"indexes": {"i": {"index": "function(doc){index('default', doc.a)}"}}})[0], 201)
        eq(req("GET", f"/{db}/_design/s/_search/i?q=a")[:2], nosearch)
        eq(req("POST", f"/{db}/_design/s/_search/i", {"q": "a"})[:2], nosearch)
        eq(req("GET", f"/{db}/_design/s/_search_info/i")[:2], nosearch)
        eq(req("PUT", f"/{db}/_design/s/_search/i", {})[:2], (405, {"error": "method_not_allowed", "reason": "Only GET,POST allowed"}))
        eq(req("POST", "/_search_analyze", {"analyzer": "standard", "text": "hi"})[:2], nosearch)
        eq(req("POST", f"/{db}/_search_cleanup", {})[:2], (202, {"ok": True}))
        eq(req("GET", f"/{db}/_search_cleanup")[:2], (405, {"error": "method_not_allowed", "reason": "Only POST allowed"}))
        # Mango: no nouveau (or text) indexes; $text alone matches anything.
        unavailable = lambda kind: (503, {"error": "required index service unavailable", "reason": kind})
        mango_idx = {"type": "nouveau", "index": {"fields": [{"name": "foo", "type": "string"}]}}
        eq(req("POST", f"/{db}/_index", mango_idx)[:2], unavailable("nouveau"))
        eq(req("POST", f"/{db}/_index", dict(mango_idx, type="text"))[:2], unavailable("text"))
        eq(req("POST", f"/{db}/_index", dict(mango_idx, type="bogus"))[:2], (400, {"error": "invalid_index", "reason": "Invalid type for index: bogus"}))
        eq(req("PUT", f"/{db}/d1", {"a": "hello"})[0], 201)
        b = req("POST", f"/{db}/_find", {"selector": {"$text": "hello"}})[1]
        eq(([d["_id"] for d in b["docs"]], b["warning"]), (["d1"], "No matching index found, create an index to optimize query time."))
        # Edits of existing design documents are left alone.
        rev = req("GET", f"/{db}/_design/foo")[1]["_rev"]
        eq(req("PUT", f"/{db}/_design/foo?rev={rev}", {"nouveau": {"idx2": {"index": "f"}}})[0], 201)
        assert "lucene_version" not in req("GET", f"/{db}/_design/foo")[1]["nouveau"]["idx2"]
    finally:
        req("DELETE", f"/{db}")


@test
def nouveau_purges():
    """Purged docs leave search indexes without a rebuild, matching an
    index built afresh, and each index keeps a purge checkpoint."""
    if req("POST", "/_nouveau_analyze", {"analyzer": "standard", "text": "x"})[0] == 404:
        return  # Nouveau is off (CouchDB as shipped).
    db = "t" + uuid.uuid4().hex[:12]
    eq(req("PUT", f"/{db}?q=1")[0], 201)
    CREATED.append(db)
    fun = 'function(doc) { index("double", "n", doc.n, {store: true}); if (doc.t) index("text", "t", doc.t); }'
    ddoc = lambda f: {"autoupdate": False, "nouveau": {"idx": {"index": f}}}
    search = lambda dd, q: req("GET", f"/{db}/_design/{dd}/_nouveau/idx?q={q}&sort=%22n%22&limit=1000")[1]
    hits = lambda b: [h["id"] for h in b["hits"]]
    prompts = lambda: req("GET", "/_node/_local/_stats/couchdb/query_server/process_prompts")[1]["value"]
    try:
        _, res, _ = req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": f"d{i:04}", "n": i, "t": f"w{i % 10} common"} for i in range(300)]})
        revs = {r["id"]: r["rev"] for r in res}
        req("POST", f"/{db}/_bulk_docs", {"new_edits": False, "docs": [{"_id": "c", "_rev": "1-aaaa", "n": -1, "t": "alpha"}, {"_id": "c", "_rev": "1-bbbb", "n": -1, "t": "beta"}]})
        req("PUT", f"/{db}/_design/s", ddoc(fun))
        eq((search("s", "t:beta")["total_hits"], search("s", "*:*")["total_hits"]), (1, 301))
        sig = req("GET", f"/{db}/_design/s/_nouveau_info/idx")[1]["search_index"]["signature"]
        cp = f"/{db}/_local/purge-nouveau-{sig}"
        b = req("GET", cp)[1]
        eq((b["_rev"], b["purge_seq"], b["indexname"], b["ddoc_id"], b["signature"], b["type"]), ("0-1", 0, "idx", "_design/s", sig, "nouveau"))
        eq(list(b), ["_id", "_rev", "purge_seq", "updated_on", "indexname", "ddoc_id", "signature", "type"])
        before = prompts()
        req("POST", f"/{db}/_purge", {"d0005": [revs["d0005"]], "c": ["1-bbbb"], "nope": ["1-x"]})
        eq((hits(search("s", "t:alpha")), search("s", "t:beta")["total_hits"], search("s", "*:*")["total_hits"]), (["c"], 0, 300))
        assert prompts() - before < 50, "purge rebuilt the search index"
        # The same as an index built after the purges.
        req("PUT", f"/{db}/_design/s2", ddoc(fun + " "))
        for q in ("*:*", "t:common", "t:alpha", "t:w5"):
            eq(hits(search("s", q)), hits(search("s2", q)), q)
        info = req("GET", f"/{db}/_design/s/_nouveau_info/idx")[1]["search_index"]
        eq((info["purge_seq"], info["num_docs"]), (3, 300))
        b = req("GET", cp)[1]
        eq((b["_rev"], b["purge_seq"]), ("0-1", 3))
        eq(req("GET", f"/{db}/_local_docs")[1]["rows"], [])
        eq(len(req("GET", f"/{db}/_local_docs?include_system=true")[1]["rows"]), 2)
        # Compaction keeps the purge infos a valid checkpoint still needs,
        # and drops those only an invalid one would.
        req("PUT", f"/{db}/_purged_infos_limit", 1)
        req("POST", f"/{db}/_purge", {"x1": ["1-a"]})
        def compact():
            req("POST", f"/{db}/_compact", {})
            for _ in range(100):
                if not req("GET", f"/{db}")[1]["compact_running"]:
                    break
                time.sleep(0.1)
            time.sleep(0.3)
            return len(req("GET", f"/{db}/_purged_infos")[1]["purged_infos"])
        sig2 = req("GET", f"/{db}/_design/s2/_nouveau_info/idx")[1]["search_index"]["signature"]
        req("DELETE", f"/{db}/_local/purge-nouveau-{sig2}")
        req("PUT", cp, {"purge_seq": 0, "updated_on": 0, "indexname": "idx", "ddoc_id": "_design/s", "signature": sig, "type": "nouveau"})
        eq(compact(), 4)
        req("PUT", cp, {"purge_seq": 0, "updated_on": 0, "indexname": "idx", "ddoc_id": "_design/s", "signature": "x" + sig[1:], "type": "nouveau"})
        eq(compact(), 1)
        # Cleanup removes the checkpoints of indexes no design doc defines.
        req("PUT", f"/{db}/_local/purge-nouveau-{sig2}", {"purge_seq": 4, "type": "nouveau"})
        req("DELETE", f"/{db}/_design/s2?rev={req('GET', f'/{db}/_design/s2')[1]['_rev']}")
        eq(req("POST", f"/{db}/_nouveau_cleanup", {})[0], 202)
        eq((req("GET", f"/{db}/_local/purge-nouveau-{sig2}")[0], req("GET", cp)[0]), (404, 200))
    finally:
        req("DELETE", f"/{db}")


def _nouveau_enabled():
    an = lambda a, t: req("POST", "/_nouveau_analyze", {"analyzer": a, "text": t})[:2]
    eq(an("standard", "hello there"), (200, {"tokens": ["hello", "there"]}))
    eq(an("standard", "ablanks@renovations.com"), (200, {"tokens": ["ablanks", "renovations.com"]}))
    eq(an("keyword", "ablanks@renovations.com"), (200, {"tokens": ["ablanks@renovations.com"]}))
    eq(an("english", "The dogs' running"), (200, {"tokens": ["dog", "run"]}))
    eq(an("bogus", "x"), (400, {"error": "bad_request", "reason": "bogus is not a valid analyzer name"}))
    eq(an("standard", ""), (400, {"error": "bad_request", "reason": "text must not be empty"}))
    eq(req("POST", "/_nouveau_analyze", {"text": "x"})[:2],
       (400, {"error": "bad_request", "reason": "'text' and 'analyzer' fields must be non-empty strings"}))

    db = new_db()
    url = f"/{db}/_design/foo/_nouveau/bar"
    ids = lambda b: [h["doc"]["_id"] if "doc" in h else h["id"] for h in b["hits"]]
    q = lambda **kw: req("POST", url, kw)
    try:
        eq(req("PUT", f"/{db}/_design/foo", NOUVEAU_DDOC)[0], 201)
        # Not built until searched.
        eq(req("GET", url + "?q=*:*&update=false")[:2], (404, {"error": "not_found", "reason": "Index does not exist"}))
        created = _nouveau_docs(db)
        s, b, _ = req("GET", url + "?q=*:*&include_docs=true")
        eq(s, 200)
        # Relevance ties break by _id.
        eq(ids(b), ["doc1", "doc2", "doc3", "doc4"])
        eq((b["total_hits"], b["total_hits_relation"], b["counts"], b["ranges"]), (4, "EQUAL_TO", {}, {}))
        assert b["update_latency"] > 0, b["update_latency"]
        eq(b["hits"][0]["fields"], {"bar": 0.0, "baz": "baz", "foo": "baz"})
        eq(b["hits"][0]["order"], [{"@type": "float", "value": 1.0}, {"@type": "string", "value": "doc1"}])
        eq(req("GET", url + "?q=*:*")[1]["update_latency"], 0)

        info = req("GET", f"/{db}/_design/foo/_nouveau_info/bar")[1]
        eq(info["name"], "_design/foo/bar")
        si = info["search_index"]
        eq((si["num_docs"], si["update_seq"], si["purge_seq"]), (4, 5, 0))
        assert si["disk_size"] > 0
        eq(si["signature"], _nouveau_sig(dict(NOUVEAU_DDOC["nouveau"]["bar"], lucene_version=10)))

        # BM25 as Lucene scores it: more hellos in as long a text rank higher.
        b = q(q="txt:hello")[1]
        eq(ids(b), ["doc4", "doc3"])
        scores = [h["order"][0]["value"] for h in b["hits"]]
        assert abs(scores[0] - 0.39608) < 1e-4 and abs(scores[1] - 0.27726) < 1e-4, scores
        eq(ids(q(q="txt:hello", sort="-<score>")[1]), ["doc4", "doc3"])
        eq(ids(q(q="txt:hello", sort="<score>")[1]), ["doc3", "doc4"])

        # Paging by bookmark; past the end, no hits and the same bookmark.
        b = q(q="*:*", limit=2)[1]
        eq(ids(b), ["doc1", "doc2"])
        b = q(q="*:*", limit=2, bookmark=b["bookmark"])[1]
        eq(ids(b), ["doc3", "doc4"])
        b2 = q(q="*:*", limit=2, bookmark=b["bookmark"])[1]
        eq((ids(b2), b2["bookmark"]), ([], b["bookmark"]))
        eq(q(q="*:*", limit=0)[:2], (400, {"error": "query_parse_error", "reason": "limit parameter must be greater than zero"}))

        # Query syntax.
        eq(ids(q(q="foo:bar")[1]), ["doc3"])
        eq(ids(q(q="txt:hel*")[1]), ["doc3", "doc4"])
        eq(ids(q(q='txt:"hello there"')[1]), ["doc3"])
        eq(ids(q(q="txt:helo~")[1]), ["doc3", "doc4"])
        eq(sorted(ids(q(q="foo:foo OR foo:bar")[1])), ["doc3", "doc4"])
        eq(ids(q(q="txt:hello AND -foo:foo")[1]), ["doc3"])
        eq(ids(q(q="bar:[10.0 TO 20.0]", locale="us")[1]), ["doc3"])
        eq(ids(q(q="bar:[10.0 TO 20.0]", locale="de")[1]), ["doc2"])
        eq(ids(q(q="bar:{0 TO 42]", sort="bar")[1]), ["doc3", "doc4"])
        eq(ids(q(q="bar>=42", sort="bar")[1]), ["doc4", "doc2"])
        eq(ids(q(q="bar:12")[1]), ["doc3"])
        s, b, _ = q(q="txt:(hello")
        eq((s, b["error"], b["reason"].startswith("Syntax Error, cannot parse txt:(hello")), (400, "bad_request", True))
        eq(q(q="txt:*llo")[:2], (400, {"error": "bad_request", "reason": "Leading wildcard is not allowed: txt:*llo"}))
        eq(q(q="bar:abc")[0], 400)

        # Sorting by string and number fields; text fields can't be.
        eq(ids(q(q="*:*", sort="foo")[1]), ["doc3", "doc1", "doc4", "doc2"])
        eq(ids(q(q="*:*", sort="-foo")[1]), ["doc2", "doc4", "doc1", "doc3"])
        eq(ids(q(q="*:*", sort="bar")[1]), ["doc1", "doc3", "doc4", "doc2"])
        eq(ids(req("GET", url + '?q=*:*&sort="-bar"')[1]), ["doc2", "doc4", "doc3", "doc1"])
        eq(ids(q(q="*:*", sort=["-bar", "foo"])[1]), ["doc2", "doc4", "doc3", "doc1"])
        eq(q(q="*:*", sort="bar", limit=1)[1]["hits"][0]["order"],
           [{"@type": "double", "value": 0.0}, {"@type": "string", "value": "doc1"}])
        eq(q(q="*:*", sort="txt")[:2], (400, {"error": "bad_request", "reason": "can't sort on field txt"}))
        eq(q(q="*:*", sort="a b")[:2], (400, {"error": "bad_request", "reason": "a b is not a valid sort parameter"}))
        # Pages of a field sort.
        b = q(q="*:*", sort="-bar", limit=3)[1]
        eq(ids(q(q="*:*", sort="-bar", bookmark=b["bookmark"])[1]), ["doc1"])

        # Facets.
        eq(q(q="*:*", counts=["foo"])[1]["counts"], {"foo": {"bar": 1, "baz": 1, "foo": 1, "foobar": 1}})
        r = {"bar": [{"label": "cheap", "min": 0, "max": 42}, {"label": "expensive", "min": 42, "min_inclusive": False, "max": 1000}]}
        eq(q(q="*:*", ranges=r)[1]["ranges"], {"bar": {"cheap": 3, "expensive": 1}})
        r = {"bar": [{"label": "cheap", "max": 42}, {"label": "expensive", "min": 42, "min_inclusive": False}]}
        eq(q(q="*:*", ranges=r)[1]["ranges"], {"bar": {"cheap": 3, "expensive": 1}})
        eq(q(q="*:*", ranges=r, top_n=1)[1]["ranges"], {"bar": {"cheap": 3}})
        eq(q(q="*:*", counts=["bar"])[:2], (400, {"error": "bad_request", "reason":
           "unexpected docvalues type SORTED_NUMERIC for field 'bar' (expected one of [SORTED, SORTED_SET]). Re-index with correct docvalues type."}))
        eq(q(q="*:*", top_n=1001)[:2], (400, {"error": "bad_request", "reason": "topN must be less than or equal to 1000"}))

        # Arguments.
        eq(req("GET", url)[:2], (400, {"error": "query_parse_error", "reason": "q parameter is mandatory"}))
        eq(req("GET", url + "?q=*:*&limit=abc")[:2], (400, {"error": "query_parse_error", "reason": "limit parameter must be an integer"}))
        eq(req("GET", url + "?q=*:*&update=yes")[:2], (400, {"error": "query_parse_error", "reason": 'Invalid value for update: "yes"'}))
        eq(req("POST", url, b"{}", headers={"Content-Type": "text/plain"})[0], 415)
        eq(req("PUT", url, {})[:2], (405, {"error": "method_not_allowed", "reason": "Only GET, POST allowed"}))
        eq(req("GET", f"/{db}/_design/foo/_nouveau/missing?q=*:*")[:2], (404, {"error": "not_found", "reason": "missing not found."}))

        # Deletions and purges leave the index.
        d1 = [c for c in created if c["id"] == "doc1"][0]
        eq(req("DELETE", f"/{db}/doc1?rev={d1['rev']}")[0], 200)
        eq(q(q="*:*")[1]["total_hits"], 3)
        si = req("GET", f"/{db}/_design/foo/_nouveau_info/bar")[1]["search_index"]
        eq((si["update_seq"], si["purge_seq"], si["num_docs"]), (6, 0, 3))
        d2 = [c for c in created if c["id"] == "doc2"][0]
        eq(req("POST", f"/{db}/_purge", {"doc2": [d2["rev"]]})[0], 201)
        eq(ids(q(q="*:*")[1]), ["doc3", "doc4"])
        dbinfo = req("GET", f"/{db}")[1]
        si = req("GET", f"/{db}/_design/foo/_nouveau_info/bar")[1]["search_index"]
        eq((si["update_seq"], si["purge_seq"]), (int(dbinfo["update_seq"].split("-")[0]), int(dbinfo["purge_seq"].split("-")[0])))
        eq(req("POST", f"/{db}/_nouveau_cleanup", {})[:2], (202, {"ok": True}))
        eq(q(q="*:*")[1]["total_hits"], 2)

        # One value of a field stored several times comes back as a list.
        eq(req("PUT", f"/{db}/c", {"colors": ["red", "green"]})[0], 201)
        many = {"nouveau": {"c": {"index": "function(doc){ (doc.colors || []).forEach(function(c){ index('string', 'color', c, {store: true}); }); }"}}}
        eq(req("PUT", f"/{db}/_design/many", many)[0], 201)
        b = req("POST", f"/{db}/_design/many/_nouveau/c", {"q": "*:*"})[1]
        eq([h["fields"] for h in b["hits"]], [{"color": ["red", "green"]}])

        # Index functions that throw index nothing; conflicting field types
        # stop the index.
        bad = {"nouveau": {"t": {"index": "function(doc){ throw(new Error('x')); }"},
                           "c": {"index": "function(doc){ index('string', 'foo', 'bar'); index('double', 'foo', 12.0); }"}}}
        eq(req("PUT", f"/{db}/_design/bad", bad)[0], 201)
        eq(req("POST", f"/{db}/_design/bad/_nouveau/t", {"q": "*:*"})[1]["hits"], [])
        eq(req("POST", f"/{db}/_design/bad/_nouveau/c", {"q": "*:*"})[:2],
           (400, {"error": "bad_request", "reason": "field foo is of type DOUBLE not STRING"}))
    finally:
        req("DELETE", f"/{db}")

    # Background indexing (ken) builds search indexes too.
    db = new_db()
    try:
        _nouveau_docs(db)
        auto = {"nouveau": {"a": {"index": "function(doc){ if (doc.foo) index('string', 'foo', doc.foo); }"}}}
        eq(req("PUT", f"/{db}/_design/auto", auto)[0], 201)
        def built():
            s, b, _ = req("GET", f"/{db}/_design/auto/_nouveau_info/a")
            return s == 200 and b["search_index"]["update_seq"] >= 5
        assert _wait(built, secs=60), req("GET", f"/{db}/_design/auto/_nouveau_info/a")
    finally:
        req("DELETE", f"/{db}")

    # Mango nouveau indexes: the selector as a Lucene query.
    db = new_db()
    mids = lambda b: [d["_id"] for d in b["docs"]]
    find = lambda body: req("POST", f"/{db}/_find", body)
    try:
        docs = [{"_id": "doc4", "foo": "foo", "bar": 42, "baz": "hello hello there"}, {"_id": "doc3", "foo": "bar", "bar": 12.0, "baz": "hello"},
                {"_id": "doc1", "foo": "baz", "bar": 0, "baz": "there"}, {"_id": "doc2", "foo": "foobar", "bar": 100, "baz": "hi"}]
        eq(req("POST", f"/{db}/_bulk_docs", {"docs": docs})[0], 201)
        fields = [{"name": "foo", "type": "string"}, {"name": "bar", "type": "number"}, {"name": "baz", "type": "string"}]
        s, b, _ = req("POST", f"/{db}/_index", {"type": "nouveau", "index": {"fields": fields}})
        eq((s, b["result"]), (200, "created"))
        ddoc, name = b["id"], b["name"]
        stored = req("GET", f"/{db}/{ddoc}")[1]
        eq((stored["language"], stored["nouveau"][name]["lucene_version"]), ("query", 10))
        listed = [i for i in req("GET", f"/{db}/_index")[1]["indexes"] if i["type"] == "nouveau"]
        eq(listed, [{"ddoc": ddoc, "name": name, "type": "nouveau", "partitioned": False,
                     "def": {"default_analyzer": "keyword", "default_field": {}, "selector": {},
                             "fields": [{"foo": "string"}, {"bar": "number"}, {"baz": "string"}], "index_array_lengths": True}}])

        eq(mids(find({"selector": {"bar": {"$gt": 5}}})[1]), ["doc2", "doc3", "doc4"])
        eq(mids(find({"selector": {"foo": {"$eq": "foo"}}})[1]), ["doc4"])
        eq(mids(find({"selector": {"$text": "hello"}})[1]), ["doc4", "doc3"])
        eq(mids(find({"sort": [{"bar:number": "asc"}], "selector": {"bar": {"$gt": 5}}})[1]), ["doc3", "doc4", "doc2"])
        eq(mids(find({"sort": [{"foo:string": "asc"}], "selector": {"bar": {"$gte": 0}}})[1]), ["doc3", "doc1", "doc4", "doc2"])
        eq(mids(find({"selector": {"bar": {"$gt": 5}}, "fields": ["_id"], "limit": 1, "skip": 1})[1]), ["doc3"])
        # A bookmark is ignored (mango_cursor_nouveau:get_bookmark/1 wants
        # a list, and is given a string).
        b = find({"selector": {"bar": {"$gt": 5}}, "limit": 2})[1]
        eq(mids(find({"selector": {"bar": {"$gt": 5}}, "bookmark": b["bookmark"]})[1]), ["doc2", "doc3", "doc4"])
        ex = req("POST", f"/{db}/_explain", {"selector": {"bar": {"$gt": 5}}})[1]
        eq((ex["index"]["type"], ex["query"], ex["partition"], ex["sort"]), ("nouveau", "(bar_3anumber:{5 TO *])", None, None))
        # A field the index lacks: another index serves it.
        eq(find({"selector": {"qux": "a"}})[1]["warning"], "No matching index found, create an index to optimize query time.")
        # Only a new design doc's index is given a lucene_version, so the
        # same index differs from it once, and is saved without one: a
        # Lucene 9 index, which can't be made.
        eq(req("POST", f"/{db}/_index", {"type": "nouveau", "index": {"fields": fields}})[1]["result"], "created")
        eq(req("POST", f"/{db}/_index", {"type": "nouveau", "index": {"fields": fields}})[1]["result"], "exists")
        s, b = find({"selector": {"bar": {"$gt": 5}}})[:2]
        eq((s, b["error"]), (500, "nouveau_search_error"))
        eq(req("DELETE", f"/{db}/_index/{ddoc}/nouveau/{name}")[:2], (200, {"ok": True}))
        eq(req("GET", f"/{db}/{ddoc}")[0], 404)
    finally:
        req("DELETE", f"/{db}")

    # Partitioned: the partition's documents only.
    db = new_db_partitioned()
    try:
        docs = [{"_id": "foo:doc4", "foo": "foo", "bar": 42}, {"_id": "bar:doc3", "foo": "bar", "bar": 12.0},
                {"_id": "foo:doc1", "foo": "baz", "bar": 0}, {"_id": "bar:doc2", "foo": "foobar", "bar": 100}]
        eq(req("POST", f"/{db}/_bulk_docs", {"docs": docs})[0], 201)
        eq(req("PUT", f"/{db}/_design/foo", NOUVEAU_DDOC)[0], 201)
        purl = f"/{db}/_partition/foo/_design/foo/_nouveau/bar"
        eq([h["id"] for h in req("GET", purl + "?q=*:*")[1]["hits"]], ["foo:doc1", "foo:doc4"])
        eq([h["id"] for h in req("POST", f"/{db}/_partition/bar/_design/foo/_nouveau/bar", {"q": "*:*"})[1]["hits"]],
           ["bar:doc2", "bar:doc3"])
        eq(req("GET", purl + "?q=*:*&partition=bar")[:2],
           (400, {"error": "bad_request", "reason": "Conflicting value for `partition` in query string"}))
        idx = {"type": "nouveau", "index": {"fields": [{"name": "foo", "type": "string"}, {"name": "bar", "type": "number"}]}}
        eq(req("POST", f"/{db}/_index", idx)[0], 200)
        eq([d["_id"] for d in req("POST", f"/{db}/_partition/foo/_find", {"selector": {"foo": {"$eq": "foo"}}})[1]["docs"]], ["foo:doc4"])
        eq([d["_id"] for d in req("POST", f"/{db}/_partition/bar/_find", {"selector": {"foo": {"$eq": "bar"}}})[1]["docs"]], ["bar:doc3"])
    finally:
        req("DELETE", f"/{db}")


def new_db_partitioned():
    name = "t" + uuid.uuid4().hex[:12]
    s, _, _ = req("PUT", "/" + name + "?partitioned=true")
    assert s == 201, s
    CREATED.append(name)
    return name


def _term(v):
    """term_to_binary(V, [{minor_version, 1}]) of JSON as EJSON, without
    the leading 131."""
    def b(x):
        x = x.encode() if isinstance(x, str) else x
        return bytes([109]) + len(x).to_bytes(4, "big") + x
    if v is True or v is False or v is None:
        a = {True: "true", False: "false", None: "null"}[v].encode()
        return bytes([100]) + len(a).to_bytes(2, "big") + a
    if isinstance(v, int) and 0 <= v <= 255:
        return bytes([97, v])
    if isinstance(v, int):
        return bytes([98]) + v.to_bytes(4, "big", signed=True)
    if isinstance(v, (str, bytes)):
        return b(v)
    if isinstance(v, list):
        return (bytes([108]) + len(v).to_bytes(4, "big") + b"".join(_term(x) for x in v) + bytes([106])) if v else bytes([106])
    if isinstance(v, dict):
        pairs = [bytes([104, 2]) + b(k) + _term(x) for k, x in v.items()]
        inner = (bytes([108]) + len(pairs).to_bytes(4, "big") + b"".join(pairs) + bytes([106])) if pairs else bytes([106])
        return bytes([104, 1]) + inner
    raise ValueError(v)


@test
def chttpd_parity():
    import hashlib
    # The welcome and what every response carries.
    s, b, h = req("GET", "/")
    eq((b["version"], "quickjs" in b["features"], "partitioned" in b["features"]), ("3.5.3", True, True))
    assert h["Server"].startswith("CouchDB/3.5.3 ("), h["Server"]
    assert len(h["X-Couch-Request-ID"]) == 10 and h["X-CouchDB-Body-Time"] == "0", dict(h)
    # HEAD has the length of what GET sends.
    s, body, h = req("GET", "/", raw=True)
    s2, body2, h2 = req("HEAD", "/", raw=True)
    eq((s2, body2, h2["Content-Length"]), (200, b"", str(len(body))))
    # Methods not allowed are named, in Allow too.
    for path, allowed in [("/_all_dbs", "GET,HEAD"), ("/_uuids", "GET"), ("/_up", "GET,HEAD"), ("/_membership", "GET"),
                          ("/_dbs_info", "GET,HEAD,POST"), ("/_db_updates", "GET")]:
        s, b, h = req("PUT", path, {})
        eq((s, b, h["Allow"]), (405, {"error": "method_not_allowed", "reason": f"Only {allowed} allowed"}, allowed), path)
    eq(req("POST", "/_session", {})[:2], (400, {"error": "bad_request", "reason": "request body must contain a username"}))
    eq(req("POST", "/_session", {"name": "a", "username": "b"})[:2],
       (400, {"error": "bad_request", "reason": "request body contains different usernames"}))
    eq(req("GET", "/_cluster_setup")[:2], (200, {"state": "single_node_enabled"}))
    eq(req("POST", "/_cluster_setup", {"action": "finish_cluster"})[:2], (400, {"error": "bad_request", "reason": "Cluster is already finished"}))
    # No _global_changes database, no _db_updates; with one, databases'
    # creations, updates and deletions.
    if req("GET", "/_global_changes")[0] == 404:
        eq(req("GET", "/_db_updates")[:2], (404, {"error": "not_found", "reason": "Database does not exist."}))
        eq(req("PUT", "/_global_changes")[0], 201)
        try:
            db = new_db()
            eq(req("PUT", f"/{db}/x", {})[0], 201)
            req("DELETE", f"/{db}")
            def events():
                rows = req("GET", "/_db_updates")[1]["results"]
                got = sorted(r["type"] for r in rows if r["db_name"] == db)
                return got if got == ["created", "deleted", "updated"] else None
            assert _wait(events, secs=20), req("GET", "/_db_updates")[1]
            s, b, _ = req("GET", "/_db_updates?feed=longpoll&limit=1")
            eq((s, len(b["results"]), sorted(b)), (200, 1, ["last_seq", "results"]))
            eq(req("GET", "/_db_updates?limit=x")[:2], (400, {"error": "bad_request", "reason": "invalid_integer"}))
        finally:
            req("DELETE", "/_global_changes")

    db = new_db()
    try:
        # Revision ids as couch_db:new_revid/1 makes them.
        body = {"a": 1, "b": [1, "x", None, True, {}], "c": {"d": "é"}}
        rev = req("PUT", f"/{db}/r", body)[1]["rev"]
        want = hashlib.md5(bytes([131]) + _term([False, 0, 0, body, []])).hexdigest()
        eq(rev, "1-" + want)
        body2 = {"a": 2}
        rev2 = req("PUT", f"/{db}/r?rev={rev}", body2)[1]["rev"]
        eq(rev2, "2-" + hashlib.md5(bytes([131]) + _term([False, 1, bytes.fromhex(want), body2, []])).hexdigest())

        # With attachments=true, a client taking multipart/related (or
        # anything) gets the attachments in parts.
        doc = {"x": 1, "_attachments": {"b.bin": {"content_type": "application/octet-stream", "data": "AAEC"}}}
        eq(req("PUT", f"/{db}/m", doc)[0], 201)
        s, body, h = req("GET", f"/{db}/m?attachments=true", headers={"Accept": "*/*"}, raw=True)
        eq((s, h["Content-Type"].split(";")[0], h["Content-Length"]), (200, "multipart/related", str(len(body))))
        boundary = h["Content-Type"].split('boundary="')[1].rstrip('"')
        assert body.endswith(f"\r\n--{boundary}--".encode()), body
        assert b'Content-Disposition: attachment; filename="b.bin"\r\nContent-Type: application/octet-stream\r\nContent-Length: 3\r\n\r\n\x00\x01\x02' in body, body
        assert b'"follows":true' in body
        eq(req("GET", f"/{db}/m?attachments=true")[1]["_attachments"]["b.bin"]["data"], "AAEC")
        s, body, h = req("GET", f"/{db}/m?open_revs=all", headers={"Accept": "*/*"}, raw=True)
        eq(h["Content-Type"].split(";")[0], "multipart/mixed")

        # Size limits, as CouchDB enforces them.
        c, np = "/_node/_local/_config", {"X-Couch-Persist": "false"}
        limits = {"couchdb/max_document_size": "100", "couchdb/max_attachment_size": "10", "couchdb/max_document_id_length": "5",
                  "chttpd/max_http_request_size": "2000"}
        before = {k: req("GET", f"{c}/{k}")[1] for k in limits}
        try:
            for k, v in limits.items():
                req("PUT", f"{c}/{k}", v, headers=np)
            # (A PUT's id is the URL's, after this check: the reason is empty.)
            eq(req("PUT", f"/{db}/big", {"x": "y" * 200})[:2], (413, {"error": "document_too_large", "reason": ""}))
            eq(req("POST", f"/{db}/_bulk_docs", {"docs": [{"_id": "ok"}, {"_id": "big2", "x": "y" * 200}]})[:2],
               (413, {"error": "document_too_large", "reason": "big2"}))
            eq(req("PUT", f"/{db}/longid1", {})[:2], (400, {"error": "illegal_docid", "reason": "Document id is too long"}))
            eq(req("PUT", f"/{db}/a/f.bin", b"0123456789abc", headers={"Content-Type": "application/octet-stream"})[:2],
               (413, {"error": "attachment_too_large", "reason": "f.bin"}))
            eq(req("PUT", f"/{db}/b", {"_attachments": {"g.bin": {"content_type": "application/octet-stream",
                                                                  "data": base64.b64encode(b"0123456789abc").decode()}}})[:2],
               (413, {"error": "attachment_too_large", "reason": "g.bin"}))
            eq(req("PUT", f"/{db}/c", {"x": "y" * 3000})[:2], (413, {"error": "too_large", "reason": "the request entity is too large"}))
        finally:
            for k, v in before.items():
                if isinstance(v, str):
                    req("PUT", f"{c}/{k}", v, headers=np)
                else:
                    req("DELETE", f"{c}/{k}", headers=np)

        # CORS: off by default; on, for its origins.
        origin = {"Origin": "http://example.com"}
        assert "Access-Control-Allow-Origin" not in req("GET", f"/{db}", headers=origin)[2]
        c, np = "/_node/_local/_config", {"X-Couch-Persist": "false"}
        old = {k: req("GET", f"{c}/{k}")[1] for k in ("chttpd/enable_cors", "cors/origins", "cors/credentials")}
        try:
            req("PUT", f"{c}/chttpd/enable_cors", "true", headers=np)
            req("PUT", f"{c}/cors/origins", "http://example.com", headers=np)
            req("PUT", f"{c}/cors/credentials", "true", headers=np)
            s, b, h = req("GET", f"/{db}", headers=origin)
            eq((h["Access-Control-Allow-Origin"], h["Access-Control-Allow-Credentials"], h["Access-Control-Expose-Headers"]),
               ("http://example.com", "true",
                "content-type, cache-control, accept-ranges, etag, server, x-couch-request-id, x-couch-update-newrev, x-couchdb-body-time"))
            assert "Access-Control-Allow-Origin" not in req("GET", f"/{db}", headers={"Origin": "http://EXAMPLE.com"})[2]
            pre = dict(origin, **{"Access-Control-Request-Method": "GET", "Access-Control-Request-Headers": "Content-Type, Accept"})
            s, b, h = req("OPTIONS", f"/{db}", headers=pre, raw=True)
            eq((s, h["Access-Control-Allow-Origin"], h["Access-Control-Allow-Headers"], h["Access-Control-Max-Age"],
                h["Access-Control-Allow-Methods"]),
               (204, "http://example.com", "Content-Type, Accept", "600", "CONNECT, COPY, DELETE, GET, HEAD, OPTIONS, POST, PUT, TRACE"))
            # Asking for headers it doesn't allow: no preflight, just OPTIONS.
            s, b, h = req("OPTIONS", f"/{db}", headers=dict(pre, **{"Access-Control-Request-Headers": "X-Foo"}))
            eq((s, h["Allow"]), (405, "DELETE,GET,HEAD,POST"))
        finally:
            for k, v in old.items():
                if isinstance(v, str):
                    req("PUT", f"{c}/{k}", v, headers=np)
                else:
                    req("DELETE", f"{c}/{k}", headers=np)
        assert "Access-Control-Allow-Origin" not in req("GET", f"/{db}", headers=origin)[2]
    finally:
        req("DELETE", f"/{db}")


@test
def view_parity():
    db = new_db()
    try:
        docs = [{"_id": f"d{i}", "g": "ab"[i % 2], "n": i, "v": [5, 3, 9, 3, 1, 7][i]} for i in range(6)]
        eq(req("POST", f"/{db}/_bulk_docs", {"docs": docs})[0], 201)
        views = {"top": {"map": "function(d){ emit([d.g, d.n], d.v); }", "reduce": "_top_2"},
                 "bottom": {"map": "function(d){ emit([d.g, d.n], d.v); }", "reduce": "_bottom_3"},
                 "first": {"map": "function(d){ emit([d.g, d.n], d.v); }", "reduce": "_first"},
                 "last": {"map": "function(d){ emit([d.g, d.n], d.v); }", "reduce": "_last"}}
        eq(req("PUT", f"/{db}/_design/r", {"views": views})[0], 201)
        v = lambda name, q="": req("GET", f"/{db}/_design/r/_view/{name}{q}")[1]["rows"]
        # The new builtin reduces (3.5): distinct top and bottom values, and
        # the first and last rows' values.
        eq(v("top"), [{"key": None, "value": [9, 7]}])
        eq(v("bottom", "?group_level=1"), [{"key": ["a"], "value": [1, 5, 9]}, {"key": ["b"], "value": [3, 7]}])
        eq(v("first", "?group_level=1"), [{"key": ["a"], "value": 5}, {"key": ["b"], "value": 3}])
        eq(v("last"), [{"key": None, "value": 7}])
        eq(req("GET", f"/{db}/_design/r/_view/top?reduce=false&group=true")[:2],
           (400, {"error": "query_parse_error", "reason": "Invalid use of grouping on a map view."}))

        # Design documents are checked as couch_mrview checks them.
        bad = lambda views: req("PUT", f"/{db}/_design/bad", {"views": views})[:2]
        inv = lambda r: (400, {"error": "invalid_design_doc", "reason": r})
        eq(bad({"x": {"map": 7}}), inv("`views.x.map` field must have string type"))
        eq(bad({"x": {"reduce": "_sum"}}), inv("View `x` must contain map function"))
        eq(bad({"x": {"map": "function(d){}", "reduce": "_bogus"}}), inv("`_bogus` is not a supported reduce function."))
        eq(bad({"x": {"map": "function(d){}", "reduce": "_top_0"}}), inv("rank value must be between 1 and 100"))
        eq(bad({"x": {"map": "function(d){}", "reduce": "_top_x"}}), inv("invalid rank reducer"))
        eq(req("PUT", f"/{db}/_design/bad", {"views": "no"})[:2], inv("`views` field must have object type"))
        eq(req("PUT", f"/{db}/_design/bad", {"options": {"partitioned": 1}})[:2], inv("`options.partitioned` field must have boolean type"))
        s, b = bad({"x": {"map": "function(doc){ emit( }"}})
        eq((s, b["error"]), (400, "compilation_error"))
        assert b["reason"].startswith("Compilation of the map function in the 'x' view failed: ") and b["reason"].endswith(" (function(doc){ emit( })"), b
        s, b = bad({"x": {"map": "function(d){}", "reduce": "function(k, v) { return ; ]"}})
        eq((s, b["error"]), (400, "compilation_error"))
        assert b["reason"].startswith("Compilation of the reduce function in the 'x' view failed: "), b
    finally:
        req("DELETE", f"/{db}")


def main():
    failed = 0
    try:
        for t in TESTS:
            try:
                t()
                print(f"ok   {t.__name__}")
            except Exception:
                failed += 1
                print(f"FAIL {t.__name__}")
                traceback.print_exc()
    finally:
        for name in CREATED:
            req("DELETE", "/" + name)
    print(f"\n{len(TESTS) - failed}/{len(TESTS)} passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
