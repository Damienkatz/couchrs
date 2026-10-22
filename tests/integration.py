#!/usr/bin/env python3
"""End-to-end checks of CouchDB API behaviour against a running server.

usage: python3 tests/integration.py [http://127.0.0.1:5984]
Stdlib only. Each test uses a fresh database.
"""
import base64
import json
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5984"


def req(method, path, body=None, headers=None, raw=False):
    data = None
    hdrs = {"Accept": "application/json"}
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


def new_db():
    name = "t" + uuid.uuid4().hex[:12]
    s, _, _ = req("PUT", "/" + name)
    assert s == 201, s
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
    s, b, _ = req("PUT", f"/{db}/_local/cp", {"_rev": "0-1", "seq": 3})
    eq(s, 409)
    s, b, _ = req("GET", f"/{db}/_local/cp")
    eq(b, {"_id": "_local/cp", "_rev": "0-2", "seq": 2})
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
    r = urllib.request.urlopen(BASE + f"/{db}/_changes?feed=continuous&timeout=300", timeout=10)
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
        "stats": {"map": "function(doc) { emit(null, doc.words); }", "reduce": "_stats"},
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
        eq(e.headers["Location"], "/_utils/")
    s, html, h = req("GET", "/_utils/", raw=True)
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
    stop = threading.Event()
    bad = []

    def check(what, s, b):
        # Deleted underneath, a write may see 404 (or 409/412 racing the
        # recreate), never a server error.
        if s >= 500:
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


def main():
    failed = 0
    for t in TESTS:
        try:
            t()
            print(f"ok   {t.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL {t.__name__}")
            traceback.print_exc()
    print(f"\n{len(TESTS) - failed}/{len(TESTS)} passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
