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
