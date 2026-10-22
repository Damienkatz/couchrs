#!/usr/bin/env python3
"""Fetch every pair's results and server-side samples, and merge them.

usage: aws/collect.py [OUT.json]     (default bench/results-aws.json)

Reads aws/hosts.txt, copies results-NAME.json and mon-NAME-PHASE.* from
each load generator, then attaches server resources the way the Mac runs
did with a local monitor:
  compact.resources       the monitor summary for the compaction
  view_compact.resources  RSS/footprint/disk before and peak in the window
  scale.steps[i]          server footprint peak and CPU cores per window
"""
import csv
import json
import os
import subprocess
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(here, "..", "results-aws.json")
NAMES = {"btree": "rust", "erlang": "erlang", "hybrid": "hybrid", "inline": "hybrid_inline"}

hosts = [l.split() for l in open(os.path.join(here, "hosts.txt")) if l.startswith("lg-")]
tmp = tempfile.mkdtemp()
merged = {}
for row in hosts:
    kind, pub = row[0][3:], row[1]
    name = NAMES[kind]
    d = os.path.join(tmp, name)
    os.makedirs(d)
    subprocess.run(f"scp -q -o BatchMode=yes 'ec2-user@{pub}:results-{name}.json' 'ec2-user@{pub}:mon-{name}-*' {d}/",
                   shell=True, check=False)
    p = os.path.join(d, f"results-{name}.json")
    if not os.path.exists(p):
        print(f"{name}: no results yet")
        continue
    r = json.load(open(p))[name]

    def samples(phase):
        c, j = os.path.join(d, f"mon-{name}-{phase}.csv"), os.path.join(d, f"mon-{name}-{phase}.json")
        if not (os.path.exists(c) and os.path.exists(j)):
            return None, None
        summ = json.load(open(j))
        t0 = summ["t0_unix"]
        rows = [{k: float(v) for k, v in x.items()} for x in csv.DictReader(open(c))]
        for x in rows:
            x["unix"] = t0 + x["t_secs"]
        return summ, rows

    summ, _ = samples("compact")
    if summ and "compact" in r:
        r["compact"]["resources"] = summ

    summ, rows = samples("view_compact")
    vc = r.get("view_compact")
    if rows and vc and "started_unix" in vc:
        a, b = vc["started_unix"], vc["ended_unix"]
        before = [x for x in rows if x["unix"] < a][-1]
        win = [x for x in rows if a <= x["unix"] <= b] or [before]
        res = {
            "rss_before_bytes": before["rss_bytes"], "rss_peak_bytes": max(x["rss_bytes"] for x in win),
            "disk_before_bytes": before["dir_bytes"], "disk_peak_bytes": max(x["dir_bytes"] for x in win),
            "footprint_before_bytes": before["footprint_bytes"], "footprint_peak_bytes": max(x["footprint_bytes"] for x in win),
        }
        for k in ("rss", "disk", "footprint"):
            res[f"{k}_peak_extra_bytes"] = res[f"{k}_peak_bytes"] - res[f"{k}_before_bytes"]
        vc["resources"] = res

    summ, rows = samples("scale")
    if rows and "scale" in r:
        for st in r["scale"]["steps"]:
            a, b = st.get("window_start_unix"), st.get("window_end_unix")
            win = [x for x in rows if a <= x["unix"] <= b]
            if len(win) >= 2:
                st["server_footprint_bytes"] = max(x["footprint_bytes"] for x in win)
                st["server_cpu_cores"] = (win[-1]["cpu_secs"] - win[0]["cpu_secs"]) / (win[-1]["unix"] - win[0]["unix"])
    merged[name] = r
    print(f"{name}: phases {sorted(k for k in r if k != 'server')}")

json.dump(merged, open(out, "w"), indent=2)
print(out)
