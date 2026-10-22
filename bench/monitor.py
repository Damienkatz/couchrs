#!/usr/bin/env python3
"""Sample a server's memory and its data directory's disk use until stopped.

usage: monitor.py --pid PID --dir DATA_DIR --out SUMMARY.json [--csv SAMPLES.csv] [--interval 1]

Every interval: resident memory of PID plus all its descendants, total bytes
of files under DATA_DIR, and free space on that volume. On SIGTERM/SIGINT it
writes a JSON summary with the starting values and the peaks.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("--pid", type=int, required=True)
ap.add_argument("--dir", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--csv")
ap.add_argument("--interval", type=float, default=1.0)
a = ap.parse_args()


def tree_rss(pid):
    """Resident bytes of pid and every descendant process."""
    out = subprocess.run(["ps", "-axo", "pid=,ppid=,rss="], capture_output=True, text=True).stdout
    kids, rss = {}, {}
    for line in out.splitlines():
        p, pp, r = (int(x) for x in line.split())
        kids.setdefault(pp, []).append(p)
        rss[p] = r * 1024
    total, stack = 0, [pid]
    while stack:
        p = stack.pop()
        total += rss.get(p, 0)
        stack.extend(kids.get(p, []))
    return total


def dir_bytes(d):
    total = 0
    for root, _, files in os.walk(d):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except FileNotFoundError:
                pass  # files appear and vanish during compaction swaps
    return total


def free_bytes(d):
    st = os.statvfs(d)
    return st.f_bavail * st.f_frsize


samples = []
csv = open(a.csv, "w") if a.csv else None
if csv:
    csv.write("t_secs,rss_bytes,dir_bytes,free_bytes\n")
t0 = time.time()
stop = False


def done(*_):
    global stop
    stop = True


signal.signal(signal.SIGTERM, done)
signal.signal(signal.SIGINT, done)
while not stop:
    s = (time.time() - t0, tree_rss(a.pid), dir_bytes(a.dir), free_bytes(a.dir))
    samples.append(s)
    if csv:
        csv.write("%.1f,%d,%d,%d\n" % s)
        csv.flush()
    time.sleep(a.interval)

start = samples[0]
peak_rss = max(s[1] for s in samples)
peak_dir = max(samples, key=lambda s: s[2])
json.dump({
    "samples": len(samples),
    "duration_secs": samples[-1][0],
    "rss_start_bytes": start[1],
    "rss_peak_bytes": peak_rss,
    "disk_start_bytes": start[2],
    "disk_peak_bytes": peak_dir[2],
    "disk_peak_extra_bytes": peak_dir[2] - start[2],
    "disk_peak_at_secs": peak_dir[0],
    "free_min_bytes": min(s[3] for s in samples),
}, open(a.out, "w"), indent=1)
