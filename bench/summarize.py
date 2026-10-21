#!/usr/bin/env python3
"""Print a one-line correctness/speed summary per server from a results file."""
import json, sys
r = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "results.json"))
for n, d in r.items():
    l, v, p = d.get("load", {}), d.get("view", {}), d.get("replicate", {})
    put = d.get("put", {}).get("1KB", {}).get("ops_per_sec", 0)
    dele = d.get("delete", {}).get("1KB", {}).get("ops_per_sec", 0)
    print(f"{n:7} load {l.get('mb_per_sec', 0):.0f} MB/s err {l.get('errors')} | "
          f"view {v.get('docs_per_sec', 0):.0f} docs/s sum_ok {v.get('reduce_total') == v.get('reduce_expected')} | "
          f"repl {p.get('secs', 0):.1f}s ok {p.get('ok')} {p.get('target_docs')}/{p.get('source_docs')} | "
          f"put 1KB {put:.0f}/s delete 1KB {dele:.0f}/s")
