#!/usr/bin/env bash
# View compaction with memory sampling: run the view_compact phase on one
# server while monitor.py samples its RSS every 0.1 s, then record the
# peak RSS and physical footprint during the compaction window, and their
# levels just before it.
#   view-compact-measure.sh NAME PORT PID INDEX_DIR
set -uo pipefail
cd "$(dirname "$0")"
name=$1 port=$2 pid=$3 dir=$4
OUT="${OUT:-results.json}"
B="${COUCHDB_BENCH:-$HOME/couchdb-bench}"
csv="$B/mon-vc-$name.csv"
t0=$(python3 -c 'import time; print(time.time())')
python3 monitor.py --pid "$pid" --dir "$dir" --out "$B/mon-vc-$name.json" --csv "$csv" --interval 0.1 &
mon=$!
couchbench/target/release/couchbench --url "http://admin:bench@127.0.0.1:$port" --name "$name" --total-gb "${TOTAL_GB:-100}" --phases view_compact --out "$OUT"
kill -TERM $mon; wait $mon
python3 - "$OUT" "$name" "$csv" "$t0" <<'PY'
import csv, json, sys
out, name, path, t0 = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
r = json.load(open(out)); vc = r[name]["view_compact"]
rows = [(t0 + float(x["t_secs"]), int(x["rss_bytes"]), int(x["dir_bytes"]), int(x["footprint_bytes"])) for x in csv.DictReader(open(path))]
a, b = vc["started_unix"], vc["ended_unix"]
before = [x for x in rows if x[0] < a][-1]
win = [x for x in rows if a <= x[0] <= b] or [before]
vc["resources"] = {"rss_before_bytes": before[1], "rss_peak_bytes": max(x[1] for x in win),
                   "disk_before_bytes": before[2], "disk_peak_bytes": max(x[2] for x in win),
                   "footprint_before_bytes": before[3], "footprint_peak_bytes": max(x[3] for x in win)}
vc["resources"]["footprint_peak_extra_bytes"] = vc["resources"]["footprint_peak_bytes"] - before[3]
vc["resources"]["rss_peak_extra_bytes"] = vc["resources"]["rss_peak_bytes"] - before[1]
vc["resources"]["disk_peak_extra_bytes"] = vc["resources"]["disk_peak_bytes"] - before[2]
json.dump(r, open(out, "w"), indent=2)
m = vc["resources"]
print(f"{name}: compaction {vc['secs']:.1f}s, RSS {before[1]/1e6:.0f} MB -> peak {m['rss_peak_bytes']/1e6:.0f} MB (+{m['rss_peak_extra_bytes']/1e6:.0f} MB), "
      f"footprint {before[3]/1e6:.0f} MB -> peak {m['footprint_peak_bytes']/1e6:.0f} MB (+{m['footprint_peak_extra_bytes']/1e6:.0f} MB), "
      f"disk +{m['disk_peak_extra_bytes']/1e6:.0f} MB")
PY
