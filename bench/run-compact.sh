#!/usr/bin/env bash
# Compact each server's loaded database, one at a time (CouchDB first, so
# its I/O evicts CouchRS's file from the page cache). Fingerprints before
# and after prove compaction preserved the data.
set -uo pipefail
cd "$(dirname "$0")"
OUT="${OUT:-results.json}"
B="${COUCHDB_BENCH:-$HOME/couchdb-bench}"
G=couchbench/target/release/couchbench
for s in "erlang 15984" "rust 25984"; do
  name=${s% *}; port=${s#* }
  U="http://admin:bench@127.0.0.1:$port"
  echo "== $(date) $name"
  python3 db_fingerprint.py "$U/bench" > "$B/fp-$name-before.json"
  # Restart the server so memory is sampled from a fresh process, not one
  # still holding memory from earlier requests (like the fingerprint above).
  ./servers.sh restart "$name"
  curl -s -o /dev/null "$U/bench"   # open the database before sampling starts
  sleep 5
  # Sample the server's memory and its data directory's size throughout.
  if [ "$name" = erlang ]; then pid=$(pgrep -f "beam.smp.*couchdb-erl" | head -1); dir="$B/data-erl"
  else pid=$(pgrep -f "couch(db-)?rs --data-dir $B/data-rs" | head -1); dir="$B/data-rs"; fi
  python3 monitor.py --pid "$pid" --dir "$dir" --out "$B/mon-$name.json" --csv "$B/mon-$name.csv" &
  mon=$!
  sleep 2
  $G --url "$U" --name "$name" --total-gb "${TOTAL_GB:-100}" --phases compact --out "$OUT"
  sleep 2
  kill -TERM $mon; wait $mon
  python3 - "$OUT" "$name" "$B/mon-$name.json" <<'PY'
import json, sys
out, name, mon = sys.argv[1:]
r = json.load(open(out)); m = json.load(open(mon))
r[name]["compact"]["resources"] = m
json.dump(r, open(out, "w"), indent=2)
print(f"{name}: peak RAM {m['rss_peak_bytes']/1e9:.2f} GB (start {m['rss_start_bytes']/1e9:.2f}), "
      f"peak disk {m['disk_peak_bytes']/1e9:.1f} GB (+{m['disk_peak_extra_bytes']/1e9:.1f} GB over start)")
PY
  python3 db_fingerprint.py "$U/bench" > "$B/fp-$name-after.json"
  if cmp -s "$B/fp-$name-before.json" "$B/fp-$name-after.json"; then
    echo "$name: fingerprint identical after compaction"
  else
    echo "$name: FINGERPRINT CHANGED"; cat "$B/fp-$name-before.json" "$B/fp-$name-after.json"
  fi
done
