#!/usr/bin/env bash
# Benchmark the RocksDB-engine build (rocksdb-experiment branch) on its own
# port and data directory, with the same dataset, phases and compaction
# measurement as the other two servers. Erlang CouchDB (15984) must be up:
# its replicator drives replication, as for the others.
set -uo pipefail
cd "$(dirname "$0")"
B="${COUCHDB_BENCH:-$HOME/couchdb-bench}"
BIN="$(cd .. && pwd)/target/release/couchrs"
OUT="${OUT:-results.json}"
GB="${TOTAL_GB:-100}"
U=http://admin:bench@127.0.0.1:35984
G=couchbench/target/release/couchbench

start() {
  RUST_LOG=warn nohup "$BIN" --data-dir "$B/data-rocks" --bind 127.0.0.1:35984 --admin admin:bench \
    < /dev/null > "$B/rocks.log" 2>&1 &
  until curl -sf http://127.0.0.1:35984/_up >/dev/null; do sleep 0.2; done
}
stop() {
  pkill -f "couch(db-)?rs --data-dir $B/data-rocks" || true
  while curl -sf -m 1 http://127.0.0.1:35984/_up >/dev/null; do sleep 0.2; done
}

stop
rm -rf "$B/data-rocks"
start
echo "== $(date) rocksdb ${GB} GB run"
$G --url "$U" --replicator http://admin:bench@127.0.0.1:15984 --name rocksdb --total-gb "$GB" --out "$OUT"
echo "== $(date) rocksdb phases done; disk: $(du -sh "$B/data-rocks" | cut -f1)"

# Compaction, measured like the others: fingerprint, restart for clean
# memory, sample RAM and disk throughout, fingerprint again.
python3 db_fingerprint.py "$U/bench" > "$B/fp-rocksdb-before.json"
stop; start
curl -s -o /dev/null "$U/bench"
sleep 5
pid=$(pgrep -f "couch(db-)?rs --data-dir $B/data-rocks" | head -1)
python3 monitor.py --pid "$pid" --dir "$B/data-rocks" --out "$B/mon-rocksdb.json" --csv "$B/mon-rocksdb.csv" &
mon=$!
sleep 2
$G --url "$U" --name rocksdb --total-gb "$GB" --phases compact --out "$OUT"
sleep 2
kill -TERM $mon; wait $mon
python3 - "$OUT" "$B/mon-rocksdb.json" <<'PY'
import json, sys
out, mon = sys.argv[1:]
r = json.load(open(out)); m = json.load(open(mon))
r["rocksdb"]["compact"]["resources"] = m
json.dump(r, open(out, "w"), indent=2)
print(f"rocksdb: peak RAM {m['rss_peak_bytes']/1e9:.2f} GB (start {m['rss_start_bytes']/1e9:.2f}), "
      f"peak disk {m['disk_peak_bytes']/1e9:.1f} GB (+{m['disk_peak_extra_bytes']/1e9:.1f} GB over start)")
PY
python3 db_fingerprint.py "$U/bench" > "$B/fp-rocksdb-after.json"
cmp -s "$B/fp-rocksdb-before.json" "$B/fp-rocksdb-after.json" && echo "rocksdb: fingerprint identical after compaction" \
  || { echo "rocksdb: FINGERPRINT CHANGED"; cat "$B/fp-rocksdb-before.json" "$B/fp-rocksdb-after.json"; }
python3 summarize.py "$OUT"
