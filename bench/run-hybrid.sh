#!/usr/bin/env bash
# Hybrid engine (RocksDB documents, couch B-tree views): rebuild the view
# on the RocksDB server's loaded database and rerun the view phase, then
# measure view compaction after incremental churn on every server.
set -uo pipefail
cd "$(dirname "$0")"
B="${COUCHDB_BENCH:-$HOME/couchdb-bench}"
BIN="$(cd .. && pwd)/target/release/couchrs"
OUT="${OUT:-results.json}"
GB="${TOTAL_GB:-100}"
G=couchbench/target/release/couchbench

pkill -f "couch(db-)?rs --data-dir $B/data-rocks" || true
while curl -sf -m 1 http://127.0.0.1:35984/_up >/dev/null; do sleep 0.2; done
# The RocksDB-format view index is unused by this engine.
find "$B/data-rocks" -path '*mrview/*.rocks' -prune -exec rm -rf {} +
RUST_LOG=warn nohup "$BIN" --data-dir "$B/data-rocks" --bind 127.0.0.1:35984 --admin admin:bench \
  < /dev/null > "$B/hybrid.log" 2>&1 &
until curl -sf http://127.0.0.1:35984/_up >/dev/null; do sleep 0.2; done

echo "== $(date) hybrid view"
$G --url http://admin:bench@127.0.0.1:35984 --name hybrid --total-gb "$GB" --phases "${HYBRID_PHASES:-view,view_compact}" --out "$OUT"
echo "== $(date) rust (B-tree) view_compact"
$G --url http://admin:bench@127.0.0.1:25984 --name rust --total-gb "$GB" --phases view_compact --out "$OUT"
echo "== $(date) erlang view_compact"
$G --url http://admin:bench@127.0.0.1:15984 --name erlang --total-gb "$GB" --phases view_compact --out "$OUT"
echo "== $(date) done"
