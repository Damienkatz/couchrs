#!/usr/bin/env bash
# Full benchmark: wipe both servers, then run the whole suite against each
# in turn (never concurrently). Replication for both is driven by the
# Erlang CouchDB replicator, so the same client exercises both servers.
set -uo pipefail
cd "$(dirname "$0")"
B="${COUCHDB_BENCH:-$HOME/couchdb-bench}"
GB="${TOTAL_GB:-100}"
OUT="${OUT:-results.json}"
G=couchbench/target/release/couchbench
REP=http://admin:bench@127.0.0.1:15984

./servers.sh stop
rm -rf "$B"/data-erl/* "$B"/data-rs/*
./servers.sh start
rm -f "$OUT"
echo "== $(date) starting ${GB} GB run"
$G --url http://admin:bench@127.0.0.1:15984 --replicator "$REP" --name erlang --total-gb "$GB" --out "$OUT"
echo "== $(date) erlang done; disk: $(du -sh "$B/data-erl" | cut -f1)"
$G --url http://admin:bench@127.0.0.1:25984 --replicator "$REP" --name rust --total-gb "$GB" --out "$OUT"
echo "== $(date) rust done; disk: $(du -sh "$B/data-rs" | cut -f1)"
python3 summarize.py "$OUT"
