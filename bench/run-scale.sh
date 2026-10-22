#!/usr/bin/env bash
# Connection scaling: 1..6 simulated hosts (loopback source addresses
# 127.0.0.2-7, 15K keep-alive clients each, up to 90K), closed-loop
# GET doc / PUT doc / GET attachment, against each server in turn.
# Needs the loopback aliases:  sudo ifconfig lo0 alias 127.0.0.N up
set -uo pipefail
cd "$(dirname "$0")"
B="${COUCHDB_BENCH:-$HOME/couchdb-bench}"
OUT="${OUT:-results.json}"
S=couchbench/target/release/couchscale
run() { # name port pid-pattern
  local pid; pid=$(pgrep -f "$3" | head -1)
  echo "== $(date) $1 (pid $pid)"
  $S --url "http://admin:bench@127.0.0.1:$2" --name "$1" --server-pid "$pid" --out "$OUT" ${SCALE_ARGS:-}
  # Let the client sockets' TIME_WAIT expire before the next server.
  sleep 60
}
run rust 25984 "couchdb-rs --data-dir $B/data-rs"
run hybrid 35984 "couchdb-rs --data-dir $B/data-rocks"
run erlang 15984 "beam.smp.*couchdb-erl"
echo "== $(date) done"
