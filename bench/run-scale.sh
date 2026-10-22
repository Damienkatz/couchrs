#!/usr/bin/env bash
# Connection scaling: 1..6 simulated hosts (loopback source addresses
# 127.0.0.2-7, 15K keep-alive clients each, up to 90K), closed-loop
# GET doc / PUT doc / GET attachment, against each server in turn. Each
# server is restarted first so every run starts from clean memory.
# Needs the loopback aliases:  sudo ifconfig lo0 alias 127.0.0.N up
#
# couchdb-rs builds come from $B/bin (copy them there first):
#   couchdb-rs-btree   main (B-tree engine)            data-rs,    25984
#   couchdb-rs-hybrid  rocksdb-experiment              data-rocks, 35984
#   couchdb-rs-inline  async-blocking (point reads     data-rocks, 35984
#                      off block_in_place)
set -uo pipefail
cd "$(dirname "$0")"
B="${COUCHDB_BENCH:-$HOME/couchdb-bench}"
OUT="${OUT:-results.json}"
S=couchbench/target/release/couchscale
export PATH="/opt/homebrew/opt/erlang@27/bin:$PATH"

up() { until curl -sf "http://127.0.0.1:$1/_up" >/dev/null; do sleep 0.3; done; }
down() { while curl -sf -m 1 "http://127.0.0.1:$1/_up" >/dev/null; do sleep 0.3; done; }

start_rs() { # binary data-dir port
  pkill -f -- "--data-dir $2 " || true
  down "$3"
  RUST_LOG=warn nohup "$1" --data-dir "$2" --bind "127.0.0.1:$3" --admin admin:bench \
    < /dev/null > "$B/scale-$(basename "$1").log" 2>&1 &
  up "$3"
}

start_erl() {
  pkill -f "$B/couchdb-erl" || true
  down 15984
  # The old VM can outlive its port; don't measure the wrong process.
  while pgrep -f "beam.smp.*couchdb-erl" >/dev/null; do sleep 0.3; done
  (cd "$B/couchdb-erl" && nohup ./bin/couchdb < /dev/null > "$B/erl.log" 2>&1 &)
  up 15984
}

run() { # name port pid-pattern
  local pid; pid=$(pgrep -f -- "$3" | head -1)
  [ -n "$pid" ] || { echo "no server process matching $3" >&2; exit 1; }
  sleep 10
  echo "== $(date) $1 (pid $pid)"
  $S --url "http://admin:bench@127.0.0.1:$2" --name "$1" --server-pid "$pid" --out "$OUT" ${SCALE_ARGS:-}
  # Let the client sockets' TIME_WAIT expire before the next server.
  sleep 60
}

start_rs "$B/bin/couchdb-rs-btree" "$B/data-rs" 25984
run rust 25984 "couchdb-rs-btree --data-dir $B/data-rs "
start_rs "$B/bin/couchdb-rs-hybrid" "$B/data-rocks" 35984
run hybrid 35984 "couchdb-rs-hybrid --data-dir $B/data-rocks "
start_rs "$B/bin/couchdb-rs-inline" "$B/data-rocks" 35984
run hybrid_inline 35984 "couchdb-rs-inline --data-dir $B/data-rocks "
start_erl
run erlang 15984 "beam.smp.*couchdb-erl"
echo "== $(date) done"
