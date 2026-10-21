#!/usr/bin/env bash
# Start/stop the two benchmark servers.
#   erlang CouchDB 3.5.3 : http://127.0.0.1:15984  (data: ~/couchdb-bench/data-erl)
#   couchdb-rs           : http://127.0.0.1:25984  (data: ~/couchdb-bench/data-rs)
# Both use admin/bench.
set -euo pipefail
B="${COUCHDB_BENCH:-$HOME/couchdb-bench}"
RS_BIN="$(cd "$(dirname "$0")/.." && pwd)/target/release/couchdb-rs"
export PATH="/opt/homebrew/opt/erlang@27/bin:$PATH"

wait_up() {
  for _ in $(seq 1 100); do
    curl -sf "$1/_up" >/dev/null && return 0
    sleep 0.2
  done
  echo "server at $1 did not come up" >&2
  return 1
}

case "${1:-}" in
  start)
    if ! curl -sf http://127.0.0.1:15984/_up >/dev/null; then
      (cd "$B/couchdb-erl" && nohup ./bin/couchdb < /dev/null > "$B/erl.log" 2>&1 &)
    fi
    if ! curl -sf http://127.0.0.1:25984/_up >/dev/null; then
      RUST_LOG=warn nohup "$RS_BIN" --data-dir "$B/data-rs" --bind 127.0.0.1:25984 \
        --admin admin:bench < /dev/null > "$B/rs.log" 2>&1 &
    fi
    wait_up http://127.0.0.1:15984 && wait_up http://127.0.0.1:25984
    # Single-node setup: CouchDB expects its system databases to exist.
    for db in _users _replicator; do
      curl -s -o /dev/null -X PUT "http://admin:bench@127.0.0.1:15984/$db"
    done
    echo "erlang: $(curl -s http://127.0.0.1:15984/ | head -c 120)"
    echo "rust:   $(curl -s http://127.0.0.1:25984/ | head -c 120)"
    ;;
  stop)
    pkill -f "$B/couchdb-erl" || true
    pkill -f "couchdb-rs --data-dir $B/data-rs" || true
    for _ in $(seq 1 100); do
      curl -sf -m 1 http://127.0.0.1:15984/_up >/dev/null || curl -sf -m 1 http://127.0.0.1:25984/_up >/dev/null || break
      sleep 0.2
    done
    ;;
  *)
    echo "usage: $0 start|stop" >&2
    exit 2
    ;;
esac
