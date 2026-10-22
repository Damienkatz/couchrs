#!/usr/bin/env bash
# Control the server on one benchmark node.
#   srvctl.sh start|stop|restart|pid erlang|btree|hybrid|inline
#   srvctl.sh repl-start          CouchDB replicator on :15984 (couchdb-rs nodes)
# The server listens on :5984 with admin/bench; data lives under /data.
set -uo pipefail
cmd=$1 kind=${2:-}
IMG=apache/couchdb:3.5.3

couch_ini() { # port data-dir
  cat <<EOF
; Benchmark configuration: single node, one shard (like couchdb-rs),
; no auto-compaction (couchdb-rs has none), QuickJS, quiet logs.
[couchdb]
single_node = true
max_document_size = 4294967296
js_engine = quickjs

[chttpd]
port = $1
bind_address = 0.0.0.0
max_http_request_size = 4294967296
; mochiweb caps connections at 65,536 by default; tokio's listen backlog is 1,024.
server_options = [{backlog, 1024}, {acceptor_pool_size, 32}, {max, 200000}]

[cluster]
q = 1
n = 1

[admins]
admin = bench

[log]
level = warning

; Auto-compaction (smoosh) off: give every built-in channel zero concurrency.
[smoosh.upgrade_dbs]
concurrency = 0
[smoosh.ratio_dbs]
concurrency = 0
[smoosh.slack_dbs]
concurrency = 0
[smoosh.upgrade_views]
concurrency = 0
[smoosh.ratio_views]
concurrency = 0
[smoosh.slack_views]
concurrency = 0
[smoosh.index_cleanup]
concurrency = 0
EOF
}

couch_start() { # name port dir
  mkdir -p "$3/data" "$3/etc"
  couch_ini "$2" > "$3/etc/bench.ini"
  sudo chown -R 5984:5984 "$3" 2>/dev/null || true
  sudo docker rm -f "$1" >/dev/null 2>&1
  sudo docker run -d --name "$1" --network host --ulimit nofile=2097152:2097152 \
    -e ERL_FLAGS="+Q 262144" \
    -v "$3/data:/opt/couchdb/data" -v "$3/etc/bench.ini:/opt/couchdb/etc/local.d/bench.ini" \
    "$IMG" >/dev/null
  until curl -sf "http://127.0.0.1:$2/_up" >/dev/null; do sleep 0.5; done
  for db in _users _replicator; do curl -s -o /dev/null -X PUT "http://admin:bench@127.0.0.1:$2/$db"; done
}

rs_pid() { pgrep -f -- "couchdb-rs-$kind --data-dir" | head -1; }

case "$cmd" in
  start)
    if [ "$kind" = erlang ]; then
      couch_start couchdb 5984 /data/couchdb
    else
      ulimit -n 2097152
      # Retry while the previous listener is still releasing the port.
      for _ in $(seq 1 60); do
        RUST_LOG=warn nohup ~/bin/couchdb-rs-$kind --data-dir /data/rs --bind 0.0.0.0:5984 --admin admin:bench \
          < /dev/null > ~/couchdb-rs-$kind.log 2>&1 &
        sleep 0.5
        until curl -sf http://127.0.0.1:5984/_up >/dev/null || [ -z "$(rs_pid)" ]; do sleep 0.3; done
        curl -sf http://127.0.0.1:5984/_up >/dev/null && break
        sleep 1
      done
    fi ;;
  stop)
    if [ "$kind" = erlang ]; then
      sudo docker stop -t 30 couchdb >/dev/null 2>&1
      while pgrep -f "beam.smp.*-name couchdb" >/dev/null; do sleep 0.3; done
    else
      pkill -f -- "couchdb-rs-$kind --data-dir"
      while [ -n "$(rs_pid)" ]; do sleep 0.3; done
    fi ;;
  restart) "$0" stop "$kind"; "$0" start "$kind" ;;
  pid)
    if [ "$kind" = erlang ]; then
      # The server's beam (the replicator container is not on this node).
      pgrep -f "beam.smp" | head -1
    else
      rs_pid
    fi ;;
  repl-start) couch_start couchdb-repl 15984 /data/replicator ;;
  *) echo "usage: $0 start|stop|restart|pid erlang|btree|hybrid|inline | repl-start" >&2; exit 2 ;;
esac
