#!/usr/bin/env bash
# Run the whole benchmark for one load-generator/server pair, on the load
# generator. Everything the Mac run measured, in the same order:
#   load, view, get, put, delete, replicate  (one couchbench run)
#   compact        (server restarted first; server-side monitor)
#   view_compact   (server-side monitor)
#   scale          (server restarted first; server-side monitor)
# Server-side samples are written on the server and fetched at the end.
#
#   pair.sh NAME KIND SERVER_IP HOSTS      e.g. pair.sh rust btree 172.31.3.104 ip1,...,ip6
set -uo pipefail
name=$1 kind=$2 srv=$3 hosts=$4
GB="${TOTAL_GB:-100}"
out=~/results-$name.json
U="http://admin:bench@$srv:5984"
if [ "$kind" = erlang ]; then REP="$U"; else REP="http://admin:bench@$srv:15984"; fi
S() { ssh -n -o BatchMode=yes -o StrictHostKeyChecking=accept-new "ec2-user@$srv" "$@"; }
log() { echo "== $(date -u +%FT%TZ) $name: $*"; }
now() { date +%s.%N; }

# Monitor the server process while "$@" runs on this node; the samples
# and the window's start/end land in ~/mon-$name-$phase.{csv,json,window}.
monitored() { # phase cmd...
  local phase=$1; shift
  local pid; pid=$(S "~/bin/srvctl.sh pid $kind")
  S "nohup sudo python3 ~/bin/monitor.py --pid $pid --dir /data --interval 1 \
      --out ~/mon-$name-$phase.json --csv ~/mon-$name-$phase.csv > /dev/null 2>&1 & echo \$! > ~/mon.pid"
  sleep 3
  local t0; t0=$(now)
  "$@"
  local t1; t1=$(now)
  sleep 2
  S 'sudo kill -TERM $(cat ~/mon.pid); while sudo kill -0 $(cat ~/mon.pid) 2>/dev/null; do sleep 0.3; done'
  echo "$t0 $t1" > ~/mon-$name-$phase.window
  scp -q -o BatchMode=yes "ec2-user@$srv:~/mon-$name-$phase.*" ~/ 2>/dev/null
}

log "fresh start"
S "~/bin/srvctl.sh stop $kind; sudo rm -rf /data/rs /data/couchdb /data/replicator; ~/bin/srvctl.sh start $kind"
[ "$kind" = erlang ] || S "~/bin/srvctl.sh repl-start"
rm -f "$out"

log "load, view, get, put, delete, replicate"
~/couchbench --url "$U" --replicator "$REP" --name "$name" --total-gb "$GB" \
  --phases load,view,get,put,delete,replicate --out "$out"

log "compact"
python3 ~/db_fingerprint.py "$U/bench" > ~/fp-$name-before.json
S "~/bin/srvctl.sh restart $kind"
sleep 5
monitored compact ~/couchbench --url "$U" --name "$name" --total-gb "$GB" --phases compact --out "$out"
python3 ~/db_fingerprint.py "$U/bench" > ~/fp-$name-after.json
cmp -s ~/fp-$name-before.json ~/fp-$name-after.json && log "fingerprint identical after compaction" || log "FINGERPRINT CHANGED"

log "view_compact"
monitored view_compact ~/couchbench --url "$U" --name "$name" --total-gb "$GB" --phases view_compact --out "$out"

log "scale"
S "~/bin/srvctl.sh restart $kind"
sleep 10
monitored scale ~/couchscale --url "$U" --name "$name" --hosts "$hosts" --out "$out" ${SCALE_ARGS:-}
log "done"
