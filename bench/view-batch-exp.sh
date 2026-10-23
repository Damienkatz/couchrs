#!/usr/bin/env bash
# Rebuild CouchRS's 100 GB view index from scratch at several write batch
# sizes; record build time and index file size for each.
set -uo pipefail
B="$HOME/couchdb-bench"; R=http://admin:bench@127.0.0.1:25984
BIN="$(cd "$(dirname "$0")/.." && pwd)/target/release/couchrs"
for N in ${SIZES:-500 5000 50000}; do
  pkill -f "couch(db-)?rs --data-dir $B/data-rs"; sleep 2
  COUCHDB_RS_VIEW_BATCH=$N RUST_LOG=warn nohup "$BIN" --data-dir "$B/data-rs" --bind 127.0.0.1:25984 --admin admin:bench < /dev/null > "$B/rs.log" 2>&1 &
  until curl -sf $R/_up >/dev/null; do sleep 0.2; done
  id="exp$N-$(date +%s)"
  curl -s -XPUT "$R/bench/_design/$id" -H 'Content-Type: application/json' \
    -d "{\"views\":{\"by_group\":{\"map\":\"function(doc) { /* $id */ if (doc.group !== undefined) { emit([doc.band, doc.group], doc.size); } }\",\"reduce\":\"_sum\"}}}" > /dev/null
  t0=$(python3 -c 'import time; print(time.time())')
  curl -s -m 7200 "$R/bench/_design/$id/_view/by_group?limit=1&reduce=false" > /dev/null
  t1=$(python3 -c 'import time; print(time.time())')
  curl -s "$R/bench/_design/$id/_info" | python3 -c "import json,sys; s=json.load(sys.stdin)['view_index']['sizes']; print('batch $N: build %.1f s, file %.2f GB, live %.2f GB' % ($t1-$t0, s['file']/1e9, s['active']/1e9))"
  rev=$(curl -s "$R/bench/_design/$id" | python3 -c 'import json,sys;print(json.load(sys.stdin)["_rev"])')
  curl -s -XDELETE "$R/bench/_design/$id?rev=$rev" > /dev/null
  curl -s -XPOST "$R/bench/_view_cleanup" -H 'Content-Type: application/json' > /dev/null
done
