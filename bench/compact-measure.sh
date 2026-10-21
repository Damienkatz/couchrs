#!/usr/bin/env bash
# Compact a database, time it, then report sizes and its fingerprint.
set -uo pipefail
U="${1:?db url}"
before=$(curl -s "$U" | python3 -c 'import json,sys; s=json.load(sys.stdin)["sizes"]; print("%.2f GB file, %.2f GB active" % (s["file"]/1e9, s["active"]/1e9))')
t0=$(python3 -c 'import time;print(time.time())')
curl -s -X POST "$U/_compact" -H 'Content-Type: application/json' > /dev/null
sleep 1
while curl -s "$U" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["compact_running"] else 1)'; do sleep 2; done
t1=$(python3 -c 'import time;print(time.time())')
after=$(curl -s "$U" | python3 -c 'import json,sys; s=json.load(sys.stdin)["sizes"]; print("%.2f GB file, %.2f GB active" % (s["file"]/1e9, s["active"]/1e9))')
python3 -c "print('compaction: %.1f s | before: $before | after: $after' % ($t1-$t0))"
python3 "$(dirname "$0")/db_fingerprint.py" "$U"
