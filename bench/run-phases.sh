#!/usr/bin/env bash
# Re-run the measurement phases against both servers' already-loaded
# databases (load results in the results file are kept). Servers run one
# at a time; each view build starts from a fresh index.
set -uo pipefail
cd "$(dirname "$0")"
GB="${TOTAL_GB:-100}"
OUT="${OUT:-results.json}"
PHASES="${PHASES:-view,get,put,delete,replicate}"
G=couchbench/target/release/couchbench
REP=http://admin:bench@127.0.0.1:15984
echo "== $(date) phases $PHASES"
$G --url http://admin:bench@127.0.0.1:15984 --replicator "$REP" --name erlang --total-gb "$GB" --phases "$PHASES" --out "$OUT"
echo "== $(date) erlang done"
$G --url http://admin:bench@127.0.0.1:25984 --replicator "$REP" --name rust --total-gb "$GB" --phases "$PHASES" --out "$OUT"
echo "== $(date) rust done"
python3 summarize.py "$OUT"
