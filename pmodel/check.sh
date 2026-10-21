#!/usr/bin/env bash
# Compile the model and run every test. Correct-protocol tests must find 0
# bugs; seeded-bug and reachability tests must find one.
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$PATH:$HOME/.dotnet/tools"
SCHEDULES="${SCHEDULES:-5000}"
p compile > /dev/null
fail=0
run() {
  local tc=$1 want=$2
  rm -rf PCheckerOutput
  local bugs
  bugs=$(p check -tc "$tc" -s "$SCHEDULES" 2>&1 | grep -oE 'Found [0-9]+ bug' | grep -oE '[0-9]+' | tail -1 || true)
  if { [ "$want" = pass ] && [ "$bugs" = 0 ]; } || { [ "$want" = bug ] && [ "$bugs" != 0 ]; }; then
    printf 'ok   %-27s (%s bugs, expected %s)\n' "$tc" "$bugs" "$want"
  else
    printf 'FAIL %-27s (%s bugs, expected %s)\n' "$tc" "$bugs" "$want"
    fail=1
  fi
}
run tcSmall pass
run tcConflicts pass
run tcCheckpointFirstBug bug
run tcCheckpointFirstDataLoss bug
run tcNoRevsHistoryBug bug
run tcReachConflicts bug
rm -rf PCheckerOutput
exit $fail
