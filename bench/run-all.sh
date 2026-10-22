#!/usr/bin/env bash
# Full benchmark on a fresh dataset: wipe and load both servers, run every
# phase, then compact both with resource sampling.
set -uo pipefail
cd "$(dirname "$0")"
./run-full.sh
./run-compact.sh
