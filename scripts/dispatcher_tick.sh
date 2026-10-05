#!/usr/bin/env bash
# Dispatcher tick — registered as a `hermes cron --no-agent --script` job.
# Runs one pass of the dispatcher and exits. Exits 0 on success, non-zero only
# when the tick itself failed (never because an individual task failed).
set -uo pipefail

DISPATCHER_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DISPATCHER_HOME"

PY="${DISPATCHER_PYTHON:-python}"
"$PY" -m dispatcher.cli tick
status=$?

if [ "$status" -ne 0 ]; then
  echo "dispatcher tick failed with status $status" >&2
fi
exit 0
