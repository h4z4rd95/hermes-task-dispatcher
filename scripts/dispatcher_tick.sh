#!/usr/bin/env bash
# Dispatcher tick — registered as a `hermes cron --no-agent --script` job.
# Runs one pass of the dispatcher and exits.
#
# FIX 2026-10-05 (T-2026-10-05-DISPATCHER-CRON-SYNC-001):
#   The original derived DISPATCHER_HOME from its own location (dirname/..).
#   Cron executes the *installed copy* of this script from $HERMES_HOME/scripts/,
#   so that resolved to $HERMES_HOME, which has no dispatcher package — every
#   tick died with ModuleNotFoundError and the trailing `exit 0` hid it from
#   the scheduler ("ok", silent). Now: the canonical dispatcher repo is named
#   explicitly (absolute path, overridable via DISPATCHER_HOME), and a failed
#   tick exits non-zero so hermes cron records it as a real failure.
#
# Deployment rule: the file at $HERMES_HOME/scripts/dispatcher_tick.sh MUST be
# a byte-identical copy of this file. `hermes cron --script` only resolves
# scripts inside $HERMES_HOME/scripts/, so the repo copy is the source of
# truth and the installed copy is the deployment target.
set -uo pipefail

# Canonical deployment of the dispatcher (single source of truth).
DISPATCHER_HOME="${DISPATCHER_HOME:-C:/Users/h4z4rd/.hermes-tools/hermes-task-dispatcher}"

if [ ! -f "$DISPATCHER_HOME/dispatcher/config.py" ]; then
  echo "dispatcher tick FAILED: no dispatcher package under DISPATCHER_HOME=$DISPATCHER_HOME" >&2
  exit 1
fi

cd "$DISPATCHER_HOME"

# Outermost guard: no tick may hang. The in-process budget
# (DISPATCHER_TICK_BUDGET_S, default 1500s) is the primary bound, but a
# subprocess wedged in the OS (a hung git/child handle, a blocked C-level
# read) can outlive a Python-level timeout. This `timeout` is the last-resort
# hard wall strictly below the cron scheduler's own 3600s script timeout, so
# the scheduler kills the tick itself instead of the run timing out.
TICK_HARD_CEILING="${DISPATCHER_TICK_CEILING_S:-1740}"

PY="${DISPATCHER_PYTHON:-python}"
if command -v timeout >/dev/null 2>&1; then
  timeout -s KILL "$TICK_HARD_CEILING" "$PY" -m dispatcher.cli tick
else
  "$PY" -m dispatcher.cli tick
fi
status=$?

if [ "$status" -eq 137 ] || [ "$status" -eq 124 ]; then
  echo "dispatcher tick FAILED: killed by hard ceiling ${TICK_HARD_CEILING}s" >&2
  exit 124
fi

if [ "$status" -ne 0 ]; then
  echo "dispatcher tick failed with status $status" >&2
fi
exit "$status"
