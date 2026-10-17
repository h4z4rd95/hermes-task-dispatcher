#!/usr/bin/env bash
# Dispatcher tick — registered as a `hermes cron --no-agent --script` job.
# Runs one pass of the dispatcher and exits.
#
# Byte-equivalent contract on every host: the file at
# $HERMES_HOME/scripts/dispatcher_tick.sh is the deployment target; the repo
# copy (h4z4rd95/hermes-task-dispatcher) is the source of truth. `hermes cron
# --script` only resolves scripts inside $HERMES_HOME/scripts/, so the repo
# copy is copied verbatim onto each host. All host differences are env vars.
set -uo pipefail

# FIX 2026-10-12 (T-2026-10-12-HERMES-AUTOMATION-READINESS-001):
#   The gateway systemd unit ships no EnvironmentFile, so cron children never
#   inherit GITHUB_TOKEN and the control-repo fetch fails authentication. Load
#   the canonical Hermes env here — the only place the credential helper looks.
#   No-op on hosts without the file; the .env may have CRLF endings, so strip
#   CRs in a temp copy instead of sourcing it directly.
if [ -f "$HOME/.hermes/.env" ]; then
  # FIX 2026-10-17 (cron unbound variable): The .env contains values with literal
  # $ characters (e.g. scrypt$16384$8$1$... password hashes) that bash would expand
  # during sourcing. Under `set -u` (line 10), $1/$8 in those values triggers
  # "unbound variable" and kills every tick. Extract only the keys the dispatcher
  # needs, using parameter expansion (which does NOT re-expand $ in values) instead
  # of sourcing the whole file.
  set -a
  _env_clean="$(mktemp)"
  tr -d '\r' < "$HOME/.hermes/.env" > "$_env_clean"
  while IFS= read -r _env_line || [ -n "$_env_line" ]; do
    case "$_env_line" in
      GITHUB_TOKEN=*) export "GITHUB_TOKEN=${_env_line#GITHUB_TOKEN=}";;
      GH_TOKEN=*)     export "GH_TOKEN=${_env_line#GH_TOKEN=}";;
    esac
  done < "$_env_clean"
  rm -f "$_env_clean"
  set +a
fi

# Canonical deployment of the dispatcher (single source of truth). The Hetzner
# migration host owns the real runtime; the Windows control host keeps its own
# checkout. Overridable via env on either.
DISPATCHER_HOME="${DISPATCHER_HOME:-}"
if [ -z "$DISPATCHER_HOME" ]; then
  if [ -d "/root/hermes-tools/hermes-task-dispatcher" ]; then
    DISPATCHER_HOME="/root/hermes-tools/hermes-task-dispatcher"
  else
    DISPATCHER_HOME="C:/Users/h4z4rd/.hermes-tools/hermes-task-dispatcher"
  fi
fi

if [ ! -f "$DISPATCHER_HOME/dispatcher/config.py" ]; then
  echo "dispatcher tick FAILED: no dispatcher package under DISPATCHER_HOME=$DISPATCHER_HOME" >&2
  exit 1
fi

cd "$DISPATCHER_HOME"

# The server runtime ships its own venv; other hosts use PATH.
if [ -x "/usr/local/lib/hermes-agent/venv/bin/python" ]; then
  PY="${DISPATCHER_PYTHON:-/usr/local/lib/hermes-agent/venv/bin/python}"
else
  PY="${DISPATCHER_PYTHON:-python}"
fi

# Outermost guard: no tick may hang. The in-process budget
# (DISPATCHER_TICK_BUDGET_S, default 1500s) is the primary bound, but a
# subprocess wedged in the OS (a hung git/child handle, a blocked C-level
# read) can outlive a Python-level timeout. This `timeout` is the last-resort
# hard wall strictly below the cron scheduler's own 3600s script timeout, so
# the scheduler kills the tick itself instead of the run timing out.
TICK_HARD_CEILING="${DISPATCHER_TICK_CEILING_S:-1740}"

if command -v timeout >/dev/null 2>&1; then
  timeout -s KILL "$TICK_HARD_CEILING" "$PY" -m dispatcher.cli tick
else
  "$PY" -m dispatcher.cli tick
fi
status=$?

if [ "$status" -ne 0 ]; then
  echo "dispatcher tick failed with status $status" >&2
fi
if [ "$status" -eq 137 ] || [ "$status" -eq 124 ]; then
  echo "dispatcher tick FAILED: killed by hard ceiling ${TICK_HARD_CEILING}s" >&2
  exit 124
fi
exit "$status"
