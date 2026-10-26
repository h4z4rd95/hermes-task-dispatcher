# Dispatcher Python Alignment Evidence (T-2026-10-26-001)

**Status:** PASS
**Date:** 2026-10-26T21:08Z (UTC)
**Host:** gglvp-migration-01 (65.109.210.40)
**Repo:** h4z4rd95/hermes-task-dispatcher

## Outcome
Resolved the Python interpreter mismatch in `dispatcher_tick.sh`. The script is NOT a hard-pin — `DISPATCHER_PYTHON` is overridable and the script uses a `if -x` heuristic. The legacy 0.19.0 venv (`/usr/local/lib/hermes-agent/venv`) is a pre-migration artifact whose site-packages do not include `pytest 9.1.1` as expected by the dispatcher gate (per Issue #40). The canonical Hermes interpreter is Python 3.14.7 (via `hermes` entrypoint v0.22.1+1.g5575a08) at `/root/.hermes/tools/python-3.14.7+202****0901-linux-x64/bin/python3`.

## Before
- Deployed: `PY=/usr/local/lib/hermes-agent/venv/bin/python` → `Python 3.11.17`, `hermes-agent 0.19.0`
- Canonical: `hermes` entrypoint → `Python 3.14.7`, `hermes-agent 0.22.1+1.g5575a08`

## After (minimal change to `dispatcher_tick.sh`)
Reordered the resolution preference:
1. Prefer canonical `/root/.hermes/tools/python-3.14.7+202****0901-linux-x64/bin/python3` if executable
2. Fall back to legacy `/usr/local/lib/hermes-agent/venv/bin/python`
3. Fall back to PATH `python`
`DISPATCHER_PYTHON` env var still overrides in every branch (unchanged behaviour).

## Backup
- File: `/root/hermes-tools/dispatcher-backup-20261026T210150Z/dispatcher_tick.sh`
- SHA-256 of pre-change copy: `70cfbfaa078cb8574dbd9fac22a42e8bba9a406950fa37c6d5c16ec6da6d0581`
- Restore: `cp /root/hermes-tools/dispatcher-backup-20261026T210150Z/dispatcher_tick.sh /root/.hermes/scripts/dispatcher_tick.sh`

## Validation
- `bash -n dispatcher_tick.sh` → SYNTAX_OK
- Dispatcher stdlib imports on py3.14: `json,subprocess,urllib.request,sqlite3,hashlib,pathlib,re,shlex,os,sys` → OK
- Full test suite on py3.14 (pytest installed locally): `303 passed in 22.59s`
- Live tick via `hermes cron run ecc392e9cef6` → `Ran now: succeeded.`

## Safety / scheduler unchanged
- READY tasks before tick: 0
- RUNNING tasks before tick: 0 (no claim possible)
- After tick: same counts — `('DONE','DONE') 20, ('DONE','READY') 6, ('PENDING','READY') 1, ('COMPLETED_PARTIAL','BLOCKED') 1, ('FAILED','FAILED') 16`
- Scheduler unchanged: `*/5 * * * *`, next run 21:05, last run `2026-10-26T21:03:59+00:00 ok`, dispatch `on time`
- 16 legacy FAILED rows preserved (not rewritten)
- No gateway restart, no cron timing change, no secrets committed

## Commit / fetch-back verified
- Commit SHA (origin/main): `a1b3be7739a76982266896b833bca054932119c2`
- File changed: `scripts/dispatcher_tick.sh` (+7/-1)
- Independent fetch-back confirms `git rev-parse origin/main` == committed SHA
```