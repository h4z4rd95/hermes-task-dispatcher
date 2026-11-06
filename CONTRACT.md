# Hermes Task Dispatcher — Component Contract

Single source of truth for cross-file interfaces. Every lane implements
**only** the files listed under its ownership below. No file is owned by two
lanes. The lead (integration lane) owns everything else and all merges.

## Runtime model

- The durable scheduler is the **installed Hermes cron** (`hermes cron`), not a
  new daemon. A single `--no-agent` cron job runs `scripts/dispatcher_tick.sh`
  every 5 minutes. That script calls `python -m dispatcher.cli tick` and exits.
- The tick is the **only writer** to `state/dispatcher.sqlite`. It:
  1. syncs the control repo, parses the inbox,
  2. recovers stale leases,
  3. claims eligible tasks (deps met, approvals satisfied, under concurrency
     limits) by writing a lease,
  4. runs the first claim inline and spawns the rest as detached worker
     processes (`python -m dispatcher.cli worker <task_id>`),
  5. ingests outcome files the workers spool, applies them, releases leases,
  6. writes outcomes back to the control repo and pushes.
- Workers never touch the SQLite store. They write
  `state/outcomes/<task_id>.json` atomically (tmp file + `os.replace`) and exit.
- Lease liveness = `(pid, process_start_time)` fingerprint. A dead fingerprint
  with no spooled outcome = stale lease → recover (retry or FAILED).
- All secrets come from `$HERMES_HOME/.env` (`GITHUB_TOKEN`) via environment,
  never read into a value that is logged or written to git.

## Paths (resolved by `dispatcher/config.py`)

```
DISPATCHER_HOME          = repo root of hermes-task-dispatcher (this repo)
DISPATCHER_CONTROL_REPO  = env or default <..>/plan-project-situation
DISPATCHER_WORKSPACE_POOL= env or default <DISPATCHER_HOME>/../dispatcher-workspaces
state/                   = DISPATCHER_HOME/state   (gitignored)
state/dispatcher.sqlite  = durable task state + leases
state/outcomes/<id>.json = worker → tick outcome spool
state/PAUSE              = kill-switch file (presence pauses all dispatch)
```

## Task inbox schema (`hermes/tasks_inbox.toml` in the control repo)

```toml
[inbox]
version = 1

[[task]]
id               = "T-2026-10-05-001"   # stable, unique, required
title            = "..."                 # required
status           = "READY"               # authoring state; dispatcher owns transitions
priority         = 100                   # higher first
workstream       = "smoke"               # isolation key (never mix workstreams)
repo             = "h4z4rd95/name"       # optional; omit = no checkout
branch           = "main"
prompt_path      = "tasks/x.md"          # inside control repo, OR
prompt_inline    = "..."                 # mutually exclusive with prompt_path
session          = "name"                # session affinity target
session_affinity = "dedicated"           # dedicated | existing | none
parallel_safe    = true
workspace_budget_mb = 500
keep_workspace   = false
requires_approval = false
approval_note    = ""
dependencies     = ["T-..."]
max_retries      = 2
timeout_seconds  = 1500
model            = ""                    # optional model override
```

Task states: `READY`, `RUNNING`, `BLOCKED`, `DONE`, `FAILED`, `CANCELLED`.
Terminal: `DONE`, `FAILED`, `CANCELLED`.

### Owner status promotion synchronization

The control-repo inbox is the owner authorization source. On each tick, an
explicit inbox transition from `PENDING` or `BLOCKED` to `READY` is mirrored
into the SQLite task state so the task can be claimed. This synchronization
clears stale lease/skip metadata. It never re-arms terminal SQLite states
(`DONE`, `FAILED`, `CANCELLED`); terminal re-arm requires the documented
owner-controlled recovery path.

## Lane ownership

| Lane | Owns (only these files) |
|---|---|
| A — core (lead) | `dispatcher/config.py`, `dispatcher/registry.py`, `dispatcher/state.py`, `dispatcher/dispatcher.py`, `dispatcher/cli.py`, `scripts/dispatcher_tick.sh`, `tests/test_registry.py`, `tests/test_state.py`, `tests/test_dispatcher.py` |
| B — routing + GitHub | `dispatcher/router.py`, `dispatcher/github.py`, `tests/test_router.py`, `tests/test_github.py` |
| C — workspace | `dispatcher/workspace.py`, `tests/test_workspace.py` |

## dispatcher/workspace.py — Lane C

```python
class WorkspaceError(Exception): ...
class WorkspaceQuotaError(WorkspaceError): ...
class WorkspaceSpaceError(WorkspaceError): ...

@dataclass
class Checkout:
    path: Path
    created: bool        # False when a valid checkout was reused
    size_bytes: int

class WorkspaceManager:
    def __init__(self, pool_root: Path, *, global_quota_mb: int,
                 github_token: str | None = None, logger=None) -> None
    # filesystem reality
    def pool_free_bytes(self) -> int          # free space on the pool's device
    def pool_used_bytes(self) -> int          # sum of managed metadata sizes
    # repo size via GitHub API (<repo> = "owner/name"); None when unknown
    def repo_size_bytes(self, repo: str) -> int | None
    # clone-or-reuse; raises WorkspaceQuotaError / WorkspaceSpaceError;
    # shallow when safe; records metadata; idempotent per (task_id, repo, branch)
    def checkout(self, *, task_id: str, repo: str, branch: str = "main",
                 purpose: str = "", keep: bool = False,
                 task_budget_mb: int = 0, shallow: bool = True,
                 depth: int = 1) -> Checkout
    def bind_session(self, path: Path, session_id: str) -> None
    def touch(self, path: Path) -> None
    def note_size(self, path: Path, size_bytes: int) -> None
    def release(self, path: Path) -> None      # mark policy RECLAIM
    def keep(self, path: Path) -> None         # mark policy KEEP
    # dry_run never deletes; deletes ONLY allowlisted released metadata dirs
    def reclaim(self, *, dry_run: bool = True,
                task_ids: list[str] | None = None) -> dict
    def report(self) -> list[dict]             # metadata rows
    def unmanaged_dirs(self) -> list[Path]     # pool dirs with no metadata; NEVER deleted
```

Metadata store: `<pool_root>/.dispatcher/workspaces.json` (atomic writes).
Record per checkout: `path, task_id, repo, branch, purpose, size_bytes,
last_use, created_at, policy` (`KEEP` | `RECLAIM`), `session_id`.

Safety invariants Lane C must honor:
- Never delete a path absent from metadata; `unmanaged_dirs()` reports them.
- `reclaim(dry_run=True)` is the default and returns what it *would* delete.
- Quota check = `pool_free_bytes()` and repo size vs `global_quota_mb` and
  `task_budget_mb` BEFORE cloning. Fail closed (raise) rather than clone.
- Reuse: an existing recorded checkout for the same (task_id, repo, branch) with
  a valid git dir is reused (`created=False`) after `git fetch --quiet origin &&
  git checkout <branch> && git reset --hard origin/<branch>` — never a fresh
  clone when a valid one exists.
- Windows-safe paths (this host is Windows; the pool root is configurable
  precisely because `/mnt/HC_Volume_107017670` lives on a different host).

## dispatcher/router.py — Lane B

```python
@dataclass
class RunResult:
    session_id: str
    exit_code: int
    stdout: str
    duration_s: float
    resumed: bool        # True = routed into an existing session

class SessionRouter:
    def __init__(self, *, workdir: Path | None = None, logger=None,
                 extra_env: dict | None = None) -> None
    def session_name_exists(self, session_name: str) -> bool
    def run(self, *, session_name: str, prompt: str,
            model: str | None = None, max_turns: int | None = None,
            run_budget_s: int | None = None) -> RunResult
```

Implementation notes (verified against the installed Hermes v0.21.5+6718):
- The `hermes` entrypoint is on PATH at
  `C:\Users\h4z4rd\AppData\Local\hermes\hermes-agent\venv\Scripts\hermes`.
- Routing primitive:
  `hermes chat --continue "<name>" --create-if-missing -q "<prompt>" -Q --format text`
  — resumes when a session named `<name>` exists, otherwise creates it. Both
  paths are verified working on this host.
  Output ends with a line `session_id: <id>`. `Resumed session` in the preamble
  ⇒ `resumed=True`; `Starting fresh` ⇒ `resumed=False`.
- `session_name_exists` must be cheap and side-effect free. Discover the right
  `hermes sessions` invocation (there is a `sessions list` verb; check for a
  JSON/title filter). If none is reliable, fall back to probing
  `hermes chat --continue "<name>" -q ""` behavior — but prefer a read-only
  query of the session store. Document what you chose and why, with evidence.
- Pass `--workdir`/`--in` when a working directory is given.
- The child must inherit `GITHUB_TOKEN` (it comes from the live gateway env).
  Never pass a token on the command line.
- Capture stdout/stderr; never echo secrets. The router logs command
  invocation **without** prompt bodies if they may contain secrets? No — prompts
  are task text, not secrets; logging them at debug level is fine. Tokens are
  never on the command line, so they cannot leak this way.

## dispatcher/github.py — Lane B

```python
class ControlRepo:
    def __init__(self, repo_dir: Path, *, logger=None) -> None
    def remote_url(self) -> str
    def sync(self) -> str                 # git fetch + ff-only pull; returns HEAD sha
    def head_sha(self) -> str
    def inbox_path(self) -> Path
    def read_inbox(self) -> str
    # record a finished task: update tasks_inbox.toml status + append one JSON
    # object to hermes/dispatch_outcomes.jsonl; commit; return commit sha
    def write_outcome(self, *, task_id: str, status: str, summary: str,
                      evidence: list[dict], commit_sha: str | None = None,
                      blocker: str | None = None,
                      owner_action: str | None = None) -> str | None
    def commit_all(self, message: str) -> str | None
    def push(self) -> None
```

Notes:
- `repo_dir` is a local clone of `h4z4rd95/Plan-Project-Situation` whose remote
  is HTTPS; git credential resolution already works there (token via env).
- Updating `tasks_inbox.toml` in place must preserve comments and formatting —
  parse minimally (regex on the `status = "..."` line of the matching
  `[[task]]` block keyed by `id`) or rewrite the whole file from the parsed
  AST. Pick the approach that keeps the file diffable and say which you used.
- `write_outcome` must be idempotent per (task_id, terminal status): appending
  the same outcome twice must not duplicate the JSONL line (keyed index check).
- Commit identity is already configured globally (`h4z4rd95`).

## What every lane must deliver

1. The module(s) listed, importable as `from dispatcher.X import Y`.
2. `tests/test_<module>.py` — pytest, runnable with
   `python -m pytest tests -q` from the repo root, no network required (mock
   subprocess/GitHub API), fast (<30s). Lane B/C tests must not actually spawn
   real agent sessions or clone real repos.
3. Zero writes outside the files you own, plus `state/` and the pytest tmp dir.

## Git discipline

- `.gitignore` must exclude `state/`, `__pycache__/`, `*.pyc`, `.pytest_cache/`.
- Implementation commits go to `hermes-task-dispatcher` only. Governance doc
  updates to `Plan-Project-Situation` are the lead's job, done separately.

## Progress-aware Hermes execution (2026-11-06)

A task is NOT killed merely because five minutes elapsed. The SessionRouter
reads the active Hermes session row from the same profile's state.db and
monitors sessions.message_count, last_activity_at, and ended_at:

- Default idle threshold: 300 seconds without a new message/activity.
- Default polling interval: 5 seconds.
- Default maximum: 2 continuation nudges per invocation.
- Default hard task budget: 1500 seconds (the existing tick ceiling); tasks
  with active message progress are not cut off at the five-minute idle mark.
- At the idle threshold, only when the session is still open and the message
  counter/activity has not advanced, the router interrupts the stalled CLI
  turn and resumes the SAME named session and workspace with a continuation
  instruction to inspect current changes, continue the original acceptance
  criteria, and commit/push/fetch-back if the work is complete.
- A terminal session (ended_at set) is allowed to flush its result; it is not
  interrupted. If the session store cannot be read or no matching session row
  exists, the watchdog fails closed and does not infer idleness.
- The task's configured timeout_seconds and the tick's tick_budget_s remain
  hard safety ceilings. They are not the five-minute idle threshold.

Environment overrides:
DISPATCHER_IDLE_TIMEOUT_SECONDS (300),
DISPATCHER_MONITOR_INTERVAL_SECONDS (5),
DISPATCHER_MAX_IDLE_RECOVERIES (2).

The outcome evidence records continuation count and the last observed
message_count, so a later reviewer can distinguish active progress,
watchdog continuation, terminal completion and hard-ceiling timeout.

### Manifest producer and fail-closed admission

The Dispatcher consumes resource manifests; it does not pretend to infer source
dependencies from task prose. Before a repo task is promoted to READY, the
Supervisor/intake path must inspect the task prompt and target repo at the
recorded baseline and populate:
- write_paths: expected files/modules/config/schema/API surfaces the task may
  modify;
- read_paths: files/modules/contracts whose current behavior the task depends
  on;
- shared_resources: mutable cross-repository resources (DB tables, runtime,
  deployment target, shared service/API contract);
- dependencies: producer/consumer task IDs where one task requires another's
  output.

The manifest must include evidence paths/SHAs and confidence. If the Supervisor
cannot establish a bounded write/read set, it must leave the task unscoped;
the Dispatcher will not claim that two same-repo unscoped tasks are independent.
A missing or ambiguous manifest is serialized/blocked, never guessed PARALLEL.

The topological admission pass blocks every member of a producer/consumer
cycle (including cycles longer than two tasks). If a durable resource lock
races after admission, partial locks are rolled back and the task returns to
READY without invoking its worker. If the decision ledger cannot be written,
the decision fails closed as BLOCKED. Remote branch HEAD must be freshly
verified; a failed fetch cannot reuse a stale remote-tracking ref.
