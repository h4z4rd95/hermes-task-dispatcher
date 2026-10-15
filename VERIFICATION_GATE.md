# Automated Verification Gate

Date: 2026-10-15
Status: **ACTIVE** — wired into the task dispatcher runtime
Implementation: `dispatcher/verification_gate.py`, `dispatcher/probes.py`, `dispatcher/dispatcher.py`
Tests: `tests/test_verification_gate.py` (24 passing) + `tests/test_verification_gate_followup.py` (33 passing)
Prompts: `IMPLEMENT_AUTOMATED_VERIFICATION_GATE_2026-10-15.md`, `VERIFICATION_GATE_FOLLOWUP_RISK_PROBES_REMOTE_VERIFY_2026-10-15.md`, `VERIFICATION_GATE_CLOSEOUT_FIXES_AND_SHIP_2026-10-15.md` (governance repo)

## What it is

A small, dependency-free (stdlib only) verification gate that sits inside the
Hermes task dispatcher and decides whether a task may become `DONE`.

Before: `status = "DONE" if run.exit_code == 0 else "FAILED"`. A zero exit
code from the agent process was the only proof of completion.

After: the dispatcher runs a `VerificationGate` pass over the task workspace.
`DONE` is granted only when the gate verdict is `PASS`, derived from recorded
evidence rather than the agent's exit code.

## How it is invoked (real integration, automatic)

`Dispatcher._run_task` in `dispatcher/dispatcher.py` builds the gate in the
same process, directly after the routed Hermes session run finishes:

```python
gate_tier = task.effective_risk_tier          # registry contract
probe = build_probe(spec=getattr(task, "runtime_probe", None),
                    workspace=checkout_root, logger=self.log)
gate = VerificationGate(
    task_id=task.id,
    repo_path=checkout_root,
    risk_tier=gate_tier,
    prompt_text=prompt,
    cache_path=self.cfg.state_dir / "verification_cache.json",
    branch=task.branch,
    require_repo=task.needs_checkout,
)
gate_bundle = gate.run(...)
```

This fires on every dispatcher task completion through the cron-driven tick
(`hermes-task-dispatcher`, every 5 minutes). No manual step.

Status derivation:

| agent exit | gate verdict | task status |
|---|---|---|
| 0 | `PASS` | `DONE` |
| 0 | `FAIL` / `PARTIAL` | `FAILED` |
| 0 | `BLOCKED` | `BLOCKED` (named reason) |
| non-zero | any | `FAILED` |

The gate is fail-closed: if the gate code raises, the task becomes `FAILED`,
never `DONE`. A failed gate does not trigger a retry loop.

## Risk tier contract (registry)

Declared on the task in the inbox TOML:

```toml
[[task]]
id = "T-..."
prompt_inline = "..."
risk_tier = "T2"
runtime_probe = { handler = "http_get", url = "...", target = "svc:health" }
```

- Allowed values: `T0`, `T1`, `T2`, `T3` (case-insensitive; normalized to
  upper case at parse time). Anything else is an `InboxError` at inbox load,
  so a bad tier never silently degrades.
- Policy floor is `T1`. `effective_risk_tier` preserves a declared higher
  tier and clamps a declared `T0` up to the floor: a caller may **raise**
  risk, never lower it below policy.
- Omitted/empty tier → policy floor `T1` (backward compatible: existing
  inbox tasks keep working unchanged).
- The tier drives the **required checks**, not just metadata.

| Tier | Required checks |
|---|---|
| `T0` | `files_exist_remote` |
| `T1` | `diff_inspect`, `targeted_tests` |
| `T2` | `diff_inspect`, `targeted_tests`, `runtime_probe` |
| `T3` | `diff_inspect`, `targeted_tests`, `runtime_probe`, `preconditions`, `rollback_plan` |

## Runtime probe contract (T2+)

A runtime probe is the runtime evidence that an integration task actually
works — not merely that its tests passed. Defined in
`dispatcher/probes.py`.

**Allowlist, no shell.** A task payload may only select a *named* handler.
There is no `shell`/`exec`/`command` handler in the contract, so no
arbitrary command can be executed from a task payload. Handlers:

| handler | what it proves |
|---|---|
| `http_get` | GET a URL; check `expect_status` and/or `expect_contains` (redirects not followed, 4 KiB body cap) |
| `file_exists` | a path **inside the task workspace** exists |
| `tcp_connect` | `host:port` accepts a TCP connection (pure reachability) |
| `pytest_module` | one named pytest module inside the workspace passes |

Spec fields: `handler` (required, allowlisted), `target` (required — evidence
identity), `timeout` (1..120 s, clamped; default 10), plus handler-specific
fields (`url`, `expect_status`, `expect_contains`, `path`, `host`, `port`).
Unknown keys are rejected.

**Path containment is enforced twice.** `validate_probe_spec` rejects any
`path` that is absolute, contains `..`, or escapes the workspace — *before*
any handler runs. Handlers keep their own `resolve()`-based containment
checks as defense in depth. Tested by `test_probe_path_traversal_blocked`.

**Fail-closed rules (tested):**

- Missing probe on a T2+ task → check is `PARTIAL` → verdict is not `PASS`.
- Malformed spec (unknown handler, missing target, unknown key, bad timeout)
  → the gate records `BLOCKED` with the reason; never `DONE`.
- Probe that raises, times out, or fails its acceptance condition → `FAIL`.
- T0/T1 tasks **never run a runtime probe**, even when a probe spec is
  declared, unless their tier requires it (`test_t0_t1_never_runs_runtime_probe`).

**Evidence identity.** Probe evidence carries `probe_id`, `handler`,
`target`, `workspace`, `ok`, `started_at`, `duration_s`. The cache identity
key includes `probe=<target>`, so probe evidence is **never reused across a
different target** (`test_probe_evidence_not_reused_across_targets`); it is
reused unchanged only when task/commit/env/task-digest/target all match
(`test_probe_evidence_reused_same_target`).

A deterministic local fixture exercises the full path: a loopback HTTP server
on an ephemeral port (`test_http_get_probe_with_local_server_fixture`), plus
`file_exists` and closed-port `tcp_connect` fixtures. No production service
is touched by any probe test.

## Evidence reuse — the duplicate-work kill-switch

Evidence is keyed by `(task_id, check_id, commit_sha, env_digest,
task_digest)` and, for `runtime_probe`, `probe_target`. Reuse is decided
**before** the check runs: on a cache hit with status `PASS` and all identity
components current, the cached result is returned and the command is never
re-executed. A rerun requires a concrete trigger: new diff, changed
environment/data, stale or contradicted evidence, missing acceptance
criterion, or raised risk tier.

Cache: `<DISPATCHER_HOME>/state/verification_cache.json`, written through
`redact()` so secrets cannot leak into the cache file (tested).

## Evidence bundle

One JSON artifact per task at `<DISPATCHER_HOME>/state/verification-<task_id>.json`:
task identity, risk tier, baseline/final SHA, changed files, acceptance
criteria, per-check records (status, command, exit code, bounded output,
evidence reference, reused flag, skip reason), blockers, skipped, rollback
reference, `runtime_probe` identity, `fetch_back`, env/task digests, and
start/finish timestamps.

Secrets never enter the bundle: values over 200 chars are truncated,
token-shaped strings are hashed, and `token=...` assignments become
`***REDACTED***` (tested in both bundle and cache).

## Independent GitHub fetch-back

`_Git.fetch_verify` performs the governance fetch-back. It is explicit about
its method, so evidence can never imply an independent GitHub read that did
not happen:

- `method: "github_api"` — an independent, read-only GitHub API call
  (`GET /repos/{owner}/{repo}/branches/{branch}`) using `GITHUB_TOKEN` /
  `GH_TOKEN` from the environment (the cron tick already loads
  `~/.hermes/.env`). One bounded retry covers pure propagation delay.
  `independent_remote_read: true`; `ok` means the remote branch HEAD SHA
  equals the local commit SHA.
- `method: "local_git"` — no token / non-GitHub remote. Backed by the local
  clone's remote-tracking state only, with `independent_remote_read: false`
  and a recorded reason. This is the honest fallback; it is never labelled as
  an independent remote read.
- `method: "error"` — the local commit itself could not be resolved.

Recorded fields: `ref`, `local_sha`, `remote_repo`, `branch`, `remote_sha`,
`verified_at`, `method`, `independent_remote_read`, `ok`, `error`. The
remote URL is stripped of any embedded user-info before parsing, and no
token is ever placed in a logged or persisted field. A commit proves a
*source* change; runtime correctness still comes from the checks.

Observed live against the shipped commit: `method=github_api`,
`independent_remote_read=true`, `ok=true`, local and remote SHA equal.

## Automation boundary

**Automatic:** gate execution on every dispatcher task completion
(cron-driven); DONE derivation (fail-closed, no retry loop); evidence
recording, cache write, and reuse; secret redaction in bundle and cache.

**Not automatic (deliberate):** risk-tier assignment per task is owner-authored
in the inbox (default `T1`); the content of an `http_get`/`tcp_connect`/
`pytest_module` probe is task-authored within the allowlist. Graphify and RAG
are not part of this gate — per governance they are optional aids, not
correctness oracles; nothing here calls them.

## Rollback / backup

- Pre-change backups: `/tmp/vg-backup-live/`, `/tmp/vg-backup-3834023/`
  (BASE_HEAD `321128937dcfd01d4d3dd81311309fbd04d2522a`) and
  `/tmp/vg-fu-backup/` (BASE `e45c5b6ca92efa8453e985ed3a74e2e7a0e8b9b9`).
- Revert: `git checkout <base> -- dispatcher/ && rm -f
  dispatcher/probes.py tests/test_verification_gate_followup.py
  VERIFICATION_GATE.md` and restart the cron job.
- No production services, DNS, firewall, trading execution, or data volumes
  were touched. No Hiddify, no TGSG crawl, no TraderBot backtest.

## Tests

`tests/test_verification_gate.py` — 24 tests (predecessor behaviours).
`tests/test_verification_gate_followup.py` — 33 tests (risk-tier contract,
probe contract and safety, evidence identity/reuse, fetch-back methods and
honesty flags, backward compatibility).

Focused regression commands and results for the close-out change:

```
python3 -m pytest tests/test_verification_gate_followup.py -q   # 33 passed
python3 -m pytest tests/test_verification_gate.py -q            # 24 passed
python3 -m pytest tests/test_registry.py -q                     # 18 passed
```

Pre-existing, unrelated failures (verified identical on the untouched
backup; not touched by this change per the prompt's debt policy):

- `test_run_uses_verified_routing_primitive`
- `test_tick_sync_uses_correct_timeout_exception`

## Out of scope (per governance prompts)

No old TraderBot backtests or research experiments were run. The unexecuted
Tabdeal evaluator was not touched. No TGSG catch-up crawl or scheduler change
was triggered. No broad RAG rebuilds, model changes, paid subscriptions, or
new dashboards. No unrelated Hermes or project code was refactored. All gate
tests use temporary scratch git repositories and loopback fixtures.
