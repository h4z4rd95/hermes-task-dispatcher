# Automated Verification Gate

Date: 2026-10-15
Status: **ACTIVE** — wired into the task dispatcher runtime
Implementation: `dispatcher/verification_gate.py` + `dispatcher/dispatcher.py`
Tests: `tests/test_verification_gate.py` (24 tests, all passing)
Prompt: `prompts/IMPLEMENT_AUTOMATED_VERIFICATION_GATE_2026-10-15.md` (governance repo)

## What it is

A small, dependency-free (stdlib only) verification gate that sits inside the
Hermes task dispatcher and decides whether a task may become `DONE`.

Before: `status = "DONE" if run.exit_code == 0 else "FAILED"`. A zero exit
code from the agent process was the only proof of completion. Any later
reviewer had to re-inspect or re-test the work to be sure.

After: the dispatcher runs a `VerificationGate` pass over the task workspace.
`DONE` is granted only when the gate verdict is `PASS`. The verdict is derived
from recorded evidence, never from the agent's exit code alone.

## How it is invoked (real integration, not guidance)

The gate is invoked by `Dispatcher._run_task` in `dispatcher/dispatcher.py`,
in the same process, directly after the routed Hermes session run finishes:

```python
gate = VerificationGate(
    task_id=task.id,
    repo_path=checkout_root,
    risk_tier=gate_tier,
    owner="dispatcher",
    prompt_text=prompt,
    criteria=list(getattr(task, "acceptance_criteria", []) or []),
    cache_path=self.cfg.state_dir / "verification_cache.json",
    branch=task.branch,
    require_repo=task.needs_checkout,
)
gate_bundle = gate.run(
    probe=None,
    test_runner=None,
    precondition_plan=str(getattr(task, "preconditions", "") or ""),
    rollback_plan=str(getattr(task, "rollback_plan", "") or ""),
)
```

This is **automatic**: it fires on every dispatcher task completion through
the cron-driven tick (`hermes-task-dispatcher` cron job, every 5 minutes).
No manual step, no second agent, no extra command to remember.

Status derivation after the gate runs:

| agent exit code | gate verdict | task status |
|---|---|---|
| 0 | `PASS` | `DONE` |
| 0 | `FAIL` / `PARTIAL` | `FAILED` |
| 0 | `BLOCKED` | `BLOCKED` (named reason recorded) |
| non-zero | any | `FAILED` |

The gate itself is fail-closed: if the gate code raises, the task becomes
`FAILED`, never `DONE`. There is no retry loop — a failed gate does not cause
the task to be re-executed.

## Risk tiers (governance section C)

| Tier | Applies to | Required checks |
|---|---|---|
| `T0` | documentation / metadata | `files_exist_remote` |
| `T1` | isolated code change | `diff_inspect`, `targeted_tests` |
| `T2` | integration / service / data pipeline | `diff_inspect`, `targeted_tests`, `runtime_probe` |
| `T3` | security, production migration, financial, destructive | `diff_inspect`, `targeted_tests`, `runtime_probe`, `preconditions`, `rollback_plan` |

A task may be **raised** to a higher tier when uncertainty requires it. It may
never be lowered to save time. `T0` never runs a test suite — that is the
point of the tier.

## Checks

Each check records: check id, status, command, exit code, bounded output,
evidence reference, start timestamp, and whether the result was reused.

- **`diff_inspect`** — `git status --porcelain` + diff against the baseline
  SHA. Fail-closed when the workspace is not a repository.
- **`files_exist_remote`** — every changed path exists in the working tree.
  T0's only check; deliberately cheap.
- **`targeted_tests`** — selects only the test files that map to the changed
  paths (by name convention: `tests/test_<module>.py` etc.). If no test files
  map to the change, the check is `PARTIAL` with a recorded reason, which is
  fatal for `T1+` — the change is not proven.
- **`runtime_probe`** — an injected real API/log/DB/runtime probe for `T2+`.
  When no probe is defined for a task the check is `PARTIAL` (recorded), so
  integration tasks fail closed until an owner defines one.
- **`preconditions` / `rollback_plan`** — documented preconditions and
  rollback plan, mandatory for `T3` only.

## Evidence reuse — the duplicate-work kill-switch (section F)

Evidence is keyed by `(task_id, check_id, commit_sha, env_digest,
task_digest)`:

- `task_id` — the stable task identifier from the inbox.
- `check_id` — which check produced this evidence.
- `commit_sha` — the final full SHA of the task workspace.
- `env_digest` — a coarse digest of the verification environment (OS, arch,
  Python minor version, git version). Changes only when the runtime changes.
- `task_digest` — SHA-256 over the task prompt text and acceptance criteria.
  Changes when the task contract changes.

The cache lives at `<DISPATCHER_HOME>/state/verification_cache.json`.

**Reuse is decided before the check runs.** For each required check the gate
computes the identity key and looks it up in the cache. On a hit with status
`PASS` and all identity components still current, the cached result is
returned and the command is **never executed**. This is the mechanism that
makes a later reviewer's "please re-test this" cheap: if the identities have
not changed, the answer is already recorded and the bundle is returned with
`evidence_reused: true`.

A rerun happens only when evidence is stale, missing, contradicted, or the
task contract changed. Any rerun must have one of these concrete triggers.

Cache entries are written through `redact()` — secrets cannot leak into the
cache file (tested).

## Evidence bundle (governance section D)

One JSON artifact per task, written to `<DISPATCHER_HOME>/state/
verification-<task_id>.json`:

```
task_id, status (gate verdict), risk_tier, owner
repo_path, branch, baseline_sha, final_sha, changed_files
acceptance_criteria, criterion_results
checks[]            — per-check: id, status, command, exit_code, output,
                     evidence_ref, started_at, duration_s, reused, skip_reason
blockers[], skipped[]
rollback_ref
validator           — "verification-gate"
evidence_reused     — whether any check was served from cache
fetch_back          — {ref, local_sha, ok, remote_contains / error}
started_at, finished_at
env_digest, task_digest
```

The agent's own evidence block is discovered at `$VG_EVIDENCE_PATH` or
`.verification-gate.json` in the task workspace; its `acceptance_criteria`
and `criterion_results` are folded into the bundle.

Secrets never enter the bundle: values longer than 200 chars are truncated,
token-shaped values are hashed, and `token=...`-style assignments are
replaced with `***REDACTED***` (tested by `test_secrets_absent_from_written_bundle`).

## Fetch-back verification (governance section 9)

`_Git.fetch_verify` runs `git fetch --dry-run origin` and then
`git branch -r --contains <sha>` (falling back to `ls-remote`) and records
the boolean in `bundle.fetch_back`. This records that the commit landed
remotely. A commit proves a **source** change, not runtime correctness —
runtime correctness is recorded separately by the checks.

Note: `fetch_back.ok` is a presence check against the local clone's view of
the remote. On the dispatcher host the workspace clones are fresh, so this
reflects the push the dispatcher itself performed.

## Automation boundary (what is and is not automatic)

**Automatic:**
- Gate execution on every dispatcher task completion (cron-driven, no manual
  step).
- DONE derivation, fail-closed, with no retry loop.
- Evidence recording, cache write, and evidence reuse on repeat verification.
- Secret redaction in both bundle and cache.

**Not automatic (deliberate):**
- Risk-tier assignment per task: currently defaults to `T1`, read from the
  task's `risk_tier` field if the inbox schema is extended. Raising a tier is
  a caller decision.
- `runtime_probe` content for `T2+` tasks: must be injected per task. Without
  it the check is PARTIAL and the task fails closed.
- Graphify and RAG are **not** part of this gate. Per governance section E
  they are optional aids (structural impact lookup; document retrieval), not
  correctness oracles. Nothing here calls them.

## Rollback / backup

- Full backup of the pre-change dispatcher: `/tmp/vg-backup-live/`
  (BASE_HEAD recorded inside), plus the earlier `/tmp/vg-backup-3834023/`.
- To revert: `git checkout HEAD -- dispatcher/dispatcher.py && rm
  dispatcher/verification_gate.py tests/test_verification_gate.py` and
  restart the cron job.
- No production services, DNS, firewall, trading execution, or data volumes
  were touched. No Hiddify, no TGSG scheduler, no TraderBot backtest.

## Tests

`tests/test_verification_gate.py` — 24 tests covering the governance
prompt's required behaviours:

1. ✅ T0 file edit does not trigger broad test suites
2. ✅ T1 code edit selects targeted tests (only the mapped test file runs)
3. ✅ existing valid evidence is reused without rerunning an expensive check
4. ✅ changed commit / changed task contract invalidates the relevant evidence
5. ✅ a failed required check cannot produce PASS/DONE
6. ✅ missing access becomes BLOCKED without unsafe escalation
7. ✅ final fetch-back/commit identity is represented in the evidence bundle
8. ✅ secrets are redacted from output, bundle, and cache files

Full suite: `213 passed, 2 failed`. The 2 failures
(`test_run_uses_verified_routing_primitive`, `test_tick_sync_uses_correct_timeout_exception`)
are **pre-existing** — they fail identically on the untouched backup at
`/tmp/vg-backup-live/` and are unrelated to the gate.

## Out of scope (per governance prompt)

No old TraderBot backtests or research experiments were run. The unexecuted
Tabdeal evaluator was not touched. No TGSG catch-up crawl or scheduler change
was triggered by this work. No broad RAG rebuilds, no model changes, no new
paid subscriptions, no new dashboard. No unrelated Hermes or project code was
refactored. The live TGSG crawl and TraderBot backtest were not used as the
gate's test fixture — all tests use temporary scratch git repositories.
