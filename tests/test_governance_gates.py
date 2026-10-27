"""Governance-gate regression tests (Issue #30 continuation, 2026-10-27).

Covers the execution-boundary contracts the dispatcher must ENFORCE, not just
document:

* dependency readiness (and fail-closed unknown dependencies);
* approval gates for production/destructive scope (requires_approval);
* no auto-promotion (PENDING is never claimable);
* stable task ids + no duplicate claim/execution under concurrent ticks;
* bounded concurrency;
* safe recovery of stale RUNNING leases without duplicate execution;
* truthful terminal mapping (DONE only on gate PASS; FAILED/BLOCKED distinct);
* project/repo/branch workspace isolation (no cross-project reuse);
* terminal outcomes written exactly once across success/failure/retry/replay
  and an interrupted write, with attempt/reason fields and secret scrubbing.

No network, no real agent sessions, no real git remote — the router,
workspace, control repo and (for ledger tests) the git runner are faked;
ledger and inbox files are real files on a real tmp filesystem.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from dispatcher.config import Config
from dispatcher.dispatcher import Dispatcher
from dispatcher.github import ControlRepo, scrub_secrets
from dispatcher.registry import Inbox, InboxError, Task, inbox_to_toml, parse_inbox
from dispatcher.state import StateStore, TaskState


# --- test doubles (same shape as tests/test_dispatcher.py) -------------------


@dataclass
class FakeRun:
    session_id: str = "20261027_000000_governance"
    exit_code: int = 0
    stdout: str = "task output"
    duration_s: float = 0.1
    resumed: bool = False


class FakeRouter:
    def __init__(self, *, runs: list[FakeRun] | None = None):
        self.calls: list[dict[str, Any]] = []
        self.runs = runs or []
        self.extra_env: dict[str, str] = {}
        self._n = 0

    def run(self, *, session_name: str, prompt: str, model: str | None = None,
            max_turns: int | None = None, run_budget_s: int | None = None) -> FakeRun:
        self.calls.append({"session_name": session_name, "prompt": prompt})
        if self.runs:
            run = self.runs[min(self._n, len(self.runs) - 1)]
            self._n += 1
            return run
        self._n += 1
        return FakeRun(resumed=self._n > 1)


class FakeCheckout:
    def __init__(self, path: Path):
        self.path = path


class FakeWorkspace:
    def __init__(self):
        self.checkouts: list[dict[str, Any]] = []
        self.released: list[Path] = []

    def checkout(self, *, task_id, repo, branch="main", purpose="", keep=False,
                 task_budget_mb=0, shallow=True, depth=1) -> FakeCheckout:
        path = Path(f"/tmp/gov-gates-fake/{task_id}")
        self.checkouts.append({"task_id": task_id, "repo": repo, "branch": branch})
        return FakeCheckout(path)

    def bind_session(self, path: Path, session_id: str) -> None:
        pass

    def release(self, path: Path) -> None:
        self.released.append(path)


class FakeControl:
    def __init__(self):
        self.written: list[dict[str, Any]] = []
        self.pushed = 0

    def sync(self) -> str:
        return "3e0652c4937d626f32241d0eebddcf64f26c759d"

    def push(self) -> None:
        self.pushed += 1

    def write_outcome(
        self, *, task_id: str, status: str, summary: str, evidence,
        commit_sha: str | None = None, run_id: str = "", source: str = "",
        attempt: int = 0, reason: str = "",
    ) -> str | None:
        self.written.append({
            "task_id": task_id, "status": status, "summary": summary,
            "evidence": list(evidence) if evidence else [],
            "commit_sha": commit_sha, "run_id": run_id, "source": source,
            "attempt": attempt, "reason": reason,
        })
        return "outcome-commit-sha"

    def commit_all(self, message: str) -> str:
        return "commit-sha"


def _make_config(tmp_path: Path, *, max_concurrent: int = 2) -> Config:
    control = tmp_path / "control"
    (control / "hermes").mkdir(parents=True, exist_ok=True)
    return Config(
        home=tmp_path / "dispatcher",
        control_repo=control,
        workspace_pool=tmp_path / "pool",
        inbox_rel=Path("hermes") / "tasks_inbox.toml",
        outcomes_rel=Path("hermes") / "dispatch_outcomes.jsonl",
        poll_interval="5m",
        max_concurrent=max_concurrent,
        global_workspace_quota_mb=20480,
        stale_lease_seconds=900,
        worker_spawn=False,
        state_dir=tmp_path / "dispatcher" / "state",
        state_db=tmp_path / "dispatcher" / "state" / "dispatcher.sqlite",
        outcomes_dir=tmp_path / "dispatcher" / "state" / "outcomes",
        pause_file=tmp_path / "dispatcher" / "state" / "PAUSE",
        cron_job_name="gov-gates-test",
        tick_budget_s=1500,
    )


@pytest.fixture
def env(tmp_path):
    cfg = _make_config(tmp_path)
    store = StateStore(cfg.state_db)
    router = FakeRouter()
    workspace = FakeWorkspace()
    control = FakeControl()
    d = Dispatcher(cfg, store=store, router=router, workspace=workspace, control=control)
    return d, cfg, store, router, workspace, control


def _write_inbox(control_repo: Path, tasks: list[Task]) -> None:
    text = inbox_to_toml(Inbox(tasks=tasks))
    (control_repo / "hermes" / "tasks_inbox.toml").write_text(text, encoding="utf-8")


# --- dependency readiness ----------------------------------------------------


def test_unknown_dependency_is_fail_closed_at_parse():
    """A dependency on a non-existent task id must be rejected, not waited on."""
    text = (
        "[inbox]\nversion = 1\n\n"
        "[[task]]\n"
        'id = "T-DEP-CHILD"\n'
        'title = "child"\n'
        'status = "READY"\n'
        'prompt_inline = "p"\n'
        'dependencies = ["T-DOES-NOT-EXIST"]\n'
    )
    with pytest.raises(InboxError) as exc:
        parse_inbox(text, source="tasks_inbox.toml")
    assert "unknown task" in str(exc.value)


def test_dependency_not_done_blocks_claim_until_done(env):
    d, cfg, store, router, workspace, control = env
    dep = Task(id="T-A", title="dep", prompt_inline="p")
    child = Task(id="T-B", title="child", prompt_inline="p",
                 dependencies=["T-A"])
    _write_inbox(cfg.control_repo, [dep, child])

    r1 = d.tick()
    # A runs first; B must wait (its dependency was READY, not DONE, at
    # eligibility time).
    assert r1.claimed == ["T-A"]
    assert store.get("T-A").status == "DONE"
    assert store.get("T-B").status == "READY"
    assert len(router.calls) == 1

    r2 = d.tick()
    # Now the dependency is DONE: B claims exactly once.
    assert r2.claimed == ["T-B"]
    assert store.get("T-B").status == "DONE"
    assert len(router.calls) == 2


# --- approval gate ------------------------------------------------------------


def test_requires_approval_is_never_claimed(env):
    d, cfg, store, router, workspace, control = env
    gated = Task(id="T-GATED", title="production deploy",
                 prompt_inline="p", status="READY",
                 requires_approval=True,
                 approval_note="production deploy gate")
    _write_inbox(cfg.control_repo, [gated])
    report = d.tick()
    assert report.claimed == []
    assert router.calls == []
    # seeded at its inbox status, never promoted
    assert store.get("T-GATED").status == "READY"
    assert "approval" in (store.get("T-GATED").last_skip_reason or "")


def test_pending_is_never_promoted(env):
    d, cfg, store, router, workspace, control = env
    parked = Task(id="T-PARKED", title="parked", prompt_inline="p",
                  status="PENDING")
    _write_inbox(cfg.control_repo, [parked])
    d.tick()
    assert store.get("T-PARKED").status == "PENDING"
    assert router.calls == []


# --- stable ids / no duplicate claim ------------------------------------------


def test_concurrent_ticks_cannot_double_claim(env, tmp_path):
    d, cfg, store, router, workspace, control = env
    _write_inbox(cfg.control_repo, [Task(id="T-RACE", title="racy",
                                         prompt_inline="p")])
    # Seed the row READY (as a prior tick's sync would have), then two
    # "ticks" race the same claim.
    store.upsert(TaskState(task_id="T-RACE", status="READY"))
    first = store.claim("T-RACE", lease_seconds=300)
    # a second tick (fresh store instance, same db) must lose the race
    other_tick_store = StateStore(cfg.state_db)
    second = other_tick_store.claim("T-RACE", lease_seconds=300)
    assert first is not None
    assert second is None
    assert other_tick_store.get("T-RACE").status == "RUNNING"
    assert other_tick_store.get("T-RACE").attempts == 1  # counted once


# --- bounded concurrency -------------------------------------------------------


def test_max_concurrent_caps_claims_per_tick(tmp_path):
    cfg = _make_config(tmp_path / "c1", max_concurrent=1)
    store = StateStore(cfg.state_db)
    router = FakeRouter()
    control = FakeControl()
    d = Dispatcher(cfg, store=store, router=router, workspace=FakeWorkspace(),
                   control=control)
    _write_inbox(cfg.control_repo, [
        Task(id="T-CAP-A", title="a", prompt_inline="p", parallel_safe=True),
        Task(id="T-CAP-B", title="b", prompt_inline="p", parallel_safe=True),
    ])
    report = d.tick()
    assert report.claimed == ["T-CAP-A"]
    assert store.get("T-CAP-B").status == "READY"
    assert len(router.calls) == 1


# --- stale RUNNING lease recovery ----------------------------------------------


def test_stale_lease_recovered_and_runs_exactly_once(env):
    d, cfg, store, router, workspace, control = env
    _write_inbox(cfg.control_repo, [
        Task(id="T-STALE", title="stale", prompt_inline="p", max_retries=1)])
    import time as _time

    now = _time.time()
    # A worker claimed this row and died: dead pid, expired lease.
    store.upsert(TaskState(
        task_id="T-STALE", status="RUNNING", attempts=0,
        lease_pid=999999, lease_start=now - 3600, lease_expiry=now - 3500,
    ))
    report = d.tick()
    # Recovered (reset to READY, attempts remaining) and executed exactly once.
    assert ("T-STALE", "reset_to_ready (lease expired)") in report.recovered
    assert report.claimed == ["T-STALE"]
    assert store.get("T-STALE").status == "DONE"
    assert len(router.calls) == 1
    # terminal outcome published once, with the recovery reason recorded
    assert [w["task_id"] for w in control.written] == ["T-STALE"]
    assert control.written[0]["status"] == "DONE"


def test_stale_lease_without_retries_fails_not_loops(env):
    d, cfg, store, router, workspace, control = env
    _write_inbox(cfg.control_repo, [
        Task(id="T-STALE2", title="stale", prompt_inline="p", max_retries=0)])
    import time as _time

    now = _time.time()
    store.upsert(TaskState(
        task_id="T-STALE2", status="RUNNING", attempts=1,
        lease_pid=999999, lease_start=now - 3600, lease_expiry=now - 3500,
    ))
    report = d.tick()
    # No retries left: terminal FAILED, no execution.
    assert ("T-STALE2", "failed (lease expired)") in report.recovered
    assert report.claimed == []
    assert store.get("T-STALE2").status == "FAILED"
    assert router.calls == []


# --- truthful terminal mapping --------------------------------------------------


def test_agent_failure_is_failed_even_with_passing_gate(env):
    d, cfg, store, router, workspace, control = env
    router.runs = [FakeRun(exit_code=1, stdout="boom")]
    _write_inbox(cfg.control_repo, [Task(id="T-FAIL", title="fail",
                                         prompt_inline="p")])
    d.tick()
    assert store.get("T-FAIL").status == "FAILED"
    assert control.written[0]["status"] == "FAILED"
    assert control.written[0]["reason"] == "agent exited 1"


def test_gate_crash_fails_closed(monkeypatch, env):
    d, cfg, store, router, workspace, control = env

    class BoomGate:
        def __init__(self, **kw):
            pass

        def run(self, **kw):
            raise RuntimeError("gate exploded")

    monkeypatch.setattr("dispatcher.dispatcher.VerificationGate", BoomGate)
    _write_inbox(cfg.control_repo, [Task(id="T-GATECRASH", title="gate",
                                         prompt_inline="p")])
    d.tick()
    # A gate that errors cannot produce DONE.
    assert store.get("T-GATECRASH").status == "FAILED"
    assert control.written[0]["status"] == "FAILED"


def test_done_only_with_gate_pass(env):
    d, cfg, store, router, workspace, control = env
    _write_inbox(cfg.control_repo, [Task(id="T-OK", title="ok", prompt_inline="p")])
    d.tick()
    assert store.get("T-OK").status == "DONE"
    assert control.written[0]["status"] == "DONE"
    assert control.written[0]["reason"] == ""


# --- workspace / project isolation ----------------------------------------------


def test_workspace_isolation_no_cross_project_reuse(env):
    d, cfg, store, router, workspace, control = env
    (cfg.control_repo / "prompts").mkdir(parents=True, exist_ok=True)
    (cfg.control_repo / "prompts" / "a.md").write_text("prompt a\n")
    (cfg.control_repo / "prompts" / "b.md").write_text("prompt b\n")
    t1 = Task(id="T-W1", title="alpha work", repo="owner/alpha",
              branch="main", prompt_path="prompts/a.md")
    t2 = Task(id="T-W2", title="beta work", repo="owner/beta",
              branch="release/1", prompt_path="prompts/b.md")
    _write_inbox(cfg.control_repo, [t1, t2])
    d.tick()
    # Each task checks out ITS OWN repo/branch into ITS OWN workspace —
    # never a sibling task's project.
    by_task = {c["task_id"]: c for c in workspace.checkouts}
    assert by_task["T-W1"]["repo"] == "owner/alpha"
    assert by_task["T-W1"]["branch"] == "main"
    assert by_task["T-W2"]["repo"] == "owner/beta"
    assert by_task["T-W2"]["branch"] == "release/1"
    assert store.get("T-W1").workspace_path != store.get("T-W2").workspace_path


# --- ledger: fields, scrubbing, exactly-once, interrupted write ------------------


class FakeRunner:
    def __init__(self, script: dict | None = None):
        self.script = dict(script or {})
        self.calls: list[list[str]] = []

    def __call__(self, cmd, *, cwd, env, capture_output, text, check):
        self.calls.append(list(cmd))
        key = "|".join(cmd[1:])
        rc, out, err = self.script.get(key, (0, "", ""))
        if rc != 0 and check:
            raise subprocess.CalledProcessError(rc, cmd, output=out, stderr=err)
        return subprocess.CompletedProcess(args=list(cmd), returncode=rc,
                                           stdout=out, stderr=err)


HEAD = "a" * 40


def _ledger_repo(tmp_path: Path) -> tuple[ControlRepo, FakeRunner]:
    d = tmp_path / "control"
    (d / "hermes").mkdir(parents=True)
    script = {
        "add|-A": (0, "", ""),
        "diff|--cached|--quiet": (1, "", ""),  # staged changes present
        "rev-parse|HEAD": (0, HEAD + "\n", ""),
    }
    runner = FakeRunner(script)
    cr = ControlRepo(d)
    cr._set_runner(runner)
    return cr, runner


def _rows(repo: Path) -> list[dict]:
    p = repo / "hermes" / "dispatch_outcomes.jsonl"
    if not p.is_file():
        return []
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def test_ledger_line_carries_attempt_and_reason(tmp_path):
    cr, _ = _ledger_repo(tmp_path)
    cr.write_outcome(task_id="T-43", status="FAILED", summary="run died",
                     evidence=[], run_id="run-1", attempt=2,
                     reason="agent exited 124")
    rows = _rows(tmp_path / "control")
    assert len(rows) == 1
    assert rows[0]["attempt"] == 2
    assert rows[0]["reason"] == "agent exited 124"
    assert rows[0]["run_id"] == "run-1"


def test_ledger_omits_empty_new_fields(tmp_path):
    cr, _ = _ledger_repo(tmp_path)
    cr.write_outcome(task_id="T-LEGACY", status="DONE", summary="ok", evidence=[])
    rows = _rows(tmp_path / "control")
    assert "attempt" not in rows[0]
    assert "reason" not in rows[0]
    assert "recovered" not in rows[0]


def test_scrub_secrets_redacts_token_shapes():
    pat = "ghp_" + "A1b2" * 12
    out = scrub_secrets(f"deploy failed; token={pat}; retrying")
    assert pat not in out
    assert "token=***REDACTED***" in out
    long_hex = "0" * 40
    out2 = scrub_secrets(f"sha {long_hex} leaked")
    assert long_hex not in out2
    assert scrub_secrets("") == ""


def test_ledger_and_commit_message_are_scrubbed(tmp_path):
    cr, runner = _ledger_repo(tmp_path)
    pat = "ghp_" + "Zz9y" * 12
    cr.write_outcome(task_id="T-LEAK", status="FAILED",
                     summary=f"agent echoed token={pat} at the end",
                     evidence=[], run_id="run-leak", attempt=1,
                     reason=f"retry with token={pat} failed")
    rows = _rows(tmp_path / "control")
    assert pat not in rows[0]["summary"]
    assert pat not in rows[0]["reason"]
    commit_calls = [c for c in runner.calls
                    if c[1:3] == ["commit", "--quiet"]]
    assert commit_calls, "expected a git commit for the outcome"
    assert pat not in " ".join(commit_calls[0])


def test_replayed_run_writes_exactly_one_line(tmp_path):
    cr, _ = _ledger_repo(tmp_path)
    kwargs = dict(task_id="T-REPLAY", status="FAILED", summary="s",
                  evidence=[], run_id="run-same", attempt=1,
                  reason="agent exited 1")
    first = cr.write_outcome(**kwargs)
    assert first is not None
    # same run re-reported (tick replay) -> deduplicated
    second = cr.write_outcome(**kwargs)
    assert second is None
    # a NEW run of the same task (resume/retry) -> its own line
    third = cr.write_outcome(task_id="T-REPLAY", status="FAILED", summary="s2",
                             evidence=[], run_id="run-new", attempt=2,
                             reason="agent exited 124")
    assert third is not None
    rows = _rows(tmp_path / "control")
    assert [r["run_id"] for r in rows] == ["run-same", "run-new"]
    assert [r["attempt"] for r in rows] == [1, 2]


def test_interrupted_write_leaves_ledger_intact(tmp_path, monkeypatch):
    import dispatcher.github as gh

    cr, _ = _ledger_repo(tmp_path)
    # a pre-existing legacy line
    (tmp_path / "control" / "hermes" / "dispatch_outcomes.jsonl").write_text(
        json.dumps({"task_id": "T-OLD", "status": "DONE", "summary": "old",
                    "evidence": [], "ts": "2026-10-01T00:00:00Z"}) + "\n",
        encoding="utf-8",
    )
    before = (tmp_path / "control" / "hermes" / "dispatch_outcomes.jsonl").read_bytes()

    def boom(*a, **kw):
        raise RuntimeError("process killed mid-append")

    monkeypatch.setattr(gh.os, "replace", boom)
    with pytest.raises(RuntimeError):
        cr.write_outcome(task_id="T-INT", status="FAILED", summary="crash",
                         evidence=[], run_id="run-int", attempt=1,
                         reason="agent exited 137")
    # the ledger is byte-identical: no torn line, no partial append
    after = (tmp_path / "control" / "hermes" / "dispatch_outcomes.jsonl").read_bytes()
    assert after == before

    # the next tick re-attempts the same run -> exactly one line lands
    monkeypatch.undo()
    cr.write_outcome(task_id="T-INT", status="FAILED", summary="crash",
                     evidence=[], run_id="run-int", attempt=1,
                     reason="agent exited 137")
    rows = _rows(tmp_path / "control")
    assert [r["task_id"] for r in rows] == ["T-OLD", "T-INT"]
    assert not list((tmp_path / "control" / "hermes").glob("*.tmp")), \
        "no temp file residue after a successful atomic publish"


def test_recovered_row_round_trips_through_reader(tmp_path):
    """Recovered rows (repair tooling) must parse back with their markers."""
    cr, _ = _ledger_repo(tmp_path)
    line = {
        "task_id": "T-RECOVERED", "status": "FAILED", "summary": "resume run",
        "evidence": [], "ts": "2026-10-22T08:20:26Z", "attempt": 2,
        "reason": "agent exited 124", "recovered": True,
        "recovered_from": "state DB row + outcome commit 0d27bb5c",
    }
    (tmp_path / "control" / "hermes" / "dispatch_outcomes.jsonl").write_text(
        json.dumps(line) + "\n", encoding="utf-8")
    recs = cr._read_ledger()
    assert len(recs) == 1
    assert recs[0].recovered is True
    assert recs[0].attempt == 2
    assert recs[0].recovered_from.startswith("state DB row")
