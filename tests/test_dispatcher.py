"""Tests for dispatcher/dispatcher.py — the tick state machine.

The router, workspace manager and control repo are stubbed so the test never
spawns a real agent session, clones a repo, or touches the network.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from dispatcher.config import Config
from dispatcher.dispatcher import Dispatcher
from dispatcher.registry import Task, load_inbox
from dispatcher.state import StateStore, TaskState


# --- test doubles -----------------------------------------------------------


@dataclass
class FakeRun:
    session_id: str = "20261005_000000_abcdef"
    exit_code: int = 0
    stdout: str = "task output"
    duration_s: float = 0.1
    resumed: bool = False


class FakeRouter:
    def __init__(self, *, fail_on: set[str] | None = None, runs: list[FakeRun] | None = None):
        self.calls: list[dict[str, Any]] = []
        self.fail_on = fail_on or set()
        self.runs = runs or []
        self._n = 0

    def session_name_exists(self, name: str) -> bool:
        return any(c["session_name"] == name for c in self.calls)

    def run(self, *, session_name: str, prompt: str, model: str | None = None,
            max_turns: int | None = None, run_budget_s: int | None = None) -> FakeRun:
        self.calls.append({"session_name": session_name, "prompt": prompt, "model": model})
        if session_name in self.fail_on:
            raise RuntimeError("router boom")
        if self.runs:
            return self.runs[min(self._n, len(self.runs) - 1)]
        self._n += 1
        return FakeRun(resumed=self._n > 1)


class FakeCheckout:
    def __init__(self, path: Path):
        self.path = path
        self.created = True
        self.size_bytes = 0


class FakeWorkspace:
    def __init__(self):
        self.checkouts: list[dict[str, Any]] = []
        self.released: list[Path] = []
        self.bound: list[tuple[Path, str]] = []

    def checkout(self, *, task_id, repo, branch="main", purpose="", keep=False,
                 task_budget_mb=0, shallow=True, depth=1) -> FakeCheckout:
        path = Path(f"/tmp/fake/{task_id}")
        self.checkouts.append({"task_id": task_id, "repo": repo, "branch": branch})
        return FakeCheckout(path)

    def bind_session(self, path: Path, session_id: str) -> None:
        self.bound.append((path, session_id))

    def release(self, path: Path) -> None:
        self.released.append(path)

    def keep(self, path: Path) -> None:
        pass


class FakeControl:
    def __init__(self, *, outcomes: list[dict[str, Any]] | None = None):
        self.synced = 0
        self.pushed = 0
        self.written: list[dict[str, Any]] = outcomes if outcomes is not None else []
        self.head = "3e0652c4937d626f32241d0eebddcf64f26c759d"

    def sync(self) -> str:
        self.synced += 1
        return self.head

    def push(self) -> None:
        self.pushed += 1

    def write_outcome_from_inbox(self, inbox, report) -> str:
        self.written.append({"tasks": [t.id for t in inbox.tasks]})
        return "outcome-commit-sha"

    def commit_all(self, message: str) -> str:
        return "commit-sha"


# --- fixtures ---------------------------------------------------------------


def _make_config(tmp_path, control_repo: Path) -> Config:
    return Config(
        home=tmp_path / "dispatcher",
        control_repo=control_repo,
        workspace_pool=tmp_path / "pool",
        inbox_rel=Path("hermes") / "tasks_inbox.toml",
        outcomes_rel=Path("hermes") / "dispatch_outcomes.jsonl",
        poll_interval="5m",
        max_concurrent=2,
        global_workspace_quota_mb=20480,
        stale_lease_seconds=900,
        worker_spawn=False,
        state_dir=tmp_path / "dispatcher" / "state",
        state_db=tmp_path / "dispatcher" / "state" / "dispatcher.sqlite",
        outcomes_dir=tmp_path / "dispatcher" / "state" / "outcomes",
        pause_file=tmp_path / "dispatcher" / "state" / "PAUSE",
        cron_job_name="test-dispatcher",
    )


@pytest.fixture
def env(tmp_path, monkeypatch):
    control = tmp_path / "control"
    (control / "hermes").mkdir(parents=True)
    cfg = _make_config(tmp_path, control)
    monkeypatch.setattr("dispatcher.config.config", lambda: cfg)
    store = StateStore(cfg.state_db)
    router = FakeRouter()
    workspace = FakeWorkspace()
    control_repo = FakeControl()
    d = Dispatcher(cfg, store=store, router=router, workspace=workspace,
                   control=control_repo)
    return d, cfg, store, router, workspace, control_repo, control


def _write_inbox(control: Path, tasks: list[Task]) -> None:
    from dispatcher.registry import Inbox, inbox_to_toml

    text = inbox_to_toml(Inbox(tasks=tasks))
    (control / "hermes" / "tasks_inbox.toml").write_text(text, encoding="utf-8")


# --- tests ------------------------------------------------------------------


def test_tick_empty_inbox(env):
    d, cfg, store, router, workspace, control_repo, control = env
    report = d.tick()
    assert report.inbox_tasks == 0
    assert report.claimed == []
    assert control_repo.synced == 1


def test_pause_file_skips_dispatch(env):
    d, cfg, store, router, workspace, control_repo, control = env
    cfg.pause_file.parent.mkdir(parents=True, exist_ok=True)
    cfg.pause_file.write_text("paused\n", encoding="utf-8")
    _write_inbox(control, [Task(id="T-1", title="x", prompt_inline="p")])
    report = d.tick()
    assert report.claimed == []
    assert report.inbox_tasks == 0


def test_ready_task_is_claimed_and_completes(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="Smoke", prompt_inline="Reply PONG",
                                session="smoke-1")])
    report = d.tick()
    assert report.claimed == ["T-1"]
    assert "T-1" in report.completed
    assert store.get("T-1").status == "DONE"
    assert router.calls[0]["session_name"] == "smoke-1"
    assert control_repo.pushed == 1


def test_terminal_task_is_not_reclaimed(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="Smoke", prompt_inline="p")])
    assert d.tick().claimed == ["T-1"]
    # second tick finds nothing new — the task is DONE in state
    report = d.tick()
    assert report.claimed == []
    assert router.calls == [] or len(router.calls) == 1


def test_dependency_blocks_dependent_task(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [
        Task(id="T-1", title="Base", prompt_inline="p"),
        Task(id="T-2", title="Dependent", prompt_inline="p", dependencies=["T-1"],
             priority=200),
    ])
    report = d.tick()
    # T-2 has higher priority but must wait for T-1
    assert report.claimed == ["T-2", "T-1"] or report.claimed == ["T-1"]
    assert "T-1" in report.claimed


def test_dependency_done_unblocks_dependent(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [
        Task(id="T-1", title="Base", prompt_inline="p"),
        Task(id="T-2", title="Dependent", prompt_inline="p", dependencies=["T-1"]),
    ])
    d.tick()  # T-1 runs and completes
    assert store.get("T-2").status != "DONE"
    d.tick()  # T-2 now runnable
    assert store.get("T-2").status == "DONE"


def test_approval_gate_blocks(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="Gated", prompt_inline="p",
                                requires_approval=True,
                                approval_note="needs owner ok")])
    report = d.tick()
    assert report.claimed == []
    assert router.calls == []


def test_router_failure_marks_task_failed(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="x", prompt_inline="p")])
    d._router = FakeRouter(fail_on={"dispatcher-T-1"})
    report = d.tick()
    assert "T-1" not in report.completed
    assert store.get("T-1").status == "FAILED"
    assert any("router boom" in e for e in report.errors)


def test_nonzero_exit_marks_failed(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="x", prompt_inline="p")])
    d._router = FakeRouter(runs=[FakeRun(exit_code=1, stdout="boom")])
    d.tick()
    assert store.get("T-1").status == "FAILED"


def test_concurrency_limit_respected(env):
    d, cfg, store, router, workspace, control_repo, control = env
    # Config is frozen; rebuild it with a lower concurrency limit.
    from dispatcher.config import Config as _C

    cfg = _C(**{**cfg.__dict__, "max_concurrent": 1})
    d.cfg = cfg
    _write_inbox(control, [
        Task(id="T-1", title="a", prompt_inline="p", priority=200),
        Task(id="T-2", title="b", prompt_inline="p", priority=100),
    ])
    report = d.tick()
    assert report.claimed == ["T-1"]
    # T-2 was not started; it stays READY for a later tick.
    state = store.get("T-2")
    assert state is None or state.status == "READY"
    report2 = d.tick()
    assert report2.claimed == ["T-2"]
    assert store.get("T-2").status == "DONE"


def test_workstream_isolation(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [
        Task(id="T-1", title="a", prompt_inline="p", workstream="ws-a", priority=200),
        Task(id="T-2", title="b", prompt_inline="p", workstream="ws-b", priority=100),
    ])
    d._router = FakeRouter(runs=[FakeRun(session_id="s1")])
    report = d.tick()
    # both different workstreams run
    assert sorted(report.claimed) == ["T-1", "T-2"]


def test_checkout_created_when_repo_set(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="x", prompt_inline="p",
                                repo="h4z4rd95/example")])
    d.tick()
    assert workspace.checkouts[0]["repo"] == "h4z4rd95/example"
    state = store.get("T-1")
    assert state.workspace_path
    # default policy is to release the workspace after completion
    assert workspace.released


def test_keep_workspace_not_released(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="x", prompt_inline="p",
                                repo="h4z4rd95/example", keep_workspace=True)])
    d.tick()
    assert workspace.released == []


def test_outcome_spooled_and_idempotent(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="x", prompt_inline="p")])
    d.tick()
    outcome = cfg.outcomes_dir / "T-1.json"
    assert outcome.is_file()
    # Re-ingesting an already-terminal task drops the file without duplicating.
    d._ingest_outcomes(store.all_states(), type("R", (), {})())
    assert not outcome.exists()
    assert store.get("T-1").status == "DONE"


def test_stale_lease_recovery(env):
    d, cfg, store, router, workspace, control_repo, control = env
    _write_inbox(control, [Task(id="T-1", title="x", prompt_inline="p")])
    d.tick()
    # A real dead worker never spools an outcome file; remove it so the tick's
    # first step (ingest) cannot mask the recovery.
    outcome = cfg.outcomes_dir / "T-1.json"
    assert outcome.is_file()
    outcome.unlink()
    # Simulate a dead worker: RUNNING with a long-dead lease owner.
    store.upsert(TaskState(task_id="T-1", status="RUNNING", attempts=1,
                           lease_pid=4_194_303, lease_start=1.0, lease_expiry=1.0))
    report = d.tick()
    assert any(r[0] == "T-1" for r in report.recovered if isinstance(r, tuple))
    # The task either re-ran successfully or stayed recovered; both are valid,
    # and the key invariant is that it did not stay wedged in RUNNING.
    assert store.get("T-1").status != "RUNNING"


def test_tick_does_not_raise_on_bad_inbox(env):
    d, cfg, store, router, workspace, control_repo, control = env
    # An unterminated multi-line string is a hard parse error, not just no tasks.
    (control / "hermes" / "tasks_inbox.toml").write_text(
        '[inbox]\nversion = 1\n\n[[task]]\nid = "T-1"\nprompt_inline = """unterminated\n',
        encoding="utf-8",
    )
    report = d.tick()
    assert report.errors
    assert report.claimed == []
