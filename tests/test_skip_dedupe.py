"""Focused test for the Issue #23 fix: a repeatedly non-claimable task is
logged once per reason change, not once per tick."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dispatcher.state import TaskState
from dispatcher.dispatcher import Dispatcher
from dispatcher.registry import Task
from dispatcher.config import Config


class FakeStore:
    def __init__(self):
        self.rows = {}

    def set_skip_reason(self, task_id, reason):
        if task_id in self.rows:
            self.rows[task_id] = TaskState(
                task_id=task_id, status=self.rows[task_id].status,
                last_skip_reason=reason)

    def upsert(self, state):
        self.rows[state.task_id] = state

    def all_states(self):
        return dict(self.rows)


def make_dispatcher(store):
    cfg = Config(
        home=Path("/tmp"), control_repo=Path("/tmp"), workspace_pool=Path("/tmp"),
        inbox_rel=Path("x"), outcomes_rel=Path("y"), poll_interval="5m",
        max_concurrent=1, global_workspace_quota_mb=1, stale_lease_seconds=900,
        worker_spawn=False, tick_budget_s=100, state_dir=Path("/tmp"),
        state_db=Path("/tmp/x.db"), outcomes_dir=Path("/tmp"),
        pause_file=Path("/tmp/PAUSE-nonexistent"), cron_job_name="t",
    )
    logs = []
    d = Dispatcher(cfg, store=store, logger=lambda lvl, msg: logs.append((lvl, msg)))
    return d, logs


def pending_task():
    return Task(id="T-PEND", title="t", status="PENDING", priority=1,
                workstream="w", prompt_inline="x", session="s")


def approval_task():
    return Task(id="T-PEND", title="t", status="READY", priority=1,
                workstream="w", prompt_inline="x", session="s",
                requires_approval=True, approval_note="owner gate")


def test_repeated_pending_logged_once():
    store = FakeStore()
    d, logs = make_dispatcher(store)
    for _ in range(3):
        d._eligible(pending_task(), None, store.all_states())
    assert len([m for _, m in logs if "PENDING" in m]) == 1
    assert store.rows["T-PEND"].last_skip_reason == "status PENDING — not claimable yet"


def test_reason_change_logged_once_then_quiet():
    store = FakeStore()
    d, logs = make_dispatcher(store)
    d._eligible(pending_task(), None, store.all_states())  # seeds the row
    logs.clear()
    store.rows["T-PEND"] = TaskState(
        task_id="T-PEND", status="READY",
        last_skip_reason="status PENDING — not claimable yet")
    d._eligible(approval_task(), None, store.all_states())
    assert len([m for _, m in logs if "approval" in m]) == 1
    logs.clear()
    d._eligible(approval_task(), None, store.all_states())
    assert len([m for _, m in logs if "approval" in m]) == 0


def test_skip_persists_changed_reason():
    store = FakeStore()
    d, _ = make_dispatcher(store)
    d._eligible(pending_task(), None, store.all_states())
    d._skip(pending_task(), store.rows["T-PEND"], "a different reason")
    assert store.rows["T-PEND"].last_skip_reason == "a different reason"


if __name__ == "__main__":
    test_repeated_pending_logged_once()
    print("PASS: repeated PENDING logged exactly once across 3 ticks")
    test_reason_change_logged_once_then_quiet()
    print("PASS: reason change logged once, then quiet")
    test_skip_persists_changed_reason()
    print("PASS: _skip persists changed reason")
    print("all focused checks passed")
