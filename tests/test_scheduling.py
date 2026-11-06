"""Focused scenario tests for conflict-aware scheduling (v4, 2026-11-06).

Maps the owner's Phase-3 scenario list (Issue #80 continuation spec) to the
cheapest possible tests:

  1  independent disjoint tasks run concurrently            -> test_scenario_01
  2  same-file write/write conflict serializes              -> test_scenario_02
  3  producer/consumer serialize even with different paths  -> test_scenario_03
  4  same lockfile/schema/migration serializes              -> test_scenario_04
  5  cross-repo shared runtime resource serializes          -> test_scenario_05
  6  parallel_safe=true cannot bypass a conflict            -> test_scenario_06
  7  explicit dependency waits until DONE                   -> test_scenario_07
  8  predecessor completion triggers baseline refresh +
     re-evaluation before claim                             -> test_scenario_08
  9  changed mid-run write set blocks new conflicting
     claims without killing the running task                -> test_scenario_09
  10 duplicate tick/restart does not duplicate a handoff or
     leak a lock                                            -> test_scenario_10
  11 ambiguous dependency graph becomes BLOCKED             -> test_scenario_11

Scenario 12 (false-DONE -> bounded correction -> independent PASS) is the
supervisor-intake pipeline, exercised by the existing E2E fixture in the
control repo (supervisor/repair/e2e/T-2026-11-06-V4-E2E-FIXTURE-001.md), not
by the dispatcher suite.

All tests run against temp SQLite DBs and stubbed routers; no network, no
production state. The remote-HEAD probe is scripted per test so the suite is
deterministic and offline.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from dispatcher.registry import Task
from dispatcher.scheduling import (
    BLOCKED,
    PARALLEL,
    SERIAL_AFTER,
    SERIAL_LOCK,
    SchedulingAdmitter,
    derive_manifest,
)
from dispatcher.state import StateStore, TaskState

order_candidates = SchedulingAdmitter.order_candidates


def _task(**kw: Any) -> Task:
    """Task with the registry-required prompt filled in."""
    kw.setdefault("prompt_inline", "p")
    return Task(**kw)

from tests.test_dispatcher import (
    FakeControl,
    FakeRun,
    FakeRouter,
    FakeWorkspace,
    _make_config,
    _stub_pass_gate,
    _write_inbox,
    env,
)


REPO = "h4z4rd95/Plan-Project-Situation"
H1 = "a" * 40
H2 = "b" * 40


# --- helpers ------------------------------------------------------------------


def _script_head(monkeypatch, heads: dict[str, str]) -> None:
    """Replace the remote-HEAD probe with a deterministic per-task value."""
    def fake(self, task, state, control=None):
        return heads.get(task.id, H1)
    monkeypatch.setattr(SchedulingAdmitter, "refresh_remote_head", fake)


def _decision_rows(store: StateStore, task_id: str) -> list[dict[str, Any]]:
    with store._conn() as conn:
        rows = conn.execute(
            "SELECT * FROM scheduling_decisions WHERE task_id = ? ORDER BY id",
            (task_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def _lock_rows(store: StateStore) -> list[dict[str, Any]]:
    with store._conn() as conn:
        rows = conn.execute("SELECT * FROM resource_locks ORDER BY resource").fetchall()
    return [dict(r) for r in rows]


def _git_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=path, check=True)
    (path / "src").mkdir(exist_ok=True)
    (path / "src" / "app.py").write_text("print('base')\n")
    subprocess.run(["git", "add", "-A"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "base"], cwd=path, check=True)


# --- admitter-level units (no tick) ---------------------------------------------


def _admitter(tmp_path) -> SchedulingAdmitter:
    store = StateStore(tmp_path / "d.sqlite")
    return SchedulingAdmitter(store, lambda *a: None)


def test_scenario_01_disjoint_tasks_parallel(tmp_path):
    ad = _admitter(tmp_path)
    a = _task(id="A", title="a", repo=REPO, branch="main",
             write_paths=["docs/a.md"], read_paths=["docs/README.md"])
    b = _task(id="B", title="b", repo=REPO, branch="main",
             write_paths=["src/b.py"], read_paths=["src/a.py"])
    ma = derive_manifest(a, baseline_sha=H1)
    mb = derive_manifest(b, baseline_sha=H1)
    inbox = _inbox([a, b])
    d_a = ad.admit(a, ma, states={}, running_manifests={}, admitted={}, inbox=inbox)
    assert d_a.decision == PARALLEL
    d_b = ad.admit(b, mb, states={}, running_manifests={},
                   admitted={a.id: ma}, inbox=inbox)
    assert d_b.decision == PARALLEL


def test_scenario_02_same_file_write_write_serializes(tmp_path):
    ad = _admitter(tmp_path)
    a = _task(id="A", title="a", repo=REPO, branch="main",
             write_paths=["src/shared.py"], priority=200)
    b = _task(id="B", title="b", repo=REPO, branch="main",
             write_paths=["src/shared.py"], priority=100)
    ma = derive_manifest(a, baseline_sha=H1)
    mb = derive_manifest(b, baseline_sha=H1)
    inbox = _inbox([a, b])
    # A admitted first (higher priority); B then sees A's write surface.
    assert ad.admit(a, ma, states={}, running_manifests={}, admitted={},
                    inbox=inbox).decision == PARALLEL
    d_b = ad.admit(b, mb, states={}, running_manifests={},
                   admitted={a.id: ma}, inbox=inbox)
    assert d_b.decision in (SERIAL_LOCK, SERIAL_AFTER)
    assert a.id in d_b.lock_owner or a.id in d_b.predecessor


def test_scenario_03_producer_consumer_serialize(tmp_path):
    # Producer writes src/core.py; consumer reads it, writes src/consumer.py.
    # Different file paths, same contract surface — must serialize with the
    # consumer waiting for the producer regardless of priority.
    producer = _task(id="PROD", title="p", repo=REPO, branch="main",
                    write_paths=["src/core.py"], priority=100)
    consumer = _task(id="CONS", title="c", repo=REPO, branch="main",
                    write_paths=["src/consumer.py"],
                    read_paths=["src/core.py"], priority=900)  # higher priority!
    mp = derive_manifest(producer, baseline_sha=H1)
    mc = derive_manifest(consumer, baseline_sha=H1)
    order, cyclic = order_candidates([consumer, producer],
                                     {producer.id: mp, consumer.id: mc})
    assert cyclic == set()
    assert order[0].id == "PROD"  # producer moved ahead of the higher-priority consumer
    ad = _admitter(tmp_path)
    inbox = _inbox([producer, consumer])
    assert ad.admit(producer, mp, states={}, running_manifests={}, admitted={},
                    inbox=inbox).decision == PARALLEL
    d_c = ad.admit(consumer, mc, states={}, running_manifests={},
                   admitted={producer.id: mp}, inbox=inbox)
    assert d_c.decision == SERIAL_AFTER
    assert d_c.predecessor == "PROD"


def test_scenario_04_shared_lockfile_schema_serializes(tmp_path):
    ad = _admitter(tmp_path)
    # Same migration file touched by two tasks (schema/migration contract).
    a = _task(id="A", title="a", repo=REPO, branch="main",
             write_paths=["supabase/migrations/0001_init.sql"], priority=200)
    b = _task(id="B", title="b", repo=REPO, branch="main",
             write_paths=["supabase/migrations/0001_init.sql"], priority=100)
    ma = derive_manifest(a, baseline_sha=H1)
    mb = derive_manifest(b, baseline_sha=H1)
    assert any(":migration:" in k for k in ma.resource_keys())
    inbox = _inbox([a, b])
    d_b = ad.admit(b, mb, states={}, running_manifests={},
                   admitted={a.id: ma}, inbox=inbox)
    assert d_b.decision in (SERIAL_LOCK, SERIAL_AFTER)
    # Same lockfile case: two tasks writing the repo lockfile.
    a2 = _task(id="A2", title="a", repo=REPO, branch="main",
              write_paths=["poetry.lock"], priority=200)
    b2 = _task(id="B2", title="b", repo=REPO, branch="main",
              write_paths=["poetry.lock"], priority=100)
    ma2 = derive_manifest(a2, baseline_sha=H1)
    mb2 = derive_manifest(b2, baseline_sha=H1)
    assert any("lockfile:poetry.lock" in k for k in ma2.resource_keys())
    d_b2 = ad.admit(b2, mb2, states={}, running_manifests={},
                    admitted={a2.id: ma2}, inbox=_inbox([a2, b2]))
    assert d_b2.decision in (SERIAL_LOCK, SERIAL_AFTER)


def test_scenario_05_cross_repo_shared_runtime_serializes(tmp_path):
    ad = _admitter(tmp_path)
    a = _task(id="A", title="a", repo="owner/repo-x", branch="main",
             write_paths=["deploy/x.py"], shared_resources=["runtime:dokploy-nginx"])
    b = _task(id="B", title="b", repo="owner/repo-y", branch="main",
             write_paths=["deploy/y.py"], shared_resources=["runtime:dokploy-nginx"])
    ma = derive_manifest(a, baseline_sha=H1)
    mb = derive_manifest(b, baseline_sha=H1)
    assert "runtime:dokploy-nginx" in ma.shared_resources
    inbox = _inbox([a, b])
    d_b = ad.admit(b, mb, states={}, running_manifests={},
                   admitted={a.id: ma}, inbox=inbox)
    assert d_b.decision == SERIAL_LOCK
    assert d_b.resource == "runtime:dokploy-nginx"


def test_scenario_06_parallel_safe_cannot_bypass_conflict(tmp_path):
    ad = _admitter(tmp_path)
    a = _task(id="A", title="a", repo=REPO, branch="main",
             write_paths=["src/conf.py"], parallel_safe=True, priority=200)
    b = _task(id="B", title="b", repo=REPO, branch="main",
             write_paths=["src/conf.py"], parallel_safe=True, priority=100)
    ma = derive_manifest(a, baseline_sha=H1)
    mb = derive_manifest(b, baseline_sha=H1)
    inbox = _inbox([a, b])
    d_b = ad.admit(b, mb, states={}, running_manifests={},
                   admitted={a.id: ma}, inbox=inbox)
    assert d_b.decision in (SERIAL_LOCK, SERIAL_AFTER)


def test_scenario_07_explicit_dependency_waits(tmp_path):
    ad = _admitter(tmp_path)
    a = _task(id="A", title="a", repo=REPO, branch="main", write_paths=["src/a.py"])
    b = _task(id="B", title="b", repo=REPO, branch="main",
             write_paths=["src/b.py"], dependencies=["A"])
    ma = derive_manifest(a, baseline_sha=H1)
    mb = derive_manifest(b, baseline_sha=H1)
    inbox = _inbox([a, b])
    # A not DONE -> B waits for A.
    d_b = ad.admit(b, mb, states={}, running_manifests={}, admitted={}, inbox=inbox)
    assert d_b.decision == SERIAL_AFTER
    assert d_b.predecessor == "A"
    # A DONE -> B is free.
    from dispatcher.state import TaskState
    states = {"A": TaskState(task_id="A", status="DONE"),
              "B": TaskState(task_id="B", status="READY")}
    d_b2 = ad.admit(b, mb, states=states, running_manifests={}, admitted={}, inbox=inbox)
    assert d_b2.decision == PARALLEL


def test_scenario_11_ambiguous_dependency_blocked(tmp_path):
    ad = _admitter(tmp_path)
    b = _task(id="B", title="b", repo=REPO, branch="main",
             write_paths=["src/b.py"], dependencies=["GHOST"])
    mb = derive_manifest(b, baseline_sha=H1)
    d = ad.admit(b, mb, states={}, running_manifests={}, admitted={},
                 inbox=_inbox([b]))
    assert d.decision == BLOCKED
    assert any("GHOST" in r for r in d.reasons)


def test_unscoped_task_blocks_independence(tmp_path):
    ad = _admitter(tmp_path)
    scoped = _task(id="S", title="s", repo=REPO, branch="main",
                  write_paths=["docs/s.md"])
    unscoped = _task(id="U", title="u", repo=REPO, branch="main")  # no write set
    ms = derive_manifest(scoped, baseline_sha=H1)
    mu = derive_manifest(unscoped, baseline_sha=H1)
    assert mu.unscoped
    d = ad.admit(scoped, ms, states={}, running_manifests={"U": mu},
                 admitted={}, inbox=_inbox([scoped, unscoped]))
    assert d.decision == BLOCKED
    assert any("unscoped" in r for r in d.reasons)


def test_decision_ledger_dedup_and_reeval(tmp_path):
    store = StateStore(tmp_path / "d.sqlite")
    ad = SchedulingAdmitter(store, lambda *a: None)
    a = _task(id="A", title="a", repo=REPO, branch="main", write_paths=["src/a.py"])
    b = _task(id="B", title="b", repo=REPO, branch="main",
             write_paths=["src/b.py"], read_paths=["src/a.py"])
    ma = derive_manifest(a, baseline_sha=H1)
    mb = derive_manifest(b, baseline_sha=H1)
    inbox = _inbox([a, b])
    d1 = ad.admit(b, mb, states={}, running_manifests={},
                  admitted={a.id: ma}, inbox=inbox)
    assert d1.decision == SERIAL_AFTER
    # Duplicate tick, identical inputs: no new ledger row.
    d1b = ad.admit(b, mb, states={}, running_manifests={},
                   admitted={a.id: ma}, inbox=inbox)
    assert d1b.decision == d1.decision
    assert len(_decision_rows(store, "B")) == 1
    # Re-evaluation after a baseline change always appends.
    mb2 = derive_manifest(b, baseline_sha=H2)
    d2 = ad.admit(b, mb2, states={}, running_manifests={},
                  admitted={a.id: derive_manifest(a, baseline_sha=H2)},
                  inbox=inbox, reeval=f"baseline_refreshed:{H1[:12]}->{H2[:12]}")
    assert len(_decision_rows(store, "B")) == 2
    assert _decision_rows(store, "B")[-1]["reeval"].startswith("baseline_refreshed:")
    # Changed decision value also appends (distinct row).
    d3 = ad.admit(b, derive_manifest(b, baseline_sha=H1), states={},
                  running_manifests={}, admitted={}, inbox=inbox)
    assert d3.decision == PARALLEL
    assert len(_decision_rows(store, "B")) == 3


def test_lock_reclaim_requires_terminal_owner(tmp_path):
    store = StateStore(tmp_path / "d.sqlite")
    ad = SchedulingAdmitter(store, lambda *a: None)
    a = _task(id="A", title="a", repo=REPO, branch="main",
             write_paths=["src/a.py"], shared_resources=["db:marketdata"])
    b = _task(id="B", title="b", repo=REPO, branch="main",
             write_paths=["src/b.py"], shared_resources=["db:marketdata"])
    ma = derive_manifest(a, baseline_sha=H1)
    res = ad.lock_resources(a, ma)
    assert "db:marketdata" in res
    # B cannot take the lock while A is alive (non-terminal, live lease).
    got, owner = store.acquire_resource_lock("B", "db:marketdata", 1200)
    assert got is False and owner == "A"
    # A becomes terminal but its lease is still live -> lock stays.
    from dispatcher.state import TaskState
    store.upsert(TaskState(task_id="A", status="DONE"))
    got, owner = store.acquire_resource_lock("B", "db:marketdata", 1200)
    assert got is False and owner == "A"
    # Lease expired + terminal owner -> B reclaims.
    now = time.time()
    with store._conn() as conn:
        conn.execute("UPDATE resource_locks SET lease_expiry = ? WHERE resource = 'db:marketdata'",
                     (now - 10,))
    got, owner = store.acquire_resource_lock("B", "db:marketdata", 1200)
    assert got is True and owner == "B"
    # Release is idempotent.
    assert ad.release_resources("B") == 1
    assert ad.release_resources("B") == 0
    rows = _lock_rows(store)
    assert len([r for r in rows if r["resource"] == "db:marketdata"]) == 1


def _inbox(tasks: list[Task]):
    from dispatcher.registry import Inbox
    return Inbox(tasks=tasks)


# --- tick-level integration (fake router, scripted remote heads) -----------------


def test_tick_scenario_01_two_disjoint_claimed_same_tick(env, monkeypatch):
    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    _script_head(monkeypatch, {})
    a = _task(id="T-A", title="a", repo=REPO, branch="main",
             write_paths=["docs/a.md"], priority=200)
    b = _task(id="T-B", title="b", repo=REPO, branch="main",
             write_paths=["src/b.py"], priority=100)
    _write_inbox(control_dir, [a, b])
    report = d.tick()
    assert sorted(report.claimed) == ["T-A", "T-B"]


def test_tick_scenario_02_and_06_same_file_serializes_across_ticks(env, monkeypatch):
    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    _script_head(monkeypatch, {t.id: H1 for t in []})
    a = _task(id="T-A", title="a", repo=REPO, branch="main",
             write_paths=["src/shared.py"], parallel_safe=True, priority=200)
    b = _task(id="T-B", title="b", repo=REPO, branch="main",
             write_paths=["src/shared.py"], parallel_safe=True, priority=100)
    _write_inbox(control_dir, [a, b])
    r1 = d.tick()
    # A claimed and finished (fake router is synchronous); B serialized this
    # tick even though BOTH are parallel_safe=True (scenario 6) — only the
    # conflict-aware decision can explain the skip.
    assert r1.claimed == ["T-A"]
    assert "T-B" in r1.skipped
    rows = _decision_rows(store, "T-B")
    assert rows and rows[0]["decision"] in (SERIAL_LOCK, SERIAL_AFTER)
    assert "T-A" in (rows[0]["lock_owner"] + rows[0]["predecessor"])
    r2 = d.tick()
    assert r2.claimed == ["T-B"]
    # After A completed, the lock was swept; B's decision changed.
    rows2 = _decision_rows(store, "T-B")
    assert rows2[-1]["decision"] == PARALLEL


def test_tick_scenario_07_dependency_waits_then_claims(env, monkeypatch):
    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    _script_head(monkeypatch, {})
    a = _task(id="T-A", title="a", repo=REPO, branch="main",
             write_paths=["src/a.py"], priority=100)
    b = _task(id="T-B", title="b", repo=REPO, branch="main",
             write_paths=["src/b.py"], dependencies=["T-A"], priority=900)
    _write_inbox(control_dir, [a, b])
    r1 = d.tick()
    # B is higher priority but must wait for A (not DONE yet). Dependency
    # enforcement lives in _eligible (pre-existing): B is not even admitted
    # to the decision loop, and its skip reason records the missing dep.
    assert r1.claimed == ["T-A"]
    assert store.get("T-B").last_skip_reason.startswith("dependency T-A")
    assert _decision_rows(store, "T-B") == []  # never reached admission
    r2 = d.tick()
    assert r2.claimed == ["T-B"]
    # On claim the stale skip reason is cleared; the fresh decision is PARALLEL.
    assert store.get("T-B").last_skip_reason == ""
    rows2 = _decision_rows(store, "T-B")
    assert rows2 and rows2[-1]["decision"] == PARALLEL


def test_tick_scenario_08_baseline_refresh_reval_before_claim(env, monkeypatch):
    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    heads = {"T-A": H1, "T-B": H1}
    _script_head(monkeypatch, heads)
    a = _task(id="T-A", title="a", repo=REPO, branch="main",
             write_paths=["src/shared.py"], priority=200)
    b = _task(id="T-B", title="b", repo=REPO, branch="main",
             write_paths=["src/shared.py"], priority=100)
    _write_inbox(control_dir, [a, b])
    r1 = d.tick()
    assert r1.claimed == ["T-A"]
    rows1 = _decision_rows(store, "T-B")
    assert rows1 and rows1[0]["baseline_sha"] == H1
    # Predecessor finished and pushed: remote HEAD moved.
    heads["T-B"] = H2
    r2 = d.tick()
    assert r2.claimed == ["T-B"]
    rows2 = _decision_rows(store, "T-B")
    last = rows2[-1]
    # A fresh decision was recorded for the new baseline (re-evaluation).
    assert last["baseline_sha"] == H2
    assert last["decision"] == PARALLEL
    assert last["reeval"].startswith("baseline_refreshed:")
    assert last["manifest_sha"] != rows1[0]["manifest_sha"]
    assert store.get("T-B").baseline_sha == H2


def test_tick_scenario_09_midrun_write_set_grows_blocks_new_claims(env, monkeypatch, tmp_path):
    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    _script_head(monkeypatch, {})

    # A is already RUNNING from an earlier tick (detached worker): a real git
    # workspace, clean at its baseline, with a live lease.
    ws_a = tmp_path / "ws-a"
    _git_repo(ws_a)
    from dispatcher.state import TaskState
    now = time.time()
    store.upsert(TaskState(task_id="T-A", status="READY"))
    claimed = store.claim("T-A", lease_seconds=3600)
    assert claimed is not None
    claimed.workspace_path = str(ws_a)
    claimed.baseline_sha = H1
    store.upsert(claimed)
    ad = SchedulingAdmitter(store, lambda *a: None)
    ad.lock_resources(
        _task(id="T-A", title="a", repo=REPO, branch="main",
             write_paths=["src/app.py"]),
        derive_manifest(_task(id="T-A", title="a", repo=REPO, branch="main",
                             write_paths=["src/app.py"]), baseline_sha=H1),
    )

    a_task = _task(id="T-A", title="a", repo=REPO, branch="main",
                  write_paths=["src/app.py"], priority=500)
    # While A's workspace is clean, a task writing src/app.py conflicts on
    # the DECLARED surface: it must not be admitted.
    b = _task(id="T-B", title="b", repo=REPO, branch="main",
             write_paths=["src/app.py"], priority=100)
    _write_inbox(control_dir, [a_task, b])
    r1 = d.tick()
    assert "T-A" not in r1.claimed and "T-B" not in r1.claimed
    assert store.get("T-A").status == "RUNNING"  # running work untouched

    # A's in-flight write set grows with a file nobody declared.
    (ws_a / "src" / "grown.py").write_text("x = 1\n")
    c = _task(id="T-C", title="c", repo=REPO, branch="main",
             write_paths=["src/grown.py"], priority=100)
    _write_inbox(control_dir, [a_task, c])
    r2 = d.tick()
    # C now conflicts with A's ACTUAL (grown) write set: blocked from claiming,
    # A still running, no kill.
    assert "T-C" not in r2.claimed
    assert "T-C" in r2.skipped or "T-C" in r2.blocked
    rows_c = _decision_rows(store, "T-C")
    assert rows_c and rows_c[0]["decision"] in (SERIAL_LOCK, SERIAL_AFTER)
    assert "T-A" in (rows_c[0]["lock_owner"] + rows_c[0]["predecessor"])
    assert store.get("T-A").status == "RUNNING"
    assert "src/grown.py" in rows_c[0]["conflict_paths"]

    # A finishes (terminal) -> sweep releases its locks -> the same write set
    # is admissible on the next tick without any queue surgery.
    store.release("T-A", status="DONE", summary="done", commit_sha=H2)
    d3 = d.tick()
    assert "T-C" in d3.claimed
    # A's locks are gone; C now legitimately holds its own.
    assert not any(r["state"] == "HELD" and r["owner_task"] == "T-A"
                   for r in _lock_rows(store))


def test_tick_scenario_10_duplicate_tick_and_restart_no_duplicate_ledger_or_locks(
        env, monkeypatch, tmp_path):
    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    _script_head(monkeypatch, {})

    # R is RUNNING (detached worker from an earlier tick) holding a lock on
    # the write surface B wants; B is READY behind it.
    ws_r = tmp_path / "ws-r"
    _git_repo(ws_r)
    store.upsert(TaskState(task_id="T-R", status="READY"))
    claimed = store.claim("T-R", lease_seconds=3600)
    assert claimed is not None
    claimed.workspace_path = str(ws_r)
    store.upsert(claimed)
    r_task = _task(id="T-R", title="r", repo=REPO, branch="main",
                  write_paths=["src/shared.py"], priority=500)
    b = _task(id="T-B", title="b", repo=REPO, branch="main",
             write_paths=["src/shared.py"], priority=100)
    _write_inbox(control_dir, [b, r_task])
    # R's durable locks (acquired at its original claim time).
    ad = SchedulingAdmitter(store, lambda *a: None)
    ad.lock_resources(r_task, derive_manifest(r_task, baseline_sha=""))

    r1 = d.tick()
    assert "T-B" not in r1.claimed
    n1_rows = len(_decision_rows(store, "T-B"))
    n1_locks = len(_lock_rows(store))
    assert n1_rows >= 1 and n1_locks >= 1

    # Duplicate tick, unchanged inputs: no new decision row, no lock churn.
    r2 = d.tick()
    assert "T-B" not in r2.claimed
    assert len(_decision_rows(store, "T-B")) == n1_rows
    assert len(_lock_rows(store)) == n1_locks

    # Restart recovery: a brand-new dispatcher process over the same DB sees
    # the same lock state; nothing is duplicated or leaked.
    from tests.test_dispatcher import Dispatcher, FakeControl, FakeRouter, FakeWorkspace
    d2 = Dispatcher(cfg, store=store, router=FakeRouter(),
                    workspace=FakeWorkspace(), control=FakeControl())
    r3 = d2.tick()
    assert "T-B" not in r3.claimed
    assert len(_decision_rows(store, "T-B")) == n1_rows
    assert len(_lock_rows(store)) == n1_locks
    assert all(r["owner_task"] == "T-R" for r in _lock_rows(store)
               if r["state"] == "HELD")


def test_tick_circular_pair_blocked(env, monkeypatch):
    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    _script_head(monkeypatch, {})
    a = _task(id="T-A", title="a", repo=REPO, branch="main",
             write_paths=["src/x.py"], read_paths=["src/y.py"], priority=200)
    b = _task(id="T-B", title="b", repo=REPO, branch="main",
             write_paths=["src/y.py"], read_paths=["src/x.py"], priority=100)
    _write_inbox(control_dir, [a, b])
    r = d.tick()
    # Circular read/write dependency: neither can be proven independent.
    assert "T-A" in r.blocked and "T-B" in r.blocked
    assert not r.claimed
    rows_a = _decision_rows(store, "T-A")
    assert rows_a and rows_a[-1]["decision"] == BLOCKED
    assert "circular" in rows_a[-1]["reasons"]


def test_three_task_dependency_cycle_blocks_every_member(tmp_path):
    a = _task(id="A", title="a", repo=REPO, write_paths=["src/a.py"],
              read_paths=["src/c.py"])
    b = _task(id="B", title="b", repo=REPO, write_paths=["src/b.py"],
              read_paths=["src/a.py"])
    c = _task(id="C", title="c", repo=REPO, write_paths=["src/c.py"],
              read_paths=["src/b.py"])
    manifests = {
        "A": derive_manifest(a, baseline_sha=H1),
        "B": derive_manifest(b, baseline_sha=H1),
        "C": derive_manifest(c, baseline_sha=H1),
    }
    ordered, cyclic = order_candidates([a, b, c], manifests)
    assert {task.id for task in ordered} == {"A", "B", "C"}
    assert cyclic == {"A", "B", "C"}


def test_lock_resources_rolls_back_partial_acquisition(tmp_path):
    store = StateStore(tmp_path / "d.sqlite")
    ad = SchedulingAdmitter(store, lambda *a: None)
    task = _task(id="B", title="b", repo=REPO,
                 write_paths=["src/a.py", "src/b.py"])
    # The second resource is already owned by another task. The first may be
    # acquired before the collision, but all partial locks must be rolled back.
    store.acquire_resource_lock("OTHER", f"write:{REPO}:src/b.py", 1200)
    with pytest.raises(RuntimeError, match="held by task 'OTHER'"):
        ad.lock_resources(task, derive_manifest(task, baseline_sha=H1))
    rows = _lock_rows(store)
    assert not any(r["owner_task"] == "B" and r["state"] == "HELD" for r in rows)
    assert any(r["owner_task"] == "OTHER" and r["state"] == "HELD" for r in rows)


def test_decision_ledger_write_failure_fails_closed(tmp_path):
    class BrokenStore:
        def record_scheduling_decision(self, _record):
            raise OSError("simulated ledger write failure")

    ad = SchedulingAdmitter(BrokenStore(), lambda *a: None)
    task = _task(id="A", title="a", repo=REPO, write_paths=["src/a.py"])
    decision = ad.admit(
        task, derive_manifest(task, baseline_sha=H1),
        states={}, running_manifests={}, admitted={}, inbox=_inbox([task]),
    )
    assert decision.decision == BLOCKED
    assert any("fail closed" in reason for reason in decision.reasons)


def test_tick_remote_head_unavailable_blocks_repo_task(env, monkeypatch):
    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    _script_head(monkeypatch, {"T-NO-HEAD": None})
    task = _task(id="T-NO-HEAD", title="cannot verify baseline",
                 repo=REPO, branch="main", write_paths=["src/new.py"])
    _write_inbox(control_dir, [task])
    report = d.tick()
    assert report.claimed == []
    assert "T-NO-HEAD" in report.blocked
    assert not router.calls
    state = store.get("T-NO-HEAD")
    assert state is not None
    assert state.status == "READY"
    assert "remote HEAD" in state.last_skip_reason


def test_tick_lock_failure_defers_without_running_worker(env, monkeypatch):
    from dispatcher.scheduling import SchedulingAdmitter

    d, cfg, store, router, workspace, control, control_dir = env
    _stub_pass_gate(monkeypatch)
    _script_head(monkeypatch, {"T-LOCK-RACE": H1})
    task = _task(id="T-LOCK-RACE", title="lock race",
                 repo=REPO, branch="main", write_paths=["src/new.py"])
    _write_inbox(control_dir, [task])
    monkeypatch.setattr(
        SchedulingAdmitter, "lock_resources",
        lambda self, task, manifest: (_ for _ in ()).throw(RuntimeError("held by peer")),
    )
    report = d.tick()
    assert report.claimed == []
    assert "T-LOCK-RACE" in report.skipped
    assert not router.calls
    state = store.get("T-LOCK-RACE")
    assert state is not None
    assert state.status == "READY"
