"""Focused tests for in-flight state visibility and duplicate suppression."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from dispatcher.state import (
    StateStore,
    TaskState,
    heartbeat_freshness,
    inflight_status_dict,
)


@pytest.fixture
def store(tmp_path):
    return StateStore(tmp_path / "state" / "dispatcher.sqlite")


def test_status_serialization_complete(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    claimed = store.claim("T-1", lease_seconds=60, source="issue-1")
    assert claimed is not None
    snap = store.inflight_status(task_id="T-1", stale_after_seconds=0)
    assert snap["task_id"] == "T-1"
    assert snap["lifecycle_state"] == "RUNNING"
    assert snap["source_issue"] == "issue-1"
    assert snap["heartbeat_freshness"] == "fresh"
    assert snap["run_id"] == claimed.run_id
    assert "started_at" in snap
    assert "last_heartbeat" in snap
    assert "session_id" in snap or "session" in snap
    assert "workspace_path" in snap or "workspace" in snap


def test_pending_claimed_running_distinction(store):
    store.upsert(TaskState(task_id="T-P", status="PENDING"))
    store.upsert(TaskState(task_id="T-R", status="READY"))
    claimed = store.claim("T-R", lease_seconds=60)
    assert claimed is not None
    # PENDING stays pending
    assert store.get("T-P").status == "PENDING"
    # CLAIMED status supported as active
    store.upsert(TaskState(task_id="T-C", status="CLAIMED"))
    snap_all = store.inflight_status(stale_after_seconds=0)
    by_id = {p["task_id"]: p for p in snap_all["tasks"]}
    assert by_id["T-P"]["lifecycle_state"] == "PENDING"
    assert by_id["T-P"]["active"] is False
    assert by_id["T-C"]["lifecycle_state"] == "CLAIMED"
    assert by_id["T-C"]["active"] is True
    assert by_id["T-R"]["lifecycle_state"] == "RUNNING"
    assert by_id["T-R"]["active"] is True


def test_heartbeat_fresh_stale_unknown(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    claimed = store.claim("T-1", lease_seconds=60)
    assert claimed is not None
    # fresh
    assert heartbeat_freshness(store.get("T-1"), stale_after_seconds=0) == "fresh"
    # stale: backdate expiry
    st = store.get("T-1")
    st.lease_expiry = time.time() - 1000
    store.upsert(st)
    assert heartbeat_freshness(store.get("T-1"), stale_after_seconds=0) == "stale"
    # unknown: owner hold, no lease fingerprint
    store.upsert(TaskState(task_id="T-2", status="RUNNING"))  # no lease
    assert heartbeat_freshness(store.get("T-2"), stale_after_seconds=0) == "unknown"


def test_duplicate_suppression_same_task_id(store):
    store.upsert(TaskState(task_id="T-1", status="READY", source="issue-1"))
    first = store.claim("T-1", lease_seconds=60, source="issue-1")
    assert first is not None
    # reset to READY to simulate re-dispatch attempt while another active? 
    # Instead second claim while still RUNNING must fail.
    store.upsert(TaskState(task_id="T-2", status="READY", source="issue-1"))
    second = store.claim("T-2", lease_seconds=60, source="issue-1")
    assert second is None


def test_duplicate_suppression_same_source_different_task(store):
    store.upsert(TaskState(task_id="T-1", status="READY", source="issue-9"))
    first = store.claim("T-1", lease_seconds=60, source="issue-9")
    assert first is not None
    store.upsert(TaskState(task_id="T-2", status="READY", source="issue-9"))
    second = store.claim("T-2", lease_seconds=60, source="issue-9")
    assert second is None
    assert store.find_active_duplicate("T-2", source="issue-9").task_id == "T-1"


def test_concurrent_claim_race(store):
    store.upsert(TaskState(task_id="T-1", status="READY", source="issue-1"))
    store.upsert(TaskState(task_id="T-2", status="READY", source="issue-1"))
    results = []

    def worker(tid):
        r = store.claim(tid, lease_seconds=60, source="issue-1")
        results.append(r)

    t1 = threading.Thread(target=worker, args=("T-1",))
    t2 = threading.Thread(target=worker, args=("T-2",))
    t1.start(); t2.start()
    t1.join(); t2.join()
    successes = [r for r in results if r is not None]
    assert len(successes) <= 1


def test_no_active_task_idle_output(store):
    snap = store.inflight_status(stale_after_seconds=0)
    assert snap["idle"] is True
    assert snap["active"] == []
    # human-ish: no active
    assert snap.get("active_count", 0) == 0


def test_ordinary_dispatch_regression_not_broken(store):
    # basic claim/release still works
    store.upsert(TaskState(task_id="T-1", status="READY"))
    c = store.claim("T-1", lease_seconds=60)
    assert c is not None
    assert c.status == "RUNNING"
    assert store.claim("T-1", lease_seconds=60) is None
    store.release("T-1", status="DONE", summary="ok")
    assert store.get("T-1").status == "DONE"
