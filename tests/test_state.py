"""Tests for dispatcher/state.py — durable state, atomic claims, stale recovery."""

from __future__ import annotations

import os
import time

import pytest

from dispatcher.state import (
    OutcomeRecord,
    StateStore,
    TaskState,
    lease_fingerprint,
    pid_alive,
    process_start_time,
)


@pytest.fixture
def store(tmp_path):
    return StateStore(tmp_path / "state" / "dispatcher.sqlite")


def test_schema_created(store):
    states = store.all_states()
    assert states == {}


def test_upsert_and_get(store):
    state = TaskState(task_id="T-1", status="READY")
    store.upsert(state)
    got = store.get("T-1")
    assert got is not None
    assert got.status == "READY"
    assert got.attempts == 0


def test_upsert_is_idempotent(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    store.upsert(TaskState(task_id="T-1", status="READY"))
    assert len(store.all_states()) == 1


def test_claim_is_atomic_and_increments_attempts(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    first = store.claim("T-1", lease_seconds=60)
    assert first is not None
    assert first.status == "RUNNING"
    assert first.attempts == 1
    # A second claim of the same READY row must fail (it is now RUNNING).
    assert store.claim("T-1", lease_seconds=60) is None


def test_claim_returns_none_when_not_ready(store):
    store.upsert(TaskState(task_id="T-1", status="DONE"))
    assert store.claim("T-1", lease_seconds=60) is None


def test_claim_returns_none_for_unknown_task(store):
    assert store.claim("T-missing", lease_seconds=60) is None


def test_release_moves_to_terminal(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    store.claim("T-1", lease_seconds=60)
    state = store.release("T-1", status="DONE", summary="ok",
                          outcome={"evidence": []}, commit_sha="abc123")
    assert state.status == "DONE"
    assert state.commit_sha == "abc123"
    assert state.lease_pid is None
    got = store.get("T-1")
    assert got.status == "DONE"
    assert '"evidence"' in got.outcome_json


def test_release_unknown_task(store):
    with pytest.raises(KeyError):
        store.release("T-nope", status="DONE")


def test_block_and_reset(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    store.block("T-1", reason="waiting on owner")
    assert store.get("T-1").status == "BLOCKED"
    store.reset_to_ready("T-1", note="unblocked")
    assert store.get("T-1").status == "READY"


def test_cancel(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    store.cancel("T-1", reason="operator cancelled")
    assert store.get("T-1").status == "CANCELLED"


def test_stale_leases_detects_expired(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    state = store.claim("T-1", lease_seconds=1)
    assert state is not None
    # Backdate the expiry into the past.
    state.lease_expiry = time.time() - 10_000
    state.lease_pid = os.getpid()
    state.lease_start = process_start_time(os.getpid())
    store.upsert(state)
    stale = store.stale_leases(stale_after_seconds=0)
    assert [s.task_id for s in stale] == ["T-1"]


def test_stale_leases_ignores_live_fresh_lease(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    claimed = store.claim("T-1", lease_seconds=3600)
    assert claimed is not None
    assert store.stale_leases(stale_after_seconds=0) == []


def test_reap_stale_retries_then_fails(store):
    store.upsert(TaskState(task_id="T-1", status="READY"))
    state = store.claim("T-1", lease_seconds=1)
    assert state is not None
    state.lease_expiry = time.time() - 10_000
    store.upsert(state)

    actions = store.reap_stale(stale_after_seconds=0, max_retries=3)
    assert actions == [("T-1", "reset_to_ready (lease expired)")]
    assert store.get("T-1").status == "READY"

    # Exhaust retries.
    for _ in range(3):
        s = store.claim("T-1", lease_seconds=1)
        if s is None:
            # reap reset it to READY; claim again to consume an attempt.
            store.reset_to_ready("T-1")
            s = store.claim("T-1", lease_seconds=1)
        assert s is not None
        s.lease_expiry = time.time() - 10_000
        store.upsert(s)
    store.reap_stale(stale_after_seconds=0, max_retries=3)
    assert store.get("T-1").status == "FAILED"


def test_pid_alive_current_process():
    pid, started = lease_fingerprint()
    assert pid == os.getpid()
    assert started > 0
    assert pid_alive(pid, started) is True


def test_pid_alive_dead_pid():
    assert pid_alive(4_194_303) is False  # almost certainly unused
    assert pid_alive(-1) is False


def test_outcome_record_roundtrip(tmp_path):
    rec = OutcomeRecord(task_id="T-1", status="DONE", summary="ok",
                        evidence=[{"kind": "session"}], commit_sha="abc")
    path = rec.write(tmp_path / "outcomes")
    assert path.is_file()
    back = OutcomeRecord.read(path)
    assert back.task_id == "T-1"
    assert back.evidence == [{"kind": "session"}]
    assert back.commit_sha == "abc"


def test_outcome_write_is_atomic(tmp_path):
    d = tmp_path / "outcomes"
    d.mkdir()
    rec = OutcomeRecord(task_id="T-1", status="DONE", summary="ok")
    path = rec.write(d)
    # no leftover tmp files
    assert not list(d.glob("*.tmp"))
    assert path.name == "T-1.json"
