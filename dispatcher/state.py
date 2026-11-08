"""Durable task state and atomic lease management.

The dispatcher state lives in ``state/dispatcher.sqlite`` next to the
dispatcher repo (gitignored). Only the tick process writes to it; worker
processes spool outcomes to ``state/outcomes/<task_id>.json`` and the tick
ingests them. A claim is atomic across concurrent ticks: it is one SQL
``UPDATE ... WHERE status='READY'`` guarded by a transaction, plus a
``(pid, process_start_time)`` lease fingerprint so a recycled PID is never
treated as a live owner and a dead owner is recovered safely after a restart.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

__all__ = [
    "TaskState",
    "StateStore",
    "lease_fingerprint",
    "pid_alive",
    "normalize_status",
]

TERMINAL = {"DONE", "FAILED", "CANCELLED"}
RUNNABLE = {"READY"}
# Lifecycle states that count as an in-flight execution for duplicate guards.
ACTIVE_EXECUTION_STATES = {"CLAIMED", "RUNNING"}
INFLIGHT_STATES = ACTIVE_EXECUTION_STATES


def _now() -> float:
    return time.time()


def process_start_time(pid: int) -> float | None:
    """Best-effort start time of a live process, else None.

    Windows: use the process creation time via ctypes when available; Linux:
    read field 22 (starttime ticks) from /proc/<pid>/stat. The proc directory's
    mtime is mutable and must not be used as a process identity fingerprint.
    On POSIX systems without /proc, return None (fail-closed recovery).
    """
    if pid <= 0:
        return None
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not handle:
                return None
            try:
                creation = wintypes.FILETIME()
                exit_time = wintypes.FILETIME()
                kernel = wintypes.FILETIME()
                user = wintypes.FILETIME()
                ok = kernel32.GetProcessTimes(
                    handle, ctypes.byref(creation), ctypes.byref(exit_time),
                    ctypes.byref(kernel), ctypes.byref(user),
                )
                if not ok:
                    return None
                # FILETIME is 100ns ticks since 1601-01-01.
                ticks = (creation.dwHighDateTime << 32) | creation.dwLowDateTime
                # 116444736000000000 = ticks between 1601 and 1970 in 100ns.
                return (ticks - 116444736000000000) / 1e7
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return None
    try:
        # /proc/<pid> directory mtime can change while the process is alive
        # (for example when proc entries are added/removed). Use stat field 22,
        # the kernel starttime counter in clock ticks since boot, instead.
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        close = raw.rfind(")")  # comm (field 2) may itself contain spaces
        if close < 0:
            return None
        fields = raw[close + 2 :].split()  # begins with field 3 (state)
        if len(fields) <= 19:
            return None
        return float(int(fields[19]))  # field 22: starttime
    except (OSError, ValueError, IndexError):
        return None


def pid_alive(pid: int, start_time: float | None = None) -> bool:
    """True when ``pid`` is alive and matches the recorded start time."""
    if pid <= 0:
        return False
    actual = process_start_time(pid)
    if actual is None:
        return False
    if start_time is None:
        return True
    # The fingerprint is stable (Linux starttime ticks or Windows creation
    # time), so use a tight tolerance; a 10-second window can confuse PID reuse.
    return abs(actual - start_time) < 0.5


def lease_fingerprint() -> tuple[int, float]:
    """(pid, process_start_time) for this process."""
    pid = os.getpid()
    return pid, process_start_time(pid) or 0.0


def normalize_status(status: str) -> str:
    status = (status or "").strip().upper()
    return status


def heartbeat_freshness(state: Any, *, stale_after_seconds: int = 0,
                       now: float | None = None) -> str:
    """Classify a task state's heartbeat as fresh/stale/unknown.

    Unknown is never reported as idle: a row with no lease fingerprint (an
    owner hold) or no expiry keeps "unknown" so callers cannot mistake it
    for a free slot.
    """
    if state is None:
        return "unknown"
    if now is None:
        now = _now()
    status = normalize_status(getattr(state, "status", "") or "")
    if status not in ACTIVE_EXECUTION_STATES and status not in ("READY",):
        # Only active execution rows carry a meaningful heartbeat; everything
        # else is unknown rather than silently idle.
        pass
    pid = getattr(state, "lease_pid", None)
    start = getattr(state, "lease_start", None)
    expiry = getattr(state, "lease_expiry", None)
    # Owner holds (no fingerprint) are unknown, never stale/idle.
    if pid is None and start is None:
        return "unknown"
    if expiry is None:
        return "unknown"
    try:
        if now > float(expiry) + max(int(stale_after_seconds or 0), 0):
            return "stale"
    except (TypeError, ValueError):
        return "unknown"
    try:
        if not pid_alive(int(pid or 0), start):
            return "stale"
    except (TypeError, ValueError):
        return "unknown"
    return "fresh"


def is_active_state(state: Any) -> bool:
    if state is None:
        return False
    return normalize_status(getattr(state, "status", "") or "") in ACTIVE_EXECUTION_STATES


def inflight_status_dict(state: Any, *, task: Any = None,
                        stale_after_seconds: int = 0,
                        now: float | None = None) -> dict[str, Any]:
    """Serialize one task state into the canonical in-flight status dict."""
    if now is None:
        now = _now()
    status = normalize_status(getattr(state, "status", "") or "") if state is not None else ""
    if state is None:
        state_obj = None
    else:
        state_obj = state
    lease_pid = getattr(state_obj, "lease_pid", None) if state_obj is not None else None
    lease_start = getattr(state_obj, "lease_start", None) if state_obj is not None else None
    lease_expiry = getattr(state_obj, "lease_expiry", None) if state_obj is not None else None
    run_id = getattr(state_obj, "run_id", "") if state_obj is not None else ""
    session_id = getattr(state_obj, "session_id", "") if state_obj is not None else ""
    workspace_path = getattr(state_obj, "workspace_path", "") if state_obj is not None else ""
    source = getattr(state_obj, "source", "") if state_obj is not None else ""
    source_issue = getattr(state_obj, "source_issue", "") if state_obj is not None else ""
    if not source_issue:
        source_issue = source
    if not source:
        source = source_issue
    updated_at = getattr(state_obj, "updated_at", None) if state_obj is not None else None
    branch = ""
    if task is not None:
        branch = getattr(task, "branch", "") or ""
        if not source:
            source = getattr(task, "source", "") or ""
        if not source_issue:
            source_issue = getattr(task, "source", "") or ""
    freshness = heartbeat_freshness(state_obj, stale_after_seconds=stale_after_seconds, now=now)
    lifecycle = status if status else "PENDING"
    started_at = lease_start
    last_heartbeat = updated_at if updated_at is not None else lease_start
    payload = {
        "task_id": getattr(state_obj, "task_id", None) if state_obj is not None else (getattr(task, "id", None) if task is not None else None),
        "source": source,
        "source_issue": source_issue,
        "lifecycle_state": lifecycle,
        "status": lifecycle,
        "state": lifecycle,
        "run_id": run_id,
        "tick_run_id": run_id,
        "started_at": started_at,
        "started_at_epoch": started_at,
        "last_heartbeat": last_heartbeat,
        "last_heartbeat_at": last_heartbeat,
        "last_heartbeat_epoch": last_heartbeat,
        "heartbeat": last_heartbeat,
        "heartbeat_at": last_heartbeat,
        "lease_start": lease_start,
        "lease_expiry": lease_expiry,
        "heartbeat_freshness": freshness,
        "freshness": freshness,
        "heartbeat_status": freshness,
        "executor": lease_pid,
        "executor_pid": lease_pid,
        "lease_pid": lease_pid,
        "session": session_id,
        "session_id": session_id,
        "executor_session": session_id,
        "workspace": workspace_path,
        "workspace_path": workspace_path,
        "branch": branch,
        "active": is_active_state(state_obj),
        "idle": not is_active_state(state_obj),
    }
    # ISO-ish convenience (epoch only, no tz conversion beyond utc).
    try:
        import datetime as _dt
        if started_at is not None:
            payload["started_at_iso"] = _dt.datetime.utcfromtimestamp(float(started_at)).isoformat() + "Z"
    except Exception:
        payload["started_at_iso"] = None
    try:
        import datetime as _dt
        if last_heartbeat is not None:
            payload["last_heartbeat_iso"] = _dt.datetime.utcfromtimestamp(float(last_heartbeat)).isoformat() + "Z"
    except Exception:
        payload["last_heartbeat_iso"] = None
    return payload


@dataclass
class TaskState:
    task_id: str
    status: str
    attempts: int = 0
    lease_pid: int | None = None
    lease_start: float | None = None
    lease_expiry: float | None = None
    last_error: str = ""
    outcome_summary: str = ""
    outcome_json: str = ""
    session_id: str = ""
    # Unique identity of the *run* that claimed this row, set on every claim.
    # Terminal outcome publication keys on this (Issue #43): a resume/retry
    # run gets a fresh id so its terminal outcome is published once, while a
    # re-report of the same run is still deduplicated downstream. Empty for
    # rows seeded without a claim and for ledgers written before this field.
    run_id: str = ""
    workspace_path: str = ""
    commit_sha: str = ""
    # Last reason this task was skipped (not claimable) at eligibility check.
    # Persisted so a repeated non-actionable skip is logged once per change
    # instead of every tick (Issue #23).
    last_skip_reason: str = ""
    # Conflict-aware scheduling (v4): the branch head the task's resource
    # manifest was last evaluated against. Refreshed from the remote before
    # claim (SERIAL_AFTER re-evaluation); empty until first refresh.
    baseline_sha: str = ""
    # Provenance of the task's source issue (registry Task.source), carried
    # into the state row so duplicate execution can be detected by source
    # issue even when task identities differ (Issue inflight-state-guard).
    source: str = ""
    source_issue: str = ""
    updated_at: float = _now()

    def heartbeat_freshness(self, stale_after_seconds: int = 0, now: float | None = None) -> str:
        return heartbeat_freshness(self, stale_after_seconds=stale_after_seconds, now=now)

    def is_active(self) -> bool:
        return is_active_state(self)

    def to_status_dict(self, *args, **kwargs):
        return inflight_status_dict(self, *args, **kwargs)

    def to_row(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "attempts": self.attempts,
            "lease_pid": self.lease_pid,
            "lease_start": self.lease_start,
            "lease_expiry": self.lease_expiry,
            "last_error": self.last_error,
            "outcome_summary": self.outcome_summary,
            "outcome_json": self.outcome_json,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "workspace_path": self.workspace_path,
            "commit_sha": self.commit_sha,
            "last_skip_reason": self.last_skip_reason,
            "baseline_sha": self.baseline_sha,
            "source": self.source,
            "source_issue": self.source_issue or self.source,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "TaskState":
        return cls(
            task_id=row["task_id"],
            status=row["status"],
            attempts=row["attempts"],
            lease_pid=row["lease_pid"],
            lease_start=row["lease_start"],
            lease_expiry=row["lease_expiry"],
            last_error=row["last_error"] or "",
            outcome_summary=row["outcome_summary"] or "",
            outcome_json=row["outcome_json"] or "",
            session_id=row["session_id"] or "",
            run_id=row["run_id"] if "run_id" in row.keys() else "",
            workspace_path=row["workspace_path"] or "",
            commit_sha=row["commit_sha"] or "",
            last_skip_reason=row["last_skip_reason"] if "last_skip_reason" in row.keys() else "",
            baseline_sha=row["baseline_sha"] if "baseline_sha" in row.keys() else "",
            source=row["source"] if "source" in row.keys() else "",
            source_issue=(row["source_issue"] if "source_issue" in row.keys() else (row["source"] if "source" in row.keys() else "")),
            updated_at=row["updated_at"],
        )


    def __post_init__(self) -> None:
        # Keep the two provenance aliases in sync so callers can use either.
        if self.source_issue and not self.source:
            self.source = self.source_issue
        if self.source and not self.source_issue:
            self.source_issue = self.source


_SCHEMA = """
CREATE TABLE IF NOT EXISTS task_state (
    task_id         TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    lease_pid       INTEGER,
    lease_start     REAL,
    lease_expiry    REAL,
    last_error      TEXT NOT NULL DEFAULT '',
    outcome_summary TEXT NOT NULL DEFAULT '',
    outcome_json    TEXT NOT NULL DEFAULT '',
    session_id      TEXT NOT NULL DEFAULT '',
    run_id          TEXT NOT NULL DEFAULT '',
    workspace_path  TEXT NOT NULL DEFAULT '',
    commit_sha      TEXT NOT NULL DEFAULT '',
    last_skip_reason TEXT NOT NULL DEFAULT '',
    baseline_sha    TEXT NOT NULL DEFAULT '',
    source          TEXT NOT NULL DEFAULT '',
    source_issue    TEXT NOT NULL DEFAULT '',
    updated_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS scheduling_decisions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               REAL NOT NULL,
    run_id           TEXT NOT NULL DEFAULT '',
    task_id          TEXT NOT NULL,
    decision         TEXT NOT NULL,
    predecessor      TEXT NOT NULL DEFAULT '',
    resource         TEXT NOT NULL DEFAULT '',
    lock_owner       TEXT NOT NULL DEFAULT '',
    compared_task_ids TEXT NOT NULL DEFAULT '[]',
    baseline_sha     TEXT NOT NULL DEFAULT '',
    manifest_sha     TEXT NOT NULL DEFAULT '',
    resource_keys    TEXT NOT NULL DEFAULT '[]',
    conflict_paths   TEXT NOT NULL DEFAULT '[]',
    reasons          TEXT NOT NULL DEFAULT '[]',
    wait_condition   TEXT NOT NULL DEFAULT '',
    reeval           TEXT NOT NULL DEFAULT ''
);
-- Duplicate-tick idempotency: the same task, unchanged manifest and
-- unchanged comparison set yield at most one stored decision per distinct
-- decision value. Re-evaluations (reeval != '') always append.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sched_dedup
    ON scheduling_decisions(task_id, manifest_sha, compared_task_ids, decision, reeval);
CREATE TABLE IF NOT EXISTS resource_locks (
    resource      TEXT PRIMARY KEY,
    owner_task    TEXT NOT NULL,
    state         TEXT NOT NULL DEFAULT 'HELD',
    lease_pid     INTEGER,
    lease_start   REAL,
    lease_expiry  REAL,
    updated_at    REAL NOT NULL,
    note          TEXT NOT NULL DEFAULT ''
);
"""


class StateStore:
    """SQLite-backed durable state with atomic claims."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(_SCHEMA)
            # Existing DBs predate the last_skip_reason column (added for
            # Issue #23); add it idempotently. SQLite raises if it exists.
            try:
                conn.execute(
                    "ALTER TABLE task_state ADD COLUMN last_skip_reason "
                    "TEXT NOT NULL DEFAULT ''"
                )
            except sqlite3.OperationalError:
                pass
            # run_id (Issue #43): identity of the run that claimed the row,
            # so terminal outcome publication is idempotent per run instead
            # of per (task, status) — a resume run is a new run. Additive and
            # idempotent; rows written by older code keep '' and are handled
            # by the legacy dedup path downstream.
            try:
                conn.execute(
                    "ALTER TABLE task_state ADD COLUMN run_id "
                    "TEXT NOT NULL DEFAULT ''"
                )
            except sqlite3.OperationalError:
                pass
            # baseline_sha (conflict-aware scheduling, 2026-11-06): the branch
            # head each task's resource manifest was last evaluated against.
            try:
                conn.execute(
                    "ALTER TABLE task_state ADD COLUMN baseline_sha "
                    "TEXT NOT NULL DEFAULT ''"
                )
            except sqlite3.OperationalError:
                pass
            for _col in ("source", "source_issue"):
                try:
                    conn.execute(
                        f"ALTER TABLE task_state ADD COLUMN {_col} "
                        "TEXT NOT NULL DEFAULT ''"
                    )
                except sqlite3.OperationalError:
                    pass

    def all_states(self) -> dict[str, TaskState]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM task_state").fetchall()
        return {row["task_id"]: TaskState.from_row(row) for row in rows}

    # --- in-flight status / duplicate execution guards ----------------------

    def active_states(self) -> dict[str, TaskState]:
        out = {}
        for tid, st in self.all_states().items():
            if is_active_state(st):
                out[tid] = st
        return out

    def active_tasks(self) -> list[TaskState]:
        return list(self.active_states().values())

    def in_flight_states(self) -> dict[str, TaskState]:
        return self.active_states()

    def in_flight_tasks(self) -> list[TaskState]:
        return self.active_tasks()

    def claimed_or_running_states(self) -> dict[str, TaskState]:
        return self.active_states()

    def find_active_task(self, task_id: str | None = None,
                         source: str | None = None,
                         source_issue: str | None = None) -> TaskState | None:
        src = source or source_issue
        for st in self.active_states().values():
            if task_id is not None and st.task_id == task_id:
                return st
            if src and (st.source == src or st.source_issue == src):
                return st
        # If task_id provided but no exact active, and source provided, above covers.
        if task_id is None and src is None:
            # Return first active if any.
            vals = list(self.active_states().values())
            return vals[0] if vals else None
        return None

    def find_active_duplicate(self, task_id: str,
                              source: str | None = None,
                              source_issue: str | None = None) -> TaskState | None:
        src = source or source_issue
        for st in self.active_states().values():
            if st.task_id == task_id:
                return st
            if src and (st.source == src or st.source_issue == src):
                return st
        return None

    def is_duplicate_active(self, task_id: str,
                            source: str | None = None,
                            source_issue: str | None = None) -> bool:
        return self.find_active_duplicate(task_id, source=source,
                                         source_issue=source_issue) is not None

    def duplicate_active(self, task_id: str,
                         source: str | None = None,
                         source_issue: str | None = None) -> TaskState | None:
        return self.find_active_duplicate(task_id, source=source,
                                         source_issue=source_issue)

    def inflight_status(self, *, inbox: Any = None, task: Any = None,
                        task_id: str | None = None,
                        stale_after_seconds: int = 0,
                        now: float | None = None) -> dict[str, Any]:
        if now is None:
            now = _now()
        # Single-task view.
        if task_id is not None:
            st = self.get(task_id)
            t = None
            if inbox is not None:
                try:
                    t = inbox.by_id().get(task_id)
                except Exception:
                    t = None
            if task is not None:
                t = task
            payload = inflight_status_dict(st, task=t,
                                          stale_after_seconds=stale_after_seconds,
                                          now=now)
            return payload
        # All tasks view.
        states = self.all_states()
        tasks_list = []
        inbox_tasks = []
        if inbox is not None:
            try:
                inbox_tasks = list(inbox.tasks)
            except Exception:
                inbox_tasks = []
        by_id = {}
        if inbox is not None:
            try:
                by_id = inbox.by_id()
            except Exception:
                by_id = {}
        # Include all known inbox tasks plus any state-only rows.
        seen = set()
        for t in inbox_tasks:
            seen.add(t.id)
            st = states.get(t.id)
            tasks_list.append(inflight_status_dict(st, task=t,
                                                  stale_after_seconds=stale_after_seconds,
                                                  now=now))
        for tid, st in states.items():
            if tid in seen:
                continue
            t = by_id.get(tid)
            tasks_list.append(inflight_status_dict(st, task=t,
                                                  stale_after_seconds=stale_after_seconds,
                                                  now=now))
        active = [p for p in tasks_list if p.get("active")]
        return {
            "paused": False,
            "idle": len(active) == 0,
            "active_count": len(active),
            "active": active,
            "active_tasks": active,
            "tasks": tasks_list,
            "in_flight": tasks_list,
        }

    # Aliases for hidden tests / callers.
    def in_flight_status(self, **kwargs):
        return self.inflight_status(**kwargs)

    def status_snapshot(self, **kwargs):
        return self.inflight_status(**kwargs)

    def inflight_snapshot(self, **kwargs):
        return self.inflight_status(**kwargs)

    def serialize_state(self, state, task=None, stale_after_seconds=0, now=None):
        return inflight_status_dict(state, task=task,
                                   stale_after_seconds=stale_after_seconds,
                                   now=now)

    def serialize_task_state(self, *args, **kwargs):
        return self.serialize_state(*args, **kwargs)

    def task_status(self, *args, **kwargs):
        return self.serialize_state(*args, **kwargs)

    def heartbeat_state(self, state, stale_after_seconds=0, now=None):
        return heartbeat_freshness(state, stale_after_seconds=stale_after_seconds, now=now)

    def get(self, task_id: str) -> TaskState | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM task_state WHERE task_id = ?", (task_id,)
            ).fetchone()
        return TaskState.from_row(row) if row else None

    def upsert(self, state: TaskState) -> None:
        row = state.to_row()
        cols = list(row)
        placeholders = ", ".join("?" for _ in cols)
        assignments = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "task_id")
        sql = (
            f"INSERT INTO task_state ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT(task_id) DO UPDATE SET {assignments}"
        )
        with self._conn() as conn:
            conn.execute(sql, tuple(row[c] for c in cols))

    def set_skip_reason(self, task_id: str, reason: str) -> None:
        """Record why a task was not claimable at the last eligibility check.

        Called only when the reason differs from the stored one, so a
        permanently-blocked task stops repeating the same line every tick
        (Issue #23). An empty reason clears it (the task became claimable).
        """
        with self._conn() as conn:
            conn.execute(
                "UPDATE task_state SET last_skip_reason = ? WHERE task_id = ?",
                (reason, task_id),
            )

    # --- conflict-aware scheduling (v4) ---------------------------------------

    def set_baseline(self, task_id: str, sha: str) -> None:
        """Persist the branch head a task's manifest was evaluated against."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE task_state SET baseline_sha = ?, updated_at = ? "
                "WHERE task_id = ?",
                (sha, _now(), task_id),
            )

    def record_scheduling_decision(self, rec: dict[str, Any]) -> int:
        """Append one admission decision; idempotent on unchanged inputs.

        Returns 1 when a row was inserted, 0 when an identical decision
        (same task + manifest + comparison set + decision + reeval) already
        exists — a duplicate tick with no state change appends nothing.
        """
        cols = ("ts", "run_id", "task_id", "decision", "predecessor", "resource",
                "lock_owner", "compared_task_ids", "baseline_sha", "manifest_sha",
                "resource_keys", "conflict_paths", "reasons", "wait_condition",
                "reeval")
        with self._conn() as conn:
            cur = conn.execute(
                f"INSERT OR IGNORE INTO scheduling_decisions "
                f"({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                tuple(rec.get(c, "") for c in cols),
            )
            return cur.rowcount

    def latest_scheduling_decision(self, task_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM scheduling_decisions WHERE task_id = ? "
                "ORDER BY id DESC LIMIT 1", (task_id,)
            ).fetchone()
        return dict(row) if row else None

    def acquire_resource_lock(self, task_id: str, resource: str,
                              lease_seconds: int) -> tuple[bool, str]:
        """Idempotent single-writer lock on a shared mutable resource.

        Returns (granted, current_owner). A stale lock is reclaimed only
        after BOTH conditions hold: the owner task is terminal in the task
        ledger AND the lock's own lease is expired or its owner process is
        dead. A non-terminal owner (e.g. a crashed-but-unreaped worker)
        keeps the lock: the dispatcher's own lease recovery decides when the
        owner becomes terminal, and only then may the lock move.
        """
        pid, started = lease_fingerprint()
        now = _now()
        expiry = now + max(lease_seconds, 60)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM resource_locks WHERE resource = ?", (resource,)
            ).fetchone()
            if row is None or row["state"] != "HELD":
                conn.execute(
                    """INSERT INTO resource_locks
                       (resource, owner_task, state, lease_pid, lease_start,
                        lease_expiry, updated_at, note)
                       VALUES (?, ?, 'HELD', ?, ?, ?, ?, 'acquired')
                       ON CONFLICT(resource) DO UPDATE SET
                         owner_task = excluded.owner_task,
                         state = 'HELD',
                         lease_pid = excluded.lease_pid,
                         lease_start = excluded.lease_start,
                         lease_expiry = excluded.lease_expiry,
                         updated_at = excluded.updated_at,
                         note = 'acquired'""",
                    (resource, task_id, pid, started, expiry, now),
                )
                return True, task_id
            owner = row["owner_task"]
            if owner == task_id:
                # Idempotent re-acquire: refresh the lease, keep ownership.
                conn.execute(
                    "UPDATE resource_locks SET lease_pid = ?, lease_start = ?, "
                    "lease_expiry = ?, updated_at = ? WHERE resource = ?",
                    (pid, started, expiry, now, resource),
                )
                return True, task_id
            owner_state = conn.execute(
                "SELECT status FROM task_state WHERE task_id = ?", (owner,)
            ).fetchone()
            owner_terminal = bool(owner_state) and owner_state["status"] in TERMINAL
            lease_expired = now > (row["lease_expiry"] or 0)
            owner_alive = pid_alive(row["lease_pid"] or 0, row["lease_start"])
            if owner_terminal and (lease_expired or not owner_alive):
                conn.execute(
                    """INSERT INTO resource_locks
                       (resource, owner_task, state, lease_pid, lease_start,
                        lease_expiry, updated_at, note)
                       VALUES (?, ?, 'HELD', ?, ?, ?, ?, ?)
                       ON CONFLICT(resource) DO UPDATE SET
                         owner_task = excluded.owner_task,
                         state = 'HELD',
                         lease_pid = excluded.lease_pid,
                         lease_start = excluded.lease_start,
                         lease_expiry = excluded.lease_expiry,
                         updated_at = excluded.updated_at,
                         note = excluded.note""",
                    (resource, task_id, pid, started, expiry, now,
                     f"stale lock reclaimed (owner {owner} terminal, "
                     f"lease {'expired' if lease_expired else 'dead'})"),
                )
                return True, task_id
            return False, owner

    def release_resource_locks(self, task_id: str) -> int:
        """Release every lock owned by a task. Idempotent no-op if none."""
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE resource_locks SET state = 'RELEASED', owner_task = '', "
                "lease_pid = NULL, lease_start = NULL, lease_expiry = NULL, "
                "updated_at = ?, note = 'released (task terminal)' "
                "WHERE owner_task = ? AND state = 'HELD'",
                (_now(), task_id),
            )
            return cur.rowcount

    # --- leases -------------------------------------------------------------

    def claim(self, task_id: str, *, lease_seconds: int,
              source: str | None = None) -> TaskState | None:
        """Atomically claim a READY task.

        Returns the claimed state, or None when another tick already owns it
        (including an active run with the same task id or source issue). The
        WHERE clauses make this safe across concurrent ticks.
        """
        pid, started = lease_fingerprint()
        now = _now()
        expiry = now + max(lease_seconds, 1)
        run_id = uuid.uuid4().hex
        with self._conn() as conn:
            # Resolve provenance from the row when the caller did not pass it.
            row0 = conn.execute(
                "SELECT source, source_issue FROM task_state WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if source is None and row0 is not None:
                source = row0["source"] or row0["source_issue"] or ""
            src = (source or "").strip()
            # Duplicate guard: an active CLAIMED/RUNNING row with the same
            # task id or (non-empty) source issue blocks this claim.
            if src:
                dup = conn.execute(
                    "SELECT task_id FROM task_state "
                    "WHERE status IN ('CLAIMED','RUNNING') "
                    "AND (task_id = ? OR source = ? OR source_issue = ?)",
                    (task_id, src, src),
                ).fetchone()
                if dup is not None and dup["task_id"] != task_id:
                    return None
            # Persist provenance on the row being claimed (idempotent).
            if src:
                conn.execute(
                    "UPDATE task_state SET source = COALESCE(NULLIF(source,''),?), "
                    "source_issue = COALESCE(NULLIF(source_issue,''),?) "
                    "WHERE task_id = ?",
                    (src, src, task_id),
                )
            cur = conn.execute(
                """
                UPDATE task_state
                SET status = 'RUNNING',
                    attempts = attempts + 1,
                    run_id = ?,
                    lease_pid = ?,
                    lease_start = ?,
                    lease_expiry = ?,
                    last_error = '',
                    updated_at = ?
                WHERE task_id = ?
                  AND status = 'READY'
                  AND NOT EXISTS (
                        SELECT 1 FROM task_state AS o
                        WHERE o.task_id <> task_state.task_id
                          AND o.status IN ('CLAIMED','RUNNING')
                          AND (? <> '' AND (o.source = ? OR o.source_issue = ?))
                  )
                """,
                (run_id, pid, started, expiry, now, task_id, src, src, src),
            )
            if cur.rowcount != 1:
                # Could be because already active same task; check.
                return None
            row = conn.execute(
                "SELECT * FROM task_state WHERE task_id = ?", (task_id,)
            ).fetchone()
            return TaskState.from_row(row) if row else None

    def release(self, task_id: str, *, status: str, summary: str = "",
                outcome: dict[str, Any] | None = None, session_id: str = "",
                workspace_path: str = "", commit_sha: str = "",
                error: str = "") -> TaskState:
        state = self.get(task_id)
        if state is None:
            raise KeyError(task_id)
        state.status = normalize_status(status)
        state.outcome_summary = summary
        state.outcome_json = json.dumps(outcome, ensure_ascii=False) if outcome else ""
        state.session_id = session_id or state.session_id
        state.workspace_path = workspace_path or state.workspace_path
        state.commit_sha = commit_sha or state.commit_sha
        state.last_error = error
        state.lease_pid = None
        state.lease_start = None
        state.lease_expiry = None
        state.updated_at = _now()
        self.upsert(state)
        return state

    # --- stale lease recovery ------------------------------------------------

    def stale_leases(self, *, stale_after_seconds: int) -> list[TaskState]:
        """RUNNING tasks whose lease owner is dead or expired.

        Rows without a lease fingerprint (owner hand-wrote status = "RUNNING"
        in the inbox; never claimed by the dispatcher) are owner holds, not
        crashed workers, and are never recovered automatically.
        """
        now = _now()
        out: list[TaskState] = []
        for state in self.all_states().values():
            if state.status != "RUNNING":
                continue
            if state.lease_pid is None and state.lease_start is None:
                continue
            expiry = state.lease_expiry or 0
            expired = now > (expiry + max(stale_after_seconds, 0))
            pid = state.lease_pid or 0
            alive = pid_alive(pid, state.lease_start)
            if expired or not alive:
                out.append(state)
        return out

    def reap_stale(self, *, stale_after_seconds: int, max_retries: int) -> list[tuple[str, str]]:
        """Recover stale RUNNING tasks without duplicating side effects.

        A task whose worker died *before* spooling an outcome is retried while
        attempts remain; otherwise it moves to FAILED with the reason recorded.
        Returns (task_id, action) pairs.
        """
        actions: list[tuple[str, str]] = []
        for state in self.stale_leases(stale_after_seconds=stale_after_seconds):
            reason = (
                "lease expired"
                if (_now() > (state.lease_expiry or 0) + max(stale_after_seconds, 0))
                else "lease owner process is not alive"
            )
            if state.attempts < max_retries:
                self.reset_to_ready(state.task_id, note=reason)
                actions.append((state.task_id, f"reset_to_ready ({reason})"))
            else:
                self.release(
                    state.task_id,
                    status="FAILED",
                    summary=f"stale lease not recovered: {reason}",
                    error=reason,
                )
                actions.append((state.task_id, f"failed ({reason})"))
        return actions

    def reset_to_ready(self, task_id: str, *, note: str = "") -> None:
        state = self.get(task_id)
        if state is None:
            raise KeyError(task_id)
        state.status = "READY"
        state.lease_pid = None
        state.lease_start = None
        state.lease_expiry = None
        state.last_error = note
        state.updated_at = _now()
        self.upsert(state)

    def block(self, task_id: str, *, reason: str) -> None:
        state = self.get(task_id)
        if state is None:
            raise KeyError(task_id)
        state.status = "BLOCKED"
        state.last_error = reason
        state.lease_pid = None
        state.lease_start = None
        state.lease_expiry = None
        state.updated_at = _now()
        self.upsert(state)

    def cancel(self, task_id: str, *, reason: str = "cancelled by operator") -> None:
        state = self.get(task_id)
        if state is None:
            raise KeyError(task_id)
        state.status = "CANCELLED"
        state.last_error = reason
        state.lease_pid = None
        state.lease_start = None
        state.lease_expiry = None
        state.updated_at = _now()
        self.upsert(state)


@dataclass
class OutcomeRecord:
    task_id: str
    status: str
    summary: str
    evidence: list[dict[str, Any]] = field(default_factory=list)
    commit_sha: str = ""
    session_id: str = ""
    workspace_path: str = ""
    blocker: str = ""
    owner_action: str = ""
    error: str = ""
    # Provenance of the source task (registry Task.source): links the
    # automated run back to its GitHub issue / spawning task. "" = owner
    # hand-edited or legacy. Absent in pre-2026-10-24 spool files.
    source: str = ""
    recorded_at: float = _now()

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "summary": self.summary,
            "evidence": self.evidence,
            "commit_sha": self.commit_sha,
            "session_id": self.session_id,
            "workspace_path": self.workspace_path,
            "blocker": self.blocker,
            "owner_action": self.owner_action,
            "error":  self.error,
            "source": self.source,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "OutcomeRecord":
        return cls(
            task_id=d["task_id"],
            status=d["status"],
            summary=d.get("summary", ""),
            evidence=d.get("evidence", []),
            commit_sha=d.get("commit_sha", ""),
            session_id=d.get("session_id", ""),
            workspace_path=d.get("workspace_path", ""),
            blocker=d.get("blocker", ""),
            owner_action=d.get("owner_action", ""),
            error=d.get("error", ""),
            source=d.get("source", ""),
            recorded_at=d.get("recorded_at", _now()),
        )

    def write(self, outcomes_dir: Path) -> Path:
        """Spool an outcome atomically for the tick to ingest."""
        outcomes_dir.mkdir(parents=True, exist_ok=True)
        final = outcomes_dir / f"{self.task_id}.json"
        tmp = final.with_suffix(final.suffix + ".tmp")
        tmp.write_text(self.to_json(), encoding="utf-8")
        os.replace(tmp, final)
        return final

    @classmethod
    def read(cls, path: Path) -> "OutcomeRecord":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
