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


def _now() -> float:
    return time.time()


def process_start_time(pid: int) -> float | None:
    """Best-effort start time of a live process, else None.

    Windows: use the process creation time via ctypes when available; POSIX:
    read /proc/<pid> mtime. A None result means we cannot prove liveness, so
    the caller treats the lease as stale (fail-closed recovery).
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
        import os as _os

        st = _os.stat(f"/proc/{pid}")
        return st.st_mtime
    except Exception:
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
    return abs(actual - start_time) >= 0 and abs(actual - start_time) < 10.0


def lease_fingerprint() -> tuple[int, float]:
    """(pid, process_start_time) for this process."""
    pid = os.getpid()
    return pid, process_start_time(pid) or 0.0


def normalize_status(status: str) -> str:
    status = (status or "").strip().upper()
    return status


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
    workspace_path: str = ""
    commit_sha: str = ""
    updated_at: float = _now()

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
            "workspace_path": self.workspace_path,
            "commit_sha": self.commit_sha,
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
            workspace_path=row["workspace_path"] or "",
            commit_sha=row["commit_sha"] or "",
            updated_at=row["updated_at"],
        )


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
    workspace_path  TEXT NOT NULL DEFAULT '',
    commit_sha      TEXT NOT NULL DEFAULT '',
    updated_at      REAL NOT NULL
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

    def all_states(self) -> dict[str, TaskState]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM task_state").fetchall()
        return {row["task_id"]: TaskState.from_row(row) for row in rows}

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

    # --- leases -------------------------------------------------------------

    def claim(self, task_id: str, *, lease_seconds: int) -> TaskState | None:
        """Atomically claim a READY task.

        Returns the claimed state, or None when another tick already owns it.
        The WHERE clauses make this safe across concurrent ticks: the row only
        moves out of READY under this transaction.
        """
        pid, started = lease_fingerprint()
        now = _now()
        expiry = now + max(lease_seconds, 1)
        with self._conn() as conn:
            cur = conn.execute(
                """
                UPDATE task_state
                SET status = 'RUNNING',
                    attempts = attempts + 1,
                    lease_pid = ?,
                    lease_start = ?,
                    lease_expiry = ?,
                    last_error = '',
                    updated_at = ?
                WHERE task_id = ?
                  AND status = 'READY'
                """,
                (pid, started, expiry, now, task_id),
            )
            if cur.rowcount != 1:
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
        """RUNNING tasks whose lease owner is dead or expired."""
        now = _now()
        out: list[TaskState] = []
        for state in self.all_states().values():
            if state.status != "RUNNING":
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
