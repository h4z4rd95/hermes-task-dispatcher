"""Dispatcher core: scan → recover → claim → route → ingest → report.

One ``Dispatcher.tick()`` pass is the entire unit of scheduled work. It is
idempotent: running it twice with no state change does nothing new. The tick is
the only writer to the state store; workers write outcome JSON files that the
tick ingests.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import Config
from .registry import Inbox, Task, load_inbox
from .state import OutcomeRecord, StateStore

__all__ = ["Dispatcher", "TickReport"]


@dataclass
class TickReport:
    inbox_tasks: int = 0
    recovered: list[tuple[str, str]] = field(default_factory=list)
    claimed: list[str] = field(default_factory=list)
    completed: list[str] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    commit_sha: str = ""
    pushed: bool = False

    def summary(self) -> str:
        parts = [
            f"inbox={self.inbox_tasks}",
            f"claimed={len(self.claimed)}",
            f"completed={len(self.completed)}",
            f"recovered={len(self.recovered)}",
            f"blocked={len(self.blocked)}",
        ]
        if self.commit_sha:
            parts.append(f"commit={self.commit_sha[:12]}")
        if self.pushed:
            parts.append("pushed")
        return " ".join(parts)


class Dispatcher:
    """One tick of the scheduled dispatcher."""

    def __init__(
        self,
        cfg: Config,
        *,
        store: StateStore | None = None,
        router: Any = None,
        workspace: Any = None,
        control: Any = None,
        logger: Any = None,
    ) -> None:
        self.cfg = cfg
        self.log = logger or _default_logger()
        self.store = store or StateStore(cfg.state_db)
        self._router = router
        self._workspace = workspace
        self._control = control

    # --- lazily constructed lane B/C collaborators --------------------------

    @property
    def control(self):
        if self._control is None:
            from .github import ControlRepo

            self._control = ControlRepo(self.cfg.control_repo, logger=self.log)
        return self._control

    @property
    def router(self):
        if self._router is None:
            from .router import SessionRouter

            self._router = SessionRouter(logger=self.log)
        return self._router

    @property
    def workspace(self):
        if self._workspace is None:
            from .workspace import WorkspaceManager

            self._workspace = WorkspaceManager(
                self.cfg.workspace_pool,
                global_quota_mb=self.cfg.global_workspace_quota_mb,
                github_token=self.cfg.github_token(),
                logger=self.log,
            )
        return self._workspace

    # --- kill switch ---------------------------------------------------------

    def _paused(self) -> bool:
        return self.cfg.pause_file.exists()

    # --- eligibility ---------------------------------------------------------

    def _eligible(self, task: Task, inbox: Inbox, states: dict) -> bool:
        state = states.get(task.id)
        if task.requires_approval:
            self.log("debug", f"{task.id}: requires owner approval — not claimable yet")
            return False
        if task.is_terminal:
            return False
        if state is None:
            return True  # a new READY task with no state row yet
        if state.status != "READY":
            return False
        # Dependencies: every dependency must be DONE.
        index = inbox.by_id()
        for dep_id in task.dependencies:
            dep_task = index.get(dep_id)
            if dep_task is None:
                return False
            dep_state = states.get(dep_id)
            dep_status = dep_state.status if dep_state else dep_task.status
            if dep_status != "DONE":
                self.log("debug", f"{task.id}: dependency {dep_id} is {dep_status} — blocked")
                return False
        return True

    def _workstream_busy(self, states: dict, inbox: Inbox, task: Task) -> bool:
        """True when a task in the same workstream is already RUNNING."""
        for state in states.values():
            if state.status != "RUNNING":
                continue
            owner = _find_task(inbox, state.task_id)
            if owner is not None and owner.workstream == task.workstream:
                return True
        return False

    # --- the tick ------------------------------------------------------------

    def tick(self) -> TickReport:
        report = TickReport()
        if self._paused():
            self.log("info", "dispatcher paused (kill-switch file present); skipping tick")
            return report
        try:
            inner = self._tick(report)
            _merge_reports(report, inner)
        except Exception as exc:  # keep the scheduler healthy
            report.errors.append(f"tick failed: {exc}")
            self.log("error", f"tick failed: {exc}\n{traceback.format_exc()}")
        return report

    def _tick(self, report: TickReport) -> TickReport:
        # 1. Sync the control plane so the inbox is current.
        try:
            head = self.control.sync()
            self.log("debug", f"control repo at {head[:12]}")
        except Exception as exc:
            report.errors.append(f"control sync failed: {exc}")
            self.log("warn", f"control sync failed: {exc} — continuing with local inbox")

        inbox: Inbox
        try:
            inbox = load_inbox(self.cfg.inbox_path)
        except Exception as exc:
            report.errors.append(f"inbox parse failed: {exc}")
            self.log("error", f"inbox parse failed: {exc}")
            return report
        report.inbox_tasks = len(inbox.tasks)

        states = self.store.all_states()

        # 2. Ingest spooled worker outcomes first (they may free capacity).
        for task_id in self._ingest_outcomes(states, report):
            report.completed.append(task_id)
        states = self.store.all_states()

        # 2b. Seed state rows for tasks new to the store so an atomic claim
        # always has a row to update. A task terminal in the inbox is seeded
        # terminal and never claimed.
        self._sync_states(inbox, states)
        states = self.store.all_states()

        # 3. Recover stale leases safely (no duplicate side effects).
        for task in inbox.tasks:
            state = states.get(task.id)
            if state is not None and state.status == "RUNNING" and _is_stale(
                state, self.cfg.stale_lease_seconds
            ):
                action = self._recover(task, state)
                if action:
                    report.recovered.append((task.id, action))
        states = self.store.all_states()

        # 4. Claim eligible tasks by priority.
        candidates = sorted(
            (t for t in inbox.tasks if self._eligible(t, inbox, states)),
            key=lambda t: (-t.priority, t.id),
        )
        running = sum(1 for s in states.values() if s.status == "RUNNING")
        for task in candidates:
            if running >= self.cfg.max_concurrent:
                self.log("debug", "concurrency limit reached; remaining tasks wait")
                break
            if self._workstream_busy(states, inbox, task) and not task.parallel_safe:
                report.skipped.append(task.id)
                continue
            claimed = self.store.claim(task.id, lease_seconds=task.timeout_seconds)
            if claimed is None:
                self.log("debug", f"{task.id}: already claimed by another tick")
                continue
            report.claimed.append(task.id)
            running += 1
            try:
                self._run_task(task, claimed, report)
            except Exception as exc:
                self.log("error", f"{task.id}: run failed: {exc}\n{traceback.format_exc()}")
                self.store.release(
                    task.id, status="FAILED", error=f"{type(exc).__name__}: {exc}",
                    summary="task run raised unexpectedly",
                )
                report.errors.append(f"{task.id}: {exc}")
            states = self.store.all_states()

        # 5. Report finished tasks back to the control repo and push.
        try:
            sha = self._write_outcomes_to_control(inbox, report)
            if sha:
                report.commit_sha = sha
                self.control.push()
                report.pushed = True
        except Exception as exc:
            report.errors.append(f"outcome reporting failed: {exc}")
            self.log("error", f"outcome reporting failed: {exc}")
        return report

    # --- state seeding --------------------------------------------------------

    def _sync_states(self, inbox: Inbox, states: dict) -> int:
        """Create state rows for inbox tasks the store has not seen.

        Returns the number of rows created. A task whose inbox status is
        already terminal is seeded as terminal (and is therefore never
        claimed); anything else is seeded READY so the atomic claim has a row
        to update.
        """
        created = 0
        for task in inbox.tasks:
            if task.id in states:
                continue
            from .state import TaskState

            seed_status = task.status if task.is_terminal else "READY"
            self.store.upsert(TaskState(task_id=task.id, status=seed_status))
            created += 1
        return created

    # --- execution -----------------------------------------------------------

    def _run_task(self, task: Task, state, report: TickReport) -> None:
        self.log("info", f"{task.id}: claiming → RUNNING (attempt {state.attempts})")
        checkout_path: Path | None = None
        if task.needs_checkout:
            checkout = self.workspace.checkout(
                task_id=task.id,
                repo=task.repo,
                branch=task.branch,
                purpose=task.purpose or task.title,
                keep=task.keep_workspace,
                task_budget_mb=task.workspace_budget_mb,
            )
            checkout_path = checkout.path
            state.workspace_path = str(checkout_path)
            state.session_id = state.session_id or ""
            self.store.upsert(state)

        try:
            prompt = task.resolved_prompt(self.cfg.control_repo)
        except Exception as exc:
            self.store.release(task.id, status="FAILED", error=str(exc),
                               summary=f"prompt resolution failed: {exc}")
            report.errors.append(f"{task.id}: {exc}")
            return

        session_name = task.session or f"dispatcher-{task.id}"
        run = self.router.run(
            session_name=session_name,
            prompt=prompt,
            model=task.model or None,
            run_budget_s=task.timeout_seconds or None,
        )
        state.session_id = run.session_id
        self.store.upsert(state)
        if checkout_path is not None:
            try:
                self.workspace.bind_session(checkout_path, run.session_id)
            except Exception as exc:
                self.log("warn", f"{task.id}: workspace bind failed: {exc}")

        status = "DONE" if run.exit_code == 0 else "FAILED"
        summary = _truncate(run.stdout.strip(), 800)
        evidence = [
            {"kind": "session", "session_id": run.session_id, "resumed": run.resumed},
            {"kind": "run", "exit_code": run.exit_code, "duration_s": round(run.duration_s, 1)},
        ]
        if checkout_path is not None:
            evidence.append({"kind": "workspace", "path": str(checkout_path)})
        commit_sha = self._maybe_task_commit(task)
        self.store.release(
            task.id,
            status=status,
            summary=summary,
            outcome={"evidence": evidence, "commit_sha": commit_sha},
            session_id=run.session_id,
            workspace_path=str(checkout_path) if checkout_path else "",
            commit_sha=commit_sha,
            error="" if status == "DONE" else f"agent exited {run.exit_code}",
        )
        # Spool the outcome file too, for durability.
        OutcomeRecord(
            task_id=task.id,
            status=status,
            summary=summary,
            evidence=evidence,
            commit_sha=commit_sha,
            session_id=run.session_id,
            workspace_path=str(checkout_path) if checkout_path else "",
        ).write(self.cfg.outcomes_dir)
        if checkout_path is not None and not task.keep_workspace:
            try:
                self.workspace.release(checkout_path)
            except Exception as exc:
                self.log("warn", f"{task.id}: workspace release failed: {exc}")
        if status == "DONE":
            report.completed.append(task.id)
        else:
            report.errors.append(f"{task.id}: agent exited {run.exit_code}")

    def _maybe_task_commit(self, task: Task) -> str:
        """If the task edited its own repo (checked out into the workspace),
        commit + push that work. Returns the commit SHA, or '' when nothing
        changed."""
        state = self.store.get(task.id)
        path = Path(state.workspace_path) if state and state.workspace_path else None
        if path is None or not path.is_dir():
            return ""
        try:
            sub = _Git(path)
            if not sub.has_changes():
                return ""
            sha = sub.commit_all(f"chore(dispatcher): task {task.id} — {task.title}")
            sub.push()
            return sha
        except Exception as exc:
            self.log("warn", f"{task.id}: commit/push failed: {exc}")
            return ""

    # --- outcome spool ingestion --------------------------------------------

    def _ingest_outcomes(self, states: dict, report: TickReport) -> list[str]:
        out_dir = self.cfg.outcomes_dir
        if not out_dir.is_dir():
            return []
        done: list[str] = []
        for path in sorted(out_dir.glob("*.json")):
            try:
                rec = OutcomeRecord.read(path)
            except Exception as exc:
                self.log("warn", f"could not read outcome {path.name}: {exc}")
                continue
            state = self.store.get(rec.task_id)
            if state is None:
                self.log("debug", f"outcome for unknown task {rec.task_id}; dropping")
                path.unlink(missing_ok=True)
                continue
            if state.status in ("DONE", "FAILED", "CANCELLED"):
                # already terminal — idempotent drop
                path.unlink(missing_ok=True)
                continue
            self.store.release(
                rec.task_id,
                status=rec.status,
                summary=rec.summary,
                outcome={"evidence": rec.evidence, "commit_sha": rec.commit_sha},
                session_id=rec.session_id,
                workspace_path=rec.workspace_path,
                commit_sha=rec.commit_sha,
                error=rec.error,
            )
            done.append(rec.task_id)
            path.unlink(missing_ok=True)
        return done

    def _recover(self, task: Task, state) -> str:
        actions = self.store.reap_stale(
            stale_after_seconds=self.cfg.stale_lease_seconds, max_retries=task.max_retries
        )
        for task_id, action in actions:
            if task_id == task.id:
                return action
        return ""

    # --- control repo reporting ---------------------------------------------

    def _write_outcomes_to_control(self, inbox: Inbox, report: TickReport) -> str | None:
        """Update terminal task statuses in the inbox TOML + append the JSONL ledger."""
        states = self.store.all_states()
        changed = False
        for task in inbox.tasks:
            state = states.get(task.id)
            if state is None or state.status == task.status:
                continue
            if state.status in ("DONE", "FAILED", "CANCELLED"):
                task.status = state.status
                changed = True
        if not changed:
            return None
        try:
            return self.control.write_outcome_from_inbox(inbox, report)
        except AttributeError:
            # Older ControlRepo shape: fall back to a plain commit.
            return self.control.commit_all("docs(dispatcher): record task outcomes")


# --- helpers ----------------------------------------------------------------


def _merge_reports(target: TickReport, other: TickReport) -> None:
    """Fold the inner tick report into the outer one (dedup by task id)."""
    for key in ("claimed", "completed", "blocked", "skipped"):
        for value in getattr(other, key):
            if value not in getattr(target, key):
                getattr(target, key).append(value)
    for key in ("recovered", "errors"):
        for value in getattr(other, key):
            if value not in getattr(target, key):
                getattr(target, key).append(value)
    target.inbox_tasks = max(target.inbox_tasks, other.inbox_tasks)
    target.commit_sha = target.commit_sha or other.commit_sha
    target.pushed = target.pushed or other.pushed


def _find_task(inbox: Inbox, task_id: str) -> Task | None:
    return inbox.by_id().get(task_id)


def _is_stale(state, stale_after_seconds: int) -> bool:
    from .state import pid_alive as _alive

    now = time.time()
    expired = now > (state.lease_expiry or 0) + max(stale_after_seconds, 0)
    alive = _alive(state.lease_pid or 0, state.lease_start)
    return expired or not alive


def _truncate(text: str, limit: int) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


class _Git:
    """Thin git wrapper for a task workspace (never the control repo)."""

    def __has_module(name: str):  # pragma: no cover
        return True

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git", "-C", str(self.path), *args],
            capture_output=True,
            text=True,
            check=False,
        )

    def has_changes(self) -> bool:
        r = self._run("status", "--porcelain")
        return bool(r.stdout.strip())

    def commit_all(self, message: str) -> str:
        self._run("add", "-A")
        self._run("commit", "-m", message)
        r = self._run("rev-parse", "HEAD")
        return r.stdout.strip()

    def push(self) -> None:
        self._run("push", "origin", "HEAD")


def _default_logger():
    def log(level: str, message: str) -> None:
        stream = sys.stderr if level in ("error", "warn") else sys.stdout
        print(f"[dispatcher:{level}] {message}", file=stream)

    return log
