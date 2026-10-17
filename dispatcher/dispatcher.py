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
from .verification_gate import VerificationGate, build_probe

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
        # Wall-clock deadline for this tick process. `None` only when a caller
        # constructs a Dispatcher outside a tick (tests); tick() always sets it.
        self.tick_deadline: float | None = None

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

            # WorkspaceManager expects a logging.Logger (it calls
            # logger.debug/.warning/.info). The dispatcher's self.log is the
            # two-argument functional logger; bridge it so a checkout never
            # crashes the task with "function object has no attribute debug".
            import logging as _logging

            class _WorkspaceLogBridge(_logging.Logger):
                def __init__(self, sink):
                    super().__init__("dispatcher.workspace.bridge")
                    self._sink = sink

                def _emit(self, level, msg, *args):
                    try:
                        if args:
                            msg = msg % args
                    except Exception:
                        pass
                    self._sink(level, str(msg))

                def debug(self, msg, *args, **kw):
                    self._emit("debug", msg, *args)

                def info(self, msg, *args, **kw):
                    self._emit("info", msg, *args)

                def warning(self, msg, *args, **kw):
                    self._emit("warn", msg, *args)

                def warn(self, msg, *args, **kw):
                    self._emit("warn", msg, *args)

                def error(self, msg, *args, **kw):
                    self._emit("error", msg, *args)

                def exception(self, msg, *args, **kw):
                    self._emit("error", msg, *args)

            self._workspace = WorkspaceManager(
                self.cfg.workspace_pool,
                global_quota_mb=self.cfg.global_workspace_quota_mb,
                github_token=self.cfg.github_token(),
                logger=_WorkspaceLogBridge(self.log),
            )
        return self._workspace

    # --- kill switch ---------------------------------------------------------

    def _paused(self) -> bool:
        return self.cfg.pause_file.exists()

    # --- eligibility ---------------------------------------------------------

    def _skip(self, task: Task, state, reason: str) -> None:
        """Log a non-claimable reason once per change, not once per tick.

        A task that stays PENDING (or otherwise blocked) would otherwise emit
        the same debug line on every 5-minute tick forever (Issue #23). The
        reason is persisted on the state row; when it changes the new reason is
        logged, otherwise the tick stays quiet. Actionable transitions
        (claim, recovery, terminal result) are logged elsewhere and are
        unaffected.
        """
        previous = state.last_skip_reason if state is not None else None
        if previous == reason:
            return
        self.log("debug", f"{task.id}: {reason}")
        try:
            if state is not None:
                self.store.set_skip_reason(task.id, reason)
            else:
                # No state row yet: seed one so the reason can be persisted.
                from .state import TaskState

                seeded = TaskState(task_id=task.id, status="READY",
                                   last_skip_reason=reason)
                self.store.upsert(seeded)
        except Exception as exc:  # logging must never break eligibility
            self.log("warn", f"{task.id}: could not persist skip reason: {exc}")

    def _eligible(self, task: Task, inbox: Inbox, states: dict) -> bool:
        state = states.get(task.id)
        # PENDING is a valid inbox state but is deliberately not runnable.
        # A task becomes dispatchable only when the owner promotes it to READY.
        if task.status == "PENDING":
            self._skip(task, state, "status PENDING — not claimable yet")
            return False
        if task.requires_approval:
            self._skip(task, state, "requires owner approval — not claimable yet")
            return False
        if task.is_terminal:
            return False
        if state is None:
            return True  # a new READY task with no state row yet
        if state.status != "READY":
            self._skip(task, state, f"state {state.status} — not claimable")
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

    # --- bounded control-repo sync ------------------------------------------

    def _sync_with_timeout(self) -> str:
        """``ControlRepo.sync()`` wrapped in a hard wall-clock timeout.

        ``git fetch`` / ``git merge`` are blocking subprocess calls with no
        internal deadline; a hung network read in either one held a tick for
        the full 3600-second scheduler timeout. This bounds the whole sync to a
        fraction of the remaining tick budget so sync-before-inbox semantics
        are preserved while the tick can still make progress.
        """
        import concurrent.futures

        remaining = self.tick_deadline - time.time() if self.tick_deadline else 300.0
        if remaining <= 0:
            raise TimeoutError(
                "tick budget exhausted before control-repo sync — refusing to "
                "sync past the tick deadline"
            )
        # Cap at 300s, but never spend more than the remaining budget minus the
        # reserve the later phases (claim/report/push) still need. The floor is
        # 1s so a nearly-expired tick still bounds the sync instead of letting
        # it run free.
        budget = max(min(int(remaining) - 60, 300), 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.control.sync)
            try:
                return future.result(timeout=budget)
            except TimeoutError as exc:
                raise TimeoutError(
                    f"control-repo sync exceeded {budget}s (tick deadline "
                    f"{int(remaining)}s remaining)"
                ) from exc

    # --- the tick ------------------------------------------------------------

    def tick(self) -> TickReport:
        report = TickReport()
        # Arm the hard tick deadline before any work runs. Every blocking phase
        # consults self.tick_deadline, so a tick can never exceed this budget
        # regardless of how slow an individual task or git operation is.
        self.tick_deadline = time.time() + max(self.cfg.tick_budget_s, 1)
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
            head = self._sync_with_timeout()
            self.log("debug", f"control repo at {head[:12]}")
        except Exception as exc:
            # A failed sync means the local inbox may be stale. Dispatching on
            # stale state is the silent-failure mode the permanent dispatcher
            # must not have (T-2026-10-05-DISPATCHER-CRON-SYNC-001): fail the
            # tick loudly instead of continuing with a possibly outdated inbox.
            report.errors.append(f"control sync failed: {exc}")
            self.log(
                "error",
                f"control sync failed: {exc} — aborting tick (no dispatch on stale inbox)",
            )
            # A failed sync means the dispatcher is blind: no new tasks are
            # seen and no outcomes are published while it looks "ok" to the
            # scheduler. Surface it as a tick-level incident (same class as a
            # failed outcome publish) instead of silently ticking on stale state.
            report.sync_failed = True
            return report

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
            # Hard deadline: never start a task the tick cannot afford to wait
            # for. A task started past the deadline would outlive the tick and
            # the scheduler's own timeout. It stays READY and is picked up by
            # the next tick instead.
            if self.tick_deadline and time.time() >= self.tick_deadline:
                self.log("warn", "tick budget exhausted; deferring remaining candidates")
                break
            if self._workstream_busy(states, inbox, task) and not task.parallel_safe:
                report.skipped.append(task.id)
                continue
            claimed = self.store.claim(task.id, lease_seconds=task.timeout_seconds)
            if claimed is None:
                self.log("debug", f"{task.id}: already claimed by another tick")
                continue
            # Claiming is an actionable transition — clear any stored skip
            # reason so a later re-block is logged as a fresh event.
            try:
                self.store.set_skip_reason(task.id, "")
            except Exception:
                pass
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
                # Always push. Skipping the push to save tick budget is not
                # safe: the next tick's fetch + ff-only merge then fails on the
                # diverged history, which wedges every subsequent sync and
                # makes the inbox blind to all remote tasks (observed
                # T-2026-10-06). A push is a small network round trip and is
                # permitted to overrun the budget by a little; the script's
                # hard ceiling is the real backstop.
                self.control.push()
                report.pushed = True
        except Exception as exc:
            report.errors.append(f"outcome reporting failed: {exc}")
            self.log("error", f"outcome reporting failed: {exc}")
            # A failed outcome publish leaves records only locally and wedges
            # the next tick's ff-only sync — this is a tick-level incident,
            # not a task failure, so surface it to the scheduler (visible
            # failure instead of a silently stale control repo).
            report.push_failed = True
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
        run_budget_s = task.timeout_seconds or None
        if run_budget_s is not None:
            # Never let a single task's configured budget consume the whole
            # tick. The tick budget is the hard ceiling the scheduler relies on;
            # a task budget larger than that is clamped (with a log line) rather
            # than allowed to run past the tick's own deadline.
            remaining = self.tick_deadline - time.time() if self.tick_deadline else 0.0
            if remaining <= 0:
                self.log(
                    "warn",
                    f"{task.id}: tick budget exhausted before run; "
                    f"deferring task (run skipped)",
                )
                self.store.release(
                    task.id,
                    status="READY",
                    error="tick budget exhausted; task deferred",
                    summary="tick budget exhausted before run; deferred",
                )
                report.skipped.append(task.id)
                return
            if run_budget_s > int(remaining):
                self.log(
                    "warn",
                    f"{task.id}: clamping run budget {run_budget_s}s to "
                    f"remaining tick budget {int(remaining)}s",
                )
                run_budget_s = int(remaining)
        run = self.router.run(
            session_name=session_name,
            prompt=prompt,
            model=task.model or None,
            run_budget_s=run_budget_s,
        )
        state.session_id = run.session_id
        self.store.upsert(state)
        if checkout_path is not None:
            try:
                self.workspace.bind_session(checkout_path, run.session_id)
            except Exception as exc:
                self.log("warn", f"{task.id}: workspace bind failed: {exc}")

        # --- Automated verification gate (governance:
        # VERIFICATION_GATE.md / prompts/IMPLEMENT_AUTOMATED_VERIFICATION_GATE) ---
        # A zero agent-exit-code is no longer sufficient for DONE. The gate
        # runs a risk-tiered verification pass over the task workspace and
        # derives PASS/FAIL/PARTIAL/BLOCKED from recorded evidence. DONE is
        # only ever granted when the gate says PASS. A gate failure turns the
        # task into FAILED immediately (no retry loop) unless it is BLOCKED,
        # in which case the task is BLOCKED with a named reason so the owner
        # can supply the missing access. The gate is fail-closed: any error
        # inside the gate itself leaves the task FAILED, not DONE.
        gate_status = "PASS"
        gate_bundle_path = ""
        gate_summary = ""
        # Effective tier from the registry contract: declared tier clamped
        # UP to the policy floor. Never silently downgraded.
        gate_tier = task.effective_risk_tier
        checkout_root = checkout_path or self.cfg.home
        # Build the runtime probe from the task's allowlisted probe spec.
        # Malformed or unknown probes yield BLOCKED (fail closed), never DONE.
        probe = build_probe(
            spec=getattr(task, "runtime_probe", None),
            workspace=checkout_root,
            logger=self.log,
        )
        try:
            gate = VerificationGate(
                task_id=task.id,
                repo_path=checkout_root,
                risk_tier=gate_tier,
                owner="dispatcher",
                prompt_text=prompt,
                criteria=list(getattr(task, "acceptance_criteria", []) or []),
                cache_path=self.cfg.state_dir / "verification_cache.json",
                baseline_sha="",
                branch=task.branch,
                # Only tasks that actually check out a repository are gated on
                # repository-based checks. Prompt-only tasks (no repo) run with
                # require_repo=False so their checks degenerate to
                # not-applicable instead of blocking DONE.
                require_repo=task.needs_checkout,
            )
            gate_bundle = gate.run(
                probe=probe,
                test_runner=None,
                precondition_plan=str(getattr(task, "preconditions", "") or ""),
                rollback_plan=str(getattr(task, "rollback_plan", "") or ""),
            )
            gate_status = gate_bundle.status
            gate_bundle_path = (
                str(self.cfg.state_dir / f"verification-{task.id}.json")
            )
            gate_bundle.write(Path(gate_bundle_path))
            gate_summary = (
                f"gate={gate_status} tier={gate_tier} "
                f"checks={len(gate_bundle.checks)} "
                f"reused={gate_bundle.evidence_reused}"
            )
            if gate_bundle.blockers:
                gate_summary += " blockers=" + ";".join(gate_bundle.blockers)[:200]
        except Exception as exc:
            # Fail closed. The task is not proven; it must not become DONE.
            gate_status = "FAIL"
            gate_summary = f"gate error ({type(exc).__name__}: {exc})"[:300]
            self.log("error", f"{task.id}: verification gate crashed: {exc}")

        task_exit_ok = run.exit_code == 0
        if not task_exit_ok:
            status = "FAILED"
        elif gate_status == "PASS":
            status = "DONE"
        elif gate_status == "BLOCKED":
            status = "BLOCKED"
        else:  # FAIL or PARTIAL: never DONE without PASS
            status = "FAILED"

        summary = _truncate(run.stdout.strip(), 800)
        if gate_summary:
            summary = (summary + "\n[gate] " + gate_summary)[:1100]
        evidence = [
            {"kind": "session", "session_id": run.session_id, "resumed": run.resumed},
            {"kind": "run", "exit_code": run.exit_code, "duration_s": round(run.duration_s, 1)},
            {"kind": "gate", "status": gate_status, "tier": gate_tier,
             "bundle": gate_bundle_path},
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
            sha: str | None = None
            for task in inbox.tasks:
                state = states.get(task.id)
                if state is None:
                    continue
                if state.status not in ("DONE", "FAILED", "CANCELLED"):
                    continue
                # NOTE: the loop above already copied state.status onto
                # task.status, so do not re-compare the two here — that would
                # skip every task this method was asked to report.
                got = self.control.write_outcome(
                    task_id=task.id,
                    status=state.status,
                    summary=state.outcome_summary or "",
                    evidence=(),
                    commit_sha=state.commit_sha or "",
                )
                if got:
                    sha = got
            return sha
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
        r = self._run("push", "origin", "HEAD")
        if r.returncode != 0:
            # A rejected push (e.g. non-fast-forward from a competing writer)
            # must be visible: returning a SHA that lives only locally makes
            # the task look published when it is not.
            raise RuntimeError(
                f"git push origin HEAD rejected (rc={r.returncode}): "
                f"{(r.stderr or r.stdout).strip()[:300]}"
            )


def _default_logger():
    def log(level: str, message: str) -> None:
        stream = sys.stderr if level in ("error", "warn") else sys.stdout
        print(f"[dispatcher:{level}] {message}", file=stream)

    return log
