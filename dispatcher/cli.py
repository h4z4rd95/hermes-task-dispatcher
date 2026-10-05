"""Command line interface for the Hermes task dispatcher.

Usage:
  python -m dispatcher.cli tick                    # one scheduled pass (cron target)
  python -m dispatcher.cli worker <task_id>        # detached worker entrypoint
  python -m dispatcher.cli status                  # task state summary
  python -m dispatcher.cli pause / resume          # kill switch
  python -m dispatcher.cli register-self           # install the cron job
  python -m dispatcher.cli unregister-self         # remove the cron job
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from .config import config
from .dispatcher import Dispatcher, TickReport


def _log(level: str, message: str) -> None:
    stream = sys.stderr if level in ("error", "warn") else sys.stdout
    print(f"[dispatcher:{level}] {message}", file=stream, flush=True)


def _adapter(level_or_message, message=None) -> None:
    """Logger bridge.

    Lane modules (router/workspace/github) call ``logger(message)`` with one
    positional argument. The core dispatcher and the CLI call
    ``logger(level, message)``. Both shapes work here so a single logger can be
    threaded through every lane without each module having to agree on a
    signature.
    """
    if message is None:
        _log("info", str(level_or_message))
    else:
        _log(str(level_or_message), str(message))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dispatcher",
        description="Hermes GitHub task dispatcher + bounded workspace manager",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("tick", help="run one scheduled dispatcher pass")
    worker = sub.add_parser("worker", help="execute one task (detached worker)")
    worker.add_argument("task_id")

    sub.add_parser("status", help="show task state")
    sub.add_parser("pause", help="create the kill-switch file")
    sub.add_parser("resume", help="remove the kill-switch file")

    reg = sub.add_parser("register-self", help="register the cron job with `hermes cron`")
    reg.add_argument("--interval", default=None, help="poll interval, e.g. 5m (default: config)")
    reg.add_argument("--dry-run", action="store_true", help="print the command instead of running it")

    sub.add_parser("unregister-self", help="remove the cron job")

    args = parser.parse_args(argv)

    if args.command == "tick":
        return _cmd_tick()
    if args.command == "worker":
        return _cmd_worker(args.task_id)
    if args.command == "status":
        return _cmd_status()
    if args.command == "pause":
        return _cmd_pause()
    if args.command == "resume":
        return _cmd_resume()
    if args.command == "register-self":
        return _cmd_register_self(args)
    if args.command == "unregister-self":
        return _cmd_unregister_self()
    parser.error("unknown command")
    return 2


# --- tick -------------------------------------------------------------------


def _cmd_tick() -> int:
    cfg = config()
    dispatcher = Dispatcher(cfg, logger=_adapter)
    started = time.time()
    report = dispatcher.tick()
    duration = time.time() - started
    _log("info", f"tick complete in {duration:.1f}s — {report.summary()}")
    for task_id, action in report.recovered:
        _log("info", f"  recovered {task_id}: {action}")
    for task_id in report.claimed:
        _log("info", f"  claimed {task_id}")
    for task_id in report.completed:
        _log("info", f"  completed {task_id}")
    for err in report.errors:
        _log("warn", f"  error: {err}")
    # A failed task must not fail the scheduler; only tick-level failures do.
    # But a tick that overran its own budget is a tick-level failure: it means
    # the scheduler's timeout was the only thing that stopped us, which is
    # exactly the silent wedge we must surface as a real incident.
    if duration > cfg.tick_budget_s:
        _log("error", f"tick overran budget: {duration:.1f}s > {cfg.tick_budget_s}s")
        return 3
    return 0


# --- worker -----------------------------------------------------------------


def _cmd_worker(task_id: str) -> int:
    cfg = config()
    dispatcher = Dispatcher(cfg, logger=_adapter)
    from .registry import load_inbox
    from .state import OutcomeRecord

    inbox = load_inbox(cfg.inbox_path)
    task = inbox.by_id().get(task_id)
    if task is None:
        _log("error", f"task {task_id} not in inbox")
        return 3
    state = dispatcher.store.get(task_id)
    if state is None:
        _log("error", f"task {task_id} has no state (not claimed)")
        return 3
    try:
        dispatcher._run_task(task, state, TickReport())
        return 0
    except Exception as exc:
        _log("error", f"worker failed: {exc}")
        try:
            dispatcher.store.release(
                task_id, status="FAILED", error=f"{type(exc).__name__}: {exc}",
                summary="worker crashed",
            )
        except Exception:
            pass
        return 1


# --- status -----------------------------------------------------------------


def _cmd_status() -> int:
    cfg = config()
    from .registry import load_inbox
    from .state import StateStore

    try:
        inbox = load_inbox(cfg.inbox_path)
    except Exception as exc:
        _log("error", f"inbox: {exc}")
        return 3
    store = StateStore(cfg.state_db)
    states = store.all_states()
    rows = []
    for task in inbox.tasks:
        state = states.get(task.id)
        rows.append(
            {
                "id": task.id,
                "title": task.title,
                "inbox_status": task.status,
                "state": state.status if state else "READY",
                "attempts": state.attempts if state else 0,
                "session": state.session_id if state else "",
                "workstream": task.workstream,
                "priority": task.priority,
            }
        )
    print(json.dumps({"paused": cfg.pause_file.exists(), "tasks": rows}, indent=2))
    return 0


# --- kill switch ------------------------------------------------------------


def _cmd_pause() -> int:
    cfg = config()
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    cfg.pause_file.write_text("paused\n", encoding="utf-8")
    _log("info", f"kill switch armed at {cfg.pause_file}")
    return 0


def _cmd_resume() -> int:
    cfg = config()
    if cfg.pause_file.exists():
        cfg.pause_file.unlink()
    _log("info", "kill switch cleared; scheduling resumes on the next tick")
    return 0


# --- cron registration ------------------------------------------------------


def _hermes_bin() -> str:
    found = os.environ.get("DISPATCHER_HERMES_BIN")
    if found:
        return found
    for candidate in ("hermes",):
        from shutil import which

        path = which(candidate)
        if path:
            return path
    return "hermes"


def _cmd_register_self(args) -> int:
    cfg = config()
    script = cfg.home / "scripts" / "dispatcher_tick.sh"
    interval = args.interval or cfg.poll_interval
    name = cfg.cron_job_name
    cmd = [
        _hermes_bin(),
        "cron", "create",
        "--name", name,
        "--no-agent",
        "--script", script.name,
        "--deliver", "local",
        interval,
        "Hermes task dispatcher tick",
    ]
    if args.dry_run:
        print(" ".join(cmd))
        return 0
    import subprocess

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        _log("error", f"cron create failed: {proc.stderr.strip() or proc.stdout.strip()}")
        return proc.returncode
    _log("info", proc.stdout.strip())
    _log("info", f"registered as cron job {name!r} (every {interval})")
    return 0


def _cmd_unregister_self() -> int:
    import subprocess

    cfg = config()
    list_proc = subprocess.run(
        [_hermes_bin(), "cron", "list"], capture_output=True, text=True
    )
    if cfg.cron_job_name not in (list_proc.stdout + list_proc.stderr):
        _log("info", f"cron job {cfg.cron_job_name!r} is not registered")
        return 0
    proc = subprocess.run(
        [_hermes_bin(), "cron", "remove", "--name", cfg.cron_job_name],
        capture_output=True, text=True,
    )
    _log("info", (proc.stdout or proc.stderr).strip() or f"removed {cfg.cron_job_name!r}")
    return proc.returncode


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
