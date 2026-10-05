"""Configuration resolution for the Hermes task dispatcher.

All paths resolve from environment variables with safe defaults so the same
code runs on the Windows control host and on a server that owns the real
workspace volume. Nothing here reads a secret into a value that can be logged.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

__all__ = ["Config", "config"]


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Config:
    """Resolved dispatcher configuration. Immutable; no secrets."""

    # Dispatcher repo root (this repo).
    home: Path
    # Local clone of the control plane (h4z4rd95/Plan-Project-Situation).
    control_repo: Path
    # Pool root for bounded workspaces. The owner prompt names
    # /mnt/HC_Volume_107017670 (~20 GB); that volume lives on the Hetzner host,
    # not here, so the root is configurable and defaults to a sibling dir.
    workspace_pool: Path

    # Path of the machine-readable task inbox, relative to the control repo.
    inbox_rel: Path
    # Path of the durable outcome ledger, relative to the control repo.
    outcomes_rel: Path

    # Bounded execution knobs.
    poll_interval: str
    max_concurrent: int
    global_workspace_quota_mb: int
    stale_lease_seconds: int
    worker_spawn: bool

    # Hard wall-clock ceiling for one tick, in seconds. A tick must never
    # approach the cron scheduler's own script timeout (3600s); this budget is
    # enforced between tick phases so one slow phase cannot starve the
    # scheduler or turn a 5-minute poll into an hour-long hang.
    tick_budget_s: int

    # Kill switch: presence pauses all dispatch.
    pause_file: Path

    # SQLite state + worker outcome spool, under state/ (gitignored).
    state_dir: Path
    state_db: Path
    outcomes_dir: Path

    # Scheduling identifiers registered with `hermes cron`.
    cron_job_name: str

    @property
    def inbox_path(self) -> Path:
        return self.control_repo / self.inbox_rel

    @property
    def outcomes_path(self) -> Path:
        return self.control_repo / self.outcomes_rel

    def github_token(self) -> str | None:
        """Resolve the GitHub token from the environment.

        The live Hermes gateway already exports GITHUB_TOKEN (it is declared in
        ``$HERMES_HOME/.env``); workers inherit it. Returned only for passing to
        subprocess env or an Authorization header — never logged.
        """
        return os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")

    def child_env(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        """Environment for spawned workers; carries no new secrets."""
        env = dict(os.environ)
        if extra:
            env.update(extra)
        # Keep workers anchored to the same control plane + pool.
        env["DISPATCHER_HOME"] = str(self.home)
        env["DISPATCHER_CONTROL_REPO"] = str(self.control_repo)
        env["DISPATCHER_WORKSPACE_POOL"] = str(self.workspace_pool)
        return env


def _default_home() -> Path:
    """Repo root = the absolute directory containing this package's parent."""
    here = Path(__file__).resolve().parent  # .../dispatcher
    return here.parent


def _resolve_repo(default_name: str, sibling: str, env_var: str) -> Path:
    """Resolve a repo path from env, then an adjacent known directory."""
    raw = os.environ.get(env_var, "").strip()
    if raw:
        return Path(raw).expanduser()
    candidate = _default_home().parent / sibling
    if candidate.is_dir():
        return candidate
    return _default_home() / default_name


def config() -> Config:
    """Build the resolved configuration from the environment."""
    home = Path(os.environ.get("DISPATCHER_HOME", "")).expanduser()
    if not home.is_absolute() or not (home / "dispatcher" / "config.py").is_file():
        home = _default_home()

    control = _resolve_repo(
        default_name="plan-project-situation",
        sibling="plan-project-situation",
        env_var="DISPATCHER_CONTROL_REPO",
    )

    pool = os.environ.get("DISPATCHER_WORKSPACE_POOL", "").strip()
    if pool:
        workspace_pool = Path(pool).expanduser()
    else:
        # The owner's real volume is /mnt/HC_Volume_107017670 (Hetzner). Use it
        # when present; otherwise a sibling directory on this host.
        hetzner = Path("/mnt/HC_Volume_107017670")
        workspace_pool = hetzner if hetzner.is_dir() else home.parent / "dispatcher-workspaces"

    state_dir = home / "state"
    return Config(
        home=home,
        control_repo=control,
        workspace_pool=workspace_pool,
        inbox_rel=Path("hermes") / "tasks_inbox.toml",
        outcomes_rel=Path("hermes") / "dispatch_outcomes.jsonl",
        poll_interval=os.environ.get("DISPATCHER_POLL_INTERVAL", "5m"),
        max_concurrent=int(os.environ.get("DISPATCHER_MAX_CONCURRENT", "2")),
        # Ceiling for one tick (default 1500s, env-overridable). Chosen well
        # below the cron scheduler's 3600s script timeout and below the 5-minute
        # recurring interval times the number of missed fires the scheduler may
        # catch up in sequence, so a tick can never wedge the scheduler.
        tick_budget_s=int(os.environ.get("DISPATCHER_TICK_BUDGET_S", "1500")),
        global_workspace_quota_mb=int(os.environ.get("DISPATCHER_WORKSPACE_QUOTA_MB", "20480")),
        stale_lease_seconds=int(os.environ.get("DISPATCHER_STALE_LEASE_SECONDS", "900")),
        worker_spawn=_env_flag("DISPATCHER_WORKER_SPAWN", default=True),
        state_dir = state_dir,
        state_db = state_dir / "dispatcher.sqlite",
        outcomes_dir = state_dir / "outcomes",
        pause_file = home / "state" / "PAUSE",
        cron_job_name=os.environ.get("DISPATCHER_CRON_JOB_NAME", "hermes-task-dispatcher"),
    )
