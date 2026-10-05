"""Bounded workspace manager (Lane C).

Owns the on-disk pool of task checkouts. Every checkout is recorded in a small
metadata store at ``<pool_root>/.dispatcher/workspaces.json`` (atomic writes),
and only paths present in that store as ``RECLAIM`` are ever deleted. Unknown
directories in the pool are reported by :meth:`WorkspaceManager.unmanaged_dirs`
and are never touched.

The pool root is configurable because the owner's real volume
(``/mnt/HC_Volume_107017670``, Hetzner) does not exist on the Windows control
host. See :func:`default_pool_root`, which honours the
``DISPATCHER_WORKSPACE_POOL`` environment override and falls back to a sibling
directory of the dispatcher repo. All path handling goes through ``pathlib``
and is Windows-safe.

Secrets: ``github_token`` is used *only* for the GitHub repo-size API over an
``Authorization`` header. It is never placed on a git command line and never
written to disk or a log line. Repository clones go through the ambient git
credential setup of the host (the same one Lane B relies on), so no secret has
to cross a process boundary here.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

__all__ = [
    "WorkspaceError",
    "WorkspaceQuotaError",
    "WorkspaceSpaceError",
    "Checkout",
    "WorkspaceManager",
    "default_pool_root",
]

MB = 1024 * 1024

# Metadata store layout.
METADATA_DIR = ".dispatcher"
METADATA_FILE = "workspaces.json"

POLICY_KEEP = "KEEP"
POLICY_RECLAIM = "RECLAIM"
POLICIES = (POLICY_KEEP, POLICY_RECLAIM)

# Strict shapes so a hostile/mangled inbox value can never reach a subprocess
# or escape the pool root as a path.
_VALID_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_VALID_BRANCH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
# Windows-forbidden characters plus separators and control bytes. The character
# class is deliberately explicit: a malformed class silently matches nothing and
# leaks raw slashes into path components (a real bug caught by the test suite).
_UNSAFE_CHARS = re.compile(r'[<>:"\|\?*\x00-\x1f\\/\\\\]')


class WorkspaceError(Exception):
    """Base class for all workspace-manager failures."""


class WorkspaceQuotaError(WorkspaceError):
    """A configured quota (global or per-task) would be exceeded, or the
    repository size could not be verified, so the clone is refused."""


class WorkspaceSpaceError(WorkspaceError):
    """The pool device does not have enough free space for the checkout."""


@dataclass
class Checkout:
    """Result of :meth:`WorkspaceManager.checkout`."""

    path: Path
    created: bool  # False when a valid recorded checkout was reused
    size_bytes: int


def _now() -> float:
    return time.time()


def _safe_component(value: str) -> str:
    """Collapse a free-form identifier into a single safe path component."""
    cleaned = _UNSAFE_CHARS.sub("_", str(value)).strip().rstrip(". ")
    return cleaned or "x"


def _dispatcher_home() -> Path:
    """Repo root containing this package (dispatcher/workspace.py -> root)."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "dispatcher" / "config.py").exists():
            return parent
    return here.parent.parent


def default_pool_root(home: Path | None = None) -> Path:
    """Resolve the workspace pool root.

    Priority: the ``DISPATCHER_WORKSPACE_POOL`` environment override, then the
    owner's real Hetzner volume when it is actually mounted, then a sibling
    directory of the dispatcher repo. Never hardcodes the volume.
    """
    raw = os.environ.get("DISPATCHER_WORKSPACE_POOL", "").strip()
    if raw:
        return Path(raw).expanduser()

    hetzner = Path("/mnt/HC_Volume_107017670")
    if hetzner.is_dir():
        return hetzner

    base = Path(home) if home is not None else _dispatcher_home()
    return base.parent / "dispatcher-workspaces"


class WorkspaceManager:
    """Bounded manager for per-task git checkouts under one pool root."""

    def __init__(
        self,
        pool_root: Path,
        *,
        global_quota_mb: int,
        github_token: str | None = None,
        logger: logging.Logger | None = None,
        git_timeout_s: int = 600,
        api_timeout_s: int = 15,
    ) -> None:
        self.pool_root = Path(pool_root)
        self.global_quota_mb = int(global_quota_mb)
        self.github_token = github_token
        self.git_timeout_s = git_timeout_s
        self.api_timeout_s = api_timeout_s
        self._log = logger or logging.getLogger("dispatcher.workspace")
        self._lock = threading.RLock()
        self._records: list[dict[str, Any]] = []
        self._meta_dir = self.pool_root / METADATA_DIR
        self._meta_path = self._meta_dir / METADATA_FILE

    # ------------------------------------------------------------------ utils

    def _ensure(self) -> None:
        self.pool_root.mkdir(parents=True, exist_ok=True)
        self._meta_dir.mkdir(parents=True, exist_ok=True)

    def _load(self) -> None:
        data: Any = None
        if self._meta_path.is_file():
            try:
                data = json.loads(self._meta_path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                # A corrupt store must not brick dispatch; start empty. Lost
                # metadata simply means those checkouts become unmanaged (and
                # are therefore protected from deletion).
                self._log.warning(
                    "workspace metadata unreadable at %s, starting empty: %s",
                    self._meta_path, exc,
                )
                data = None
        records = data.get("records") if isinstance(data, dict) else None
        self._records = [
            dict(rec)
            for rec in (records or [])
            if isinstance(rec, dict) and rec.get("path")
        ]

    def _save(self) -> None:
        """Write the metadata store atomically (tmp file + os.replace)."""
        self._ensure()
        tmp = self._meta_dir / f"{METADATA_FILE}.tmp"
        blob = json.dumps(
            {"version": 1, "records": self._records}, indent=2, sort_keys=False
        )
        try:
            with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:  # pragma: no cover - defensive
            raise WorkspaceError(f"cannot write metadata store: {exc}") from exc
        os.replace(tmp, self._meta_path)

    @staticmethod
    def _key(task_id: str, repo: str, branch: str) -> tuple[str, str, str]:
        return (task_id, repo, branch)

    def _path_for(self, task_id: str, repo: str, branch: str) -> Path:
        name = "__".join(
            _safe_component(part) for part in (task_id, repo, branch)
        )
        return self.pool_root / name

    def _find(self, task_id: str, repo: str, branch: str) -> dict[str, Any] | None:
        key = self._key(task_id, repo, branch)
        for record in self._records:
            if (record.get("task_id"), record.get("repo"), record.get("branch")) == key:
                return record
        return None

    def _record_for_path(self, path: Path) -> dict[str, Any]:
        target = Path(path).resolve()
        for record in self._records:
            try:
                if Path(record["path"]).resolve() == target:
                    return record
            except (OSError, ValueError):
                continue
        raise WorkspaceError(f"not a managed checkout: {path}")

    def _inside_pool(self, path: Path) -> bool:
        """True only for an existing directory strictly beneath the pool root."""
        try:
            rel = Path(path).resolve().relative_to(self.pool_root.resolve())
        except (ValueError, OSError):
            return False
        return rel.parts != () and rel.parts[0] != ".."

    @staticmethod
    def _validate(task_id: str, repo: str, branch: str) -> None:
        if not task_id or not _safe_component(task_id) or _safe_component(
            task_id
        ) != task_id:
            raise WorkspaceError(
                f"invalid task_id (must be a single safe path component): {task_id!r}"
            )
        if not _VALID_REPO.match(repo or ""):
            raise WorkspaceError(
                f"invalid repo (expected 'owner/name'): {repo!r}"
            )
        if not _VALID_BRANCH.match(branch or ""):
            raise WorkspaceError(f"invalid branch: {branch!r}")

    # --------------------------------------------------- filesystem reality

    def pool_free_bytes(self) -> int:
        """Free space on the pool's device (0 when the pool is absent)."""
        try:
            return shutil.disk_usage(self.pool_root).free
        except OSError as exc:
            self._log.debug("cannot measure pool free space: %s", exc)
            return 0

    def pool_used_bytes(self) -> int:
        """Bytes occupied by managed checkouts (sum of recorded sizes)."""
        return sum(int(rec.get("size_bytes") or 0) for rec in self._records)

    def repo_size_bytes(self, repo: str) -> int | None:
        """Repository size in bytes via the GitHub API, or None if unknown.

        The GitHub API reports ``size`` in kilobytes. Never raises: an unknown
        size is the caller's signal to fail closed rather than clone blind.
        """
        if not _VALID_REPO.match(repo or ""):
            return None
        url = f"https://api.github.com/repos/{repo}"
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "hermes-task-dispatcher",
        }
        if self.github_token:
            # Header only; the token never appears in a URL, argv or a log.
            headers["Authorization"] = f"Bearer {self.github_token}"
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=self.api_timeout_s) as resp:
                payload = json.loads(resp.read())
        except Exception as exc:  # noqa: BLE001 - any failure means "unknown"
            self._log.debug("repo size unknown for %s: %s", repo, exc)
            return None
        size_kb = payload.get("size") if isinstance(payload, dict) else None
        if not isinstance(size_kb, (int, float)):
            return None
        return int(size_kb) * 1024

    # ------------------------------------------------------------ git plumbing

    def _git(self, args: Sequence[str], *, cwd: Path) -> subprocess.CompletedProcess:
        cmd = ["git", *args]
        self._log.debug("git %s (cwd=%s)", " ".join(args), cwd)
        try:
            return subprocess.run(
                cmd,
                cwd=str(cwd),
                check=True,
                capture_output=True,
                text=True,
                timeout=self.git_timeout_s,
            )
        except subprocess.TimeoutExpired as exc:
            raise WorkspaceError(f"git {' '.join(args)} timed out") from exc
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or str(exc.stdout or "")).strip()
            raise WorkspaceError(
                f"git {' '.join(args)} failed: {detail}"
            ) from exc
        except OSError as exc:
            raise WorkspaceError(f"git {' '.join(args)} failed: {exc}") from exc

    def _clone(self, *, repo: str, branch: str, path: Path,
               shallow: bool, depth: int) -> None:
        url = f"https://github.com/{repo}.git"
        args = ["clone", "--quiet"]
        if shallow:
            args += ["--depth", str(max(1, int(depth))), "--single-branch"]
        args += ["--branch", branch, "--", url, str(path)]
        path.parent.mkdir(parents=True, exist_ok=True)
        self._git(args, cwd=self.pool_root)

    def _sync(self, path: Path, branch: str) -> None:
        """Refresh a reused checkout to the tip of ``branch``."""
        self._git(["fetch", "--quiet", "origin"], cwd=path)
        self._git(["checkout", branch], cwd=path)
        self._git(["reset", "--hard", f"origin/{branch}"], cwd=path)

    @staticmethod
    def _valid_checkout(path: Path) -> bool:
        try:
            return path.is_dir() and (path / ".git").exists()
        except OSError:  # pragma: no cover - defensive
            return False

    @staticmethod
    def _measure(path: Path) -> int:
        """Size of a checkout on disk (best effort; symlinks not followed)."""
        total = 0
        for dirpath, _dirs, files in os.walk(path, onerror=lambda _e: None,
                                             followlinks=False):
            for name in files:
                try:
                    total += os.stat(
                        os.path.join(dirpath, name), follow_symlinks=False
                    ).st_size
                except OSError:
                    continue
        return total

    # ------------------------------------------------------------- public API

    def checkout(
        self,
        *,
        task_id: str,
        repo: str,
        branch: str = "main",
        purpose: str = "",
        keep: bool = False,
        task_budget_mb: int = 0,
        shallow: bool = True,
        depth: int = 1,
    ) -> Checkout:
        """Clone (or reuse) a checkout for ``(task_id, repo, branch)``.

        Idempotent: an existing recorded checkout with a valid git directory is
        reused with ``created=False`` after a fetch/checkout/reset to the tip of
        ``branch``. Quota and free-space checks run *before* any clone and fail
        closed.
        """
        self._validate(task_id, repo, branch)
        self._ensure()

        path = self._path_for(task_id, repo, branch)

        # Fast path: reuse a recorded, still-valid checkout.
        with self._lock:
            self._load()
            record = self._find(task_id, repo, branch)
            if record is not None:
                recorded_path = Path(record["path"])
                if self._valid_checkout(recorded_path):
                    self._sync(recorded_path, branch)
                    record["last_use"] = _now()
                    record["policy"] = (
                        POLICY_KEEP if keep else record.get("policy", POLICY_RECLAIM)
                    )
                    self._save()
                    self._log.debug("reused workspace %s for %s", recorded_path, task_id)
                    return Checkout(
                        path=recorded_path,
                        created=False,
                        size_bytes=int(record.get("size_bytes") or 0),
                    )
                # Stale record (dir gone / not a git repo). The path is still
                # metadata-owned, so removing the leftover is within the safety
                # invariants; then fall through to a fresh clone.
                self._log.debug("discarding invalid checkout record for %s", task_id)
                if recorded_path.exists():
                    shutil.rmtree(recorded_path, ignore_errors=True)
                self._records.remove(record)
                self._save()

            # Never silently clobber an unmanaged directory. This is the hard
            # safety fence: a path on disk that we do not own is never deleted
            # or cloned over, even when it matches a computed task path.
            if path.exists():
                raise WorkspaceError(
                    f"refusing to clone over existing unmanaged path: {path}"
                )

        # Re-check just before cloning (outside the lock): another process may
        # have created the path while we were measuring quota. Fail closed.
        if path.exists():
            raise WorkspaceError(
                f"refusing to clone over existing unmanaged path: {path}"
            )

        # Quota gate BEFORE cloning. Fail closed on every violation.
        size_bytes = self._check_quota(repo=repo, task_budget_mb=task_budget_mb)

        with self._lock:
            self._clone(repo=repo, branch=branch, path=path,
                        shallow=shallow, depth=depth)
            measured = self._measure(path)
            now = _now()
            record = {
                "path": str(path),
                "task_id": task_id,
                "repo": repo,
                "branch": branch,
                "purpose": purpose,
                "size_bytes": measured or size_bytes,
                "last_use": now,
                "created_at": now,
                "policy": POLICY_KEEP if keep else POLICY_RECLAIM,
                "session_id": "",
            }
            self._records.append(record)
            self._save()

        self._log.info(
            "checked out %s (%d bytes) for task %s", path, record["size_bytes"], task_id
        )
        return Checkout(path=path, created=True, size_bytes=record["size_bytes"])

    def _check_quota(self, *, repo: str, task_budget_mb: int) -> int:
        """Verify the repo fits every configured bound; return its size in bytes."""
        size_bytes = self.repo_size_bytes(repo)
        if size_bytes is None:
            # Fail closed: an unverified size cannot be shown to fit any quota.
            raise WorkspaceQuotaError(
                f"cannot verify repository size for {repo}; refusing to clone"
            )
        if self.global_quota_mb > 0 and size_bytes > self.global_quota_mb * MB:
            raise WorkspaceQuotaError(
                f"repository {repo} is {size_bytes / MB:.1f} MiB, exceeding the "
                f"global workspace quota of {self.global_quota_mb} MiB"
            )
        if task_budget_mb > 0 and size_bytes > task_budget_mb * MB:
            raise WorkspaceQuotaError(
                f"repository {repo} is {size_bytes / MB:.1f} MiB, exceeding the "
                f"task budget of {task_budget_mb} MiB"
            )
        free_bytes = self.pool_free_bytes()
        if free_bytes < size_bytes:
            raise WorkspaceSpaceError(
                f"pool has {free_bytes / MB:.1f} MiB free but {repo} needs "
                f"{size_bytes / MB:.1f} MiB"
            )
        return size_bytes

    def bind_session(self, path: Path, session_id: str) -> None:
        """Record the agent session that owns a checkout."""
        with self._lock:
            record = self._record_for_path(path)
            record["session_id"] = str(session_id)
            record["last_use"] = _now()
            self._save()

    def touch(self, path: Path) -> None:
        """Mark a checkout as recently used."""
        with self._lock:
            record = self._record_for_path(path)
            record["last_use"] = _now()
            self._save()

    def note_size(self, path: Path, size_bytes: int) -> None:
        """Update the recorded on-disk size of a checkout."""
        with self._lock:
            record = self._record_for_path(path)
            record["size_bytes"] = int(size_bytes)
            record["last_use"] = _now()
            self._save()

    def release(self, path: Path) -> None:
        """Mark a checkout reclaimable (policy ``RECLAIM``)."""
        self._set_policy(path, POLICY_RECLAIM)

    def keep(self, path: Path) -> None:
        """Mark a checkout to survive reclaim (policy ``KEEP``)."""
        self._set_policy(path, POLICY_KEEP)

    def _set_policy(self, path: Path, policy: str) -> None:
        with self._lock:
            record = self._record_for_path(path)
            record["policy"] = policy
            record["last_use"] = _now()
            self._save()

    def reclaim(self, *, dry_run: bool = True,
                task_ids: list[str] | None = None) -> dict:
        """Reclaim checkout directories marked ``RECLAIM``.

        ``dry_run=True`` (the default) deletes nothing and reports what *would*
        be removed. Only directories that are both recorded in the metadata
        store and located inside the pool root are ever deleted; everything else
        is reported, never removed.
        """
        self._ensure()
        with self._lock:
            self._load()
            targets: list[tuple[dict[str, Any], Path, bool]] = []
            for record in list(self._records):
                if record.get("policy") != POLICY_RECLAIM:
                    continue
                if task_ids is not None and record.get("task_id") not in task_ids:
                    continue
                path = Path(record["path"])
                if not self._inside_pool(path):
                    self._log.warning(
                        "skipping reclaim of recorded path outside pool: %s", path
                    )
                    continue
                exists = path.is_dir()
                targets.append((record, path, exists))

            would_free = sum(
                int(rec.get("size_bytes") or 0)
                for rec, _path, exists in targets
                if exists
            )
            would_delete = [str(path) for _rec, path, exists in targets if exists]
            missing = [str(path) for _rec, path, exists in targets if not exists]

            if dry_run:
                kept = [rec["path"] for rec in self._records
                        if rec.get("policy") == POLICY_KEEP]
                return {
                    "dry_run": True,
                    "count": len(would_delete),
                    "would_delete": would_delete,
                    "freed_bytes": would_free,
                    "missing": missing,
                    "kept": kept,
                }

            deleted: list[str] = []
            failed: list[dict[str, str]] = []
            for record, path, exists in targets:
                if not exists:
                    # Recorded workspace already gone: just drop the record.
                    self._records.remove(record)
                    continue
                try:
                    shutil.rmtree(path)
                except OSError as exc:
                    self._log.warning("could not reclaim %s: %s", path, exc)
                    failed.append({"path": str(path), "error": str(exc)})
                    continue
                deleted.append(str(path))
                self._records.remove(record)
            self._save()

            freed = sum(
                int(rec.get("size_bytes") or 0)
                for rec in self._records
            )
            return {
                "dry_run": False,
                "count": len(deleted),
                "deleted": deleted,
                "freed_bytes": would_free,
                "missing": missing,
                "failed": failed,
                "remaining_used_bytes": freed,
            }

    def report(self) -> list[dict]:
        """All metadata rows, most recently used first (copies)."""
        with self._lock:
            self._load()
            rows = sorted(
                (dict(rec) for rec in self._records),
                key=lambda rec: float(rec.get("last_use") or 0.0),
                reverse=True,
            )
        return rows

    def unmanaged_dirs(self) -> list[Path]:
        """Directories in the pool with no metadata record. NEVER deleted.

        Scans one level beneath the pool root (the checkout layout is flat) and
        always excludes the metadata directory itself.
        """
        self._ensure()
        with self._lock:
            self._load()
            managed: set[Path] = set()
            for record in self._records:
                try:
                    managed.add(Path(record["path"]).resolve())
                except (OSError, ValueError):
                    continue

            found: list[Path] = []
            try:
                children = sorted(self.pool_root.iterdir())
            except OSError:  # pragma: no cover - defensive
                return found
            for child in children:
                if not child.is_dir() or child.name == METADATA_DIR:
                    continue
                try:
                    if child.resolve() in managed:
                        continue
                except (OSError, ValueError):
                    continue
                found.append(child)
            return found
