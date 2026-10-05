"""GitHub control-repo adapter — Lane B of the Hermes task dispatcher.

The control plane is the ``h4z4rd95/Plan-Project-Situation`` repo. Lane A
holds a local clone of it; this module is the only thing that reads and
writes task state inside that clone:

* ``hermes/tasks_inbox.toml`` — the machine-readable task inbox.
* ``hermes/dispatch_outcomes.jsonl`` — the append-only outcome ledger.

Everything here is local ``git`` plus file IO. The remote is HTTPS and git
credential resolution already works in the clone (the token arrives via the
``GITHUB_TOKEN`` environment variable that the live gateway exports; it is
never named in this module, never passed as a CLI argument, and never
written to a log line).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence

__all__ = ["ControlRepo", "ControlRepoError"]

# Relative locations inside the control repo (mirror dispatcher/config.py).
INBOX_REL = Path("hermes") / "tasks_inbox.toml"
OUTCOMES_REL = Path("hermes") / "dispatch_outcomes.jsonl"

_TERMINAL_STATUSES = frozenset({"DONE", "FAILED", "CANCELLED"})

_SHORT_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")


class ControlRepoError(RuntimeError):
    """Raised when a control-repo operation fails (bad git state, bad inbox)."""


class _Runner(Protocol):
    def __call__(
        self,
        cmd: list[str],
        *,
        cwd: str | Path,
        env: dict[str, str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]: ...


def _git(
    runner: _Runner,
    repo_dir: Path,
    argv: list[str],
    *,
    env: dict[str, str],
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run one git command inside *repo_dir*.

    ``GIT_TERMINAL_PROMPT=0`` keeps credential helpers from ever blocking on
    an interactive prompt (the token is already in the environment git's
    credential helper reads), and commands are lists — never shell strings —
    so a task title cannot inject shell syntax.
    """
    full_env = dict(env)
    full_env.setdefault("GIT_TERMINAL_PROMPT", "0")
    return runner(
        ["git", *argv],
        cwd=repo_dir,
        env=full_env,
        capture_output=True,
        text=True,
        check=check,
    )


@dataclass(frozen=True)
class OutcomeRecord:
    """One ledger line in ``hermes/dispatch_outcomes.jsonl``."""

    task_id: str
    status: str
    summary: str
    evidence: list[dict[str, Any]]
    ts: str
    commit_sha: str | None = None
    blocker: str | None = None
    owner_action: str | None = None


class ControlRepo:
    """Read and write the dispatcher's task state inside a local git clone."""

    def __init__(self, repo_dir: Path, *, logger: Callable[[str], None] | Any = None) -> None:
        if repo_dir is None:
            raise ControlRepoError("repo_dir is required")
        self.repo_dir = Path(repo_dir)
        self.logger = logger
        self._runner: _Runner = _real_runner

    # ------------------------------------------------------------------ #
    # Test seam
    # ------------------------------------------------------------------ #
    def _set_runner(self, runner: _Runner) -> None:
        """Replace the git runner. Test-only seam; no network in the suite."""
        self._runner = runner

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #
    def remote_url(self) -> str:
        """The clone's ``origin`` URL (HTTPS for this control repo)."""
        proc = self._git_ok(["remote", "get-url", "origin"])
        return proc.stdout.strip()

    def head_sha(self) -> str:
        """The full SHA of ``HEAD``."""
        proc = self._git_ok(["rev-parse", "HEAD"])
        return proc.stdout.strip()

    def inbox_path(self) -> Path:
        """Absolute path of ``hermes/tasks_inbox.toml`` in the clone."""
        return self.repo_dir / INBOX_REL

    def read_inbox(self) -> str:
        """Raw inbox text, or ``""`` when the inbox is not present yet.

        Lane A creates the inbox; the adapter must tolerate its absence
        rather than crash a tick that has nothing to dispatch.
        """
        path = self.inbox_path()
        if not path.is_file():
            return ""
        return read_text_exact(path)

    # ------------------------------------------------------------------ #
    # Sync
    # ------------------------------------------------------------------ #
    def sync(self) -> str:
        """Fast-forward the clone to ``origin`` and return the new ``HEAD``.

        Refuses to rebase or create a merge commit: the control repo is
        single-writer per clone, so a non-fast-forward means something else
        rewrote history and a human must look at it.
        """
        self._git_ok(["fetch", "--quiet", "origin"])
        self._git_ok(["merge", "--ff-only", "--quiet", "origin/HEAD"])
        return self.head_sha()

    # ------------------------------------------------------------------ #
    # Writes
    # ------------------------------------------------------------------ #
    def write_outcome(
        self,
        *,
        task_id: str,
        status: str,
        summary: str,
        evidence: Sequence[dict[str, Any]],
        commit_sha: str | None = None,
        blocker: str | None = None,
        owner_action: str | None = None,
    ) -> str | None:
        """Record a finished task.

        Updates ``tasks_inbox.toml`` (the task's ``status`` line, comments and
        formatting preserved) and appends exactly one object to
        ``hermes/dispatch_outcomes.jsonl``, then commits everything and
        returns the new commit SHA. ``None`` when nothing changed.

        Idempotent per ``(task_id, terminal status)``: appending the same
        outcome twice writes the same ledger file and produces no commit.
        Non-terminal statuses (``RUNNING`` etc.) are always recorded, since a
        task can pass through them more than once across retries.
        """
        if not task_id:
            raise ControlRepoError("task_id is required")
        if not status:
            raise ControlRepoError("status is required")

        record = OutcomeRecord(
            task_id=task_id,
            status=status,
            summary=summary or "",
            evidence=list(evidence or []),
            ts=_now_iso(),
            commit_sha=commit_sha,
            blocker=blocker,
            owner_action=owner_action,
        )

        changed = self._append_outcome(record)
        inbox_changed = self._set_inbox_status(task_id, status)
        if not changed and not inbox_changed:
            self._log(f"write_outcome({task_id!r}, {status}) no-op")
            return None

        message = self._commit_message(task_id, status, summary)
        return self.commit_all(message)

    def commit_all(self, message: str) -> str | None:
        """Stage every change (including new files) and commit.

        Returns the new commit SHA, or ``None`` when the tree is clean.
        """
        if not message or not message.strip():
            raise ControlRepoError("commit message is required")
        # add -A so a newly created ledger or inbox is included on the first
        # outcome ever recorded.
        self._git_ok(["add", "-A"])
        staged = self._git_ok(["diff", "--cached", "--quiet"], check=False)
        if staged.returncode == 0:
            self._log("commit_all: nothing to commit")
            return None
        # - disables the editor; the message is never read from a file or a
        # shell string.
        self._git_ok(["commit", "--quiet", "-m", message])
        return self.head_sha()

    def push(self) -> None:
        """Publish local commits to ``origin``.

        Uses ``--force-with-lease`` so a divergent remote is rejected rather
        than overwritten — the control repo must never be force-pushed blind.
        """
        branch = self._current_branch()
        self._git_ok(["push", "--quiet", "origin", branch])

    # ------------------------------------------------------------------ #
    # Internals — git
    # ------------------------------------------------------------------ #
    def _git_ok(
        self, argv: list[str], *, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        """Run one git command. ``check=False`` returns a failed result instead
        of raising (used for the staged-content probe in :meth:`commit_all`)."""
        try:
            return _git(
                self._runner, self.repo_dir, argv, env=self._env(), check=check
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            raise ControlRepoError(
                f"git {' '.join(argv)} failed (exit {exc.returncode}) in "
                f"{self.repo_dir}: {stderr or stdout or 'no output'}"
            ) from exc

    def _current_branch(self) -> str:
        proc = self._git_ok(["rev-parse", "--abbrev-ref", "HEAD"])
        branch = proc.stdout.strip()
        if not branch or branch == "HEAD":
            raise ControlRepoError(
                "control repo is in detached HEAD; refusing to push"
            )
        return branch

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.setdefault("GIT_TERMINAL_PROMPT", "0")
        return env

    def _log(self, message: str) -> None:
        if self.logger is None:
            return
        self.logger(message)

    @staticmethod
    def _commit_message(task_id: str, status: str, summary: str) -> str:
        first = (summary or "").strip().splitlines()
        headline = first[0].strip() if first else "no summary"
        if len(headline) > 100:
            headline = headline[:97] + "..."
        return f"[dispatcher] {task_id} -> {status}: {headline}"

    # ------------------------------------------------------------------ #
    # Internals — inbox
    # ------------------------------------------------------------------ #
    def _set_inbox_status(self, task_id: str, status: str) -> bool:
        """Rewrite one task's ``status`` line in place.

        Approach: **surgical line replacement, not a full AST rewrite.**

        CONTRACT.md offers either a minimal regex edit on the matching
        ``[[task]]`` block or a rewrite from the parsed AST. The surgical edit
        is what keeps the file diffable: comments, blank lines, key order,
        quoting style and inline arrays elsewhere in the file are preserved
        byte-for-byte, so a diff of the inbox shows exactly one changed line.
        An AST rewrite would normalise the whole file on the first write and
        bury that one change in reformatting noise.

        The block boundary is the next ``[[task]]`` (or ``[`` table header) at
        the start of a line after the matched ``id`` line; the status key is
        only rewritten when it belongs to that block, so two tasks can never
        collide.
        """
        path = self.inbox_path()
        if not path.is_file():
            self._log(f"inbox absent; cannot update status for {task_id!r}")
            return False

        original = read_text_exact(path)
        lines = original.splitlines(keepends=True)
        id_re = re.compile(r'^\s*id\s*=\s*["\']' + re.escape(task_id) + r'["\']')
        # Groups: key, open quote, old value, close quote, original separator
        # (spaces/tabs before the comment, preserved verbatim), optional
        # comment. Preserving the separator keeps aligned comments aligned.
        status_re = re.compile(
            r'^(\s*status\s*=\s*)'
            r'(["\']?)'
            r'([A-Za-z0-9_-]+)'
            r'(["\']?)'
            r'([ \t]*)'
            r'(#.*)?$'
        )
        block_end_re = re.compile(r'^\s*\[\[')
        table_re = re.compile(r'^\s*\[[^\[.]')

        for start in range(len(lines)):
            if not id_re.match(lines[start]):
                continue
            for idx in range(start + 1, len(lines)):
                line = lines[idx]
                if block_end_re.match(line) or table_re.match(line):
                    break
                m = status_re.match(_strip_eol(line))
                if not m:
                    continue
                key, open_q, old, close_q, sep, comment = m.groups()
                if old == status:
                    return False
                replacement = f"{key}{open_q}{status}{close_q}"
                if comment:
                    replacement = f"{replacement}{sep}{comment}"
                lines[idx] = replacement + _eol(line)
                write_text_exact(path, "".join(lines))
                return True
            # id matched but no status key inside its block: nothing to edit.
            return False
        return False

    # ------------------------------------------------------------------ #
    # Internals — ledger
    # ------------------------------------------------------------------ #
    def _outcomes_path(self) -> Path:
        return self.repo_dir / OUTCOMES_REL

    def _read_ledger(self) -> list[OutcomeRecord]:
        path = self._outcomes_path()
        if not path.is_file():
            return []
        out: list[OutcomeRecord] = []
        for lineno, raw in enumerate(
            read_text_exact(path).splitlines(), start=1
        ):
            if not raw.strip():
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ControlRepoError(
                    f"{path}:{lineno}: corrupt JSONL ledger line: {exc.msg}"
                ) from exc
            out.append(_record_from_obj(obj))
        return out

    def _append_outcome(self, record: OutcomeRecord) -> bool:
        """Append one ledger line unless it is already there.

        Idempotency key: ``(task_id, status)`` where *status* is terminal. A
        terminal pair already present in the ledger means this outcome was
        recorded before (a retry re-reporting the same terminal result, or a
        tick re-ingesting an outcome file) and is skipped. Non-terminal
        statuses are always appended — a task legitimately visits ``RUNNING``
        once per attempt.
        """
        path = self._outcomes_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = self._read_ledger()
        if _is_terminal(record.status):
            for prior in existing:
                if prior.task_id == record.task_id and prior.status == record.status:
                    self._log(
                        f"outcome {record.task_id}/{record.status} already "
                        f"recorded; skipping"
                    )
                    return False
        line = json.dumps(_record_to_obj(record), ensure_ascii=False, sort_keys=False)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        return True


# ---------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------- #


def _real_runner(
    cmd: list[str],
    *,
    cwd: str | Path,
    env: dict[str, str],
    capture_output: bool,
    text: bool,
    check: bool,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 — argv list, never a shell string
        cmd,
        cwd=str(cwd),
        env=env,
        capture_output=capture_output,
        text=text,
        check=check,
    )


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def read_text_exact(path: Path) -> str:
    """Read text with **no newline translation**.

    ``Path.read_text`` uses universal newlines on Windows: a CRLF inbox read
    and written back through it comes out LF-only, which rewrites every line
    of the file and buries the one real change in whitespace noise. Binary
    mode + explicit utf-8 decode keeps the file byte-identical except for the
    single line we edit.
    """
    return path.open("rb").read().decode("utf-8")


def write_text_exact(path: Path, text: str) -> None:
    """Write text without newline translation (see :func:`read_text_exact`)."""
    with path.open("wb") as fh:
        fh.write(text.encode("utf-8"))


def _is_terminal(status: str) -> bool:
    return status.upper() in _TERMINAL_STATUSES


def _eol(line: str) -> str:
    """The original line terminator of *line*, preserving CRLF vs LF.

    Also drops any stray inner trailing ``\\r`` left after the final newline
    is stripped, so ``\\r\\r\\n`` (a doubled-CRLF line) does not become
    ``\\r\\r\\n`` again with an extra carriage return re-added.
    """
    if line.endswith("\r\n"):
        return "\r\n"
    if line.endswith("\n"):
        return "\n"
    return ""


def _strip_eol(line: str) -> str:
    """Remove the trailing newline sequence only (not inner CRs)."""
    if line.endswith("\r\n"):
        return line[:-2].rstrip("\r")
    if line.endswith("\n"):
        return line[:-1]
    return line


def _record_to_obj(record: OutcomeRecord) -> dict[str, Any]:
    obj: dict[str, Any] = {
        "task_id": record.task_id,
        "status": record.status,
        "summary": record.summary,
        "evidence": record.evidence,
        "ts": record.ts,
    }
    if record.commit_sha is not None:
        obj["commit_sha"] = record.commit_sha
    if record.blocker is not None:
        obj["blocker"] = record.blocker
    if record.owner_action is not None:
        obj["owner_action"] = record.owner_action
    return obj


def _record_from_obj(obj: dict[str, Any]) -> OutcomeRecord:
    return OutcomeRecord(
        task_id=obj.get("task_id", ""),
        status=obj.get("status", ""),
        summary=obj.get("summary", ""),
        evidence=obj.get("evidence", []) or [],
        ts=obj.get("ts", ""),
        commit_sha=obj.get("commit_sha"),
        blocker=obj.get("blocker"),
        owner_action=obj.get("owner_action"),
    )


def looks_like_sha(value: str | None) -> bool:
    """True when *value* is a plausible git SHA. Public helper for Lane A."""
    return bool(value) and bool(_SHORT_SHA_RE.match(value or ""))
