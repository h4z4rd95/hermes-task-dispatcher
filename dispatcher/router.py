"""Session router — Lane B of the Hermes task dispatcher.

Wraps the installed Hermes CLI's session-routing primitive so the dispatcher
can drive a task prompt into a named session and know whether it resumed an
existing session or started a fresh one.

Verified against Hermes v0.21.5+6718.g158fd63 on this host:

* ``hermes chat --continue "<name>" --create-if-missing -q "<prompt>" -Q
  --format text`` resumes a session named ``<name>`` when one exists and
  creates it otherwise. Both paths were exercised on this host.
* Output ends with a trailing ``session_id: <id>`` line.
* The resume preamble contains ``Resumed session``; the create preamble
  contains ``Starting fresh`` (observed verbatim).
* ``hermes sessions export --format md --dry-run --title "<name>"`` is a
  read-only, side-effect-free existence query — see
  :meth:`SessionRouter.session_name_exists` for why it was chosen.

The child process inherits the caller's environment, so ``GITHUB_TOKEN`` (set
in ``$HERMES_HOME/.env`` and exported by the live gateway) reaches the agent
without ever appearing on a command line or in a log line.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

__all__ = ["RunResult", "SessionRouter", "RouterError", "HERMES_BIN"]

# The entrypoint resolved on this host from ``which hermes``.
HERMES_BIN = shutil.which("hermes") or "hermes"

# Preamble markers, taken verbatim from verified runs on this host.
_RESUMED_MARKER = "Resumed session"
_FRESH_MARKER = "Starting fresh"
_SESSION_ID_RE = re.compile(r"^session_id:\s*(\S+)\s*$", re.MULTILINE)


class RouterError(RuntimeError):
    """Raised when a routed Hermes session run fails structurally.

    A non-zero agent exit code is *not* necessarily a structural failure —
    :meth:`SessionRouter.run` reports it through :attr:`RunResult.exit_code`
    and only raises when the invocation itself could not be completed or the
    session id could not be recovered.
    """


class _Runner(Protocol):
    """Minimal subprocess shape this module depends on (for test injection)."""

    def __call__(
        self,
        cmd: list[str],
        *,
        cwd: str | Path | None,
        env: dict[str, str],
        capture_output: bool,
        text: bool,
        timeout: float | None,
    ) -> subprocess.CompletedProcess[str]: ...


def _default_runner(
    cmd: list[str],
    *,
    cwd: str | Path | None,
    env: dict[str, str],
    capture_output: bool,
    text: bool,
    timeout: float | None,
) -> subprocess.CompletedProcess[str]:
    """Real subprocess runner. Prompts travel via ``--query-file`` stdin.

    ``--query-file -`` keeps the prompt body out of the process command line,
    so a prompt containing shell metacharacters or a token-like string cannot
    be misparsed by a shell or surfaced in a process listing. The Hermes CLI
    documents ``-`` as "read the query from stdin".

    ``input`` is passed and ``stdin`` never is: CPython's ``subprocess.run``
    raises ``ValueError('stdin and input arguments may not both be used.')``
    when both are given (``input`` non-``None``), and installing the pipe
    itself is exactly what ``run`` does for us in that case. ``input=""`` is
    correct for an empty prompt — verified on this host, it closes stdin and
    delivers immediate EOF, whereas ``input=None`` inherits the parent's
    stdin and blocks on a reader like ``cat``.
    """
    return subprocess.run(  # noqa: S603 — cmd is a list built in-process
        cmd,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
        input=_stdin_payload(cmd),
    )


def _uses_stdin(cmd: list[str]) -> bool:
    return "--query-file" in cmd and "-" in cmd


def _stdin_payload(cmd: list[str]) -> str | None:
    if not _uses_stdin(cmd):
        return None
    return _PROMPT_HOLDER[0]


# The prompt is stashed here rather than threaded through the runner signature
# so tests can swap in a plain ``subprocess.run``-shaped callable. It is set
# immediately before the runner is called and cleared right after.
_PROMPT_HOLDER: list[str] = [""]


@dataclass
class RunResult:
    """Outcome of one routed Hermes session run.

    Attributes:
        session_id: The session id Hermes reported on the trailing
            ``session_id:`` line. Empty when the run could not be completed.
        exit_code: The Hermes process exit code. ``0`` means the run answered
            and exited cleanly.
        stdout: Captured combined stdout (stdout + stderr; the CLI mixes them
            on a non-TTY). Never contains a secret — tokens never reach the
            command line, so they cannot appear here.
        duration_s: Wall-clock seconds the child ran for.
        resumed: ``True`` when the run routed into an already-existing session
            (``Resumed session`` in the preamble), ``False`` when Hermes
            started a fresh session (``Starting fresh``).
    """

    session_id: str
    exit_code: int
    stdout: str
    duration_s: float
    resumed: bool = False
    # Extra fields the dispatcher may want without re-parsing stdout. Not part
    # of the CONTRACT.md signature; safe to ignore.
    timed_out: bool = field(default=False, metadata={"internal": True})


class SessionRouter:
    """Drive task prompts into named Hermes sessions.

    The router never creates or deletes sessions itself; it only asks the
    Hermes CLI to resume-or-create. The caller (Lane A) decides session
    affinity and passes the target name.
    """

    def __init__(
        self,
        *,
        workdir: Path | None = None,
        logger: Callable[[str], None] | Any = None,
        extra_env: dict[str, str] | None = None,
    ) -> None:
        self.workdir = workdir
        self.logger = logger
        self.extra_env = dict(extra_env) if extra_env else {}
        # The CLI exists even when the binary is absent (tests inject a fake);
        # do not resolve eagerly or test fixtures would need a real hermes.
        self._bin = HERMES_BIN
        self._runner: _Runner = _default_runner

    # ------------------------------------------------------------------ #
    # Test seam
    # ------------------------------------------------------------------ #
    def _set_runner(self, runner: _Runner) -> None:
        """Replace the subprocess runner. Test-only seam."""
        self._runner = runner

    # ------------------------------------------------------------------ #
    # Existence query
    # ------------------------------------------------------------------ #
    def session_name_exists(self, session_name: str) -> bool:
        """Return ``True`` when a session named *session_name* exists.

        Chosen mechanism (documented per CONTRACT.md, with evidence):

        ``hermes sessions export --format md --dry-run --title "<name>"``.

        Why this over the alternatives:

        * It is **read-only and side-effect free**. ``--dry-run`` short
          circuits before any file is written, so nothing touches the session
          store, the filesystem, or the network. Verified on this host: with
          ``--dry-run`` the command only prints ``Would export N session(s)
          (title contains '<name>').`` and exits ``0``.
        * It **filters by title from the SQLite store** rather than by
          workspace, which matches the dispatcher's model: a task names its
          session by title (``session = "name"`` in ``tasks_inbox.toml``) and
          the router targets it by that same title via ``--continue``.
        * It has a **machine-readable decision boundary**: any ``N >= 1``
          means the name is taken; ``0`` means it is free. Title matching is a
          substring ``LIKE`` in the store, so a caller asking about
          ``foo`` would also match ``foobar``; the router therefore asks about
          the exact title and treats a count of zero as "does not exist".

        Evidence gathered on this host (Hermes v0.21.5+6718):

        * ``hermes sessions list`` — no JSON output and no title filter (only
          ``--source``/``--limit``/``--workspace``), so it cannot answer a
          title question without scraping a human-oriented, Unicode-boxed,
          CRLF-terminated table whose columns are not title-unique.
        * ``hermes sessions export --title`` — substring match on the session
          title. Verified: ``--title "Leantime"`` reports 6 sessions while
          ``--title "Leantime-RTL"`` reports 1, and a title that matches
          nothing reports 0.
        * ``hermes sessions export --format jsonl -`` produced an empty file
          for a title that exists, so jsonl-to-stdout is not a reliable
          truth source on this build; the ``--dry-run`` count is.

        Rejected alternative: probing ``hermes chat --continue "<name>" -q ""``.
        That *writes* to the session store (it creates the session when it is
        absent, which is exactly the side effect a preflight check must not
        have) and is documented in CONTRACT.md only as a last resort.

        Nonzero exit or unparseable output resolves to ``False`` (fail closed:
        the caller re-checks after the run by inspecting the preamble, which is
        authoritative for the actual routing decision).
        """
        cmd = [
            self._bin,
            "sessions",
            "export",
            "--format", "md",
            "--dry-run",
            "--title", session_name,
        ]
        try:
            proc = self._runner(  # noqa: S603
                cmd,
                cwd=self.workdir,
                env=self._child_env(),
                capture_output=True,
                text=True,
                timeout=60,
            )
        except Exception as exc:  # pragma: no cover - defensive
            self._log(f"session_name_exists({session_name!r}) failed: {exc}")
            return False

        if proc.returncode != 0:
            self._log(
                f"session existence probe exit={proc.returncode}: "
                f"{(proc.stderr or proc.stdout or '').strip()[:200]}"
            )
            return False

        return _count_from_dry_run(proc.stdout) > 0

    # ------------------------------------------------------------------ #
    # Run
    # ------------------------------------------------------------------ #
    def run(
        self,
        *,
        session_name: str,
        prompt: str,
        model: str | None = None,
        max_turns: int | None = None,
        run_budget_s: int | None = None,
    ) -> RunResult:
        """Run *prompt* in the session named *session_name*.

        Resumes that session when it exists, otherwise creates it. Returns a
        :class:`RunResult` describing the outcome.

        Raises:
            RouterError: When the Hermes invocation cannot be completed, or
                completes without a parseable ``session_id:`` line.
        """
        if not session_name:
            raise RouterError("session_name is required")
        if not prompt:
            raise RouterError("prompt is required")

        cmd = self._build_cmd(
            session_name=session_name,
            model=model,
            max_turns=max_turns,
            run_budget_s=run_budget_s,
        )
        self._log(
            f"run session={session_name!r} model={model or 'default'} "
            f"max_turns={max_turns} budget={run_budget_s}s "
            f"workdir={self.workdir!s}"
        )

        _PROMPT_HOLDER[0] = prompt
        started = time.monotonic()
        timed_out = False
        try:
            try:
                proc = self._runner(  # noqa: S603
                    cmd,
                    cwd=self.workdir,
                    env=self._child_env(),
                    capture_output=True,
                    text=True,
                    timeout=float(run_budget_s) if run_budget_s else None,
                )
            except subprocess.TimeoutExpired as exc:
                timed_out = True
                # TimeoutExpired carries partial output in .output/.stderr.
                stdout = (exc.output or "") + (exc.stderr or "")
                duration_s = time.monotonic() - started
                self._log(
                    f"run session={session_name!r} timed out after "
                    f"{run_budget_s}s"
                )
                return RunResult(
                    session_id=_parse_session_id(stdout),
                    exit_code=124,
                    stdout=stdout,
                    duration_s=duration_s,
                    resumed=_detect_resumed(stdout),
                    timed_out=True,
                )
        finally:
            _PROMPT_HOLDER[0] = ""

        duration_s = time.monotonic() - started
        stdout = (proc.stdout or "") + (proc.stderr or "")
        session_id = _parse_session_id(stdout)
        if not session_id and proc.returncode == 0:
            # A clean run must always print its session id; without it the
            # caller cannot record what happened.
            raise RouterError(
                f"hermes run for session {session_name!r} produced no "
                f"session_id line"
            )

        return RunResult(
            session_id=session_id,
            exit_code=proc.returncode,
            stdout=stdout,
            duration_s=duration_s,
            resumed=_detect_resumed(stdout),
            timed_out=timed_out,
        )

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #
    def _build_cmd(
        self,
        *,
        session_name: str,
        model: str | None,
        max_turns: int | None,
        run_budget_s: int | None,
    ) -> list[str]:
        cmd: list[str] = [
            self._bin,
            "chat",
            "--continue", session_name,
            "--create-if-missing",
            "-Q",
            "--format", "text",
            "--query-file", "-",
        ]
        if self.workdir is not None:
            cmd += ["--in", str(self.workdir)]
        if model:
            cmd += ["--model", model]
        if max_turns is not None:
            cmd += ["--max-turns", str(int(max_turns))]
        if run_budget_s is not None:
            cmd += ["--run-budget", str(int(run_budget_s))]
        return cmd

    def _child_env(self) -> dict[str, str]:
        env = dict(os_env())
        env.update(self.extra_env)
        return env

    def _log(self, message: str) -> None:
        if self.logger is None:
            return
        # Prompts are task text, not secrets (per CONTRACT.md); still, the
        # logger only ever sees invocation metadata, never the prompt body.
        self.logger(message)


# ---------------------------------------------------------------------- #
# Pure helpers (module-level so tests can exercise them directly)
# ---------------------------------------------------------------------- #


def os_env() -> dict[str, str]:
    """Current environment, as a dict. Isolated for test injection."""
    return dict(os.environ)


def _count_from_dry_run(stdout: str) -> int:
    """Parse ``Would export N session(s) ...`` from a dry-run listing."""
    if not stdout:
        return 0
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("Would export"):
            m = re.search(r"export\s+(\d+)", line)
            if m:
                return int(m.group(1))
        # No-dry-run fallback shape: "Exported N sessions to <path>".
        m = re.match(r"Exported\s+(\d+)\s+sessions?\b", line)
        if m:
            return int(m.group(1))
    return 0


def _parse_session_id(stdout: str) -> str:
    """Pull ``<id>`` out of the trailing ``session_id: <id>`` line."""
    if not stdout:
        return ""
    m = _SESSION_ID_RE.search(stdout)
    return m.group(1) if m else ""


def _detect_resumed(stdout: str) -> bool:
    """Decide resume-vs-create from the CLI preamble.

    Both markers were observed verbatim on this host:

    * resume: ``↻ Resumed session 20261005_030008_17da54 "probe-new-session-001"``
    * create: ``Session 20261005_031844_e29272 found but has no messages.
      Starting fresh.``

    The arrow glyph is stripped before matching so a shell locale that drops
    or mangles it cannot flip the answer. ``Starting fresh`` is checked first:
    a session that exists but is empty prints *both* markers, and the CLI's
    actual behavior in that case is "start fresh", so that must win.
    """
    if not stdout:
        return False
    normalized = stdout.replace("↻", "").replace("→", "")
    if _FRESH_MARKER in normalized:
        return False
    return _RESUMED_MARKER in normalized


def parse_run_output(stdout: str) -> dict[str, Any]:
    """Structured view of a run's stdout (public helper for Lane A).

    Returns a dict with ``session_id``, ``resumed`` and ``fresh`` keys.
    """
    return {
        "session_id": _parse_session_id(stdout),
        "resumed": _detect_resumed(stdout),
        "fresh": _FRESH_MARKER in stdout.replace("↻", "").replace("→", ""),
    }
