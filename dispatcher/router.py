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
import signal
import sqlite3
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

__all__ = ["RunResult", "SessionRouter", "RouterError", "HERMES_BIN"]

# The entrypoint resolved on this host from ``which hermes``.
HERMES_BIN = shutil.which("hermes") or "hermes"

# ``--format text`` support cache. None = not probed yet. The probe is a
# ``--help`` parse (no side effects, no network); the result is per-process.
_FORMAT_TEXT_SUPPORTED: bool | None = None

# ``chat --create-if-missing`` / ``--query-file`` support cache. These exist
# only on Hermes >= 0.21.x. Older builds (the deployed v0.19.0) have neither,
# and their ``--continue <name>`` fails when the named session is unknown, so
# the router must fall back to a probe-then-rename flow (see
# :meth:`SessionRouter.run`). None = not probed yet; per-process.
_NEW_SESSION_FLAGS_SUPPORTED: bool | None = None


def _set_format_text_supported(value: bool | None) -> None:
    """Test seam: pin the probe result so tests do not shell out."""
    global _FORMAT_TEXT_SUPPORTED  # noqa: PLW0603 — intentional test seam
    _FORMAT_TEXT_SUPPORTED = value


def _set_new_session_flags_supported(value: bool | None) -> None:
    """Test seam: pin the probe result so tests do not shell out."""
    global _NEW_SESSION_FLAGS_SUPPORTED  # noqa: PLW0603 — intentional test seam
    _NEW_SESSION_FLAGS_SUPPORTED = value


def _cli_supports_new_session_flags(hermes_bin: str) -> bool:
    """Whether the installed Hermes CLI has the modern session flags.

    ``chat --create-if-missing`` (create a named session on first run) and
    ``--query-file`` (prompt via stdin) are both >= 0.21.x. Probed with
    ``chat --help`` (read-only, no session created).
    """
    try:
        proc = subprocess.run(  # noqa: S603 — argv list, never a shell string
            [hermes_bin, "chat", "--help"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0 and "--create-if-missing" in (proc.stdout or "")


def _cli_supports_format_text(hermes_bin: str) -> bool:
    """Whether the installed Hermes CLI accepts ``chat --format``.

    Probed with ``chat --help`` (read-only, no session created). Older builds
    print usage to stderr and exit 2 on the unrecognised flag; argparse exits
    2 *before* any prompt is read, which a caller can only distinguish from a
    real task failure by checking the usage banner.
    """
    try:
        proc = subprocess.run(  # noqa: S603 — argv list, never a shell string
            [hermes_bin, "chat", "--help"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0 and "--format" in (proc.stdout or "")

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


def _to_text(chunk: bytes | str | None) -> str:
    """Decode partial subprocess output that arrives as raw bytes on timeout."""
    if chunk is None:
        return ""
    if isinstance(chunk, bytes):
        return chunk.decode(errors="replace")
    return chunk


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
    idle_recoveries: int = field(default=0, metadata={"internal": True})
    message_count: int | None = field(default=None, metadata={"internal": True})


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
        self._popen_factory = subprocess.Popen
        self._progress_reader = self._read_session_progress
        self._force_idle_watchdog = False
        self.supports_idle_watchdog = True

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
        """Fail-closed boolean wrapper used by the router's run path."""
        return self.probe_session_name(session_name) is True

    def probe_session_name(self, session_name: str) -> bool | None:
        """Return True/False when known, or None when existence is unknown.

        The CLI dry-run is read-only. If it fails or returns no match, consult
        the read-only session DB fallback. If both probes fail, return None so
        recovery callers do not confuse an inspection failure with a missing
        session. The run path remains fail-closed via session_name_exists().
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
            proc = self._runner(
                cmd,
                cwd=self.workdir,
                env=self._child_env(),
                capture_output=True,
                text=True,
                timeout=60,
            )
        except Exception as exc:  # pragma: no cover - defensive
            self._log(f"session existence CLI probe failed: {exc}")
            return self._session_title_exists_in_db(session_name)

        if proc.returncode != 0:
            self._log(
                f"session existence probe exit={proc.returncode}: "
                f"{(proc.stderr or proc.stdout or '').strip()[:200]}"
            )
            return self._session_title_exists_in_db(session_name)

        if _count_from_dry_run(proc.stdout) > 0:
            return True

        # CLI count 0 may be genuine absence or a build-specific filter issue;
        # use the read-only DB lookup as the final authority.
        return self._session_title_exists_in_db(session_name)

    def _session_title_exists_in_db(self, session_name: str) -> bool | None:
        """Read-only exact-title lookup; None means the database probe failed."""
        import sqlite3

        home = os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
        db = os.path.join(home, "state.db")
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            try:
                row = con.execute(
                    "SELECT 1 FROM sessions WHERE title = ? LIMIT 1",
                    (session_name,),
                ).fetchone()
                return row is not None
            finally:
                con.close()
        except Exception as exc:
            self._log(f"session db title lookup failed: {exc}")
            return None

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
        idle_timeout_s: int = 300,
        monitor_interval_s: float = 5.0,
        max_idle_recoveries: int = 2,
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

        if _NEW_SESSION_FLAGS_SUPPORTED is None:
            _set_new_session_flags_supported(
                _cli_supports_new_session_flags(self._bin)
            )
        new_cli = bool(_NEW_SESSION_FLAGS_SUPPORTED)

        # Probe whether the session already exists so we can decide which
        # creation-only flags to forward. On new-CLI builds --create-if-missing
        # handles create-vs-resume transparently, but creation-only overrides
        # like --run-budget must NOT be sent on a resume (the gateway freezes the
        # launch route and refuses differing creation overrides). --run-budget
        # is a creation-only override; omitting it on a resume is the root-cause
        # fix for the Oct-31 resume failure (Issue #64).
        session_exists = self.session_name_exists(session_name)
        is_fresh = not bool(session_exists)

        cmd = self._build_cmd(
            session_name=session_name,
            model=model,
            max_turns=max_turns,
            run_budget_s=run_budget_s,
            resume_by_name=bool(session_exists),
            is_fresh=is_fresh,
        )
        self._log(
            f"run session={session_name!r} model={model or 'default'} "
            f"max_turns={max_turns} budget={run_budget_s}s "
            f"workdir={self.workdir!s} resume_by_name={session_exists} "
            f"is_fresh={is_fresh}"
        )

        _PROMPT_HOLDER[0] = prompt
        started = time.monotonic()
        if (self._runner is _default_runner or self._force_idle_watchdog) and idle_timeout_s > 0:
            try:
                return self._run_with_idle_watchdog(
                    cmd=cmd,
                    session_name=session_name,
                    prompt=prompt,
                    model=model,
                    run_budget_s=run_budget_s,
                    idle_timeout_s=idle_timeout_s,
                    monitor_interval_s=monitor_interval_s,
                    max_idle_recoveries=max_idle_recoveries,
                    started=started,
                    session_exists=bool(session_exists),
                )
            finally:
                _PROMPT_HOLDER[0] = ""

        timed_out = False
        # Never run unbounded, even if the caller passed no budget: a session
        # with no ceiling can hang on a network read and wedge the whole
        # scheduler (observed 3600s timeout). Two minutes is the safety net; a
        # caller that wants longer passes an explicit budget.
        if run_budget_s is None:
            run_budget_s = 120
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
                # CPython attaches whatever the pipes had already buffered as
                # raw bytes even when text=True, so decode defensively — a
                # timeout must record a failed run, not crash the tick with a
                # str/bytes TypeError.
                stdout = _to_text(exc.output) + _to_text(exc.stderr)
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

        if not new_cli and not session_exists and session_id:
            # Name the freshly created session so the next run resumes it
            # instead of spawning another one.
            self._rename_session(session_id, session_name)

        return RunResult(
            session_id=session_id,
            exit_code=proc.returncode,
            stdout=stdout,
            duration_s=duration_s,
            resumed=_detect_resumed(stdout),
            timed_out=timed_out,
        )

    # ------------------------------------------------------------------ #
    # Progress-aware execution
    # ------------------------------------------------------------------ #
    def _read_session_progress(self, session_name: str, env: dict[str, str] | None = None):
        """Read the same message counter shown by the Hermes session UI.

        The probe is read-only and fail-closed: if the session store cannot be
        read, the watchdog never interrupts a possibly active model/tool call.
        """
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class Progress:
            session_id: str
            message_count: int
            last_activity_at: float
            ended_at: float | None
            end_reason: str | None

        env = env or self._child_env()
        home = Path(env.get("HERMES_HOME") or Path.home() / ".hermes")
        profile = env.get("HERMES_PROFILE") or env.get("HERMES_PROFILE_NAME")
        candidates = []
        if profile and profile not in ("default", "main"):
            candidates.append(home / "profiles" / profile / "state.db")
        candidates.append(home / "state.db")
        for db in candidates:
            if not db.is_file():
                continue
            try:
                con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=0.5)
                try:
                    row = con.execute(
                        """SELECT id, COALESCE(message_count, 0),
                                  COALESCE(last_activity_at, 0), ended_at, end_reason
                           FROM sessions WHERE title = ?
                           ORDER BY started_at DESC LIMIT 1""",
                        (session_name,),
                    ).fetchone()
                finally:
                    con.close()
                if row:
                    return Progress(
                        session_id=str(row[0]),
                        message_count=int(row[1] or 0),
                        last_activity_at=float(row[2] or 0),
                        ended_at=float(row[3]) if row[3] is not None else None,
                        end_reason=str(row[4]) if row[4] else None,
                    )
            except (sqlite3.Error, OSError) as exc:
                self._log(f"session progress probe unavailable for {session_name!r}: {exc}")
                return None
        return None

    @staticmethod
    def _idle_continuation_prompt(idle_seconds: int, recovery_number: int) -> str:
        return (
            "[DISPATCHER IDLE-WATCHDOG CONTINUATION]\n"
            f"The previous invocation produced no new Hermes session messages for {idle_seconds} seconds "
            "and did not publish a terminal result. This is a continuation of the SAME task, not a new task.\n"
            "Continue from the existing conversation, current repository workspace and existing changes. "
            "Do not reset, clean, recreate, or discard the workspace. First inspect the current git status "
            "and diff, then continue the original acceptance criteria without repeating completed work.\n"
            "If implementation is complete, finish the required focused validation, commit and push the "
            "changes, independently fetch the remote branch/file state, and report the full commit SHA. "
            "If not complete, make the next concrete in-scope change. If blocked, preserve all work and "
            "report the exact blocker and remaining criterion. Do not claim DONE without durable evidence.\n"
            f"Watchdog continuation {recovery_number}; keep the original task scope and safety boundaries."
        )

    def _run_with_idle_watchdog(
        self, *, cmd: list[str], session_name: str, prompt: str,
        model: str | None, run_budget_s: int | None, idle_timeout_s: int,
        monitor_interval_s: float, max_idle_recoveries: int,
        started: float, session_exists: bool,
    ) -> RunResult:
        """Run Hermes while watching its persisted message_count/last_activity.

        A five-minute quiet session is nudged by interrupting the stalled CLI
        turn and resuming the same named session with a continuation prompt.
        A hard wall-clock ceiling remains as the final safety boundary.
        """
        hard_budget = float(run_budget_s) if run_budget_s else 120.0
        deadline = started + max(hard_budget, 1.0)
        interval = max(0.25, float(monitor_interval_s or 5.0))
        recoveries = 0
        outputs: list[str] = []
        last_progress = time.monotonic()
        last_count: int | None = None
        last_activity: float | None = None
        latest_session_id = ""
        current_prompt = prompt
        current_cmd = list(cmd)
        current_resumed = session_exists

        while True:
            recoveries_before_invocation = recoveries
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return RunResult(
                    session_id=latest_session_id, exit_code=124,
                    stdout="".join(outputs), duration_s=time.monotonic() - started,
                    resumed=current_resumed, timed_out=True,
                    idle_recoveries=recoveries,
                    message_count=last_count,
                )
            try:
                proc = self._popen_factory(
                    current_cmd, cwd=self.workdir, env=self._child_env(),
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                    errors="replace", bufsize=1,
                )
            except OSError as exc:
                raise RouterError(f"could not start Hermes session {session_name!r}: {exc}") from exc

            invocation_started = time.monotonic()
            snapshot = self._progress_reader(session_name, self._child_env())
            if snapshot:
                latest_session_id = snapshot.session_id or latest_session_id
                last_count = snapshot.message_count
                last_activity = snapshot.last_activity_at or None
            last_progress = invocation_started
            first_communicate = True
            invocation_output = ""
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._interrupt_process(proc)
                    try:
                        invocation_output, _ = proc.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        self._stop_process(proc)
                        invocation_output, _ = proc.communicate()
                    outputs.append(invocation_output or "")
                    return RunResult(
                        session_id=latest_session_id or _parse_session_id("".join(outputs)),
                        exit_code=124, stdout="".join(outputs),
                        duration_s=time.monotonic() - started,
                        resumed=current_resumed, timed_out=True,
                        idle_recoveries=recoveries, message_count=last_count,
                    )

                try:
                    invocation_output, _ = proc.communicate(
                        input=current_prompt if first_communicate else None,
                        timeout=min(interval, remaining),
                    )
                    break
                except subprocess.TimeoutExpired:
                    first_communicate = False
                    current = self._progress_reader(session_name, self._child_env())
                    if current:
                        latest_session_id = current.session_id or latest_session_id
                        changed = (
                            last_count is None
                            or current.message_count != last_count
                            or (last_activity is not None and current.last_activity_at > last_activity)
                        )
                        if changed:
                            last_progress = time.monotonic()
                            last_count = current.message_count
                            last_activity = current.last_activity_at
                        if current.ended_at is not None:
                            # Hermes has finalized the session; wait for the CLI
                            # to flush its final result instead of sending a nudge.
                            last_progress = time.monotonic()
                            continue

                    if time.monotonic() - last_progress < idle_timeout_s:
                        continue

                    # Re-read immediately before interrupting to close the race
                    # where a message arrived between the previous poll and the
                    # idle threshold.
                    confirm = self._progress_reader(session_name, self._child_env())
                    if not confirm or confirm.ended_at is not None:
                        last_progress = time.monotonic()
                        continue
                    if (last_count is None or confirm.message_count != last_count
                            or (last_activity is not None and confirm.last_activity_at > last_activity)):
                        last_count = confirm.message_count
                        last_activity = confirm.last_activity_at
                        last_progress = time.monotonic()
                        continue

                    if recoveries >= max(0, int(max_idle_recoveries)):
                        self._log(
                            f"session={session_name!r} idle watchdog reached {recoveries} continuations; "
                            "stopping at the safety limit"
                        )
                        self._interrupt_process(proc)
                        try:
                            invocation_output, _ = proc.communicate(timeout=5)
                        except subprocess.TimeoutExpired:
                            self._stop_process(proc)
                            invocation_output, _ = proc.communicate()
                        outputs.append(invocation_output or "")
                        return RunResult(
                            session_id=latest_session_id or confirm.session_id,
                            exit_code=124, stdout="".join(outputs),
                            duration_s=time.monotonic() - started,
                            resumed=current_resumed, timed_out=True,
                            idle_recoveries=recoveries, message_count=last_count,
                        )

                    self._log(
                        f"session={session_name!r} idle for {idle_timeout_s}s at "
                        f"message_count={confirm.message_count}; sending continuation {recoveries + 1}"
                    )
                    self._interrupt_process(proc)
                    try:
                        invocation_output, _ = proc.communicate(timeout=15)
                    except subprocess.TimeoutExpired:
                        self._stop_process(proc)
                        invocation_output, _ = proc.communicate()
                    outputs.append(invocation_output or "")
                    recoveries += 1
                    current_prompt = self._idle_continuation_prompt(idle_timeout_s, recoveries)
                    current_cmd = self._build_cmd(
                        session_name=session_name, model=model, max_turns=None,
                        run_budget_s=None, resume_by_name=True, is_fresh=False,
                    )
                    current_resumed = True
                    last_progress = time.monotonic()
                    break

            if recoveries > recoveries_before_invocation:
                # The previous process was interrupted and its output already
                # captured. Start the continuation in the same named session.
                continue

            outputs.append(invocation_output or "")
            parsed_id = _parse_session_id(invocation_output or "")
            if parsed_id:
                latest_session_id = parsed_id
            if proc.returncode == 0 and not latest_session_id:
                raise RouterError(
                    f"hermes run for session {session_name!r} produced no session_id line "
                    "and the session store had no matching row"
                )
            if not _NEW_SESSION_FLAGS_SUPPORTED and not session_exists and latest_session_id:
                self._rename_session(latest_session_id, session_name)
            return RunResult(
                session_id=latest_session_id, exit_code=int(proc.returncode or 0),
                stdout="".join(outputs), duration_s=time.monotonic() - started,
                resumed=current_resumed or _detect_resumed("".join(outputs)),
                timed_out=False, idle_recoveries=recoveries,
                message_count=last_count,
            )

    @staticmethod
    def _interrupt_process(proc) -> None:
        try:
            proc.send_signal(signal.SIGINT)
        except (OSError, ProcessLookupError):
            pass

    @staticmethod
    def _stop_process(proc) -> None:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

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
        resume_by_name: bool = True,
        is_fresh: bool = True,
    ) -> list[str]:
        if _NEW_SESSION_FLAGS_SUPPORTED is None:
            _set_new_session_flags_supported(
                _cli_supports_new_session_flags(self._bin)
            )
        new_cli = bool(_NEW_SESSION_FLAGS_SUPPORTED)

        cmd: list[str] = [
            self._bin,
            "chat",
        ]
        if new_cli:
            # Creates the named session on first run, resumes it afterwards.
            cmd += ["--continue", session_name, "--create-if-missing"]
        elif resume_by_name:
            # v0.19.x: --continue only resumes; a name that does not exist is
            # a hard error, so it is only used when the session is known to
            # exist (the caller probes first).
            cmd += ["--continue", session_name]
        cmd += ["-Q"]
        # ``--format text`` exists only on Hermes >= 0.21.5 (upstream 158fd638).
        # The server runtime at takeover is 0.21.3 (40f2702b); emitting the flag
        # there dies in argparse before the task ever runs, which is how a
        # pre-execution CLI error came to be recorded as a task FAILED.
        # It is a pure output-preference flag, so probe once per process and
        # skip it when the installed CLI does not accept it.
        if _FORMAT_TEXT_SUPPORTED is None:
            _set_format_text_supported(_cli_supports_format_text(self._bin))
        if _FORMAT_TEXT_SUPPORTED:
            cmd += ["--format", "text"]
        if new_cli:
            cmd += ["--query-file", "-"]
        else:
            # v0.19.x reads the prompt from stdin via ``-q -``.
            cmd += ["-q", "-"]
        if self.workdir is not None:
            if new_cli:
                cmd += ["--in", str(self.workdir)]
            # v0.19.x has no --in; the child inherits the dispatcher's cwd.
        if model:
            cmd += ["--model", model]
        if max_turns is not None:
            cmd += ["--max-turns", str(int(max_turns))]
        # NB: --run-budget is deliberately NEVER sent (v0.22.x, 2026-11-03 fix).
        # The CLI's own _resolve_continue_arg turns `-c <name> --create-if-missing`
        # into a session.create (frozen launch_request WITHOUT the creation flags)
        # followed by a RESUME of that brand-new session, so check_resume_policy
        # refuses any --run-budget mismatch — even on a fresh name (verified by
        # repro on gglvp-migration-01). run_budget_s is still enforced by the
        # caller as the subprocess timeout. The Oct-31 "creation-only" condition
        # (new_cli and is_fresh) did not help because is_fresh is computed from
        # the session-title probe, which is a false negative whenever the store
        # title differs from the session name — and even when it is correct, the
        # CLI's internal create-then-resume path still trips the policy check.
        return cmd

    def _rename_session(self, session_id: str, title: str) -> None:
        """Give a freshly created session its task name (v0.19.x path only).

        On builds without ``--create-if-missing`` a session is created unnamed;
        naming it lets the next run resume it by title.
        """
        cmd = [self._bin, "sessions", "rename", session_id, title]
        try:
            proc = self._runner(
                cmd,
                cwd=self.workdir,
                env=self._child_env(),
                capture_output=True,
                text=True,
                timeout=60,
            )
            if proc.returncode != 0:
                self._log(
                    f"sessions rename {session_id} -> {title!r} exit="
                    f"{proc.returncode}: "
                    f"{(proc.stderr or proc.stdout or '').strip()[:200]}"
                )
        except Exception as exc:  # renaming is a convenience, not a gate
            self._log(f"sessions rename {session_id} -> {title!r} failed: {exc}")

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
