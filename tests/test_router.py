"""Tests for dispatcher/router.py — Lane B.

No network, no real ``hermes`` process. A fake runner stands in for
subprocess.run and records every invocation.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from dispatcher.router import (
    RunResult,
    RouterError,
    SessionRouter,
    _count_from_dry_run,
    _detect_resumed,
    _parse_session_id,
)

RESUME_STDOUT = (
    "↻ Resumed session 20261005_030008_17da54 \"probe-new-session-001\" "
    "(2 user messages, 4 total messages)\r\n"
    "PONG\r\n"
    "\r\n"
    "session_id: 20261005_030008_17da54\r\n"
)

FRESH_STDOUT = (
    "Session 20261005_031844_e29272 found but has no messages. "
    "Starting fresh.\r\n"
    "FRESH\r\n"
    "\r\n"
    "session_id: 20261005_031844_e29272\r\n"
)

EMPTY_BOTH_STDOUT = (
    "↻ Resumed session 20261005_031844_e29272 \"probe\" "
    "(0 user messages, 0 total messages)\r\n"
    "Session 20261005_031844_e29272 found but has no messages. "
    "Starting fresh.\r\n"
    "\r\n"
    "session_id: 20261005_031844_e29272\r\n"
)


class FakeRunner:
    """Stands in for subprocess.run; records calls and replies canned output."""

    def __init__(self, *, stdout: str = "", stderr: str = "",
                 returncode: int = 0, raise_exc: BaseException | None = None):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.raise_exc = raise_exc
        self.calls: list[dict] = []
        self.stdin_inputs: list[str] = []
        self.timeouts: list[float] = []

    def __call__(self, cmd, *, cwd, env, capture_output, text, timeout):
        self.calls.append(
            {
                "cmd": list(cmd),
                "cwd": cwd,
                "env": env,
                "capture_output": capture_output,
                "text": text,
                "timeout": timeout,
            }
        )
        # Record the prompt the router piped in via --query-file -.
        if "--query-file" in cmd and "-" in cmd:
            import dispatcher.router as r

            self.stdin_inputs.append(r._PROMPT_HOLDER[0])
        self.timeouts.append(timeout)
        if self.raise_exc is not None:
            raise self.raise_exc
        return subprocess.CompletedProcess(
            args=list(cmd),
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
        )


def make_router(runner: FakeRunner, **kw) -> SessionRouter:
    router = SessionRouter(**kw)
    router._set_runner(runner)
    return router


# ---------------------------------------------------------------------- #
# Command construction
# ---------------------------------------------------------------------- #


def test_run_uses_verified_routing_primitive():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    router.run(session_name="smoke-1", prompt="Do the thing.")
    cmd = runner.calls[0]["cmd"]
    assert cmd[1] == "chat"
    for needle in (
        "--continue", "smoke-1",
        "--create-if-missing",
        "-Q",
        "--format", "text",
    ):
        assert needle in cmd
    assert "--query-file" in cmd and "-" in cmd


def test_prompt_travels_via_stdin_never_the_command_line():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    secret_ish = "key='value' $(rm -rf /) `whoami` \"quoted\""
    router.run(session_name="smoke-1", prompt=secret_ish)
    cmd = runner.calls[0]["cmd"]
    assert secret_ish not in cmd
    assert runner.stdin_inputs == [secret_ish]


def test_workdir_becomes_in_flag():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner, workdir=Path("/some/where"))
    router.run(session_name="smoke-1", prompt="hi")
    cmd = runner.calls[0]["cmd"]
    i = cmd.index("--in")
    assert cmd[i + 1] == str(Path("/some/where"))


def test_no_workdir_means_no_in_flag():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    router.run(session_name="smoke-1", prompt="hi")
    assert "--in" not in runner.calls[0]["cmd"]


def test_optional_flags_only_when_supplied():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    router.run(
        session_name="smoke-1",
        prompt="hi",
        model="anthropic/claude-sonnet-4",
        max_turns=12,
        run_budget_s=900,
    )
    cmd = runner.calls[0]["cmd"]
    assert cmd[cmd.index("--model") + 1] == "anthropic/claude-sonnet-4"
    assert cmd[cmd.index("--max-turns") + 1] == "12"
    assert cmd[cmd.index("--run-budget") + 1] == "900"


def test_no_optional_flags_by_default():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    router.run(session_name="smoke-1", prompt="hi")
    cmd = runner.calls[0]["cmd"]
    for flag in ("--model", "--max-turns", "--run-budget"):
        assert flag not in cmd


# ---------------------------------------------------------------------- #
# RunResult parsing
# ---------------------------------------------------------------------- #


def test_resume_path_reports_resumed_true():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    res = router.run(session_name="probe-new-session-001", prompt="ping")
    assert isinstance(res, RunResult)
    assert res.session_id == "20261005_030008_17da54"
    assert res.exit_code == 0
    assert res.resumed is True
    assert res.duration_s >= 0.0


def test_fresh_path_reports_resumed_false():
    runner = FakeRunner(stdout=FRESH_STDOUT)
    router = make_router(runner)
    res = router.run(session_name="probe-new-session-002", prompt="ping")
    assert res.session_id == "20261005_031844_e29272"
    assert res.resumed is False


def test_empty_session_prints_both_markers_and_counts_as_fresh():
    # Verified on host: an existing-but-empty session prints BOTH the resume
    # line and "Starting fresh."; the CLI starts fresh, so resumed must be
    # False.
    runner = FakeRunner(stdout=EMPTY_BOTH_STDOUT)
    router = make_router(runner)
    res = router.run(session_name="probe", prompt="ping")
    assert res.resumed is False
    assert res.session_id == "20261005_031844_e29272"


def test_nonzero_exit_code_is_reported_not_raised():
    runner = FakeRunner(stdout="boom\nsession_id: abc123\n", returncode=1)
    router = make_router(runner)
    res = router.run(session_name="smoke-1", prompt="hi")
    assert res.exit_code == 1
    assert res.session_id == "abc123"
    assert "boom" in res.stdout


def test_missing_session_id_on_clean_exit_raises():
    runner = FakeRunner(stdout="no id anywhere", returncode=0)
    router = make_router(runner)
    with pytest.raises(RouterError):
        router.run(session_name="smoke-1", prompt="hi")


def test_missing_session_id_on_failure_yields_empty_not_raise():
    runner = FakeRunner(stdout="crash", returncode=2)
    router = make_router(runner)
    res = router.run(session_name="smoke-1", prompt="hi")
    assert res.session_id == ""
    assert res.exit_code == 2


def test_empty_session_name_rejected():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    with pytest.raises(RouterError):
        router.run(session_name="", prompt="hi")
    assert runner.calls == []


def test_empty_prompt_rejected():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    with pytest.raises(RouterError):
        router.run(session_name="smoke-1", prompt="")
    assert runner.calls == []


# ---------------------------------------------------------------------- #
# Timeout
# ---------------------------------------------------------------------- #


def test_run_budget_passed_as_timeout_and_handled():
    partial = "in-flight output\nsession_id: partial1\n"
    runner = FakeRunner(
        raise_exc=subprocess.TimeoutExpired(
            cmd=["hermes", "chat"], timeout=900, output=partial, stderr=""
        )
    )
    router = make_router(runner)
    res = router.run(session_name="smoke-1", prompt="hi", run_budget_s=900)
    assert runner.timeouts[0] == 900.0
    assert res.timed_out is True
    assert res.exit_code == 124
    assert res.session_id == "partial1"
    assert res.resumed is False
    assert "in-flight output" in res.stdout


def test_run_without_budget_still_bounded():
    # A run with no explicit budget must never be unbounded — that is how a
    # session hang once consumed the full 3600s scheduler timeout. The router
    # installs a safety net instead of passing timeout=None.
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    router.run(session_name="smoke-1", prompt="hi")
    assert runner.timeouts[0] is not None
    assert runner.timeouts[0] > 0


# ---------------------------------------------------------------------- #
# Environment / token hygiene
# ---------------------------------------------------------------------- #


def test_github_token_inherited_not_on_command_line(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_SUPERSECRET")
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    router.run(session_name="smoke-1", prompt="hi")
    env = runner.calls[0]["env"]
    assert env["GITHUB_TOKEN"] == "ghp_SUPERSECRET"
    cmd_str = " ".join(runner.calls[0]["cmd"])
    assert "ghp_SUPERSECRET" not in cmd_str


def test_extra_env_merged(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_X")
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner, extra_env={"DISPATCHER_TASK": "T-1"})
    router.run(session_name="smoke-1", prompt="hi")
    env = runner.calls[0]["env"]
    assert env["DISPATCHER_TASK"] == "T-1"
    assert env["GITHUB_TOKEN"] == "ghp_X"


def test_prompt_cleared_after_run():
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner)
    router.run(session_name="smoke-1", prompt="secret-ish prompt")
    import dispatcher.router as r

    assert r._PROMPT_HOLDER[0] == ""


def test_logger_sees_invocation_metadata_not_prompts():
    seen: list[str] = []
    runner = FakeRunner(stdout=RESUME_STDOUT)
    router = make_router(runner, logger=seen.append)
    router.run(session_name="smoke-1", prompt="BODY-MUST-NOT-LEAK")
    assert seen, "logger should have been called"
    assert "smoke-1" in seen[0]
    assert all("BODY-MUST-NOT-LEAK" not in m for m in seen)


# ---------------------------------------------------------------------- #
# session_name_exists
# ---------------------------------------------------------------------- #


def test_exists_true_when_dry_run_reports_one():
    runner = FakeRunner(
        stdout="Would export 1 session(s) (title contains 'smoke-1').\r\n"
    )
    router = make_router(runner)
    assert router.session_name_exists("smoke-1") is True
    cmd = runner.calls[0]["cmd"]
    assert cmd[1:4] == ["sessions", "export", "--format"]
    assert "--dry-run" in cmd
    assert cmd[cmd.index("--title") + 1] == "smoke-1"


def test_exists_false_when_dry_run_reports_zero():
    runner = FakeRunner(stdout="Would export 0 session(s) (title contains 'nope').\r\n")
    router = make_router(runner)
    assert router.session_name_exists("nope") is False


def test_exists_is_read_only_and_side_effect_free():
    """The chosen probe must not create, write or delete anything."""
    runner = FakeRunner(
        stdout="Would export 0 session(s) (title contains 'x').\r\n", stderr=""
    )
    router = make_router(runner)
    router.session_name_exists("x")
    cmd = runner.calls[0]["cmd"]
    assert "export" in cmd
    assert "--dry-run" in cmd
    assert "delete" not in " ".join(cmd)
    assert "chat" not in " ".join(cmd)


def test_exists_fails_closed_on_nonzero_exit():
    runner = FakeRunner(stdout="", stderr="boom", returncode=1)
    router = make_router(runner)
    assert router.session_name_exists("smoke-1") is False


def test_exists_fails_closed_on_exception():
    runner = FakeRunner(raise_exc=OSError("no such binary"))
    router = make_router(runner)
    assert router.session_name_exists("smoke-1") is False


def test_exists_tolerates_crlf_and_box_whitespace():
    runner = FakeRunner(
        stdout="Would export 2 session(s) (title contains 'smoke').\r\n"
    )
    router = make_router(runner)
    assert router.session_name_exists("smoke") is True


def test_probing_does_not_use_chat_continue():
    """The rejected alternative: `chat --continue` would create the session."""
    runner = FakeRunner(stdout="Would export 0 session(s).\r\n")
    router = make_router(runner)
    router.session_name_exists("maybe-new")
    assert "chat" not in " ".join(runner.calls[0]["cmd"])


# ---------------------------------------------------------------------- #
# Pure helpers
# ---------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Would export 0 session(s) (title contains 'x').", 0),
        ("Would export 1 session(s) (title contains 'x').", 1),
        ("Would export 12 session(s) (title contains 'x').", 12),
        ("Exported 3 sessions to ./out.jsonl", 3),
        ("", 0),
        ("garbage", 0),
    ],
)
def test_count_from_dry_run(text, expected):
    assert _count_from_dry_run(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (RESUME_STDOUT, "20261005_030008_17da54"),
        (FRESH_STDOUT, "20261005_031844_e29272"),
        ("session_id:abc", "abc"),
        ("no id", ""),
        ("", ""),
    ],
)
def test_parse_session_id(text, expected):
    assert _parse_session_id(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (RESUME_STDOUT, True),
        (FRESH_STDOUT, False),
        (EMPTY_BOTH_STDOUT, False),
        ("", False),
        ("Resumed session xyz", True),
    ],
)
def test_detect_resumed(text, expected):
    assert _detect_resumed(text) is expected


def test_arrow_strip_does_not_flip_detection():
    # A locale that drops the arrow must not turn a resume into a fresh start.
    text = RESUME_STDOUT.replace("↻", "")
    assert _detect_resumed(text) is True


def test_runresult_defaults():
    r = RunResult(session_id="x", exit_code=0, stdout="s", duration_s=1.0)
    assert r.resumed is False
    assert r.timed_out is False
