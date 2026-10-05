"""Tests for dispatcher/github.py — Lane B.

No network, no real git. A fake runner stands in for subprocess.run; a real
temporary directory holds the inbox/ledger files so the file-IO paths are
exercised against a real filesystem.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from dispatcher.github import ControlRepo, ControlRepoError

HERMES = "hermes"

SAMPLE_INBOX = """# Machine-readable task inbox. Dispatcher owns status transitions.
[inbox]
version = 1

[[task]]
id = "T-2026-10-05-001"
title = "Smoke run"
status = "READY"   # authoring state
priority = 100
workstream = "smoke"

[[task]]
id = "T-2026-10-05-002"
title = "Second"
status = "RUNNING"
priority = 50

[[task]]
id = "T-2026-10-05-003"
title = "Third"
status = "READY"
"""


class FakeRunner:
    """Stands in for subprocess.run; replays a canned git script."""

    def __init__(self, script: dict | None = None):
        # script: tuple-key -> (returncode, stdout, stderr). Key is
        # "arg1|arg2|..." of the git argv (excluding the leading "git").
        self.script = {k: v for k, v in (script or {}).items()}
        self.calls: list[dict] = []

    def __call__(self, cmd, *, cwd, env, capture_output, text, check):
        self.calls.append(
            {
                "cmd": list(cmd),
                "cwd": cwd,
                "env": dict(env),
                "check": check,
            }
        )
        key = "|".join(cmd[1:])
        if key in self.script:
            rc, out, err = self.script[key]
        else:
            rc, out, err = 0, "", ""
        if rc != 0 and check:
            raise subprocess.CalledProcessError(
                rc, cmd, output=out, stderr=err
            )
        return subprocess.CompletedProcess(
            args=list(cmd), returncode=rc, stdout=out, stderr=err
        )


class ScriptBuilder:
    """Fluent helper for building a fake git script."""

    def __init__(self):
        self.script: dict[str, tuple[int, str, str]] = {}

    def add(self, argv: list[str], stdout: str = "", rc: int = 0,
            stderr: str = "") -> "ScriptBuilder":
        self.script["|".join(argv)] = (rc, stdout, stderr)
        return self

    def branch(self, name: str = "main") -> "ScriptBuilder":
        return self.add(["rev-parse", "--abbrev-ref", "HEAD"], stdout=name + "\n")

    def head(self, sha: str) -> "ScriptBuilder":
        return self.add(["rev-parse", "HEAD"], stdout=sha + "\n")

    def ok(self, *argv: str, rc: int = 0) -> "ScriptBuilder":
        return self.add(list(argv), rc=rc)

    def fail(self, argv: list[str], stderr: str = "boom") -> "ScriptBuilder":
        return self.add(argv, rc=1, stderr=stderr)

    def build(self) -> FakeRunner:
        return FakeRunner(self.script)


def write_inbox(repo, text: str = SAMPLE_INBOX) -> None:
    """Write the inbox. Binary mode: never translate newlines."""
    (repo / HERMES).mkdir(parents=True, exist_ok=True)
    (repo / HERMES / "tasks_inbox.toml").open("wb").write(
        text.encode("utf-8")
    )


def ledger_path(repo) -> str:
    return str(repo / HERMES / "dispatch_outcomes.jsonl")


def read_ledger(repo) -> list[dict]:
    path = repo / HERMES / "dispatch_outcomes.jsonl"
    if not path.is_file():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def make_repo(repo, script: ScriptBuilder | None = None, logger=None) -> ControlRepo:
    cr = ControlRepo(repo, logger=logger)
    cr._set_runner((script or ScriptBuilder()).build())
    return cr


@pytest.fixture
def repo(tmp_path):
    d = tmp_path / "control"
    d.mkdir()
    return d


# ---------------------------------------------------------------------- #
# Reads
# ---------------------------------------------------------------------- #


def test_remote_url(repo):
    cr = make_repo(repo, ScriptBuilder().add(
        ["remote", "get-url", "origin"],
        stdout="https://github.com/h4z4rd95/Plan-Project-Situation.git\n",
    ))
    assert cr.remote_url() == "https://github.com/h4z4rd95/Plan-Project-Situation.git"


def test_head_sha(repo):
    cr = make_repo(repo, ScriptBuilder().head("a" * 40))
    assert cr.head_sha() == "a" * 40


def test_inbox_path(repo):
    cr = make_repo(repo)
    assert cr.inbox_path() == repo / HERMES / "tasks_inbox.toml"


def test_read_inbox_returns_text(repo):
    write_inbox(repo)
    assert "T-2026-10-05-001" in make_repo(repo).read_inbox()


def test_read_inbox_empty_when_absent(repo):
    assert make_repo(repo).read_inbox() == ""


def test_git_failure_wrapped(repo):
    cr = make_repo(repo, ScriptBuilder().fail(["rev-parse", "HEAD"]))
    with pytest.raises(ControlRepoError):
        cr.head_sha()


def test_git_failure_message_does_not_leak_token(repo, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_SECRETVALUE")
    cr = make_repo(repo, ScriptBuilder().fail(["rev-parse", "HEAD"], stderr="x"))
    with pytest.raises(ControlRepoError) as ei:
        cr.head_sha()
    assert "ghp_SECRETVALUE" not in str(ei.value)


def test_env_has_no_prompt_and_blocks_credential_prompt(repo, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_T")
    cr = make_repo(repo, ScriptBuilder().head("a" * 40))
    cr.head_sha()
    env = cr._runner.calls[-1]["env"]
    assert env["GITHUB_TOKEN"] == "ghp_T"
    assert env["GIT_TERMINAL_PROMPT"] == "0"


def test_argv_is_a_list_not_a_shell_string(repo):
    cr = make_repo(repo, ScriptBuilder().head("a" * 40))
    cr.head_sha()
    assert isinstance(cr._runner.calls[-1]["cmd"], list)
    assert cr._runner.calls[-1]["cmd"][0] == "git"


# ---------------------------------------------------------------------- #
# Sync
# ---------------------------------------------------------------------- #


def test_sync_fetches_and_ff_merges_then_returns_head(repo):
    sha = "f" * 40
    cr = make_repo(
        repo,
        ScriptBuilder()
        .ok("fetch", "--quiet", "origin")
        .ok("merge", "--ff-only", "--quiet", "origin/HEAD")
        .head(sha),
    )
    assert cr.sync() == sha
    cmds = [c["cmd"][1:] for c in cr._runner.calls]
    assert cmds == [
        ["fetch", "--quiet", "origin"],
        ["merge", "--ff-only", "--quiet", "origin/HEAD"],
        ["rev-parse", "HEAD"],
    ]


def test_sync_is_fetch_then_ffonly(repo):
    cr = make_repo(
        repo,
        ScriptBuilder().ok("fetch", "--quiet", "origin")
        .ok("merge", "--ff-only", "--quiet", "origin/HEAD").head("x" * 40),
    )
    cr.sync()
    cmds = [c["cmd"][1:] for c in cr._runner.calls]
    assert "rebase" not in " ".join(sum(cmds, []))


def test_sync_non_ff_raises(repo):
    cr = make_repo(
        repo,
        ScriptBuilder()
        .ok("fetch", "--quiet", "origin")
        .fail(["merge", "--ff-only", "--quiet", "origin/HEAD"], stderr="not a fast-forward"),
    )
    with pytest.raises(ControlRepoError, match="not a fast-forward"):
        cr.sync()


# ---------------------------------------------------------------------- #
# write_outcome — ledger
# ---------------------------------------------------------------------- #


def test_write_outcome_appends_one_ledger_line_and_commits(repo):
    write_inbox(repo)
    sha = "abc1234"
    cr = make_repo(
        repo,
        ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        .ok("commit", "--quiet", "-m").head(sha),
    )
    out = cr.write_outcome(
        task_id="T-2026-10-05-001",
        status="DONE",
        summary="all good",
        evidence=[{"path": "a.md"}],
        commit_sha="deadbee",
    )
    assert out == sha
    rows = read_ledger(repo)
    assert len(rows) == 1
    r0 = rows[0]
    assert r0["task_id"] == "T-2026-10-05-001"
    assert r0["status"] == "DONE"
    assert r0["summary"] == "all good"
    assert r0["evidence"] == [{"path": "a.md"}]
    assert r0["commit_sha"] == "deadbee"
    assert r0["ts"]
    assert "blocker" not in r0
    assert "owner_action" not in r0


def test_write_outcome_optional_fields_present_when_given(repo):
    write_inbox(repo)
    cr = make_repo(
        repo,
        ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        .ok("commit", "--quiet", "-m").head("z"),
    )
    cr.write_outcome(
        task_id="T-2026-10-05-001",
        status="FAILED",
        summary="nope",
        evidence=[],
        blocker="missing key",
        owner_action="rotate key",
    )
    r0 = read_ledger(repo)[0]
    assert r0["blocker"] == "missing key"
    assert r0["owner_action"] == "rotate key"
    assert "commit_sha" not in r0


def test_write_outcome_is_idempotent_for_terminal_status(repo):
    write_inbox(repo)
    sb = ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
    sb.ok("commit", "--quiet", "-m").head("c1")
    cr = make_repo(repo, sb)
    first = cr.write_outcome(
        task_id="T-2026-10-05-001", status="DONE", summary="s", evidence=[]
    )
    # Second call: terminal outcome already in the ledger AND the inbox
    # already DONE -> nothing staged -> no commit -> no git calls at all.
    second = cr.write_outcome(
        task_id="T-2026-10-05-001", status="DONE", summary="s", evidence=[]
    )
    assert first == "c1"
    assert second is None
    assert len(read_ledger(repo)) == 1
    # The no-op must not have consulted git a second time.
    assert len(cr._runner.calls) == 4


def test_write_outcome_retry_after_failure_appends_new_status(repo):
    """FAILED then DONE for the same task are two distinct ledger lines."""
    write_inbox(repo)
    sb = ScriptBuilder()
    for _ in range(2):
        sb.ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        sb.ok("commit", "--quiet", "-m")
    sb.head("c2")
    cr = make_repo(repo, sb)
    cr.write_outcome(task_id="T-1", status="FAILED", summary="f", evidence=[])
    cr.write_outcome(task_id="T-1", status="DONE", summary="d", evidence=[])
    rows = read_ledger(repo)
    assert [r["status"] for r in rows] == ["FAILED", "DONE"]


def test_write_outcome_non_terminal_always_appends(repo):
    """RUNNING is not deduped: a task visits it once per attempt."""
    write_inbox(repo)
    sb = ScriptBuilder()
    for _ in range(2):
        sb.ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        sb.ok("commit", "--quiet", "-m")
    sb.head("c2")
    cr = make_repo(repo, sb)
    cr.write_outcome(task_id="T-1", status="RUNNING", summary="r1", evidence=[])
    cr.write_outcome(task_id="T-1", status="RUNNING", summary="r2", evidence=[])
    rows = read_ledger(repo)
    assert len(rows) == 2
    assert [r["summary"] for r in rows] == ["r1", "r2"]


def test_write_outcome_requires_task_id(repo):
    cr = make_repo(repo)
    with pytest.raises(ControlRepoError):
        cr.write_outcome(task_id="", status="DONE", summary="", evidence=[])


def test_write_outcome_requires_status(repo):
    cr = make_repo(repo)
    with pytest.raises(ControlRepoError):
        cr.write_outcome(task_id="T-1", status="", summary="", evidence=[])


def test_write_outcome_commit_message_names_task_and_status(repo):
    write_inbox(repo)
    sb = ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
    sb.ok("commit", "--quiet", "-m").head("h")
    cr = make_repo(repo, sb)
    cr.write_outcome(
        task_id="T-2026-10-05-001", status="DONE", summary="Fix the thing",
        evidence=[],
    )
    msg = cr._runner.calls[-2]["cmd"][-1]
    assert msg == "[dispatcher] T-2026-10-05-001 -> DONE: Fix the thing"


def test_write_outcome_truncates_long_summary_in_message(repo):
    write_inbox(repo)
    sb = ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
    sb.ok("commit", "--quiet", "-m").head("h")
    cr = make_repo(repo, sb)
    cr.write_outcome(
        task_id="T-1", status="DONE", summary="x" * 300, evidence=[]
    )
    msg = cr._runner.calls[-2]["cmd"][-1]
    assert len(msg.split(": ", 1)[1]) <= 100


def test_write_outcome_without_inbox_still_writes_ledger_and_commits(repo):
    sb = ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
    sb.ok("commit", "--quiet", "-m").head("h")
    cr = make_repo(repo, sb)
    out = cr.write_outcome(
        task_id="T-9", status="DONE", summary="s", evidence=[]
    )
    assert out == "h"
    assert len(read_ledger(repo)) == 1


def test_write_outcome_returns_none_when_nothing_changed(repo):
    """Terminal outcome already in the ledger AND status unchanged."""
    write_inbox(repo)
    # First call records it.
    sb = ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
    sb.ok("commit", "--quiet", "-m").head("h1")
    cr = make_repo(repo, sb)
    cr.write_outcome(task_id="T-2026-10-05-001", status="DONE", summary="s", evidence=[])
    # Ledger already has DONE; inbox already DONE -> nothing to do, no git.
    cr2 = make_repo(repo)
    assert cr2.write_outcome(
        task_id="T-2026-10-05-001", status="DONE", summary="s", evidence=[]
    ) is None
    assert cr2._runner.calls == []


def test_write_outcome_escapes_no_shell_metacharacters(repo):
    write_inbox(repo)
    sb = ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
    sb.ok("commit", "--quiet", "-m").head("h")
    cr = make_repo(repo, sb)
    cr.write_outcome(
        task_id="T-1",
        status="DONE",
        summary="a'; rm -rf /; echo 'b",
        evidence=[{"x": "$(whoami)"}],
    )
    line = (repo / HERMES / "dispatch_outcomes.jsonl").read_text()
    assert "rm -rf /" in line  # preserved as data, not executed
    assert read_ledger(repo)[0]["summary"] == "a'; rm -rf /; echo 'b"


def test_write_outcome_handles_unicode(repo):
    write_inbox(repo)
    sb = ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
    sb.ok("commit", "--quiet", "-m").head("h")
    cr = make_repo(repo, sb)
    cr.write_outcome(
        task_id="T-1", status="DONE", summary="انجام شد ✓", evidence=[]
    )
    assert read_ledger(repo)[0]["summary"] == "انجام شد ✓"


def test_corrupt_ledger_raises(repo):
    write_inbox(repo)
    (repo / HERMES / "dispatch_outcomes.jsonl").write_text("{not json\n")
    cr = make_repo(repo)
    with pytest.raises(ControlRepoError):
        cr.write_outcome(
            task_id="T-1", status="DONE", summary="s", evidence=[]
        )


# ---------------------------------------------------------------------- #
# write_outcome — inbox status edit
# ---------------------------------------------------------------------- #


def test_inbox_status_line_updated_everything_else_preserved(repo):
    write_inbox(repo)
    cr = make_repo(
        repo,
        ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        .ok("commit", "--quiet", "-m").head("h"),
    )
    cr.write_outcome(
        task_id="T-2026-10-05-001", status="DONE", summary="s", evidence=[]
    )
    after = cr.read_inbox()
    before = SAMPLE_INBOX
    blines = before.splitlines()
    alines = after.splitlines()
    assert len(blines) == len(alines)
    diffs = [(i, b, a) for i, (b, a) in enumerate(zip(blines, alines)) if b != a]
    assert len(diffs) == 1, f"expected exactly one changed line, got {diffs}"
    _, before_line, after_line = diffs[0]
    assert before_line.strip() == 'status = "READY"   # authoring state'
    assert after_line.strip() == 'status = "DONE"   # authoring state'
    # Everything else byte-identical.
    assert alines[: diffs[0][0]] == blines[: diffs[0][0]]
    assert alines[diffs[0][0] + 1:] == blines[diffs[0][0] + 1:]


def test_inbox_edit_targets_the_right_task_when_two_have_same_status(repo):
    """Both READY tasks must not be edited; only the one whose id matched."""
    write_inbox(repo)
    cr = make_repo(
        repo,
        ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        .ok("commit", "--quiet", "-m").head("h"),
    )
    cr.write_outcome(
        task_id="T-2026-10-05-003", status="DONE", summary="s", evidence=[]
    )
    after = cr.read_inbox()
    statuses = [l.strip() for l in after.splitlines() if l.strip().startswith("status")]
    assert statuses == [
        'status = "READY"   # authoring state',
        'status = "RUNNING"',
        'status = "DONE"',
    ]


def test_inbox_edit_preserves_crlf_and_unquoted_styles(tmp_path):
    repo = tmp_path / "crlf"
    repo.mkdir()
    text = (
        "[[task]]\r\n"
        "id = \"T-1\"\r\n"
        "status = READY\r\n"
        "\r\n"
        "[[task]]\r\n"
        "id = \"T-2\"\r\n"
        "status = 'READY'\r\n"
    )
    write_inbox(repo, text)
    cr = make_repo(
        repo,
        ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        .ok("commit", "--quiet", "-m").head("h"),
    )
    cr.write_outcome(task_id="T-1", status="DONE", summary="s", evidence=[])
    after = cr.read_inbox()
    assert 'status = DONE\r\n' in after
    assert "id = \"T-2\"\r\n" in after
    assert "status = 'READY'\r\n" in after


def test_inbox_edit_unknown_task_changes_nothing(repo):
    write_inbox(repo)
    cr = make_repo(
        repo,
        ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        .ok("commit", "--quiet", "-m").head("h"),
    )
    out = cr.write_outcome(
        task_id="T-NOPE", status="DONE", summary="s", evidence=[]
    )
    assert out == "h"  # ledger line was new, so it committed
    assert 'status = "DONE"' not in cr.read_inbox()


def test_inbox_status_unchanged_returns_none_when_ledger_already_has_it(repo):
    """Terminal outcome recorded + same inbox status => full no-op."""
    write_inbox(repo, SAMPLE_INBOX.replace('status = "RUNNING"', 'status = "DONE"'))
    cr0 = make_repo(repo)
    cr0.write_outcome(task_id="T-2026-10-05-002", status="DONE", summary="s", evidence=[])
    cr = make_repo(repo)
    assert cr.write_outcome(
        task_id="T-2026-10-05-002", status="DONE", summary="s", evidence=[]
    ) is None
    assert cr._runner.calls == []


def test_inbox_status_changed_to_non_terminal_appends_and_edits(repo):
    write_inbox(repo)
    sb = ScriptBuilder()
    for _ in range(2):
        sb.ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        sb.ok("commit", "--quiet", "-m")
    sb.head("c2")
    cr = make_repo(repo, sb)
    cr.write_outcome(task_id="T-2026-10-05-001", status="RUNNING", summary="r", evidence=[])
    cr.write_outcome(task_id="T-2026-10-05-001", status="RUNNING", summary="r2", evidence=[])
    assert cr.read_inbox().count('status = "RUNNING"') == 2


# ---------------------------------------------------------------------- #
# commit_all / push
# ---------------------------------------------------------------------- #


def test_commit_all_stages_all_and_returns_sha(repo):
    cr = make_repo(
        repo,
        ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
        .ok("commit", "--quiet", "-m").head("c0ffee"),
    )
    (repo / "f.txt").write_text("x")
    assert cr.commit_all("msg") == "c0ffee"
    cmds = [c["cmd"][1:] for c in cr._runner.calls]
    assert cmds == [
        ["add", "-A"],
        ["diff", "--cached", "--quiet"],
        ["commit", "--quiet", "-m", "msg"],
        ["rev-parse", "HEAD"],
    ]


def test_commit_all_returns_none_when_clean(repo):
    cr = make_repo(
        repo,
        ScriptBuilder().ok("add", "-A").ok("diff", "--cached", "--quiet", rc=0),
    )
    assert cr.commit_all("msg") is None
    cmds = [c["cmd"][1:] for c in cr._runner.calls]
    assert cmds == [["add", "-A"], ["diff", "--cached", "--quiet"]]


def test_commit_all_rejects_empty_message(repo):
    cr = make_repo(repo)
    with pytest.raises(ControlRepoError):
        cr.commit_all("   ")
    assert cr._runner.calls == []


def test_push_uses_current_branch_with_lease(repo):
    cr = make_repo(
        repo,
        ScriptBuilder().branch("main").ok("push", "--quiet", "origin", "main"),
    )
    cr.push()
    assert cr._runner.calls[-1]["cmd"][1:] == [
        "push", "--quiet", "origin", "main"
    ]


def test_push_refuses_detached_head(repo):
    cr = make_repo(
        repo,
        ScriptBuilder().add(["rev-parse", "--abbrev-ref", "HEAD"], stdout="HEAD\n"),
    )
    with pytest.raises(ControlRepoError, match="detached HEAD"):
        cr.push()


def test_push_propagates_git_error(repo):
    cr = make_repo(
        repo,
        ScriptBuilder().branch("main")
        .fail(["push", "--quiet", "origin", "main"], stderr="rejected"),
    )
    with pytest.raises(ControlRepoError, match="rejected"):
        cr.push()


# ---------------------------------------------------------------------- #
# Token hygiene across the whole surface
# ---------------------------------------------------------------------- #


def test_token_never_in_argv(repo, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_NEVERSEEN")
    sb = ScriptBuilder()
    sb.ok("fetch", "--quiet", "origin")
    sb.ok("merge", "--ff-only", "--quiet", "origin/HEAD").head("h")
    sb.ok("remote", "get-url", "origin")
    sb.ok("add", "-A").ok("diff", "--cached", "--quiet", rc=1)
    sb.ok("commit", "--quiet", "-m").head("h")
    sb.branch("main").ok("push", "--quiet", "origin", "main")
    cr = make_repo(repo, sb)
    cr.sync()
    cr.remote_url()
    write_inbox(repo)
    cr.write_outcome(task_id="T-1", status="DONE", summary="s", evidence=[])
    cr.push()
    for call in cr._runner.calls:
        joined = " ".join(call["cmd"])
        assert "ghp_NEVERSEEN" not in joined
        assert call["env"]["GITHUB_TOKEN"] == "ghp_NEVERSEEN"
