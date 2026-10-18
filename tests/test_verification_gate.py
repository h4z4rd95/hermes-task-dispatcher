"""Tests for dispatcher/verification_gate.py.

Covers the governance prompt's required gate behaviours with inexpensive,
reproducible tests run against temporary git repos — never the live TGSG
crawl or TraderBot backtest:

1. a T0 file edit does not trigger broad test suites
2. a T1 code edit selects targeted tests
3. existing valid evidence is reused without rerunning an expensive check
4. a changed commit/environment invalidates the relevant evidence
5. a failed required check cannot result in DONE (verdict FAIL)
6. missing access becomes BLOCKED without unsafe escalation
7. final GitHub fetch-back/commit identity is represented in the evidence
8. secrets are redacted/excluded from output and artifacts
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dispatcher.verification_gate import (  # noqa: E402
    CheckResult,
    EvidenceBundle,
    GateError,
    VerificationGate,
    redact,
    run_from_cli,
    task_digest,
    utcnow,
)

PY = sys.executable


def git(repo: Path, *args: str) -> str:
    out = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout


def make_repo(tmp_path: Path, name: str = "repo") -> Path:
    repo = tmp_path / name
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    git(repo, "config", "user.email", "vg@test.local")
    git(repo, "config", "user.name", "vg test")
    return repo


def commit(repo: Path, msg: str = "c") -> str:
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", msg)
    return git(repo, "rev-parse", "HEAD").strip()


def write(repo: Path, rel: str, content: str) -> None:
    p = repo / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def gate_for(tmp_path: Path, task_id: str = "T-TEST-1", **kw) -> VerificationGate:
    repo = kw.pop("repo_path", None) or (tmp_path / "repo")
    kw.setdefault("repo_path", repo)
    kw.setdefault("task_id", task_id)
    kw.setdefault("cache_path", tmp_path / "cache.json")
    return VerificationGate(**kw)


# --- 1: T0 does not run broad suites ---------------------------------------


def test_t0_doc_edit_does_not_run_test_suites(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "# docs\n")
    commit(repo, "docs")
    write(repo, "docs/readme.md", "# docs changed\n")
    commit(repo, "docs2")

    ran = {"count": 0}

    def spy_runner(files):
        ran["count"] += 1
        return 0

    gate = gate_for(tmp_path, risk_tier="T0")
    bundle = gate.run(test_runner=spy_runner)

    assert bundle.risk_tier == "T0"
    # T0's required set is files_exist_remote only — no test suite may run
    assert [c["check_id"] for c in bundle.checks] == ["files_exist_remote"]
    assert ran["count"] == 0, "T0 must not run any test suite"
    assert bundle.status == "PASS"
    # T0 changed set is the docs file; the version control diff sees it
    assert bundle.changed_files == ["docs/readme.md"]


# --- 2: T1 selects targeted tests ------------------------------------------


def test_t1_code_edit_selects_targeted_tests(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")

    write(repo, "dispatcher/mod.py", "def f():\n    return 2\n")
    commit(repo, "change")

    calls: list[list[str]] = []

    def runner(files):
        calls.append(list(files))
        return 0

    gate = gate_for(tmp_path, risk_tier="T1", repo_path=repo)
    bundle = gate.run(test_runner=runner)

    assert [c["check_id"] for c in bundle.checks] == ["diff_inspect", "targeted_tests"]
    tt = next(c for c in bundle.checks if c["check_id"] == "targeted_tests")
    assert tt["status"] == "PASS"
    # only the test file that maps to the changed module was selected
    assert calls == [["tests/test_mod.py"]]
    assert bundle.status == "PASS"


def test_t1_with_no_applicable_tests_is_partial_not_done(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/notes.md", "notes\n")
    commit(repo, "init")
    write(repo, "docs/notes.md", "notes2\n")
    commit(repo, "change")

    gate = gate_for(tmp_path, risk_tier="T1")
    bundle = gate.run()
    # no tests map to a docs-only change at T1 → PARTIAL, never PASS
    assert bundle.status == "PARTIAL"
    tt = next(c for c in bundle.checks if c["check_id"] == "targeted_tests")
    assert tt["skip_reason"] == "no targeted tests applicable"


def test_t1_empty_diff_is_not_applicable_and_passes(tmp_path):
    # Regression (2026-10-18): a workspace with no changes at all (e.g. a
    # repo-backed echo task; shallow clones where the worker never commits)
    # must be not-applicable at T1, collapsing to PASS — not PARTIAL/FAILED.
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    head = commit(repo, "init")

    gate = gate_for(tmp_path, risk_tier="T1", baseline_sha=head)
    bundle = gate.run()

    assert bundle.status == "PASS"
    assert bundle.changed_files == []
    tt = next(c for c in bundle.checks if c["check_id"] == "targeted_tests")
    assert tt["status"] == "PARTIAL"
    assert tt["skip_reason"].startswith("not applicable")


def test_targeted_tests_never_selects_non_python_files(tmp_path):
    # Regression (2026-10-18): markdown task notes whose *name* contains
    # "test" were handed to pytest, which exits 4 (usage error); the verdict
    # read that as a test failure. Only .py files are pytest targets.
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")
    write(repo, "prompts/task_a_live_test_notes.md", "# notes\n")
    write(repo, "test/GITHUB_SMOKE_TEST.md", "# smoke doc\n")
    write(repo, "dispatcher/mod.py", "def f():\n    return 2\n")
    commit(repo, "change")

    calls: list[list[str]] = []

    def runner(files):
        calls.append(list(files))
        return 0

    gate = gate_for(tmp_path, risk_tier="T1")
    bundle = gate.run(test_runner=runner)

    assert bundle.status == "PASS"
    assert calls == [["tests/test_mod.py"]], "markdown files must never reach pytest"
    for files in calls:
        assert all(f.endswith(".py") for f in files)


# --- 3: reuse of valid evidence without rerunning ---------------------------


def test_valid_evidence_is_reused_without_rerun(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    sha = commit(repo, "init")

    calls = {"count": 0}

    def runner(files):
        calls["count"] += 1
        return 0

    cache = tmp_path / "cache.json"
    gate = gate_for(tmp_path, risk_tier="T1", repo_path=repo, cache_path=cache)
    first = gate.run(test_runner=runner)
    assert first.status == "PASS"
    assert calls["count"] == 1
    assert first.evidence_reused is False

    # A second gate on the same task/commit/env/digest must reuse, not rerun.
    gate2 = VerificationGate(
        task_id="T-TEST-1",
        repo_path=repo,
        risk_tier="T1",
        cache_path=cache,
        prompt_text=gate.prompt_text,
        criteria=gate.criteria,
    )
    second = gate2.run(test_runner=runner)
    assert second.status == "PASS"
    assert calls["count"] == 1, "PASS evidence must be reused, not recomputed"
    assert second.evidence_reused is True
    assert any(c.get("reused") for c in second.checks)


# --- 4: changed commit invalidates evidence ---------------------------------


def test_changed_commit_invalidates_evidence(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")

    calls = {"count": 0}

    def runner(files):
        calls["count"] += 1
        return 0

    cache = tmp_path / "cache.json"
    gate = gate_for(tmp_path, risk_tier="T1", repo_path=repo, cache_path=cache)
    assert gate.run(test_runner=runner).status == "PASS"
    assert calls["count"] == 1

    # new commit → identity key changes → fresh run required
    write(repo, "dispatcher/mod.py", "def f():\n    return 2\n")
    commit(repo, "change")
    gate2 = VerificationGate(
        task_id="T-TEST-1",
        repo_path=repo,
        risk_tier="T1",
        cache_path=cache,
        prompt_text=gate.prompt_text,
        criteria=gate.criteria,
    )
    second = gate2.run(test_runner=runner)
    assert second.status == "PASS"
    assert calls["count"] == 2, "changed commit must invalidate cached evidence"
    assert second.evidence_reused is False


def test_changed_task_digest_invalidates_evidence(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")

    calls = {"count": 0}

    def runner(files):
        calls["count"] += 1
        return 0

    cache = tmp_path / "cache.json"
    gate = gate_for(tmp_path, risk_tier="T1", repo_path=repo, cache_path=cache,
                    prompt_text="original task")
    assert gate.run(test_runner=runner).status == "PASS"

    # same commit + env, different task contract → stale, rerun
    gate2 = VerificationGate(
        task_id="T-TEST-1",
        repo_path=repo,
        risk_tier="T1",
        cache_path=cache,
        prompt_text="changed task contract",
        criteria=gate.criteria,
    )
    gate2.run(test_runner=runner)
    assert calls["count"] == 2, "changed task digest must invalidate evidence"


# --- 5: failed required check cannot be DONE --------------------------------


def test_failed_required_check_yields_fail(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")

    def failing_runner(files):
        return 1

    gate = gate_for(tmp_path, risk_tier="T1", repo_path=repo)
    bundle = gate.run(test_runner=failing_runner)
    assert bundle.status == "FAIL"
    tt = next(c for c in bundle.checks if c["check_id"] == "targeted_tests")
    assert tt["status"] == "FAIL"
    assert tt["exit_code"] == 1


def test_verdict_order_fail_beats_blocked_and_partial(tmp_path):
    checks = [
        CheckResult(check_id="diff_inspect", status="PASS"),
        CheckResult(check_id="targeted_tests", status="FAIL"),
        CheckResult(check_id="runtime_probe", status="BLOCKED"),
    ]
    assert VerificationGate._verdict(checks, ["diff_inspect", "targeted_tests"])[0] == "FAIL"
    checks2 = [
        CheckResult(check_id="diff_inspect", status="PASS"),
        CheckResult(check_id="targeted_tests", status="BLOCKED"),
    ]
    assert VerificationGate._verdict(checks2, ["diff_inspect", "targeted_tests"])[0] == "BLOCKED"
    checks3 = [
        CheckResult(check_id="diff_inspect", status="PASS"),
        CheckResult(check_id="targeted_tests", status="PARTIAL"),
    ]
    assert VerificationGate._verdict(checks3, ["diff_inspect", "targeted_tests"])[0] == "PARTIAL"
    # non-required advisory check cannot downgrade a PASS
    checks4 = [
        CheckResult(check_id="diff_inspect", status="PASS"),
        CheckResult(check_id="targeted_tests", status="PASS"),
        CheckResult(check_id="advisory", status="FAIL"),
    ]
    assert VerificationGate._verdict(checks4, ["diff_inspect", "targeted_tests"])[0] == "PASS"


def test_missing_required_check_is_partial_not_pass(tmp_path):
    checks = [CheckResult(check_id="diff_inspect", status="PASS")]
    assert VerificationGate._verdict(checks, ["diff_inspect", "targeted_tests"])[0] == "PARTIAL"


# --- 6: missing access → BLOCKED --------------------------------------------


def test_not_a_repo_blocks_rather_than_escalates(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    gate = VerificationGate(
        task_id="T-BLOCKED-1",
        repo_path=empty,
        risk_tier="T1",
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    assert bundle.status == "BLOCKED"
    assert bundle.blockers, "BLOCKED must carry a named reason"
    assert any("not a git repository" in b for b in bundle.blockers)


def test_unknown_required_check_fails_closed(tmp_path):
    repo = make_repo(tmp_path)
    gate = VerificationGate(
        task_id="T-UNKNOWN-1",
        repo_path=repo,
        risk_tier="T1",
        checks={"nonexistent_check_id": lambda: CheckResult(check_id="x", status="PASS")},
        cache_path=tmp_path / "cache.json",
    )
    # patch TIER_CHECKS to require a check with no runner
    import dispatcher.verification_gate as vg

    original = vg.TIER_CHECKS["T1"]
    vg.TIER_CHECKS["T1"] = ("diff_inspect", "no_such_runner")
    try:
        bundle = gate.run()
    finally:
        vg.TIER_CHECKS["T1"] = original
    assert bundle.status == "BLOCKED"
    assert any("no runner registered" in b for b in bundle.blockers)


def test_bad_tier_is_rejected(tmp_path):
    with pytest.raises(GateError):
        VerificationGate(
            task_id="T-X",
            repo_path=tmp_path,
            risk_tier="T9",
        )
    with pytest.raises(GateError):
        VerificationGate(task_id="", repo_path=tmp_path)


# --- 7: fetch-back identity represented -------------------------------------


def test_fetch_back_field_present_and_boolean(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    gate = gate_for(tmp_path, risk_tier="T0", repo_path=repo)
    bundle = gate.run()
    fb = bundle.fetch_back
    assert isinstance(fb, dict)
    assert "ok" in fb and isinstance(fb["ok"], bool)
    # local identity always recorded
    assert Path(bundle.repo_path).is_dir()


# --- 8: secrets redacted -----------------------------------------------------


def test_secrets_redacted_from_output():
    raw = "export GITHUB_TOKEN=ghp_1234567890abcdefghijklmnopqrstuv and token=abc"
    out = redact(raw)
    assert "ghp_1234567890" not in out
    assert "abc" not in out.split("token=")[-1][:30] or "REDACTED" in out
    assert "REDACTED" in out


def test_long_output_truncated():
    raw = "x" * 5000
    assert len(redact(raw)) < 300


def test_secrets_absent_from_written_bundle(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    leaky = "password=hunter2 GITHUB_TOKEN=ghp_" + "a" * 40
    cache = tmp_path / "cache.json"
    gate = VerificationGate(
        task_id="T-SECRET-1",
        repo_path=repo,
        risk_tier="T1",
        cache_path=cache,
    )
    # a check that leaks a secret into its output
    gate.custom_checks["targeted_tests"] = lambda: CheckResult(
        check_id="targeted_tests",
        status="PASS",
        output=leaky,
        command="pytest tests/secret.py",
    )
    bundle = gate.run()
    out_path = tmp_path / "bundle.json"
    bundle.write(out_path)
    text = out_path.read_text(encoding="utf-8")
    assert "ghp_" not in text
    assert "hunter2" not in text
    assert "REDACTED" in text
    # cache file must be clean too
    assert "ghp_" not in cache.read_text(encoding="utf-8")


# --- CLI + bundle round-trip -------------------------------------------------


def test_cli_exit_codes_pass_and_not_pass(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    out = tmp_path / "gate.json"
    cache = tmp_path / "cache.json"
    rc = run_from_cli(
        [
            "--task-id", "T-CLI-1",
            "--repo", str(repo),
            "--tier", "T0",
            "--out", str(out),
            "--cache", str(cache),
        ]
    )
    assert rc == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["task_id"] == "T-CLI-1"
    assert payload["final_sha"]
    # rerun: reuse path, still PASS
    rc2 = run_from_cli(
        [
            "--task-id", "T-CLI-1",
            "--repo", str(repo),
            "--tier", "T0",
            "--out", str(out),
            "--cache", str(cache),
        ]
    )
    assert rc2 == 0
    payload2 = json.loads(out.read_text(encoding="utf-8"))
    assert payload2["evidence_reused"] is True


def test_cli_rejects_bad_args(tmp_path):
    with pytest.raises(SystemExit):
        run_from_cli(["--task-id", "X"])  # missing --repo
    rc = run_from_cli(["--task-id", "X", "--repo", str(tmp_path / "nope")])
    assert rc == 2


def test_bundle_roundtrip(tmp_path):
    b = EvidenceBundle(task_id="T-RT", status="PASS", final_sha="abc123")
    p = tmp_path / "b.json"
    b.write(p)
    loaded = EvidenceBundle.load(p)
    assert loaded.task_id == "T-RT"
    assert loaded.status == "PASS"
    with pytest.raises(GateError):
        EvidenceBundle.load(tmp_path / "missing.json")


def test_identity_key_stability(tmp_path):
    b1 = EvidenceBundle(task_id="T-K", final_sha="s1", env_digest="e1", task_digest="d1")
    b2 = EvidenceBundle(task_id="T-K", final_sha="s1", env_digest="e1", task_digest="d1")
    b3 = EvidenceBundle(task_id="T-K", final_sha="s2", env_digest="e1", task_digest="d1")
    assert b1.identity_key("c") == b2.identity_key("c")
    assert b1.identity_key("c") != b3.identity_key("c")


def test_task_digest_changes_with_contract():
    d1 = task_digest("prompt-a", ["c1"])
    d2 = task_digest("prompt-a", ["c1", "c2"])
    assert d1 != d2


# --- discovery contract ------------------------------------------------------


def test_discovers_agent_evidence_block(tmp_path, monkeypatch):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    ev = repo / ".verification-gate.json"
    ev.write_text(
        json.dumps({"acceptance_criteria": ["docs render"], "criterion_results": {"docs render": "PASS"}}),
        encoding="utf-8",
    )
    gate = gate_for(tmp_path, risk_tier="T0", repo_path=repo)
    assert gate.discover_evidence_path([repo]) == ev
    bundle = gate.run()
    assert bundle.acceptance_criteria == ["docs render"]
    assert bundle.criterion_results == {"docs render": "PASS"}


def test_env_override_discovery(tmp_path, monkeypatch):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    other = tmp_path / "other.json"
    other.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("VG_EVIDENCE_PATH", str(other))
    gate = gate_for(tmp_path, task_id="T-ENV-1", risk_tier="T0", repo_path=repo)
    assert gate.discover_evidence_path([repo]) == other


# --- redaction of timestamps / determinism -----------------------------------


def test_utcnow_format():
    ts = utcnow()
    assert ts.endswith("Z") and "T" in ts
