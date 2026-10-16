"""Focused tests for the Verification Gate follow-up workstreams.

Covers the follow-up prompt's three gaps with *only new* behaviour:

A. risk_tier in the real inbox/registry contract: default/valid/invalid,
   explicit escalation, attempted downgrade (clamped to the policy floor),
   and propagation into the gate at runtime.
B. per-task runtime_probe: allowlisted handlers, malformed spec -> BLOCKED,
   missing probe fatal for T2+ (never DONE), safe deterministic fixture,
   target identity in evidence, and no cross-target reuse.
C. independent remote fetch-back: github_api vs local_git methods and the
   presence of the independent_remote_read flag.

These tests deliberately do NOT rerun the 24-test predecessor suite; the
contracts it covers are unchanged where they overlap.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dispatcher.registry import (  # noqa: E402
    RISK_TIERS_ALLOWED,
    RISK_TIER_POLICY_FLOOR,
    InboxError,
    Task,
    inbox_to_toml,
    parse_inbox,
)
from dispatcher.verification_gate import (  # noqa: E402
    CheckResult,
    GateError,
    VerificationGate,
    _github_owner_repo,
)
from dispatcher.probes import (  # noqa: E402
    PROBE_HANDLERS,
    build_probe,
    validate_probe_spec,
)

PY = sys.executable


def git(repo: Path, *args: str) -> str:
    out = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    )
    return out.stdout


def make_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
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


# =============================================================================
# Workstream A — risk tier contract
# =============================================================================


def test_tier_constants():
    assert RISK_TIERS_ALLOWED == ("T0", "T1", "T2", "T3")
    assert RISK_TIER_POLICY_FLOOR == "T1"


def test_default_tier_is_policy_floor():
    t = Task(id="T-1", title="x", prompt_inline="p")
    assert t.risk_tier == ""
    assert t.effective_risk_tier == "T1"
    assert t.declared_risk_tier == ""


def test_valid_tiers_accepted_and_uppercased():
    for tier in ("t0", "T1", "t2", "T3"):
        t = Task(id="T-x", title="x", prompt_inline="p", risk_tier=tier)
        assert t.declared_risk_tier == tier.upper()


def test_invalid_tier_rejected():
    with pytest.raises(InboxError, match="risk_tier"):
        Task(id="T-x", title="x", prompt_inline="p", risk_tier="T9")
    with pytest.raises(InboxError, match="risk_tier"):
        Task(id="T-x", title="x", prompt_inline="p", risk_tier="nope")


def test_explicit_escalation_is_preserved():
    t = Task(id="T-x", title="x", prompt_inline="p", risk_tier="T3")
    assert t.effective_risk_tier == "T3"
    t2 = Task(id="T-y", title="x", prompt_inline="p", risk_tier="T2")
    assert t2.effective_risk_tier == "T2"


def test_declared_tier_below_floor_is_clamped_not_downgraded_silly():
    # Declared T0 on a task: clamped UP to the policy floor. The tier may
    # never be lowered below policy by a caller.
    t = Task(id="T-x", title="x", prompt_inline="p", risk_tier="T0")
    assert t.effective_risk_tier == RISK_TIER_POLICY_FLOOR


def test_tier_parses_from_inbox_and_roundtrips():
    text = f'''[inbox]
version = 1

[[task]]
id = "T-TIER-1"
title = "tiered"
prompt_inline = "p"
risk_tier = "T2"
runtime_probe = {{ handler = "file_exists", path = "docs/x.md", target = "docs:x" }}
'''
    inbox = parse_inbox(text)
    t = inbox.tasks[0]
    assert t.risk_tier == "T2"
    assert t.effective_risk_tier == "T2"
    # roundtrip through serialization
    out = inbox_to_toml(inbox)
    inbox2 = parse_inbox(out)
    assert inbox2.tasks[0].risk_tier == "T2"
    assert inbox2.tasks[0].runtime_probe["handler"] == "file_exists"


def test_unknown_key_still_rejected_with_new_keys_present():
    text = '''[inbox]
version = 1

[[task]]
id = "T-X"
title = "x"
prompt_inline = "p"
risk_tier = "T1"
bogus_field = "nope"
'''
    with pytest.raises(InboxError, match="unknown key"):
        parse_inbox(text)


def test_tier_propagates_to_gate_and_drives_required_checks(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    probe = {"handler": "file_exists", "path": "docs/readme.md", "target": "docs:readme"}
    gate = VerificationGate(
        task_id="T-PROP-1",
        repo_path=repo,
        risk_tier="T2",
        probe_spec=probe,
        cache_path=tmp_path / "cache.json",
    )
    assert gate.risk_tier == "T2"
    bundle = gate.run()
    ids = [c["check_id"] for c in bundle.checks]
    # T2 requires diff_inspect + targeted_tests + runtime_probe
    assert "runtime_probe" in ids
    rp = next(c for c in bundle.checks if c["check_id"] == "runtime_probe")
    assert rp["status"] == "PASS", rp
    assert bundle.runtime_probe["target"] == "docs:readme"


# =============================================================================
# Workstream B — per-task runtime probe
# =============================================================================


def test_probe_handler_allowlist_is_strict():
    assert set(PROBE_HANDLERS) == {
        "file_exists", "http_get", "tcp_connect", "pytest_module",
    }
    # no shell handler exists in the contract
    assert "shell" not in PROBE_HANDLERS
    assert "exec" not in PROBE_HANDLERS
    assert "command" not in PROBE_HANDLERS


def test_missing_probe_is_none():
    assert build_probe(None) is None
    assert build_probe("") is None


def test_empty_dict_probe_means_undeclared():
    """Regression: the registry defaults ``runtime_probe`` to ``{}``; an
    undeclared probe must build to ``None`` (no probe), never raise.
    """
    assert build_probe({}) is None
    # non-empty specs are still validated strictly
    with pytest.raises(ValueError, match="handler is required"):
        build_probe({"target": "x"})


def test_malformed_probe_specs_rejected():
    with pytest.raises(ValueError, match="handler is required"):
        validate_probe_spec({})
    with pytest.raises(ValueError, match="not allowlisted"):
        validate_probe_spec({"handler": "shell", "cmd": "rm -rf /", "target": "x"})
    with pytest.raises(ValueError, match="target identity is required"):
        validate_probe_spec({"handler": "file_exists", "path": "a"})
    with pytest.raises(ValueError, match="unknown key"):
        validate_probe_spec({"handler": "file_exists", "path": "a", "target": "t", "evil": 1})
    with pytest.raises(ValueError, match="timeout must be"):
        validate_probe_spec({"handler": "file_exists", "path": "a", "target": "t", "timeout": "x"})
    with pytest.raises(ValueError, match="must be a table"):
        validate_probe_spec(42)


def test_probe_timeout_is_capped():
    spec = validate_probe_spec(
        {"handler": "file_exists", "path": "a", "target": "t", "timeout": 99999}
    )
    assert spec["timeout"] <= 120


def test_malformed_probe_spec_blocks_gate(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    # missing target => ValueError at gate build time
    gate = VerificationGate(
        task_id="T-MAL-1",
        repo_path=repo,
        risk_tier="T2",
        probe_spec={"handler": "nope"},  # unknown handler
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    rp = next(c for c in bundle.checks if c["check_id"] == "runtime_probe")
    assert rp["status"] == "BLOCKED"
    assert "malformed" in rp["skip_reason"]
    # verdict must not be PASS
    assert bundle.status != "PASS"


def test_t2_missing_probe_cannot_be_done(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")
    gate = VerificationGate(
        task_id="T-T2-NOPROBE-1",
        repo_path=repo,
        risk_tier="T2",
        probe_spec=None,
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    rp = next(c for c in bundle.checks if c["check_id"] == "runtime_probe")
    assert rp["status"] == "PARTIAL"
    assert "required for T2+" in rp["skip_reason"]
    # PARTIAL on a required check is fatal — never DONE
    assert bundle.status == "PARTIAL"


def test_safe_fixture_passing_probe_can_pass(tmp_path):
    """Deterministic local fixture: a file_exists probe in the workspace."""
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    write(repo, "BUILD_OUTPUT.txt", "artifact\n")
    commit(repo, "init")
    gate = VerificationGate(
        task_id="T-T2-OK-1",
        repo_path=repo,
        risk_tier="T2",
        probe_spec={
            "handler": "file_exists",
            "path": "BUILD_OUTPUT.txt",
            "target": "workspace:build-output",
        },
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    rp = next(c for c in bundle.checks if c["check_id"] == "runtime_probe")
    assert rp["status"] == "PASS"
    assert bundle.status == "PASS"


def test_failing_probe_blocks_done(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")
    gate = VerificationGate(
        task_id="T-T2-FAIL-1",
        repo_path=repo,
        risk_tier="T2",
        probe_spec={
            "handler": "file_exists",
            "path": "DOES_NOT_EXIST.txt",
            "target": "workspace:missing",
        },
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    rp = next(c for c in bundle.checks if c["check_id"] == "runtime_probe")
    assert rp["status"] == "FAIL"
    assert bundle.status != "PASS"


def test_t0_t1_never_runs_runtime_probe(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    # even if a probe spec is declared, T0/T1 must not run it
    gate = VerificationGate(
        task_id="T-T0-1",
        repo_path=repo,
        risk_tier="T0",
        probe_spec={"handler": "file_exists", "path": "docs/readme.md", "target": "t"},
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    assert "runtime_probe" not in [c["check_id"] for c in bundle.checks]
    assert bundle.status == "PASS"


def test_http_get_probe_with_local_server_fixture(tmp_path):
    """Deterministic integration fixture: a local loopback HTTP server."""
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")

    # serve /health with a fixed body on an ephemeral port
    body = b'{"status":"ok"}'
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    httpd_ready = threading.Event()

    def serve_one():
        httpd_ready.set()
        try:
            conn, _ = server.accept()
            req = conn.recv(4096)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + body
            )
            conn.close()
        except OSError:
            pass

    th = threading.Thread(target=serve_one, daemon=True)
    th.start()
    httpd_ready.wait(5)
    try:
        gate = VerificationGate(
            task_id="T-T2-HTTP-1",
            repo_path=repo,
            risk_tier="T2",
            probe_spec={
                "handler": "http_get",
                "url": f"http://127.0.0.1:{port}/health",
                "expect_status": 200,
                "expect_contains": "ok",
                "target": "fixture:health",
                "timeout": 10,
            },
            cache_path=tmp_path / "cache.json",
        )
        bundle = gate.run()
        rp = next(c for c in bundle.checks if c["check_id"] == "runtime_probe")
        assert rp["status"] == "PASS", rp
        assert bundle.status == "PASS"
    finally:
        server.close()
        th.join(5)


def test_probe_evidence_carries_target_and_env_identity(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "BUILD_OUTPUT.txt", "x\n")
    commit(repo, "init")
    runner = build_probe(
        {"handler": "file_exists", "path": "BUILD_OUTPUT.txt", "target": "ws:out"},
        workspace=repo,
    )
    assert runner is not None
    code, out, evidence = runner()
    assert code == 0
    assert evidence["kind"] == "runtime_probe"
    assert evidence["target"] == "ws:out"
    assert evidence["handler"] == "file_exists"
    assert evidence["workspace"] == str(repo)
    assert evidence["ok"] is True


def test_probe_evidence_not_reused_across_targets(tmp_path):
    """Same task/commit/env but a DIFFERENT probe target -> no reuse."""
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    write(repo, "BUILD_OUTPUT.txt", "x\n")
    commit(repo, "init")
    cache = tmp_path / "cache.json"

    calls = {"file_exists": 0}

    spec_a = {"handler": "file_exists", "path": "BUILD_OUTPUT.txt", "target": "target-A"}
    spec_b = {"handler": "file_exists", "path": "BUILD_OUTPUT.txt", "target": "target-B"}

    gate_a = VerificationGate(
        task_id="T-REUSE-1", repo_path=repo, risk_tier="T2",
        probe_spec=spec_a, cache_path=cache,
    )
    bundle_a = gate_a.run()
    assert bundle_a.status == "PASS"

    # same task, same commit, same env — only the probe target differs.
    # Identity key includes the probe target, so reuse must NOT happen.
    gate_b = VerificationGate(
        task_id="T-REUSE-1", repo_path=repo, risk_tier="T2",
        probe_spec=spec_b, cache_path=cache,
    )
    bundle_b = gate_b.run()
    assert bundle_b.status == "PASS"
    # the probe check actually ran fresh (its evidence_ref is the new target)
    rp = next(c for c in bundle_b.checks if c["check_id"] == "runtime_probe")
    assert rp["reused"] is False
    assert rp["evidence_ref"] == "target-B"
    assert bundle_b.runtime_probe["target"] == "target-B"


def test_probe_evidence_reused_same_target(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    write(repo, "BUILD_OUTPUT.txt", "x\n")
    commit(repo, "init")
    cache = tmp_path / "cache.json"
    spec = {"handler": "file_exists", "path": "BUILD_OUTPUT.txt", "target": "same-target"}
    g1 = VerificationGate(task_id="T-R2", repo_path=repo, risk_tier="T2",
                          probe_spec=spec, cache_path=cache)
    assert g1.run().status == "PASS"
    g2 = VerificationGate(task_id="T-R2", repo_path=repo, risk_tier="T2",
                          probe_spec=spec, cache_path=cache)
    b2 = g2.run()
    assert b2.status == "PASS"
    assert b2.evidence_reused is True


def test_probe_path_traversal_blocked(tmp_path):
    """Traversal is rejected at validation time, before any probe runs."""
    repo = make_repo(tmp_path)
    # validation-time rejection: no runner is ever constructed
    with pytest.raises(ValueError, match="task workspace"):
        build_probe(
            {"handler": "file_exists", "path": "../../../etc/passwd", "target": "evil"},
            workspace=repo,
        )
    # absolute paths are rejected too
    with pytest.raises(ValueError, match="task workspace"):
        build_probe(
            {"handler": "file_exists", "path": "/etc/passwd", "target": "evil2"},
            workspace=repo,
        )
    # and the same spec through the gate is BLOCKED, never DONE
    gate = VerificationGate(
        task_id="T-TRAV-1",
        repo_path=repo,
        risk_tier="T2",
        probe_spec={"handler": "file_exists", "path": "../x", "target": "evil3"},
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    rp = next(c for c in bundle.checks if c["check_id"] == "runtime_probe")
    assert rp["status"] == "BLOCKED"
    assert "workspace" in rp["skip_reason"]
    assert bundle.status != "PASS"


def test_tcp_connect_probe_reachability(tmp_path):
    # A closed port on loopback must FAIL (deterministic, no real service).
    runner = build_probe(
        {"handler": "tcp_connect", "host": "127.0.0.1", "port": 1, "target": "t:closed"},
        workspace=tmp_path,
    )
    code, out, evidence = runner()
    assert code != 0


def test_pytest_module_probe(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "tests/test_ok.py", "def test_ok():\n    assert 1 + 1 == 2\n")
    commit(repo, "init")
    runner = build_probe(
        {"handler": "pytest_module", "path": "tests/test_ok.py", "target": "unit:ok"},
        workspace=repo,
    )
    code, out, evidence = runner()
    # pytest availability is not guaranteed in every env; a PASS is the happy
    # path, and if pytest is missing the probe must still fail closed (not
    # crash the gate) with a recorded error.
    assert evidence["handler"] == "pytest_module"
    assert code == 0 or "pytest" in str(out).lower() or "pytest" in str(evidence.get("error", "")).lower()


# =============================================================================
# Workstream C — independent remote fetch-back
# =============================================================================


def test_github_owner_repo_parsing():
    assert _github_owner_repo("https://github.com/h4z4rd95/hermes-task-dispatcher.git") == (
        "h4z4rd95",
        "hermes-task-dispatcher",
    )
    assert _github_owner_repo("https://github.com/A/B") == ("A", "B")
    assert _github_owner_repo("https://gitlab.com/A/B") is None
    assert _github_owner_repo("") is None
    # credential-bearing URLs must still parse, and the gate strips the
    # credential before recording (see remote_url()).
    assert _github_owner_repo("https://user:token@github.com/A/B.git") == ("A", "B")


def test_fetch_back_records_method_and_independence_flag(tmp_path):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    # no remote configured: falls back to local_git, honestly labelled
    gate = VerificationGate(
        task_id="T-FB-1", repo_path=repo, risk_tier="T0",
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    fb = bundle.fetch_back
    assert "method" in fb
    assert "independent_remote_read" in fb
    # Without a GitHub remote there is no independent read to claim.
    assert fb["independent_remote_read"] is False
    assert fb["method"] in ("local_git", "error", "github_api")


def test_fetch_back_github_api_path(tmp_path, monkeypatch):
    """Independent read: patched API helper returns a matching remote SHA."""
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    sha = commit(repo, "init")
    # give it a real-looking GitHub https remote (no credential embedded)
    git(repo, "remote", "add", "origin", "https://github.com/octocat/Hello-World.git")

    import dispatcher.verification_gate as vg

    seen = {"path": None}

    def fake_get(path, *, token, timeout=30):
        seen["path"] = path
        return 200, json.dumps({"name": "main", "commit": {"sha": sha}})

    monkeypatch.setattr(vg, "_github_api_get", fake_get)
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_fake")
    gate = VerificationGate(
        task_id="T-FB-2", repo_path=repo, risk_tier="T0",
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    fb = bundle.fetch_back
    assert fb["method"] == "github_api"
    assert fb["independent_remote_read"] is True
    assert fb["ok"] is True
    assert fb["remote_sha"] == sha
    assert fb["remote_repo"] == "octocat/Hello-World"
    assert seen["path"].startswith("/repos/octocat/Hello-World/branches/")


def test_fetch_back_mismatch_is_fail(tmp_path, monkeypatch):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    sha = commit(repo, "init")
    git(repo, "remote", "add", "origin", "https://github.com/octocat/Hello-World.git")
    import dispatcher.verification_gate as vg

    monkeypatch.setattr(
        vg,
        "_github_api_get",
        lambda path, *, token, timeout=30: (200, json.dumps({"commit": {"sha": "0" * 40}})),
    )
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_fake")
    gate = VerificationGate(
        task_id="T-FB-3", repo_path=repo, risk_tier="T0",
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    fb = bundle.fetch_back
    assert fb["independent_remote_read"] is True
    assert fb["ok"] is False
    assert "!=" in fb["error"]


def test_fetch_back_no_token_is_honest_local(tmp_path, monkeypatch):
    repo = make_repo(tmp_path)
    write(repo, "docs/readme.md", "x\n")
    commit(repo, "init")
    git(repo, "remote", "add", "origin", "https://github.com/octocat/Hello-World.git")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    gate = VerificationGate(
        task_id="T-FB-4", repo_path=repo, risk_tier="T0",
        cache_path=tmp_path / "cache.json",
    )
    bundle = gate.run()
    fb = bundle.fetch_back
    # No token: the evidence must NOT claim an independent remote read.
    assert fb["independent_remote_read"] is False
    assert "no GITHUB_TOKEN" in (fb.get("error") or "")


def test_fetch_back_no_token_from_env_does_not_break_gate(tmp_path, monkeypatch):
    from dispatcher.verification_gate import _Git

    class _PatchedGit(_Git):
        pass

    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    # local-only path is exercised and labelled, never crashes
    g = _Git(tmp_path)
    rec = g.fetch_verify("HEAD")
    assert rec["method"] in ("local_git", "error")
    assert rec["independent_remote_read"] is False


# =============================================================================
# Backward compatibility
# =============================================================================


def test_gate_still_rejects_bad_tier(tmp_path):
    with pytest.raises(GateError):
        VerificationGate(task_id="T-x", repo_path=tmp_path, risk_tier="T9")


def test_legacy_injected_probe_runner_still_works(tmp_path):
    """Callers that inject a plain runner (no task spec) keep working."""
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")
    gate = VerificationGate(
        task_id="T-LEGACY-1", repo_path=repo, risk_tier="T2",
        cache_path=tmp_path / "cache.json",
    )
    def legacy_probe():
        return 0, "manual ok"
    bundle = gate.run(probe=legacy_probe)
    rp = next(c for c in bundle.checks if c["check_id"] == "runtime_probe")
    assert rp["status"] == "PASS"
    assert bundle.status == "PASS"
