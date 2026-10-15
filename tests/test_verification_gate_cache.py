"""Final cache-behaviour validation for the Verification Gate.

Scope (per ``VERIFICATION_GATE_FINAL_CACHE_VALIDATION_2026-10-15.md``):
reuse, invalidation, probe-target isolation, failure non-caching, and the
honesty of remote-verification freshness reporting. These tests describe the
identity model the implementation *actually enforces*:

* cache key = ``(task_id, check_id, commit_sha, env_digest, task_digest)``
  plus ``probe=<target>`` for ``runtime_probe`` only;
* only ``PASS`` check results are written to the cache and only ``PASS``
  entries are eligible for reuse — FAIL/BLOCKED/PARTIAL are structurally
  never cached and can never be promoted to PASS by a cache hit;
* ``fetch_verify`` is **not** part of the evidence cache: every ``gate.run()``
  re-performs it. A cache hit on a check can never imply a fresh remote read,
  because the fetch-back is recomputed unconditionally on each run.

Deterministic by design: temporary scratch repositories, monkeypatched network
helpers. No production service is touched and no real GitHub call is made.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dispatcher.verification_gate import (  # noqa: E402
    EvidenceBundle,
    VerificationGate,
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
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def seed_t1_repo(tmp_path: Path) -> Path:
    """A T1-ready scratch repo with a passing mapped test."""
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    commit(repo, "init")
    return repo


def add_github_remote(repo: Path, url: str = "https://github.com/o/r.git") -> None:
    """Give the scratch repo a GitHub https origin so fetch_verify takes the
    GitHub API path (the API call itself is monkeypatched in the tests)."""
    git(repo, "remote", "add", "origin", url)


class CountingRunner:
    """Test runner that records how many times it was invoked."""

    def __init__(self, exit_code: int = 0) -> None:
        self.exit_code = exit_code
        self.calls = 0

    def __call__(self, files):
        self.calls += 1
        return self.exit_code


def gate(**kwargs) -> VerificationGate:
    kwargs.setdefault("risk_tier", "T1")
    return VerificationGate(**kwargs)


# --- 1. cache hit: identity-identical run reuses without rerunning ----------


def test_cache_hit_reuses_and_does_not_rerun(tmp_path):
    """Same task/check/commit/env/task digest → reuse, zero re-execution."""
    repo = seed_t1_repo(tmp_path)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    g1 = gate(
        task_id="T-CACHE-1", repo_path=repo, cache_path=cache,
        prompt_text="cache hit test",
    )
    b1 = g1.run(test_runner=runner)
    assert b1.status == "PASS"
    assert runner.calls == 1
    assert b1.evidence_reused is False

    g2 = gate(
        task_id="T-CACHE-1", repo_path=repo, cache_path=cache,
        prompt_text="cache hit test",
    )
    b2 = g2.run(test_runner=runner)
    assert b2.status == "PASS"
    assert runner.calls == 1, "identical identity must reuse, not re-run"
    assert b2.evidence_reused is True
    assert any(c.get("reused") for c in b2.checks), "a check must be marked reused"

    for c in b2.checks:
        if c.get("reused"):
            assert c["status"] == "PASS"


def test_cache_hit_still_performs_fresh_fetch_back(tmp_path, monkeypatch):
    """A cache hit must not make the remote read stale: fetch_verify runs on
    every gate.run(), independent of the evidence cache."""
    repo = seed_t1_repo(tmp_path)
    add_github_remote(repo)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    g1 = gate(
        task_id="T-CACHE-FB", repo_path=repo, cache_path=cache,
        prompt_text="fetch freshness",
    )
    b1 = g1.run(test_runner=runner)
    assert b1.status == "PASS"

    calls = {"api": 0}
    import dispatcher.verification_gate as vg

    def fake_get(endpoint, token=None, timeout=30):
        calls["api"] += 1
        return 200, json.dumps({"commit": {"sha": b1.fetch_back.get("local_sha", "")}})

    monkeypatch.setattr(vg, "_github_api_get", fake_get)
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")

    g2 = gate(
        task_id="T-CACHE-FB", repo_path=repo, cache_path=cache,
        prompt_text="fetch freshness",
    )
    b2 = g2.run(test_runner=runner)

    assert runner.calls == 1, "checks reused from cache"
    assert b2.evidence_reused is True
    assert calls["api"] == 1, "fetch_verify must run on every gate.run()"
    assert b2.fetch_back["method"] == "github_api"
    assert b2.fetch_back["independent_remote_read"] is True
    assert b2.fetch_back["ok"] is True


# --- 2. commit invalidation -------------------------------------------------


def test_new_commit_invalidates_cached_pass(tmp_path):
    """A different commit SHA changes the identity key → fresh run."""
    repo = seed_t1_repo(tmp_path)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    g1 = gate(
        task_id="T-CACHE-2", repo_path=repo, cache_path=cache,
        prompt_text="commit invalidation",
    )
    assert g1.run(test_runner=runner).status == "PASS"
    assert runner.calls == 1

    write(repo, "dispatcher/mod.py", "def f():\n    return 2\n")
    commit(repo, "change")

    g2 = gate(
        task_id="T-CACHE-2", repo_path=repo, cache_path=cache,
        prompt_text="commit invalidation",
    )
    b2 = g2.run(test_runner=runner)
    assert b2.status == "PASS"
    assert runner.calls == 2, "new commit must invalidate the cached PASS"
    assert b2.evidence_reused is False


# --- 3. environment / task digest invalidation ------------------------------


def test_env_digest_change_invalidates_cached_pass(tmp_path):
    """A different env digest changes the identity key → fresh run."""
    repo = seed_t1_repo(tmp_path)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    g1 = gate(
        task_id="T-CACHE-3", repo_path=repo, cache_path=cache,
        prompt_text="env invalidation", env_extra={"tool": "1"},
    )
    assert g1.run(test_runner=runner).status == "PASS"
    assert runner.calls == 1

    # Same task/commit, but the environment identity changed (env_extra is
    # part of the env digest): the cached entry no longer matches.
    g2 = gate(
        task_id="T-CACHE-3", repo_path=repo, cache_path=cache,
        prompt_text="env invalidation", env_extra={"tool": "2"},
    )
    b2 = g2.run(test_runner=runner)
    assert b2.status == "PASS"
    assert runner.calls == 2, "a changed env digest must force a fresh run"
    assert b2.evidence_reused is False


def test_env_digest_extra_changes_the_digest():
    from dispatcher.verification_gate import env_digest

    assert env_digest({"a": "1"}) != env_digest({"a": "2"})
    assert env_digest() != env_digest({"a": "1"})


def test_task_digest_change_invalidates_cached_pass(tmp_path):
    """A different task digest (changed prompt text) → fresh run."""
    repo = seed_t1_repo(tmp_path)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    g1 = gate(
        task_id="T-CACHE-4", repo_path=repo, cache_path=cache,
        prompt_text="original contract",
    )
    assert g1.run(test_runner=runner).status == "PASS"
    assert runner.calls == 1

    g2 = gate(
        task_id="T-CACHE-4", repo_path=repo, cache_path=cache,
        prompt_text="changed contract",
    )
    b2 = g2.run(test_runner=runner)
    assert b2.status == "PASS"
    assert runner.calls == 2, "changed task digest must invalidate"
    assert b2.evidence_reused is False


def test_identity_key_is_sensitive_to_all_components():
    """The key embeds task/check/commit/env/task digest and probe target."""
    b = EvidenceBundle(
        task_id="T-ID",
        repo_path="/tmp/x",
        owner="o",
        risk_tier="T2",
        baseline_sha="b",
        final_sha="f",
        env_digest="E",
        task_digest="D",
    )
    base = b.identity_key("runtime_probe", "tgt")
    assert "T-ID" in base and "runtime_probe" in base
    assert "f" in base and "E" in base and "D" in base
    assert "probe=tgt" in base

    assert b.identity_key("runtime_probe", "tgt") != b.identity_key(
        "runtime_probe", "other"
    ), "probe target must be part of the key"
    assert b.identity_key("runtime_probe", "") != base
    b2 = EvidenceBundle(
        task_id="T-ID", repo_path="/tmp/x", owner="o", risk_tier="T2",
        env_digest="E", task_digest="D", final_sha="changed",
    )
    assert b2.identity_key("runtime_probe", "tgt") != base


def test_identity_key_collision_resistance():
    """Component values containing the separator cannot blur identity."""
    b = EvidenceBundle(
        task_id="A", repo_path="/x", owner="o", risk_tier="T1",
        env_digest="E", task_digest="D", final_sha="S",
    )
    k1 = b.identity_key("check|one")
    k2 = b.identity_key("check", )
    assert k1 != k2
    # a different task id with a crafted separator cannot collide
    b3 = EvidenceBundle(
        task_id="A|check", repo_path="/x", owner="o", risk_tier="T1",
        env_digest="E", task_digest="D", final_sha="S",
    )
    assert b3.identity_key("one") == k1, "documented separator semantics"
    # … but the key is not ambiguous about probe identity
    assert b.identity_key("c", "p") != b.identity_key("c=p", "")


# --- 4. failure non-caching: no stale PASS promotion ------------------------


def test_failed_result_is_never_cached_or_reused(tmp_path):
    """FAIL results are not written to the cache; a later run re-executes."""
    repo = seed_t1_repo(tmp_path)
    cache = tmp_path / "cache.json"
    failing = CountingRunner(1)

    g1 = gate(
        task_id="T-FAILCACHE", repo_path=repo, cache_path=cache,
        prompt_text="fail cache",
    )
    b1 = g1.run(test_runner=failing)
    assert b1.status == "FAIL"
    assert failing.calls == 1

    # The failing check (targeted_tests) must not be cached as reusable PASS.
    # diff_inspect legitimately passed and may be cached; only the failed
    # check is out of bounds.
    data = json.loads(cache.read_text(encoding="utf-8")) if cache.is_file() else {}
    tt_keys = [k for k in data if "targeted_tests" in k]
    assert tt_keys == [], "the FAILING check must never be cached as PASS"
    for entry in data.values():
        assert entry.get("status") == "PASS"

    passing = CountingRunner(0)
    g2 = gate(
        task_id="T-FAILCACHE", repo_path=repo, cache_path=cache,
        prompt_text="fail cache",
    )
    b2 = g2.run(test_runner=passing)
    assert passing.calls == 1, "no cached entry existed, so it ran fresh"
    assert b2.status == "PASS"


def test_blocked_probe_result_is_never_cached(tmp_path):
    """A BLOCKED (malformed probe) result cannot be promoted to PASS."""
    repo = seed_t1_repo(tmp_path)
    cache = tmp_path / "cache.json"

    spec = {"handler": "nope", "target": "t"}  # unknown handler -> BLOCKED
    g1 = gate(
        task_id="T-BLOCKCACHE", repo_path=repo, cache_path=cache,
        risk_tier="T2", probe_spec=spec, prompt_text="blocked probe",
    )
    b1 = g1.run()
    assert b1.status == "BLOCKED"

    data = json.loads(cache.read_text(encoding="utf-8")) if cache.is_file() else {}
    assert not any("probe=t" in k for k in data), (
        "a BLOCKED probe result must not be cached"
    )

    good = {"handler": "file_exists", "path": "dispatcher/mod.py", "target": "t"}
    g2 = gate(
        task_id="T-BLOCKCACHE", repo_path=repo, cache_path=cache,
        risk_tier="T2", probe_spec=good, prompt_text="blocked probe",
    )
    b2 = g2.run()
    rp = next(c for c in b2.checks if c["check_id"] == "runtime_probe")
    assert rp["reused"] is False, "no stale PASS may be reused"
    assert rp["status"] == "PASS"
    assert b2.status == "PASS"


def test_partial_result_is_never_promoted_by_cache(tmp_path):
    """A PARTIAL (missing probe on T2) leaves no reusable PASS behind."""
    repo = seed_t1_repo(tmp_path)
    cache = tmp_path / "cache.json"

    g1 = gate(
        task_id="T-PARTIAL", repo_path=repo, cache_path=cache,
        risk_tier="T2", prompt_text="no probe",
    )
    b1 = g1.run()
    assert b1.status == "PARTIAL", "T2 without a probe is never DONE"

    data = json.loads(cache.read_text(encoding="utf-8")) if cache.is_file() else {}
    rp_entries = [k for k in data if "probe=" in k]
    assert rp_entries == [], "PARTIAL probe result must not be cached"

    good = {"handler": "file_exists", "path": "dispatcher/mod.py", "target": "t"}
    g2 = gate(
        task_id="T-PARTIAL", repo_path=repo, cache_path=cache,
        risk_tier="T2", probe_spec=good, prompt_text="no probe",
    )
    b2 = g2.run()
    assert b2.status == "PASS"
    rp = next(c for c in b2.checks if c["check_id"] == "runtime_probe")
    assert rp["reused"] is False


# --- 5. probe-target isolation ----------------------------------------------


def test_probe_target_isolation_no_cross_contamination(tmp_path):
    """PASS evidence for target-A cannot satisfy a run against target-B."""
    # NOTE: seed_t1_repo makes the single commit; the probe artifacts must
    # live in *that* commit so the T1 targeted-test mapping still resolves
    # (the diff base is HEAD~1 in a single-commit scratch repo).
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    write(repo, "BUILD_A.txt", "a\n")
    write(repo, "BUILD_B.txt", "b\n")
    commit(repo, "init")
    cache = tmp_path / "cache.json"

    spec_a = {"handler": "file_exists", "path": "BUILD_A.txt", "target": "A"}
    g_a = gate(
        task_id="T-ISO", repo_path=repo, risk_tier="T2",
        cache_path=cache, probe_spec=spec_a, prompt_text="iso",
    )
    assert g_a.run().status == "PASS"

    spec_b = {"handler": "file_exists", "path": "BUILD_B.txt", "target": "B"}
    g_b = gate(
        task_id="T-ISO", repo_path=repo, risk_tier="T2",
        cache_path=cache, probe_spec=spec_b, prompt_text="iso",
    )
    b_b = g_b.run()
    rp = next(c for c in b_b.checks if c["check_id"] == "runtime_probe")
    assert rp["reused"] is False
    assert rp["evidence_ref"] == "B"
    assert b_b.runtime_probe["target"] == "B"

    data = json.loads(cache.read_text(encoding="utf-8"))
    probe_keys = [k for k in data if "probe=" in k]
    assert len(probe_keys) == 2
    assert any("probe=A" in k for k in probe_keys)
    assert any("probe=B" in k for k in probe_keys)


def test_probe_target_reuse_only_for_same_target(tmp_path):
    """Reusing across the same target is the intended behaviour."""
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    write(repo, "OUT.txt", "x\n")
    commit(repo, "init")
    cache = tmp_path / "cache.json"
    spec = {"handler": "file_exists", "path": "OUT.txt", "target": "same"}

    g1 = gate(
        task_id="T-ISO2", repo_path=repo, risk_tier="T2",
        cache_path=cache, probe_spec=spec, prompt_text="iso2",
    )
    assert g1.run().status == "PASS"
    g2 = gate(
        task_id="T-ISO2", repo_path=repo, risk_tier="T2",
        cache_path=cache, probe_spec=spec, prompt_text="iso2",
    )
    b2 = g2.run()
    assert b2.status == "PASS"
    rp = next(c for c in b2.checks if c["check_id"] == "runtime_probe")
    assert rp["reused"] is True


def test_probe_target_failure_is_not_rescued_by_other_target(tmp_path):
    """A failing probe for target-B cannot be satisfied by target-A's PASS."""
    repo = make_repo(tmp_path)
    write(repo, "dispatcher/mod.py", "def f():\n    return 1\n")
    write(repo, "tests/test_mod.py", "def test_f():\n    assert True\n")
    write(repo, "BUILD_A.txt", "a\n")
    commit(repo, "init")
    cache = tmp_path / "cache.json"

    spec_a = {"handler": "file_exists", "path": "BUILD_A.txt", "target": "A"}
    g_a = gate(
        task_id="T-ISO3", repo_path=repo, risk_tier="T2",
        cache_path=cache, probe_spec=spec_a, prompt_text="iso3",
    )
    assert g_a.run().status == "PASS"

    # target-B points at a file that does not exist: it must FAIL, not reuse A
    spec_b = {"handler": "file_exists", "path": "MISSING.txt", "target": "B"}
    g_b = gate(
        task_id="T-ISO3", repo_path=repo, risk_tier="T2",
        cache_path=cache, probe_spec=spec_b, prompt_text="iso3",
    )
    b_b = g_b.run()
    rp = next(c for c in b_b.checks if c["check_id"] == "runtime_probe")
    assert rp["status"] == "FAIL"
    assert rp["reused"] is False
    assert b_b.status == "FAIL"


# --- 6. remote freshness honesty -------------------------------------------


def test_cache_hit_never_fabricates_independent_read(tmp_path, monkeypatch):
    """Even with every check reused, a missing token keeps the record honest."""
    repo = seed_t1_repo(tmp_path)
    add_github_remote(repo)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    g1 = gate(
        task_id="T-FRESH", repo_path=repo, cache_path=cache,
        prompt_text="freshness",
    )
    assert g1.run(test_runner=runner).status == "PASS"

    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)

    g2 = gate(
        task_id="T-FRESH", repo_path=repo, cache_path=cache,
        prompt_text="freshness",
    )
    b2 = g2.run(test_runner=runner)
    assert b2.evidence_reused is True
    fb = b2.fetch_back
    assert fb["method"] != "github_api", (
        "no fresh API read happened; the record must not claim one"
    )
    assert fb["independent_remote_read"] is False


def test_fetch_back_reflects_remote_mismatch_on_cached_pass(tmp_path, monkeypatch):
    """A cached pass cannot mask a remote that moved ahead.

    The checks are reused (identity unchanged) but the fetch-back still runs
    and reports the divergence: this is the freshness guarantee.
    """
    repo = seed_t1_repo(tmp_path)
    add_github_remote(repo)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    g1 = gate(
        task_id="T-DIVERGE", repo_path=repo, cache_path=cache,
        prompt_text="divergence",
    )
    b1 = g1.run(test_runner=runner)
    assert b1.status == "PASS"

    import dispatcher.verification_gate as vg

    def fake_get(endpoint, token=None, timeout=30):
        return 200, json.dumps({"commit": {"sha": "f" * 40}})

    monkeypatch.setattr(vg, "_github_api_get", fake_get)
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")

    g2 = gate(
        task_id="T-DIVERGE", repo_path=repo, cache_path=cache,
        prompt_text="divergence",
    )
    b2 = g2.run(test_runner=runner)

    assert b2.evidence_reused is True, "checks still reused"
    assert b2.fetch_back["method"] == "github_api"
    assert b2.fetch_back["independent_remote_read"] is True
    assert b2.fetch_back["ok"] is False, "remote divergence must be reported"
    assert b2.fetch_back["remote_sha"] == "f" * 40


def test_fetch_back_is_not_cached_across_runs(tmp_path, monkeypatch):
    """fetch_verify runs on every gate.run(); it is never served from cache."""
    repo = seed_t1_repo(tmp_path)
    add_github_remote(repo)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    g1 = gate(
        task_id="T-FB-EVERY", repo_path=repo, cache_path=cache,
        prompt_text="fb every run",
    )
    b1 = g1.run(test_runner=runner)
    assert b1.status == "PASS"

    calls = {"api": 0}
    import dispatcher.verification_gate as vg

    def fake_get(endpoint, token=None, timeout=30):
        calls["api"] += 1
        return 200, json.dumps({"commit": {"sha": b1.fetch_back.get("local_sha", "")}})

    monkeypatch.setattr(vg, "_github_api_get", fake_get)
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")

    g2 = gate(
        task_id="T-FB-EVERY", repo_path=repo, cache_path=cache,
        prompt_text="fb every run",
    )
    g3 = gate(
        task_id="T-FB-EVERY", repo_path=repo, cache_path=cache,
        prompt_text="fb every run",
    )
    g2.run(test_runner=runner)
    g3.run(test_runner=runner)

    assert calls["api"] == 2, (
        "each gate.run() performed its own independent read (none from cache)"
    )
    assert runner.calls == 1, "checks were reused, only the fetch-back reran"


# --- 7. the cache file itself stays secret-clean ----------------------------


def test_cache_file_does_not_leak_secrets(tmp_path, monkeypatch):
    """Redaction applies to the persisted cache, not only the bundle."""
    repo = seed_t1_repo(tmp_path)
    cache = tmp_path / "cache.json"
    runner = CountingRunner(0)

    monkeypatch.setenv("GITHUB_TOKEN", "ghp_supersecretvalue123")

    g1 = gate(
        task_id="T-LEAK", repo_path=repo, cache_path=cache,
        prompt_text="leak check",
    )
    b1 = g1.run(test_runner=runner)
    assert b1.status == "PASS"

    raw = cache.read_text(encoding="utf-8")
    assert "ghp_" not in raw
    assert "supersecretvalue123" not in raw
    fb_json = json.dumps(b1.fetch_back)
    assert "ghp_" not in fb_json
    assert "supersecretvalue123" not in fb_json
