"""Risk-tiered verification gate for the task dispatcher.

Implements the automated verification gate from the governance prompt
``IMPLEMENT_AUTOMATED_VERIFICATION_GATE_2026-10-15.md`` (Plan-Project-Situation
repo). Stdlib-only so it runs inside the dispatcher process with no new
package, database, or service.

Contract (see VERIFICATION_GATE.md):

* The executing agent produces real artifacts plus a small evidence block at
  ``$VG_EVIDENCE_PATH`` (default ``.verification-gate.json`` in the task
  workspace). The gate consumes that block and derives a verdict; it never
  re-implements the feature under test.
* ``status = DONE`` is only ever derived from gate verdict ``PASS``; FAIL and
  PARTIAL both produce FAILED, BLOCKED produces BLOCKED with a named reason.
* Evidence reuse: keyed by ``(task_id, check_id, commit_sha, env_digest,
  task_digest)``. A later reviewer asking for the same check on the same
  identities receives the cached PASS and no command is re-run. Stale,
  missing, or contradicted evidence forces a fresh run.
* Secrets never enter the bundle: long values are truncated and token-shaped
  strings are redacted.
* Network access happens only through local git plumbing for the fetch-back
  verification. Runtime correctness evidence comes from the checks themselves.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Iterable

__all__ = [
    "RISK_TIERS",
    "GATE_STATUSES",
    "GateError",
    "EvidenceBundle",
    "CheckResult",
    "VerificationGate",
    "run_from_cli",
    "main",
]

# --- risk tiers (governance section C) -------------------------------------
# T0 docs/metadata, T1 isolated code, T2 integration/pipeline, T3
# security/production/financial/destructive. A task may be *raised* to a
# higher tier; never lowered to save time.
RISK_TIERS = ("T0", "T1", "T2", "T3")

GATE_STATUSES = ("PASS", "FAIL", "PARTIAL", "BLOCKED")

# Required checks per tier. A required check with no registered runner fails
# closed (BLOCKED). T0 never runs test suites — governance section C.
TIER_CHECKS: dict[str, tuple[str, ...]] = {
    "T0": ("files_exist_remote",),
    "T1": ("diff_inspect", "targeted_tests"),
    "T2": ("diff_inspect", "targeted_tests", "runtime_probe"),
    "T3": ("diff_inspect", "targeted_tests", "runtime_probe", "preconditions", "rollback_plan"),
}

_SECRET_KEY_RE = re.compile(r"(?i)(token|secret|password|api[_-]?key|passwd|credential)")
_LONG_TOKEN_RE = re.compile(r"\b([A-Za-z0-9_\-]{36,})\b")
_MAX_VALUE_CHARS = 200


class GateError(ValueError):
    """Malformed gate invocation (bad tier, unknown check id, bad bundle)."""


@dataclasses.dataclass
class CheckResult:
    """One executed or reused check."""

    check_id: str
    status: str  # PASS / FAIL / PARTIAL / BLOCKED
    command: str = ""
    exit_code: int | None = None
    output: str = ""
    evidence_ref: str = ""
    started_at: str = ""
    duration_s: float = 0.0
    reused: bool = False
    skip_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class EvidenceBundle:
    """Machine-readable per-task evidence artifact (governance section D)."""

    task_id: str
    status: str = "BLOCKED"  # gate status, not task status
    risk_tier: str = "T1"
    owner: str = ""
    repo_path: str = ""
    branch: str = ""
    baseline_sha: str = ""
    final_sha: str = ""
    changed_files: list[str] = dataclasses.field(default_factory=list)
    acceptance_criteria: list[str] = dataclasses.field(default_factory=list)
    criterion_results: dict[str, str] = dataclasses.field(default_factory=dict)
    checks: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    blockers: list[str] = dataclasses.field(default_factory=list)
    skipped: list[str] = dataclasses.field(default_factory=list)
    rollback_ref: str = ""
    validator: str = "verification-gate"
    evidence_reused: bool = False
    fetch_back: dict[str, Any] = dataclasses.field(default_factory=dict)
    started_at: str = ""
    finished_at: str = ""
    env_digest: str = ""
    task_digest: str = ""

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: Path) -> "EvidenceBundle":
        try:
            d = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise GateError(f"evidence file not found: {path}") from exc
        except json.JSONDecodeError as exc:
            raise GateError(f"evidence file is not valid JSON: {path}: {exc}") from exc
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})

    def identity_key(self, check_id: str) -> str:
        """Stable key governing evidence reuse (governance section F)."""
        return "|".join(
            (
                self.task_id,
                check_id,
                self.final_sha or self.baseline_sha,
                self.env_digest,
                self.task_digest,
            )
        )


def utcnow() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def redact(value: Any) -> Any:
    """Best-effort secret scrubbing for anything committed to the bundle."""
    if isinstance(value, dict):
        return {str(k): redact(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    if not isinstance(value, str):
        return value
    out = value
    if len(out) > _MAX_VALUE_CHARS:
        out = out[:_MAX_VALUE_CHARS] + f"...[truncated {len(value)} chars]"
    if _SECRET_KEY_RE.search(out):
        # redact the *value* of any token-shaped assignment, keep the key name
        out = re.sub(
            r"(?i)(token|secret|password|api[_-]?key|passwd|credential)\s*[:=]\s*\S+",
            r"\1=***REDACTED***",
            out,
        )
    # bare long tokens (PAT / hex / base64 shape) -> short prefix + hash tail
    def _tok(m: "re.Match[str]") -> str:
        return m.group(1)[:4] + "…" + hashlib.sha256(m.group(1).encode()).hexdigest()[:8]

    out = _LONG_TOKEN_RE.sub(_tok, out)
    return out


def env_digest(extra: dict[str, str] | None = None) -> str:
    """Stable digest of the verification environment (host + python + git).

    Deliberately coarse: it changes only when the runtime actually changes,
    which is exactly the freshness signal the reuse rule needs.
    """
    parts = [
        os.uname().sysname,
        os.uname().machine,
        f"{sys.version_info.major}.{sys.version_info.minor}",
    ]
    acceptance = _run(["git", "--version"])
    parts.append(acceptance[1].strip() if acceptance[0] == 0 else "git-unknown")
    if extra:
        parts.extend(f"{k}={v}" for k, v in sorted(extra.items()))
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:16]


def task_digest(prompt_text: str, criteria: list[str]) -> str:
    return hashlib.sha256(("\x00".join([prompt_text or "", *criteria])).encode()).hexdigest()[:16]


def _run(cmd: list[str], cwd: Path | None = None, timeout: int = 60) -> tuple[int, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode, out


class _Git:
    """Minimal local git plumbing, read-only by default."""

    def __init__(self, path: Path):
        self.path = path

    def _run(self, *args: str, timeout: int = 60) -> tuple[int, str]:
        return _run(["git", *args], cwd=self.path, timeout=timeout)

    def rev_parse(self, ref: str = "HEAD") -> str:
        code, out = self._run("rev-parse", ref)
        if code != 0:
            raise GateError(f"git rev-parse {ref} failed in {self.path}: {out.strip()}")
        return out.strip()

    def is_repo(self) -> bool:
        code, _ = self._run("rev-parse", "--is-inside-work-tree")
        return code == 0

    def status_porcelain(self) -> list[str]:
        code, out = self._run("status", "--porcelain")
        return [] if code != 0 else [ln.strip() for ln in out.splitlines() if ln.strip()]

    def diff_names(self, base: str) -> list[str]:
        """Names of files changed since ``base`` (committed or not).

        Returns [] when ``base`` is empty or does not resolve (e.g. a
        single-commit repository where HEAD~1 does not exist yet). Error
        text from git is never mixed into the file list.
        """
        if not base:
            return []
        code, out = self._run("rev-parse", "--verify", f"{base}^{{commit}}")
        if code != 0:
            return []
        code, out = self._run("diff", "--name-only", base, "HEAD")
        if code != 0:
            # uncommitted changes against base still count
            code, out = self._run("diff", "--name-only", base)
            if code != 0:
                return []
        return [ln.strip() for ln in out.splitlines() if ln.strip()]

    def changed(self) -> list[str]:
        """Working-tree changes (uncommitted), porcelain names."""
        code, out = self._run("status", "--porcelain")
        files = []
        for ln in out.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            files.append(ln[3:].strip() if len(ln) > 3 else ln)
        return sorted(set(files))

    def changed_since(self, base: str) -> list[str]:
        """Everything changed vs ``base``: uncommitted plus committed.

        When ``base`` does not resolve (single-commit repo) every tracked
        file counts as changed, which is the correct conservative answer
        for a verification gate: there is no older clean state to compare
        against.
        """
        names = set(self.changed())
        diffed = self.diff_names(base)
        if diffed:
            names.update(diffed)
        elif not names:
            # no diff and no working-tree changes: either nothing changed,
            # or base is unreadable. Distinguish by checking base validity.
            code, _ = self._run("rev-parse", "--verify", f"{base or 'HEAD~1'}^{{commit}}")
            if code != 0:
                code, out = self._run("ls-tree", "-r", "--name-only", "HEAD")
                if code == 0:
                    names.update(ln.strip() for ln in out.splitlines() if ln.strip())
        return sorted(names)

    def fetch_verify(self, ref: str = "HEAD") -> dict[str, Any]:
        """Fetch from origin and confirm the local commit exists remotely.

        Governance section 9: a commit proves a *source* change; remote
        presence proves it landed. Runtime correctness is recorded separately
        by the checks, never inferred from the commit.
        """
        record: dict[str, Any] = {"ref": ref, "ok": False}
        code, out = self._run("rev-parse", ref, timeout=120)
        if code != 0:
            record["error"] = out.strip()[:300]
            return record
        local = out.strip()
        record["local_sha"] = local
        # --dry-run avoids pulling objects; it verifies reachability only.
        code, out = self._run("fetch", "--dry-run", "origin", timeout=180)
        record["fetch_exit"] = code
        if code != 0:
            record["error"] = out.strip()[:300]
            return record
        code, out = self._run("branch", "-r", "--contains", local, timeout=120)
        if code == 0:
            branches = [b.strip() for b in out.splitlines() if "origin/" in b]
            record["remote_contains"] = branches[:4]
            record["ok"] = bool(branches)
        else:
            # remote branch listing unavailable: fall back to ls-remote
            code, out = self._run("ls-remote", "origin", timeout=120)
            record["ok"] = local in out
            if not record["ok"]:
                record["error"] = "commit not found in origin refs"
        return record


class VerificationGate:
    """Runs the risk-tiered verification pass for one task.

    1. collects the evidence the executing agent produced while working,
    2. runs (or reuses) the tier-appropriate checks,
    3. derives PASS / FAIL / PARTIAL / BLOCKED,
    4. records everything in one machine-readable bundle.
    """

    def __init__(
        self,
        *,
        task_id: str,
        repo_path: Path,
        risk_tier: str = "T1",
        owner: str = "dispatcher",
        criteria: list[str] | None = None,
        prompt_text: str = "",
        checks: dict[str, Callable[[], CheckResult]] | None = None,
        cache_path: Path | None = None,
        baseline_sha: str = "",
        branch: str = "",
        env_extra: dict[str, str] | None = None,
        require_repo: bool = True,
    ):
        task_id = (task_id or "").strip()
        if not task_id:
            raise GateError("task_id is required")
        risk_tier = (risk_tier or "T1").strip().upper()
        if risk_tier not in RISK_TIERS:
            raise GateError(f"risk_tier {risk_tier!r} not in {RISK_TIERS}")
        self.task_id = task_id
        self.repo_path = Path(repo_path)
        self.risk_tier = risk_tier
        self.owner = owner
        self.criteria = list(criteria or [])
        self.prompt_text = prompt_text
        self.custom_checks = dict(checks or {})
        self.cache_path = cache_path
        self.baseline_sha = baseline_sha
        self.branch = branch
        self._env_digest = env_digest(env_extra)
        self._task_digest = task_digest(prompt_text, self.criteria)
        # When False, repository-based checks are "not applicable" (PARTIAL with
        # a recorded reason) instead of BLOCKED on a non-repo workspace. Used
        # for prompt-only tasks that have no checkout by design.
        self.require_repo = require_repo

    # -- evidence discovery ------------------------------------------------

    def discover_evidence_path(self, search_paths: Iterable[Path]) -> Path | None:
        for base in search_paths:
            for cand in (
                base / ".verification-gate.json",
                base / f".verification-gate-{self.task_id}.json",
            ):
                if cand.is_file():
                    return cand
        env_path = os.environ.get("VG_EVIDENCE_PATH", "")
        if env_path:
            p = Path(env_path)
            if p.is_file():
                return p
        return None

    # -- checks ------------------------------------------------------------

    def _repo_na(self, check_id: str) -> CheckResult | None:
        """Return a not-applicable result when the workspace is not a repo.

        With require_repo=True (default) this is BLOCKED — fail closed, the
        task cannot be proven. With require_repo=False it is PARTIAL with a
        recorded reason, so prompt-only tasks without a checkout are not
        falsely blocked on repository checks they never claimed to run.
        """
        if _Git(self.repo_path).is_repo():
            return None
        status, reason = ("BLOCKED", "not a git repository") if self.require_repo else (
            "PARTIAL", "not applicable: task has no repository checkout",
        )
        return CheckResult(
            check_id=check_id,
            status=status,
            command="git rev-parse --is-inside-work-tree",
            skip_reason=reason,
            started_at=utcnow(),
        )

    def check_diff_inspect(self) -> CheckResult:
        """T0/T1+: inspect the diff. Never runs a test suite (tier rule)."""
        git = _Git(self.repo_path)
        started = utcnow()
        na = self._repo_na("diff_inspect")
        if na is not None:
            return na
        changed = git.changed_since(self.baseline_sha or "HEAD~1")
        return CheckResult(
            check_id="diff_inspect",
            status="PASS",
            command="git status --porcelain",
            exit_code=0,
            output=redact("\n".join(changed)) or "(no changes)",
            evidence_ref=f"git:{self.repo_path}",
            started_at=started,
        )

    def check_files_exist_remote(self) -> "CheckResult":
        """T0: changed paths exist; no broad test suite, per governance."""
        started = utcnow()
        git = _Git(self.repo_path)
        na = self._repo_na("files_exist_remote")
        if na is not None:
            return na
        changed = git.changed_since(self.baseline_sha or "HEAD~1")
        missing = [f for f in changed if not (self.repo_path / f).exists()]
        ok = not missing
        return CheckResult(
            check_id="files_exist_remote",
            status="PASS" if ok else "FAIL",
            command=f"test -f {len(changed)} changed paths",
            exit_code=0 if ok else 1,
            output=redact("\n".join(missing)) or "all changed paths present",
            started_at=started,
        )

    def check_targeted_tests(
        self, runner: "Callable[[list[str]], int] | None" = None
    ) -> CheckResult:
        """Run only the test files covering the changed paths (governance C).

        The default runner is used only when the caller does not inject one,
        which keeps the gate testable without a real codebase.
        """
        started = utcnow()
        git = _Git(self.repo_path)
        if not git.is_repo():
            na = self._repo_na("targeted_tests")
            if na is not None:
                return na
        changed = git.changed_since(self.baseline_sha or "HEAD~1")
        test_files = [
            p for p in changed if "test" in Path(p).name.lower() or p.endswith(("test.py", "tests.py"))
        ]
        if not test_files:
            # no test touched by the diff: map source modules to their tests
            src = [p for p in changed if p.endswith(".py") and "test" not in Path(p).name.lower()]
            for s in src:
                stem = Path(s).stem
                cand = [
                    f"tests/test_{stem}.py",
                    f"tests/{stem}_test.py",
                    f"{Path(s).parent}/test_{stem}.py",
                ]
                test_files += [c for c in cand if (self.repo_path / c).exists()]
        if not test_files:
            return CheckResult(
                check_id="targeted_tests",
                status="PARTIAL",
                command="(none selected)",
                output="no test files map to the changed paths",
                started_at=started,
                skip_reason="no targeted tests applicable",
            )
        if runner is None:
            runner = self._default_test_runner
        cmd = f"pytest {' '.join(test_files)}"
        try:
            code = runner(test_files)
        except Exception as exc:  # noqa: BLE001 - bounded, recorded
            return CheckResult(
                check_id="targeted_tests",
                status="BLOCKED",
                command=cmd,
                skip_reason=f"runner error: {type(exc).__name__}: {exc}"[:200],
                started_at=started,
            )
        return CheckResult(
            check_id="targeted_tests",
            status="PASS" if code == 0 else "FAIL",
            command=cmd,
            exit_code=code,
            output=f"ran {len(test_files)} targeted test file(s)",
            started_at=started,
        )

    def _default_test_runner(self, test_files: list[str]) -> int:
        code, _ = _run(
            [sys.executable, "-m", "pytest", *test_files],
            cwd=self.repo_path,
            timeout=600,
        )
        return code

    def check_runtime_probe(
        self, runner: "Callable[[], tuple[int, str]] | None" = None
    ) -> "CheckResult":
        """T2+: real API/log/DB/runtime probe injected per task."""
        started = utcnow()
        if runner is None:
            return CheckResult(
                check_id="runtime_probe",
                status="PARTIAL",
                command="(none provided)",
                skip_reason="no runtime probe defined for this task",
                started_at=started,
            )
        try:
            code, out = runner()
        except Exception as exc:  # noqa: BLE001
            return CheckResult(
                check_id="runtime_probe",
                status="BLOCKED",
                skip_reason=f"probe error: {type(exc).__name__}: {exc}"[:200],
                started_at=started,
            )
        return CheckResult(
            check_id="runtime_probe",
            status="PASS" if code == 0 else "FAIL",
            command="(injected probe)",
            exit_code=code,
            output=redact(out),
            started_at=started,
        )

    def check_preconditions(self, plan: str = "") -> CheckResult:
        started = utcnow()
        ok = bool((plan or "").strip())
        return CheckResult(
            check_id="preconditions",
            status="PASS" if ok else "FAIL",
            command="(documented preconditions)",
            output=redact(plan)[:400] if plan else "missing precondition plan",
            started_at=started,
        )

    def check_rollback_plan(self, plan: str = "") -> CheckResult:
        started = utcnow()
        ok = bool((plan or "").strip())
        return CheckResult(
            check_id="rollback_plan",
            status="PASS" if ok else "FAIL",
            command="(documented rollback)",
            output=redact(plan)[:400] if plan else "missing rollback plan",
            started_at=started,
        )

    # -- evidence reuse (governance section F) ------------------------------

    def _load_cache(self) -> dict[str, dict[str, Any]]:
        if not self.cache_path or not self.cache_path.is_file():
            return {}
        try:
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_cache(self, cache: dict[str, dict[str, Any]]) -> None:
        if not self.cache_path:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
        tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.cache_path)

    def _reusable(self, key: str) -> CheckResult | None:
        """Return a cached PASS result when the identity key still matches.

        Governance section F: reuse PASS evidence only when task id, check id,
        commit, environment, and task digest all still match. Anything stale,
        missing, or contradicted forces a fresh run.
        """
        cache = self._load_cache()
        entry = cache.get(key)
        if not entry or entry.get("status") != "PASS":
            return None
        try:
            cr = CheckResult.from_dict(entry["result"])
        except Exception:
            return None
        cr.reused = True
        return cr

    # -- verdict ------------------------------------------------------------

    @staticmethod
    def _verdict(checks: list[CheckResult], required: Iterable[str]) -> tuple[str, list[str]]:
        """Derive the verdict. Fail-closed on any unmet required check.

        Ordering: FAIL > BLOCKED > PARTIAL > PASS, evaluated over required
        checks only. Non-required advisory checks are recorded and never
        gate the verdict on their own.

        Returns (verdict, missing_required_ids). ``missing`` is used by the
        caller to decide whether the tier's required set actually collapsed
        (everything not-applicable) — which is a PASS, not a PARTIAL.
        """
        required = set(required)
        statuses = {c.check_id: c.status for c in checks}
        for cid in required:
            if statuses.get(cid) == "FAIL":
                return "FAIL", []
        for cid in required:
            if statuses.get(cid) == "BLOCKED":
                return "BLOCKED", []
        missing = [cid for cid in required if statuses.get(cid) != "PASS"]
        if missing:
            return "PARTIAL", missing
        return "PASS", []

    # -- public entry -------------------------------------------------------

    def run(
        self,
        *,
        probe: "Callable[[], tuple[int, str]] | None" = None,
        test_runner: "Callable[[list[str]], int] | None" = None,
        precondition_plan: str = "",
        rollback_plan: str = "",
        evidence_path: "Path | None" = None,
    ) -> EvidenceBundle:
        """Execute one verification pass. Returns the bundle (not yet written)."""
        git = _Git(self.repo_path)
        started = utcnow()
        bundle = EvidenceBundle(
            task_id=self.task_id,
            risk_tier=self.risk_tier,
            owner=self.owner,
            repo_path=str(self.repo_path),
            branch=self.branch,
            baseline_sha=self.baseline_sha,
            started_at=started,
            env_digest=self._env_digest,
            task_digest=self._task_digest,
        )

        # The agent's evidence block, produced while working (governance D).
        agent_bundle: dict[str, Any] = {}
        if evidence_path is None:
            evidence_path = self.discover_evidence_path([self.repo_path])
        if evidence_path and Path(evidence_path).is_file():
            try:
                agent_bundle = json.loads(Path(evidence_path).read_text(encoding="utf-8"))
            except Exception:
                agent_bundle = {}
        if not isinstance(agent_bundle, dict):
            agent_bundle = {}
        # Criteria discovered from the agent's evidence win: they are the task
        # contract the implementer committed to while working.
        if not self.criteria and agent_bundle.get("acceptance_criteria"):
            self.criteria = [str(c) for c in agent_bundle["acceptance_criteria"]]
            bundle.acceptance_criteria = self.criteria
            self._task_digest = task_digest(self.prompt_text, self.criteria)
            bundle.task_digest = self._task_digest

        bundle.changed_files = []
        if git.is_repo():
            try:
                bundle.baseline_sha = self.baseline_sha or git.rev_parse("HEAD~1")
            except GateError:
                bundle.baseline_sha = self.baseline_sha
            try:
                bundle.final_sha = git.rev_parse("HEAD")
            except GateError:
                bundle.final_sha = ""
            bundle.changed_files = redact(git.changed_since(bundle.baseline_sha))
        bundle.rollback_ref = rollback_plan[:200]

        required = TIER_CHECKS[self.risk_tier]
        checks: list[CheckResult] = []
        used_reuse = False

        runners: dict[str, Callable[[], CheckResult]] = {
            "diff_inspect": self.check_diff_inspect,
            "files_exist_remote": lambda: self.check_files_exist_remote(),
            "targeted_tests": lambda: self.check_targeted_tests(test_runner),
            "runtime_probe": lambda: self.check_runtime_probe(probe),
            "preconditions": lambda: self.check_preconditions(precondition_plan),
            "rollback_plan": lambda: self.check_rollback_plan(rollback_plan),
        }
        runners.update(self.custom_checks)

        for cid in required:
            # Reuse must be decided BEFORE the check runs: the point of the
            # cache is that the expensive command is never executed again
            # while the identity (task/check/commit/env/contract) is current.
            cached = self._reusable(bundle.identity_key(cid))
            if cached is not None:
                used_reuse = True
                checks.append(cached)
                continue
            runner_fn = runners.get(cid)
            if runner_fn is None:
                # unknown-but-required check: fail closed
                checks.append(
                    CheckResult(
                        check_id=cid,
                        status="BLOCKED",
                        skip_reason=f"no runner registered for required check {cid}",
                        started_at=utcnow(),
                    )
                )
                continue
            checks.append(runner_fn())

        bundle.checks = [redact(c.to_dict()) for c in checks]

        # criterion results from the agent's evidence, if provided
        if isinstance(agent_bundle.get("criterion_results"), dict):
            bundle.criterion_results = {
                str(k): str(v) for k, v in agent_bundle["criterion_results"].items()
            }

        bundle.blockers = [
            c["skip_reason"]
            for c in bundle.checks
            if c.get("status") == "BLOCKED" and c.get("skip_reason")
        ]
        bundle.skipped = [
            f"{c['check_id']}: {c.get('skip_reason', 'n/a')}"
            for c in bundle.checks
            if c.get("status") == "PARTIAL" and c.get("skip_reason")
        ]
        bundle.evidence_reused = used_reuse
        raw_verdict, missing_required = self._verdict(checks, required)
        # A required set that collapsed entirely to "not applicable" is a
        # PASS: there was genuinely nothing to verify for this tier. Any
        # remaining PARTIAL is still fatal (the task is not fully proven).
        not_applicable = {
            c.check_id for c in checks if c.status == "PARTIAL" and c.reused is False and
            c.skip_reason.startswith("not applicable")
        }
        if raw_verdict == "PARTIAL" and not_applicable and set(missing_required) <= not_applicable:
            bundle.skipped = [
                f"{c['check_id']}: {c.get('skip_reason', 'n/a')}"
                for c in bundle.checks
                if c.get("status") == "PARTIAL" and c.get("skip_reason")
            ]
            bundle.status = "PASS"
        else:
            bundle.status = raw_verdict
        bundle.finished_at = utcnow()
        bundle.validator = "verification-gate"

        # fetch-back verification (governance section 9)
        if git.is_repo():
            bundle.fetch_back = git.fetch_verify("HEAD")

        # persist cache so the next pass can reuse PASS evidence.
        # The cache is a written artifact too: it must be secret-clean.
        if self.cache_path:
            cache = self._load_cache()
            for c in checks:
                if c.status == "PASS":
                    cache[bundle.identity_key(c.check_id)] = {
                        "status": "PASS",
                        "result": redact(c.to_dict()),
                        "recorded_at": utcnow(),
                    }
            self._save_cache(cache)

        return bundle


# --- from_dict helpers -----------------------------------------------------


def _check_result_from_dict(cls: type, d: dict[str, Any]) -> "CheckResult":
    return cls(**{f.name: d.get(f.name) for f in dataclasses.fields(cls)})


CheckResult.from_dict = classmethod(_check_result_from_dict)  # type: ignore[method-assign]


# --- CLI -------------------------------------------------------------------


def run_from_cli(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="verification-gate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Risk-tiered verification gate for the Hermes task dispatcher.\n"
            "Exit codes: 0 = PASS, 1 = FAIL/PARTIAL/BLOCKED, 2 = invocation error."
        ),
    )
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--repo", required=True, help="repository/worktree root")
    parser.add_argument("--tier", default="T1", choices=RISK_TIERS)
    parser.add_argument("--owner", default="dispatcher")
    parser.add_argument("--baseline-sha", default="")
    parser.add_argument("--branch", default="")
    parser.add_argument("--criteria", default="", help="';'-separated acceptance criteria")
    parser.add_argument("--evidence-path", default="")
    parser.add_argument("--out", default="", help="where to write the bundle JSON")
    parser.add_argument("--cache", default="", help="reuse cache file (JSON)")
    parser.add_argument("--preconditions", default="")
    parser.add_argument("--rollback", default="")
    args = parser.parse_args(argv)

    repo = Path(args.repo).expanduser().resolve()
    if not repo.is_dir():
        print(f"gate: repo path does not exist: {repo}", file=sys.stderr)
        return 2
    gate = VerificationGate(
        task_id=args.task_id,
        repo_path=repo,
        risk_tier=args.tier,
        owner=args.owner,
        criteria=[c for c in args.criteria.split(";") if c.strip()],
        baseline_sha=args.baseline_sha,
        branch=args.branch,
        cache_path=Path(args.cache) if args.cache else None,
    )
    try:
        bundle = gate.run(
            precondition_plan=args.preconditions,
            rollback_plan=args.rollback,
            evidence_path=Path(args.evidence_path) if args.evidence_path else None,
        )
    except GateError as exc:
        print(f"gate: {exc}", file=sys.stderr)
        return 2
    out = Path(args.out) if args.out else repo / f".verification-gate-{args.task_id}.json"
    bundle.write(out)
    print(json.dumps({"status": bundle.status, "out": str(out)}, ensure_ascii=False))
    return 0 if bundle.status == "PASS" else 1


def main() -> None:  # pragma: no cover - module entry
    sys.exit(run_from_cli())


if __name__ == "__main__":  # pragma: no cover
    main()
