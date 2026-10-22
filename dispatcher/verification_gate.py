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
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterable

from .probes import build_probe as _build_probe
from .probes import validate_probe_spec as _validate_probe_spec

__all__ = [
    "RISK_TIERS",
    "GATE_STATUSES",
    "GateError",
    "EvidenceBundle",
    "CheckResult",
    "VerificationGate",
    "PROBE_HANDLERS",
    "build_probe",
    "validate_probe_spec",
    "run_from_cli",
    "main",
]

# --- risk tiers (governance section C) -------------------------------------
# T0 docs/metadata, T1 isolated code, T2 integration/pipeline, T3
# security/production/financial/destructive. A task may be *raised* to a
# higher tier; never lowered to save time.
RISK_TIERS = ("T0", "T1", "T2", "T3")

GATE_STATUSES = ("PASS", "FAIL", "PARTIAL", "BLOCKED")

# Re-export the probe contract (defined in dispatcher/probes.py) so callers
# import everything from one place.
from .probes import PROBE_HANDLERS as PROBE_HANDLERS  # noqa: F401

build_probe = _build_probe
validate_probe_spec = _validate_probe_spec

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


class _MalformedProbe(ValueError):
    """A task's runtime_probe spec failed contract validation."""


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
    # Probe target identity: probe evidence must never be reused across
    # different targets or environments (governance B).
    runtime_probe: dict[str, Any] = dataclasses.field(default_factory=dict)
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

    def identity_key(self, check_id: str, probe_target: str = "") -> str:
        """Stable key governing evidence reuse (governance section F).

        Probe evidence additionally keys on the probe target so it can never
        be reused across a different target or environment.
        """
        parts = [
            self.task_id,
            check_id,
            self.final_sha or self.baseline_sha,
            self.env_digest,
            self.task_digest,
        ]
        if probe_target:
            parts.append(f"probe={probe_target}")
        return "|".join(parts)


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


def _github_owner_repo(url: str) -> "tuple[str, str] | None":
    """Extract (owner, repo) from a GitHub https URL, else None.

    Uses the standard URL parser so URLs carrying user-info/credentials parse
    correctly. User-info is *never* returned: this function yields only the
    owner and repo, and callers must not log the input URL verbatim.
    """
    if not url:
        return None
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return None
    if parts.scheme != "https":
        return None
    host = (parts.hostname or "").lower()
    if host not in {"github.com", "www.github.com"}:
        return None
    path = parts.path
    if not path:
        return None
    segments = [seg for seg in path.split("/") if seg]
    if len(segments) != 2:
        return None
    owner, repo = segments
    if repo.endswith(".git"):
        repo = repo[:-4]
    if not owner or not repo or owner in {"settings", "orgs", "features"} or repo in {"", "."}:
        return None
    return owner, repo


def _github_api_get(path: str, *, token: str, timeout: int = 30) -> "tuple[int, str]":
    """One bounded, read-only GitHub API GET. Returns (status_code, body).

    Never raises: network errors are returned as a nonzero pseudo-status with
    a short reason. No token is ever embedded in the returned body.
    """
    url = f"https://api.github.com{path}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "hermes-verification-gate",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read(64 * 1024).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read(1024).decode("utf-8", "replace")
        except Exception:
            pass
        return int(exc.code), body
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return -1, f"{type(exc).__name__}: {exc}"[:200]


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

    def remote_url(self) -> str:
        """HTTPS origin URL, or '' when the origin is not a GitHub https URL."""
        code, out = self._run("config", "--get", "remote.origin.url", timeout=30)
        if code != 0:
            return ""
        url = out.strip()
        # Strip an embedded credential (https://user:token@host/...) so the
        # recorded evidence never carries a secret.
        if "@" in url and url.startswith("https://"):
            url = "https://" + url.split("@", 1)[1]
        return url

    def fetch_verify(self, ref: str = "HEAD") -> dict[str, Any]:
        """Independent, read-only GitHub verification of the pushed commit.

        This is the governance fetch-back (section 9 / follow-up workstream C):
        the evidence must represent a fresh, independent read of GitHub — not
        local workspace presence.

        It resolves the local commit, then performs a read-only GitHub API
        call (``/repos/{owner}/{repo}/branches/{branch}``) using the token
        from the environment when available. The check is honest about its
        method: ``method`` is always one of ``github_api`` (independent read,
        ``ok`` meaningful), ``local_git`` (no token/network: the result only
        proves the local clone mirrors its remote-tracking ref — NOT an
        independent read), or ``error``.

        A commit proves a *source* change; this check proves it landed
        remotely. Runtime correctness comes from the checks, never from here.
        """
        record: dict[str, Any] = {
            "ref": ref,
            "ok": False,
            # The field is explicit so no consumer can mistake a local check
            # for an independent remote read.
            "method": "error",
            "independent_remote_read": False,
        }
        code, out = self._run("rev-parse", ref, timeout=120)
        if code != 0:
            record["error"] = out.strip()[:300]
            return record
        local = out.strip()
        record["local_sha"] = local
        record["verified_at"] = utcnow()

        url = self.remote_url()
        owner_repo = _github_owner_repo(url)
        if owner_repo is None:
            # Not a GitHub https remote: fall back to an honest local check.
            return self._fetch_verify_local(ref, local, record)

        owner, repo_name = owner_repo
        record["remote_repo"] = f"{owner}/{repo_name}"
        # branch resolution: prefer the ref name, else HEAD's upstream
        branch = ref if ref != "HEAD" else self._current_branch()
        if not branch:
            record["error"] = "could not resolve branch for independent read"
            return record
        record["branch"] = branch

        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if not token:
            record["method"] = "local_git"
            record["error"] = (
                "no GITHUB_TOKEN/GH_TOKEN: independent GitHub API read not "
                "available; falling back to local remote-tracking ref only"
            )
            return self._fetch_verify_local(ref, local, record)

        code, out = _github_api_get(
            f"/repos/{owner}/{repo_name}/branches/{urllib.parse.quote(branch, safe='')}",
            token=token,
            timeout=30,
        )
        if code not in (200, 404):
            # transient/network failure; a bounded single retry
            time.sleep(2)
            code, out = _github_api_get(
                f"/repos/{owner}/{repo_name}/branches/{urllib.parse.quote(branch, safe='')}",
                token=token,
                timeout=30,
            )
        if code != 200:
            record["method"] = "github_api"
            record["error"] = (
                f"github api branch read failed: HTTP {code}: {out.strip()[:150]}"
            )
            return record
        try:
            payload = json.loads(out)
            remote_sha = str(payload.get("commit", {}).get("sha", ""))
        except Exception as exc:  # noqa: BLE001
            record["method"] = "github_api"
            record["error"] = f"could not parse branch payload: {exc}"[:200]
            return record
        record["method"] = "github_api"
        record["independent_remote_read"] = True
        record["remote_sha"] = remote_sha
        record["ok"] = remote_sha == local
        if not record["ok"]:
            record["error"] = (
                f"remote branch HEAD {remote_sha[:12]} != local {local[:12]}"
            )
        return record

    def _current_branch(self) -> str:
        code, out = self._run("rev-parse", "--abbrev-ref", "HEAD", timeout=30)
        return out.strip() if code == 0 else ""

    def _fetch_verify_local(
        self, ref: str, local: str, record: dict[str, Any]
    ) -> dict[str, Any]:
        """Honest local-only fallback; never claims an independent read.

        Renamed and scoped deliberately (follow-up workstream C): the previous
        implementation reported ``ok`` from a local ``git fetch --dry-run`` +
        ``branch -r --contains``, which some consumers could read as an
        independent GitHub check. This fallback keeps the same local logic but
        labels it ``local_git`` with ``independent_remote_read=False``.
        """
        record.setdefault("method", "local_git")
        record["method"] = "local_git"
        record["independent_remote_read"] = False
        code, out = self._run("fetch", "--dry-run", "origin", timeout=180)
        record["fetch_exit"] = code
        if code != 0:
            record["error"] = (record.get("error", "") + " | local fetch failed").strip(" |")
            return record
        code, out = self._run("branch", "-r", "--contains", local, timeout=120)
        if code == 0:
            branches = [b.strip() for b in out.splitlines() if "origin/" in b]
            record["remote_contains"] = branches[:4]
            record["ok"] = bool(branches)
        else:
            code, out = self._run("ls-remote", "origin", timeout=120)
            record["ok"] = local in out
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
        probe_spec: Any = None,
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
        # Task-declared runtime probe spec (allowlisted handlers only; see
        # dispatcher/probes.py). None/'' means no probe: T2+ then fails closed
        # with a recorded reason. A malformed spec yields BLOCKED.
        self.probe_spec = probe_spec
        self._probe_error = ""
        try:
            self._probe_runner = _build_probe(probe_spec, self.repo_path, logger=None)
        except ValueError as exc:
            self._probe_runner = None
            self._probe_error = str(exc)

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
        if not changed:
            # Empty diff: nothing was changed in the workspace, so there is
            # genuinely nothing to verify for this tier. This is a
            # not-applicable result that collapses to PASS (governance: a
            # required set that collapsed entirely to not-applicable is a
            # PASS), e.g. repo-backed echo tasks. A non-empty diff with no
            # mappable tests stays PARTIAL (fail-closed) per the governance
            # tests.
            return CheckResult(
                check_id="targeted_tests",
                status="PARTIAL",
                command="(none selected)",
                output="no changes to verify",
                skip_reason="not applicable: no changes in the workspace",
                started_at=started,
            )
        # Only Python files can be pytest targets: a markdown or json file
        # whose *name* contains "test" (task notes, evidence reports) made
        # pytest exit 4 (usage error), which the verdict read as FAIL even
        # though no test had failed.
        python_files = [p for p in changed if p.endswith(".py")]
        test_files = [p for p in python_files if "test" in Path(p).name.lower()]
        if not test_files:
            # no test touched by the diff: map source modules to their tests
            src = [p for p in python_files if "test" not in Path(p).name.lower()]
            for s in src:
                stem = Path(s).stem
                cand = [
                    f"tests/test_{stem}.py",
                    f"tests/{stem}_test.py",
                    f"{Path(s).parent}/test_{stem}.py",
                ]
                test_files += [c for c in cand if (self.repo_path / c).exists()]
        if not test_files:
            if not python_files:
                # Issue #43: a diff that carries no Python at all (docs,
                # prompts, data) has nothing this check could ever run. It is
                # explicitly not-applicable — a PARTIAL that collapses to PASS
                # under the governance rule — not a false test failure. A diff
                # that DOES touch code but maps to no test files stays
                # fail-closed below: a code change with no applicable tests
                # is not proven.
                return CheckResult(
                    check_id="targeted_tests",
                    status="PARTIAL",
                    command="(none selected)",
                    output="diff carries no testable code paths",
                    skip_reason=(
                        "not applicable: no testable code paths in diff "
                        "(docs/data only)"
                    ),
                    started_at=started,
                )
            return CheckResult(
                check_id="targeted_tests",
                status="PARTIAL",
                command="(none selected)",
                output="no test files map to the changed paths",
                started_at=started,
                skip_reason="no targeted tests applicable",
            )
        if runner is None:
            # Issue #43: the default runner is the dispatcher's own
            # interpreter. When pytest is genuinely unavailable under it, the
            # runner exits nonzero before any test executes, which used to be
            # recorded as a test FAILURE — a false failure that masks the real
            # owner action (install the runner). Probe once and classify the
            # missing runner as BLOCKED with a named reason; any other nonzero
            # exit still means a test ran and failed (FAIL, never masked).
            available, reason = self._default_runner_available()
            if not available:
                return CheckResult(
                    check_id="targeted_tests",
                    status="BLOCKED",
                    command=self._runner_probe_cmd,
                    output=reason,
                    skip_reason=f"test runner unavailable: {reason}"[:200],
                    started_at=started,
                )
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

    _runner_probe_cmd = "sys.executable -m pytest --version"

    def _default_runner_available(self) -> "tuple[bool, str]":
        """Whether the default targeted-tests runner can execute at all.

        A missing pytest (``No module named pytest``) makes the runner exit
        nonzero before any test runs; without this probe the gate recorded
        that as a test failure (Issue #43). Returns (available, reason) where
        reason is a short human-readable diagnostic for the bundle.
        """
        code, out = _run(
            [sys.executable, "-m", "pytest", "--version"],
            cwd=self.repo_path,
            timeout=120,
        )
        if code == 0:
            return True, ""
        detail = " ".join((out or "").split())[:160]
        return False, (
            f"{sys.executable} -m pytest --version exited {code}"
            + (f" ({detail})" if detail else "")
        )

    def _default_test_runner(self, test_files: list[str]) -> int:
        code, _ = _run(
            [sys.executable, "-m", "pytest", *test_files],
            cwd=self.repo_path,
            timeout=600,
        )
        return code

    def _probe_effective_runner(self):
        """Runner for the task's declared probe, or the reason it is unusable.

        Distinguishes the failure modes the governance prompt requires to
        fail closed: malformed spec (raises, -> BLOCKED), missing runner
        (None -> PARTIAL, fatal for T2+), and execution error (BLOCKED).
        """
        if self._probe_error:
            raise _MalformedProbe(self._probe_error)
        return self._probe_runner

    def check_runtime_probe(
        self,
        runner: "Callable[[], tuple[int, str, dict[str, Any]]] | None" = None,
    ) -> "CheckResult":
        """T2+: real API/log/DB/runtime probe built from the task's spec.

        ``runner`` is the zero-argument runner produced by
        :func:`dispatcher.probes.build_probe`; it returns
        ``(exit_code, output, evidence)`` where ``evidence`` carries the
        probe/target/environment identity recorded in the bundle.

        A missing probe is PARTIAL for T2+ (fatal: the task is not proven).
        A malformed probe spec, a probe timeout, or a probe that raises is
        BLOCKED — never DONE.
        """
        started = utcnow()
        if runner is None:
            return CheckResult(
                check_id="runtime_probe",
                status="PARTIAL",
                command="(none provided)",
                skip_reason="no runtime_probe defined for this task (required for T2+)",
                started_at=started,
            )
        try:
            result = runner()
            # Accept both 2-tuple (legacy) and 3-tuple (with evidence).
            if isinstance(result, tuple) and len(result) == 3:
                code, out, probe_evidence = result
            else:
                code, out = result
                probe_evidence = {}
        except _MalformedProbe:
            raise  # handled by the dispatch loop -> BLOCKED
        except Exception as exc:  # noqa: BLE001
            return CheckResult(
                check_id="runtime_probe",
                status="BLOCKED",
                command="(probe)",
                skip_reason=f"probe error: {type(exc).__name__}: {exc}"[:200],
                started_at=started,
            )
        return CheckResult(
            check_id="runtime_probe",
            status="PASS" if code == 0 else "FAIL",
            command=f"probe:{probe_evidence.get('handler', '?')}",
            exit_code=code,
            output=redact(out),
            evidence_ref=str(probe_evidence.get("target", "")),
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
        probe: "Callable[[], tuple[int, str, dict[str, Any]]] | None" = None,
        test_runner: "Callable[[list[str]], int] | None" = None,
        precondition_plan: str = "",
        rollback_plan: str = "",
        evidence_path: "Path | None" = None,
    ) -> EvidenceBundle:
        """Execute one verification pass. Returns the bundle (not yet written).

        ``probe`` is accepted for backward compatibility; the probe is now
        built from the task's ``probe_spec`` (see :mod:`dispatcher.probes`).
        """
        if probe is not None and self.probe_spec is None:
            # A legacy injected runner was passed with no task spec: use it
            # directly so old callers keep working.
            self._probe_runner = probe
            self._probe_error = ""
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
            "runtime_probe": lambda: self.check_runtime_probe(
                self._probe_effective_runner()
            ),
            "preconditions": lambda: self.check_preconditions(precondition_plan),
            "rollback_plan": lambda: self.check_rollback_plan(rollback_plan),
        }
        runners.update(self.custom_checks)

        # Probe target identity gates reuse of runtime_probe evidence: probe
        # evidence must never be reused across a different target.
        probe_target = ""
        if self.probe_spec is not None:
            try:
                probe_target = str(_validate_probe_spec(self.probe_spec).get("target", ""))
            except Exception:
                probe_target = ""

        for cid in required:
            # Reuse must be decided BEFORE the check runs: the point of the
            # cache is that the expensive command is never executed again
            # while the identity (task/check/commit/env/contract) is current.
            cache_key = bundle.identity_key(
                cid, probe_target if cid == "runtime_probe" else ""
            )
            cached = self._reusable(cache_key)
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
            try:
                checks.append(runner_fn())
            except _MalformedProbe as exc:
                # The task's probe spec failed contract validation: never DONE.
                checks.append(
                    CheckResult(
                        check_id=cid,
                        status="BLOCKED",
                        command="(probe spec)",
                        skip_reason=f"malformed probe spec: {exc}"[:200],
                        started_at=utcnow(),
                    )
                )
                continue

        bundle.checks = [redact(c.to_dict()) for c in checks]

        # Record probe identity (target/environment) in the bundle so probe
        # evidence can never be silently reused across a different target.
        for c in checks:
            if c.check_id == "runtime_probe" and c.status == "PASS":
                bundle.runtime_probe = {
                    "probe_id": c.evidence_ref,
                    "handler": c.command.removeprefix("probe:"),
                    "target": c.evidence_ref,
                }
                break

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
                    cache[bundle.identity_key(
                        c.check_id,
                        probe_target if c.check_id == "runtime_probe" else "",
                    )] = {
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
