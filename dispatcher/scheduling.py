"""Conflict-aware parallel scheduling (v4).

Extends the existing Dispatcher queue with resource/dependency manifests so
admission is decided on *actual* file/module/API/schema/runtime conflicts,
not only on the coarse ``parallel_safe`` hint, explicit dependencies,
workstream serialization and the max-concurrency limit.

Owner direction 2026-11-06 (Issue #80, comment 2; contract v4 §12):

- For each READY task, derive a resource manifest: repo/branch/baseline SHA,
  declared dependencies, expected and actual write paths, read paths,
  contracts (API routes/schemas/lockfiles/migrations/generated outputs/config)
  and shared mutable resources (DB tables, runtime/deployment targets).
- Detect write/write overlaps, write/read producer-consumer edges, shared
  contracts, shared cross-repo resources, unscoped-overlap ambiguity and
  baseline drift.
- Return exactly one decision per task:
    PARALLEL            no conflict, no unmet dependency
    SERIAL_AFTER(task)  wait for the predecessor's verified completion,
                        fetch the new remote HEAD, refresh the baseline,
                        re-evaluate
    SERIAL_LOCK(res)    one writer holds the shared mutable resource
    BLOCKED             ownership ambiguous / independence unprovable
- ``parallel_safe`` is only a coarse hint: any detected conflict overrides
  it. Explicit dependencies remain enforced. Decisions are recomputed
  immediately before claim and after resource manifests/diffs change.
- Overlap appearing mid-run blocks *new* conflicting claims; it never kills
  active work. Lock acquisition/release and restart recovery are idempotent;
  stale locks are released only after the owner task is terminal and no live
  lease remains.
- Every comparison is persisted to the existing SQLite state DB
  (``scheduling_decisions`` / ``resource_locks``), keyed so duplicate ticks
  with unchanged inputs are no-ops.

No second queue, no new schedule, no concurrency increase: admission happens
inside the existing claim loop, one tick at a time.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .registry import Task
from .state import TERMINAL, StateStore, TaskState, pid_alive

__all__ = [
    "PARALLEL",
    "SERIAL_AFTER",
    "SERIAL_LOCK",
    "BLOCKED",
    "Conflict",
    "ResourceManifest",
    "SchedulingDecision",
    "SchedulingAdmitter",
    "normalize_shared_resource",
    "paths_overlap",
    "classify_contracts",
]

PARALLEL = "PARALLEL"
SERIAL_AFTER = "SERIAL_AFTER"
SERIAL_LOCK = "SERIAL_LOCK"
BLOCKED = "BLOCKED"

# --- contract (shared-contract) classification --------------------------------

_LOCKFILE_NAMES = frozenset({
    "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "bun.lockb",
    "requirements.txt", "pyproject.toml", "setup.py", "setup.cfg",
    "cargo.toml", "cargo.lock", "go.mod", "go.sum", "pom.xml",
    "build.gradle", "build.gradle.kts", "gemfile.lock", "poetry.lock",
    "uv.lock", "composer.lock",
})
_MIGRATION_RX = re.compile(r"(^|/)migrations?/|(^|/)alembic/|(^|/)migrate[-_]", re.I)
_SCHEMA_RX = re.compile(
    r"(\.schema\.json$|\.prisma$|schema\.sql$|(^|/)openapi\.(ya?ml|json)$|(^|/)proto/)", re.I
)
_GENERATED_RX = re.compile(r"(^|/)__generated__/|(^|/)generated/|^dist/|^build/|^node_modules/", re.I)
_API_RX = re.compile(r"^(routes|api|controllers|app)/|/routes\.py$|/views/|/serializers/", re.I)
_CONFIG_RX = re.compile(r"^config/|^\.env($|\.)|(^|/)docker-compose.*\.ya?ml$|^Makefile$", re.I)


def normalize_shared_resource(name: str) -> str:
    """Normalize a declared shared mutable resource to a stable key.

    ``runtime:dokploy-nginx`` -> ``shared:runtime:dokploy-nginx``;
    ``marketdata-db`` -> ``shared:marketdata-db``. Lowercase, single spaces,
    so ``DB: marketdata`` and ``db:MarketData`` collide (case-folded).
    """
    n = " ".join(str(name or "").strip().lower().split())
    if not n:
        return ""
    return n if ":" in n else f"shared:{n}"


def paths_overlap(a: str, b: str) -> bool:
    """File-or-directory prefix overlap on normalized relative paths."""
    a, b = a.strip("/"), b.strip("/")
    if not a or not b:
        return False
    return a == b or a.startswith(b + "/") or b.startswith(a + "/")


def classify_contracts(repo: str, paths: Iterable[str]) -> set[str]:
    """Map paths to normalized shared-contract keys for this repo."""
    out: set[str] = set()
    for p in paths:
        p = p.strip("/")
        if not p:
            continue
        base = p.rsplit("/", 1)[-1].lower()
        if base in _LOCKFILE_NAMES:
            out.add(f"contract:{repo}:lockfile:{base}")
        elif _MIGRATION_RX.search(p):
            out.add(f"contract:{repo}:migration:{p}")
        elif _SCHEMA_RX.search(p):
            out.add(f"contract:{repo}:schema:{p}")
        elif _GENERATED_RX.search(p):
            out.add(f"contract:{repo}:generated:{p}")
        elif _API_RX.search(p):
            out.add(f"contract:{repo}:api:{p}")
        elif _CONFIG_RX.search(p):
            out.add(f"contract:{repo}:config:{base}")
    return out


# --- manifest -------------------------------------------------------------------


@dataclass(frozen=True)
class ResourceManifest:
    task_id: str
    repo: str
    branch: str
    baseline_sha: str
    write_paths: frozenset[str]
    read_paths: frozenset[str]
    shared_resources: frozenset[str]
    contracts: frozenset[str]
    unscoped: bool
    source: str  # "declared" | "declared+actual" | "none"
    manifest_sha: str

    def resource_keys(self) -> list[str]:
        keys: list[str] = []
        if self.repo:
            keys.append(f"repo:{self.repo}")
        keys.extend(sorted(self.contracts))
        keys.extend(sorted(self.shared_resources))
        keys.extend(f"write:{p}" for p in sorted(self.write_paths))
        keys.extend(f"read:{p}" for p in sorted(self.read_paths))
        if self.unscoped and self.repo:
            keys.append(f"unscoped:{self.repo}:{self.branch}")
        return keys


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def derive_manifest(
    task: Task,
    *,
    baseline_sha: str = "",
    actual_write_paths: Iterable[str] | None = None,
) -> ResourceManifest:
    """Build the normalized resource manifest for one task.

    Expected writes come from the declared ``write_paths``; actual writes
    (mid-run ``git status`` of the task's workspace) are merged in so a
    write set that grows during a run immediately blocks conflicting claims.
    A repo task with no declared *and* no derivable write set is ``unscoped``:
    its write surface cannot be proven, which is a BLOCKED input against any
    other task in the same repo (safe independence cannot be proven).
    """
    declared_w = {p.strip("/") for p in (task.write_paths or []) if p.strip("/")}
    declared_r = {p.strip("/") for p in (task.read_paths or []) if p.strip("/")}
    shared = {normalize_shared_resource(x) for x in (task.shared_resources or [])
              if normalize_shared_resource(x)}
    actual = {p.strip("/") for p in (actual_write_paths or []) if p.strip("/")}
    writes = declared_w | actual
    # Reading a path you write is always true; keep read set explicit.
    reads = declared_r
    source = "declared"
    if actual:
        source = "declared+actual"
    repo = task.repo
    branch = task.branch
    if task.is_artifact_task:
        # Non-repository artifact task (Issue #99): its real write surface
        # is the artifact store on its isolated branch (the task's own repo
        # field is empty by contract). The deliverable is a declared write,
        # and the store+branch pair is a shared mutable resource, so two
        # artifact tasks on the same branch always serialize.
        repo = task.artifact_store
        branch = task.artifact_branch
        writes.add(task.deliverable.strip("/"))
        shared.add(normalize_shared_resource(
            f"branch:{task.artifact_store}:{task.artifact_branch}"
        ))
    if not repo and not writes and not reads and not shared:
        source = "none"
    unscoped = bool(repo) and not writes and not actual and not declared_w
    contracts = classify_contracts(repo, writes) | classify_contracts(repo, reads)
    body = {
        "task_id": task.id, "repo": repo, "branch": branch,
        "baseline_sha": baseline_sha or "", "w": sorted(writes),
        "r": sorted(reads), "s": sorted(shared), "c": sorted(contracts),
        "unscoped": unscoped,
    }
    return ResourceManifest(
        task_id=task.id, repo=repo, branch=branch,
        baseline_sha=baseline_sha or "",
        write_paths=frozenset(writes), read_paths=frozenset(reads),
        shared_resources=frozenset(shared), contracts=frozenset(contracts),
        unscoped=unscoped, source=source,
        manifest_sha=hashlib.sha256(_canonical(body).encode()).hexdigest()[:32],
    )


# --- conflicts -------------------------------------------------------------------


@dataclass(frozen=True)
class Conflict:
    kind: str  # write_write | write_read | shared_contract | shared_resource
    #              | unscoped_overlap
    path: str
    other_task: str
    producer: str = ""  # for write_read: the task that writes
    reason: str = ""


def _pair_conflicts(a: ResourceManifest, b: ResourceManifest) -> list[Conflict]:
    """Conflicts visible from task ``a``'s point of view against ``b``."""
    out: list[Conflict] = []
    if a.repo and b.repo and a.repo == b.repo:
        for wa in a.write_paths:
            for wb in b.write_paths:
                if paths_overlap(wa, wb):
                    out.append(Conflict("write_write", wa, b.task_id,
                                        reason=f"both write {wa}"))
        for ra in a.read_paths:
            for wb in b.write_paths:
                if paths_overlap(ra, wb):
                    out.append(Conflict("write_read", ra, b.task_id,
                                        producer=b.task_id,
                                        reason=f"{b.task_id} produces {ra} which {a.task_id} reads"))
        for wa in a.write_paths:
            for rb in b.read_paths:
                if paths_overlap(wa, rb):
                    out.append(Conflict("write_read", wa, b.task_id,
                                        producer=a.task_id,
                                        reason=f"{a.task_id} modifies {wa} which {b.task_id} (running) reads"))
        if a.unscoped or b.unscoped:
            un = a if a.unscoped else b
            other = b if un is a else a
            out.append(Conflict("unscoped_overlap", "", un.task_id,
                                reason=f"{un.task_id} is unscoped in {un.repo}:{un.branch or 'main'}; "
                                       f"its write surface cannot be proven, so safe "
                                       f"independence from {other.task_id} cannot be established"))
    common = (a.contracts & b.contracts)
    for key in sorted(common):
        out.append(Conflict("shared_contract", key, b.task_id,
                            reason=f"shared contract {key}"))
    shared_common = a.shared_resources & b.shared_resources
    for key in sorted(shared_common):
        out.append(Conflict("shared_resource", key, b.task_id,
                            reason=f"shared mutable resource {key}"))
    return out


# --- decision ---------------------------------------------------------------------


@dataclass
class SchedulingDecision:
    task_id: str
    decision: str
    predecessor: str = ""
    resource: str = ""
    lock_owner: str = ""
    reasons: list[str] = field(default_factory=list)
    compared_task_ids: list[str] = field(default_factory=list)
    baseline_sha: str = ""
    manifest_sha: str = ""
    wait_condition: str = ""

    def to_record(self, run_id: str = "") -> dict[str, Any]:
        return {
            "ts": time.time(), "run_id": run_id,
            "task_id": self.task_id, "decision": self.decision,
            "predecessor": self.predecessor, "resource": self.resource,
            "lock_owner": self.lock_owner,
            "compared_task_ids": _canonical(self.compared_task_ids),
            "baseline_sha": self.baseline_sha,
            "manifest_sha": self.manifest_sha,
            "resource_keys": _canonical(self._resource_keys or []),
            "conflict_paths": _canonical(self._conflict_paths or []),
            "reasons": _canonical(self.reasons),
            "wait_condition": self.wait_condition,
            "reeval": "",
        }


# --- admitter ----------------------------------------------------------------------


def _github_git_env() -> dict[str, str]:
    """Pass an existing GitHub token to git through environment config only.

    The credential never appears in argv, URL, file, or logs. Public repositories
    continue to work without a token; private repositories can resolve HEAD when
    the dispatcher inherited its normal GITHUB_TOKEN/GH_TOKEN environment.
    """
    env = dict(os.environ)
    token = env.get("GITHUB_TOKEN") or env.get("GH_TOKEN")
    if not token:
        return env
    try:
        count = max(0, int(env.get("GIT_CONFIG_COUNT", "0") or 0))
    except ValueError:
        count = 0
    encoded = base64.b64encode(f"x-access-token:{token}".encode()).decode("ascii")
    env[f"GIT_CONFIG_KEY_{count}"] = "http.https://github.com/.extraheader"
    env[f"GIT_CONFIG_VALUE_{count}"] = f"AUTHORIZATION: basic {encoded}"
    env["GIT_CONFIG_COUNT"] = str(count + 1)
    return env


class SchedulingAdmitter:
    """Conflicts-aware admission inside the existing claim loop.

    All state lives in the dispatcher's existing SQLite store; the admitter
    never claims, kills or reorders work on its own — it only reports the one
    decision per task and manages the resource locks that make serialization
    durable and restart-safe.
    """

    def __init__(self, store: StateStore, logger: Any = None) -> None:
        self.store = store
        self.log = logger or (lambda level, msg: None)

    # -- manifest refresh -----------------------------------------------------

    @staticmethod
    def git_changed_paths(workspace: Path) -> list[str]:
        """Actual in-flight write set of a running task's workspace."""
        try:
            proc = subprocess.run(
                ["git", "-C", str(workspace), "status", "--porcelain"],
                capture_output=True, text=True, timeout=30, check=False,
            )
            if proc.returncode != 0:
                return []
        except Exception:
            return []
        out: list[str] = []
        for line in proc.stdout.splitlines():
            if len(line) < 4:
                continue
            code, rest = line[:2], line[3:]
            if code == "??":
                out.append(rest)
                continue
            if code.startswith("R"):
                # "old -> new"
                if " -> " in rest:
                    rest = rest.rsplit(" -> ", 1)[-1]
            out.append(rest.strip('"'))
        return [p for p in out if p]

    def refresh_manifest(self, task: Task, state: TaskState | None) -> ResourceManifest:
        """Manifest for a RUNNING task, merging declared + actual writes."""
        actual: list[str] = []
        if state is not None and state.workspace_path:
            ws = Path(state.workspace_path)
            if ws.is_dir():
                actual = self.git_changed_paths(ws)
        baseline = (state.baseline_sha if state is not None else "") or ""
        return derive_manifest(task, baseline_sha=baseline,
                               actual_write_paths=actual)

    def refresh_remote_head(self, task: Task, state: TaskState | None,
                            control: Any = None) -> str | None:
        """Fetch the current remote HEAD of the task's branch (read-only).

        Used before claim so every admission decision is evaluated against
        the branch tip, and again after a predecessor completes (baseline
        refresh). Never modifies worktrees:
          1. the task's existing workspace checkout: `git fetch` + rev-parse;
          2. the control-repo checkout when the task targets the control repo
             (its per-tick sync already fetched all remote branches);
          3. a read-only `git ls-remote` for any other repository.
        Returns None when the remote cannot be probed (the caller then
        refuses to prove baseline freshness and fails closed where that
        matters).
        """
        if not task.repo:
            return None
        branch = task.branch or "main"
        ws = Path(state.workspace_path) if state is not None and state.workspace_path else None
        if ws is not None and ws.is_dir():
            try:
                fetched = subprocess.run(
                    ["git", "-C", str(ws), "fetch", "--quiet", "origin", branch],
                    capture_output=True, text=True, timeout=60, check=False,
                    env=_github_git_env())
                if fetched.returncode == 0:
                    proc = subprocess.run(
                        ["git", "-C", str(ws), "rev-parse", "--verify",
                         f"refs/remotes/origin/{branch}"],
                        capture_output=True, text=True, timeout=30, check=False)
                    if proc.returncode == 0 and len(proc.stdout.strip()) == 40:
                        return proc.stdout.strip()
            except Exception:
                pass
        if control is not None:
            try:
                remote = control.remote_url()
            except Exception:
                remote = ""
            slug = str(task.repo).lower().replace(".git", "")
            if slug in remote.lower():
                try:
                    proc = subprocess.run(
                        ["git", "-C", str(control.repo_dir), "rev-parse", "--verify",
                         f"refs/remotes/origin/{branch}"],
                        capture_output=True, text=True, timeout=30, check=False)
                    if proc.returncode == 0 and len(proc.stdout.strip()) == 40:
                        return proc.stdout.strip()
                except Exception:
                    pass
        try:
            url = f"https://github.com/{task.repo}.git"
            proc = subprocess.run(["git", "ls-remote", url, branch],
                                  capture_output=True, text=True, timeout=60,
                                  check=False, env=_github_git_env())
            if proc.returncode == 0:
                for line in proc.stdout.splitlines():
                    parts = line.split()
                    if parts and len(parts[0]) == 40:
                        return parts[0]
        except Exception:
            pass
        return None

    def candidate_manifest(self, task: Task, state: TaskState | None,
                           remote_head: str | None) -> ResourceManifest:
        """Manifest for a READY candidate after baseline refresh.

        ``remote_head`` is the freshly fetched branch head (None when the
        remote could not be probed). The baseline is advanced to it so the
        task always evaluates against the current branch tip.
        """
        baseline = ""
        if state is not None:
            baseline = state.baseline_sha or ""
        if remote_head:
            baseline = remote_head
        return derive_manifest(task, baseline_sha=baseline)

    # -- decision -------------------------------------------------------------

    @staticmethod
    def order_candidates(tasks: list, manifests: dict) -> tuple[list, set]:
        """Stable topological order; every node in a dependency cycle is blocked."""
        order = list(tasks)
        if len(order) < 2:
            return order, set()

        def produces(producer, consumer) -> bool:
            if producer.id == consumer.id:
                return False
            if not producer.repo or producer.repo != consumer.repo:
                return False
            mw, mr = manifests.get(producer.id), manifests.get(consumer.id)
            if not mw or not mr or not mr.read_paths:
                return False
            return any(paths_overlap(wa, rb)
                       for wa in mw.write_paths for rb in mr.read_paths)

        original_index = {task.id: i for i, task in enumerate(order)}
        by_id = {task.id: task for task in order}
        edges: dict[str, set[str]] = {task.id: set() for task in order}
        indegree: dict[str, int] = {task.id: 0 for task in order}
        for producer in order:
            for consumer in order:
                if produces(producer, consumer) and consumer.id not in edges[producer.id]:
                    edges[producer.id].add(consumer.id)
                    indegree[consumer.id] += 1

        ready = sorted(
            (task_id for task_id, degree in indegree.items() if degree == 0),
            key=lambda task_id: original_index[task_id],
        )
        emitted: list[str] = []
        while ready:
            task_id = ready.pop(0)
            emitted.append(task_id)
            for consumer_id in sorted(edges[task_id], key=lambda x: original_index[x]):
                indegree[consumer_id] -= 1
                if indegree[consumer_id] == 0:
                    ready.append(consumer_id)
                    ready.sort(key=lambda x: original_index[x])

        cyclic = set(by_id) - set(emitted)
        emitted.extend(sorted(cyclic, key=lambda task_id: original_index[task_id]))
        return [by_id[task_id] for task_id in emitted], cyclic

    def admit(
        self,
        task: Task,
        manifest: ResourceManifest,
        *,
        states: dict[str, TaskState],
        running_manifests: dict[str, ResourceManifest],
        admitted: dict[str, ResourceManifest],
        inbox: Any,
        run_id: str = "",
        reeval: str = "",
    ) -> SchedulingDecision:
        """One explicit admission decision for ``task``.

        Priority: BLOCKED > SERIAL_AFTER > SERIAL_LOCK > PARALLEL.
        """
        compared: list[str] = []
        reasons: list[str] = []
        conflict_paths: list[str] = []
        after_pred = ""
        lock_resource = ""
        lock_owner = ""

        # 1) Explicit dependencies remain enforced (all must be DONE).
        index = inbox.by_id()
        for dep_id in task.dependencies:
            dep_task = index.get(dep_id)
            if dep_task is None:
                # Unknown dependency: the graph is ambiguous.
                return self._finish(task, manifest, BLOCKED, compared=compared,
                                    reasons=[f"dependency {dep_id} is not in the inbox "
                                             f"(ambiguous dependency graph)"],
                                    run_id=run_id, reeval=reeval)
            dep_state = states.get(dep_id)
            dep_status = dep_state.status if dep_state else dep_task.status
            compared.append(dep_id)
            if dep_status != "DONE":
                after_pred = after_pred or dep_id
                reasons.append(f"explicit dependency {dep_id} is {dep_status} (needs DONE)")

        # 2) RUNNING tasks: live conflict analysis.
        for rid, rman in running_manifests.items():
            compared.append(rid)
            for c in _pair_conflicts(manifest, rman):
                if c.path:
                    conflict_paths.append(c.path)
                if c.kind == "unscoped_overlap":
                    return self._finish(
                        task, manifest, BLOCKED, compared=compared,
                        reasons=[f"{c.reason} (vs running {rid})"],
                        conflict_paths=conflict_paths,
                        baseline_sha=manifest.baseline_sha, run_id=run_id, reeval=reeval,
                    )
                if c.kind == "write_read" and c.producer == task.id:
                    # The running task reads a contract this candidate would
                    # modify: it cannot be killed, and the running task would
                    # consume a stale version. Bounded wait: hold back the new
                    # claim until the running task is verified terminal, then
                    # refresh the baseline and re-evaluate against its result.
                    after_pred = after_pred or rid
                    reasons.append(f"running {rid} reads {c.path} which this task would "
                                   f"modify; kill is not allowed — wait for its verified "
                                   f"completion, then refresh baseline and re-evaluate")
                    continue
                if c.kind == "write_read" and c.producer == rid:
                    after_pred = after_pred or rid
                    reasons.append(f"producer {rid} writes {c.path} which this task reads; "
                                   f"wait for its verified completion + baseline refresh")
                    continue
                if c.kind in ("write_write", "shared_contract", "shared_resource"):
                    key = c.path if c.kind != "shared_resource" else c.path
                    lock_resource = lock_resource or key
                    lock_owner = rid
                    reasons.append(f"{c.kind} with running {rid}: {c.reason}")

        # 3) Other candidates admitted earlier in this same tick.
        for aid, aman in admitted.items():
            compared.append(aid)
            for c in _pair_conflicts(manifest, aman):
                if c.path:
                    conflict_paths.append(c.path)
                if c.kind == "unscoped_overlap":
                    return self._finish(
                        task, manifest, BLOCKED, compared=compared,
                        reasons=[f"{c.reason} (vs {aid}, admitted this tick)"],
                        conflict_paths=conflict_paths,
                        baseline_sha=manifest.baseline_sha, run_id=run_id, reeval=reeval,
                    )
                if c.kind == "write_read" and c.producer == task.id:
                    # Both READY: the other is admitted and will run; the
                    # candidate would modify what the other reads. Deterministic
                    # order: the earlier (higher priority) wins; the later one
                    # waits for it.
                    after_pred = after_pred or aid
                    reasons.append(f"{aid} (admitted this tick) reads {c.path} which this "
                                   f"task would modify; serialize after it")
                    continue
                if c.kind == "write_read" and c.producer == aid:
                    after_pred = after_pred or aid
                    reasons.append(f"producer {aid} (admitted this tick) writes {c.path} "
                                   f"which this task reads")
                    continue
                if c.kind in ("write_write", "shared_contract", "shared_resource"):
                    key = c.path
                    lock_resource = lock_resource or key
                    lock_owner = lock_owner or aid
                    reasons.append(f"{c.kind} with {aid} (admitted this tick): {c.reason}")

        if after_pred:
            return self._finish(
                task, manifest, SERIAL_AFTER, predecessor=after_pred,
                compared=compared, reasons=reasons, conflict_paths=conflict_paths,
                baseline_sha=manifest.baseline_sha,
                wait_condition=f"predecessor {after_pred} DONE + remote HEAD fetched + "
                               f"baseline refreshed + re-evaluation",
                run_id=run_id, reeval=reeval,
            )
        if lock_resource:
            return self._finish(
                task, manifest, SERIAL_LOCK, resource=lock_resource,
                lock_owner=lock_owner, compared=compared, reasons=reasons,
                conflict_paths=conflict_paths,
                baseline_sha=manifest.baseline_sha,
                wait_condition=f"resource {lock_resource} free (owner {lock_owner or '?'})",
                run_id=run_id, reeval=reeval,
            )
        return self._finish(task, manifest, PARALLEL, compared=compared,
                            reasons=reasons, conflict_paths=conflict_paths,
                            baseline_sha=manifest.baseline_sha,
                            run_id=run_id, reeval=reeval)

    def _finish(self, task: Task, manifest: ResourceManifest, decision: str, *,
                predecessor: str = "", resource: str = "", lock_owner: str = "",
                compared: list[str], reasons: list[str],
                conflict_paths: list[str] | None = None, baseline_sha: str = "",
                wait_condition: str = "", run_id: str = "", reeval: str = "") -> SchedulingDecision:
        d = SchedulingDecision(
            task_id=task.id, decision=decision, predecessor=predecessor,
            resource=resource, lock_owner=lock_owner, reasons=reasons,
            compared_task_ids=compared,
            baseline_sha=baseline_sha or manifest.baseline_sha,
            manifest_sha=manifest.manifest_sha, wait_condition=wait_condition,
        )
        d._resource_keys = manifest.resource_keys()  # type: ignore[attr-defined]
        d._conflict_paths = sorted(set(conflict_paths or []))  # type: ignore[attr-defined]
        rec = d.to_record(run_id=run_id)
        if reeval:
            rec["reeval"] = reeval
        try:
            inserted = self.store.record_scheduling_decision(rec)
        except Exception as exc:
            self.log("error", f"scheduling decision persist failed for {task.id}: {exc}")
            d.decision = BLOCKED
            d.reasons.append(
                f"scheduling decision could not be durably recorded; fail closed: {type(exc).__name__}"
            )
            return d
        if not inserted and not reeval:
            self.log("debug", f"{task.id}: decision unchanged ({decision}); no new ledger row")
        elif reeval:
            self.log("info", f"{task.id}: re-evaluated after {reeval} -> {decision}")
        else:
            self.log("info", f"{task.id}: scheduling decision {decision}"
                             + (f" (after={predecessor})" if predecessor else "")
                             + (f" (lock={resource} owner={lock_owner})" if resource else ""))
        return d

    # -- locks -----------------------------------------------------------------

    def lock_resources(self, task: Task, manifest: ResourceManifest) -> list[str]:
        """Acquire durable locks for the resources a claimed task owns.

        A claimed (RUNNING) task is the single writer of its write paths and
        shared resources, so concurrent claims of the same resource serialize
        on these locks. Idempotent: re-acquiring an own lock is a no-op.
        """
        resources: set[str] = set(manifest.shared_resources)
        resources |= manifest.contracts
        if manifest.repo:
            for p in manifest.write_paths:
                resources.add(f"write:{manifest.repo}:{p}")
        owned: list[str] = []
        for res in sorted(resources):
            # write:/contract: keys are already canonical; shared: resources
            # are normalized in the manifest.
            got, owner = self.store.acquire_resource_lock(
                task.id, res, lease_seconds=self._lock_lease(task)
            )
            if not got:
                # A concurrent tick may have won the race after admission.
                # Roll back any partial locks and fail closed; the caller must
                # return the task to READY without invoking its worker.
                self.store.release_resource_locks(task.id)
                raise RuntimeError(
                    f"resource lock {res!r} is held by task {owner!r}; "
                    "claim must be deferred and re-evaluated next tick"
                )
            owned.append(res)
        return owned

    def _lock_lease(self, task: Task) -> int:
        # One tick of slack beyond the task's own run budget: a stale lease is
        # only reclaimable after the owner task is terminal anyway.
        return max(int(task.timeout_seconds or 900) + 600, 1200)

    def release_resources(self, task_id: str) -> int:
        return self.store.release_resource_locks(task_id)

    def record_blocked(self, task: Task, manifest: ResourceManifest,
                       reason: str, *, reeval: str = "") -> SchedulingDecision:
        """Persist a BLOCKED decision made outside the pair loop.

        Used for circular read/write pairs, which no pairwise admit() call
        can see: each task only sees the other one, and the cycle is a
        property of the tick's candidate set as a whole.
        """
        return self._finish(task, manifest, BLOCKED, compared=[],
                            reasons=[reason], baseline_sha=manifest.baseline_sha,
                            reeval=reeval)
