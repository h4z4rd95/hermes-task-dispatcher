"""Per-task runtime probe contract for the verification gate.

A *runtime probe* is the piece of evidence that a T2+ (integration / service /
data pipeline) task actually works at runtime — not merely that its tests
passed or its files exist. Governance prompt
``VERIFICATION_GATE_FOLLOWUP_RISK_PROBES_REMOTE_VERIFY_2026-10-15.md``
workstream B.

Design constraints (from the governance prompt):

* No arbitrary shell execution from task payloads. Probes are selected from a
  strict allowlist of named handlers; the task payload only supplies
  parameters, never a command.
* Probes are deterministic, read-only, and bounded (timeout enforced).
* A probe must demonstrate the acceptance condition *for that task*, not
  merely that some process is running. Every handler therefore returns a
  (ok, output, detail) triple where ``ok`` is the acceptance verdict.
* Probe evidence carries target/environment identity and obeys the same
  task/commit/environment/task-digest freshness rules as all other gate
  evidence (implemented in :mod:`dispatcher.verification_gate`).
* T0/T1 tasks never run a runtime probe unless their acceptance criteria
  explicitly require one.
* Secrets in probe output are redacted before persistence.


Task contract (TOML, inside ``[[task]]``)::

    runtime_probe = { handler = "http_get", url = "http://127.0.0.1:9999/health",
                      expect_status = 200, timeout_seconds = 5 }

Fields recognised in the spec:

``handler``      (required) name of an allowlisted handler, e.g. ``http_get``.
``expect_status``/``expect_contains`` acceptance condition for ``http_get``.
``timeout``     probe timeout in seconds (default 10, hard ceiling 120).
``target``      free-form target identity recorded in the evidence (e.g.
                 "leantime:api:health"). Required for evidence identity.
``id``          optional probe identifier for evidence identity.

Unknown keys, unknown handlers, or missing required keys yield a BLOCKED
probe (fail closed) instead of DONE.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import json
import os
import socket
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterable

__all__ = ["PROBE_HANDLERS", "ProbeOutcome", "build_probe", "validate_probe_spec"]

# Hard ceiling on any single probe, regardless of the task's asking timeout.
_PROBE_TIMEOUT_CEILING_S = 120
_PROBE_TIMEOUT_DEFAULT_S = 10


def _utcnow() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclasses.dataclass
class ProbeOutcome:
    """Result of one runtime probe, ready to record as gate evidence."""

    ok: bool
    probe_id: str
    handler: str
    target: str
    command: str
    output: str = ""
    exit_code: int | None = None
    started_at: str = ""
    duration_s: float = 0.0
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def _validation_error(msg: str) -> ValueError:
    return ValueError(f"runtime_probe: {msg}")


def _validate_paths(spec: dict[str, Any]) -> None:
    """Reject filesystem paths that escape the task workspace.

    Validation-time, before any probe runs: a path containing ``..``
    components or an absolute path is a contract violation. Handlers keep
    their own runtime containment checks as defense in depth.
    """
    for key in ("path",):
        raw = spec.get(key)
        if not raw:
            continue
        rel = str(raw)
        if rel.startswith("/") or "\\" in rel or ".." in Path(rel).parts:
            raise _validation_error(
                f"{key} must be a relative path inside the task workspace"
            )


def validate_probe_spec(spec: Any) -> dict[str, Any]:
    """Validate a probe spec against the contract. Raises ValueError.

    Returns the normalized spec dict on success. This is the boundary that
    makes "malformed probe" a BLOCKED result rather than an arbitrary
    execution risk.
    """
    if spec is None:
        raise _validation_error("no probe defined (required for T2+)")
    if isinstance(spec, str):
        spec = _maybe_json(spec)
    if not isinstance(spec, dict):
        raise _validation_error(f"spec must be a table, got {type(spec).__name__}")
    handler = str(spec.get("handler", "")).strip().lower()
    if not handler:
        raise _validation_error("handler is required")
    if handler not in PROBE_HANDLERS:
        raise _validation_error(
            f"handler {handler!r} not allowlisted (known: {sorted(PROBE_HANDLERS)})"
        )
    timeout_raw = spec.get("timeout", _PROBE_TIMEOUT_DEFAULT_S)
    try:
        timeout = int(timeout_raw)
    except (TypeError, ValueError) as exc:
        raise _validation_error(f"timeout must be an integer, got {timeout_raw!r}") from exc
    timeout = max(1, min(timeout, _PROBE_TIMEOUT_CEILING_S))
    target = str(spec.get("target", "")).strip()
    if not target:
        raise _validation_error("target identity is required for evidence identity")
    probe_id = str(spec.get("id", "")).strip() or f"{handler}:{target}"
    unknown = {k for k in spec if k not in _ALLOWED_SPEC_KEYS}
    if unknown:
        raise _validation_error(f"unknown key(s) {sorted(unknown)}")
    out = dict(spec)
    out["handler"] = handler
    out["timeout"] = timeout
    out["target"] = target
    out["id"] = probe_id
    _validate_paths(out)
    return out


def _maybe_json(text: str) -> Any:
    text = text.strip()
    if text.startswith("{"):
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise _validation_error(f"invalid JSON spec: {exc}") from exc
    raise _validation_error(f"spec string {text[:40]!r} is not a JSON object")


# --- allowlisted handlers ---------------------------------------------------
#
# A handler is a function (spec, workspace) -> (ok, output, detail). It must
# be read-only, deterministic, and safe to run in the dispatcher process.
# Only named handlers are reachable from a task payload — there is no shell
# escape hatch in this contract.


def _probe_http_get(spec: dict[str, Any], workspace: Path) -> tuple[bool, str, str]:
    """GET a URL and check the status code and/or a body substring.

    Local-loopback deterministic fixtures are the intended use; the handler
    also works against any read-only health endpoint the task is allowed to
    name. Redirects are NOT followed (they would widen the reachable surface).
    """
    url = str(spec.get("url", "")).strip()
    if not url:
        return False, "", "url is required"
    if not (url.startswith("http://") or url.startswith("https://")):
        return False, "", "url must be http(s)"
    expect_status = spec.get("expect_status")
    expect_contains = str(spec.get("expect_contains", "") or "")
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": "hermes-vg-probe"})
    detail_bits = []
    try:
        with urllib.request.urlopen(req, timeout=int(spec.get("timeout", 10))) as resp:
            status = int(resp.status)
            # bounded read: 4 KiB max, secrets redacted by the caller
            body = resp.read(4096).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        body = ""
        # a 4xx/5xx still answers the question "is this endpoint reachable";
        # the acceptance verdict depends on expect_status.
        detail_bits.append(f"http_error status={status}")
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
        return False, "", f"connection failed: {type(exc).__name__}: {exc}"[:200]
    detail_bits.insert(0, f"status={status}")
    ok = True
    if expect_status not in (None, ""):
        try:
            wanted = int(expect_status)
        except (TypeError, ValueError):
            return False, "", f"expect_status must be an integer, got {expect_status!r}"
        ok = ok and (status == wanted)
        detail_bits.append(f"expect_status={wanted}")
    if expect_contains:
        ok = ok and (expect_contains in body)
        detail_bits.append(f"expect_contains={'yes' if expect_contains in body else 'no'}")
    output = body[:512] if body else ""
    return ok, output, "; ".join(detail_bits)


def _probe_file_exists(spec: dict[str, Any], workspace: Path) -> tuple[bool, str, str]:
    """Check that a path inside the task workspace exists.

    Deliberately restricted to the workspace: a task may not probe arbitrary
    filesystem locations (no /etc/passwd, no secrets paths).
    """
    rel = str(spec.get("path", "")).strip()
    if not rel:
        return False, "", "path is required"
    if rel.startswith("/") or ".." in Path(rel).parts:
        return False, "", "path must be relative to the task workspace"
    candidate = (workspace / rel).resolve()
    try:
        candidate.relative_to(workspace.resolve())
    except ValueError:
        return False, "", "path escapes the task workspace"
    exists = candidate.exists()
    return exists, f"path={rel} exists={exists}", f"exists={exists}"


def _probe_tcp_connect(spec: dict[str, Any], workspace: Path) -> tuple[bool, str, str]:
    """Open a TCP connection to host:port and close it immediately.

    A pure reachability probe with no payload; used for "the service is up and
    the port we registered is listening" evidence.
    """
    host = str(spec.get("host", "")).strip()
    try:
        port = int(spec.get("port", 0))
    except (TypeError, ValueError):
        return False, "", "port must be an integer"
    if not host or not (1 <= port <= 65535):
        return False, "", "host and 1..65535 port are required"
    try:
        with socket.create_connection((host, port), timeout=int(spec.get("timeout", 10))):
            return True, f"connected {host}:{port}", f"connect=ok host={host} port={port}"
    except (socket.timeout, TimeoutError, OSError) as exc:
        return False, "", f"connect failed: {type(exc).__name__}: {exc}"[:200]


def _probe_pytest_module(spec: dict[str, Any], workspace: Path) -> tuple[bool, str, str]:
    """Run ONE named pytest module inside the workspace.

    Unlike the gate's ``targeted_tests`` check (which is diff-driven), this
    probe says "this specific acceptance test must pass at runtime". The
    module must exist in the workspace; arbitrary paths outside are rejected.
    """
    rel = str(spec.get("path", "")).strip()
    if not rel:
        return False, "", "path is required"
    if rel.startswith("/") or ".." in Path(rel).parts:
        return False, "", "path must be relative to the task workspace"
    target = (workspace / rel).resolve()
    try:
        target.relative_to(workspace.resolve())
    except ValueError:
        return False, "", "path escapes the task workspace"
    if not target.is_file():
        return False, "", f"probe module {rel} not found in workspace"
    import subprocess
    import sys

    proc = subprocess.run(
        [sys.executable, "-m", "pytest", str(target), "-q"],
        cwd=str(workspace),
        capture_output=True,
        text=True,
        timeout=int(spec.get("timeout", 60)),
    )
    ok = proc.returncode == 0
    tail = (proc.stdout or proc.stderr or "").strip().splitlines()
    last = tail[-1] if tail else ""
    return ok, f"pytest rc={proc.returncode} {last}"[:300], f"rc={proc.returncode}"


PROBE_HANDLERS: dict[str, Callable[[dict[str, Any], Path], tuple[bool, str, str]]] = {
    "http_get": _probe_http_get,
    "file_exists": _probe_file_exists,
    "tcp_connect": _probe_tcp_connect,
    "pytest_module": _probe_pytest_module,
}

_ALLOWED_SPEC_KEYS = {
    "handler", "id", "target", "timeout",
    "url", "expect_status", "expect_contains",
    "path", "host", "port",
}


# --- construction ------------------------------------------------------------


def build_probe(
    spec: Any,
    workspace: "Path | None" = None,
    logger: "Callable[[str, str], None] | None" = None,
) -> "Callable[[], tuple[int, str, dict[str, Any]]] | None":
    """Build a zero-argument probe runner from a validated task spec.

    Returns ``None`` when no probe is declared (T0/T1 default). Returns a
    runner that yields ``(exit_code, output, evidence)`` where ``evidence``
    carries the probe/target/environment identity the gate records.

    A malformed spec raises ``ValueError``; the caller turns that into a
    BLOCKED gate check (never DONE).
    """
    if spec is None or (isinstance(spec, str) and not spec.strip()):
        return None
    normalized = validate_probe_spec(spec)
    handler = PROBE_HANDLERS[normalized["handler"]]
    workspace = Path(workspace) if workspace is not None else Path.cwd()

    def run() -> tuple[int, str, dict[str, Any]]:
        started = _utcnow()
        started_monotonic = _dt.datetime.now().timestamp()
        try:
            ok, output, detail = handler(normalized, workspace)
        except Exception as exc:  # noqa: BLE001 - bounded and recorded
            outcome = ProbeOutcome(
                ok=False,
                probe_id=normalized["id"],
                handler=normalized["handler"],
                target=normalized["target"],
                command=f"probe:{normalized['handler']}",
                output="",
                error=f"handler raised {type(exc).__name__}: {exc}"[:200],
                started_at=started,
            )
            evidence = _evidence(outcome, normalized, workspace)
            return 1, outcome.error, evidence
        duration = round(_dt.datetime.now().timestamp() - started_monotonic, 3)
        outcome = ProbeOutcome(
            ok=bool(ok),
            probe_id=normalized["id"],
            handler=normalized["handler"],
            target=normalized["target"],
            command=f"probe:{normalized['handler']}",
            output=output,
            exit_code=0 if ok else 1,
            started_at=started,
            duration_s=duration,
        )
        evidence = _evidence(outcome, normalized, workspace)
        if logger is not None and not ok:
            try:
                logger("warn", f"probe {outcome.probe_id} failed: {detail or outcome.error}")
            except Exception:
                pass
        return (0 if ok else 1), output, evidence

    return run


def _evidence(outcome: ProbeOutcome, spec: dict[str, Any], workspace: Path) -> dict[str, Any]:
    """Probe identity as recorded in the bundle.

    Target and environment identity are mandatory: probe evidence must never
    be reused across different targets or environments.
    """
    return {
        "kind": "runtime_probe",
        "probe_id": outcome.probe_id,
        "handler": outcome.handler,
        "target": outcome.target,
        "ok": outcome.ok,
        "command": outcome.command,
        "command_detail": json.dumps(
            {k: v for k, v in spec.items() if k not in ("handler", "id", "target")},
            sort_keys=True,
            default=str,
        )[:400],
        "started_at": outcome.started_at,
        "duration_s": outcome.duration_s,
        "workspace": str(workspace),
        "error": outcome.error,
        "exit_code": outcome.exit_code,
    }
