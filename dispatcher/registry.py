"""Machine-readable task inbox for the control repo.

The inbox is a TOML file inside ``h4z4rd95/Plan-Project-Situation`` at
``hermes/tasks_inbox.toml``. It is the declarative source of truth that the
scheduled dispatcher polls. We hand-roll a minimal parser so the task schema is
validated explicitly and errors point at the offending task block — and so no
third-party dependency is required on the Windows control host.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

__all__ = [
    "Task",
    "Inbox",
    "InboxError",
    "parse_inbox",
    "load_inbox",
    "task_to_toml_block",
    "inbox_to_toml",
]

# Task states. Terminal states stop the dispatcher from re-claiming a task.
STATES = ("PENDING", "READY", "RUNNING", "BLOCKED", "DONE", "FAILED", "CANCELLED")
TERMINAL_STATES = frozenset({"DONE", "FAILED", "CANCELLED"})
ACTIVE_STATES = frozenset({"READY", "RUNNING", "BLOCKED"})
SESSION_AFFINITY = ("dedicated", "existing", "none")

_INT_FIELDS = {"priority", "workspace_budget_mb", "max_retries", "timeout_seconds"}
_STR_FIELDS = {
    "id", "title", "status", "workstream", "repo", "branch", "prompt_path",
    "prompt_inline", "session", "session_affinity", "model", "approval_note",
    "purpose",
}
_BOOL_FIELDS = {"parallel_safe", "keep_workspace", "requires_approval"}
_LIST_FIELDS = {"dependencies"}


class InboxError(ValueError):
    """Raised when the inbox is malformed or a task block is invalid."""


@dataclass
class Task:
    id: str
    title: str
    status: str = "READY"
    priority: int = 100
    workstream: str = "default"
    repo: str = ""
    branch: str = "main"
    prompt_path: str = ""
    prompt_inline: str = ""
    session: str = ""
    session_affinity: str = "dedicated"
    parallel_safe: bool = True
    workspace_budget_mb: int = 0
    keep_workspace: bool = False
    requires_approval: bool = False
    approval_note: str = ""
    dependencies: list[str] = field(default_factory=list)
    max_retries: int = 2
    timeout_seconds: int = 900
    model: str = ""
    purpose: str = ""

    def __post_init__(self) -> None:
        errs = self.validate()
        if errs:
            raise InboxError(f"task {self.id!r}: {'; '.join(errs)}")

    def validate(self) -> list[str]:
        errs: list[str] = []
        if not self.id or not isinstance(self.id, str):
            errs.append("id is required")
        if not self.title:
            errs.append("title is required")
        if self.status not in STATES:
            errs.append(f"status {self.status!r} not in {STATES}")
        if self.session_affinity not in SESSION_AFFINITY:
            errs.append(f"session_affinity {self.session_affinity!r} not in {SESSION_AFFINITY}")
        if self.prompt_path and self.prompt_inline:
            errs.append("prompt_path and prompt_inline are mutually exclusive")
        if not self.prompt_path and not self.prompt_inline:
            errs.append("prompt_path or prompt_inline is required")
        if self.requires_approval and not self.approval_note:
            errs.append("requires_approval requires an approval_note naming the gate")
        if not isinstance(self.priority, int):
            errs.append("priority must be an integer")
        if not isinstance(self.dependencies, list):
            errs.append("dependencies must be a list of task ids")
        return errs

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATES

    @property
    def needs_checkout(self) -> bool:
        return bool(self.repo)

    def resolved_prompt(self, control_repo: Path) -> str:
        """Return the prompt text, loading prompt_path from the control repo."""
        if self.prompt_inline:
            return self.prompt_inline
        path = control_repo / self.prompt_path
        if not path.is_file():
            raise InboxError(f"task {self.id!r}: prompt_path {self.prompt_path!r} not found")
        return path.read_text(encoding="utf-8")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Inbox:
    version: int = 1
    tasks: list[Task] = field(default_factory=list)

    def by_id(self) -> dict[str, Task]:
        return {t.id: t for t in self.tasks}

    def validate(self) -> list[str]:
        errs: list[str] = []
        ids: set[str] = set()
        for task in self.tasks:
            if task.id in ids:
                errs.append(f"duplicate task id {task.id!r}")
            ids.add(task.id)
        # Dependencies must reference real task ids (unknown deps are a real
        # error: a task can never become runnable).
        index = self.by_id()
        for task in self.tasks:
            for dep in task.dependencies:
                if dep not in index:
                    errs.append(f"task {task.id!r} depends on unknown task {dep!r}")
        return errs


# --- minimal TOML parsing for the subset we use -----------------------------

_TASK_BLOCK = re.compile(r"^\[\[task\]\]\s*$", re.MULTILINE)
_KEY_VALUE = re.compile(r'^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$')


def _strip_comment(line: str) -> str:
    """Remove a trailing comment, respecting simple double-quoted strings."""
    in_str = False
    escaped = False
    for i, ch in enumerate(line):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
        elif ch == '"':
            in_str = not in_str
        elif ch == "#" and not in_str:
            return line[:i]
    return line


def _parse_value(raw: str, *, source: str) -> Any:
    raw = raw.strip()
    if not raw:
        return ""
    if raw.startswith('"""') and raw.endswith('"""') and len(raw) >= 6:
        return raw[3:-3]
    if raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
        return _unescape(raw[1:-1])
    if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
        return raw[1:-1]
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        items = []
        for part in _split_top_level(inner):
            items.append(_parse_value(part.strip(), source=source))
        return items
    if raw in ("true", "false"):
        return raw == "true"
    # integer
    try:
        return int(raw)
    except ValueError:
        pass
    # float fallback (not used by the schema, but keep parsing sane)
    try:
        return float(raw)
    except ValueError:
        pass
    raise InboxError(f"{source}: cannot parse value {raw!r}")


def _split_top_level(text: str) -> list[str]:
    """Split a TOML array body on top-level commas."""
    parts: list[str] = []
    depth = 0
    current = []
    in_str = False
    escaped = False
    for ch in text:
        if escaped:
            current.append(ch)
            escaped = False
            continue
        if ch == "\\":
            current.append(ch)
            escaped = True
        elif ch == '"':
            current.append(ch)
            in_str = not in_str
        elif in_str:
            current.append(ch)
        elif ch == "[":
            depth += 1
            current.append(ch)
        elif ch == "]":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _unescape(text: str) -> str:
    return (
        text.replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace('\\"', '"')
        .replace("\\\\", "\\")
    )


def _parse_inline_table(body: str, *, source: str) -> dict[str, Any]:
    """Parse the key=value lines of one [[task]] block."""
    out: dict[str, Any] = {}
    lines = body.splitlines()
    lineno = 0
    while lineno < len(lines):
        line = _strip_comment(lines[lineno]).strip()
        lineno += 1
        if not line:
            continue
        m = _KEY_VALUE.match(line)
        if not m:
            # A stray table header (e.g. a second [inbox] when files are
            # concatenated) is not a key/value line — ignore it rather than
            # abort the whole parse.
            if line.startswith("[") and line.endswith("]"):
                continue
            raise InboxError(f"{source}:{lineno}: expected key = value, got {line!r}")
        key, raw = m.group(1), m.group(2).strip()
        if key in out:
            raise InboxError(f"{source}:{lineno}: duplicate key {key!r}")
        # Multi-line basic string: """ ... """ may span lines.
        if raw.startswith('"""') and not (raw.endswith('"""') and len(raw) >= 6):
            buffer = [raw]
            closed = False
            while lineno < len(lines):
                nxt = lines[lineno]
                lineno += 1
                buffer.append(nxt)
                if nxt.rstrip().endswith('"""'):
                    closed = True
                    break
            if not closed:
                raise InboxError(f"{source}: unterminated multi-line string for {key!r}")
            value = "\n".join(_strip_comment(b) for b in buffer)
            out[key] = _unescape(value[3:-3])
            continue
        out[key] = _parse_value(raw, source=f"{source}:{lineno}")
    return out


def parse_inbox(text: str, *, source: str = "tasks_inbox.toml") -> Inbox:
    """Parse the inbox TOML text into an :class:`Inbox`."""
    header: dict[str, Any] = {}
    header_end = len(text)

    # The optional [inbox] header runs from its marker to the first [[task]].
    m = re.search(r"^\[inbox\][ \t]*$", text, re.MULTILINE)
    if m:
        rest = text[m.end():]
        end = re.search(r"^\[\[task\]\][ \t]*$", rest, re.MULTILINE)
        header_body = rest[: end.start()] if end else rest
        header_end = (m.end() + end.start()) if end else len(text)
        header = _parse_inline_table(header_body, source=f"{source}:[inbox]")

    body = text[header_end:]
    starts = [mm.start() for mm in _TASK_BLOCK.finditer(body)]
    if not starts:
        return Inbox(version=int(header.get("version", 1)), tasks=[])

    tasks: list[Task] = []
    bounds = starts + [len(body)]
    for i in range(len(starts)):
        block = body[bounds[i]: bounds[i + 1]]
        # drop the [[task]] header line itself
        kv_block = re.sub(r"^\[\[task\]\][ \t]*\n", "", block, count=1)
        kv = _parse_inline_table(kv_block, source=f"{source}:task[{i}]")
        known = set(_STR_FIELDS) | _INT_FIELDS | _BOOL_FIELDS | _LIST_FIELDS
        unknown = set(kv) - known
        if unknown:
            raise InboxError(f"{source}:task[{i}]: unknown key(s) {sorted(unknown)}")
        tasks.append(Task(**kv))

    inbox = Inbox(version=int(header.get("version", 1)), tasks=tasks)
    errs = inbox.validate()
    if errs:
        raise InboxError("; ".join(errs))
    return inbox


def load_inbox(path: Path) -> Inbox:
    if not path.is_file():
        raise InboxError(f"inbox not found: {path}")
    return parse_inbox(path.read_text(encoding="utf-8"), source=str(path))


# --- serialization (deterministic, diffable) --------------------------------

_ORDER = (
    "id", "title", "status", "priority", "workstream", "repo", "branch",
    "purpose", "prompt_path", "prompt_inline", "session", "session_affinity",
    "parallel_safe", "workspace_budget_mb", "keep_workspace", "requires_approval",
    "approval_note", "dependencies", "max_retries", "timeout_seconds", "model",
)


def _fmt_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, list):
        if not value:
            return "[]"
        inner = ", ".join(_fmt_value(v) for v in value)
        return f"[{inner}]"
    text = str(value)
    if "\n" in text:
        escaped = text.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
        return f'"""{escaped}"""'
    escaped = (
        text.replace("\\", "\\\\").replace('"', '\\"')
    )
    return f'"{escaped}"'


def task_to_toml_block(task: Task) -> str:
    lines = ["[[task]]"]
    data = task.to_dict()
    for key in _ORDER:
        if key not in data:
            continue
        value = data[key]
        # Keep the block tight: skip empty/opted-out fields we never serialize.
        if key in ("prompt_path", "prompt_inline", "purpose", "model") and not value:
            continue
        if key == "dependencies" and not value:
            continue
        lines.append(f"{key} = {_fmt_value(value)}")
    lines.append("")
    return "\n".join(lines)


def inbox_to_toml(inbox: Inbox) -> str:
    out = ["# Hermes task dispatcher inbox. Edit by hand; the dispatcher", "# updates only task status fields.",
           "", "[inbox]", "version = 1", ""]
    for task in inbox.tasks:
        out.append(task_to_toml_block(task))
    return "\n".join(out).rstrip() + "\n"
