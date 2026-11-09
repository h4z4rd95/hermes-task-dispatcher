"""Tests for dispatcher/registry.py — inbox parsing, validation, serialization."""

from __future__ import annotations

import pytest

from dispatcher.registry import (
    Inbox,
    InboxError,
    STATES,
    TERMINAL_STATES,
    Task,
    inbox_to_toml,
    load_inbox,
    parse_inbox,
    task_to_toml_block,
)

MINIMAL = """
[inbox]
version = 1

[[task]]
id = "T-001"
title = "Smoke test"
status = "READY"
priority = 100
workstream = "smoke"
prompt_inline = "Reply with PONG"
session = "smoke-001"
session_affinity = "dedicated"
"""


def test_minimal_parse():
    inbox = parse_inbox(MINIMAL)
    assert inbox.version == 1
    assert len(inbox.tasks) == 1
    t = inbox.tasks[0]
    assert t.id == "T-001"
    assert t.status == "READY"
    assert t.session_affinity == "dedicated"


def test_defaults_match_contract():
    t = Task(id="T-x", title="x", prompt_inline="p")
    assert t.priority == 100
    assert t.branch == "main"
    assert t.parallel_safe is True
    assert t.max_retries == 2
    assert t.timeout_seconds == 1500
    assert t.is_terminal is False
    assert t.needs_checkout is False


def test_requires_validation():
    with pytest.raises(InboxError, match="approval_note"):
        Task(id="T-1", title="x", prompt_inline="p", requires_approval=True)
    with pytest.raises(InboxError, match="mutually exclusive"):
        Task(id="T-1", title="x", prompt_path="a.md", prompt_inline="p")
    with pytest.raises(InboxError, match="prompt_path or prompt_inline"):
        Task(id="T-1", title="x")
    with pytest.raises(InboxError, match="session_affinity"):
        Task(id="T-1", title="x", prompt_inline="p", session_affinity="bogus")


def test_pending_status_is_valid_and_non_terminal():
    task = Task(id="T-1", title="x", prompt_inline="p", status="PENDING")
    assert task.status == "PENDING"
    assert task.is_terminal is False


def test_bad_status_rejected():
    with pytest.raises(InboxError, match="not in"):
        Task(id="T-1", title="x", prompt_inline="p", status="WAT")


def test_dependencies_must_exist():
    with pytest.raises(InboxError, match="T-NOPE"):
        parse_inbox(MINIMAL + """
[[task]]
id = "T-002"
title = "Depends on missing"
prompt_inline = "p"
dependencies = ["T-NOPE"]
""")


def test_unknown_key_rejected():
    with pytest.raises(InboxError, match="unknown key"):
        parse_inbox(MINIMAL + """
[[task]]
id = "T-002"
title = "x"
prompt_inline = "p"
bogus_field = 1
""")


def test_duplicate_id_rejected():
    # two task blocks (no duplicated headers), different ids → valid
    inbox = parse_inbox(MINIMAL + MINIMAL.replace('id = "T-001"', 'id = "T-002"').split("[[task]]", 1)[1].join(["[[task]]", ""]))
    assert len(inbox.tasks) == 2
    assert not inbox.validate()


def test_duplicate_id_detected():
    # the same task block twice → duplicate id error
    with pytest.raises(InboxError, match="duplicate task id"):
        parse_inbox(MINIMAL + MINIMAL.split("[[task]]", 1)[1].join(["[[task]]", ""]))


def test_comments_and_multiline_preserved():
    text = """
# leading comment
[inbox]
version = 1  # version comment

[[task]]
# task comment
id = "T-001"
title = "Multi"
prompt_inline = \"\"\"line one
line two\"\"\"
"""
    inbox = parse_inbox(text)
    assert inbox.tasks[0].prompt_inline == "line one\nline two"


def test_load_inbox_missing(tmp_path):
    with pytest.raises(InboxError, match="not found"):
        load_inbox(tmp_path / "nope.toml")


def test_roundtrip_serialization(tmp_path):
    t = Task(id="T-001", title="Smoke", prompt_inline="Reply with PONG")
    block = task_to_toml_block(t)
    assert 'id = "T-001"' in block
    assert 'prompt_inline = "Reply with PONG"' in block
    # re-parse the serialized form
    reparsed = parse_inbox("[inbox]\nversion = 1\n\n" + block)
    assert reparsed.tasks[0].id == "T-001"
    assert reparsed.tasks[0].prompt_inline == "Reply with PONG"


def test_roundtrip_keeps_dependencies():
    t = Task(id="T-002", title="Smoke", prompt_inline="p")
    reparsed = parse_inbox(
        "[inbox]\nversion = 1\n\n"
        + '[[task]]\nid = "T-001"\ntitle = "a"\nprompt_inline = "p"\nstatus = "DONE"\n\n'
        + task_to_toml_block(t)
    )
    assert reparsed.tasks[1].id == "T-002"
    assert reparsed.tasks[0].id == "T-001"


def test_inbox_to_toml_roundtrip():
    inbox = parse_inbox(MINIMAL)
    text = inbox_to_toml(inbox)
    back = parse_inbox(text)
    assert back.tasks[0].id == inbox.tasks[0].id
    assert "[inbox]" in text
    assert text.endswith("\n")


def test_resolved_prompt_from_path(tmp_path):
    prompt = tmp_path / "tasks" / "p.md"
    prompt.parent.mkdir(parents=True)
    prompt.write_text("Do the thing", encoding="utf-8")
    t = Task(id="T-1", title="x", prompt_path="tasks/p.md")
    assert t.resolved_prompt(tmp_path) == "Do the thing"
    missing = Task(id="T-2", title="x", prompt_path="tasks/nope.md")
    with pytest.raises(InboxError, match="not found"):
        missing.resolved_prompt(tmp_path)


def test_inbox_without_header_parses_tasks():
    # A hand-edited inbox may omit the optional [inbox] header entirely;
    # the task blocks then start at byte 0 and must still be found.
    text = '[[task]]\nid = "T-001"\ntitle = "Smoke"\nprompt_inline = "x"\n'
    inbox = parse_inbox(text)
    assert len(inbox.tasks) == 1
    assert inbox.tasks[0].id == "T-001"


def test_result_note_is_accepted_and_preserved():
    # The owner records a free-text result_note on completed tasks; an
    # unknown key would make the whole inbox unparseable (inbox=0, blind).
    text = (
        "[inbox]\nversion = 1\n\n"
        "[[task]]\nid = \"T-001\"\ntitle = \"Smoke\"\nstatus = \"DONE\"\n"
        "prompt_inline = \"x\"\nresult_note = \"completed 2026-10-12\"\n"
    )
    inbox = parse_inbox(text)
    assert inbox.tasks[0].result_note == "completed 2026-10-12"

    block = task_to_toml_block(inbox.tasks[0])
    assert 'result_note = "completed 2026-10-12"' in block
    # An empty result_note is not serialised (block stays tight).
    bare = Task(id="T-2", title="x", prompt_inline="p")
    assert "result_note" not in task_to_toml_block(bare)


def test_completed_partial_is_terminal_but_not_claimable():
    # The owner marks a task COMPLETED_PARTIAL when acceptance criteria are
    # only partly met. It must parse, count as terminal (never re-claimed),
    # and stay out of the DONE/FAILED/CANCELLED triple.
    task = Task(id="T-1", title="x", prompt_inline="p", status="COMPLETED_PARTIAL")
    assert task.is_terminal is True
    assert task.status not in TERMINAL_STATES
    # The claim guard treats it the same as DONE.
    assert not task.is_terminal or task.status == "COMPLETED_PARTIAL"

    text = (
        "[[task]]\nid = \"T-1\"\ntitle = \"Acceptance\"\n"
        "status = \"COMPLETED_PARTIAL\"\nprompt_inline = \"x\"\n"
    )
    inbox = parse_inbox(text)
    assert inbox.tasks[0].is_terminal is True



# --- T-2026-10-24 auto-intake owner control: provenance + allow-rule --------


def test_source_and_spawn_allowed_round_trip():
    task = Task(id="T-48", title="x", prompt_inline="p",
                source="github-issue-48", spawn_allowed=True)
    block = task_to_toml_block(task)
    assert 'source = "github-issue-48"' in block
    assert "spawn_allowed = true" in block
    parsed = parse_inbox(block).tasks[0]
    assert parsed.source == "github-issue-48"
    assert parsed.spawn_allowed is True


def test_source_defaults_absent_and_backwards_compatible():
    # Legacy blocks without the new fields still parse; the fields default
    # off so nothing becomes spawn-eligible or falsely provenanced.
    text = ("[[task]]\nid = \"T-LEGACY\"\ntitle = \"x\"\n"
            "status = \"READY\"\nprompt_inline = \"p\"\n")
    task = parse_inbox(text).tasks[0]
    assert task.source == ""
    assert task.spawn_allowed is False
    # And a block that never used the fields serialises without them.
    assert "source" not in task_to_toml_block(task)
    assert "spawn_allowed" not in task_to_toml_block(task)


def test_spawned_by_source_value_parses():
    text = ("[[task]]\nid = \"T-49\"\ntitle = \"x\"\n"
            "status = \"PENDING\"\nprompt_inline = \"p\"\n"
            "source = \"spawned-by:T-48\"\n")
    task = parse_inbox(text).tasks[0]
    assert task.source == "spawned-by:T-48"


# --- Issue #99: non-repository artifact metadata -----------------------------
#
# The three artifact_* fields form ONE contract: present together, with an
# empty repo, or absent. Missing or contradictory declarations must fail
# closed at parse time — never silently fall back to the Hermes home.

ARTIFACT_BLOCK = """\
[[task]]
id = "T-ART"
title = "Artifact dispatch"
status = "READY"
prompt_inline = "write the report into the declared deliverable"
task_class = "CODE"
project = "Hermes Task Dispatcher"
artifact_store = "h4z4rd95/evidence-store"
artifact_branch = "work/evidence-2026-11-08"
deliverable = "evidence/report-2026-11-08.md"
"""


def _parse_artifact_block(drop: tuple[str, ...] = ()) -> Task:
    """Parse ARTIFACT_BLOCK, optionally dropping whole artifact lines."""
    text = ARTIFACT_BLOCK
    lines = {
        "artifact_store = ": "artifact_store = \"h4z4rd95/evidence-store\"\n",
        "artifact_branch = ": "artifact_branch = \"work/evidence-2026-11-08\"\n",
        "deliverable = ": "deliverable = \"evidence/report-2026-11-08.md\"\n",
    }
    for prefix in drop:
        text = text.replace(lines[prefix], "")
    return parse_inbox(text).tasks[0]


def test_artifact_task_parses_with_full_metadata():
    task = _parse_artifact_block()
    assert task.is_artifact_task is True
    assert task.needs_checkout is False
    assert task.repo == ""
    assert task.artifact_store == "h4z4rd95/evidence-store"
    assert task.artifact_branch == "work/evidence-2026-11-08"
    assert task.deliverable == "evidence/report-2026-11-08.md"
    assert task.task_class == "CODE"
    assert task.project == "Hermes Task Dispatcher"


def test_task_class_and_project_are_optional_labels():
    text = (ARTIFACT_BLOCK.replace('task_class = "CODE"\n', "")
                       .replace('project = "Hermes Task Dispatcher"\n', ""))
    task = parse_inbox(text).tasks[0]
    # Provenance labels are optional; the artifact contract still holds.
    assert task.task_class == ""
    assert task.project == ""
    assert task.is_artifact_task is True


@pytest.mark.parametrize("drop", [
    ("artifact_store = ",),
    ("artifact_branch = ",),
    ("deliverable = ",),
])
def test_partial_artifact_metadata_rejected(drop):
    with pytest.raises(InboxError, match="artifact"):
        _parse_artifact_block(drop=drop)


def test_artifact_metadata_rejected_with_repo():
    text = ARTIFACT_BLOCK + 'repo = "owner/alpha"\n'
    with pytest.raises(InboxError, match="artifact"):
        parse_inbox(text)


@pytest.mark.parametrize("store", [
    "owner",                      # no slash
    "owner/repo/extra",           # extra component
    "own er/repo",                # space
    "../owner/repo",              # traversal
    "owner/repo?x=1",             # query-like
])
def test_artifact_store_must_be_owner_repo_slug(store):
    text = ARTIFACT_BLOCK.replace(
        'artifact_store = "h4z4rd95/evidence-store"',
        f'artifact_store = "{store}"')
    with pytest.raises(InboxError, match="artifact_store"):
        parse_inbox(text)


@pytest.mark.parametrize("branch", [
    "main",
    "master",
    "MAIN",
    "work/main",           # forbidden component
    "work/../main",        # traversal
    "work//evidence",      # duplicated separator
    "work/evidence/",      # trailing slash
    "work/evidence.lock",  # lock suffix
    "-c",                  # option-like
    "--all",               # option-like
    "work/.hidden",        # dot component
])
def test_artifact_branch_must_be_safe_non_main(branch):
    text = ARTIFACT_BLOCK.replace(
        'artifact_branch = "work/evidence-2026-11-08"',
        f'artifact_branch = "{branch}"')
    with pytest.raises(InboxError, match="artifact_branch"):
        parse_inbox(text)


@pytest.mark.parametrize("deliverable", [
    "report-2026-11-08.md",      # not under evidence/ or docs/
    "evidence/",                 # no file
    "evidence/../etc/passwd",    # traversal
    "evidence/sub/.hidden.md",   # dot component
    "evidence/-flag.md",         # option-like component
    "/evidence/report.md",       # absolute
    "evidence\\report.md",       # backslash
])
def test_deliverable_must_be_safe_relative_path(deliverable):
    text = ARTIFACT_BLOCK.replace(
        'deliverable = "evidence/report-2026-11-08.md"',
        f'deliverable = "{deliverable}"')
    with pytest.raises(InboxError, match="deliverable"):
        parse_inbox(text)


def test_docs_deliverable_is_accepted():
    text = ARTIFACT_BLOCK.replace(
        'deliverable = "evidence/report-2026-11-08.md"',
        'deliverable = "docs/ops/runbook-2026-11-08.md"')
    task = parse_inbox(text).tasks[0]
    assert task.deliverable == "docs/ops/runbook-2026-11-08.md"


def test_artifact_fields_roundtrip_toml():
    task = _parse_artifact_block()
    block = task_to_toml_block(task)
    assert 'artifact_store = "h4z4rd95/evidence-store"' in block
    assert 'artifact_branch = "work/evidence-2026-11-08"' in block
    assert 'deliverable = "evidence/report-2026-11-08.md"' in block
    parsed = parse_inbox(block).tasks[0]
    assert parsed.is_artifact_task is True
    assert parsed.artifact_store == task.artifact_store
    # A plain task serialises without any artifact keys.
    plain = Task(id="T-PLAIN", title="x", prompt_inline="p")
    plain_block = task_to_toml_block(plain)
    assert "artifact_" not in plain_block
    assert "deliverable" not in plain_block
    assert plain.is_artifact_task is False
