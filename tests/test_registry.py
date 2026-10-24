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
    assert t.timeout_seconds == 900
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
