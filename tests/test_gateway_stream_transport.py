"""Installed Hermes gateway transport contract; no live model calls."""
import pytest
from dispatcher.router import SessionRouter, _set_new_session_flags_supported, _set_format_text_supported

@pytest.fixture(autouse=True)
def modern():
    _set_new_session_flags_supported(True)
    _set_format_text_supported(True)
    yield
    _set_new_session_flags_supported(None)
    _set_format_text_supported(None)

def build(router, fresh):
    return router._build_cmd(session_name="fixture", model="qualified-route",
                             max_turns=8, run_budget_s=180,
                             resume_by_name=not fresh, is_fresh=fresh)

def test_gateway_transport_creates_named_session_without_classic_precreate(monkeypatch):
    monkeypatch.setenv("DISPATCHER_HERMES_STREAM_JSON", "1")
    cmd = build(SessionRouter(), True)
    assert cmd[cmd.index("--format") + 1] == "stream-json"
    assert "--create-if-missing" in cmd
    assert "--run-budget" not in cmd
    assert cmd[cmd.index("--model") + 1] == "qualified-route"

def test_resume_retains_frozen_model_and_turn_policy(monkeypatch):
    monkeypatch.setenv("DISPATCHER_HERMES_STREAM_JSON", "1")
    cmd = build(SessionRouter(), False)
    assert "--model" not in cmd
    assert "--max-turns" not in cmd
    assert "--run-budget" not in cmd

def test_default_transport_remains_compatible(monkeypatch):
    monkeypatch.delenv("DISPATCHER_HERMES_STREAM_JSON", raising=False)
    cmd = build(SessionRouter(), True)
    assert cmd[cmd.index("--format") + 1] == "text"
