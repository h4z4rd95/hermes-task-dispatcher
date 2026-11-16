import json
from dispatcher.dispatcher import _owner_summary

def test_only_final_result_reaches_owner():
    raw = "\n".join([json.dumps({"type":"tool_result","output":"LONG_PRIVATE_TOOL_OUTPUT"*500}),
                       json.dumps({"type":"result","text":"PASS commit abc123","session_id":"same-session"})])
    value = _owner_summary(raw)
    assert "PASS commit abc123" in value and "same-session" in value
    assert "LONG_PRIVATE_TOOL_OUTPUT" not in value
    assert len(value) <= 800

def test_gateway_error_and_classic_output_remain_visible():
    assert "bounded failure" in _owner_summary('{"type":"result","error":"bounded failure"}')
    assert _owner_summary("classic result") == "classic result"


def test_agent_pushed_sha_requires_independent_exact_match():
    from types import SimpleNamespace
    from dispatcher.dispatcher import _verified_agent_commit
    sha = "a"*40
    bundle = SimpleNamespace(status="PASS", changed_files=["evidence/result.md"],
                             final_sha=sha, fetch_back={"ok":True, "independent_remote_read":True,
                                                      "local_sha":sha, "remote_sha":sha})
    assert _verified_agent_commit(bundle) == sha
    bundle.fetch_back["remote_sha"] = "b"*40
    assert _verified_agent_commit(bundle) == ""

def test_no_diff_or_unverified_sha_is_not_completion_evidence():
    from types import SimpleNamespace
    from dispatcher.dispatcher import _verified_agent_commit
    assert _verified_agent_commit(SimpleNamespace(status="PASS", changed_files=[],
                                                  final_sha="a"*40, fetch_back={})) == ""
