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
