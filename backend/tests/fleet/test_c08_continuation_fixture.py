"""Finite fixture policy only; installed graph proof is a separate gate."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage


def test_continuation_policy_uses_current_user_turn_and_retains_prior_history():
    from .c08_continuation_fixture import continuation_reply

    history = [
        HumanMessage(content="original", id="old-user"),
        AIMessage(content="", id="old-ai", tool_calls=[{"id": "old-present", "name": "present_files", "args": {}, "type": "tool_call"}]),
        ToolMessage(content="Successfully presented files", name="present_files", tool_call_id="old-present", id="old-tool"),
    ]
    current = HumanMessage(content="c08-continuation=next", id="new-user")
    messages = [*history, current]
    first = continuation_reply(messages)
    assert [call["name"] for call in first.tool_calls] == ["bash"]
    assert first.tool_calls[0]["id"] == "c08-next-write"
    messages.extend([first, ToolMessage(content="written", name="bash", tool_call_id="c08-next-write", id="new-write")])
    second = continuation_reply(messages)
    assert [call["name"] for call in second.tool_calls] == ["present_files"]
    messages.extend([second, ToolMessage(content="Successfully presented files", name="present_files", tool_call_id="c08-next-present", id="new-present")])
    assert continuation_reply(messages).tool_calls == []
    assert messages[:3] == history


def test_continuation_policy_rejects_empty_history_and_failed_tools():
    from .c08_continuation_fixture import continuation_reply

    with pytest.raises(AssertionError, match="accepted source history"):
        continuation_reply([HumanMessage(content="c08-continuation=next")])
    with pytest.raises(AssertionError, match="Actual continuation tool failed"):
        continuation_reply([ToolMessage(content="source", name="present_files", tool_call_id="old"), HumanMessage(content="c08-continuation=next"), ToolMessage(content="failed", status="error", name="bash", tool_call_id="c08-next-write")])
