"""Graph-owned execution identity is stable across retries of a persisted task."""

import importlib
import importlib.util

import pytest
from langchain.tools import ToolRuntime, tool
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode


def helper():
    assert importlib.util.find_spec("deerflow.mcp.tasks.invocation") is not None, "Durable graph invocation identity missing"
    return importlib.import_module("deerflow.mcp.tasks.invocation").durable_tool_invocation_id


@pytest.mark.asyncio
async def test_checkpoint_replay_keeps_invocation_but_next_tool_turn_changes_it():
    identity = helper()
    observed = []
    crash = True

    @tool
    def capture(runtime: ToolRuntime[dict, MessagesState]) -> str:
        """Capture a durable execution identity."""
        value = identity(runtime)
        observed.append((value, runtime.execution_info.checkpoint_id, runtime.execution_info.task_id))
        if crash:
            raise RuntimeError("crash after persisted invocation")
        return value

    builder = StateGraph(MessagesState, context_schema=dict)
    builder.add_node("tools", ToolNode([capture], handle_tool_errors=False))
    builder.add_edge(START, "tools")
    builder.add_edge("tools", END)
    saver = InMemorySaver()
    graph = builder.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "t"}}
    call = {"name": "capture", "args": {}, "id": "provider-reused-id", "type": "tool_call"}
    with pytest.raises(RuntimeError, match="crash"):
        await graph.ainvoke({"messages": [AIMessage(content="", tool_calls=[call])]}, config, context={"run_id": "r", "user_id": "u"})
    snapshot = await graph.aget_state(config)
    assert snapshot.next == ("tools",)
    assert snapshot.tasks[0].id == observed[0][2]
    crash = False
    resumed = builder.compile(checkpointer=saver)
    await resumed.ainvoke(None, config, context={"run_id": "r", "user_id": "u"})
    assert observed[0] == observed[1]
    await resumed.ainvoke({"messages": [AIMessage(content="", tool_calls=[call])]}, config, context={"run_id": "r", "user_id": "u"})
    assert observed[2][0] != observed[0][0]


def test_identity_without_durable_graph_context_is_rejected():
    from types import SimpleNamespace

    identity = helper()
    with pytest.raises(ValueError, match="durable"):
        identity(SimpleNamespace(execution_info=None, context={"run_id": "r"}, tool_call_id="call"))
