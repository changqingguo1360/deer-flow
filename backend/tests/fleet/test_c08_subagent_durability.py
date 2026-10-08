"""Actual PG parent sync durability and original isolated no-saver child executor."""

import asyncio
import importlib
import sys

import pytest
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "delta"])
async def test_original_remote_subagent_does_not_inherit_parent_sync_without_saver(checkpoint_owner, monkeypatch, mode):
    from langchain.tools import ToolRuntime
    from langchain_core.messages import AIMessage, HumanMessage
    from langchain_core.tools import tool
    from langgraph.checkpoint.base import empty_checkpoint
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    import deerflow.agents  # warm the original cycle before loading canonical executor
    import deerflow.subagents
    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import FleetCheckpointFence
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.subagents.config import SubagentConfig

    monkeypatch.delitem(sys.modules, "deerflow.subagents.executor", raising=False)
    monkeypatch.delattr(deerflow.subagents, "executor", raising=False)
    module = importlib.import_module("deerflow.subagents.executor")
    monkeypatch.setattr(module, "build_tracing_callbacks", lambda: [])
    item = checkpoint_owner
    capability = FleetMutationCapability(item.identity, item.spec)
    observed = []

    @tool
    def original_child_sideeffect(runtime: ToolRuntime) -> str:
        """Record the actual child tool runtime once."""
        observed.append((runtime.context["user_id"], runtime.context["thread_id"], runtime.context["run_id"], runtime.execution_info.checkpoint_ns))
        return "child tool executed"

    child = StateGraph(MessagesState)
    child.add_node("child_model", lambda state: {"messages": [AIMessage(content="", id="child-call-message", tool_calls=[{"name": original_child_sideeffect.name, "args": {}, "id": "child-call", "type": "tool_call"}])]})
    child.add_node("tools", ToolNode([original_child_sideeffect]))
    child.add_node("final", lambda state: {"messages": [AIMessage(content="child completed", id="child-final")]})
    child.add_edge(START, "child_model")
    child.add_edge("child_model", "tools")
    child.add_edge("tools", "final")
    child.add_edge("final", END)
    child_graph = child.compile(checkpointer=False)
    original_stream = child_graph.astream
    stream_options = []

    def observe_stream(*args, **kwargs):
        stream_options.append(kwargs.get("durability"))
        return original_stream(*args, **kwargs)

    monkeypatch.setattr(child_graph, "astream", observe_stream)
    executor = module.SubagentExecutor(
        config=SubagentConfig(name="general-purpose", description="Original child sync isolation", system_prompt="Run the child", max_turns=12, timeout_seconds=10),
        tools=[original_child_sideeffect],
        parent_model="deterministic-child",
        app_config=item.private,
        user_id=capability.context.user_id,
        thread_id=capability.context.thread_id,
        run_id=capability.context.run_id,
    )

    async def initial(task):
        return {"messages": [HumanMessage(content=task)]}, [original_child_sideeffect], None

    monkeypatch.setattr(executor, "_build_initial_state", initial)
    monkeypatch.setattr(executor, "_create_agent", lambda *args, **kwargs: child_graph)

    async def delegate(state):
        task = executor.execute_async("actual child tool")
        try:
            async with asyncio.timeout(12):
                while True:
                    result = module.get_background_task_result(task)
                    if result is not None and result.status.is_terminal:
                        break
                    await asyncio.sleep(0.005)
            assert result.status.value == "completed", result.error
            assert result.result == "child completed"
        finally:
            module.cleanup_background_task(task)
        return {"messages": [AIMessage(content="parent completed", id="parent-final")]}

    private = item.private.model_copy(update={"database": item.private.database.model_copy(update={"checkpoint_channel_mode": mode})})
    async with make_checkpointer(private, write_fence=FleetCheckpointFence(item.identity, item.spec)) as saver:
        config = {"configurable": {"thread_id": capability.context.thread_id, "checkpoint_ns": ""}}
        with remote_mutation_scope(capability.context):
            await saver.aput(config, empty_checkpoint(), {"source": "input", "step": -1, "parents": {}}, {})
            parent = StateGraph(MessagesState)
            parent.add_node("delegate", delegate)
            parent.add_edge(START, "delegate")
            parent.add_edge("delegate", END)
            graph = parent.compile(checkpointer=saver)
            chunks = [item async for item in graph.astream({"messages": [HumanMessage(content="delegate")]}, config=config, stream_mode="messages", durability="sync")]
        assert [message.id for message, metadata in chunks] == ["parent-final"]
        state = await graph.aget_state(config)
        assert state.values["messages"][-1].content == "parent completed"
    assert stream_options == ["async"]
    assert len(observed) == 1
    assert observed[0][:3] == (capability.context.user_id, capability.context.thread_id, capability.context.run_id)
    assert observed[0][3] and "|" in observed[0][3]
    async with item.engine.connect() as connection:
        namespaces = list((await connection.execute(text("SELECT DISTINCT checkpoint_ns FROM checkpoints"))).scalars())
        assert namespaces == [""]
