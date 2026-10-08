"""Native original clarification middleware observation, never client authority."""

from dataclasses import replace

import pytest
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "delta"])
@pytest.mark.parametrize("case", ["historical_new_run", "nested", "disabled", "wrong_owner"])
async def test_actual_human_card_does_not_grant_unobserved_root_pause(checkpoint_owner, mode, case):
    from langgraph.graph import END, START, StateGraph

    from app.fleet.mutation import FleetMutationCapability
    from deerflow.agents.factory import create_deerflow_agent
    from deerflow.agents.middlewares.clarification_middleware import ClarificationMiddleware
    from deerflow.agents.thread_state import get_thread_state_schema
    from deerflow.runtime.checkpoint_state import CheckpointStateAccessor
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope
    from deerflow.tools.builtins.clarification_tool import ask_clarification_tool

    p = checkpoint_owner
    capability = FleetMutationCapability(p.identity, p.spec)
    controller = WorkspaceWriterController()
    controller.bind_execution_context(capability.context)
    model = FakeToolCallingModel(
        responses=[
            AIMessage(content="", tool_calls=[{"id": "provider-reused-human", "name": "ask_clarification", "args": {"question": "Proceed?", "clarification_type": "missing_info"}, "type": "tool_call"}]),
            AIMessage(content="ordinary answer"),
        ]
    )
    graph = create_deerflow_agent(model, tools=[ask_clarification_tool], middleware=[ClarificationMiddleware()], checkpoint_channel_mode=mode)
    if case == "nested":
        wrapper = StateGraph(get_thread_state_schema(mode))
        wrapper.add_node("original_nested", graph)
        wrapper.add_edge(START, "original_nested")
        wrapper.add_edge("original_nested", END)
        graph = wrapper.compile()
    accessor = CheckpointStateAccessor.bind(graph, p.writer, mode=mode)
    config = {
        "configurable": {"thread_id": p.spec.thread_id},
        "context": {"user_id": "foreign" if case == "wrong_owner" else p.spec.user_id, "run_id": p.spec.run_id, "thread_id": p.spec.thread_id, "disable_clarification": case == "disabled"},
    }
    with remote_mutation_scope(capability.context), workspace_writer_scope(controller):
        await p.writer.adelete_thread(p.spec.thread_id)
        execute = graph.ainvoke({"messages": [HumanMessage(content="Original request")]}, config, context=config["context"], durability="sync")
        if case == "wrong_owner":
            with pytest.raises(OwnershipRejected, match="owner"):
                await execute
        else:
            await execute
        snapshot = await accessor.aget(config)
        tool_messages = [m for m in snapshot.values["messages"] if isinstance(m, ToolMessage)]
        assert tool_messages or case == "wrong_owner"
        if case == "historical_new_run":
            assert controller.awaiting_human_input(snapshot)
            message = tool_messages[0]
            assert message.id == message.artifact["human_input"]["request_id"] and message.tool_call_id == "provider-reused-human"
            fresh = WorkspaceWriterController()
            fresh.bind_execution_context(replace(capability.context, run_id="another-original-run"))
            with remote_mutation_scope(fresh._execution_context), workspace_writer_scope(fresh):
                assert not fresh.awaiting_human_input(snapshot)
        else:
            assert not controller.awaiting_human_input(snapshot)
            assert controller._human_inputs == {}
            if case == "disabled":
                assert all(not m.artifact for m in tool_messages)
            if case == "wrong_owner":
                assert not tool_messages
            if case == "nested":
                assert any(isinstance(m.artifact, dict) and "human_input" in m.artifact for m in tool_messages)
