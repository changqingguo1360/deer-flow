"""Private native compiled-graph resume contract for metadata root copies."""

import pytest
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.types import Command, interrupt

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "delta"])
@pytest.mark.parametrize("mutation", ["title", "duration"])
@pytest.mark.parametrize("parallel", [False, True])
async def test_actual_metadata_checkpoint_pending_interrupt_resumes_exactly_once(checkpoint_owner, mode, mutation, parallel):
    from deerflow.agents.factory import create_deerflow_agent
    from deerflow.runtime.checkpoint_state import CheckpointStateAccessor
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.runs.worker import _ensure_interrupted_title, persist_run_durations

    p = checkpoint_owner
    calls = []

    @tool
    def original_question() -> str:
        """Ask the original graph for input."""
        calls.append("entered")
        return interrupt({"question": "Proceed?"})

    completed = []

    @tool
    def original_completed_sideeffect() -> str:
        """Complete an original side effect before awaiting human input."""
        completed.append("done")
        return "completed once"

    calls_for_turn = [{"id": "original", "name": "original_question", "args": {}, "type": "tool_call"}]
    if parallel:
        calls_for_turn.insert(0, {"id": "completed", "name": "original_completed_sideeffect", "args": {}, "type": "tool_call"})
    graph = create_deerflow_agent(
        FakeToolCallingModel(responses=[AIMessage(content="", tool_calls=calls_for_turn), AIMessage(content="accepted original answer")]), tools=[original_question, original_completed_sideeffect], checkpoint_channel_mode=mode
    )
    graph.checkpointer = p.writer
    accessor = CheckpointStateAccessor.bind(graph, p.writer, mode=mode)
    config = {"configurable": {"thread_id": p.spec.thread_id}, "context": {"user_id": p.spec.user_id, "run_id": p.spec.run_id}}
    from app.fleet.mutation import FleetMutationCapability

    with remote_mutation_scope(FleetMutationCapability(p.identity, p.spec).context):
        await p.writer.adelete_thread(p.spec.thread_id)
        await graph.ainvoke({"messages": [HumanMessage(content="Original question")], "title": "Existing title" if mutation == "duration" else ""}, config, durability="sync")
        before = await accessor.aget(config)
        assert before.next and before.interrupts and calls == ["entered"]
        if mutation == "title":
            await _ensure_interrupted_title(checkpointer=p.writer, thread_id=p.spec.thread_id, app_config=None, graph_input={"messages": [HumanMessage(content="Original question")]}, preserve_pending_accessor=accessor)
        else:
            options = {"preserve_pending_accessor": accessor} if "preserve_pending_accessor" in __import__("inspect").signature(persist_run_durations).parameters else {}
            await persist_run_durations(checkpointer=p.writer, thread_id=p.spec.thread_id, durations={p.spec.run_id: 17}, **options)
        copied = await accessor.aget(config)
        assert copied.config["configurable"]["checkpoint_id"] != before.config["configurable"]["checkpoint_id"]
        assert copied.interrupts and calls == ["entered"]
        await graph.ainvoke(Command(resume={copied.interrupts[0].id: "approved"}), config, durability="sync")
        final = await accessor.aget(config)
        assert not final.next and not final.interrupts, "final published interrupt id cannot resume its actual copied task"
        assert sorted(m.content for m in final.values["messages"] if isinstance(m, ToolMessage)) == (["approved", "completed once"] if parallel else ["approved"])
        assert completed == (["done"] if parallel else [])
        assert calls == ["entered", "entered"]
