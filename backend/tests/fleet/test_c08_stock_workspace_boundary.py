"""Native stock worker + real agent ToolNode, saver, HTTP/NAS and pair lifecycle.

This native graph test supplements the installed full lead-agent image gate.
Only provider responses are scripted; publication paths are production code.
"""

import asyncio
import os
import time
from contextlib import AsyncExitStack, contextmanager

import httpx
import pytest
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "delta"])
@pytest.mark.parametrize("streams", [["values"], ["values", "custom"]])
@pytest.mark.parametrize("pause", [False, True, "clarification"])
async def test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final(checkpoint_owner, tmp_path, monkeypatch, mode, streams, pause, fault=None, goal_continuation=False):
    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions, WorkspaceBoundaryIdentity
    from deerflow_ecs_fleet.persistence.outbox import EventOutbox
    from deerflow_ecs_fleet.workspace import NASWorkspace

    from app.fleet.events import FleetEventParticipant, FleetProducerBridge, FleetStreamSeals, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import _AgentResourceTeardown
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.agents.factory import create_deerflow_agent
    from deerflow.agents.middlewares.thread_data_middleware import ThreadDataMiddleware
    from deerflow.config import paths
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent
    from deerflow.tools.builtins.present_file_tool import present_file_tool

    if fault == "duration":

        async def fail_duration(**kwargs):
            raise ValueError("original duration checkpoint fault")

        monkeypatch.setattr("deerflow.runtime.runs.worker._persist_run_duration", fail_duration)
    if fault == "migration":

        async def fail_migration(*args, **kwargs):
            raise ValueError("original pending checkpoint mapping fault")

        monkeypatch.setattr("deerflow.runtime.runs.worker._preserve_remote_pending_writes", fail_migration)
    if fault in {"rollback_fault", "editrollback"}:

        async def fail_rollback(**kwargs):
            raise ValueError("original rollback checkpoint fault")

        monkeypatch.setattr("deerflow.runtime.runs.worker._rollback_to_pre_run_checkpoint", fail_rollback)
    if fault == "latetitle":

        async def fail_late_title(**kwargs):
            raise ValueError("original late title checkpoint fault")

        monkeypatch.setattr("deerflow.runtime.runs.worker._ensure_interrupted_title", fail_late_title)
    item = checkpoint_owner
    import deerflow.persistence.models  # noqa: F401 - register complete actual host metadata
    from deerflow.persistence.base import Base

    async with item.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sf = item.env[1]
    if fault == "completion":
        async with item.engine.begin() as conn:
            await conn.execute(text("CREATE FUNCTION reject_task4_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'original completion SQL fault'; END $$"))
            await conn.execute(text("CREATE TRIGGER task4_completion_fault BEFORE UPDATE ON runs FOR EACH ROW WHEN (OLD.status='success') EXECUTE FUNCTION reject_task4_completion()"))
    capability = FleetMutationCapability(item.identity, item.spec)
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 30
    from types import SimpleNamespace

    from deerflow.runtime.user_context import reset_current_user, set_current_user

    @contextmanager
    def original_scope():
        token = set_current_user(SimpleNamespace(id=item.spec.user_id))
        try:
            with remote_mutation_scope(capability.context), workspace_writer_scope(controller):
                yield
        finally:
            reset_current_user(token)

    teardown = _AgentResourceTeardown(AsyncExitStack(), original_scope, capability.context)
    teardown.workspace_writers = controller
    publisher = FleetWorkspacePublisher(sf, capability, controller=controller, teardown=teardown, session_pool=MCPSessionPool())
    repository = RunRepository(sf, mutation_capability=capability, terminal_participant=publisher.terminal)
    manager = RunManager(store=repository, worker_id=item.identity.owner_worker_id)
    paths_value = paths.Paths(tmp_path / "home")
    monkeypatch.setattr(paths, "_paths", paths_value)
    paths_value.ensure_thread_dirs(item.spec.thread_id, user_id=item.spec.user_id)
    output = paths_value.sandbox_outputs_dir(item.spec.thread_id, user_id=item.spec.user_id) / "same.txt"
    output.write_bytes(b"original real presentation bytes\n")
    source = paths_value.sandbox_user_data_dir(item.spec.thread_id, user_id=item.spec.user_id)
    cfg = item.env[3].config
    versions = AgentWorkspaceVersions(NASWorkspace(cfg.nas_root, identity=cfg.nas_identity), max_input_bytes=item.grant["input_limits"]["max_input_bytes"], max_output_bytes=item.grant["execution_profile"]["max_output_bytes"])
    responses = [AIMessage(content="", tool_calls=[{"id": "provider-reused", "name": "present_files", "args": {"filepaths": ["/mnt/user-data/outputs/same.txt"]}, "type": "tool_call"}]) for _ in range(2)]
    from langchain_core.tools import tool
    from langgraph.types import interrupt

    question_entries, completed_goal_tools, goal_evaluations = [], [], []

    @tool
    def completed_goal_sideeffect() -> str:
        """Complete once while the original parallel question awaits input."""
        completed_goal_tools.append("completed")
        return "completed before original question"

    @tool
    def request_original_input() -> str:
        """Pause this original graph for human input."""
        question_entries.append("entered")
        return interrupt({"question": "Continue?"})

    from deerflow.agents.middlewares.clarification_middleware import ClarificationMiddleware
    from deerflow.tools.builtins.clarification_tool import ask_clarification_tool

    if goal_continuation:
        # The first original graph turn must complete before the real active
        # goal helper writes/streams its hidden continuation.
        responses.append(AIMessage(content="Original first turn still needs a user decision"))

        class GoalProvider(FakeToolCallingModel):
            async def _agenerate(self, *args, **kwargs):
                goal_evaluations.append("called")
                return await super()._agenerate(*args, **kwargs)

        evaluator = GoalProvider(
            responses=[
                AIMessage(content='{"satisfied":false,"blocker":"goal_not_met_yet","reason":"Needs another turn","evidence_summary":"First turn prepared artifacts"}'),
                AIMessage(content='{"satisfied":true,"blocker":"none","reason":"Unexpected second evaluation","evidence_summary":"Must not evaluate a paused turn"}'),
            ]
        )
        monkeypatch.setattr("deerflow.runtime.runs.worker.create_goal_evaluator_model", lambda **kwargs: evaluator)
    if pause:
        pause_call = {
            "id": "pause-turn",
            "name": "ask_clarification" if pause == "clarification" else "request_original_input",
            "args": {"question": "Continue?", "clarification_type": "missing_info"} if pause == "clarification" else {},
            "type": "tool_call",
        }
        calls = [pause_call]
        if goal_continuation and pause is True:
            calls.insert(0, {"id": "goal-completed", "name": "completed_goal_sideeffect", "args": {}, "type": "tool_call"})
        responses.append(AIMessage(content="", tool_calls=calls))
    responses.append(AIMessage(content="original final answer", additional_kwargs={"deerflow_error_fallback": True} if fault == "editrollback" else {}))
    graph = create_deerflow_agent(
        FakeToolCallingModel(responses=responses),
        tools=[present_file_tool, request_original_input, completed_goal_sideeffect, ask_clarification_tool],
        middleware=[ThreadDataMiddleware(base_dir=str(paths_value.base_dir), lazy_init=True), ClarificationMiddleware()],
        checkpoint_channel_mode=mode,
    )
    stream_identity = RemoteStreamIdentity.from_context(capability.context)
    events = DbRunEventStore(sf, mutation_capability=capability, transaction_participant=FleetEventParticipant(identity=stream_identity, spec=item.spec, capability=capability, outbox=EventOutbox()))
    bridge = FleetProducerBridge(event_store=events, identity=stream_identity, spec=item.spec, capability=capability, seals=FleetStreamSeals(sf), manager=manager)
    thread_store = ThreadMetaRepository(sf, mutation_capability=capability)
    terminal_observation = []

    async def observe_original_terminal(record):
        terminal_observation.append((record.status.value, record.error))
        await publisher.prepare_terminal(record)
        if fault == "latetitle":
            await RunRepository(sf).request_cancel(item.spec.run_id, action="interrupt")

    context = RunContext(
        checkpointer=item.writer, event_store=events, thread_store=thread_store, checkpoint_channel_mode=mode, checkpoint_durability="sync", bind_checkpoint_accessor=publisher.bind_accessor, prepare_terminal=observe_original_terminal
    )
    item.writer.after_root_commit = publisher.on_root_commit
    if fault in {"cancel", "rollback", "rollback_fault"}:
        injected = False

        async def cancel_after_second_accepted_turn(config, metadata):
            nonlocal injected
            await publisher.on_root_commit(config, metadata)
            if not injected and len(controller._accepted_presentations) == 2:
                injected = True
                action = "interrupt" if fault == "cancel" else "rollback"
                await RunRepository(sf).request_cancel(item.spec.run_id, action=action)
                await manager._signal_local_cancel(item.spec.run_id, action=action)

        item.writer.after_root_commit = cancel_after_second_accepted_turn
    stop = asyncio.Event()
    prepared = []

    async def original_node_requests():
        credential = item.env[7]
        base = "/api/fleet/node/attempts/" + item.accepted.attempt_id + "/workspace/"
        auth = {"node_session_id": item.identity.node_session_id, "token": item.accepted.token}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=item.env[4]), base_url="http://native", headers={"Authorization": "Bearer " + credential.token}) as client:
            while not stop.is_set():
                response = await client.post(base + "poll", json=auth)
                if response.status_code == 409:
                    await asyncio.sleep(0.01)
                    continue
                assert response.status_code == 200
                request = response.json()["request"]
                if request is None or request["identity"]["request_id"] in prepared:
                    await asyncio.sleep(0.01)
                    continue
                identity = WorkspaceBoundaryIdentity(**{**request["identity"], "presented_paths": tuple(request["identity"]["presented_paths"])})
                fields = {"request_id": identity.request_id, "request_digest": identity.request_digest, "barrier_epoch": request["barrier_epoch"], "nonce": "e" * 64}
                claim = await client.post(base + "claim", json=auth | fields)
                assert claim.status_code == 200
                assert controller._closed and not controller.unsettled
                fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    candidate = await asyncio.to_thread(versions.seal, identity, fd)
                finally:
                    os.close(fd)
                ack = await client.post(base + "prepared", json=auth | fields | {"manifest": candidate.model_dump(mode="json")})
                assert ack.status_code == 200
                prepared.append(identity.request_id)

    graph_input = {"messages": [HumanMessage(content="Present same path in two turns")]}
    if goal_continuation:
        from deerflow.runtime.goal import build_goal_state

        graph_input["goal"] = build_goal_state("Publish artifacts then obtain the original user decision", max_continuations=2)
    with original_scope():
        await item.writer.adelete_thread(item.spec.thread_id)
        record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
        if fault == "editrollback":
            record.metadata = {"replay_kind": "edit"}
        await thread_store.ensure_executor_thread(item.spec.thread_id, user_id=item.spec.user_id)
        node_task = asyncio.create_task(original_node_requests())
        try:
            async with asyncio.timeout(30):
                execute = run_agent(
                    bridge,
                    manager,
                    record,
                    ctx=context,
                    agent_factory=lambda **_: graph,
                    graph_input=graph_input,
                    config={"configurable": {"thread_id": item.spec.thread_id}, "context": {"user_id": item.spec.user_id}},
                    stream_modes=streams,
                )
                if fault is not None:
                    from deerflow.runtime.execution.mutation_context import ExecutionWorkspaceFailure

                    with pytest.raises(
                        ExecutionWorkspaceFailure,
                        match="duration"
                        if fault == "duration"
                        else "rollback"
                        if fault in {"rollback_fault", "editrollback"}
                        else "checkpoint"
                        if fault == "migration"
                        else "completion"
                        if fault == "completion"
                        else "late title"
                        if fault == "latetitle"
                        else "preparation",
                    ):
                        await execute
                else:
                    await execute
        finally:
            stop.set()
            await node_task
    if fault is not None:
        async with sf() as session:
            assert await session.scalar(text("SELECT status FROM runs")) == ("success" if fault == "completion" else "running")
            assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE kind!='partial'")) == (1 if fault == "completion" else 0)
            assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 0
        if fault == "completion":
            async with sf() as session:
                original_point = await session.scalar(text("SELECT row_to_json(p)::text FROM fleet_workspace_points p WHERE kind='final'"))
                process_ref = await session.scalar(text("SELECT process_ref FROM fleet_attempts"))
            await item.env[4].state.fleet_ownership.stopped(
                reason="exit", exit_code=0, process_ref=process_ref, physical_stopped=True, node_id=item.identity.node_id, node_session_id=item.identity.node_session_id, attempt_id=item.identity.attempt_id, token=item.accepted.token
            )
            async with sf() as session:
                assert await session.scalar(text("SELECT row_to_json(p)::text FROM fleet_workspace_points p WHERE kind='final'")) == original_point
                assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "succeeded"
                assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 1
                assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "released"
        if fault in {"cancel", "rollback", "rollback_fault"}:
            if fault != "rollback_fault":
                assert terminal_observation[0] == (("interrupted", None) if fault == "cancel" else ("error", "Rolled back by user"))
            async with sf() as session:
                process_ref = await session.scalar(text("SELECT process_ref FROM fleet_attempts"))
            await item.env[4].state.fleet_ownership.stopped(
                reason="exit", exit_code=0, process_ref=process_ref, physical_stopped=True, node_id=item.identity.node_id, node_session_id=item.identity.node_session_id, attempt_id=item.identity.attempt_id, token=item.accepted.token
            )
            async with sf() as session:
                assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "recovery_required"
                assert await session.scalar(text("SELECT state FROM fleet_run_placements")) == "recovery_required"
                assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 0
                assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "released"
        return
    snapshot = await publisher.accessor.aget({"configurable": {"thread_id": item.spec.thread_id}})
    messages = [m for m in snapshot.values["messages"] if isinstance(m, ToolMessage) and m.name == "present_files"]
    diagnostics = {"tools": [{"name": m.name, "status": m.status, "id": m.id, "content": str(m.content)} for m in messages], "pending": list(controller.pending_presentations)}
    evidence = __import__("pathlib").Path(__file__).resolve().parents[3] / ".local/fleet-evidence/c08-task4"
    (evidence / f"stock-observation-{mode}-{len(streams)}-{pause}-{__import__('uuid').uuid4().hex}.json").write_text(__import__("json").dumps(diagnostics, indent=2))
    assert len(messages) == 2 and all(m.status == "success" for m in messages), diagnostics
    if goal_continuation:
        hidden = [message for message in snapshot.values["messages"] if isinstance(message, HumanMessage) and message.additional_kwargs.get("deerflow_goal_continuation")]
        raw = await item.writer.aget_tuple(snapshot.config)

        def tool_messages(value):
            if isinstance(value, ToolMessage):
                yield value
            elif isinstance(value, dict):
                for child in value.values():
                    yield from tool_messages(child)
            elif isinstance(value, (list, tuple)):
                for child in value:
                    yield from tool_messages(child)

        pending_tools = [message for _, _, value in raw.pending_writes for message in tool_messages(value)]
        goal_diagnostic = {
            "goal_evaluator_calls": len(goal_evaluations),
            "hidden_turn_count": len(hidden),
            "goal": snapshot.values.get("goal"),
            "pending_next": list(snapshot.next),
            "interrupt_ids": [entry.id for entry in snapshot.interrupts],
            "pending_tool_names": [message.name for message in pending_tools],
            "question_entries": question_entries,
            "completed_goal_tools": completed_goal_tools,
            "root_checkpoint_id": snapshot.config["configurable"]["checkpoint_id"],
        }
        (evidence / f"goal-pause-{mode}-{len(streams)}-{pause}-{__import__('uuid').uuid4().hex}.json").write_text(__import__("json").dumps(goal_diagnostic, indent=2))
        assert len(goal_evaluations) == 1, goal_diagnostic
        assert len(hidden) == 1 and snapshot.values["goal"]["continuation_count"] == 1, goal_diagnostic
        assert not any(message.content == "original final answer" for message in snapshot.values["messages"]), goal_diagnostic
        if pause is True:
            assert question_entries == ["entered"] and completed_goal_tools == ["completed"], goal_diagnostic
            assert snapshot.next and snapshot.interrupts, goal_diagnostic
            assert any(message.name == "completed_goal_sideeffect" and message.tool_call_id == "goal-completed" for message in pending_tools), goal_diagnostic
        else:
            card = snapshot.values["messages"][-1]
            assert card.tool_call_id == "pause-turn" and card.id == card.artifact["human_input"]["request_id"]
            assert controller.awaiting_human_input(snapshot) and not snapshot.next

    async with sf() as session:
        points = (await session.execute(text("SELECT kind,checkpoint_id,manifest_id,publication_key FROM fleet_workspace_points ORDER BY accepted_at"))).all()
        assert [row.kind for row in points] == ["partial", "partial", "paused" if pause else "final"]
        assert len({row.manifest_id for row in points}) == 3
        assert len({row.publication_key for row in points[:2]}) == 2
        assert await session.scalar(text("SELECT status FROM runs")) == ("interrupted" if pause else "success")
        assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "finishing"
        assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 1
        head = await session.scalar(text("SELECT max(checkpoint_id) FROM checkpoints WHERE checkpoint_ns=''"))
        assert head == points[-1].checkpoint_id
    if pause:
        if pause == "clarification":
            assert not snapshot.next
            human_message = snapshot.values["messages"][-1]
            assert isinstance(human_message, ToolMessage) and human_message.artifact["human_input"]["kind"] == "human_input_request"
        else:
            assert snapshot.next and any(task.interrupts for task in snapshot.tasks)
        async with sf() as session:
            assert await session.scalar(text("SELECT desired_task_status FROM fleet_workspace_points WHERE kind='paused'")) == "input_required"
            assert await session.scalar(text("SELECT desired_placement_status FROM fleet_workspace_points WHERE kind='paused'")) == "cancelled"
    assert controller._final and controller._closed
    assert len(prepared) == 3
    async with sf() as session:
        process_ref = await session.scalar(text("SELECT process_ref FROM fleet_attempts"))
    result = await item.env[4].state.fleet_ownership.stopped(
        reason="exit", exit_code=0, process_ref=process_ref, physical_stopped=True, node_id=item.identity.node_id, node_session_id=item.identity.node_session_id, attempt_id=item.identity.attempt_id, token=item.accepted.token
    )
    assert result["stopped"]
    async with sf() as session:
        assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == ("input_required" if pause else "succeeded")
        assert await session.scalar(text("SELECT state FROM fleet_run_placements")) == ("cancelled" if pause else "succeeded")
        assert await session.scalar(text("SELECT state FROM fleet_attempts")) == ("cancelled" if pause else "succeeded")
        assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "released"
    with original_scope():
        await teardown.close()


@pytest.mark.asyncio
async def test_actual_duration_checkpoint_failure_cannot_accept_final(checkpoint_owner, tmp_path, monkeypatch):
    await test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final(checkpoint_owner, tmp_path, monkeypatch, "full", ["values"], False, fault="duration")


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["cancel", "rollback", "rollback_fault"])
async def test_actual_cancellation_cannot_accept_previous_success_pair(checkpoint_owner, tmp_path, monkeypatch, fault):
    await test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final(checkpoint_owner, tmp_path, monkeypatch, "full", ["values"], False, fault=fault)


@pytest.mark.asyncio
async def test_actual_pending_checkpoint_migration_failure_cannot_accept_pause(checkpoint_owner, tmp_path, monkeypatch):
    await test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final(checkpoint_owner, tmp_path, monkeypatch, "delta", ["values", "custom"], True, fault="migration")


@pytest.mark.asyncio
async def test_actual_completion_sql_failure_retains_final_point_but_cannot_hide_error(checkpoint_owner, tmp_path, monkeypatch):
    await test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final(checkpoint_owner, tmp_path, monkeypatch, "full", ["values"], False, fault="completion")


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["editrollback", "latetitle"])
async def test_actual_final_checkpoint_tail_failure_cannot_be_replaced(checkpoint_owner, tmp_path, monkeypatch, fault):
    await test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final(checkpoint_owner, tmp_path, monkeypatch, "full", ["values"], False, fault=fault)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "delta"])
@pytest.mark.parametrize("streams", [["values"], ["values", "custom"]])
@pytest.mark.parametrize("pause", [True, "clarification"])
async def test_original_goal_hidden_turn_pause_precedes_any_next_evaluation(checkpoint_owner, tmp_path, monkeypatch, mode, streams, pause):
    await test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final(checkpoint_owner, tmp_path, monkeypatch, mode, streams, pause, goal_continuation=True)
