"""Native original AgentRunner/worker with real PG, ASGI Node RPC and NAS.

The held graph node is a native control probe. It does not claim installed
lead-agent image execution or a physical Docker process stop.
"""

import asyncio
import os
import time
from contextlib import AsyncExitStack, contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx


async def original_native_execution(item, directory, monkeypatch, *, seed_rollback=False, transactional_lifecycle=False, complete_graph=None, checkpoint_mode=None, seed_pending=False, graph_override=None, wait_entered=True):
    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions, WorkspaceBoundaryIdentity
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility, thaw
    from deerflow_ecs_fleet.persistence.outbox import EventOutbox
    from deerflow_ecs_fleet.worker.agent_environment import AgentEnvironment
    from deerflow_ecs_fleet.worker.agent_runner import AgentRunner
    from deerflow_ecs_fleet.workspace import NASWorkspace
    from langgraph.graph import END, START, StateGraph

    import deerflow.persistence.models  # noqa: F401
    from app.fleet.agent_control import OriginalAgentCancellation
    from app.fleet.events import FleetEventParticipant, FleetProducerBridge, FleetStreamSeals, RemoteStreamIdentity
    from app.fleet.execution import decode_graph_input
    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import _AgentResourceTeardown
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.agents.thread_state import ThreadState, get_thread_state_schema
    from deerflow.config import paths
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.persistence.base import Base
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext

    async with item.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sf = item.env[1]
    capability = FleetMutationCapability(item.identity, item.spec)
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + (item.spec.execution_deadline - datetime.now(UTC)).total_seconds()

    @contextmanager
    def scope():
        with remote_mutation_scope(capability.context), capability.cancellation_settlement_scope(), workspace_writer_scope(controller):
            yield

    teardown = _AgentResourceTeardown(AsyncExitStack(), scope, capability.context)
    teardown.workspace_writers = controller
    private, writer = item.private, item.writer
    if checkpoint_mode is not None:
        import deerflow.runtime.checkpoint_mode as mode_module
        from app.fleet.runner_context import FleetCheckpointFence
        from deerflow.runtime.checkpointer.async_provider import make_checkpointer

        monkeypatch.setattr(mode_module, "_frozen_checkpoint_channel_mode", None)
        private = item.private.model_copy(update={"database": item.private.database.model_copy(update={"checkpoint_channel_mode": checkpoint_mode})})
        writer = await teardown.stack.enter_async_context(make_checkpointer(private, write_fence=FleetCheckpointFence(item.identity, item.spec)))
    publisher = FleetWorkspacePublisher(sf, capability, controller=controller, teardown=teardown, session_pool=MCPSessionPool())
    repository = RunRepository(sf, mutation_capability=capability, terminal_participant=publisher.terminal)
    manager = RunManager(store=repository, worker_id=item.identity.owner_worker_id)
    cancellation = OriginalAgentCancellation(sf, capability, manager, teardown, controller)
    paths_value = paths.Paths(directory / "home")
    monkeypatch.setattr(paths, "_paths", paths_value)
    paths_value.ensure_thread_dirs(item.spec.thread_id, user_id=item.spec.user_id)
    source = paths_value.sandbox_user_data_dir(item.spec.thread_id, user_id=item.spec.user_id)
    paths_value.sandbox_outputs_dir(item.spec.thread_id, user_id=item.spec.user_id).joinpath("original.txt").write_bytes(b"original workspace before interrupt\n")
    cfg = item.env[3].config
    versions = AgentWorkspaceVersions(NASWorkspace(cfg.nas_root, identity=cfg.nas_identity), max_input_bytes=item.grant["input_limits"]["max_input_bytes"], max_output_bytes=item.grant["execution_profile"]["max_output_bytes"])
    entered, release_observer, stop_node = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def held_original_node(state):
        entered.set()
        await (complete_graph if complete_graph is not None else asyncio.Event()).wait()
        from langchain_core.messages import AIMessage

        return {"messages": [AIMessage(content="Original native graph completed", id="c09-completed")]}

    graph = StateGraph(ThreadState if checkpoint_mode is None else get_thread_state_schema(checkpoint_mode))
    graph.add_node("held_original", held_original_node)
    graph.add_edge(START, "held_original")
    graph.add_edge("held_original", END)
    compiled = graph.compile(checkpointer=writer) if graph_override is None else graph_override
    if graph_override is not None:
        compiled.checkpointer = writer
    identity = RemoteStreamIdentity.from_context(capability.context)
    events = DbRunEventStore(sf, mutation_capability=capability, transaction_participant=FleetEventParticipant(identity=identity, spec=item.spec, capability=capability, outbox=EventOutbox()))
    bridge = FleetProducerBridge(event_store=events, identity=identity, spec=item.spec, capability=capability, seals=FleetStreamSeals(sf), manager=manager)
    extensions = None
    lifecycle = None
    if transactional_lifecycle:
        # Activate the original installed C04 task-lifecycle contributor.
        # Its unrelated service startup/private MCP checks are outside this
        # native worker receipt probe; the host binds the original APIs here.
        from deerflow_c04_fixture import Service
        from deerflow_extension_api import ExtensionRuntimeDeps
        from sqlalchemy import text

        from deerflow.extensions.gateway import _TerminalExtensionOperations, bind_remote_extensions
        from deerflow.extensions.registry import ExtensionRegistry
        from deerflow.runtime.execution.mutation_transactions import BoundMutationTransactions

        async with item.engine.begin() as conn:
            await conn.execute(text("CREATE TABLE c06_extension(value text)"))
        lifecycle = Service()
        lifecycle._lead = None
        lifecycle._deps = ExtensionRuntimeDeps(
            terminal_operations=_TerminalExtensionOperations(sf, capability),
            mutation_transactions=BoundMutationTransactions(capability, operation="extension.write", session_factory=sf),
        )
        registry = ExtensionRegistry()
        with registry.attributed_to("original-installed-c04-lifecycle"):
            registry.task_lifecycle(lifecycle)
        extensions = bind_remote_extensions(registry.build(), capability)
    context = RunContext(
        checkpointer=writer,
        event_store=events,
        thread_store=ThreadMetaRepository(sf, mutation_capability=capability),
        app_config=private,
        extensions=extensions,
        checkpoint_durability="sync",
        checkpoint_channel_mode=checkpoint_mode or "full",
        bind_checkpoint_accessor=publisher.bind_accessor,
        prepare_terminal=publisher.prepare_terminal,
        prepare_cancellation=cancellation.prepare,
        observe_cancellation=cancellation.read,
        cancellation_checkpoint_scope=capability.cancellation_checkpoint_scope,
        cancellation_rollback_scope=capability.cancellation_rollback_scope,
    )
    writer.after_root_commit = publisher.on_root_commit
    with scope():
        if graph_override is None:
            await writer.adelete_thread(item.spec.thread_id)
        if seed_rollback:
            from langchain_core.messages import HumanMessage

            seed_config = await compiled.aupdate_state(
                {"configurable": {"thread_id": item.spec.thread_id}}, {"messages": [HumanMessage(content="Retain this pre-run message", id="c09-before-run")], "title": "Pre-run title"}, as_node="held_original"
            )

            if seed_pending:
                await writer.aput_writes(seed_config, [("messages", HumanMessage(content="Pending pre-run message", id="c09-before-pending")), ("title", "Pending pre-run title")], "c09-pending-task")

    async def observe():
        await release_observer.wait()
        await cancellation.observe()

    async def node_requests():
        auth = {"node_session_id": item.identity.node_session_id, "token": item.accepted.token}
        base = "/api/fleet/node/attempts/" + item.accepted.attempt_id + "/workspace/"
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=item.env[4]), base_url="http://native", headers={"Authorization": "Bearer " + item.env[7].token}) as client:
            while not stop_node.is_set():
                reply = await client.post(base + "poll", json=auth)
                assert reply.status_code == 200, reply.text
                request = reply.json()["request"]
                if request is None or request["state"] == "prepared":
                    await asyncio.sleep(0.01)
                    continue
                boundary = WorkspaceBoundaryIdentity(**{**request["identity"], "presented_paths": tuple(request["identity"]["presented_paths"])})
                fields = {"request_id": boundary.request_id, "request_digest": boundary.request_digest, "barrier_epoch": request["barrier_epoch"], "nonce": "e" * 64}
                reply = await client.post(base + "claim", json=auth | fields)
                assert reply.status_code == 200, reply.text
                assert controller._closed and controller._final and not controller.unsettled
                fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    candidate = await asyncio.to_thread(versions.seal, boundary, fd)
                finally:
                    os.close(fd)
                reply = await client.post(base + "prepared", json=auth | fields | {"manifest": candidate.model_dump(mode="json")})
                assert reply.status_code == 200, reply.text

    environment = AgentEnvironment(
        identity=item.identity,
        context=context,
        manager=manager,
        bridge=bridge,
        agent_factory=lambda **_: compiled,
        compatibility=WorkerCompatibility(runtime_digest=item.spec.runtime_digest, skill_snapshot=item.spec.skill_snapshot, plugin_snapshot=item.spec.plugin_snapshot, workspace_contract_version=1),
        credential_resolver=lambda *args: {},
        decode_input=lambda value: decode_graph_input(thaw(value)),
        close=teardown.close,
        mutation_scope=scope,
        observe_cancellation=observe,
        retain_executor=cancellation.retain,
    )
    node = asyncio.create_task(node_requests(), name="c09-native-node-RPC")
    execute = asyncio.create_task(AgentRunner().run(item.spec, grant=item.grant, environment=environment), name="c09-original-AgentRunner")
    try:
        async with asyncio.timeout(5):
            while wait_entered and not entered.is_set():
                if execute.done():
                    execute.result()
                await asyncio.sleep(0.01)
    except BaseException:
        release_observer.set()
        execute.cancel()
        node.cancel()
        await asyncio.gather(execute, node, return_exceptions=True)
        raise
    return SimpleNamespace(
        writer=writer,
        cancellation=cancellation,
        repository=repository,
        publisher=publisher,
        lifecycle=lifecycle,
        source=source,
        execute=execute,
        node=node,
        release=release_observer,
        stop_node=stop_node,
        manager=manager,
        controller=controller,
        teardown=teardown,
        scope=scope,
        environment=environment,
    )
