"""Durable candidate protocol on real PostgreSQL; never accepts a point."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from .test_c08_workspace_transactions import boundary_db as boundary_db  # noqa: F401
from .test_c08_workspace_transactions import owner


@pytest.mark.asyncio
async def test_actual_request_claim_replay_nonce_and_epoch_are_fenced(boundary_db):  # noqa: F811
    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity
    from deerflow_ecs_fleet.persistence import workspace_points

    _, sf, models = boundary_db
    identity = WorkspaceBoundaryIdentity(request_id="real-request", checkpoint_id="actual-root", kind="partial", publication_key="presentation", presented_paths=(), source_workspace_version="original", **owner())
    requests_type = getattr(workspace_points, "WorkspaceRequests", None)
    assert requests_type is not None, "durable original workspace publication/claim protocol is absent"
    requests = requests_type()
    async with sf.begin() as session:
        first = await requests.create(session, identity, barrier_epoch=1)
        same = await requests.create(session, identity, barrier_epoch=1)
        assert first.id == same.id
    deadline = datetime.now(UTC) + timedelta(seconds=30)
    async with sf.begin() as session:
        claimed = await requests.claim(session, identity, nonce="e" * 64, barrier_epoch=1, deadline=deadline)
        original_expiry = claimed.claim_lease_expires_at
    async with sf.begin() as session:
        same = await requests.claim(session, identity, nonce="e" * 64, barrier_epoch=1, deadline=deadline)
        assert same.claim_lease_expires_at == original_expiry
    for changes in ({"nonce": "f" * 64}, {"barrier_epoch": 2}):
        with pytest.raises(ValueError):
            async with sf.begin() as session:
                await requests.claim(session, identity, **({"nonce": "e" * 64, "barrier_epoch": 1} | changes), deadline=deadline)
    async with sf() as session:
        row = await session.get(models.WorkspaceRequestRow, identity.request_id)
        assert row.state == "sealing" and row.claim_nonce == "e" * 64 and row.barrier_epoch == 1
        assert not (await session.execute(select(models.WorkspacePointRow.id))).all()


@pytest.mark.asyncio
async def test_actual_prepared_candidate_lost_ack_retry_and_sql_rollback(boundary_db, tmp_path):  # noqa: F811
    import os

    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions, WorkspaceBoundaryIdentity
    from deerflow_ecs_fleet.persistence import workspace_points
    from deerflow_ecs_fleet.workspace import NASWorkspace
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    _, sf, models = boundary_db
    identity = WorkspaceBoundaryIdentity(request_id="actual-ack", checkpoint_id="actual-root", kind="partial", publication_key="presentation", presented_paths=(), source_workspace_version="original", **owner())
    requests = workspace_points.WorkspaceRequests()
    deadline = datetime.now(UTC) + timedelta(seconds=30)
    async with sf.begin() as session:
        await requests.create(session, identity, barrier_epoch=1)
        await requests.claim(session, identity, nonce="e" * 64, barrier_epoch=1, deadline=deadline)
    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("actual-test\n")
    source = tmp_path / "source"
    source.mkdir()
    for category in ("workspace", "uploads", "outputs"):
        (source / category).mkdir()
    (source / "outputs/result").write_bytes(b"actual candidate")
    versions = AgentWorkspaceVersions(NASWorkspace(nas, identity="actual-test"), max_input_bytes=1024, max_output_bytes=1024)
    source_fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        # Physical copy precedes the SQL transaction. A real SQL failure leaves
        # its immutable NAS candidate unaccepted and its request still sealing.
        manifest = versions.seal(identity, source_fd)
    finally:
        os.close(source_fd)
    with pytest.raises(DBAPIError):
        async with sf.begin() as session:
            await requests.prepared(session, identity, nonce="e" * 64, barrier_epoch=1, manifest=manifest)
            await session.execute(text("SELECT 1/0"))
    async with sf() as session:
        assert (await session.get(models.WorkspaceRequestRow, identity.request_id)).state == "sealing"
        assert not (await session.execute(select(models.WorkspaceManifestRow.id))).all()
    verified = versions.verify(manifest)
    async with sf.begin() as session:
        first = await requests.prepared(session, identity, nonce="e" * 64, barrier_epoch=1, manifest=verified)
        original_id = first.candidate_manifest_id
    # Simulate the trusted Node losing its ack response and restarting after
    # the short claim lease. The original execution deadline still bounds it.
    async with sf.begin() as session:
        row = await session.get(models.WorkspaceRequestRow, identity.request_id)
        row.claim_lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    verified_again = versions.verify(manifest)
    async with sf.begin() as session:
        same = await requests.claim(session, identity, nonce="e" * 64, barrier_epoch=1, deadline=deadline)
        assert same.candidate_manifest_id == original_id
        same = await requests.prepared(session, identity, nonce="e" * 64, barrier_epoch=1, manifest=verified_again)
        assert same.candidate_manifest_id == original_id
    async with sf() as session:
        assert len((await session.execute(select(models.WorkspaceManifestRow.id))).all()) == 1
        assert not (await session.execute(select(models.WorkspacePointRow.id))).all()


from .test_c02_remote_agent_admission import admission as admission  # noqa: E402,F401
from .test_c03_remote_agent_admission import owner_environment as owner_environment  # noqa: E402,F401
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner  # noqa: E402,F401


@pytest.mark.asyncio
async def test_actual_private_publication_barrier_uses_original_teardown_without_starting_partial_budget(checkpoint_owner):  # noqa: F811
    import time
    from contextlib import AsyncExitStack, contextmanager

    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

    import app.fleet.workspace as workspace
    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import _AgentResourceTeardown
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    item = checkpoint_owner
    from .c08_native_terminal_pair import advance_original_root

    await advance_original_root(item)
    capability = FleetMutationCapability(item.identity, item.spec)
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 2

    @contextmanager
    def original_scope():
        with remote_mutation_scope(capability.context), workspace_writer_scope(controller):
            yield

    teardown = _AgentResourceTeardown(AsyncExitStack(), original_scope, capability.context)
    teardown.workspace_writers = controller
    publisher_type = getattr(workspace, "FleetWorkspacePublisher", None)
    assert publisher_type is not None, "original private publication/teardown boundary is absent"
    publisher = publisher_type(item.env[1], capability, controller=controller, teardown=teardown, session_pool=MCPSessionPool())
    identity = WorkspaceBoundaryIdentity.from_context(
        capability.context, request_id="actual-private-partial", checkpoint_id=item.config["configurable"]["checkpoint_id"], kind="partial", publication_key="actual-presentation", presented_paths=(), source_workspace_version="original"
    )
    with original_scope():
        epoch = await publisher.publish(identity)
        assert epoch == controller.barrier_epoch == 1
        assert teardown.budget._deadline is None
        with pytest.raises(TimeoutError):
            await publisher.wait_prepared(identity, barrier_epoch=epoch, deadline=time.monotonic() + 0.03)
        assert teardown.budget._deadline is None
        assert not teardown.closed
        await teardown.close()
    assert teardown.closed and teardown.budget._deadline is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("partial", "final"))
async def test_actual_producer_stops_owned_native_process_before_sql_with_original_budget(checkpoint_owner, tmp_path, monkeypatch, kind):  # noqa: F811
    import asyncio
    import hashlib
    import subprocess
    import sys
    import time
    from contextlib import AsyncExitStack, contextmanager

    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import _AgentResourceTeardown
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    item = checkpoint_owner
    from .c08_native_terminal_pair import advance_original_root

    await advance_original_root(item)
    capability = FleetMutationCapability(item.identity, item.spec)
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 3
    ticks = tmp_path / "native-owned.ticks"
    # Native physical-owner proof only; Linux subreaper/namespace proof remains
    # the fresh image gate. This handle represents the optional host interface.
    child = subprocess.Popen([sys.executable, "-c", "import sys,time; f=open(sys.argv[1],'ab',buffering=0);\nwhile True: f.write(b't'); time.sleep(.02)", str(ticks)])

    class OriginalHandle:
        _registered_shell = _registered_supervisor = False

        def stop_and_join(self, deadline):
            child.terminate()
            child.wait(timeout=max(0, deadline - time.monotonic()))

        def join(self, deadline):
            child.wait(timeout=max(0, deadline - time.monotonic()))

    controller.retain_process(OriginalHandle())

    @contextmanager
    def original_scope():
        with remote_mutation_scope(capability.context), workspace_writer_scope(controller):
            yield

    teardown = _AgentResourceTeardown(AsyncExitStack(), original_scope, capability.context)
    teardown.workspace_writers = controller
    publisher = FleetWorkspacePublisher(item.env[1], capability, controller=controller, teardown=teardown, session_pool=MCPSessionPool())
    original_create = publisher.requests.create
    reached_actual_sql = []

    async def checked_create(session, identity, **kwargs):
        assert child.poll() is not None, "Original retained process must be physically joined before durable request SQL"
        assert not controller.unsettled
        if kind == "partial":
            assert teardown.budget._deadline is None
        else:
            assert teardown.budget._deadline is not None
            assert 119 < teardown.budget._deadline - time.monotonic() <= 120
        reached_actual_sql.append(identity.request_id)
        return await original_create(session, identity, **kwargs)

    monkeypatch.setattr(publisher.requests, "create", checked_create)
    identity = WorkspaceBoundaryIdentity.from_context(
        capability.context,
        request_id=kind + "-physical-stop",
        checkpoint_id=item.config["configurable"]["checkpoint_id"],
        kind=kind,
        publication_key="native-outputs",
        presented_paths=(),
        source_workspace_version="original",
        **({} if kind == "partial" else {"desired_core_status": "success", "desired_task_status": "succeeded", "desired_placement_status": "succeeded"}),
    )
    try:
        for _ in range(100):
            if ticks.exists():
                break
            await asyncio.sleep(0.01)
        assert ticks.exists() and child.poll() is None
        with original_scope():
            await publisher.publish(identity)
        assert reached_actual_sql == [identity.request_id]
        before = hashlib.sha256(ticks.read_bytes()).hexdigest()
        await asyncio.sleep(0.06)
        assert hashlib.sha256(ticks.read_bytes()).hexdigest() == before
        assert not teardown.closed
        if kind == "partial":
            assert teardown.budget._deadline is None
        else:
            original_deadline = teardown.budget._deadline
            assert controller._final
            await teardown.close()
            assert teardown.budget._deadline == original_deadline
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=3)
        await teardown.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ("cancel", "timeout"))
@pytest.mark.parametrize("kind", ("partial", "final"))
async def test_actual_publisher_mcp_owner_pending_prevents_original_resource_stack_unwind(checkpoint_owner, monkeypatch, interruption, kind):  # noqa: F811
    import asyncio
    import sys
    import time
    from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
    from pathlib import Path

    import langchain_mcp_adapters.sessions as sessions
    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import _AgentResourceTeardown
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    item = checkpoint_owner
    from .c08_native_terminal_pair import advance_original_root

    await advance_original_root(item)
    capability = FleetMutationCapability(item.identity, item.spec)
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + (0.05 if interruption == "timeout" else 3)
    release, exiting = asyncio.Event(), asyncio.Event()
    original_create = sessions.create_session

    @asynccontextmanager
    async def parked_real_session(connection):
        async with original_create(connection) as actual_session:
            try:
                yield actual_session
            finally:
                # Park before the real SDK child/context closes, on the real
                # pool owner task and its original anyio ownership scope.
                exiting.set()
                await release.wait()

    monkeypatch.setattr(sessions, "create_session", parked_real_session)
    pool = MCPSessionPool()
    scope = capability.context.user_id + ":" + capability.context.thread_id
    session = await pool.get_session("real-c04", scope, {"transport": "stdio", "command": sys.executable, "args": [str(Path(__file__).with_name("c04_mcp_fixture.py"))], "env": {"ERP_AUTH": "c04-target-access"}})
    assert (await session.call_tool("echo", {"value": "actual-retained-owner"})).content[0].text == "actual-retained-owner"
    owner = pool._entries[("real-c04", scope)][2]
    # Session startup is outside the deliberately short publication deadline.
    controller.execution_deadline = time.monotonic() + (0.05 if interruption == "timeout" else 3)

    @contextmanager
    def original_scope():
        with remote_mutation_scope(capability.context), workspace_writer_scope(controller):
            yield

    stack = AsyncExitStack()
    unwound = []
    for resource in ("database", "memory", "plugins"):
        stack.callback(unwound.append, resource)
    stack.push_async_callback(pool.close_all)
    teardown = _AgentResourceTeardown(stack, original_scope, capability.context)
    teardown.workspace_writers = controller
    publisher = FleetWorkspacePublisher(item.env[1], capability, controller=controller, teardown=teardown, session_pool=pool)
    identity = WorkspaceBoundaryIdentity.from_context(
        capability.context,
        request_id="actual-mcp-pending-" + interruption + kind,
        checkpoint_id=item.config["configurable"]["checkpoint_id"],
        kind=kind,
        publication_key="presentation",
        presented_paths=(),
        source_workspace_version="original",
        **({} if kind == "partial" else {"desired_core_status": "success", "desired_task_status": "succeeded", "desired_placement_status": "succeeded"}),
    )
    if kind == "final" and interruption == "timeout":
        import deerflow_ecs_fleet.worker.agent_cleanup as cleanup

        monkeypatch.setattr(cleanup, "TOTAL_CLEANUP_SECONDS", 0.05)
    joining = None
    try:
        with original_scope():
            publication = asyncio.create_task(publisher.publish(identity))
            await asyncio.wait_for(exiting.wait(), 2)
            if interruption == "cancel":
                publication.cancel()
            with pytest.raises(BaseException):
                await publication
            assert (teardown.budget._deadline is None) == (kind == "partial")
            retained_phase = teardown._phases.get("workspace-mcp")
            if kind == "final" and interruption == "cancel":
                assert retained_phase is not None and not retained_phase.done()
            assert not owner.done() and pool._scope_closing[scope]
            joining = asyncio.create_task(teardown.close())
            await asyncio.sleep(0.03)
            assert not unwound and not teardown.closed, "Original resources unwound before the real MCP owner joined"
            deadline = teardown.budget.deadline
            if retained_phase is not None and kind == "final" and interruption == "cancel":
                assert teardown._phases["workspace-mcp"] is retained_phase
            release.set()
            await asyncio.gather(joining, return_exceptions=True)
            await asyncio.wait_for(asyncio.shield(owner), 2)
            assert owner.done()
            if kind == "final" and interruption == "timeout":
                # Exhaustion preserves the original resource owner rather than
                # granting another cleanup window or unwinding live resources.
                assert not teardown.closed and not unwound
                assert teardown.budget.remaining() == 0
            else:
                assert not pool._scope_closing.get(scope)
                assert teardown.closed and unwound == ["plugins", "memory", "database"]
            assert teardown.budget.deadline == deadline
    finally:
        release.set()
        await asyncio.gather(owner, return_exceptions=True)
        if joining is not None:
            await asyncio.gather(joining, return_exceptions=True)
        await stack.aclose()
        # Fixture-only release of an exhausted owner after the physical SDK
        # task has joined; production never receives another cleanup budget.
        from app.fleet.runner_context import _pending_agent_teardowns

        _pending_agent_teardowns.pop(capability.context, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("final", "paused"))
async def test_actual_final_candidate_wait_cannot_outlive_original_started_cleanup_budget(checkpoint_owner, kind):  # noqa: F811
    import time
    from contextlib import AsyncExitStack, contextmanager

    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import _AgentResourceTeardown
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    item = checkpoint_owner
    from .c08_native_terminal_pair import advance_original_root

    await advance_original_root(item)
    capability = FleetMutationCapability(item.identity, item.spec)
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 3

    @contextmanager
    def original_scope():
        with remote_mutation_scope(capability.context), workspace_writer_scope(controller):
            yield

    teardown = _AgentResourceTeardown(AsyncExitStack(), original_scope, capability.context)
    teardown.workspace_writers = controller
    publisher = FleetWorkspacePublisher(item.env[1], capability, controller=controller, teardown=teardown, session_pool=MCPSessionPool())
    identity = WorkspaceBoundaryIdentity.from_context(
        capability.context,
        request_id="actual-final-budget-" + kind,
        checkpoint_id=item.config["configurable"]["checkpoint_id"],
        kind=kind,
        publication_key="presentation",
        presented_paths=(),
        source_workspace_version="original",
        desired_core_status="success",
        desired_task_status="paused" if kind == "paused" else "succeeded",
        desired_placement_status="succeeded",
    )
    with original_scope():
        epoch = await publisher.publish(identity)
        # Trusted fixture shortens only the already-started same budget. The
        # production default remains one cumulative 120-second deadline.
        teardown.budget._deadline = time.monotonic() + 0.04
        original_deadline = teardown.budget.deadline
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await publisher.wait_prepared(identity, barrier_epoch=epoch, deadline=started + 0.2)
        assert time.monotonic() - started < 0.13, "Final wait crossed the original cumulative cleanup deadline"
        assert teardown.budget.deadline == original_deadline
