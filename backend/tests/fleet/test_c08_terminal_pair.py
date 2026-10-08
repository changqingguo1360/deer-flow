"""Native original RunRepository/fleet pair transactions and failure rollback."""

import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


@pytest_asyncio.fixture
async def prepared_pair(checkpoint_owner, tmp_path, request):
    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions, WorkspaceBoundaryIdentity
    from deerflow_ecs_fleet.persistence.workspace_points import WorkspaceRequests
    from deerflow_ecs_fleet.workspace import NASWorkspace

    from app.fleet.mutation import FleetMutationCapability
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController

    item = checkpoint_owner
    sf = item.env[1]
    capability = FleetMutationCapability(item.identity, item.spec)
    from langgraph.checkpoint.base import empty_checkpoint

    selection = getattr(request, "param", "final")
    outcome = selection if isinstance(selection, dict) else {}
    with remote_mutation_scope(capability.context):
        checkpoint = empty_checkpoint() | {"channel_values": item.checkpoint["channel_values"], "channel_versions": item.checkpoint["channel_versions"]}
        versions = {}
        if "messages" in outcome:
            checkpoint["channel_values"] = dict(checkpoint["channel_values"], messages=outcome["messages"])
            checkpoint["channel_versions"] = dict(checkpoint["channel_versions"], messages="2")
            versions = {"messages": "2"}
        item.config = await item.writer.aput(item.config, checkpoint, {"source": "loop", "step": 1, "parents": {}}, versions)
    kind = outcome.get("kind", selection if isinstance(selection, str) else "final")
    identity = WorkspaceBoundaryIdentity.from_context(
        capability.context,
        request_id="task4-final",
        checkpoint_id=item.config["configurable"]["checkpoint_id"],
        kind=kind,
        publication_key="terminal-success",
        presented_paths=(),
        source_workspace_version="initial",
        desired_core_status=outcome.get("core", "success") if kind != "partial" else None,
        desired_task_status=outcome.get("task", "paused" if kind == "paused" else "succeeded") if kind != "partial" else None,
        desired_placement_status=outcome.get("placement", "succeeded") if kind != "partial" else None,
    )
    source, nas = tmp_path / "source", tmp_path / "nas"
    source.mkdir()
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("task4\n")
    for category in ("workspace", "uploads", "outputs"):
        (source / category).mkdir()
    for name, content in outcome.get("files", {}).items():
        leaf = source / name
        leaf.parent.mkdir(parents=True, exist_ok=True)
        leaf.write_bytes(content)
    versions = AgentWorkspaceVersions(NASWorkspace(nas, identity="task4"), max_input_bytes=1024, max_output_bytes=1024)
    fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        candidate = versions.seal(identity, fd)
    finally:
        os.close(fd)
    candidate = versions.verify(candidate)
    requests = WorkspaceRequests()
    async with sf.begin() as session:
        await requests.create(session, identity, barrier_epoch=1)
        await requests.claim(session, identity, nonce="e" * 64, barrier_epoch=1, deadline=datetime.now(UTC) + timedelta(seconds=30))
        await requests.prepared(session, identity, nonce="e" * 64, barrier_epoch=1, manifest=candidate)
    controller = WorkspaceWriterController()
    controller.execution_deadline = __import__("time").monotonic() + 30
    await controller.close_and_wait(deadline=controller.execution_deadline or __import__("time").monotonic() + 10, final=kind != "partial")
    yield SimpleNamespace(versions=versions, item=item, sf=sf, capability=capability, identity=identity, candidate=candidate, controller=controller, scope=lambda: remote_mutation_scope(capability.context))


async def snapshot(pair):
    async with pair.item.engine.connect() as conn:
        return {
            table: tuple((await conn.execute(text("SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"))).scalars())
            for table in ("runs", "fleet_agent_tasks", "fleet_run_placements", "fleet_workspace_requests", "fleet_workspace_points")
        }


def participant(pair):
    import app.fleet.workspace as workspace

    cls = getattr(workspace, "FleetWorkspaceTerminalParticipant", None)
    assert cls is not None, "original terminal transaction has no exact workspace pair participant"
    result = cls(pair.capability, controller=pair.controller)
    result.bind_prepared(pair.identity, pair.candidate, barrier_epoch=1)
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["update_status", "finalize_if_not_cancelled", "update_run_completion"])
async def test_original_terminal_write_atomically_accepts_pair(prepared_pair, method):
    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    repo = RunRepository(p.sf, mutation_capability=p.capability)
    repo._terminal_participant = participant(p)
    with p.scope():
        await getattr(repo, method)(p.identity.run_id, status="success")
    async with p.sf() as session:
        states = (
            await session.execute(
                text(
                    "SELECT r.status,t.state,p.state,w.manifest_id,w.checkpoint_id "
                    "FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id "
                    "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id JOIN fleet_workspace_points w ON w.id=t.accepted_workspace_point_id"
                )
            )
        ).one()
        assert tuple(states) == ("success", "finishing", "finishing", p.candidate.manifest_id, p.identity.checkpoint_id)


@pytest.mark.asyncio
async def test_pair_insert_failure_rolls_back_original_core_transition(prepared_pair):
    from sqlalchemy.exc import DBAPIError

    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    repo = RunRepository(p.sf, mutation_capability=p.capability)
    repo._terminal_participant = participant(p)
    async with p.item.engine.begin() as conn:
        await conn.execute(text("CREATE FUNCTION reject_task4_pair() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'task4 pair insertion fault'; END $$"))
        await conn.execute(text("CREATE TRIGGER task4_fault BEFORE INSERT ON fleet_workspace_points FOR EACH ROW EXECUTE FUNCTION reject_task4_pair()"))
    before = await snapshot(p)
    with p.scope(), pytest.raises(DBAPIError, match="task4 pair insertion fault"):
        await repo.update_status(p.identity.run_id, "success")
    assert await snapshot(p) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["checkpoint", "metadata", "generation", "owner", "token", "node"])
async def test_pair_exact_identity_rejection_keeps_original_sql(prepared_pair, change):
    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    repo = RunRepository(p.sf, mutation_capability=p.capability)
    repo._terminal_participant = participant(p)
    statements = {
        "checkpoint": "UPDATE checkpoints SET checkpoint_id=checkpoint_id || '-different' WHERE checkpoint_id=(SELECT max(checkpoint_id) FROM checkpoints)",
        "metadata": "UPDATE checkpoints SET metadata=jsonb_set(metadata,'{deerflow_execution_run_id}', '\"other-run\"') WHERE checkpoint_id=(SELECT max(checkpoint_id) FROM checkpoints)",
        "generation": "UPDATE fleet_agent_tasks SET generation=generation+1",
        "owner": "UPDATE runs SET owner_worker_id='other-owner'",
        "token": "UPDATE fleet_attempts SET token_hash=repeat('f',64)",
        "node": "UPDATE fleet_nodes SET session_id='other-session'",
    }
    async with p.item.engine.begin() as conn:
        if change != "token":
            await conn.execute(text(statements[change]))
    if change == "token":
        p.capability._guard._identity = replace(p.item.identity, token_stamp="f" * 64)
    before = await snapshot(p)
    with p.scope(), pytest.raises(RuntimeError):
        await repo.update_status(p.identity.run_id, "success")
    assert await snapshot(p) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_table", ["fleet_agent_tasks", "runs"])
async def test_task_or_run_lock_wait_rechecks_actual_lease(prepared_pair, lock_table):
    import asyncio

    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    repo = RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p))
    async with p.item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '0.25 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs LIMIT 1)"))
    before = await snapshot(p)
    async with p.sf.begin() as blocker:
        await blocker.execute(text("SELECT 1 FROM " + lock_table + " FOR UPDATE"))

        async def write():
            with p.scope():
                await repo.update_status(p.identity.run_id, "success")

        task = asyncio.create_task(write())
        await asyncio.sleep(0.4)
        assert not task.done()
    with pytest.raises(RuntimeError):
        await task
    assert await snapshot(p) == before


@pytest.mark.asyncio
async def test_pair_flush_unique_wait_rechecks_fresh_clock(prepared_pair):
    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    repo = RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p))
    async with p.item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '0.25 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs LIMIT 1)"))
        await conn.execute(text("CREATE FUNCTION delay_task4_pair() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.4); RETURN NEW; END $$"))
        await conn.execute(text("CREATE TRIGGER task4_delay BEFORE INSERT ON fleet_workspace_points FOR EACH ROW EXECUTE FUNCTION delay_task4_pair()"))
    before = await snapshot(p)
    with p.scope(), pytest.raises(RuntimeError):
        await repo.update_status(p.identity.run_id, "success")
    assert await snapshot(p) == before


@pytest.mark.asyncio
async def test_concurrent_duplicate_pair_is_exact_and_conflict_rejected(prepared_pair):
    import asyncio

    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    repo = RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p))

    async def write(status):
        with p.scope():
            return await repo.update_status(p.identity.run_id, status)

    assert await asyncio.gather(write("success"), write("success")) == [True, True]
    before = await snapshot(p)
    with pytest.raises(RuntimeError):
        await write("interrupted")
    assert await snapshot(p) == before
    async with p.sf() as session:
        assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points")) == 1


@pytest.mark.asyncio
async def test_cancel_wins_cas_requires_new_cancelled_preparation(prepared_pair):
    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    repo = RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p))
    # Original gateway cancellation runs outside worker private authority.
    await RunRepository(p.sf).request_cancel(p.identity.run_id, action="interrupt")
    before = await snapshot(p)
    with p.scope():
        assert not (await repo.finalize_if_not_cancelled(p.identity.run_id, status="success")).finalized
        with pytest.raises(RuntimeError):
            await repo.update_status(p.identity.run_id, "interrupted")
    assert await snapshot(p) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["partial"], indirect=True)
async def test_prepared_partial_remains_closed_until_exact_point_commit(prepared_pair):
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool

    p = prepared_pair
    teardown = SimpleNamespace(context=p.capability.context, workspace_writers=p.controller, workspace_sessions=None)
    pool = MCPSessionPool()
    publisher = FleetWorkspacePublisher(p.sf, p.capability, controller=p.controller, teardown=teardown, session_pool=pool)
    pool.freeze_scope(publisher.scope_key, barrier_epoch=1)
    with pytest.raises(RuntimeError, match="closed"):
        p.controller.reserve()
    accept = getattr(publisher, "accept_partial", None)
    assert accept is not None, "prepared candidate has no exact committed partial acceptance/reopen path"
    with p.scope():
        await accept(p.identity, p.candidate, barrier_epoch=1)
    ticket = p.controller.reserve()
    ticket.finish()
    async with p.sf() as session:
        assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE kind='partial'")) == 1
        assert await session.scalar(text("SELECT status FROM runs")) == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["final", "paused"], indirect=True)
async def test_physical_stop_applies_exact_desired_point_and_releases_capacity(prepared_pair):
    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    with p.scope():
        await RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p)).update_status(p.identity.run_id, "success")
    ownership = p.item.env[4].state.fleet_ownership
    auth = dict(node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token)
    async with p.sf() as session:
        assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "reserved"
        assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "finishing"
    result = await ownership.stopped(reason="exit", exit_code=0, process_ref=p.identity.process_ref, physical_stopped=True, **auth)
    assert result["stopped"]
    async with p.sf() as session:
        assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == p.identity.desired_task_status
        assert await session.scalar(text("SELECT state FROM fleet_run_placements")) == p.identity.desired_placement_status
        assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "released"
        assert await session.scalar(text("SELECT core_status FROM fleet_stream_seals")) == "success"


@pytest.mark.asyncio
async def test_finishing_renew_only_exact_cleanup_authority_never_running(prepared_pair):
    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    with p.scope():
        await RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p)).update_status(p.identity.run_id, "success")
    ownership = p.item.env[4].state.fleet_ownership
    auth = dict(node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token)
    renewed = await ownership.renew(running=False, **auth)
    assert not renewed["stop"]
    before = await snapshot(p)
    with pytest.raises(ValueError):
        await ownership.renew(running=True, **auth)
    assert await snapshot(p) == before


@pytest.mark.asyncio
async def test_stage_point_cannot_authorize_terminal_streamseal(prepared_pair):
    from deerflow.runtime.execution.mutation_context import validate_mutation

    p = prepared_pair
    async with p.item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
    before = await snapshot(p)
    with p.scope(), pytest.raises(RuntimeError):
        async with p.sf.begin() as session:
            await validate_mutation(p.capability, session, "stream.seal", run_id=p.identity.run_id, status="success")
    assert await snapshot(p) == before


@pytest.mark.asyncio
async def test_final_pair_sets_both_original_authority_pointers(prepared_pair):
    from deerflow.persistence.run.sql import RunRepository

    p = prepared_pair
    with p.scope():
        await RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p)).update_status(p.identity.run_id, "success")
    async with p.sf() as session:
        row = (await session.execute(text("SELECT t.accepted_workspace_point_id,p.final_workspace_point_id FROM fleet_agent_tasks t JOIN fleet_run_placements p ON p.agent_task_id=t.id"))).one()
        assert tuple(row) == (p.identity.request_id, p.identity.request_id)


@pytest.mark.asyncio
async def test_physical_stopped_unpaired_core_cannot_complete_or_end(prepared_pair):
    p = prepared_pair
    async with p.item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
    await p.item.env[4].state.fleet_ownership.stopped(
        reason="exit", exit_code=0, process_ref=p.identity.process_ref, physical_stopped=True, node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token
    )
    async with p.sf() as session:
        assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "recovery_required"
        assert await session.scalar(text("SELECT state FROM fleet_run_placements")) == "recovery_required"
        assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 0
        assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "released"


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["persist_current_status", "set_status_if_not_cancelled", "update_run_completion"])
async def test_original_manager_terminal_entry_with_local_heartbeat_disabled(prepared_pair, entry):
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.runs.manager import RunManager, RunStatus

    p = prepared_pair
    repo = RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p))
    manager = RunManager(store=repo, worker_id=p.identity.owner_worker_id)
    assert not manager.heartbeat_enabled
    with p.scope():
        record = await manager.attach_existing_executor(p.identity.run_id, user_id=p.identity.user_id, thread_id=p.identity.thread_id, owner_worker_id=p.identity.owner_worker_id, execution_backend="fleet")
        if entry == "persist_current_status":
            record.status = RunStatus.success
            await manager.persist_current_status(p.identity.run_id)
        elif entry == "set_status_if_not_cancelled":
            await manager.set_status_if_not_cancelled(p.identity.run_id, RunStatus.success)
        else:
            await manager.update_run_completion(p.identity.run_id, status="success")
    async with p.sf() as session:
        assert await session.scalar(text("SELECT status FROM runs")) == "success"
        assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points")) == 1
        assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "finishing"
        assert await session.scalar(text("SELECT final_workspace_point_id FROM fleet_run_placements")) == p.identity.request_id
