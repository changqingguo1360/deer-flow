"""Shared host admission remains exclusive after core terminal/expired leases."""

import pytest
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import owner_environment as owner_environment


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["recovery_required", "finishing", "queued"])
@pytest.mark.parametrize("operation", ["run", "artifact_write", "checkpoint_write", "branch"])
async def test_actual_host_manager_blocks_recovery_and_finishing_despite_terminal_core(owner_environment, operation, state):
    from deerflow.runtime import ConflictError, RunManager, ThreadOperationKind

    engine, sf, user, runtime, app, record, *_ = owner_environment
    manager = RunManager(store=app.state.run_store)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success',lease_expires_at=clock_timestamp()-interval '1 hour'"))
        await conn.execute(text("UPDATE fleet_agent_tasks SET state=:state"), {"state": state})
    with pytest.raises(ConflictError):
        if operation == "run":
            await manager.create_or_reject(record.thread_id, user_id=user.id)
        else:
            async with manager.reserve_thread_operation(record.thread_id, kind=ThreadOperationKind(operation), user_id=user.id):
                pytest.fail("Recovery admitted a Local mutation")
    assert (await manager.get(record.run_id, user_id=user.id)).status.value == "success"


@pytest.mark.asyncio
async def test_actual_http_recovery_blocks_regenerate_and_state_before_graph_access(owner_environment):

    import httpx
    from fastapi import FastAPI

    from app.gateway.routers import thread_runs, threads
    from deerflow.runtime import RunManager

    from .test_c02_remote_agent_admission import request

    engine, sf, user, runtime, original, record, *_ = owner_environment
    manager = RunManager(store=original.state.run_store)
    req = request(manager, user)
    app = FastAPI()
    for key, value in vars(req.app.state).items():
        setattr(app.state, key, value)
    app.state.run_store = original.state.run_store
    await app.state.thread_store.create(record.thread_id, user_id=user.id)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success',lease_expires_at=clock_timestamp()-interval '1 hour'"))
        await conn.execute(text("UPDATE fleet_agent_tasks SET state='recovery_required'"))

    @app.middleware("http")
    async def authenticated_fixture(request, call_next):
        request.state.user = user
        request.state.auth_source = "session"
        return await call_next(request)

    app.include_router(thread_runs.router)
    app.include_router(threads.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/api/threads/{record.thread_id}/runs/regenerate/prepare", json={"message_id": "previous-answer"})
        assert response.status_code == 409 and "remote" in response.text.lower(), response.text
        response = await client.post(f"/api/threads/{record.thread_id}/state", json={"values": {"messages": []}})
        assert response.status_code == 409 and "remote" in response.text.lower(), response.text
        assert (await client.get(f"/api/threads/{record.thread_id}")).status_code == 200


@pytest.mark.asyncio
async def test_plugin_absent_durable_marker_and_legacy_parent_fail_closed(owner_environment):
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.persistence.thread_meta.model import ThreadMetaRow
    from deerflow.runtime import ConflictError, RunManager

    engine, sf, user, runtime, original, record, *_ = owner_environment
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
        await conn.run_sync(lambda sync: ThreadMetaRow.__table__.create(sync))
    async with sf.begin() as session:
        session.add(ThreadMetaRow(thread_id="legacy-child", user_id=user.id, metadata_json={"deerflow_branch": True, "branch_parent_thread_id": record.thread_id}))
    absent = RunManager(store=RunRepository(sf))
    with pytest.raises(ConflictError):
        await absent.create_or_reject(record.thread_id, user_id=user.id)
    with pytest.raises(ConflictError):
        await absent.create_or_reject("legacy-child", user_id=user.id)


def test_client_cannot_set_or_clear_server_branch_routing_metadata():
    from app.gateway.routers.threads import ThreadCreateRequest, ThreadPatchRequest

    forged = dict(deerflow_branch=True, branch_parent_thread_id="fleet-parent", branch_parent_checkpoint_id="root", execution_backend="local")
    for cls in (ThreadCreateRequest, ThreadPatchRequest):
        assert not {"deerflow_branch", "branch_parent_thread_id", "branch_parent_checkpoint_id", "execution_backend"}.intersection(cls(metadata=forged).metadata)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["queued", "running", "finishing", "recovery_required"])
async def test_same_manager_cached_remote_pending_obeys_durable_task_after_core_terminal(owner_environment, state):
    from fastapi import HTTPException

    from app.gateway import services
    from deerflow.runtime import RunManager

    from .test_c02_remote_agent_admission import backend, body, request

    engine, sf, user, runtime, app, record, *_ = owner_environment
    manager = RunManager(store=app.state.run_store)
    cached = await manager.get(record.run_id, user_id=user.id)
    assert cached.store_only and cached.status.value == "pending"
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success',lease_expires_at=clock_timestamp()-interval '1 hour'"))
        await conn.execute(text("UPDATE fleet_agent_tasks SET state=:state"), {"state": state})
    with pytest.raises(HTTPException) as conflict:
        await services.start_run(body("a distinct user turn"), record.thread_id, request(manager, user), execution_backend=backend())
    assert conflict.value.status_code == 409
    async with sf() as session:
        assert await session.scalar(text("SELECT count(*) FROM runs")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_only", [False, True])
async def test_actual_local_reservation_cancel_retains_original_sql_release(admission, monkeypatch, cleanup_only):
    import asyncio

    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime import RunManager
    from deerflow.runtime.runs.schemas import ThreadOperationKind

    engine, sf, _, user = admission
    store = RunRepository(sf)
    manager = RunManager(store=store)
    entered, releasing, allow_release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_delete = store.delete_thread_operation
    owners = []

    async def held_delete(run_id, *, user_id):
        owners.append((run_id, user_id))
        releasing.set()
        await allow_release.wait()
        await original_delete(run_id, user_id=user_id)

    monkeypatch.setattr(store, "delete_thread_operation", held_delete)

    async def operation():
        async with manager.reserve_thread_operation("local-neighbor-reservation", kind=ThreadOperationKind.branch, user_id=user.id):
            entered.set()
            if not cleanup_only:
                await asyncio.Event().wait()

    pending = asyncio.create_task(operation())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if not cleanup_only:
            pending.cancel()
        await asyncio.wait_for(releasing.wait(), 5)
        pending.cancel()
        if cleanup_only:
            delivered = asyncio.Event()
            asyncio.get_running_loop().call_soon(delivered.set)
            await delivered.wait()
            pending.cancel()
        turn = asyncio.Event()
        asyncio.get_running_loop().call_soon(turn.set)
        await turn.wait()
        assert not pending.done(), "Local cancellation returned before original SQL release"
        allow_release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert len(owners) == 1 and owners[0][1] == user.id
        async with sf() as session:
            assert await session.scalar(text("SELECT count(*) FROM runs WHERE thread_id='local-neighbor-reservation'")) == 0
        assert not await manager.has_inflight("local-neighbor-reservation")
    finally:
        allow_release.set()
        await asyncio.gather(pending, return_exceptions=True)
