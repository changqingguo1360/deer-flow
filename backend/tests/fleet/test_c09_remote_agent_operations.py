"""C09 minimal real admission and original HTTP cancellation boundary."""

import asyncio

import httpx
import pytest
from fastapi import FastAPI, Request
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


@pytest.mark.asyncio
async def test_c09_interrupt_actual_main(checkpoint_owner, tmp_path, monkeypatch):
    from app.gateway.routers.thread_runs import router
    from deerflow.runtime.runs.manager import RunManager

    item = checkpoint_owner
    from langgraph.store.memory import InMemoryStore

    from app.gateway.authz import AuthContext
    from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore

    app = FastAPI()
    app.state.run_store = item.env[4].state.run_store
    app.state.fleet_ownership = item.env[4].state.fleet_ownership
    app.state.thread_store = MemoryThreadMetaStore(InMemoryStore())
    await app.state.thread_store.create(item.spec.thread_id, user_id=item.spec.user_id)
    manager = RunManager(store=app.state.run_store)
    app.state.run_manager = manager
    app.include_router(router)

    @app.middleware("http")
    async def original_owner(request: Request, call_next):
        request.state.user = item.env[2]
        request.state.auth_source = "session"
        request.state.auth = AuthContext(user=item.env[2], permissions=["runs:cancel"])
        return await call_next(request)

    auth = dict(node_id=item.identity.node_id, node_session_id=item.identity.node_session_id, attempt_id=item.accepted.attempt_id, token=item.accepted.token)
    from .c09_integration_fixture import original_native_execution

    native = await original_native_execution(item, tmp_path, monkeypatch)
    record = await native.manager.get(item.spec.run_id)
    assert record.task is not None and not record.task.done()
    pending_wait = None
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"csrf_token": "c09-csrf"}, headers={"X-CSRF-Token": "c09-csrf"}) as client:
            result = await client.post(f"/api/threads/{item.spec.thread_id}/runs/{item.spec.run_id}/cancel?wait=false&action=interrupt")
            assert result.status_code == 202, result.text
            winning = await manager._store.get(item.spec.run_id)
            repeated = await client.post(f"/api/threads/{item.spec.thread_id}/runs/{item.spec.run_id}/cancel?wait=false&action=rollback")
            assert repeated.status_code == 202, repeated.text
            assert (await manager._store.get(item.spec.run_id))["cancel_requested_at"] == winning["cancel_requested_at"]
        async with item.env[1]() as session:
            row = (
                await session.execute(
                    text(
                        "SELECT r.status,r.cancel_action,r.cancel_requested_at,t.cancel_requested_at,t.generation,p.active_attempt_id,res.state,a.stopped_at FROM runs r JOIN "
                        "fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id JOIN fleet_attempts a ON a.id=p.active_attempt_id JOIN "
                        "fleet_reservations res ON res.attempt_id=a.id"
                    )
                )
            ).one()
            assert row[0:2] == ("running", "interrupt")
            assert row[2] is not None and row[3] is None
            assert row[4] == item.spec.generation and row[5] == item.accepted.attempt_id
            assert row[6] in {"reserved", "active"} and row[7] is None
        renewed = await app.state.fleet_ownership.renew(running=False, **auth)
        assert renewed["stop"] is False and renewed["control"] == "cancel"

        # Intent revokes ordinary execution writes without losing the original identity.
        with pytest.raises(asyncio.CancelledError):
            await item.writer.aput_writes(item.config, [("messages", "after intent")], "late-tool")
        native.release.set()
        completed = await asyncio.wait_for(native.execute, 10)
        assert completed.run_id == item.spec.run_id and completed.status.value == "interrupted"
        assert not completed.ownership_lost and completed.task.done()
        async with item.env[1]() as session:
            pair = (
                await session.execute(
                    text(
                        "SELECT w.kind,w.desired_core_status,w.desired_task_status,w.checkpoint_id,t.accepted_workspace_point_id,p.final_workspace_point_id,a.stopped_at,res.state FROM "
                        "fleet_workspace_points w JOIN fleet_agent_tasks t ON t.id=w.agent_task_id JOIN fleet_run_placements p ON p.run_id=w.run_id JOIN fleet_attempts a ON "
                        "a.id=w.attempt_id JOIN fleet_reservations res ON res.attempt_id=a.id"
                    )
                )
            ).one()
            assert pair[0:3] == ("paused", "interrupted", "paused")
            assert pair[4] == pair[5] and pair[6] is None and pair[7] != "released"
            assert await session.scalar(text("SELECT count(*) FROM runs")) == 1
        with pytest.raises(RuntimeError, match="closed"):
            native.controller.reserve()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            pending_wait = asyncio.create_task(client.post(f"/api/threads/{item.spec.thread_id}/runs/{item.spec.run_id}/cancel?wait=true&action=interrupt"))
            await asyncio.sleep(0.1)
            assert not pending_wait.done(), "core terminal/END does not prove physical stop"
            native.stop_node.set()
            await asyncio.wait_for(native.node, 3)
            # Native driver of the original trusted STOP API; installed Docker
            # process census and NodeDaemon proof remain the runtime gate.
            await app.state.fleet_ownership.stopped(reason="exit", exit_code=0, process_ref=item.grant["process_ref"], physical_stopped=True, **auth)
            assert (await asyncio.wait_for(pending_wait, 3)).status_code == 204
        async with item.env[1]() as session:
            assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "paused"
            assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "released"
    finally:
        native.release.set()
        native.stop_node.set()
        if pending_wait is not None and not pending_wait.done():
            pending_wait.cancel()
            await asyncio.gather(pending_wait, return_exceptions=True)
        if not native.execute.done():
            native.execute.cancel()
        await asyncio.gather(native.execute, return_exceptions=True)
        native.node.cancel()
        await asyncio.gather(native.node, return_exceptions=True)
        if native.node.done() and not native.node.cancelled():
            native.node.result()
        with native.scope():
            await native.environment.close()
        assert not native.teardown._phases and native.teardown.closed
