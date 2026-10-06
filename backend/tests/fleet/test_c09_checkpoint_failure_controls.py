"""Original PG/Runner checkpoint and failure controls; no Docker STOP proof."""

import asyncio
import copy
import json

import httpx
import pytest
from fastapi import FastAPI, Request
from sqlalchemy import text

from .test_c09_stale_prepared_publication import admission as admission
from .test_c09_stale_prepared_publication import checkpoint_owner as checkpoint_owner
from .test_c09_stale_prepared_publication import owner_environment as owner_environment
from .test_c09_stale_prepared_publication import retain_publication_cleanup


async def public_client(item):
    from langgraph.store.memory import InMemoryStore

    from app.gateway.authz import AuthContext
    from app.gateway.routers.thread_runs import router
    from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
    from deerflow.runtime.runs.manager import RunManager

    app = FastAPI()
    app.state.run_store = item.env[4].state.run_store
    app.state.fleet_ownership = item.env[4].state.fleet_ownership
    app.state.thread_store = MemoryThreadMetaStore(InMemoryStore())
    await app.state.thread_store.create(item.spec.thread_id, user_id=item.spec.user_id)
    app.state.run_manager = RunManager(store=app.state.run_store)
    app.include_router(router)

    @app.middleware("http")
    async def owner(request: Request, call_next):
        request.state.user = item.env[2]
        request.state.auth_source = "session"
        request.state.auth = AuthContext(user=item.env[2], permissions=["runs:cancel"])
        return await call_next(request)

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://native", cookies={"csrf_token": "c09"}, headers={"X-CSRF-Token": "c09"})


async def cancel(client, item):
    reply = await client.post(f"/api/threads/{item.spec.thread_id}/runs/{item.spec.run_id}/cancel?wait=false&action=rollback")
    assert reply.status_code == 202, reply.text


async def terminal_rows(item):
    async with item.env[1]() as session:
        return (
            await session.execute(
                text(
                    "SELECT r.status,t.state,p.state,t.accepted_workspace_point_id,p.final_workspace_point_id,res.state FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id "
                    "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id JOIN fleet_reservations res ON res.attempt_id=p.active_attempt_id"
                )
            )
        ).one()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,pending", [("full", False), ("delta", False), ("full", True), ("delta", True)])
async def test_original_rollback_empty_and_pending(checkpoint_owner, tmp_path, monkeypatch, mode, pending):
    from langchain_core.messages import HumanMessage

    import deerflow.runtime.runs.worker as worker
    from deerflow.runtime.checkpoint_state import CheckpointStateAccessor

    from .c09_integration_fixture import original_native_execution

    item = checkpoint_owner
    captured = {}
    original = worker._capture_rollback_point

    async def observe_capture(accessor, writer, config):
        point = await original(accessor, writer, config)
        captured["point"] = point
        captured["values"] = copy.deepcopy((await accessor.aget(config)).values)
        captured["tuple"] = None if point is None else await writer.aget_tuple(point.config)
        return point

    monkeypatch.setattr(worker, "_capture_rollback_point", observe_capture)
    native = await original_native_execution(item, tmp_path, monkeypatch, checkpoint_mode=mode, seed_rollback=pending, seed_pending=pending)
    client = None
    try:
        client = await public_client(item)
        accessor = CheckpointStateAccessor.bind(native.environment.agent_factory(), native.writer, mode=mode)
        with native.scope():
            await accessor.aupdate({"configurable": {"thread_id": item.spec.thread_id}}, {"messages": [HumanMessage(content="Changed during current run", id="c09-current")], "title": "Current title"}, as_node="held_original")
        native.source.joinpath("outputs/current.txt").write_bytes(b"current workspace survives rollback\n")
        await cancel(client, item)
        native.release.set()
        result = await asyncio.wait_for(asyncio.shield(native.execute), 10)
        assert result.status.value == "error" and result.error == "Rolled back by user" and not result.ownership_lost
        row = await terminal_rows(item)
        assert row[:3] == ("error", "finishing", "finishing") and row[3] == row[4] and row[3] is not None and row[5] != "released"
        async with item.env[1]() as session:
            point_id = await session.scalar(text("SELECT checkpoint_id FROM fleet_workspace_points WHERE kind='paused'"))
            assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points")) == 1
        with native.scope():
            restored = await accessor.aget({"configurable": {"thread_id": item.spec.thread_id}})
            root = await native.writer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id, "checkpoint_id": point_id}})
        assert point_id and root is not None
        assert restored.config["configurable"]["checkpoint_id"] == point_id
        assert root.config["configurable"]["checkpoint_id"] == point_id
        assert not restored.next
        assert "c09-current" not in [m.id for m in restored.values.get("messages", [])]
        if pending:
            selected = captured["point"]
            assert selected is not None and selected.pending_writes == tuple(captured["tuple"].pending_writes)
            assert any(task == "c09-pending-task" for task, _, _ in selected.pending_writes)
            assert [m.id for m in restored.values["messages"]] == [m.id for m in selected.messages]
            assert restored.values["title"] == captured["values"]["title"]
            # Read the actual restored accepted root: these are original saver
            # writes, rather than a synthesized checkpoint or fake pair.
            assert all(write in root.pending_writes for write in selected.pending_writes)
            assert bool(selected.state_values) is (mode == "delta")
        else:
            assert captured["point"] is None and not restored.values.get("messages")
            assert restored.values.get("title") != "Current title"
        assert native.source.joinpath("outputs/current.txt").read_bytes() == b"current workspace survives rollback\n"
        print(
            json.dumps(
                {
                    "event": "c09-original-checkpoint-control",
                    "mode": mode,
                    "pending": pending,
                    "captured_root": None if captured["point"] is None else captured["point"].config["configurable"]["checkpoint_id"],
                    "accepted_root": point_id,
                    "restored_root": restored.config["configurable"]["checkpoint_id"],
                    "tuple_root": root.config["configurable"]["checkpoint_id"],
                    "pending_count": len(root.pending_writes),
                    "delta_channels": [] if captured["point"] is None else sorted(captured["point"].state_values),
                    "actual_root_readable": True,
                    "current_files_retained": True,
                }
            )
        )
    finally:
        await retain_publication_cleanup(native, None, None, client, native.release.set, item.publication_cleanup)


@pytest.mark.asyncio
async def test_original_capture_read_failure_fails_closed(checkpoint_owner, tmp_path, monkeypatch):
    import deerflow.runtime.runs.worker as worker
    from deerflow.runtime.execution.mutation_context import ExecutionWorkspaceFailure

    from .c09_integration_fixture import original_native_execution

    item = checkpoint_owner
    corrupted = []
    captured = {}
    original_capture = worker._capture_rollback_point

    async def corrupt_original_blob(writer):
        assert await writer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id}}) is not None
        async with item.engine.begin() as conn:
            rows = (await conn.execute(text("SELECT thread_id,checkpoint_ns,channel,version,type,blob FROM checkpoint_blobs WHERE channel='messages'"))).all()
            assert rows
            corrupted.extend(rows)
            await conn.execute(text("UPDATE checkpoint_blobs SET type='msgpack',blob=:blob WHERE channel='messages'"), {"blob": b"\xc1"})

    async def capture_and_repair(accessor, writer, config):
        await corrupt_original_blob(writer)
        try:
            return await original_capture(accessor, writer, config)
        except Exception as error:
            captured["error"] = error
            raise
        finally:
            # Repair only this test's actual corrupted pre-run bytes, after the
            # original read. The worker retains its real failed-capture flag.
            async with item.engine.begin() as conn:
                for thread, namespace, channel, version, kind, blob in corrupted:
                    await conn.execute(
                        text("UPDATE checkpoint_blobs SET type=:type,blob=:blob WHERE thread_id=:thread AND checkpoint_ns=:ns AND channel=:channel AND version=:version"),
                        {"type": kind, "blob": blob, "thread": thread, "ns": namespace, "channel": channel, "version": version},
                    )

    monkeypatch.setattr(worker, "_capture_rollback_point", capture_and_repair)
    native = await original_native_execution(item, tmp_path, monkeypatch, seed_rollback=True)
    client = None
    primary = None
    try:
        assert isinstance(captured["error"], Exception)
        client = await public_client(item)
        await cancel(client, item)
        native.release.set()
        try:
            await asyncio.wait_for(asyncio.shield(native.execute), 10)
        except ExecutionWorkspaceFailure as error:
            primary = error
        record = await native.manager.get(item.spec.run_id)
        assert record.ownership_lost
        row = await terminal_rows(item)
        assert row[3:5] == (None, None) and row[5] != "released"
        async with item.env[1]() as session:
            assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points")) == 0
        assert record.error != "Rolled back by user"
        print(
            json.dumps(
                {
                    "event": "c09-original-capture-failed-closed",
                    "original_read_error_type": type(captured["error"]).__name__,
                    "runner_error_type": None if primary is None else type(primary).__name__,
                    "accepted_points": 0,
                    "ownership_lost": True,
                }
            )
        )
    finally:
        await retain_publication_cleanup(native, None, None, client, native.release.set, item.publication_cleanup, original_error=primary)


@pytest.mark.asyncio
async def test_original_observer_query_failure_retains_executor(checkpoint_owner, tmp_path, monkeypatch):
    import time

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    import app.fleet.agent_control as control

    from .c09_integration_fixture import original_native_execution

    item = checkpoint_owner
    native = await original_native_execution(item, tmp_path, monkeypatch, seed_rollback=True)
    pool = None
    held = None
    captured = {}
    primary = None
    try:
        pool = create_async_engine(item.engine.url, pool_size=1, max_overflow=0)
        native.teardown.stack.push_async_callback(pool.dispose)
        held = await pool.connect()
        native.teardown.stack.push_async_callback(held.close)
        sf = async_sessionmaker(pool, expire_on_commit=False)
        observer = native.teardown._phases["control-observer"]
        executor = native.teardown._phases["original-executor"]
        original = control.observe_original_control

        async def observe_on_owned_exhausted_pool(session_factory, capability):
            if asyncio.current_task() is not observer or capability.context != native.publisher.capability.context:
                return await original(session_factory, capability)
            started = time.monotonic()
            try:
                return await original(sf, capability)
            except TimeoutError as error:
                captured.update(error=error, elapsed=time.monotonic() - started)
                raise
            finally:
                await held.close()

        monkeypatch.setattr(control, "observe_original_control", observe_on_owned_exhausted_pool)
        native.release.set()
        try:
            await asyncio.wait_for(asyncio.shield(native.execute), 10)
        except BaseException as error:
            if not native.execute.done():
                raise
            primary = error
        assert observer.done() and executor.done()
        assert observer.exception() is captured["error"] and type(captured["error"]) is TimeoutError
        assert 0.8 <= captured["elapsed"] < 3
        record = await native.manager.get(item.spec.run_id)
        assert record.ownership_lost
        assert (await terminal_rows(item))[3:5] == (None, None)
        print(
            json.dumps(
                {
                    "event": "c09-original-observer-query-failure",
                    "original_error_type": type(captured["error"]).__name__,
                    "elapsed": captured["elapsed"],
                    "same_observer_done": observer.done(),
                    "same_executor_done": executor.done(),
                    "accepted_pair": False,
                }
            )
        )
    finally:
        await retain_publication_cleanup(native, None, None, None, native.release.set, item.publication_cleanup, original_error=primary)
        assert pool is not None and held is not None and held.closed


@pytest.mark.asyncio
async def test_original_terminal_commit_failure_rolls_back_all_participants(checkpoint_owner, tmp_path, monkeypatch):
    from deerflow.runtime.execution.mutation_context import ExecutionWorkspaceFailure

    from .c09_integration_fixture import original_native_execution

    item = checkpoint_owner
    native = await original_native_execution(item, tmp_path, monkeypatch, seed_rollback=True)
    client = None
    primary = None
    attempted = []
    try:
        # Owned UUID database only: the deferred constraint fires at actual
        # commit, after original core/point/task/placement/request flushes.
        async with item.engine.begin() as conn:
            await conn.execute(
                text("CREATE FUNCTION c09_terminal_commit_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='paused' THEN RAISE EXCEPTION 'c09 owned deferred terminal commit fault'; END IF; RETURN NEW; END $$")
            )
            await conn.execute(text("CREATE CONSTRAINT TRIGGER c09_terminal_commit_fault AFTER INSERT ON fleet_workspace_points DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION c09_terminal_commit_fault()"))
        original = native.publisher.terminal.after_transition

        async def observed_transition(session, **outcome):
            result = await original(session, **outcome)
            row = (
                await session.execute(
                    text(
                        "SELECT r.status,t.state,p.state,t.accepted_workspace_point_id,p.final_workspace_point_id,w.kind,q.state FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id "
                        "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id JOIN fleet_workspace_points w ON w.id=p.final_workspace_point_id "
                        "JOIN fleet_workspace_requests q ON q.id=w.request_id"
                    )
                )
            ).one()
            attempted.append(tuple(row))
            return result

        monkeypatch.setattr(native.publisher.terminal, "after_transition", observed_transition)
        client = await public_client(item)
        before = await terminal_rows(item)
        await cancel(client, item)
        native.release.set()
        try:
            await asyncio.wait_for(asyncio.shield(native.execute), 10)
        except ExecutionWorkspaceFailure as error:
            primary = error
        assert attempted and attempted[0][:3] == ("error", "finishing", "finishing")
        assert attempted[0][3] == attempted[0][4] and attempted[0][5:] == ("paused", "accepted")
        after = await terminal_rows(item)
        assert after == before
        async with item.env[1]() as session:
            assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points")) == 0
            assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_requests WHERE state='accepted'")) == 0
        assert (await native.manager.get(item.spec.run_id)).ownership_lost
        print(
            json.dumps(
                {
                    "event": "c09-original-terminal-commit-rollback",
                    "transaction_attempted": list(attempted[0]),
                    "durable_after": list(after),
                    "runner_error_type": None if primary is None else type(primary).__name__,
                    "half_pair": False,
                    "resources_released": False,
                }
            )
        )
    finally:
        await retain_publication_cleanup(native, None, None, client, native.release.set, item.publication_cleanup, original_error=primary)
