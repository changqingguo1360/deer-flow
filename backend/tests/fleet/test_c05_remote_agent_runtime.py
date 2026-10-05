"""Real PostgreSQL checkpoint mutation ownership, transaction and runner tests."""

import asyncio
import hashlib
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import claim
from .test_c03_remote_agent_admission import owner_environment as owner_environment


async def checkpoint_rows(engine):
    async with engine.connect() as connection:
        return {table: tuple((await connection.execute(text("SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"))).scalars()) for table in ("checkpoints", "checkpoint_blobs", "checkpoint_writes")}


@pytest_asyncio.fixture
async def checkpoint_owner(owner_environment):
    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity
    from langgraph.checkpoint.base import empty_checkpoint

    from deerflow.config.app_config import AppConfig
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer

    env = owner_environment
    engine, _, user, _, app, record, session_id, _, _ = env
    accepted = await claim(env)
    original_grant = await app.state.fleet_ownership.authorize_start(node_id="node-c03", node_session_id=session_id, attempt_id=accepted.attempt_id, token=accepted.token)
    async with engine.connect() as connection:
        schema = (await connection.execute(text("SELECT current_schema()"))).scalar_one()
    private = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "database": {"backend": "postgres", "postgres_url": engine.url.render_as_string(hide_password=False), "postgres_schema": schema}})
    identity = ExecutionIdentity(
        node_id="node-c03",
        node_session_id=session_id,
        agent_task_id=accepted.agent_task_id,
        generation=accepted.launch_spec["generation"],
        attempt_id=accepted.attempt_id,
        owner_worker_id=accepted.owner_worker_id,
        token_stamp=hashlib.sha256(accepted.token.encode()).hexdigest(),
    )
    async with engine.connect() as connection:
        spec = LaunchSpec.model_validate((await connection.execute(text("SELECT payload FROM fleet_launch_specs"))).scalar_one())
    async with make_checkpointer(private) as writer:
        config = {"configurable": {"thread_id": record.thread_id, "checkpoint_ns": ""}}
        checkpoint = empty_checkpoint()
        checkpoint["channel_values"] = {"messages": [{"content": "durable baseline"}]}
        checkpoint["channel_versions"] = {"messages": "1"}
        config = await writer.aput(config, checkpoint, {"source": "input", "step": 0, "parents": {}}, {"messages": "1"})
        await writer.aput_writes(config, [("messages", {"content": "pending baseline"})], "baseline-task")
    from app.fleet.runner_context import FleetCheckpointFence

    async with make_checkpointer(private, write_fence=FleetCheckpointFence(identity, spec)) as writer:
        yield SimpleNamespace(engine=engine, private=private, identity=identity, spec=spec, writer=writer, config=config, checkpoint=checkpoint, env=env, grant=original_grant, accepted=accepted)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["aput", "aput_writes", "adelete_thread"])
async def test_remote_checkpoint_mutators_reject_original_token_replacement(checkpoint_owner, method):
    item = checkpoint_owner
    async with item.engine.begin() as connection:
        await connection.execute(text("UPDATE fleet_attempts SET token_hash=:hash"), {"hash": "a" * 64})
    before = await checkpoint_rows(item.engine)
    with pytest.raises(RuntimeError, match="checkpoint|ownership|fence"):
        if method == "aput":
            changed = item.checkpoint | {"channel_values": {"messages": [{"content": "unauthorized blob"}]}}
            await item.writer.aput(item.config, changed, {"source": "update", "step": 1, "parents": {}}, {"messages": "2"})
        elif method == "aput_writes":
            await item.writer.aput_writes(item.config, [("messages", {"content": "unauthorized pending"})], "stale-task")
        else:
            await item.writer.adelete_thread(item.config["configurable"]["thread_id"])
    assert await checkpoint_rows(item.engine) == before


async def mutate(item, method, config=None):
    config = config or item.config
    if method == "aput":
        return await item.writer.aput(config, item.checkpoint | {"channel_values": {"messages": [{"content": "next blob"}]}}, {"source": "update", "step": 1, "parents": {}}, {"messages": "2"})
    if method == "aput_writes":
        return await item.writer.aput_writes(config, [("messages", {"content": "next pending"}), ("__error__", "special pending")], "next-task")
    return await item.writer.adelete_thread(config["configurable"]["thread_id"])


CHANGES = {
    "generation": "UPDATE fleet_agent_tasks SET generation=generation+1",
    "owner": "UPDATE runs SET owner_worker_id='another-owner'",
    "session": "UPDATE fleet_nodes SET session_id='replacement-session'",
    "attempt_session": "UPDATE fleet_attempts SET node_session_id='replacement-session'",
    "placement_node": "UPDATE fleet_run_placements SET node_id=NULL",
    "active_attempt": "UPDATE fleet_run_placements SET active_attempt_id=NULL",
    "released": "UPDATE fleet_reservations SET state='released',released_at=clock_timestamp()",
    "stopped": "UPDATE fleet_attempts SET stopped_at=clock_timestamp()",
    "expired": "UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'",
    "terminal": "UPDATE runs SET status='success'",
    "task_deadline": "UPDATE fleet_agent_tasks SET deadline=clock_timestamp()-interval '1 second'",
    "execution_deadline": "UPDATE fleet_attempts SET execution_deadline=clock_timestamp()-interval '1 second'",
    "backend": "UPDATE runs SET kwargs_json=jsonb_set(kwargs_json::jsonb,'{execution_backend}','\"local\"')",
    "quarantined": "UPDATE fleet_reservations SET state='quarantined'",
}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["aput", "aput_writes", "adelete_thread"])
@pytest.mark.parametrize("change", list(CHANGES) + ["cross_thread"])
async def test_all_mutations_reject_stale_identity_without_table_changes(checkpoint_owner, method, change):
    item = checkpoint_owner
    if change != "cross_thread":
        async with item.engine.begin() as conn:
            await conn.execute(text(CHANGES[change]))
    before = await checkpoint_rows(item.engine)
    config = item.config | {"configurable": item.config["configurable"] | {"thread_id": "different-thread"}} if change == "cross_thread" else None
    with pytest.raises(RuntimeError, match="Checkpoint ownership"):
        await mutate(item, method, config)
    assert await checkpoint_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["put", "put_writes", "delete_thread"])
@pytest.mark.parametrize("stale", [None, "token", "cross_thread", *CHANGES])
async def test_sync_aliases_from_actual_external_thread_are_fenced(checkpoint_owner, method, stale):
    item = checkpoint_owner
    if stale and stale != "cross_thread":
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'" if stale == "token" else CHANGES[stale]))
    before = await checkpoint_rows(item.engine)
    config = item.config | {"configurable": item.config["configurable"] | {"thread_id": "other-thread"}} if stale == "cross_thread" else item.config

    def operation():
        if method == "put":
            return item.writer.put(config, item.checkpoint, {"source": "update", "step": 1, "parents": {}}, {"messages": "2"})
        if method == "put_writes":
            return item.writer.put_writes(config, [("messages", "sync pending")], "sync-task")
        return item.writer.delete_thread(config["configurable"]["thread_id"])

    if stale:
        with pytest.raises(RuntimeError, match="Checkpoint ownership"):
            await asyncio.to_thread(operation)
        assert await checkpoint_rows(item.engine) == before
    else:
        await asyncio.to_thread(operation)
        assert await checkpoint_rows(item.engine) != before


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["aput", "aput_writes", "adelete_thread"])
async def test_valid_subgraph_mutations_preserve_stock_behavior(checkpoint_owner, method):
    item = checkpoint_owner
    config = item.config | {"configurable": item.config["configurable"] | {"checkpoint_ns": "child:task"}}
    await mutate(item, method, config)
    if method == "adelete_thread":
        assert all(not rows for rows in (await checkpoint_rows(item.engine)).values())
    else:
        assert any("child:task" in row for rows in (await checkpoint_rows(item.engine)).values() for row in rows)


async def wait_blocked(engine, pid):
    async with asyncio.timeout(5):
        while True:
            async with engine.connect() as conn:
                if (await conn.execute(text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": pid})).scalar_one():
                    return
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["expire", "replace"])
async def test_waiting_guard_checks_fresh_clock_and_identity_after_row_lock(checkpoint_owner, change):
    item = checkpoint_owner
    fence = item.writer._write_fence
    pid_future = asyncio.get_running_loop().create_future()
    original = fence.validate

    async def observed(cur, **kwargs):
        await cur.execute("SELECT pg_backend_pid() AS pid")
        pid_future.set_result((await cur.fetchone())["pid"])
        await original(cur, **kwargs)

    fence.validate = observed
    before = await checkpoint_rows(item.engine)
    async with item.engine.connect() as blocker:
        transaction = await blocker.begin()
        await blocker.execute(text("SELECT id FROM fleet_agent_tasks FOR UPDATE"))
        writing = asyncio.create_task(mutate(item, "aput"))
        pid = await asyncio.wait_for(pid_future, 5)
        await wait_blocked(item.engine, pid)
        if change == "expire":
            # Preserve both rows' equality, expire while the already-open writer TX waits.
            await blocker.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
            await blocker.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
        else:
            await blocker.execute(text("UPDATE fleet_nodes SET session_id='replacement-during-wait'"))
        await transaction.commit()
        with pytest.raises(RuntimeError, match="Checkpoint ownership"):
            await writing
    assert await checkpoint_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", [False, True])
async def test_guard_locks_hold_same_backend_and_transaction_until_writer_finishes(checkpoint_owner, rollback):
    item = checkpoint_owner
    validated, release = asyncio.Event(), asyncio.Event()
    observed = []
    original = item.writer._write_fence.validate

    async def barrier(cur, **kwargs):
        await original(cur, **kwargs)
        await cur.execute("SELECT pg_backend_pid() AS pid, txid_current() AS txid")
        observed.append(await cur.fetchone())
        validated.set()
        await release.wait()
        await cur.execute("SELECT pg_backend_pid() AS pid, txid_current() AS txid")
        observed.append(await cur.fetchone())
        if rollback:
            raise RuntimeError("injected rollback")

    item.writer._write_fence.validate = barrier
    from contextlib import asynccontextmanager

    stock_cursor = item.writer._cursor

    class ObservedCursor:
        def __init__(self, cur):
            self.cur = cur

        async def capture(self):
            await self.cur.execute("SELECT pg_backend_pid() AS pid, txid_current() AS txid")
            observed.append(await self.cur.fetchone())

        async def execute(self, *args, **kwargs):
            await self.capture()
            return await self.cur.execute(*args, **kwargs)

        async def executemany(self, *args, **kwargs):
            await self.capture()
            return await self.cur.executemany(*args, **kwargs)

    @asynccontextmanager
    async def recording(**kwargs):
        async with stock_cursor(**kwargs) as cur:
            yield ObservedCursor(cur)

    item.writer._cursor = recording
    before = await checkpoint_rows(item.engine)
    writing = asyncio.create_task(mutate(item, "aput"))
    await asyncio.wait_for(validated.wait(), 5)
    async with item.engine.connect() as takeover:
        transaction = await takeover.begin()
        pid = (await takeover.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        changing = asyncio.create_task(takeover.execute(text("UPDATE fleet_agent_tasks SET state='paused'")))
        await wait_blocked(item.engine, pid)
        assert not changing.done()
        release.set()
        if rollback:
            with pytest.raises(RuntimeError, match="injected rollback"):
                await writing
        else:
            await writing
        await changing
        await transaction.commit()
    assert all(row == observed[0] for row in observed)
    assert len(observed) == (2 if rollback else 4)
    assert (await checkpoint_rows(item.engine) == before) is rollback


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["aput", "adelete_thread"])
@pytest.mark.parametrize("exception", [RuntimeError, asyncio.CancelledError])
async def test_exception_after_first_stock_sql_rolls_back_all_tables(checkpoint_owner, method, exception):
    from contextlib import asynccontextmanager

    item = checkpoint_owner
    original = item.writer._cursor
    before = await checkpoint_rows(item.engine)

    class InterruptedCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        async def execute(self, *args, **kwargs):
            await self.cursor.execute(*args, **kwargs)
            raise exception("injected after stock SQL")

        async def executemany(self, *args, **kwargs):
            await self.cursor.executemany(*args, **kwargs)
            raise exception("injected after stock SQL")

    @asynccontextmanager
    async def interrupted(**kwargs):
        async with original(**kwargs) as cur:
            yield InterruptedCursor(cur)

    item.writer._cursor = interrupted
    with pytest.raises(exception):
        await mutate(item, method)
    item.writer._cursor = original
    assert await checkpoint_rows(item.engine) == before
    assert item.writer._mutation.get() is None
    # Another real writer can obtain the locks immediately after rollback.
    await mutate(item, "aput_writes")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["uninitialized", "missing", "stale", "future", "hole", "column", "type", "primary", "index", "fallback"])
async def test_remote_readiness_is_exact_and_never_performs_ddl(checkpoint_owner, change):
    from app.fleet.runner_context import FleetCheckpointFence
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer

    item = checkpoint_owner
    private = item.private
    async with item.engine.begin() as conn:
        if change == "uninitialized":
            for table in ("checkpoints", "checkpoint_blobs", "checkpoint_writes", "checkpoint_migrations"):
                await conn.execute(text("DROP TABLE " + table))
        elif change == "missing":
            await conn.execute(text("DROP TABLE checkpoint_writes"))
        elif change == "stale":
            await conn.execute(text("DELETE FROM checkpoint_migrations WHERE v=9"))
        elif change == "hole":
            await conn.execute(text("DELETE FROM checkpoint_migrations WHERE v=2"))
        elif change == "future":
            await conn.execute(text("INSERT INTO checkpoint_migrations VALUES(10)"))
        elif change == "column":
            await conn.execute(text("ALTER TABLE checkpoint_writes DROP COLUMN task_path"))
        elif change == "type":
            await conn.execute(text("ALTER TABLE checkpoints ALTER COLUMN metadata TYPE json USING metadata::json"))
        elif change == "primary":
            await conn.execute(text("ALTER TABLE checkpoints DROP CONSTRAINT checkpoints_pkey"))
        elif change == "index":
            await conn.execute(text("DROP INDEX checkpoint_writes_thread_id_idx"))
        else:
            private = private.model_copy(update={"database": private.database.model_copy(update={"postgres_schema": "missing_c05_schema"})})

    async def catalog():
        async with item.engine.connect() as conn:
            return tuple(
                (
                    await conn.execute(
                        text(
                            "SELECT n.nspname,c.relname,c.relkind,a.attname,a.atttypid FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid LEFT JOIN pg_attribute a ON a.attrelid=c.oid WHERE n.nspname NOT LIKE 'pg_%' ORDER BY 1,2,4"
                        )
                    )
                ).all()
            )

    before = await catalog()
    with pytest.raises(RuntimeError, match="Checkpoint .*not ready"):
        async with make_checkpointer(private, write_fence=FleetCheckpointFence(item.identity, item.spec)):
            pytest.fail("unready remote saver accepted")
    assert await catalog() == before


@pytest.mark.asyncio
async def test_independent_guard_negative_control_does_not_hold_writer_locks(checkpoint_owner):
    from app.fleet.runner_context import FleetCheckpointFence
    from deerflow.runtime.checkpointer.async_provider import _build_postgres_pool

    item = checkpoint_owner
    original = FleetCheckpointFence(item.identity, item.spec)
    checked, release = asyncio.Event(), asyncio.Event()
    before = await checkpoint_rows(item.engine)
    # Deliberately wrong design: authorization commits on a separate connection.
    # This proves our takeover-blocking assertion distinguishes the defect.
    pool = _build_postgres_pool(item.private.database.postgres_url, item.private.database.postgres_schema)
    async with pool:

        class IndependentGuard:
            async def validate(self, ignored_cursor, **kwargs):
                async with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
                    await original.validate(cur, **kwargs)
                checked.set()
                await release.wait()

        item.writer._write_fence = IndependentGuard()
        writing = asyncio.create_task(mutate(item, "aput_writes"))
        await asyncio.wait_for(checked.wait(), 5)
        async with item.engine.begin() as conn:
            await asyncio.wait_for(conn.execute(text("UPDATE fleet_agent_tasks SET state='paused'")), 2)
        release.set()
        await writing
        assert (await checkpoint_rows(item.engine))["checkpoint_writes"] != before["checkpoint_writes"]


@pytest.mark.asyncio
@pytest.mark.parametrize("unsupported", ["acopy_thread", "aprune", "adelete_for_runs"])
async def test_unimplemented_postgres_mutations_remain_unsupported(checkpoint_owner, unsupported):
    item = checkpoint_owner
    with pytest.raises(NotImplementedError):
        if unsupported == "acopy_thread":
            await item.writer.acopy_thread(item.spec.thread_id, "other")
        elif unsupported == "aprune":
            await item.writer.aprune([item.spec.thread_id])
        else:
            await item.writer.adelete_for_runs([item.spec.run_id])


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "delta"])
async def test_actual_state_accessor_materializes_fenced_full_and_cached_delta(checkpoint_owner, monkeypatch, mode):
    from langchain_core.messages import HumanMessage

    import deerflow.runtime.checkpoint_mode as checkpoint_mode
    from app.fleet.runner_context import FleetCheckpointFence
    from deerflow.runtime import CheckpointStateAccessor
    from deerflow.runtime.checkpoint_state import build_state_mutation_graph
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.checkpointer.cached_saver import CachedHistorySaver

    item = checkpoint_owner
    # A fresh same-owner thread head for the mode-specific graph.
    await item.writer.adelete_thread(item.spec.thread_id)
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_channel_mode", None)
    private = item.private.model_copy(update={"database": item.private.database.model_copy(update={"checkpoint_channel_mode": mode})})
    async with make_checkpointer(private, write_fence=FleetCheckpointFence(item.identity, item.spec)) as saver:
        assert isinstance(saver, CachedHistorySaver) is (mode == "delta")
        graph = build_state_mutation_graph("writer", mode)
        accessor = CheckpointStateAccessor.bind(graph, saver, mode=mode)
        config = {"configurable": {"thread_id": item.spec.thread_id}}
        await accessor.aupdate(config, {"messages": [HumanMessage(content="first message", id="m1")]}, as_node="writer")
        await accessor.aupdate(config, {"messages": [HumanMessage(content="second message", id="m2")]}, as_node="writer")
        assert [m.content for m in (await accessor.aget(config)).values["messages"]] == ["first message", "second message"]
        assert len(await accessor.ahistory(config)) == 2
        before = await checkpoint_rows(item.engine)
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE runs SET status='success'"))
        with pytest.raises(RuntimeError, match="Checkpoint ownership"):
            await accessor.aupdate(config, {"messages": [HumanMessage(content="rejected")]}, as_node="writer")
        assert await checkpoint_rows(item.engine) == before


def test_unreviewed_saver_version_is_rejected_only_for_fenced_adapter(monkeypatch):
    pytest.importorskip("langgraph.checkpoint.postgres.aio", reason="requires optional PostgreSQL saver extra")
    import deerflow.runtime.checkpointer.fenced_saver as module

    monkeypatch.setattr(module, "version", lambda _: "99.0.0")
    with pytest.raises(RuntimeError, match="Unsupported fenced"):
        module.FencedAsyncPostgresSaver(None, write_fence=None)


@pytest.mark.asyncio
async def test_pure_special_pending_writes_use_upsert_and_remain_fenced(checkpoint_owner):
    item = checkpoint_owner
    await item.writer.aput_writes(item.config, [("__error__", "first")], "special-task")
    await item.writer.aput_writes(item.config, [("__error__", "replacement")], "special-task")
    snapshot = await item.writer.aget_tuple(item.config)
    assert [value for task, channel, value in snapshot.pending_writes if task == "special-task" and channel == "__error__"] == ["replacement"]
    before = await checkpoint_rows(item.engine)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_nodes SET session_id='replaced'"))
    with pytest.raises(RuntimeError, match="Checkpoint ownership"):
        await item.writer.aput_writes(item.config, [("__error__", "rejected")], "special-task")
    assert await checkpoint_rows(item.engine) == before


@pytest.mark.asyncio
async def test_late_cancellation_title_is_written_before_durable_terminal(monkeypatch):
    from unittest.mock import AsyncMock

    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import StateGraph

    import deerflow.runtime.runs.worker as worker
    from deerflow.agents.thread_state import ThreadState
    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent

    manager = RunManager()
    record = await manager.create("late-cancel-c05")
    saver = InMemorySaver()

    async def response(state):
        return {"messages": [AIMessage(content="completed response")]}

    builder = StateGraph(ThreadState)
    builder.add_node("response", response)
    builder.set_entry_point("response")
    builder.set_finish_point("response")
    graph = builder.compile(checkpointer=saver)
    calls = []
    original_status = manager.set_status_if_not_cancelled

    async def cancel_at_final_commit(*args, **kwargs):
        if kwargs.get("persist", True):
            calls.append("late_cancel")
            return "interrupt"
        return await original_status(*args, **kwargs)

    original_persist = manager.persist_current_status

    async def persist(*args, **kwargs):
        calls.append("terminal")
        assert "title" in calls and calls.index("title") < calls.index("terminal")
        return await original_persist(*args, **kwargs)

    async def title(**kwargs):
        calls.append("title")
        assert record.status.value == "interrupted"

    monkeypatch.setattr(manager, "set_status_if_not_cancelled", cancel_at_final_commit)
    monkeypatch.setattr(manager, "persist_current_status", persist)
    monkeypatch.setattr(worker, "_ensure_interrupted_title", title)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=saver, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: graph,
        graph_input={"messages": [HumanMessage(content="request")]},
        config={"configurable": {"thread_id": record.thread_id}},
    )
    assert calls == ["late_cancel", "title", "terminal"]


@pytest.mark.asyncio
async def test_real_interrupted_title_fallback_writes_while_owned_and_rejects_terminal(checkpoint_owner):
    from langchain_core.messages import HumanMessage

    from deerflow.runtime.runs.worker import _ensure_interrupted_title

    item = checkpoint_owner
    await item.writer.adelete_thread(item.spec.thread_id)
    private = item.private.model_copy(update={"title": item.private.title.model_copy(update={"enabled": True})})
    title = await _ensure_interrupted_title(checkpointer=item.writer, thread_id=item.spec.thread_id, app_config=private, graph_input={"messages": [HumanMessage(content="Interrupted real PG request")]})
    assert title == "Interrupted real PG request"
    assert (await item.writer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id}})).checkpoint["channel_values"]["title"] == title
    await item.writer.adelete_thread(item.spec.thread_id)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='interrupted'"))
    before = await checkpoint_rows(item.engine)
    with pytest.raises(RuntimeError, match="Checkpoint ownership"):
        await _ensure_interrupted_title(checkpointer=item.writer, thread_id=item.spec.thread_id, app_config=private, graph_input={"messages": [HumanMessage(content="Too late request")]})
    assert await checkpoint_rows(item.engine) == before
