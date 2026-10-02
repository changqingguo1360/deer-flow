"""Actual PostgreSQL execution-bound primary repository mutation contracts."""

import asyncio
import inspect
from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import owner_environment as owner_environment
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner


async def durable_rows(engine):
    async with engine.connect() as conn:
        return {table: tuple((await conn.execute(text("SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"))).scalars()) for table in ("runs", "threads_meta", "run_events")}


def remote_repository(cls, sf, item):
    # Before the feature exists, execute the original real writes. A missing
    # constructor/import is never counted as the behavior RED.
    if "mutation_capability" in inspect.signature(cls).parameters:
        from app.fleet.mutation import FleetMutationCapability

        return cls(sf, mutation_capability=FleetMutationCapability(item.identity, item.spec))
    return cls(sf)


@pytest_asyncio.fixture
async def mutations(checkpoint_owner):
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.thread_meta.model import ThreadMetaRow
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime.events.store.db import DbRunEventStore

    item = checkpoint_owner
    sf = item.env[1]
    async with item.engine.begin() as conn:
        await conn.run_sync(lambda sync: ThreadMetaRow.__table__.create(sync, checkfirst=True))
        await conn.run_sync(lambda sync: RunEventRow.__table__.create(sync, checkfirst=True))
        await conn.execute(text("UPDATE runs SET status='running'"))
    local_thread = ThreadMetaRepository(sf)
    await local_thread.create(item.spec.thread_id, user_id=item.spec.user_id, display_name="baseline")
    await DbRunEventStore(sf).put(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="run.delivery", category="outputs", content="baseline")
    item.runs = remote_repository(RunRepository, sf, item)
    item.threads = remote_repository(ThreadMetaRepository, sf, item)
    item.events = remote_repository(DbRunEventStore, sf, item)
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    with remote_mutation_scope(item.runs._mutation_capability.context):
        yield item


async def write(item, operation):
    run, thread = item.spec.run_id, item.spec.thread_id
    if operation == "completion":
        return await item.runs.update_run_completion(run, status="success", total_tokens=9)
    if operation == "status":
        return await item.runs.update_status(run, "success")
    if operation == "progress":
        return await item.runs.update_run_progress(run, total_tokens=9)
    if operation == "model":
        return await item.runs.update_model_name(run, "changed-model")
    if operation == "display":
        return await item.threads.update_display_name(thread, "changed-title", user_id=item.spec.user_id)
    if operation == "thread_status":
        return await item.threads.update_status(thread, "idle", user_id=item.spec.user_id)
    if operation == "metadata":
        return await item.threads.update_metadata(thread, {"changed": True}, user_id=item.spec.user_id)
    event = dict(thread_id=thread, run_id=run, event_type="ai_message", category="messages", content="changed")
    if operation == "put":
        return await item.events.put(**event)
    if operation == "batch":
        return await item.events.put_batch([event])
    if operation == "singleton":
        return await item.events.put_if_absent(**event)
    if operation == "delete_run":
        return await item.events.delete_by_run(thread, run, user_id=item.spec.user_id)
    return await item.events.delete_by_thread(thread, user_id=item.spec.user_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["completion", "status", "progress", "model", "display", "thread_status", "metadata", "put", "batch", "singleton", "delete_run", "delete_thread"])
@pytest.mark.parametrize("stale", ["token", "owner", "expiry"])
async def test_primary_repositories_reject_stale_execution_without_row_or_sequence_changes(mutations, operation, stale):
    item = mutations
    async with item.engine.begin() as conn:
        statement = {"token": "UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'", "owner": "UPDATE runs SET owner_worker_id='replaced'", "expiry": "UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"}[stale]
        await conn.execute(text(statement))
    before = await durable_rows(item.engine)
    with pytest.raises(RuntimeError, match="ownership|mutation|execution"):
        await write(item, operation)
    assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["completion", "status", "progress", "model", "display", "thread_status", "metadata", "put", "batch", "singleton", "delete_run"])
async def test_original_execution_primary_writes_are_supported(mutations, operation):
    before = await durable_rows(mutations.engine)
    await write(mutations, operation)
    assert await durable_rows(mutations.engine) != before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["completion", "metadata", "batch"])
@pytest.mark.parametrize("context", ["missing", "wrong"])
async def test_bound_repositories_require_current_original_context(mutations, operation, context):
    from deerflow.runtime.execution.mutation_context import _current_mutation_context

    item = mutations
    before = await durable_rows(item.engine)
    supplied = None if context == "missing" else replace(item.runs._mutation_capability.context, attempt_id="wrong-attempt")
    token = _current_mutation_context.set(supplied)
    try:
        with pytest.raises(RuntimeError, match="mutation context"):
            await write(item, operation)
    finally:
        _current_mutation_context.reset(token)
    assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["run", "thread", "user", "batch-user", "mixed-run", "persisted-owner"])
async def test_remote_mutation_wrong_targets_reject_before_changes(mutations, target):
    item = mutations
    if target == "persisted-owner":
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE threads_meta SET user_id='other-owner'"))
    before = await durable_rows(item.engine)
    with pytest.raises(RuntimeError, match="mutation.*(target|ownership)"):
        if target == "run":
            await item.runs.update_status("other-run", "success")
        elif target == "thread":
            await item.threads.update_metadata("other-thread", {"wrong": True})
        elif target == "user":
            await item.threads.update_status(item.spec.thread_id, "idle", user_id="other-user")
        elif target == "persisted-owner":
            await item.threads.update_status(item.spec.thread_id, "idle", user_id=None)
        else:
            event = dict(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="run.end", category="lifecycle")
            events = [event | {"user_id": "other-user"}] if target == "batch-user" else [event, event | {"run_id": "other-run"}]
            await item.events.put_batch(events)
    assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
async def test_missing_ambient_auth_uses_bound_owner_for_thread_and_event_writes(mutations):
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    item = mutations
    token = set_current_user(None)
    try:
        await item.threads.update_metadata(item.spec.thread_id, {"trusted_callback": True}, user_id=None)
        event = await item.events.put(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="run.end", category="lifecycle")
        assert event["user_id"] == item.spec.user_id
    finally:
        reset_current_user(token)
    async with item.engine.connect() as conn:
        assert (await conn.execute(text("SELECT metadata_json FROM threads_meta"))).scalar_one()["trusted_callback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["success", "error", "interrupted", "timeout"])
async def test_original_terminal_bookkeeping_is_narrow_and_consistent(mutations, terminal):
    item = mutations
    await item.runs.update_status(item.spec.run_id, terminal, error="original-error", stop_reason="original-stop")
    await item.runs.update_status(item.spec.run_id, terminal, error="original-error", stop_reason="original-stop")
    await item.runs.finalize_if_not_cancelled(item.spec.run_id, status=terminal)
    await item.runs.update_run_completion(item.spec.run_id, status=terminal, total_tokens=17)
    await item.threads.update_status(item.spec.thread_id, "idle" if terminal == "success" else terminal)
    await item.threads.update_checkpoint_display_name(item.spec.thread_id, "checkpoint-derived title")
    before = await durable_rows(item.engine)
    for mutation in (
        lambda: item.runs.update_status(item.spec.run_id, "running"),
        lambda: item.runs.update_status(item.spec.run_id, terminal, error="different-error"),
        lambda: item.runs.finalize_if_not_cancelled(item.spec.run_id, status=terminal, stop_reason="different-stop"),
        lambda: item.runs.update_run_completion(item.spec.run_id, status=terminal, error="different-error"),
        lambda: item.runs.update_run_progress(item.spec.run_id, total_tokens=29),
        lambda: item.threads.update_display_name(item.spec.thread_id, "arbitrary-title"),
        lambda: item.threads.update_metadata(item.spec.thread_id, {"arbitrary": True}),
        lambda: item.threads.update_status(item.spec.thread_id, "running"),
        lambda: item.events.put_batch([dict(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="run.end", category="lifecycle")]),
        lambda: item.events.put_if_absent(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="run.delivery", category="outputs"),
    ):
        with pytest.raises(RuntimeError, match="ownership|terminal"):
            await mutation()
        assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["put", "delete", "start", "lease", "renew", "cancel", "takeover", "admit", "thread-delete", "thread-owner", "events-thread-delete"])
async def test_remote_admin_ownership_and_new_admission_apis_reject(mutations, operation):
    from datetime import UTC, datetime, timedelta

    item = mutations
    run, thread = item.spec.run_id, item.spec.thread_id
    deadline = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    actions = {
        "put": lambda: item.runs.put(run, thread_id=thread),
        "delete": lambda: item.runs.delete(run),
        "start": lambda: item.runs.start_run(run),
        "lease": lambda: item.runs.update_lease(run, owner_worker_id=item.identity.owner_worker_id, lease_expires_at=deadline),
        "renew": lambda: item.runs.renew_lease(run, owner_worker_id=item.identity.owner_worker_id, lease_expires_at=deadline),
        "cancel": lambda: item.runs.request_cancel(run, action="interrupt"),
        "takeover": lambda: item.runs.claim_for_takeover(run, grace_seconds=0, error="wrong"),
        "admit": lambda: item.runs.create_thread_operation_atomic(
            run_id="other", thread_id=thread, user_id=item.spec.user_id, operation_kind="run", multitask_strategy="reject", assistant_id=None, owner_worker_id=None, lease_expires_at=None, metadata={}, kwargs={}
        ),
        "thread-delete": lambda: item.threads.delete(thread),
        "thread-owner": lambda: item.threads.update_owner(thread, "other-owner"),
        "events-thread-delete": lambda: item.events.delete_by_thread(thread),
    }
    before = await durable_rows(item.engine)
    with pytest.raises(RuntimeError, match="unsupported"):
        await actions[operation]()
    assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["progress", "metadata", "batch"])
async def test_primary_write_rechecks_expiry_after_real_lock_wait(mutations, operation):
    from .test_c05_remote_agent_runtime import wait_blocked

    item = mutations
    repository = {"progress": item.runs, "metadata": item.threads, "batch": item.events}[operation]
    capability = repository._mutation_capability
    pid_future = asyncio.get_running_loop().create_future()
    original = capability.validate_async

    async def observed(session, **kwargs):
        pid_future.set_result((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        await original(session, **kwargs)

    capability.validate_async = observed
    before = await durable_rows(item.engine)
    async with item.engine.connect() as blocker:
        tx = await blocker.begin()
        await blocker.execute(text("SELECT id FROM fleet_agent_tasks FOR UPDATE"))
        writing = asyncio.create_task(write(item, operation))
        await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
        await blocker.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
        await blocker.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
        before["runs"] = tuple((await blocker.execute(text("SELECT row_to_json(t)::text FROM runs t ORDER BY row_to_json(t)::text"))).scalars())
        await tx.commit()
        with pytest.raises(RuntimeError, match="ownership"):
            await writing
    assert await durable_rows(item.engine) == before


async def require_takeover_blocked(engine, pid, changing):
    from .test_c05_remote_agent_runtime import wait_blocked

    await wait_blocked(engine, pid)
    assert not changing.done(), "Execution locks were released before writer commit"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["progress", "metadata", "batch"])
async def test_primary_guard_and_write_share_transaction_and_block_takeover(mutations, operation):
    from sqlalchemy import event

    item = mutations
    repository = {"progress": item.runs, "metadata": item.threads, "batch": item.events}[operation]
    capability = repository._mutation_capability
    ready, release = asyncio.Event(), asyncio.Event()
    identities, sql_connections = [], []
    original = capability.validate_async

    async def observed(session, **kwargs):
        await original(session, **kwargs)
        identities.append(tuple((await session.execute(text("SELECT pg_backend_pid(),txid_current()"))).one()))
        ready.set()
        await release.wait()
        identities.append(tuple((await session.execute(text("SELECT pg_backend_pid(),txid_current()"))).one()))

    capability.validate_async = observed

    def writing_sql(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith(("UPDATE THREADS_META", "UPDATE RUNS", "INSERT INTO RUN_EVENTS")):
            row = conn.exec_driver_sql("SELECT pg_backend_pid(),txid_current()").one()
            sql_connections.append(tuple(row))

    event.listen(item.engine.sync_engine, "before_cursor_execute", writing_sql)
    try:
        writing = asyncio.create_task(write(item, operation))
        await asyncio.wait_for(ready.wait(), 5)
        async with item.engine.connect() as takeover:
            tx = await takeover.begin()
            pid = (await takeover.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            changing = asyncio.create_task(takeover.execute(text("UPDATE fleet_agent_tasks SET state='paused'")))
            await require_takeover_blocked(item.engine, pid, changing)
            release.set()
            await writing
            await changing
            await tx.commit()
    finally:
        event.remove(item.engine.sync_engine, "before_cursor_execute", writing_sql)
    assert identities[0] == identities[1] and sql_connections and all(value == identities[0] for value in sql_connections)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize("operation", ["progress", "metadata", "batch"])
async def test_actual_first_write_failure_rolls_back_primary_tables(mutations, operation, error):
    from sqlalchemy import event

    item = mutations
    before = await durable_rows(item.engine)

    def fail(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith(("UPDATE RUNS", "UPDATE THREADS_META", "INSERT INTO RUN_EVENTS")):
            raise error("injected after first actual SQL write")

    event.listen(item.engine.sync_engine, "after_cursor_execute", fail)
    try:
        with pytest.raises(error):
            await write(item, operation)
    finally:
        event.remove(item.engine.sync_engine, "after_cursor_execute", fail)
    assert await durable_rows(item.engine) == before
    await write(item, operation)


@pytest.mark.asyncio
async def test_independent_transaction_negative_control_permits_untracked_owner_race(mutations):
    item = mutations
    cap = item.events._mutation_capability
    original = cap.validate_async
    ready, release = asyncio.Event(), asyncio.Event()

    async def independent(ignored, **kwargs):
        async with item.env[1].begin() as other:
            await original(other, **kwargs)
        ready.set()
        await release.wait()

    cap.validate_async = independent
    before = await durable_rows(item.engine)
    writing = asyncio.create_task(write(item, "batch"))
    await asyncio.wait_for(ready.wait(), 5)
    async with item.engine.begin() as takeover:
        pid = (await takeover.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        changing = asyncio.create_task(takeover.execute(text("UPDATE fleet_agent_tasks SET state='paused'")))
        # The identical production race criterion must reject this control.
        with pytest.raises(TimeoutError):
            await require_takeover_blocked(item.engine, pid, changing)
        await changing
    release.set()
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    with pytest.raises(OwnershipRejected):
        await writing
    assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
async def test_owned_attach_and_start_obtain_task_before_core_run(mutations):
    from .test_c05_remote_agent_runtime import wait_blocked

    item = mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='pending'"))
    for method in (item.runs.get_owned_execution, item.runs.start_owned_run):
        cap = item.runs._mutation_capability
        original = cap.validate_async
        pid_future = asyncio.get_running_loop().create_future()

        async def seen(session, **kwargs):
            if not pid_future.done():
                pid_future.set_result((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
            await original(session, **kwargs)

        cap.validate_async = seen
        async with item.engine.connect() as blocker:
            tx = await blocker.begin()
            await blocker.execute(text("SELECT id FROM fleet_agent_tasks FOR UPDATE"))
            operation = asyncio.create_task(method(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet"))
            await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
            # Would block/deadlock if the remote helper locked run before task.
            await asyncio.wait_for(blocker.execute(text("SELECT run_id FROM runs FOR UPDATE")), 2)
            await tx.commit()
            await operation
        cap.validate_async = original


@pytest.mark.asyncio
async def test_batch_explicit_none_is_stamped_with_original_owner(mutations):
    item = mutations
    rows = await item.events.put_batch([dict(thread_id=item.spec.thread_id, run_id=item.spec.run_id, user_id=None, event_type="run.end", category="lifecycle")])
    assert rows[0]["user_id"] == item.spec.user_id


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", [False, True])
async def test_actual_sync_session_capability_keeps_scope_and_transaction(mutations, stale):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from deerflow.runtime.execution.mutation_context import MutationTarget, current_remote_mutation_context

    item = mutations
    cap = item.runs._mutation_capability
    async with item.engine.connect() as conn:
        schema = (await conn.execute(text("SELECT current_schema()"))).scalar_one()
    if stale:
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    before = await durable_rows(item.engine)

    def execute():
        engine = create_engine(item.engine.url.set(drivername="postgresql+psycopg"), connect_args={"options": "-csearch_path=" + schema})
        try:
            with Session(engine) as session, session.begin():
                cap.validate_sync(session, context=current_remote_mutation_context(), operation="run.progress", targets=(MutationTarget(run_id=item.spec.run_id),))
                session.execute(text("UPDATE runs SET total_tokens=21 WHERE run_id=:run"), {"run": item.spec.run_id})
        finally:
            engine.dispose()

    if stale:
        with pytest.raises(RuntimeError, match="ownership"):
            await asyncio.to_thread(execute)
        assert await durable_rows(item.engine) == before
    else:
        await asyncio.to_thread(execute)
        assert await durable_rows(item.engine) != before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["status", "completion", "progress", "model"])
async def test_actual_manager_rejection_is_nonretryable_and_marks_even_staged_terminal(mutations, operation):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.runtime.runs.manager import RunManager, _is_retryable_persistence_error
    from deerflow.runtime.runs.schemas import RunStatus

    item = mutations
    # This fixture seeds low-level checkpoint bytes for saver tests; the real
    # graph starts from its actual HumanMessage input, not that raw blob.
    await item.writer.adelete_thread(item.spec.thread_id)
    manager = RunManager(store=item.runs, worker_id=item.identity.owner_worker_id)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='pending'"))
    record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
    await manager.try_start(record.run_id)
    if operation == "completion":
        await manager.set_status(record.run_id, RunStatus.success, persist=False)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    before = await durable_rows(item.engine)
    assert not _is_retryable_persistence_error(OwnershipRejected("database is locked"))
    calls = 0
    name = {"status": "update_status", "completion": "update_run_completion", "progress": "update_run_progress", "model": "update_model_name"}[operation]
    original = getattr(item.runs, name)

    async def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await original(*args, **kwargs)

    setattr(item.runs, name, counted)
    if operation == "status":
        await manager.set_status(record.run_id, RunStatus.success)
    elif operation == "completion":
        await manager.update_run_completion(record.run_id, status="success", total_tokens=9)
    elif operation == "progress":
        await manager.update_run_progress(record.run_id, total_tokens=9)
    else:
        await manager.update_model_name(record.run_id, "model-change")
    assert calls == 1 and record.ownership_lost and record.status == RunStatus.error
    assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("lose_owner", [False, True])
async def test_real_worker_terminal_order_and_lost_owner_finalization(mutations, lose_owner):
    from unittest.mock import AsyncMock

    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.graph import StateGraph

    from deerflow.agents.thread_state import ThreadState
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent

    item = mutations
    # This fixture seeds low-level checkpoint bytes for saver tests; the real
    # graph starts from its actual HumanMessage input, not that raw blob.
    await item.writer.adelete_thread(item.spec.thread_id)
    manager = RunManager(store=item.runs, worker_id=item.identity.owner_worker_id)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='pending'"))
    record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")

    async def answer(state):
        if lose_owner:
            async with item.engine.begin() as conn:
                await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
        return {"messages": [AIMessage(content="actual worker response")], "title": "actual checkpoint title"}

    graph = StateGraph(ThreadState)
    graph.add_node("answer", answer)
    graph.set_entry_point("answer")
    graph.set_finish_point("answer")
    compiled = graph.compile(checkpointer=item.writer)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=item.writer, event_store=item.events, thread_store=item.threads, app_config=item.private),
        agent_factory=lambda *, config: compiled,
        graph_input={"messages": [HumanMessage(content="primary worker request")]},
        config={"configurable": {"thread_id": item.spec.thread_id}},
    )
    async with item.engine.connect() as conn:
        status = (await conn.execute(text("SELECT status FROM runs"))).scalar_one()
        title, thread_status = (await conn.execute(text("SELECT display_name,status FROM threads_meta"))).one()
    if lose_owner:
        assert record.ownership_lost and record.status.value == "error" and status == "running"
        assert title == "baseline"
    else:
        assert not record.ownership_lost and record.status.value == status == "success"
        assert title == "actual checkpoint title" and thread_status == "idle"
        assert any(event["event_type"] == "run.delivery" for event in await item.events.list_events(item.spec.thread_id, item.spec.run_id))


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["receipt", "journal", "completed_background"])
async def test_stale_event_ownership_is_not_retried_or_swallowed(mutations, path):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.runtime.journal import RunJournal
    from deerflow.runtime.runs.worker import _persist_delivery_receipt

    item = mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    before = await durable_rows(item.engine)
    calls = 0
    method = "put_if_absent" if path == "receipt" else "put_batch"
    original = getattr(item.events, method)

    async def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await original(*args, **kwargs)

    setattr(item.events, method, counted)
    with pytest.raises(OwnershipRejected):
        if path == "receipt":
            await _persist_delivery_receipt(item.events, thread_id=item.spec.thread_id, run_id=item.spec.run_id, content={})
        else:
            journal = RunJournal(item.spec.run_id, item.spec.thread_id, item.events, flush_threshold=1)
            journal.record_delivery()
            if path == "completed_background":
                await asyncio.gather(*tuple(journal._pending_flush_tasks), return_exceptions=True)
                await asyncio.sleep(0)
                assert not journal._pending_flush_tasks
            else:
                await asyncio.sleep(0)
            await journal.flush()
    assert calls == 1
    assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
async def test_original_remote_thread_can_be_initialized_before_first_status(mutations):
    item = mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("DELETE FROM threads_meta"))
    assert await item.threads.get(item.spec.thread_id, user_id=item.spec.user_id) is None
    await item.threads.create(item.spec.thread_id, assistant_id=item.spec.assistant_id, user_id=item.spec.user_id)
    await item.threads.update_status(item.spec.thread_id, "running")
    async with item.engine.connect() as conn:
        row = (await conn.execute(text("SELECT user_id,status FROM threads_meta"))).one()
    assert row == (item.spec.user_id, "running")


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["missing", "existing", "wrong_owner", "null_owner", "stale", "concurrent"])
async def test_executor_thread_initialization_is_atomic_original_owned_and_idempotent(mutations, state):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = mutations
    async with item.engine.begin() as conn:
        if state in {"missing", "stale", "concurrent"}:
            await conn.execute(text("DELETE FROM threads_meta"))
        elif state in {"wrong_owner", "null_owner"}:
            await conn.execute(text("UPDATE threads_meta SET user_id=:owner"), {"owner": None if state == "null_owner" else "other-user"})
        if state == "stale":
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    before = await durable_rows(item.engine)

    async def ensure():
        return await item.threads.ensure_executor_thread(item.spec.thread_id, user_id=item.spec.user_id, assistant_id=item.spec.assistant_id, metadata={"new": True})

    if state in {"wrong_owner", "null_owner", "stale"}:
        with pytest.raises(OwnershipRejected):
            await ensure()
        assert await durable_rows(item.engine) == before
    else:
        rows = await asyncio.gather(ensure(), ensure()) if state == "concurrent" else [await ensure()]
        assert all(row["user_id"] == item.spec.user_id for row in rows)
        if state == "existing":
            assert await durable_rows(item.engine) == before
        async with item.engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM threads_meta"))).scalar_one() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("revocation", ["token", "owner", "expiry"])
@pytest.mark.parametrize("caller", ["manager", "worker"])
async def test_owned_start_rejection_immediately_fences_original_record(mutations, revocation, caller):
    from unittest.mock import AsyncMock

    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent

    item = mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='pending'"))
    manager = RunManager(store=item.runs, worker_id=item.identity.owner_worker_id)
    record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
    async with item.engine.begin() as conn:
        if revocation == "token":
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
        elif revocation == "owner":
            await conn.execute(text("UPDATE runs SET owner_worker_id='other-owner'"))
        else:
            await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
            await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    before = await durable_rows(item.engine)
    calls = {"start": 0, "normal_finalization": 0}
    original_start = item.runs.start_owned_run

    async def counted_start(*args, **kwargs):
        calls["start"] += 1
        return await original_start(*args, **kwargs)

    item.runs.start_owned_run = counted_start
    for name in ("update_status", "finalize_if_not_cancelled", "update_run_completion"):
        original = getattr(item.runs, name)

        async def counted_finalization(*args, _original=original, **kwargs):
            calls["normal_finalization"] += 1
            return await _original(*args, **kwargs)

        setattr(item.runs, name, counted_finalization)

    original_finalize = manager.set_status_if_not_cancelled

    async def counted_manager_finalization(*args, **kwargs):
        calls["normal_finalization"] += 1
        return await original_finalize(*args, **kwargs)

    manager.set_status_if_not_cancelled = counted_manager_finalization

    factory_calls = 0

    def factory(*, config):
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("Rejected executor must not construct its graph")

    if caller == "manager":
        with pytest.raises(OwnershipRejected):
            await manager.try_start(record.run_id)
    else:
        bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
        await run_agent(
            bridge,
            manager,
            record,
            ctx=RunContext(checkpointer=item.writer, event_store=item.events, thread_store=item.threads, app_config=item.private),
            agent_factory=factory,
            graph_input={},
            config={"configurable": {"thread_id": item.spec.thread_id}},
        )
    assert record.ownership_lost and record.abort_event.is_set() and record.status.value == "error"
    assert factory_calls == 0 and calls == {"start": 1, "normal_finalization": 0}
    assert await durable_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", ["threshold", "end", "pending"])
async def test_bound_subagent_batch_rejection_propagates_without_rebuffer(mutations, trigger):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.runtime.runs.worker import _SubagentEventBuffer

    item = mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    before = await durable_rows(item.engine)
    calls = 0
    original = item.events.put_batch

    async def counted(events):
        nonlocal calls
        calls += 1
        return await original(events)

    item.events.put_batch = counted
    buffer = _SubagentEventBuffer(item.events, item.spec.thread_id, item.spec.run_id)
    step = {"type": "task_running", "task_id": "actual-task", "message": {"type": "tool", "name": "bash", "content": "actual step"}, "message_index": 1}
    with pytest.raises(OwnershipRejected):
        if trigger == "threshold":
            for index in range(buffer.FLUSH_THRESHOLD):
                await buffer.add({**step, "message_index": index})
        elif trigger == "end":
            await buffer.add(step)
            await buffer.add({"type": "task_completed", "task_id": "actual-task", "result": "done"})
        else:
            await buffer.add(step)
            await buffer.flush()
    assert not buffer._pending
    await buffer.flush()
    assert calls == 1 and await durable_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", ["threshold", "end", "pending"])
@pytest.mark.parametrize("multiple_modes", [False, True])
async def test_actual_worker_subagent_rejection_fences_and_completes_cleanup(mutations, trigger, multiple_modes, monkeypatch):
    from unittest.mock import AsyncMock

    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.config import get_stream_writer
    from langgraph.graph import StateGraph

    from deerflow.agents.thread_state import ThreadState
    from deerflow.runtime.journal import RunJournal
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, _SubagentEventBuffer, run_agent

    journals = []
    original_journal_init = RunJournal.__init__

    def remember_owned_journal(journal, *args, **kwargs):
        original_journal_init(journal, *args, **kwargs)
        journals.append(journal)

    monkeypatch.setattr(RunJournal, "__init__", remember_owned_journal)
    item = mutations
    await item.writer.adelete_thread(item.spec.thread_id)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='pending'"))
    manager = RunManager(store=item.runs, worker_id=item.identity.owner_worker_id)
    record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
    rejected_calls = 0
    before = None
    after_rejection_finalization = 0
    original_batch = item.events.put_batch

    async def revoke_then_write(events):
        nonlocal rejected_calls, before
        if any(event["event_type"].startswith("subagent.") for event in events):
            rejected_calls += 1
            async with item.engine.begin() as conn:
                await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
            before = await durable_rows(item.engine)
        return await original_batch(events)

    item.events.put_batch = revoke_then_write
    original_flush = _SubagentEventBuffer.flush
    rejected_flush_returned = False

    async def observe_flush(buffer):
        nonlocal rejected_flush_returned
        await original_flush(buffer)
        if before is not None:
            rejected_flush_returned = True

    monkeypatch.setattr(_SubagentEventBuffer, "flush", observe_flush)
    for name in ("update_status", "finalize_if_not_cancelled", "update_run_completion"):
        original = getattr(item.runs, name)

        async def counted_finalization(*args, _original=original, **kwargs):
            nonlocal after_rejection_finalization
            if before is not None:
                after_rejection_finalization += 1
            return await _original(*args, **kwargs)

        setattr(item.runs, name, counted_finalization)

    async def answer(state):
        writer = get_stream_writer()
        step = {"type": "task_running", "task_id": "actual-task", "message": {"type": "tool", "name": "bash", "content": "actual step"}, "message_index": 1}
        for index in range(_SubagentEventBuffer.FLUSH_THRESHOLD if trigger == "threshold" else 1):
            writer({**step, "message_index": index})
        if trigger == "end":
            writer({"type": "task_completed", "task_id": "actual-task", "result": "done"})
        return {"messages": [AIMessage(content="actual worker response")]}

    graph = StateGraph(ThreadState)
    graph.add_node("answer", answer)
    graph.set_entry_point("answer")
    graph.set_finish_point("answer")
    compiled = graph.compile(checkpointer=item.writer)
    streams, checkpoint_tasks = [], set()
    run_task = asyncio.current_task()
    original_astream = compiled.astream
    for method in ("aput", "aput_writes"):
        original = getattr(item.writer, method)

        async def remember_checkpoint_task(*args, _original=original, **kwargs):
            if asyncio.current_task() is not run_task:
                checkpoint_tasks.add(asyncio.current_task())
            return await _original(*args, **kwargs)

        monkeypatch.setattr(item.writer, method, remember_checkpoint_task)

    def remember_owned_stream(*args, **kwargs):
        stream = original_astream(*args, **kwargs)
        streams.append(stream)
        return stream

    monkeypatch.setattr(compiled, "astream", remember_owned_stream)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=item.writer, event_store=item.events, thread_store=item.threads, app_config=item.private),
        agent_factory=lambda *, config: compiled,
        graph_input={"messages": [HumanMessage(content="subagent stream")]},
        config={"configurable": {"thread_id": item.spec.thread_id}},
        stream_modes=["custom", "updates"] if multiple_modes else ["custom"],
    )
    # The worker must settle the actual graph generator and its checkpoint
    # executor before reporting completion. Keep a reference so GC timing cannot
    # make this race disappear; fixture cleanup closes only these owned streams.
    try:
        assert len(streams) == 1 and streams[0].ag_frame is None
        assert checkpoint_tasks and all(task.done() for task in checkpoint_tasks)
    finally:
        await asyncio.gather(*(stream.aclose() for stream in streams), return_exceptions=True)
    # Settle only callbacks owned by this actual run before its schema teardown.
    # Loss prevents normal finalization/flush; await existing tasks without
    # re-flushing, re-buffering, retrying writes or masking their rejection.
    assert len(journals) == 1
    await asyncio.sleep(0)
    async with asyncio.timeout(5):
        while True:
            pending = {task for task in journals[0]._pending_flush_tasks if not task.done()}
            progress = journals[0]._pending_progress_task
            if progress is not None and not progress.done():
                pending.add(progress)
            if not pending:
                break
            await asyncio.gather(*pending, return_exceptions=True)
    assert rejected_calls == 1 and before is not None and not rejected_flush_returned
    assert record.ownership_lost and record.status.value == "error" and not record.finalizing
    assert after_rejection_finalization == 0 and await durable_rows(item.engine) == before
    bridge.publish_end.assert_awaited_once_with(record.run_id)
    bridge.cleanup.assert_awaited_once_with(record.run_id, delay=60)


async def secondary_rows(engine):
    async with engine.connect() as conn:
        return {table: tuple((await conn.execute(text("SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"))).scalars()) for table in ("store", "agents", "managed_subagents")}


@pytest_asyncio.fixture
async def secondary_mutations(mutations):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from deerflow.persistence.agents.model import AgentRow
    from deerflow.persistence.agents.sql import SqlAgentStore
    from deerflow.persistence.managed_subagents.base import ManagedSubagentDefinition
    from deerflow.persistence.managed_subagents.model import ManagedSubagentRow
    from deerflow.persistence.managed_subagents.sql import SqlManagedSubagentStore
    from deerflow.runtime.store.async_provider import make_store

    item = mutations
    async with item.engine.begin() as conn:
        await conn.run_sync(lambda sync: AgentRow.__table__.create(sync, checkfirst=True))
        await conn.run_sync(lambda sync: ManagedSubagentRow.__table__.create(sync, checkfirst=True))
        schema = (await conn.execute(text("SELECT current_schema()"))).scalar_one()
    sync_engine = create_engine(item.engine.url.set(drivername="postgresql+psycopg"), connect_args={"options": "-csearch_path=" + schema})
    sync_sf = sessionmaker(sync_engine, expire_on_commit=False)
    cap = item.runs._mutation_capability
    kwargs = {"session_factory": sync_sf}

    def make(cls):
        return cls(str(sync_engine.url), **kwargs, **({"mutation_capability": cap} if "mutation_capability" in inspect.signature(cls).parameters else {}))

    item.agents = make(SqlAgentStore)
    item.managed = make(SqlManagedSubagentStore)
    definition = ManagedSubagentDefinition(name="bound-managed", description="baseline", system_prompt="baseline")
    await asyncio.to_thread(SqlAgentStore(str(sync_engine.url), session_factory=sync_sf).create, "bound-agent", {"description": "baseline"}, "baseline", user_id=item.spec.user_id)
    await asyncio.to_thread(SqlManagedSubagentStore(str(sync_engine.url), session_factory=sync_sf).create, definition)
    # Only the trusted unbound factory initializes Store migrations.
    async with make_store(item.private) as trusted:
        await trusted.aput(("user", item.spec.user_id), "bound-key", {"baseline": True}, ttl=1)
    try:
        async with make_store(item.private, **({"mutation_capability": cap} if "mutation_capability" in inspect.signature(make_store).parameters else {})) as store:
            item.state_store = store
            yield item
    finally:
        sync_engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["aput", "adelete", "aget", "asearch", "abatch", "put", "delete", "batch", "get", "search", "agent.create", "agent.update", "agent.delete", "managed.create", "managed.update", "managed.delete"])
async def test_store_and_definitions_reject_stale_original_token_before_real_writes(secondary_mutations, operation):
    from langgraph.store.base import PutOp

    from deerflow.persistence.managed_subagents.base import ManagedSubagentDefinition
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = secondary_mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    before = await secondary_rows(item.engine)
    namespace = ("user", item.spec.user_id)
    with pytest.raises(OwnershipRejected):
        if operation == "aput":
            await item.state_store.aput(namespace, "new-key", {"changed": True})
        elif operation == "adelete":
            await item.state_store.adelete(namespace, "bound-key")
        elif operation == "aget":
            await item.state_store.aget(namespace, "bound-key", refresh_ttl=True)
        elif operation == "asearch":
            await item.state_store.asearch(namespace, refresh_ttl=True)
        elif operation == "abatch":
            await item.state_store.abatch([PutOp(namespace, "new-key", {"changed": True})])
        elif operation == "put":
            await asyncio.to_thread(item.state_store.put, namespace, "new-key", {"changed": True})
        elif operation == "delete":
            await asyncio.to_thread(item.state_store.delete, namespace, "bound-key")
        elif operation == "batch":
            await asyncio.to_thread(item.state_store.batch, [PutOp(namespace, "new-key", {"changed": True})])
        elif operation == "get":
            await asyncio.to_thread(item.state_store.get, namespace, "bound-key", refresh_ttl=True)
        elif operation == "search":
            await asyncio.to_thread(item.state_store.search, namespace, refresh_ttl=True)
        elif operation == "agent.create":
            await asyncio.to_thread(item.agents.create, "new-agent", {"description": "changed"}, "changed", user_id=item.spec.user_id)
        elif operation == "agent.update":
            await asyncio.to_thread(item.agents.update, "bound-agent", {"description": "changed"}, "changed", user_id=item.spec.user_id)
        elif operation == "agent.delete":
            await asyncio.to_thread(item.agents.delete, "bound-agent", user_id=item.spec.user_id)
        elif operation == "managed.create":
            await asyncio.to_thread(item.managed.create, ManagedSubagentDefinition(name="new-managed", description="changed", system_prompt="changed"))
        elif operation == "managed.update":
            await asyncio.to_thread(item.managed.update, ManagedSubagentDefinition(name="bound-managed", description="changed", system_prompt="changed"))
        else:
            await asyncio.to_thread(item.managed.delete, "bound-managed")
    assert await secondary_rows(item.engine) == before


@pytest.mark.asyncio
async def test_original_store_and_definitions_write_in_caller_and_external_thread_context(secondary_mutations):
    from langgraph.store.base import GetOp, PutOp

    from deerflow.persistence.managed_subagents.base import ManagedSubagentDefinition

    item = secondary_mutations
    namespace = ("user", item.spec.user_id)
    await item.state_store.aput(namespace, "positive", {"v": 1}, ttl=2)
    await asyncio.to_thread(item.state_store.put, namespace, "sync-positive", {"v": 2})
    results = await asyncio.to_thread(item.state_store.batch, [PutOp(namespace, "batch-positive", {"v": 3}), GetOp(namespace, "positive", refresh_ttl=False)])
    assert results[1].value == {"v": 1}
    await asyncio.to_thread(item.agents.update, "bound-agent", {"description": "positive"}, "positive", user_id=item.spec.user_id)
    await asyncio.to_thread(item.managed.update, ManagedSubagentDefinition(name="bound-managed", description="positive", system_prompt="positive"))
    assert (await item.state_store.aget(namespace, "sync-positive", refresh_ttl=False)).value == {"v": 2}
    assert (await asyncio.to_thread(item.agents.get, "bound-agent", user_id=item.spec.user_id)).description == "positive"
    assert (await asyncio.to_thread(item.managed.get, "bound-managed")).description == "positive"


@pytest.mark.asyncio
async def test_terminal_store_readonly_get_search_and_sync_alias_do_not_refresh_ttl(secondary_mutations):
    item = secondary_mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
    before = await secondary_rows(item.engine)
    namespace = ("user", item.spec.user_id)
    assert (await item.state_store.aget(namespace, "bound-key", refresh_ttl=False)).value == {"baseline": True}
    assert len(await item.state_store.asearch(namespace, refresh_ttl=False)) == 1
    assert (await asyncio.to_thread(item.state_store.get, namespace, "bound-key", refresh_ttl=False)).value == {"baseline": True}
    assert await secondary_rows(item.engine) == before
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    for action in (
        lambda: item.state_store.aget(namespace, "bound-key"),
        lambda: item.state_store.asearch(namespace),
        lambda: item.state_store.aput(namespace, "new", {}),
        lambda: item.state_store.sweep_ttl(),
        lambda: item.state_store.start_ttl_sweeper(),
    ):
        with pytest.raises(OwnershipRejected):
            await action()
    assert await secondary_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_context", ["missing", "different"])
async def test_store_and_definition_instances_cannot_fall_back_when_context_lost(secondary_mutations, bad_context):
    from contextlib import nullcontext
    from dataclasses import replace

    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context, remote_mutation_scope

    item = secondary_mutations
    token = _current_mutation_context.set(None)
    before = await secondary_rows(item.engine)
    try:
        scope = nullcontext() if bad_context == "missing" else remote_mutation_scope(replace(item.runs._mutation_capability.context, run_id="wrong-run"))
        with scope:
            with pytest.raises(OwnershipRejected):
                await item.state_store.aput(("user", item.spec.user_id), "new", {})
            with pytest.raises(OwnershipRejected):
                await asyncio.to_thread(item.agents.update, "bound-agent", {}, "wrong", user_id=item.spec.user_id)
            with pytest.raises(OwnershipRejected):
                await asyncio.to_thread(item.managed.delete, "bound-managed")
    finally:
        _current_mutation_context.reset(token)
    assert await secondary_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["migration_gap", "future_migration", "type", "index", "wrong_schema"])
async def test_store_readiness_rejects_without_ddl(secondary_mutations, drift):
    item = secondary_mutations
    async with item.engine.begin() as conn:
        if drift == "migration_gap":
            await conn.execute(text("DELETE FROM store_migrations WHERE v=1"))
        elif drift == "future_migration":
            await conn.execute(text("INSERT INTO store_migrations VALUES (99)"))
        elif drift == "type":
            await conn.execute(text("ALTER TABLE store ALTER COLUMN ttl_minutes TYPE bigint"))
        elif drift == "index":
            await conn.execute(text("DROP INDEX store_prefix_idx"))
            await conn.execute(text("CREATE INDEX store_prefix_idx ON store(key)"))
        else:
            item.state_store._schema = "missing_schema"

    async def structure():
        async with item.engine.connect() as conn:
            return tuple((await conn.execute(text("SELECT c.relname, c.relkind, pg_get_indexdef(c.oid) FROM pg_class c WHERE c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=current_schema()) ORDER BY c.relname"))).all())

    before = await structure()
    with pytest.raises(RuntimeError, match="ready"):
        await item.state_store.setup()
    assert await structure() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["scoped", "lost"])
async def test_bound_runtime_global_soul_rejected_before_any_file_side_effect(secondary_mutations, tmp_path, monkeypatch, mode):
    from contextlib import nullcontext

    from deerflow.persistence.agent_definition_context import _stores, agent_definition_store_scope
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context
    from deerflow.tools.builtins.setup_agent_tool import setup_agent

    item = secondary_mutations
    paths = SimpleNamespace(base_dir=tmp_path / "must-not-exist")
    monkeypatch.setattr("deerflow.tools.builtins.setup_agent_tool.get_paths", lambda: paths)
    runtime = SimpleNamespace(context={}, store=item.state_store, tool_call_id="global-soul")
    mutation_token = _current_mutation_context.set(None) if mode == "lost" else None
    definition_token = _stores.set(None)
    try:
        scope = agent_definition_store_scope(item.agents, item.managed) if mode == "scoped" else nullcontext()
        with scope, pytest.raises(OwnershipRejected):
            await asyncio.to_thread(setup_agent.func, soul="must not persist", description="bound", runtime=runtime)
    finally:
        _stores.reset(definition_token)
        if mutation_token is not None:
            _current_mutation_context.reset(mutation_token)
    assert not paths.base_dir.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_real_definition_tool_rejection_propagates_through_actual_middleware(secondary_mutations, asynchronous):
    from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
    from deerflow.persistence.agent_definition_context import agent_definition_store_scope
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.tools.builtins.setup_agent_tool import setup_agent

    item = secondary_mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    before = await secondary_rows(item.engine)
    runtime = SimpleNamespace(context={"agent_name": "bound-agent", "user_id": item.spec.user_id}, store=item.state_store, tool_call_id="stale-tool")
    request = SimpleNamespace(tool_call={"name": "setup_agent", "id": "stale-tool"})
    middleware = ToolErrorHandlingMiddleware(app_config=item.private)
    with agent_definition_store_scope(item.agents, item.managed):
        if asynchronous:

            async def handler(request):
                return await asyncio.to_thread(setup_agent.func, soul="must not persist", description="bound", runtime=runtime)

            with pytest.raises(OwnershipRejected):
                await middleware.awrap_tool_call(request, handler)
        else:
            with pytest.raises(OwnershipRejected):
                await asyncio.to_thread(middleware.wrap_tool_call, request, lambda request: setup_agent.func(soul="must not persist", description="bound", runtime=runtime))
    assert await secondary_rows(item.engine) == before


@pytest_asyncio.fixture
async def vector_mutations(secondary_mutations):
    from langchain_core.embeddings import Embeddings
    from langgraph.store.postgres.aio import AsyncPostgresStore

    from deerflow.runtime.checkpointer.async_provider import dsn_with_search_path
    from deerflow.runtime.store.fenced_store import FencedAsyncPostgresStore

    class Embedded(Embeddings):
        async def aembed_documents(self, texts):
            if self.callback is not None:
                await self.callback()
            return [[float(len(text)), 1.0] for text in texts]

        def embed_documents(self, texts):
            return [[float(len(text)), 1.0] for text in texts]

        def embed_query(self, text):
            return [float(len(text)), 1.0]

        callback = None

    item = secondary_mutations
    embed = Embedded()
    index = {"dims": 2, "embed": embed, "fields": ["text"], "ann_index_config": {"kind": "hnsw"}}
    dsn = dsn_with_search_path(item.private.database.postgres_url, item.private.database.postgres_schema)
    async with AsyncPostgresStore.from_conn_string(dsn, index=index, ttl={"default_ttl": 1, "refresh_on_read": True}) as trusted:
        await trusted.setup()
        await trusted.aput(("vector",), "existing", {"text": "baseline"})
    async with FencedAsyncPostgresStore.from_conn_string(dsn, index=index, ttl={"default_ttl": 1, "refresh_on_read": True}, mutation_capability=item.runs._mutation_capability, schema=item.private.database.postgres_schema) as store:
        await store.setup()
        item.vector_store = store
        item.embeddings = embed
        yield item


async def vector_rows(engine):
    async with engine.connect() as conn:
        return {table: tuple((await conn.execute(text("SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"))).scalars()) for table in ("store", "store_vectors")}


@pytest.mark.asyncio
async def test_vector_upsert_search_ttl_and_delete_use_real_tables(vector_mutations):
    item = vector_mutations
    await item.vector_store.aput(("vector",), "new", {"text": "actual vector"})
    assert len((await vector_rows(item.engine))["store_vectors"]) == 2
    assert len(await item.vector_store.asearch(("vector",), query="actual", refresh_ttl=False)) == 2
    await asyncio.to_thread(item.vector_store.delete, ("vector",), "new")
    assert len((await vector_rows(item.engine))["store_vectors"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["put", "search"])
async def test_late_embedding_result_revoked_before_actual_sql_cannot_mutate(vector_mutations, operation):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = vector_mutations
    calls = 0

    async def revoke():
        nonlocal calls
        calls += 1
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))

    item.embeddings.callback = revoke
    before = await vector_rows(item.engine)
    with pytest.raises(OwnershipRejected):
        if operation == "put":
            await item.vector_store.aput(("vector",), "new", {"text": "late vector"})
        else:
            await item.vector_store.asearch(("vector",), query="late", refresh_ttl=True)
    assert calls == 1 and await vector_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["store", "agent", "managed"])
@pytest.mark.parametrize("change", ["owner", "generation", "session", "active_attempt", "released", "stopped", "terminal"])
async def test_secondary_repositories_reject_all_changed_identity_rows(secondary_mutations, operation, change):
    from deerflow.persistence.managed_subagents.base import ManagedSubagentDefinition
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import CHANGES

    item = secondary_mutations
    async with item.engine.begin() as conn:
        await conn.execute(text(CHANGES[change]))
    before = await secondary_rows(item.engine)
    with pytest.raises(OwnershipRejected):
        if operation == "store":
            await item.state_store.aput(("generic",), "new", {"changed": True})
        elif operation == "agent":
            await asyncio.to_thread(item.agents.update, "bound-agent", {}, "changed", user_id=item.spec.user_id)
        else:
            await asyncio.to_thread(item.managed.update, ManagedSubagentDefinition(name="bound-managed", description="changed", system_prompt="changed"))
    assert await secondary_rows(item.engine) == before


@pytest.mark.asyncio
async def test_store_expiry_rechecked_after_actual_fleet_lock_wait(secondary_mutations):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked

    item = secondary_mutations
    cap = item.state_store._mutation_capability
    original = cap.validate_cursor
    pid_future = asyncio.get_running_loop().create_future()

    async def observed(cur, **kwargs):
        await cur.execute("SELECT pg_backend_pid() AS pid")
        pid_future.set_result((await cur.fetchone())["pid"])
        await original(cur, **kwargs)

    cap.validate_cursor = observed
    before = await secondary_rows(item.engine)
    async with item.engine.connect() as blocker:
        tx = await blocker.begin()
        await blocker.execute(text("SELECT id FROM fleet_agent_tasks FOR UPDATE"))
        writing = asyncio.create_task(item.state_store.aput(("any",), "new", {}))
        await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
        await blocker.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
        await blocker.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
        await tx.commit()
        with pytest.raises(OwnershipRejected):
            await writing
    assert await secondary_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["exception", "cancelled"])
async def test_vector_first_real_sql_failure_rolls_back_store_and_vector_rows(vector_mutations, failure):
    from contextlib import asynccontextmanager

    item = vector_mutations
    original = item.vector_store._cursor
    writes = 0

    class Cursor:
        def __init__(self, inner):
            self.inner = inner

        def __getattr__(self, key):
            return getattr(self.inner, key)

        async def execute(self, query, *args, **kwargs):
            nonlocal writes
            result = await self.inner.execute(query, *args, **kwargs)
            if str(query).lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                writes += 1
                if failure == "cancelled":
                    raise asyncio.CancelledError()
                raise RuntimeError("first actual Store SQL fault")
            return result

    @asynccontextmanager
    async def injected(**kwargs):
        async with original(**kwargs) as cur:
            yield Cursor(cur)

    item.vector_store._cursor = injected
    before = await vector_rows(item.engine)
    with pytest.raises(asyncio.CancelledError if failure == "cancelled" else RuntimeError):
        await item.vector_store.aput(("vector",), "new", {"text": "rollback vector"})
    assert writes == 1 and await vector_rows(item.engine) == before
    item.vector_store._cursor = original
    # Rollback also releases the execution locks and leaves the connection usable.
    await item.vector_store.aput(("vector",), "after", {"text": "after rollback"})


@pytest.mark.asyncio
@pytest.mark.parametrize("independent_guard", [False, True])
async def test_store_guard_and_actual_stock_sql_same_pid_txid_block_takeover(secondary_mutations, independent_guard):
    from contextlib import asynccontextmanager

    item = secondary_mutations
    cap = item.state_store._mutation_capability
    original_guard = cap.validate_cursor
    identities = []
    guarded = asyncio.Event()
    release = asyncio.Event()

    async def observed(cur, **kwargs):
        if independent_guard:
            from psycopg import AsyncConnection
            from psycopg.rows import dict_row

            from deerflow.persistence.postgres_schema import dsn_with_search_path

            dsn = dsn_with_search_path(item.private.database.postgres_url, item.private.database.postgres_schema)
            async with await AsyncConnection.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
                async with conn.transaction(), conn.cursor() as independent:
                    await original_guard(independent, **kwargs)
                    await independent.execute("SELECT pg_backend_pid() AS pid,txid_current() AS txid")
                    identities.append(await independent.fetchone())
        else:
            await original_guard(cur, **kwargs)
            await cur.execute("SELECT pg_backend_pid() AS pid,txid_current() AS txid")
            identities.append(await cur.fetchone())

    cap.validate_cursor = observed
    original_cursor = item.state_store._cursor

    class Cursor:
        def __init__(self, inner):
            self.inner = inner

        def __getattr__(self, key):
            return getattr(self.inner, key)

        async def execute(self, query, *args, **kwargs):
            if str(query).lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                await self.inner.execute("SELECT pg_backend_pid() AS pid,txid_current() AS txid")
                identities.append(await self.inner.fetchone())
                guarded.set()
                await release.wait()
            return await self.inner.execute(query, *args, **kwargs)

    @asynccontextmanager
    async def cursor(**kwargs):
        async with original_cursor(**kwargs) as inner:
            yield Cursor(inner)

    item.state_store._cursor = cursor
    writing = asyncio.create_task(item.state_store.aput(("generic",), "actual-sql", {}))
    await asyncio.wait_for(guarded.wait(), 5)
    async with item.engine.connect() as takeover:
        pid = (await takeover.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        changing = asyncio.create_task(takeover.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'")))
        try:

            async def actual_same_transaction_criterion():
                await require_takeover_blocked(item.engine, pid, changing)
                assert len(identities) == 2 and identities[0] == identities[1]

            if independent_guard:
                # Same positive gate must fail for a guard committed on another
                # connection before stock SQL, not merely observe changed rows.
                with pytest.raises((TimeoutError, AssertionError)):
                    await actual_same_transaction_criterion()
                assert identities[0] != identities[1]
                await changing
                await takeover.rollback()
            else:
                await actual_same_transaction_criterion()
        finally:
            release.set()
        await writing
        await changing
        await takeover.rollback()
    assert (await item.state_store.aget(("generic",), "actual-sql", refresh_ttl=False)).value == {}


@pytest.mark.asyncio
async def test_agent_integrity_retry_revalidates_original_context_in_new_transaction(secondary_mutations, monkeypatch):
    from sqlalchemy import event

    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = secondary_mutations
    loop = asyncio.get_running_loop()
    original_row = item.agents._row
    reads = 0

    def race_winner(session, *args, **kwargs):
        nonlocal reads
        reads += 1
        # Actual competing winner already exists: force the losing first
        # upsert to execute a real UNIQUE violation and rollback in PostgreSQL.
        return None if reads == 1 else original_row(session, *args, **kwargs)

    monkeypatch.setattr(item.agents, "_row", race_winner)

    async def revoke():
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))

    rollbacks = 0

    def after_rollback(session):
        nonlocal rollbacks
        rollbacks += 1
        asyncio.run_coroutine_threadsafe(revoke(), loop).result(5)

    session_type = item.agents._Session.class_
    event.listen(session_type, "after_rollback", after_rollback)
    before = await secondary_rows(item.engine)
    try:
        with pytest.raises(OwnershipRejected):
            await asyncio.to_thread(item.agents.update, "bound-agent", {}, "must not replace winner", user_id=item.spec.user_id)
    finally:
        event.remove(session_type, "after_rollback", after_rollback)
    assert rollbacks == 1 and reads == 1 and await secondary_rows(item.engine) == before


@pytest.mark.asyncio
async def test_remote_agent_delete_with_file_memory_rejected_before_sql_and_cleanup(secondary_mutations, tmp_path, monkeypatch):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = secondary_mutations
    directory = tmp_path / "file-memory"
    directory.mkdir()
    (directory / "memory.json").write_bytes(b"preserve original memory")
    monkeypatch.setattr("deerflow.persistence.agents.sql.get_paths", lambda: SimpleNamespace(user_agent_dir=lambda user, name: directory))
    before = await secondary_rows(item.engine)
    with pytest.raises(OwnershipRejected):
        await asyncio.to_thread(item.agents.delete, "bound-agent", user_id=item.spec.user_id)
    assert await secondary_rows(item.engine) == before
    assert (directory / "memory.json").read_bytes() == b"preserve original memory"


@pytest.mark.asyncio
async def test_definition_explicit_wrong_user_rejected_and_absent_ambient_stamped_original(secondary_mutations):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    item = secondary_mutations
    token = set_current_user(None)
    before = await secondary_rows(item.engine)
    try:
        with pytest.raises(OwnershipRejected):
            await asyncio.to_thread(item.agents.create, "wrong-owner", {}, "wrong", user_id="other-user")
        assert await secondary_rows(item.engine) == before
        await asyncio.to_thread(item.agents.create, "original-owner", {}, "positive", user_id=None)
        async with item.engine.connect() as conn:
            assert (await conn.execute(text("SELECT user_id FROM agents WHERE name='original-owner'"))).scalar_one() == item.spec.user_id
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_actual_installed_toolnode_propagates_definition_rejection(secondary_mutations):
    from langchain_core.messages import AIMessage
    from langgraph.graph import MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    from deerflow.persistence.agent_definition_context import agent_definition_store_scope
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.tools.builtins.setup_agent_tool import setup_agent

    item = secondary_mutations
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    graph = StateGraph(MessagesState, context_schema=dict)
    graph.add_node("tools", ToolNode([setup_agent]))
    graph.set_entry_point("tools")
    graph.set_finish_point("tools")
    compiled = graph.compile(store=item.state_store)
    before = await secondary_rows(item.engine)
    with agent_definition_store_scope(item.agents, item.managed), pytest.raises(OwnershipRejected):
        await compiled.ainvoke(
            {"messages": [AIMessage(content="", tool_calls=[{"name": "setup_agent", "args": {"soul": "must not write", "description": "bound"}, "id": "actual-tool", "type": "tool_call"}])]},
            context={"agent_name": "bound-agent", "user_id": item.spec.user_id},
        )
    assert await secondary_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["setup_agent", "update_agent"])
@pytest.mark.parametrize("stale", [False, True])
async def test_actual_toolnode_definition_positive_and_rejection_paths(secondary_mutations, tool_name, stale):
    from langchain_core.messages import AIMessage
    from langgraph.graph import MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    from deerflow.persistence.agent_definition_context import agent_definition_store_scope
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.tools.builtins.setup_agent_tool import setup_agent
    from deerflow.tools.builtins.update_agent_tool import update_agent

    item = secondary_mutations
    if stale:
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    tool = setup_agent if tool_name == "setup_agent" else update_agent
    graph = StateGraph(MessagesState, context_schema=dict)
    graph.add_node("tools", ToolNode([tool]))
    graph.set_entry_point("tools")
    graph.set_finish_point("tools")
    compiled = graph.compile(store=item.state_store)
    args = {"soul": "actual positive soul"}
    if tool_name == "setup_agent":
        args["description"] = "actual positive description"
    before = await secondary_rows(item.engine)
    with agent_definition_store_scope(item.agents, item.managed):
        running = compiled.ainvoke({"messages": [AIMessage(content="", tool_calls=[{"name": tool_name, "args": args, "id": "actual-tool", "type": "tool_call"}])]}, context={"agent_name": "bound-agent", "user_id": item.spec.user_id})
        if stale:
            with pytest.raises(OwnershipRejected):
                await running
            assert await secondary_rows(item.engine) == before
        else:
            result = await running
            assert result["messages"][-1].type == "tool"
            assert await asyncio.to_thread(item.agents.get_soul, "bound-agent", user_id=item.spec.user_id) == "actual positive soul"


@pytest.mark.asyncio
async def test_missing_definition_scope_never_constructs_unbound_fallback(secondary_mutations):
    from deerflow.persistence.agent_definition_context import _stores
    from deerflow.persistence.agents import get_agent_store
    from deerflow.persistence.managed_subagents import get_managed_subagent_store
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    token = _stores.set(None)
    try:
        for factory in (get_agent_store, get_managed_subagent_store):
            with pytest.raises(OwnershipRejected):
                factory()
    finally:
        _stores.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["opclass", "options", "dimensions", "missing_fk", "unvalidated_fk"])
async def test_vector_readonly_readiness_rejects_actual_index_and_schema_drift(vector_mutations, drift):
    item = vector_mutations
    async with item.engine.begin() as conn:
        if drift in {"opclass", "options"}:
            await conn.execute(text("DROP INDEX store_vectors_embedding_idx"))
            definition = "vector_l2_ops" if drift == "opclass" else "vector_cosine_ops"
            options = " WITH (m=8)" if drift == "options" else ""
            await conn.execute(text("CREATE INDEX store_vectors_embedding_idx ON store_vectors USING hnsw(embedding " + definition + ")" + options))
        elif drift == "dimensions":
            await conn.execute(text("TRUNCATE store_vectors"))
            await conn.execute(text("ALTER TABLE store_vectors ALTER COLUMN embedding TYPE vector(3)"))
        else:
            await conn.execute(text("ALTER TABLE store_vectors DROP CONSTRAINT store_vectors_prefix_key_fkey"))
            if drift == "unvalidated_fk":
                await conn.execute(text("ALTER TABLE store_vectors ADD CONSTRAINT store_vectors_prefix_key_fkey FOREIGN KEY(prefix,key) REFERENCES store(prefix,key) ON DELETE CASCADE NOT VALID"))
    before = await vector_rows(item.engine)
    with pytest.raises(RuntimeError, match="ready"):
        await item.vector_store.setup()
    assert await vector_rows(item.engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["setup_agent", "update_agent"])
@pytest.mark.parametrize("stale", [False, True])
async def test_actual_agent_model_tool_middleware_loop_stops_on_ownership_rejection(secondary_mutations, tool_name, stale):
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langchain_core.messages import AIMessage, HumanMessage

    from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
    from deerflow.persistence.agent_definition_context import agent_definition_store_scope
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.tools.builtins.setup_agent_tool import setup_agent
    from deerflow.tools.builtins.update_agent_tool import update_agent

    class Scripted(FakeMessagesListChatModel):
        calls: int = 0

        def bind_tools(self, tools, **kwargs):
            return self

        def _generate(self, *args, **kwargs):
            self.calls += 1
            return super()._generate(*args, **kwargs)

    item = secondary_mutations
    if stale:
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    args = {"soul": "actual agent-produced soul"}
    if tool_name == "setup_agent":
        args["description"] = "actual agent-produced description"
    model = Scripted(responses=[AIMessage(content="", tool_calls=[{"name": tool_name, "args": args, "id": "actual-model-tool", "type": "tool_call"}]), AIMessage(content="completed")])
    agent = create_agent(model, tools=[setup_agent if tool_name == "setup_agent" else update_agent], middleware=[ToolErrorHandlingMiddleware(app_config=item.private)], context_schema=dict, store=item.state_store)
    before = await secondary_rows(item.engine)
    with agent_definition_store_scope(item.agents, item.managed):
        invocation = agent.ainvoke({"messages": [HumanMessage(content="update the definition")]}, context={"agent_name": "bound-agent", "user_id": item.spec.user_id})
        if stale:
            with pytest.raises(OwnershipRejected):
                await invocation
            assert model.calls == 1 and await secondary_rows(item.engine) == before
        else:
            result = await invocation
            assert model.calls == 2 and result["messages"][-1].content == "completed"
            assert await asyncio.to_thread(item.agents.get_soul, "bound-agent", user_id=item.spec.user_id) == "actual agent-produced soul"


@pytest.mark.asyncio
@pytest.mark.parametrize("schema_exists", [False, True])
async def test_remote_store_factory_missing_readiness_does_not_create_schema_or_tables(secondary_mutations, schema_exists):
    from deerflow.runtime.store.async_provider import make_store

    item = secondary_mutations
    schema = item.private.database.postgres_schema + "_missing"
    private = item.private.model_copy(deep=True)
    private.database.postgres_schema = schema
    async with item.engine.begin() as conn:
        if schema_exists:
            await conn.execute(text('CREATE SCHEMA "' + schema + '"'))

    async def catalog():
        async with item.engine.connect() as conn:
            return tuple((await conn.execute(text("SELECT n.nspname,c.relname,c.relkind FROM pg_namespace n LEFT JOIN pg_class c ON c.relnamespace=n.oid WHERE n.nspname=:schema ORDER BY c.relname"), {"schema": schema})).all())

    before = await catalog()
    try:
        with pytest.raises(RuntimeError, match="ready"):
            async with make_store(private, mutation_capability=item.runs._mutation_capability):
                pass
        assert await catalog() == before
    finally:
        if schema_exists:
            async with item.engine.begin() as conn:
                await conn.execute(text('DROP SCHEMA "' + schema + '" CASCADE'))


@pytest.mark.asyncio
async def test_remote_pool_constructor_preserves_original_context_and_explicit_transactions(secondary_mutations):
    from deerflow.persistence.postgres_schema import dsn_with_search_path
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.runtime.store.fenced_store import FencedAsyncPostgresStore

    item = secondary_mutations
    dsn = dsn_with_search_path(item.private.database.postgres_url, item.private.database.postgres_schema)
    async with FencedAsyncPostgresStore.from_conn_string(dsn, mutation_capability=item.runs._mutation_capability, schema=item.private.database.postgres_schema, pool_config={"min_size": 1, "max_size": 2}) as store:
        await store.setup()
        await store.aput(("pool",), "positive", {})
        assert (await asyncio.to_thread(store.get, ("pool",), "positive", refresh_ttl=False)).value == {}
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
        before = await secondary_rows(item.engine)
        with pytest.raises(OwnershipRejected):
            await asyncio.to_thread(store.put, ("pool",), "rejected", {})
        assert await secondary_rows(item.engine) == before
    with pytest.raises(OwnershipRejected):
        async with FencedAsyncPostgresStore.from_conn_string(dsn, mutation_capability=item.runs._mutation_capability, pipeline=True):
            pass


@pytest.mark.asyncio
@pytest.mark.parametrize("error_kind", ["ownership", "cancelled"])
async def test_actual_worker_stream_pending_preserves_original_error_and_blocks_recovery(mutations, error_kind):
    from unittest.mock import AsyncMock

    from langchain_core.messages import HumanMessage
    from langgraph.config import get_stream_writer
    from langgraph.graph import StateGraph

    from deerflow.agents.thread_state import ThreadState
    from deerflow.runtime.execution.mutation_context import ExecutionCleanupPending, OwnershipRejected
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent

    item = mutations
    await item.writer.adelete_thread(item.spec.thread_id)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='pending'"))
    manager = RunManager(store=item.runs, worker_id=item.identity.owner_worker_id)
    record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
    original = OwnershipRejected("original stream rejection") if error_kind == "ownership" else asyncio.CancelledError("original stream cancellation")
    marker = ExecutionCleanupPending()
    streams = []

    async def answer(state):
        get_stream_writer()({"owned-step": True})
        await asyncio.Event().wait()

    async def publish(run_id, event, payload):
        if event == "custom":
            raise original

    async def settle(stream):
        streams.append(stream)
        # Only test the neutral marker handoff here; genuine retained SQL Task
        # and physical deadline behavior are exercised by the host/process cases.
        await stream.aclose()
        raise marker

    graph = StateGraph(ThreadState)
    graph.add_node("answer", answer)
    graph.set_entry_point("answer")
    graph.set_finish_point("answer")
    compiled = graph.compile()
    bridge = SimpleNamespace(publish=publish, publish_end=AsyncMock(), cleanup=AsyncMock())
    with pytest.raises(type(original)) as caught:
        await run_agent(
            bridge,
            manager,
            record,
            ctx=RunContext(checkpointer=None, app_config=item.private, settle_stream=settle),
            agent_factory=lambda *, config: compiled,
            graph_input={"messages": [HumanMessage(content="stream identity")]},
            config={"configurable": {"thread_id": item.spec.thread_id}},
            stream_modes=["custom"],
        )
    assert caught.value is original and caught.value.__cause__ is marker
    assert marker.original_error is original
    assert len(streams) == 1 and streams[0].ag_frame is None
    assert record.ownership_lost and not record.finalizing
    async with item.engine.connect() as conn:
        assert (await conn.execute(text("SELECT status FROM runs"))).scalar_one() == "running"
    bridge.publish_end.assert_awaited_once_with(record.run_id)
    bridge.cleanup.assert_awaited_once_with(record.run_id, delay=60)
