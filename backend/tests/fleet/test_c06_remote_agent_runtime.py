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
    await writing
    assert (await durable_rows(item.engine))["run_events"] != before["run_events"]


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
async def test_actual_worker_subagent_rejection_fences_and_completes_cleanup(mutations, trigger, monkeypatch):
    from unittest.mock import AsyncMock

    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.config import get_stream_writer
    from langgraph.graph import StateGraph

    from deerflow.agents.thread_state import ThreadState
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, _SubagentEventBuffer, run_agent

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
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=item.writer, event_store=item.events, thread_store=item.threads, app_config=item.private),
        agent_factory=lambda *, config: compiled,
        graph_input={"messages": [HumanMessage(content="subagent stream")]},
        config={"configurable": {"thread_id": item.spec.thread_id}},
        stream_modes=["custom"],
    )
    await asyncio.sleep(0)
    assert rejected_calls == 1 and before is not None and not rejected_flush_returned
    assert record.ownership_lost and record.status.value == "error" and not record.finalizing
    assert after_rejection_finalization == 0 and await durable_rows(item.engine) == before
    bridge.publish_end.assert_awaited_once_with(record.run_id)
    bridge.cleanup.assert_awaited_once_with(record.run_id, delay=60)
