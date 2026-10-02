"""Actual scheduler two-transaction completion and MCP tracking fences."""

import asyncio
import inspect
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import owner_environment as owner_environment
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c06_remote_agent_runtime import mutations as mutations


def bound(cls, item):
    kwargs = {"mutation_capability": item.capability} if "mutation_capability" in inspect.signature(cls).parameters else {}
    return cls(item.env[1], **kwargs)


@pytest_asyncio.fixture
async def tracking(checkpoint_owner):
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.persistence.mcp_tasks.model import McpTaskRow
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository
    from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
    from deerflow.persistence.scheduled_task_runs.sql import ScheduledTaskRunRepository
    from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow
    from deerflow.persistence.scheduled_tasks.sql import ScheduledTaskRepository
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = checkpoint_owner
    item.capability = FleetMutationCapability(item.identity, item.spec)
    async with item.engine.begin() as conn:
        await conn.run_sync(lambda sync: ScheduledTaskRow.__table__.create(sync))
        await conn.run_sync(lambda sync: ScheduledTaskRunRow.__table__.create(sync))
        await conn.run_sync(lambda sync: McpTaskRow.__table__.create(sync))
        await conn.execute(text("UPDATE runs SET status='running', metadata_json='{}'"))
    local = ScheduledTaskRepository(item.env[1])
    await local.create(
        task_id="scheduled", user_id=item.spec.user_id, thread_id=item.spec.thread_id, context_mode="reuse_thread", assistant_id=None, title="Scheduled", prompt="run", schedule_type="once", schedule_spec={}, timezone="UTC", next_run_at=None
    )
    await ScheduledTaskRunRepository(item.env[1]).create(run_record_id="occurrence", task_id="scheduled", thread_id=item.spec.thread_id, scheduled_for=datetime.now(UTC), trigger="scheduled", status="running")
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE scheduled_tasks SET last_run_id=:r,last_thread_id=:t,status='running'"), {"r": item.spec.run_id, "t": item.spec.thread_id})
        await conn.execute(text("UPDATE scheduled_task_runs SET run_id=:r"), {"r": item.spec.run_id})
        await conn.execute(text("UPDATE runs SET metadata_json=:m"), {"m": '{"scheduled_task_id":"scheduled","scheduled_task_run_id":"occurrence"}'})
    item.tasks = bound(ScheduledTaskRepository, item)
    item.occurrences = bound(ScheduledTaskRunRepository, item)
    item.mcp = bound(McpTaskRepository, item)
    with remote_mutation_scope(item.capability.context):
        yield item


async def rows(item):
    async with item.engine.connect() as conn:
        return {table: list((await conn.execute(text("SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"))).scalars()) for table in ("scheduled_tasks", "scheduled_task_runs", "mcp_tasks")}


async def complete(item):
    from app.scheduler.service import ScheduledTaskService

    service = ScheduledTaskService(task_repo=item.tasks, task_run_repo=item.occurrences, launch_run=None, poll_interval_seconds=1, lease_seconds=30, max_concurrent_runs=1)
    record = SimpleNamespace(
        metadata={"scheduled_task_id": "scheduled", "scheduled_task_run_id": "occurrence"}, user_id=item.spec.user_id, thread_id=item.spec.thread_id, run_id=item.spec.run_id, status=SimpleNamespace(value="success"), error=None
    )
    await service.handle_run_completion(record)


async def create_mcp(item):
    return await item.mcp.create(
        task_id="mcp-original",
        user_id=item.spec.user_id,
        thread_id=item.spec.thread_id,
        run_id=item.spec.run_id,
        tool_call_id="tool",
        server_name="server",
        driver_name="driver",
        remote_task_id="remote",
        task_name="task",
        status="working",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["scheduler", "mcp-create", "mcp-cancel"])
async def test_actual_tracking_writes_reject_revoked_original_owner(tracking, operation):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = tracking
    if operation == "mcp-cancel":
        await create_mcp(item)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
        if operation == "scheduler":
            await conn.execute(text("UPDATE runs SET status='success'"))
    before = await rows(item)
    try:
        if operation == "scheduler":
            await complete(item)
        elif operation == "mcp-create":
            await create_mcp(item)
        else:
            await item.mcp.request_cancel("mcp-original", user_id=item.spec.user_id, thread_id=item.spec.thread_id, requested_at=datetime.now(UTC))
    except OwnershipRejected:
        rejected = True
    else:
        rejected = False
    assert await rows(item) == before
    assert rejected


@pytest.mark.asyncio
async def test_actual_scheduler_completion_is_legal_and_both_transactions_commit(tracking):
    item = tracking
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
    await complete(item)
    async with item.engine.connect() as conn:
        assert (await conn.execute(text("SELECT status FROM scheduled_task_runs"))).scalar_one() == "success"
        assert (await conn.execute(text("SELECT status FROM scheduled_tasks"))).scalar_one() == "completed"


@pytest.mark.asyncio
async def test_scheduler_revocation_between_actual_completion_transactions(tracking, monkeypatch):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = tracking
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
    original = item.occurrences.update_status

    async def revoke_after_occurrence(*args, **kwargs):
        result = await original(*args, **kwargs)
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE runs SET owner_worker_id='replacement'"))
        return result

    monkeypatch.setattr(item.occurrences, "update_status", revoke_after_occurrence)
    with pytest.raises(OwnershipRejected):
        await complete(item)
    async with item.engine.connect() as conn:
        assert (await conn.execute(text("SELECT status FROM scheduled_task_runs"))).scalar_one() == "success"
        assert (await conn.execute(text("SELECT status FROM scheduled_tasks"))).scalar_one() == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("association", ["core-metadata", "occurrence-run", "occurrence-task", "occurrence-thread", "parent-user", "parent-run", "parent-thread", "terminal-status", "terminal-error"])
async def test_actual_scheduler_completion_wrong_association_or_outcome_has_no_effect(tracking, association):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = tracking
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
        changes = {
            "core-metadata": "UPDATE runs SET metadata_json='{}'",
            "occurrence-run": "UPDATE scheduled_task_runs SET run_id='other'",
            "occurrence-task": "UPDATE scheduled_task_runs SET task_id='other'",
            "occurrence-thread": "UPDATE scheduled_task_runs SET thread_id='other'",
            "parent-user": "UPDATE scheduled_tasks SET user_id='other'",
            "parent-run": "UPDATE scheduled_tasks SET last_run_id='other'",
            "parent-thread": "UPDATE scheduled_tasks SET last_thread_id='other'",
            "terminal-status": "UPDATE runs SET status='error',error='real error'",
            "terminal-error": "UPDATE scheduled_task_runs SET status='success',error='conflict'",
        }
        await conn.execute(text(changes[association]))
    before = await rows(item)
    with pytest.raises(OwnershipRejected):
        await complete(item)
    assert await rows(item) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource,method,kwargs",
    [
        ("tasks", "release_queued_admission_lease", {"task_id": "scheduled"}),
        ("tasks", "delete", {"task_id": "scheduled", "user_id": "user-c02"}),
        ("tasks", "cancel_stuck_once_tasks", {"error": "bad"}),
        ("occurrences", "mark_stale_active_runs", {"error": "bad"}),
        ("occurrences", "recover_expired_launch_claims", {"error": "bad", "now": datetime.now(UTC)}),
        ("mcp", "claim_due_tasks", {"lease_owner": "host", "now": datetime.now(UTC), "lease_seconds": 30, "limit": 10}),
    ],
)
async def test_runner_cannot_call_host_scheduler_or_mcp_mutations(tracking, resource, method, kwargs):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = tracking
    before = await rows(item)
    with pytest.raises(OwnershipRejected):
        await getattr(getattr(item, resource), method)(**kwargs)
    assert await rows(item) == before


@pytest.mark.asyncio
async def test_mcp_original_tracking_cancel_and_duplicate_binding(tracking):
    item = tracking
    created = await create_mcp(item)
    cancelled = await item.mcp.request_cancel(created["id"], user_id=item.spec.user_id, thread_id=item.spec.thread_id, requested_at=datetime.now(UTC))
    assert cancelled["cancel_requested_at"] is not None
    assert len((await rows(item))["mcp_tasks"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["missing", "wrong-context", "user", "thread", "run", "persisted-run"])
async def test_mcp_cancel_or_create_wrong_original_context_has_no_effect(tracking, identity):
    from dataclasses import replace

    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = tracking
    await create_mcp(item)
    if identity == "persisted-run":
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE mcp_tasks SET run_id='other'"))
    before = await rows(item)
    token = _current_mutation_context.set(None if identity == "missing" else replace(item.capability.context, attempt_id="wrong") if identity == "wrong-context" else item.capability.context)
    try:
        with pytest.raises(OwnershipRejected):
            if identity == "run":
                await item.mcp.create_idempotent(
                    task_id="other",
                    user_id=item.spec.user_id,
                    thread_id=item.spec.thread_id,
                    run_id="other",
                    tool_call_id=None,
                    server_name="server",
                    driver_name="driver",
                    remote_task_id="other",
                    task_name="task",
                    status="working",
                    result=None,
                    result_preview=None,
                    result_truncated=False,
                    result_artifact=None,
                    error=None,
                    input_required=None,
                    next_poll_at=None,
                )
            else:
                await item.mcp.request_cancel("mcp-original", user_id="other" if identity == "user" else item.spec.user_id, thread_id="other" if identity == "thread" else item.spec.thread_id, requested_at=datetime.now(UTC))
    finally:
        _current_mutation_context.reset(token)
    assert await rows(item) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["valid", "revoked", "missing", "wrong", "late-revoke"])
async def test_actual_mcp_background_tool_private_submitter_service_repository(tracking, identity):
    from dataclasses import replace

    from langchain_core.tools import StructuredTool
    from pydantic import BaseModel

    from app.mcp_tasks.service import McpTaskService
    from deerflow.config.extensions_config import ExtensionsConfig
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSnapshot, TaskStatus, TaskSubmission
    from deerflow.mcp.tasks.runtime import mcp_task_submitter_scope, set_mcp_task_submitter
    from deerflow.mcp.tools import _make_background_submit_tool
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = tracking
    entered, release = __import__("asyncio").Event(), __import__("asyncio").Event()
    calls = []

    class Driver:
        async def submit(self, request):
            calls.append(request)
            if identity == "late-revoke":
                entered.set()
                await release.wait()
            return TaskSubmission(remote_task_id="actual-external-handle", snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED))

    class LocalSubmitter:
        async def submit(self, **kw):
            raise AssertionError("Local unbound global submitter leaked into remote")

    class Args(BaseModel):
        value: str

    drivers = McpTaskDriverRegistry()
    drivers.register("ordinary-tools", Driver())
    service = McpTaskService(repository=item.mcp, drivers=drivers, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    raw = StructuredTool(name="actual_submit", description="Submit", args_schema=Args, coroutine=lambda **kw: None)
    tool = _make_background_submit_tool(raw, server_name="configured", task_name="actual", submit_tool="submit", status_tool="status", cancel_tool="cancel")
    runtime = SimpleNamespace(context={"user_id": item.spec.user_id, "thread_id": item.spec.thread_id, "run_id": item.spec.run_id}, config={}, tool_call_id="call")
    set_mcp_task_submitter(LocalSubmitter())
    if identity == "revoked":
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE runs SET owner_worker_id='other'"))
    before = await rows(item)
    try:
        with mcp_task_submitter_scope(service, ExtensionsConfig()):
            token = _current_mutation_context.set(None if identity == "missing" else replace(item.capability.context, attempt_id="other") if identity == "wrong" else item.capability.context)
            try:
                if identity == "late-revoke":
                    task = __import__("asyncio").create_task(tool.coroutine(runtime=runtime, value="external"))
                    await __import__("asyncio").wait_for(entered.wait(), 10)
                    async with item.engine.begin() as conn:
                        await conn.execute(text("UPDATE runs SET owner_worker_id='other'"))
                    release.set()
                    with pytest.raises(OwnershipRejected):
                        await task
                elif identity != "valid":
                    with pytest.raises(OwnershipRejected):
                        await tool.coroutine(runtime=runtime, value="external")
                else:
                    result = await tool.coroutine(runtime=runtime, value="external")
                    assert result["status"] == "submitted"
            finally:
                _current_mutation_context.reset(token)
    finally:
        set_mcp_task_submitter(None)
        await service.stop()
    if identity == "valid":
        assert len((await rows(item))["mcp_tasks"]) == 1
    else:
        assert await rows(item) == before
    if identity in {"missing", "wrong"}:
        assert calls == []


@pytest.mark.asyncio
async def test_mcp_idempotent_conflict_never_returns_other_users_persisted_row(tracking):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = tracking
    original = await create_mcp(item)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE mcp_tasks SET user_id='other'"))
    values = {key: original[key] for key in ("thread_id", "run_id", "server_name", "driver_name", "remote_task_id", "task_name", "status", "result", "result_preview", "result_truncated", "result_artifact", "error", "input_required")}
    values.update(task_id=original["id"], user_id=item.spec.user_id, tool_call_id="tool", next_poll_at=None)
    before = await rows(item)
    with pytest.raises(OwnershipRejected):
        await item.mcp.create_idempotent(**values)
    assert await rows(item) == before


async def prepare_tracking_transaction(item, operation):
    if operation.startswith("scheduler"):
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE runs SET status='success'"))
            if operation == "scheduler-parent":
                await conn.execute(text("UPDATE scheduled_task_runs SET status='success'"))
    elif operation == "mcp-cancel":
        await create_mcp(item)


async def tracking_transaction(item, operation):
    if operation == "scheduler-occurrence":
        return await item.occurrences.update_status("occurrence", status="success", run_id=item.spec.run_id, finished_at=datetime.now(UTC), completion_task_id="scheduled")
    if operation == "scheduler-parent":
        return await item.tasks.update("scheduled", user_id=item.spec.user_id, updates={"last_error": None, "status": "completed"}, completion_run_id=item.spec.run_id, completion_occurrence_id="occurrence")
    if operation == "mcp-create":
        return await create_mcp(item)
    return await item.mcp.request_cancel("mcp-original", user_id=item.spec.user_id, thread_id=item.spec.thread_id, requested_at=datetime.now(UTC))


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["scheduler-occurrence", "scheduler-parent", "mcp-create", "mcp-cancel"])
async def test_actual_tracking_guard_writer_same_pid_txid_holds_locks_until_commit(tracking, operation):
    from sqlalchemy import event

    from .test_c05_remote_agent_runtime import wait_blocked

    item = tracking
    await prepare_tracking_transaction(item, operation)
    ready, release = asyncio.Event(), asyncio.Event()
    guarded, written = [], []
    original = item.capability.validate_async
    prefix = {"scheduler-occurrence": "UPDATE SCHEDULED_TASK_RUNS", "scheduler-parent": "UPDATE SCHEDULED_TASKS", "mcp-create": "INSERT INTO MCP_TASKS", "mcp-cancel": "UPDATE MCP_TASKS"}[operation]

    async def observed(session, **kwargs):
        await original(session, **kwargs)
        guarded.append(tuple((await session.execute(text("SELECT pg_backend_pid(),txid_current()"))).one()))
        ready.set()
        await asyncio.wait_for(release.wait(), 10)

    def sql(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith(prefix):
            written.append(tuple(conn.exec_driver_sql("SELECT pg_backend_pid(),txid_current()").one()))

    item.capability.validate_async = observed
    event.listen(item.engine.sync_engine, "before_cursor_execute", sql)
    try:
        writing = asyncio.create_task(tracking_transaction(item, operation))
        await asyncio.wait_for(ready.wait(), 5)
        async with item.engine.connect() as takeover:
            tx = await takeover.begin()
            pid = (await takeover.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            changing = asyncio.create_task(takeover.execute(text("UPDATE fleet_agent_tasks SET state='paused'")))
            await wait_blocked(item.engine, pid)
            assert not changing.done()
            release.set()
            await writing
            await changing
            await tx.rollback()
    finally:
        release.set()
        item.capability.validate_async = original
        event.remove(item.engine.sync_engine, "before_cursor_execute", sql)
    assert guarded and written and all(identity == guarded[0] for identity in written)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["scheduler-occurrence", "scheduler-parent", "mcp-create", "mcp-cancel"])
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_actual_tracking_first_sql_failure_rolls_back_original_tables(tracking, operation, failure):
    from sqlalchemy import event

    item = tracking
    await prepare_tracking_transaction(item, operation)
    before = await rows(item)
    prefix = {"scheduler-occurrence": "UPDATE SCHEDULED_TASK_RUNS", "scheduler-parent": "UPDATE SCHEDULED_TASKS", "mcp-create": "INSERT INTO MCP_TASKS", "mcp-cancel": "UPDATE MCP_TASKS"}[operation]
    writes = []

    def failed(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith(prefix):
            writes.append(statement)
            raise failure("first actual tracking SQL completed")

    event.listen(item.engine.sync_engine, "after_cursor_execute", failed)
    try:
        with pytest.raises(failure):
            await tracking_transaction(item, operation)
    finally:
        event.remove(item.engine.sync_engine, "after_cursor_execute", failed)
    assert len(writes) == 1 and await rows(item) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["scheduler-occurrence", "scheduler-parent", "mcp-create", "mcp-cancel"])
async def test_actual_tracking_lock_wait_rechecks_fresh_clock(tracking, operation):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked

    item = tracking
    await prepare_tracking_transaction(item, operation)
    before = await rows(item)
    pid_future = asyncio.get_running_loop().create_future()
    original = item.capability.validate_async

    async def observed(session, **kwargs):
        pid_future.set_result((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        await original(session, **kwargs)

    item.capability.validate_async = observed
    try:
        async with item.engine.connect() as blocker:
            tx = await blocker.begin()
            await blocker.execute(text("SELECT id FROM fleet_agent_tasks FOR UPDATE"))
            writing = asyncio.create_task(tracking_transaction(item, operation))
            await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
            await blocker.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
            await blocker.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
            await tx.commit()
            with pytest.raises(OwnershipRejected):
                await writing
    finally:
        item.capability.validate_async = original
    assert await rows(item) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", [False, True])
async def test_actual_toolnode_preserves_private_mcp_runtime_to_bound_service(tracking, stale):
    from langchain_core.messages import AIMessage
    from langchain_core.tools import StructuredTool
    from langgraph.graph import MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode
    from pydantic import BaseModel

    from app.mcp_tasks.service import McpTaskService
    from deerflow.config.extensions_config import ExtensionsConfig
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSnapshot, TaskStatus, TaskSubmission
    from deerflow.mcp.tasks.runtime import mcp_task_submitter_scope
    from deerflow.mcp.tools import _make_background_submit_tool
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = tracking
    calls = []

    class Driver:
        async def submit(self, request):
            calls.append(request)
            assert (request.user_id, request.thread_id, request.run_id) == (item.spec.user_id, item.spec.thread_id, item.spec.run_id)
            return TaskSubmission(remote_task_id="toolnode-external-handle", snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED))

    class Args(BaseModel):
        value: str

    drivers = McpTaskDriverRegistry()
    drivers.register("ordinary-tools", Driver())
    service = McpTaskService(repository=item.mcp, drivers=drivers, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    raw = StructuredTool(name="actual_submit", description="Submit", args_schema=Args, coroutine=lambda **kw: None)
    tool = _make_background_submit_tool(raw, server_name="configured", task_name="actual", submit_tool="submit", status_tool="status", cancel_tool="cancel")
    from deerflow.tools.sync import make_sync_tool_wrapper

    tool.func = make_sync_tool_wrapper(tool.coroutine, tool.name)
    assert "runtime" not in tool.tool_call_schema.model_json_schema()["properties"]
    graph = StateGraph(MessagesState, context_schema=dict)
    graph.add_node("tools", ToolNode([tool]))
    graph.set_entry_point("tools")
    graph.set_finish_point("tools")
    compiled = graph.compile()
    if stale:
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE runs SET owner_worker_id='other'"))
    before = await rows(item)
    with mcp_task_submitter_scope(service, ExtensionsConfig()):
        kwargs = {"context": {"user_id": item.spec.user_id, "thread_id": item.spec.thread_id, "run_id": item.spec.run_id}}
        state = {"messages": [AIMessage(content="", tool_calls=[{"id": "actual-call", "name": tool.name, "args": {"value": "external"}, "type": "tool_call"}])]}
        if stale:
            with pytest.raises(OwnershipRejected):
                await compiled.ainvoke(state, **kwargs)
        else:
            result = await compiled.ainvoke(state, **kwargs)
            assert "submitted" in result["messages"][-1].content
    await service.stop()
    if stale:
        assert await rows(item) == before
    else:
        assert len(calls) == 1 and len((await rows(item))["mcp_tasks"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_status", ["failed", "cancelled", "completed"])
async def test_actual_scheduler_cannot_replace_contradictory_terminal_parent(tracking, parent_status):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = tracking
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
        await conn.execute(text("UPDATE scheduled_task_runs SET status='success',error=NULL"))
        await conn.execute(text("UPDATE scheduled_tasks SET status=:s,last_error='other terminal outcome'"), {"s": parent_status})
    before = await rows(item)
    with pytest.raises(OwnershipRejected):
        await complete(item)
    assert await rows(item) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("domain", ["occurrence", "parent", "mcp-create", "mcp-cancel"])
async def test_actual_tracking_target_wait_expiry_cannot_commit(tracking, domain):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked

    item = tracking
    if domain == "mcp-cancel":
        await create_mcp(item)
    async with item.engine.begin() as conn:
        if domain in {"occurrence", "parent"}:
            await conn.execute(text("UPDATE runs SET status='success'"))
        if domain == "parent":
            await conn.execute(text("UPDATE scheduled_task_runs SET status='success',error=NULL"))
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    before = await rows(item)
    pid_future = asyncio.get_running_loop().create_future()
    original = item.capability.validate_async

    async def observed(session, **kwargs):
        if not pid_future.done():
            pid_future.set_result((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        return await original(session, **kwargs)

    item.capability.validate_async = observed
    try:
        async with item.engine.connect() as blocker:
            tx = await blocker.begin()
            table = "scheduled_task_runs" if domain == "occurrence" else "scheduled_tasks" if domain == "parent" else "mcp_tasks"
            await blocker.execute(text("LOCK TABLE mcp_tasks IN ACCESS EXCLUSIVE MODE") if domain == "mcp-create" else text("SELECT * FROM " + table + " FOR UPDATE"))
            operation = (
                item.occurrences.update_status("occurrence", status="success", run_id=item.spec.run_id, finished_at=datetime.now(UTC), completion_task_id="scheduled")
                if domain == "occurrence"
                else item.tasks.update("scheduled", user_id=item.spec.user_id, updates={"status": "completed", "last_error": None}, completion_run_id=item.spec.run_id, completion_occurrence_id="occurrence")
                if domain == "parent"
                else create_mcp(item)
                if domain == "mcp-create"
                else item.mcp.request_cancel("mcp-original", user_id=item.spec.user_id, thread_id=item.spec.thread_id, requested_at=datetime.now(UTC))
            )
            writing = asyncio.create_task(operation)
            try:
                await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
                async with asyncio.timeout(5):
                    while not (await blocker.execute(text("SELECT clock_timestamp() >= lease_expires_at FROM runs"))).scalar_one():
                        await asyncio.sleep(0)
                await tx.rollback()
                with pytest.raises(OwnershipRejected):
                    await writing
            finally:
                if tx.is_active:
                    await tx.rollback()
                await asyncio.gather(writing, return_exceptions=True)
    finally:
        item.capability.validate_async = original
    assert await rows(item) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("idempotent", [False, True])
async def test_actual_mcp_idempotent_unique_conflict_wait_expiry_rejects_original(tracking, idempotent):
    from deerflow.persistence.mcp_tasks.model import McpTaskRow
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked

    item = tracking
    await create_mcp(item)
    async with item.engine.begin() as conn:
        template = dict((await conn.execute(text("SELECT * FROM mcp_tasks"))).mappings().one())
        await conn.execute(text("DELETE FROM mcp_tasks"))
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    values = {name: template[name] for name in inspect.signature(item.mcp.create).parameters if name in template}
    values["task_id"] = "mcp-contender"
    original = item.capability.validate_async
    pid_future = asyncio.get_running_loop().create_future()

    async def observed(session, **kwargs):
        if not pid_future.done():
            pid_future.set_result((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        return await original(session, **kwargs)

    item.capability.validate_async = observed
    try:
        async with item.engine.connect() as peer:
            tx = await peer.begin()
            await peer.execute(McpTaskRow.__table__.insert().values(**template))
            writing = asyncio.create_task((item.mcp.create_idempotent if idempotent else item.mcp.create)(**values))
            try:
                await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
                async with asyncio.timeout(5):
                    while not (await peer.execute(text("SELECT clock_timestamp() >= lease_expires_at FROM runs"))).scalar_one():
                        await asyncio.sleep(0)
                await tx.commit()
                expected_peer_rows = await rows(item)
                with pytest.raises(OwnershipRejected):
                    await writing
            finally:
                if tx.is_active:
                    await tx.rollback()
                await asyncio.gather(writing, return_exceptions=True)
    finally:
        item.capability.validate_async = original
    assert await rows(item) == expected_peer_rows


@pytest.mark.asyncio
@pytest.mark.parametrize("single_mode", [False, True])
async def test_real_worker_full_agent_preserves_mcp_original_runtime(mutations, tracking, single_mode):
    from unittest.mock import AsyncMock

    from langchain.agents import create_agent
    from langchain_core.language_models import BaseChatModel
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langchain_core.outputs import ChatGeneration, ChatResult
    from langchain_core.tools import StructuredTool
    from pydantic import BaseModel

    from app.mcp_tasks.service import McpTaskService
    from deerflow.agents.thread_state import ThreadState
    from deerflow.config.extensions_config import ExtensionsConfig
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSnapshot, TaskStatus, TaskSubmission
    from deerflow.mcp.tasks.runtime import mcp_task_submitter_scope
    from deerflow.mcp.tools import _make_background_submit_tool
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent

    item = tracking
    await item.writer.adelete_thread(item.spec.thread_id)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='pending'"))
    manager = RunManager(store=item.runs, worker_id=item.identity.owner_worker_id)
    record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
    calls = []

    class Driver:
        async def submit(self, request):
            calls.append(request)
            return TaskSubmission(remote_task_id="worker-handle", snapshot=TaskSnapshot(status=TaskStatus.SUBMITTED))

    class Args(BaseModel):
        value: str

    drivers = McpTaskDriverRegistry()
    drivers.register("ordinary-tools", Driver())
    service = McpTaskService(repository=item.mcp, drivers=drivers, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    raw = StructuredTool(name="actual_submit", description="Submit", args_schema=Args, coroutine=lambda **kw: None)
    tool = _make_background_submit_tool(raw, server_name="configured", task_name="actual", submit_tool="submit", status_tool="status", cancel_tool="cancel")
    from deerflow.tools.sync import make_sync_tool_wrapper

    tool.func = make_sync_tool_wrapper(tool.coroutine, tool.name)

    class Model(BaseChatModel):
        @property
        def _llm_type(self):
            return "worker-context-test"

        def bind_tools(self, tools, **kwargs):
            return self

        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            response = (
                AIMessage(content="done") if any(isinstance(message, ToolMessage) for message in messages) else AIMessage(content="", tool_calls=[{"id": "worker-tool", "name": tool.name, "args": {"value": "external"}, "type": "tool_call"}])
            )
            return ChatResult(generations=[ChatGeneration(message=response)])

    compiled = create_agent(model=Model(), tools=[tool], state_schema=ThreadState)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    with mcp_task_submitter_scope(service, ExtensionsConfig()):
        await run_agent(
            bridge,
            manager,
            record,
            ctx=RunContext(checkpointer=item.writer, event_store=item.events, thread_store=item.threads, app_config=item.private),
            agent_factory=lambda *, config: compiled,
            graph_input={"messages": [HumanMessage(content="submit real durable task")]},
            config={"configurable": {"thread_id": item.spec.thread_id}, "context": {"user_id": item.spec.user_id}},
            stream_modes=["values"] if single_mode else ["values", "messages-tuple", "custom"],
            stream_subgraphs=True,
        )
    await service.stop()
    assert not record.ownership_lost
    assert record.status.value == "success"
    assert len(calls) == 1 and calls[0].run_id == item.spec.run_id
    assert len((await rows(item))["mcp_tasks"]) == 1
