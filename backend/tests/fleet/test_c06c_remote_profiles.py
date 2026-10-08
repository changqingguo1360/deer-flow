"""Configured real MemoryManager and extension entrypoint acceptance."""

import asyncio
import inspect
import threading
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import owner_environment as owner_environment
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c06_remote_agent_runtime import mutations as mutations
from .test_c06_remote_agent_runtime import secondary_mutations as secondary_mutations
from .test_c08_terminal_pair import prepared_pair as prepared_pair


@pytest_asyncio.fixture
async def memory_profile(checkpoint_owner, monkeypatch):
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.agents.memory import manager as factory
    from deerflow.config.memory_config import MemoryConfig
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = checkpoint_owner
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='running'"))
        await conn.execute(text("CREATE TABLE c06_memory(user_id text NOT NULL, agent_name text NOT NULL, fact_id text NOT NULL, content text NOT NULL, PRIMARY KEY(user_id,agent_name,fact_id))"))
        schema = (await conn.execute(text("SELECT current_schema()"))).scalar_one()
    engine = create_engine(item.engine.url.render_as_string(hide_password=False).replace("+asyncpg", "+psycopg"), connect_args={"options": "-csearch_path=" + schema})
    item.sync_sf = sessionmaker(engine)
    item.capability = FleetMutationCapability(item.identity, item.spec)
    monkeypatch.syspath_prepend(str(Path(__file__).parent / "fixtures/c04-runtime-plugin"))
    config = MemoryConfig(manager_class="deerflow_c04_fixture.memory:PostgresMemory")
    monkeypatch.setattr(factory, "get_memory_config", lambda: config)
    hooks = {"session_factory": item.sync_sf}
    monkeypatch.setattr(factory, "_collect_host_hooks", lambda: dict(hooks))
    factory.reset_memory_manager()
    scope_factory = getattr(factory, "memory_manager_scope", None)
    with remote_mutation_scope(item.capability.context):
        if scope_factory is None:
            item.manager = factory.get_memory_manager()
            yield item
        else:
            item.manager = factory.make_remote_memory_manager(config, mutation_capability=item.capability, host_hooks=hooks)
            with scope_factory(item.manager):
                yield item
    factory.reset_memory_manager()
    engine.dispose()


async def memory_rows(item):
    async with item.engine.connect() as conn:
        return list((await conn.execute(text("SELECT user_id,agent_name,fact_id,content FROM c06_memory ORDER BY fact_id"))).all())


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", ["token", "owner", "lease"])
async def test_configured_memory_late_extraction_rejects_original_revoked_execution(memory_profile, stale):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = memory_profile
    entered, release = threading.Event(), threading.Event()

    def extraction(messages):
        entered.set()
        assert release.wait(10)
        return "late durable fact"

    item.manager._extract = extraction
    item.manager.add_nowait(item.spec.thread_id, ["extract"], user_id=item.spec.user_id)
    assert await asyncio.to_thread(entered.wait, 10)
    async with item.engine.begin() as conn:
        await conn.execute(
            text({"token": "UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'", "owner": "UPDATE runs SET owner_worker_id='replacement'", "lease": "UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"}[stale])
        )
    release.set()
    try:
        await asyncio.to_thread(item.manager.shutdown_flush, 10)
    except OwnershipRejected:
        rejected = True
    else:
        rejected = False
    assert await memory_rows(item) == []
    assert rejected


@pytest.mark.asyncio
async def test_extension_preflight_all_contributors_before_any_start(checkpoint_owner):
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.extensions.gateway import start_services
    from deerflow.extensions.registry import ExtensionRegistry
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, remote_mutation_scope

    effects = []

    class Unsafe:
        async def start(self, deps):
            effects.append(deps.session_factory)

    registry = ExtensionRegistry()
    with registry.attributed_to("unsafe"):
        registry.service(Unsafe())
    capability = FleetMutationCapability(checkpoint_owner.identity, checkpoint_owner.spec)
    kwargs = {"mutation_capability": capability} if "mutation_capability" in inspect.signature(start_services).parameters else {}
    with remote_mutation_scope(capability.context), pytest.raises(OwnershipRejected):
        await start_services(registry.build(), checkpoint_owner.private, checkpoint_owner.env[1], **kwargs)
    assert effects == []


@pytest.mark.asyncio
async def test_memory_legal_actual_crud_extraction_and_shutdown_raw_thread(memory_profile):
    item = memory_profile
    _, fact = await asyncio.to_thread(item.manager.create_fact, "legal", user_id=item.spec.user_id)
    await asyncio.to_thread(item.manager.update_fact, fact, "updated", user_id=item.spec.user_id)
    assert "updated" in str(await memory_rows(item))
    await asyncio.to_thread(item.manager.delete_fact, fact, user_id=item.spec.user_id)
    assert await memory_rows(item) == []
    item.manager.add(item.spec.thread_id, ["queued legal"], user_id=item.spec.user_id)
    result = []
    worker = threading.Thread(target=lambda: result.append(item.manager.shutdown_flush(10)))
    worker.start()
    await asyncio.to_thread(worker.join, 10)
    assert result == [True]
    assert "queued legal" in str(await memory_rows(item))


@pytest.mark.asyncio
@pytest.mark.parametrize("supplied", ["missing", "wrong", "wrong-user", "wrong-thread"])
@pytest.mark.parametrize("method", ["create_fact", "update_fact", "delete_fact", "add"])
async def test_memory_actual_crud_and_enqueue_reject_lost_identity(memory_profile, supplied, method):
    from dataclasses import replace

    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = memory_profile
    _, fact = await asyncio.to_thread(item.manager.create_fact, "baseline", user_id=item.spec.user_id)
    before = await memory_rows(item)
    context = None if supplied == "missing" else replace(item.capability.context, attempt_id="wrong") if supplied == "wrong" else item.capability.context
    token = _current_mutation_context.set(context)
    try:
        user = "other" if supplied == "wrong-user" else item.spec.user_id
        if method == "create_fact":

            def call():
                return item.manager.create_fact("changed", user_id=user)
        elif method == "update_fact":

            def call():
                return item.manager.update_fact(fact, "changed", user_id=user)
        elif method == "delete_fact":

            def call():
                return item.manager.delete_fact(fact, user_id=user)
        else:

            def call():
                return item.manager.add("other" if supplied == "wrong-thread" else item.spec.thread_id, ["changed"], user_id=user)

        if supplied == "wrong-thread" and method != "add":
            # CRUD has no public thread argument; wrong original execution
            # thread is represented by the immutable bound context.
            _current_mutation_context.set(replace(item.capability.context, thread_id="other"))
        with pytest.raises(OwnershipRejected):
            await asyncio.to_thread(call)
    finally:
        _current_mutation_context.reset(token)
    assert await memory_rows(item) == before
    assert item.manager._pending == {}


@pytest.mark.asyncio
async def test_remote_factory_never_reuses_preheated_local_singleton(memory_profile):
    from deerflow.agents.memory import manager as factory
    from deerflow.agents.memory.backends.noop.noop_manager import NoopMemoryManager
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = memory_profile
    factory._memory_manager = NoopMemoryManager()
    assert factory.get_memory_manager() is item.manager
    token = _current_mutation_context.set(None)
    try:
        with pytest.raises(OwnershipRejected):
            factory.get_memory_manager()
    finally:
        _current_mutation_context.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["deermem", "mem0", "honcho", "openviking"])
async def test_unsafe_memory_profile_rejected_before_constructor(memory_profile, monkeypatch, backend):
    from deerflow.agents.memory import manager as factory
    from deerflow.config.memory_config import MemoryConfig
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    cls = factory._resolve_manager_class(backend)
    effects = []
    monkeypatch.setattr(cls, "from_config", lambda *args, **kw: effects.append("construction"))
    with pytest.raises(OwnershipRejected):
        factory.make_remote_memory_manager(MemoryConfig(manager_class=backend), mutation_capability=memory_profile.capability, host_hooks={})
    assert effects == []


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["tool-add", "tool-update", "tool-delete", "emergency-hook"])
async def test_actual_memory_tool_and_summarization_hooks_preserve_ownership(memory_profile, monkeypatch, path):
    from types import SimpleNamespace

    import deerflow.agents.memory.summarization_hook as hooks
    from deerflow.agents.memory.summarization_hook import memory_flush_hook
    from deerflow.agents.memory.tools import memory_add_tool, memory_delete_tool, memory_update_tool
    from deerflow.agents.middlewares.summarization_middleware import DeerFlowSummarizationMiddleware
    from deerflow.config.memory_config import MemoryConfig
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = memory_profile
    monkeypatch.setattr(hooks, "get_memory_config", lambda: MemoryConfig(enabled=True))
    runtime = SimpleNamespace(context={"user_id": item.spec.user_id, "thread_id": item.spec.thread_id}, config={"configurable": {"thread_id": item.spec.thread_id}})
    token = _current_mutation_context.set(None)
    try:
        with pytest.raises(OwnershipRejected):
            if path == "tool-add":
                await asyncio.to_thread(memory_add_tool.func, runtime, "tool fact")
            elif path == "tool-update":
                await asyncio.to_thread(memory_update_tool.func, runtime, "id", "changed")
            elif path == "tool-delete":
                await asyncio.to_thread(memory_delete_tool.func, runtime, "id")
            else:
                # Invoke the actual middleware dispatcher -> emergency hook ->
                # factory -> add_nowait path, without constructing another graph.
                middleware = object.__new__(DeerFlowSummarizationMiddleware)
                middleware._before_summarization_hooks = [memory_flush_hook]
                from langchain_core.messages import AIMessage, HumanMessage

                middleware._fire_hooks([HumanMessage(content="remember preference"), AIMessage(content="acknowledged")], [], runtime)
    finally:
        _current_mutation_context.reset(token)
    assert await memory_rows(item) == []


class SqlExtension:
    remote_state_mode = "transactional"

    def __init__(self):
        self.deps = None

    async def write(self, value):
        async with self.deps.mutation_transactions.async_transaction() as session:
            await session.execute(text("INSERT INTO c06_extension(value) VALUES (:v)"), {"v": value})

    async def start(self, deps):
        self.deps = deps
        assert deps.session_factory is None
        await self.write("start")

    async def stop(self):
        await self.write("stop")

    async def on_task_start(self, *args):
        await self.write("task")


@pytest_asyncio.fixture
async def extension_profile(checkpoint_owner):
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.extensions.gateway import start_services
    from deerflow.extensions.registry import ExtensionRegistry
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = checkpoint_owner
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='running'"))
        await conn.execute(text("CREATE TABLE c06_extension(value text)"))
    item.capability = FleetMutationCapability(item.identity, item.spec)
    item.service = SqlExtension()
    registry = ExtensionRegistry()
    with registry.attributed_to("configured-pg"):
        registry.service(item.service)
        registry.task_lifecycle(item.service)
    from deerflow.extensions.gateway import bind_remote_extensions

    item.extensions = bind_remote_extensions(registry.build(), item.capability)
    with remote_mutation_scope(item.capability.context):
        await start_services(item.extensions, item.private, item.env[1], mutation_capability=item.capability)
        yield item


async def extension_rows(item):
    async with item.engine.connect() as conn:
        return list((await conn.execute(text("SELECT value FROM c06_extension ORDER BY value"))).scalars())


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["start", "stop", "callback", "cross-loop"])
@pytest.mark.parametrize("stale", [None, "token", "owner", "lease", "missing", "wrong", "terminal"])
async def test_actual_extension_sql_entrypoints_fence_and_propagate(extension_profile, path, stale):
    from dataclasses import replace

    from deerflow.extensions.gateway import start_services, stop_services
    from deerflow.extensions.notify import _notify_each_on_extension_loop, reset_extension_notify_loop, set_extension_notify_loop
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = extension_profile
    if stale in {"token", "owner", "lease", "terminal"}:
        async with item.engine.begin() as conn:
            await conn.execute(
                text(
                    {
                        "token": "UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'",
                        "owner": "UPDATE runs SET owner_worker_id='replacement'",
                        "lease": "UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'",
                        "terminal": "UPDATE runs SET status='success'",
                    }[stale]
                )
            )
    before = await extension_rows(item)
    token = _current_mutation_context.set(None if stale == "missing" else replace(item.capability.context, attempt_id="wrong") if stale == "wrong" else item.capability.context)
    set_extension_notify_loop(asyncio.get_running_loop())

    async def operation():
        if path == "start":
            await start_services(item.extensions, item.private, item.env[1], mutation_capability=item.capability)
        elif path == "stop":
            await stop_services(item.extensions)
        elif path == "callback":
            await _notify_each_on_extension_loop(item.extensions.task_lifecycle, "on_task_start", lambda contributor: contributor.on_task_start(), "task", None)
        else:

            async def isolated():
                await _notify_each_on_extension_loop(item.extensions.task_lifecycle, "on_task_start", lambda contributor: contributor.on_task_start(), "task", None)

            await asyncio.to_thread(lambda: asyncio.run(isolated()))

    try:
        if stale:
            with pytest.raises(OwnershipRejected):
                await operation()
            assert await extension_rows(item) == before
        else:
            await operation()
            assert len(await extension_rows(item)) == len(before) + 1
    finally:
        reset_extension_notify_loop()
        _current_mutation_context.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["missing", "wrong"])
async def test_remote_memory_factory_rejects_before_configured_constructor(memory_profile, monkeypatch, identity):
    from dataclasses import replace

    from deerflow.agents.memory import manager as factory
    from deerflow.config.memory_config import MemoryConfig
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = memory_profile
    effects = []
    monkeypatch.setattr(type(item.manager), "from_config", lambda *a, **k: effects.append("effect"))
    token = _current_mutation_context.set(None if identity == "missing" else replace(item.capability.context, attempt_id="wrong"))
    try:
        with pytest.raises(OwnershipRejected):
            factory.make_remote_memory_manager(MemoryConfig(manager_class="deerflow_c04_fixture.memory:PostgresMemory"), mutation_capability=item.capability, host_hooks={"session_factory": item.sync_sf})
    finally:
        _current_mutation_context.reset(token)
    assert effects == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome,core_status,prepared_pair",
    [
        ("completed", "success", {"kind": "final", "core": "success", "task": "succeeded", "placement": "succeeded"}),
        ("failed", "error", {"kind": "final", "core": "error", "task": "failed", "placement": "failed"}),
        ("failed", "timeout", {"kind": "final", "core": "timeout", "task": "timed_out", "placement": "timed_out"}),
        ("aborted", "interrupted", {"kind": "paused", "core": "interrupted", "task": "input_required", "placement": "cancelled"}),
    ],
    indirect=["prepared_pair"],
)
async def test_extension_terminal_receipt_is_narrow_original_operation(extension_profile, prepared_pair, outcome, core_status):
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = extension_profile
    from deerflow.persistence.run.sql import RunRepository

    from .test_c08_terminal_pair import participant

    assert prepared_pair.item is item
    async with item.engine.begin() as conn:
        await conn.run_sync(lambda sync: RunEventRow.__table__.create(sync, checkfirst=True))
    repository = RunRepository(item.env[1], mutation_capability=prepared_pair.capability, terminal_participant=participant(prepared_pair))
    with prepared_pair.scope():
        await repository.update_status(item.spec.run_id, core_status)
    operation = item.service.deps.terminal_operations
    receipt = await operation.record_task_stop(task_id=item.spec.run_id, outcome=outcome)
    assert receipt["event_type"] == "run.extension.task_stop"
    assert receipt["content"] == {"task_id": item.spec.run_id, "outcome": outcome}
    assert not hasattr(operation, "session_factory") and not hasattr(operation, "async_transaction")
    with pytest.raises(OwnershipRejected):
        async with item.service.deps.mutation_transactions.async_transaction() as session:
            await session.execute(text("UPDATE runs SET status='running'"))
    # Repeated stop is idempotent and does not advance the event sequence.
    assert await operation.record_task_stop(task_id=item.spec.run_id, outcome=outcome) == receipt
    async with item.engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM run_events"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT status FROM runs"))).scalar_one() == core_status


@pytest.mark.asyncio
@pytest.mark.parametrize("rejection", ["target", "outcome", "active", "token", "owner", "missing", "wrong", "sql-payload"])
async def test_extension_terminal_receipt_rejects_wrong_association_without_effect(extension_profile, prepared_pair, rejection):
    from dataclasses import replace

    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    from .test_c08_node_workspace_idle import sql_digests
    from .test_c08_terminal_pair import participant

    item = extension_profile
    assert prepared_pair.item is item
    async with item.engine.begin() as conn:
        await conn.run_sync(lambda sync: RunEventRow.__table__.create(sync, checkfirst=True))
    repository = RunRepository(item.env[1], mutation_capability=prepared_pair.capability, terminal_participant=participant(prepared_pair))
    with prepared_pair.scope():
        await repository.update_status(item.spec.run_id, "success")
    async with item.engine.begin() as conn:
        if rejection in {"active", "owner"}:
            await conn.execute(text({"active": "UPDATE runs SET status='running'", "token": "UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'", "owner": "UPDATE runs SET owner_worker_id='other'"}[rejection]))
    operation = item.service.deps.terminal_operations
    current_context = item.capability.context
    if rejection == "token":
        # Original composite request FK forbids mutating accepted token_hash.
        # Supply an actual wrong original private token to the original fence.
        from app.fleet.mutation import FleetMutationCapability
        from deerflow.extensions.gateway import _TerminalExtensionOperations

        stale = FleetMutationCapability(replace(item.identity, token_stamp="b" * 64), item.spec)
        operation = _TerminalExtensionOperations(item.env[1], stale)
        current_context = stale.context
    before = await sql_digests(prepared_pair)
    token = _current_mutation_context.set(None if rejection == "missing" else replace(item.capability.context, attempt_id="other") if rejection == "wrong" else current_context)
    try:
        if rejection == "sql-payload":
            with pytest.raises(TypeError):
                await operation.record_task_stop(task_id=item.spec.run_id, outcome="completed", sql="UPDATE runs SET status='running'")
        else:
            with pytest.raises(OwnershipRejected):
                await operation.record_task_stop(task_id="other" if rejection == "target" else item.spec.run_id, outcome="failed" if rejection == "outcome" else "completed")
    finally:
        _current_mutation_context.reset(token)
    assert await sql_digests(prepared_pair) == before
    async with item.engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM run_events"))).scalar_one() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome,core_status", [("completed", "success"), ("failed", "error"), ("failed", "timeout"), ("aborted", "interrupted")])
async def test_extension_core_only_terminal_receipt_rejected_without_sql_effect(extension_profile, outcome, core_status):
    from types import SimpleNamespace

    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c08_node_workspace_idle import sql_digests

    item = extension_profile
    async with item.engine.begin() as conn:
        await conn.run_sync(lambda sync: RunEventRow.__table__.create(sync, checkfirst=True))
        await conn.execute(text("UPDATE runs SET status=:s"), {"s": core_status})
    projection = SimpleNamespace(sf=item.env[1])
    before = await sql_digests(projection)
    for _ in range(2):
        with pytest.raises(OwnershipRejected, match="Checkpoint ownership fence"):
            await item.service.deps.terminal_operations.record_task_stop(task_id=item.spec.run_id, outcome=outcome)
        assert await sql_digests(projection) == before
    async with item.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM run_events")) == 0


@pytest.mark.asyncio
async def test_extension_shutdown_cleans_successors_and_preserves_first_ownership_error(extension_profile):
    from dataclasses import replace

    from deerflow.extensions.gateway import stop_services
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = extension_profile
    cleaned = []

    class Cleanup:
        remote_state_mode = "stateless"

        async def stop(self):
            cleaned.append(True)

    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET owner_worker_id='other'"))
    snapshot = replace(item.extensions, services=(("clean", Cleanup()), ("sql", item.service)))
    with pytest.raises(OwnershipRejected):
        await stop_services(snapshot)
    assert cleaned == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["valid", "missing", "wrong", "revoked"])
async def test_detached_extension_dispatch_captures_bound_attempt_and_failure(extension_profile, identity):
    from dataclasses import replace

    from deerflow.extensions.notify import dispatch_system_model_observation, drain_extension_dispatches, extension_dispatch_failures, reset_extension_notify_loop, set_extension_notify_loop
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = extension_profile
    set_extension_notify_loop(asyncio.get_running_loop())
    before = await extension_rows(item)
    if identity == "revoked":
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE runs SET owner_worker_id='other'"))
    token = _current_mutation_context.set(None if identity == "missing" else replace(item.capability.context, attempt_id="wrong") if identity == "wrong" else item.capability.context)
    try:
        coro = item.service.on_task_start()
        if identity in {"missing", "wrong"}:
            with pytest.raises(OwnershipRejected):
                dispatch_system_model_observation(coro, "actual-sql-observer", mutation_context=item.extensions.mutation_context)
            assert coro.cr_frame is None
        else:
            assert dispatch_system_model_observation(coro, "actual-sql-observer", mutation_context=item.extensions.mutation_context)
            if identity == "revoked":
                with pytest.raises(OwnershipRejected):
                    await drain_extension_dispatches()
                assert isinstance(extension_dispatch_failures()[-1], OwnershipRejected)
            else:
                await drain_extension_dispatches()
                assert extension_dispatch_failures() == ()
    finally:
        _current_mutation_context.reset(token)
        reset_extension_notify_loop()
    if identity != "valid":
        assert await extension_rows(item) == before
    else:
        assert len(await extension_rows(item)) == len(before) + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("domain", ["memory", "extension"])
@pytest.mark.parametrize("independent_guard", [False, True])
async def test_actual_adapted_writer_same_pid_txid_and_commit_lock_exclusion(memory_profile, extension_profile, domain, independent_guard):
    from sqlalchemy import event

    from .test_c05_remote_agent_runtime import wait_blocked

    item = memory_profile if domain == "memory" else extension_profile
    ready, release = threading.Event(), threading.Event()
    async_ready, async_release = asyncio.Event(), asyncio.Event()
    identities, writers = [], []
    cap = item.manager._transactions._capability if domain == "memory" else item.capability
    original_sync, original_async = cap.validate_sync, cap.validate_async
    writer_engine = item.sync_sf.kw["bind"] if domain == "memory" else item.engine.sync_engine

    def observe_sql(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith("INSERT INTO C06_" + ("MEMORY" if domain == "memory" else "EXTENSION")):
            writers.append(tuple(conn.exec_driver_sql("SELECT pg_backend_pid(),txid_current()").one()))

    def sync_guard(session, **kwargs):
        if independent_guard:
            with item.sync_sf() as separate, separate.begin():
                original_sync(separate, **kwargs)
                identities.append(tuple(separate.execute(text("SELECT pg_backend_pid(),txid_current()")).one()))
        else:
            original_sync(session, **kwargs)
            identities.append(tuple(session.execute(text("SELECT pg_backend_pid(),txid_current()")).one()))
        ready.set()
        assert release.wait(10)

    async def async_guard(session, **kwargs):
        if independent_guard:
            async with item.env[1]() as separate, separate.begin():
                await original_async(separate, **kwargs)
                identities.append(tuple((await separate.execute(text("SELECT pg_backend_pid(),txid_current()"))).one()))
        else:
            await original_async(session, **kwargs)
            identities.append(tuple((await session.execute(text("SELECT pg_backend_pid(),txid_current()"))).one()))
        async_ready.set()
        await asyncio.wait_for(async_release.wait(), 10)

    cap.validate_sync, cap.validate_async = sync_guard, async_guard
    event.listen(writer_engine, "before_cursor_execute", observe_sql)
    try:
        writing = asyncio.create_task(asyncio.to_thread(item.manager.create_fact, "transaction proof", user_id=item.spec.user_id)) if domain == "memory" else asyncio.create_task(item.service.write("transaction proof"))
        if domain == "memory":
            assert await asyncio.to_thread(ready.wait, 10)
        else:
            await asyncio.wait_for(async_ready.wait(), 10)
        async with item.engine.connect() as takeover:
            tx = await takeover.begin()
            pid = (await takeover.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            changing = asyncio.create_task(takeover.execute(text("UPDATE fleet_agent_tasks SET state='paused'")))
            if independent_guard:
                await asyncio.wait_for(changing, 5)
                await tx.rollback()
            else:
                await wait_blocked(item.engine, pid)
                assert not changing.done(), "Execution lock escaped writer transaction"
            release.set()
            async_release.set()
            await writing
            await changing
            if tx.is_active:
                await tx.rollback()
    finally:
        release.set()
        async_release.set()
        cap.validate_sync, cap.validate_async = original_sync, original_async
        event.remove(writer_engine, "before_cursor_execute", observe_sql)
    assert writers
    assert all(identity == identities[0] for identity in writers) is (not independent_guard)


@pytest.mark.asyncio
@pytest.mark.parametrize("domain", ["memory", "extension"])
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_actual_adapted_first_sql_exception_rolls_back(memory_profile, extension_profile, domain, failure):
    from sqlalchemy import event

    item = memory_profile if domain == "memory" else extension_profile
    writer_engine = item.sync_sf.kw["bind"] if domain == "memory" else item.engine.sync_engine
    rows = memory_rows if domain == "memory" else extension_rows
    before = await rows(item)
    writes = []

    def fail_after_sql(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith("INSERT INTO C06_" + ("MEMORY" if domain == "memory" else "EXTENSION")):
            writes.append(statement)
            raise failure("first actual adapted SQL completed")

    event.listen(writer_engine, "after_cursor_execute", fail_after_sql)
    try:
        with pytest.raises(failure):
            if domain == "memory":
                await asyncio.to_thread(item.manager.create_fact, "rollback proof", user_id=item.spec.user_id)
            else:
                await item.service.write("rollback proof")
    finally:
        event.remove(writer_engine, "after_cursor_execute", fail_after_sql)
    assert len(writes) == 1
    assert await rows(item) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("domain", ["memory", "extension"])
async def test_actual_adapted_writer_rechecks_expiry_after_lock_wait(memory_profile, extension_profile, domain):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked

    item = memory_profile if domain == "memory" else extension_profile
    cap = item.manager._transactions._capability if domain == "memory" else item.capability
    loop = asyncio.get_running_loop()
    pid_future = loop.create_future()
    original_sync, original_async = cap.validate_sync, cap.validate_async
    rows = memory_rows if domain == "memory" else extension_rows
    before = await rows(item)

    def observed_sync(session, **kwargs):
        pid = session.execute(text("SELECT pg_backend_pid()")).scalar_one()
        if not pid_future.done():
            loop.call_soon_threadsafe(pid_future.set_result, pid)
        original_sync(session, **kwargs)

    async def observed_async(session, **kwargs):
        if not pid_future.done():
            pid_future.set_result((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        await original_async(session, **kwargs)

    cap.validate_sync, cap.validate_async = observed_sync, observed_async
    try:
        async with item.engine.connect() as blocker:
            tx = await blocker.begin()
            await blocker.execute(text("SELECT id FROM fleet_agent_tasks FOR UPDATE"))
            writing = asyncio.create_task(asyncio.to_thread(item.manager.create_fact, "expired lockwait", user_id=item.spec.user_id)) if domain == "memory" else asyncio.create_task(item.service.write("expired lockwait"))
            await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
            await blocker.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
            await blocker.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
            await tx.commit()
            with pytest.raises(OwnershipRejected):
                await writing
    finally:
        cap.validate_sync, cap.validate_async = original_sync, original_async
    assert await rows(item) == before


@pytest.mark.asyncio
async def test_actual_memory_queue_preserves_original_capability_across_token_takeover(memory_profile):
    from dataclasses import replace

    from app.fleet.mutation import FleetMutationCapability
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, remote_mutation_scope
    from deerflow.runtime.execution.mutation_transactions import BoundMutationTransactions

    item = memory_profile
    manager = item.manager
    manager.add(item.spec.thread_id, ["old authority"], user_id=item.spec.user_id)
    old_transactions = manager._transactions
    new_identity = replace(item.identity, token_stamp="e" * 64)
    new_capability = FleetMutationCapability(new_identity, item.spec)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET token_hash=:hash"), {"hash": new_identity.token_stamp})
    manager._transactions = BoundMutationTransactions(new_capability, operation="memory.write", sync_session_factory=item.sync_sf)
    with remote_mutation_scope(new_capability.context):
        # Same user/thread/agent must not coalesce over the stale queue item.
        await asyncio.to_thread(manager.add, item.spec.thread_id, ["new authority"], user_id=item.spec.user_id)
    assert len(manager._pending) == 2
    assert {entry[1] for entry in manager._pending.values()} == {old_transactions, manager._transactions}
    # Shutdown uses raw threads and original queue-item contexts, even with no
    # ambient ContextVar in the caller.
    with pytest.raises(OwnershipRejected):
        await asyncio.to_thread(manager.shutdown_flush, 10)
    persisted = await memory_rows(item)
    assert len(persisted) == 1 and persisted[0][3] == "new authority"


@pytest.mark.asyncio
async def test_actual_memory_enqueue_on_isolated_subagent_loop_keeps_original_authority(memory_profile):
    from contextvars import copy_context

    item = memory_profile
    context = copy_context()
    errors = []

    async def isolated():
        await item.manager.aadd(item.spec.thread_id, ["isolated subagent loop"], user_id=item.spec.user_id, agent_name="child")

    def thread():
        try:
            context.run(asyncio.run, isolated())
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=thread)
    worker.start()
    await asyncio.to_thread(worker.join, 10)
    assert not errors and not worker.is_alive()
    assert await asyncio.to_thread(item.manager.shutdown_flush, 10)
    assert "isolated subagent loop" in str(await memory_rows(item))


@pytest.mark.asyncio
@pytest.mark.parametrize("revoke_late_result", [False, True])
@pytest.mark.parametrize("queued_observer", [False, True])
@pytest.mark.parametrize("late_cancel", [False, True])
async def test_actual_worker_normal_memory_middleware_drains_before_terminal(memory_profile, mutations, extension_profile, revoke_late_result, queued_observer, late_cancel, monkeypatch, tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from langchain.agents import create_agent
    from langchain_core.language_models import BaseChatModel
    from langchain_core.messages import AIMessage, HumanMessage
    from langchain_core.outputs import ChatGeneration, ChatResult

    from deerflow.agents.middlewares.memory_middleware import MemoryMiddleware
    from deerflow.agents.thread_state import ThreadState
    from deerflow.config.memory_config import MemoryConfig
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent

    class Model(BaseChatModel):
        @property
        def _llm_type(self):
            return "memory-drain-proof"

        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            if queued_observer:
                dispatch_system_model_observation(observer(), "actual-final-model-observer", mutation_context=item.extensions.mutation_context)
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content="normal middleware queued durable result"))])

    from app.fleet.runner_context import drain_remote_mutations
    from deerflow.extensions.notify import dispatch_system_model_observation, drain_extension_dispatches, reset_extension_notify_loop, set_extension_notify_loop
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = memory_profile
    set_extension_notify_loop(asyncio.get_running_loop())
    observer_entered, observer_release = asyncio.Event(), asyncio.Event()

    async def observer():
        observer_entered.set()
        await observer_release.wait()
        await item.service.write("final model observer")

    await item.writer.adelete_thread(item.spec.thread_id)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='pending'"))
    from deerflow.config.run_ownership_config import RunOwnershipConfig

    manager = RunManager(store=item.runs, worker_id=item.identity.owner_worker_id, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=late_cancel))
    record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
    entered, release = threading.Event(), threading.Event()

    def extract(messages):
        entered.set()
        assert release.wait(10)
        return "normal middleware queued durable result"

    item.manager._extract = extract
    if late_cancel:

        async def interrupted_title(**kwargs):
            dispatch_system_model_observation(item.service.write("late interrupted title observer"), "late-title-model-result", mutation_context=item.extensions.mutation_context)

        monkeypatch.setattr("deerflow.runtime.runs.worker._ensure_interrupted_title", interrupted_title)
    graph = create_agent(Model(), tools=[], middleware=[MemoryMiddleware(memory_config=MemoryConfig(enabled=True))], checkpointer=item.writer, state_schema=ThreadState, context_schema=dict)
    drained_status = []

    async def drain():
        async with item.engine.connect() as conn:
            drained_status.append((await conn.execute(text("SELECT status FROM runs"))).scalar_one())
        if len(drained_status) == 1:
            assert item.manager._pending, "Actual middleware did not enqueue"
        draining = asyncio.create_task(drain_remote_mutations(item.manager, timeout=10))
        assert await asyncio.to_thread(entered.wait, 10)
        if revoke_late_result:
            async with item.engine.begin() as conn:
                await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
        release.set()
        if queued_observer and not revoke_late_result and len(drained_status) == 1:
            await asyncio.wait_for(observer_entered.wait(), 5)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(draining), 0.1)
        observer_release.set()
        await draining
        if late_cancel and not revoke_late_result and len(drained_status) == 1:
            from deerflow.persistence.run import RunRepository

            assert await RunRepository(item.env[1]).request_cancel(item.spec.run_id, action="interrupt") == "interrupt"

    from .c08_native_terminal_pair import NativeTerminalPreparation

    prepare_terminal = NativeTerminalPreparation(item, tmp_path / "memory-terminal", cancellation=True)
    from contextlib import AsyncExitStack, contextmanager

    from app.fleet.agent_control import OriginalAgentCancellation
    from app.fleet.runner_context import _AgentResourceTeardown
    from deerflow.runtime.execution.mutation_context import ExecutionCancellationRequested

    capability = prepare_terminal.capability

    @contextmanager
    def settlement_scope():
        with capability.cancellation_settlement_scope():
            yield

    teardown = _AgentResourceTeardown(AsyncExitStack(), settlement_scope, capability.context)
    teardown.workspace_writers = prepare_terminal.controller
    cancellation = OriginalAgentCancellation(item.env[1], capability, manager, teardown, prepare_terminal.controller)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    try:
        with settlement_scope():
            await run_agent(
                bridge,
                manager,
                record,
                ctx=RunContext(
                    checkpointer=item.writer,
                    event_store=item.events,
                    thread_store=item.threads,
                    app_config=item.private,
                    before_terminal_mutations=drain,
                    prepare_terminal=prepare_terminal,
                    prepare_cancellation=cancellation.prepare,
                    observe_cancellation=cancellation.read,
                    cancellation_checkpoint_scope=capability.cancellation_checkpoint_scope,
                    cancellation_rollback_scope=capability.cancellation_rollback_scope,
                ),
                agent_factory=lambda *, config: graph,
                graph_input={"messages": [HumanMessage(content="remember my preference")]},
                config={"configurable": {"thread_id": item.spec.thread_id}},
            )
    finally:
        release.set()
        observer_release.set()
        try:
            await drain_extension_dispatches()
        except OwnershipRejected:
            assert revoke_late_result
        except ExecutionCancellationRequested:
            assert late_cancel and not revoke_late_result
        finally:
            reset_extension_notify_loop()
    async with item.engine.connect() as conn:
        status = (await conn.execute(text("SELECT status FROM runs"))).scalar_one()
    assert drained_status == ["running"]
    if revoke_late_result:
        assert record.ownership_lost and status == "running"
        assert await memory_rows(item) == []
    else:
        assert not record.ownership_lost and status == ("interrupted" if late_cancel else "success")
        if late_cancel:
            assert "late interrupted title observer" not in await extension_rows(item)
            async with item.engine.connect() as conn:
                point = (await conn.execute(text("SELECT kind,desired_core_status,desired_task_status,desired_placement_status FROM fleet_workspace_points WHERE kind!='partial'"))).one()
                assert tuple(point) == ("paused", "interrupted", "paused", "cancelled")
        assert "normal middleware queued durable result" in str(await memory_rows(item))
        if queued_observer:
            assert "final model observer" in await extension_rows(item)


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked", [False, True])
async def test_detached_actual_sql_from_raw_thread_to_owner_loop_retains_attempt_failure(extension_profile, revoked):
    from contextvars import copy_context
    from dataclasses import replace

    from deerflow.extensions.notify import dispatch_system_model_observation, drain_extension_dispatches, extension_dispatch_failures, reset_extension_notify_loop, set_extension_notify_loop
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = extension_profile
    set_extension_notify_loop(asyncio.get_running_loop())
    before = await extension_rows(item)
    if revoked:
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='" + "b" * 64 + "'"))
    original = copy_context()
    submitted, errors = [], []

    def dispatch():
        try:
            submitted.append(dispatch_system_model_observation(item.service.on_task_start(), "raw-thread-real-sql", mutation_context=item.extensions.mutation_context))
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=lambda: original.run(dispatch))
    worker.start()
    await asyncio.to_thread(worker.join, 10)
    assert submitted == [True] and not errors
    try:
        if revoked:
            with pytest.raises(OwnershipRejected):
                await drain_extension_dispatches()
            failures = extension_dispatch_failures()
            assert len(failures) == 1 and isinstance(failures[0], OwnershipRejected)
            # Another run / Local must neither drain nor inherit this failure.
            for foreign in (None, replace(item.capability.context, attempt_id="next-attempt")):
                token = _current_mutation_context.set(foreign)
                try:
                    assert extension_dispatch_failures() == ()
                    await drain_extension_dispatches()
                finally:
                    _current_mutation_context.reset(token)
            # Original attempt still has the retained failure after foreign drain.
            assert extension_dispatch_failures() == failures
            with pytest.raises(OwnershipRejected):
                await drain_extension_dispatches()
            assert await extension_rows(item) == before
        else:
            await drain_extension_dispatches()
            assert extension_dispatch_failures() == ()
            assert len(await extension_rows(item)) == len(before) + 1
    finally:
        reset_extension_notify_loop()


@pytest.mark.asyncio
@pytest.mark.parametrize("read", ["get_memory", "get_context", "search"])
@pytest.mark.parametrize("identity", ["missing", "wrong", "user"])
async def test_configured_memory_reads_require_original_private_scope(memory_profile, read, identity):
    from dataclasses import replace

    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = memory_profile
    await asyncio.to_thread(item.manager.create_fact, "original fact", user_id=item.spec.user_id)
    user = "wrong-user" if identity == "user" else item.spec.user_id
    token = _current_mutation_context.set(None if identity == "missing" else replace(item.capability.context, attempt_id="wrong") if identity == "wrong" else item.capability.context)
    try:
        with pytest.raises(OwnershipRejected):
            if read == "get_memory":
                await asyncio.to_thread(item.manager.get_memory, user_id=user)
            elif read == "get_context":
                await asyncio.to_thread(item.manager.get_context, user, thread_id=item.spec.thread_id)
            else:
                await asyncio.to_thread(item.manager.search, "fact", user_id=user)
    finally:
        _current_mutation_context.reset(token)
    assert "original fact" in str(await memory_rows(item))


@pytest.mark.asyncio
async def test_actual_extension_drain_includes_nested_owned_dispatch(extension_profile):
    from deerflow.extensions.notify import dispatch_system_model_observation, drain_extension_dispatches, reset_extension_notify_loop, set_extension_notify_loop

    item = extension_profile
    set_extension_notify_loop(asyncio.get_running_loop())
    entered, release = asyncio.Event(), asyncio.Event()
    before = await extension_rows(item)

    async def child():
        entered.set()
        await release.wait()
        await item.service.write("nested observer")

    async def parent():
        assert dispatch_system_model_observation(child(), "nested-child", mutation_context=item.extensions.mutation_context)

    assert dispatch_system_model_observation(parent(), "nested-parent", mutation_context=item.extensions.mutation_context)
    draining = asyncio.create_task(drain_extension_dispatches())
    await asyncio.wait_for(entered.wait(), 5)
    returned_before_child = False
    try:
        await asyncio.wait_for(asyncio.shield(draining), 0.1)
        returned_before_child = True
    except TimeoutError:
        pass
    finally:
        release.set()
        await draining
        await drain_extension_dispatches()
        reset_extension_notify_loop()
    assert not returned_before_child, "Owned nested callback escaped drain"
    assert len(await extension_rows(item)) == len(before) + 1


@pytest.mark.asyncio
async def test_local_extension_notify_reset_releases_local_failure_history():
    from deerflow.extensions.notify import dispatch_system_model_observation, drain_extension_dispatches, extension_dispatch_failures, reset_extension_notify_loop, set_extension_notify_loop
    from deerflow.runtime.execution.mutation_context import _current_mutation_context

    token = _current_mutation_context.set(None)
    try:
        set_extension_notify_loop(asyncio.get_running_loop())

        async def failed():
            raise RuntimeError("Local callback diagnostic")

        assert dispatch_system_model_observation(failed(), "Local-diagnostic")
        await drain_extension_dispatches()
        assert extension_dispatch_failures()
        reset_extension_notify_loop()
        assert extension_dispatch_failures() == ()
    finally:
        _current_mutation_context.reset(token)


def test_loaded_extensions_keeps_historical_positional_constructor():
    from deerflow_extension_api import ExtensionData

    from deerflow.extensions.registry import LoadedExtensions

    store, middlewares, lifecycle = ExtensionData("historical"), (("old", object()),), (("old-lifecycle", object()),)
    snapshot = LoadedExtensions(store, middlewares, lifecycle)
    assert snapshot.app_store is store and snapshot.middleware_contributors is middlewares and snapshot.task_lifecycle is lifecycle
    assert snapshot.mutation_context is None
    assert inspect.signature(LoadedExtensions).parameters["mutation_context"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch_path", ["detached", "awaited"])
async def test_actual_dispatch_budget_does_not_release_writer_before_rollback_finishes(extension_profile, dispatch_path):
    from deerflow.extensions.notify import dispatch_system_model_observation, drain_extension_dispatches, reset_extension_notify_loop, set_extension_notify_loop
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = extension_profile
    before = await extension_rows(item)
    entered, cleaning, release, done = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()
    set_extension_notify_loop(asyncio.get_running_loop())

    async def writer():
        try:
            async with item.service.deps.mutation_transactions.async_transaction() as session:
                await session.execute(text("INSERT INTO c06_extension(value) VALUES ('cancelled observer')"))
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cleaning.set()
                    await release.wait()
        finally:
            done.set()

    if dispatch_path == "detached":
        assert dispatch_system_model_observation(writer(), "real-SQL-cancel-rollback", mutation_context=item.extensions.mutation_context)
        await asyncio.wait_for(entered.wait(), 5)
        draining = asyncio.create_task(drain_extension_dispatches(timeout=0.1))
    else:
        from deerflow.extensions.notify import _notify_each_on_extension_loop

        async def from_subagent_loop():
            await _notify_each_on_extension_loop(item.extensions.task_lifecycle, "on_task_start", lambda contributor: writer(), "real-SQL-awaited", 0.1)

        draining = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(from_subagent_loop())))
        await asyncio.wait_for(entered.wait(), 5)
    await asyncio.wait_for(cleaning.wait(), 5)
    premature = False
    try:
        # The callback is inside its real transaction's cancellation cleanup.
        # A completed concurrentFuture cannot grant reset/resource disposal.
        try:
            reset_extension_notify_loop()
        except OwnershipRejected:
            pass
        else:
            premature = True
        assert not done.is_set()
    finally:
        release.set()
        await asyncio.wait_for(done.wait(), 5)
        with pytest.raises(OwnershipRejected):
            await draining
        reset_extension_notify_loop()
    assert not premature, "Original SQL writer was released before rollback/finally"
    assert await extension_rows(item) == before


def test_actual_task_cleanup_marker_survives_future_cancel_publication_race():
    from concurrent.futures import Future

    from deerflow.extensions.notify import _finish_dispatch

    class CancelBetweenCheckAndPublish(Future):
        def set_result(self, result):
            # Deterministically simulate cancellation on another thread after
            # _finish_dispatch checked done(), before result publication.
            self.cancel()
            super().set_result(result)

    future, cleanup = CancelBetweenCheckAndPublish(), Future()
    _finish_dispatch(future, cleanup)
    assert future.cancelled() and cleanup.done() and cleanup.result() is None


@pytest.mark.asyncio
async def test_remote_dispatch_failure_release_is_only_original_resource_teardown(extension_profile):
    from dataclasses import replace

    from deerflow.extensions.notify import dispatch_system_model_observation, drain_extension_dispatches, extension_dispatch_failures, release_extension_dispatch_failures, reset_extension_notify_loop, set_extension_notify_loop
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, _current_mutation_context

    item = extension_profile
    set_extension_notify_loop(asyncio.get_running_loop())
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET owner_worker_id='other'"))
    assert dispatch_system_model_observation(item.service.on_task_start(), "original-failed-resource", mutation_context=item.extensions.mutation_context)
    with pytest.raises(OwnershipRejected):
        await drain_extension_dispatches()
    original = extension_dispatch_failures()
    for foreign in (None, replace(item.capability.context, attempt_id="foreign")):
        token = _current_mutation_context.set(foreign)
        try:
            if foreign is None:
                with pytest.raises(OwnershipRejected):
                    release_extension_dispatch_failures()
            else:
                assert release_extension_dispatch_failures() == ()
        finally:
            _current_mutation_context.reset(token)
        assert extension_dispatch_failures() == original
    released = release_extension_dispatch_failures()
    assert released == original and isinstance(released[0], OwnershipRejected)
    assert extension_dispatch_failures() == ()
    reset_extension_notify_loop()


@pytest.mark.asyncio
async def test_actual_owned_dispatch_bookkeeping_is_safe_across_raw_threads(extension_profile):
    from contextvars import copy_context

    from deerflow.extensions.notify import dispatch_system_model_observation, drain_extension_dispatches, reset_extension_notify_loop, set_extension_notify_loop

    item = extension_profile
    set_extension_notify_loop(asyncio.get_running_loop())
    before = await extension_rows(item)
    release = asyncio.Event()
    errors = []
    original = copy_context()

    async def callback(index):
        await release.wait()
        await item.service.write("raw-thread-" + str(index))

    def dispatch(index):
        try:
            assert dispatch_system_model_observation(callback(index), "thread-" + str(index), mutation_context=item.extensions.mutation_context)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=lambda i=i, context=original.copy(): context.run(dispatch, i)) for i in range(24)]
    for thread in threads:
        thread.start()
    for thread in threads:
        await asyncio.to_thread(thread.join, 5)
    assert not errors and all(not thread.is_alive() for thread in threads)
    release.set()
    await drain_extension_dispatches()
    reset_extension_notify_loop()
    assert len(await extension_rows(item)) == len(before) + len(threads)


@pytest.mark.asyncio
@pytest.mark.parametrize("domain", ["memory", "extension"])
@pytest.mark.parametrize("waiting", ["domain", "target"])
async def test_actual_adapted_domain_and_target_wait_expiry(memory_profile, extension_profile, domain, waiting):
    from sqlalchemy import event

    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked

    item = memory_profile if domain == "memory" else extension_profile
    table = "c06_memory" if domain == "memory" else "c06_extension"
    key = "deerflow:memory:" + item.spec.user_id if domain == "memory" else "deerflow:extension"
    rows = memory_rows if domain == "memory" else extension_rows
    before = await rows(item)
    loop = asyncio.get_running_loop()
    pid_future = loop.create_future()
    writer_engine = item.sync_sf.kw["bind"] if domain == "memory" else item.engine.sync_engine

    def observe(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith("INSERT INTO " + table.upper()):
            pid = conn.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            loop.call_soon_threadsafe(pid_future.set_result, pid)

    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    event.listen(writer_engine, "before_cursor_execute", observe)
    try:
        async with item.engine.connect() as blocker:
            tx = await blocker.begin()
            if waiting == "domain":
                await blocker.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"), {"key": key})
            else:
                await blocker.execute(text("LOCK TABLE " + table + " IN ACCESS EXCLUSIVE MODE"))
            writing = asyncio.create_task(asyncio.to_thread(item.manager.create_fact, "wait expiry", user_id=item.spec.user_id)) if domain == "memory" else asyncio.create_task(item.service.write("wait expiry"))
            try:
                if waiting == "domain":
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(asyncio.shield(writing), 0.1)
                else:
                    await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
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
        event.remove(writer_engine, "before_cursor_execute", observe)
    assert await rows(item) == before


@pytest.mark.asyncio
async def test_actual_memory_update_row_wait_expiry_rolls_back(memory_profile):
    from sqlalchemy import event

    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked

    item = memory_profile
    _, fact_id = await asyncio.to_thread(item.manager.create_fact, "original row", user_id=item.spec.user_id)
    before = await memory_rows(item)
    loop = asyncio.get_running_loop()
    pid_future = loop.create_future()
    writer_engine = item.sync_sf.kw["bind"]

    def observe(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith("UPDATE C06_MEMORY"):
            loop.call_soon_threadsafe(pid_future.set_result, conn.exec_driver_sql("SELECT pg_backend_pid()").scalar_one())

    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    event.listen(writer_engine, "before_cursor_execute", observe)
    try:
        async with item.engine.connect() as blocker:
            tx = await blocker.begin()
            await blocker.execute(text("SELECT fact_id FROM c06_memory WHERE fact_id=:id FOR UPDATE"), {"id": fact_id})
            writing = asyncio.create_task(asyncio.to_thread(item.manager.update_fact, fact_id, "late replacement", user_id=item.spec.user_id))
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
        event.remove(writer_engine, "before_cursor_execute", observe)
    assert await memory_rows(item) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("domain", ["store", "agent", "managed", "thread", "events", "events-batch", "events-singleton", "events-terminal"])
async def test_actual_existing_durable_target_wait_expiry_cannot_commit(secondary_mutations, domain, tmp_path):
    from deerflow.persistence.managed_subagents.base import ManagedSubagentDefinition
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked
    from .test_c06_remote_agent_runtime import durable_rows, secondary_rows

    item = secondary_mutations
    cap = (
        item.state_store._mutation_capability
        if domain == "store"
        else item.agents._mutation_capability
        if domain == "agent"
        else item.managed._mutation_capability
        if domain == "managed"
        else item.threads._mutation_capability
        if domain == "thread"
        else item.events._mutation_capability
    )
    original_cursor, original_async, original_sync = cap.validate_cursor, cap.validate_async, cap.validate_sync
    loop = asyncio.get_running_loop()
    pid_future = loop.create_future()

    async def observed_cursor(cur, **kwargs):
        if not pid_future.done():
            await cur.execute("SELECT pg_backend_pid() AS pid")
            pid_future.set_result((await cur.fetchone())["pid"])
        return await original_cursor(cur, **kwargs)

    async def observed_async(session, **kwargs):
        if not pid_future.done():
            pid_future.set_result((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        return await original_async(session, **kwargs)

    def observed_sync(session, **kwargs):
        if not pid_future.done():
            loop.call_soon_threadsafe(pid_future.set_result, session.execute(text("SELECT pg_backend_pid()")).scalar_one())
        return original_sync(session, **kwargs)

    if domain == "events-terminal":
        from types import SimpleNamespace

        from deerflow.runtime.execution.mutation_context import remote_mutation_scope

        from .c08_native_terminal_pair import NativeTerminalPreparation

        prepare_terminal = NativeTerminalPreparation(item, tmp_path / "events-terminal")
        with remote_mutation_scope(cap.context):
            await prepare_terminal(SimpleNamespace(status="success", error=None, stop_reason=None))
            assert await item.runs.update_status(item.spec.run_id, "success")
    cap.validate_cursor, cap.validate_async, cap.validate_sync = observed_cursor, observed_async, observed_sync
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    before = (await secondary_rows(item.engine), await durable_rows(item.engine))
    try:
        async with item.engine.connect() as blocker:
            tx = await blocker.begin()
            if domain.startswith("events"):
                await blocker.execute(text("SELECT pg_advisory_xact_lock(hashtext(CAST(:thread AS text))::bigint)"), {"thread": item.spec.thread_id})
            else:
                table = {"store": "store", "agent": "agents", "managed": "managed_subagents", "thread": "threads_meta"}[domain]
                await blocker.execute(text("SELECT * FROM " + table + " FOR UPDATE"))
            operation = (
                item.state_store.aput(("user", item.spec.user_id), "bound-key", {"late": True})
                if domain == "store"
                else asyncio.to_thread(item.agents.update, "bound-agent", {"late": True}, "late", user_id=item.spec.user_id)
                if domain == "agent"
                else asyncio.to_thread(item.managed.update, ManagedSubagentDefinition(name="bound-managed", description="late", system_prompt="late"))
                if domain == "managed"
                else item.threads.update_display_name(item.spec.thread_id, "late", user_id=item.spec.user_id)
                if domain == "thread"
                else item.events.put(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="late", category="outputs", content="late")
            )
            if domain == "events-batch":
                operation.close()
                operation = item.events.put_batch([dict(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="late", category="outputs", content="late")])
            elif domain == "events-singleton":
                operation.close()
                operation = item.events.put_if_absent(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="run.delivery", category="outputs", content="baseline")
            elif domain == "events-terminal":
                operation.close()
                operation = item.events.record_extension_task_stop(task_id=item.spec.run_id, outcome="completed")
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
        cap.validate_cursor, cap.validate_async, cap.validate_sync = original_cursor, original_async, original_sync
    assert (await secondary_rows(item.engine), await durable_rows(item.engine)) == before


@pytest.mark.asyncio
async def test_actual_agent_create_unique_wait_expiry_rejects_original(secondary_mutations):
    from deerflow.persistence.agents.model import AgentRow
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked
    from .test_c06_remote_agent_runtime import secondary_rows

    item = secondary_mutations
    before = await secondary_rows(item.engine)
    async with item.engine.begin() as conn:
        template = dict((await conn.execute(text("SELECT * FROM agents"))).mappings().one())
        await conn.execute(text("DELETE FROM agents"))
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    cap = item.agents._mutation_capability
    original = cap.validate_sync
    loop = asyncio.get_running_loop()
    pid_future = loop.create_future()

    def observed(session, **kwargs):
        if not pid_future.done():
            loop.call_soon_threadsafe(pid_future.set_result, session.execute(text("SELECT pg_backend_pid()")).scalar_one())
        return original(session, **kwargs)

    cap.validate_sync = observed
    try:
        async with item.engine.connect() as peer:
            tx = await peer.begin()
            await peer.execute(AgentRow.__table__.insert().values(**template))
            writing = asyncio.create_task(asyncio.to_thread(item.agents.create, "bound-agent", {"late": True}, "late", user_id=item.spec.user_id))
            try:
                await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
                async with asyncio.timeout(5):
                    while not (await peer.execute(text("SELECT clock_timestamp() >= lease_expires_at FROM runs"))).scalar_one():
                        await asyncio.sleep(0)
                await tx.commit()
                with pytest.raises(OwnershipRejected):
                    await writing
            finally:
                if tx.is_active:
                    await tx.rollback()
                await asyncio.gather(writing, return_exceptions=True)
    finally:
        cap.validate_sync = original
    assert await secondary_rows(item.engine) == before


async def _expire_blocked_writer(item, cap, operation, blocker_sql, *, sync=False, peer_values=None):
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .test_c05_remote_agent_runtime import wait_blocked

    loop = asyncio.get_running_loop()
    pid_future = loop.create_future()
    name = "validate_sync" if sync else "validate_async"
    original = getattr(cap, name)

    async def observed_async(session, **kwargs):
        if not pid_future.done():
            pid_future.set_result((await session.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        return await original(session, **kwargs)

    def observed_sync(session, **kwargs):
        if not pid_future.done():
            loop.call_soon_threadsafe(pid_future.set_result, session.execute(text("SELECT pg_backend_pid()")).scalar_one())
        return original(session, **kwargs)

    setattr(cap, name, observed_sync if sync else observed_async)
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    try:
        async with item.engine.connect() as blocker:
            tx = await blocker.begin()
            await blocker.execute(blocker_sql, peer_values or {})
            writing = asyncio.create_task(operation())
            try:
                await wait_blocked(item.engine, await asyncio.wait_for(pid_future, 5))
                async with asyncio.timeout(5):
                    while not (await blocker.execute(text("SELECT clock_timestamp() >= lease_expires_at FROM runs"))).scalar_one():
                        await asyncio.sleep(0)
                await tx.commit()
                with pytest.raises(OwnershipRejected):
                    await writing
            finally:
                if tx.is_active:
                    await tx.rollback()
                await asyncio.gather(writing, return_exceptions=True)
    finally:
        setattr(cap, name, original)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["async-extension", "sync-memory"])
async def test_actual_bound_callback_orm_flush_wait_expiry_rejects(memory_profile, extension_profile, mode):
    from sqlalchemy import Column, MetaData, Table, Text
    from sqlalchemy.orm import registry

    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = extension_profile if mode == "async-extension" else memory_profile
    transactions = item.service.deps.mutation_transactions if mode == "async-extension" else item.manager._transactions
    mapper = registry()

    class CallbackRow:
        pass

    table = (
        Table("c06_extension", MetaData(), Column("value", Text, primary_key=True))
        if mode == "async-extension"
        else Table("c06_memory", MetaData(), Column("user_id", Text, primary_key=True), Column("agent_name", Text, primary_key=True), Column("fact_id", Text, primary_key=True), Column("content", Text))
    )
    mapper.map_imperatively(CallbackRow, table)
    row = CallbackRow()
    if mode == "async-extension":
        row.value = "orm-late"
        before = await extension_rows(item)
    else:
        row.user_id, row.agent_name, row.fact_id, row.content = item.spec.user_id, "lead", "orm-late", "late"
        before = await memory_rows(item)

    async def operation():
        with remote_mutation_scope(transactions.context):
            if mode == "async-extension":
                async with transactions.async_transaction() as session:
                    session.add(row)
            else:

                def callback():
                    with transactions.sync() as session:
                        session.add(row)

                await asyncio.to_thread(callback)

    try:
        await _expire_blocked_writer(item, transactions._capability, operation, text("LOCK TABLE " + table.name + " IN SHARE MODE"), sync=mode == "sync-memory")
        assert (await extension_rows(item) if mode == "async-extension" else await memory_rows(item)) == before
    finally:
        mapper.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["status", "progress", "model", "completion", "finalize", "start"])
async def test_actual_primary_run_table_upgrade_wait_expiry_rejects(mutations, operation):
    from .test_c06_remote_agent_runtime import durable_rows, write

    item = mutations
    before = await durable_rows(item.engine)

    async def perform():
        if operation == "finalize":
            return await item.runs.finalize_if_not_cancelled(item.spec.run_id, status="success")
        if operation == "start":
            return await item.runs.start_owned_run(item.spec.run_id, thread_id=item.spec.thread_id, user_id=item.spec.user_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
        return await write(item, operation)

    await _expire_blocked_writer(item, item.runs._mutation_capability, perform, text("LOCK TABLE runs IN SHARE MODE"))
    # Lease setup changes only authority fields; no output/status/model mutation may commit.
    import json

    def without_lease(rows):
        return {table: [{key: value for key, value in json.loads(row).items() if key != "lease_expires_at"} for row in values] for table, values in rows.items()}

    assert without_lease(await durable_rows(item.engine)) == without_lease(before)


@pytest.mark.asyncio
async def test_actual_thread_create_unique_wait_expiry_rejects(mutations):
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    item = mutations
    async with item.engine.begin() as conn:
        template = dict((await conn.execute(text("SELECT * FROM threads_meta"))).mappings().one())
        await conn.execute(text("DELETE FROM threads_meta"))

    async def perform():
        await item.threads.create(item.spec.thread_id, user_id=item.spec.user_id, display_name="late")

    await _expire_blocked_writer(item, item.threads._mutation_capability, perform, ThreadMetaRow.__table__.insert().values(**template))
    async with item.engine.connect() as conn:
        assert dict((await conn.execute(text("SELECT * FROM threads_meta"))).mappings().one()) == template


@pytest.mark.parametrize("policy", ["fail_closed", "fail_open", "opaque-private-policy", None])
def test_public_execution_configuration_strips_private_memory_backend_config(monkeypatch, policy):
    from app.fleet.runner_context import execution_configuration
    from deerflow.config.app_config import AppConfig

    private = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "memory": {"backend_config": {"dsn": "host=localhost dbname=memory user=private password=opaque-private-bearer", "batch_size": 7, "failure_policy": {"read": policy, "private": "opaque-policy-secret"}}},
        }
    )
    original = private.memory.model_dump()
    public, _ = execution_configuration(private)
    assert public.memory.backend_config == ({"failure_policy": {"read": policy}} if policy in ("fail_closed", "fail_open") else {})
    assert "opaque-private-bearer" not in public.model_dump_json()
    assert private.memory.model_dump() == original

    from types import SimpleNamespace

    from deerflow.agents.lead_agent.prompt import _get_memory_context
    from deerflow.agents.memory import MemoryManagerError

    def unavailable(**kwargs):
        raise MemoryManagerError("private backend unavailable")

    monkeypatch.setattr("deerflow.agents.memory.get_memory_manager", lambda: SimpleNamespace(get_context=unavailable))
    if policy == "fail_closed":
        with pytest.raises(MemoryManagerError, match="private backend unavailable"):
            _get_memory_context(app_config=public, user_id="original-user")
    else:
        assert _get_memory_context(app_config=public, user_id="original-user") == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry,dispatch_path,caller",
    [(entry, path, caller) for caller in ("host", "bootstrap") for path in ("detached", "awaited") for entry in ("close", "build-failure")]
    + [
        ("settled-build-failure", "detached", "host"),
        ("early-build-failure", "detached", "host"),
        ("early-budget-failure", "detached", "host"),
        ("native-memory-close", "native-queue", "host"),
        ("native-memory-build-failure", "native-queue", "host"),
        ("native-memory-stop-enqueue", "native-queue", "host"),
        ("native-memory-observer-roundtrip", "native-queue", "host"),
        ("native-memory-stop-observer-roundtrip", "native-queue", "host"),
    ],
)
async def test_actual_host_teardown_keeps_resources_until_owned_sql_cleanup(extension_profile, monkeypatch, entry, dispatch_path, caller, tmp_path):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    import sqlalchemy.ext.asyncio as async_sql
    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine

    import app.fleet.runner_context as host
    import deerflow.extensions as extensions_api
    import deerflow.extensions.gateway as gateway
    import deerflow.extensions.notify as notify
    import deerflow.mcp.tools as mcp_tools
    import deerflow.runtime.checkpointer.async_provider as checkpoint_api
    import deerflow.runtime.store.async_provider as store_api
    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.base import Base
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item = extension_profile
    effects, engines = [], []
    entered, cleaning, release, done = (asyncio.Event() for _ in range(4))
    async with item.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    private = AppConfig.model_validate({**item.private.model_dump(), "agent_storage": {"backend": "db"}, "run_events": {"backend": "db"}, "memory": {"enabled": True, "manager_class": "noop"}})
    async with store_api.make_store(private):
        pass
    monkeypatch.setattr(host, "validate_model_bindings", lambda *args: None)
    monkeypatch.setattr(host, "validate_runtime_configuration", lambda *args: None)
    monkeypatch.setattr(host, "validate_secret_bindings", lambda *args: (set(), {}))
    from .c08_contract_fixture import install_empty_workspace_contract

    installed_bundle = install_empty_workspace_contract(monkeypatch, tmp_path, private)
    monkeypatch.setattr(host, "runtime_bundle", lambda: installed_bundle)
    monkeypatch.setattr(host, "installed_compatibility", lambda: SimpleNamespace(runtime_digest=item.spec.runtime_digest, skill_snapshot=item.spec.skill_snapshot, plugin_snapshot=item.spec.plugin_snapshot, workspace_contract_version=1))
    original_read = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda path: b"{}" if str(path) == "/opt/deerflow/model-bindings.json" else original_read(path))
    monkeypatch.setattr(extensions_api, "load_extensions", lambda *args: (item.extensions, []))

    async def no_mcp():
        return []

    monkeypatch.setattr(mcp_tools, "get_mcp_tools", no_mcp)
    original_engine = async_sql.create_async_engine

    def create_engine(*args, **kwargs):
        engine = original_engine(*args, **kwargs)
        engines.append(engine)
        return engine

    monkeypatch.setattr(async_sql, "create_async_engine", create_engine)
    original_async_dispose, original_sync_dispose = AsyncEngine.dispose, Engine.dispose

    async def dispose(engine, *args, **kwargs):
        if engine in engines:
            effects.append("async-engine-dispose")
        await original_async_dispose(engine, *args, **kwargs)

    def sync_dispose(engine, *args, **kwargs):
        effects.append("sync-engine-dispose")
        return original_sync_dispose(engine, *args, **kwargs)

    monkeypatch.setattr(AsyncEngine, "dispose", dispose)
    monkeypatch.setattr(Engine, "dispose", sync_dispose)
    for module, name, label in ((store_api, "make_store", "store-exit"), (checkpoint_api, "make_checkpointer", "checkpointer-exit")):
        original = getattr(module, name)

        def tracked_factory(*args, _original=original, _label=label, **kwargs):
            @asynccontextmanager
            async def tracked():
                async with _original(*args, **kwargs) as value:
                    try:
                        yield value
                    finally:
                        effects.append(_label)

            return tracked()

        monkeypatch.setattr(module, name, tracked_factory)

    async def stop_service():
        effects.append("service-stop")

    monkeypatch.setattr(item.service, "stop", stop_service)
    original_reset = notify.reset_extension_notify_loop

    def reset():
        effects.append("notify-reset")
        return original_reset()

    monkeypatch.setattr(notify, "reset_extension_notify_loop", reset)
    original_release = notify.release_extension_dispatch_failures

    def release_failures():
        effects.append("failure-release")
        return original_release()

    monkeypatch.setattr(notify, "release_extension_dispatch_failures", release_failures)

    if entry.startswith("native-memory-"):
        monkeypatch.syspath_prepend(str(Path(__file__).parent / "fixtures/c04-runtime-plugin"))
        from contextlib import contextmanager

        from deerflow_c04_fixture.memory import PostgresMemory

        from deerflow.agents.memory.manager import get_memory_manager
        from deerflow.runtime.execution.mutation_transactions import BoundMutationTransactions

        async with item.engine.begin() as conn:
            await conn.execute(text("CREATE TABLE c06_memory(user_id text NOT NULL,agent_name text NOT NULL,fact_id text NOT NULL,content text NOT NULL,PRIMARY KEY(user_id,agent_name,fact_id))"))
        private = AppConfig.model_validate({**private.model_dump(), "memory": {"enabled": True, "manager_class": "deerflow_c04_fixture.memory:PostgresMemory"}})
        native_entered, native_release = threading.Event(), threading.Event()
        flush_false, native_done = threading.Event(), threading.Event()
        managers = []
        observer_ready, owner_loop = asyncio.Event(), asyncio.get_running_loop()
        observer_waiting = threading.Event()
        roundtrip = entry in {"native-memory-observer-roundtrip", "native-memory-stop-observer-roundtrip"}
        original_sync, original_flush, original_close = BoundMutationTransactions.sync, PostgresMemory.shutdown_flush, PostgresMemory.close

        @contextmanager
        def held_memory_transaction(transactions, **kwargs):
            with original_sync(transactions, **kwargs) as session:
                try:
                    yield session
                finally:
                    if transactions._operation == "memory.write" and (not roundtrip or session.execute(text("SELECT content FROM c06_memory")).scalar_one() == "roundtrip second fact"):
                        native_entered.set()
                        native_release.wait(10)
            if transactions._operation == "memory.write":
                native_done.set()

        def short_flush(manager, timeout):
            result = original_flush(manager, min(timeout, 0.01))
            if not result:
                flush_false.set()
            elif roundtrip:
                owner_loop.call_soon_threadsafe(observer_ready.set)
            return result

        def track_close(manager):
            effects.append("memory-close")
            return original_close(manager)

        monkeypatch.setattr(BoundMutationTransactions, "sync", held_memory_transaction)
        monkeypatch.setattr(PostgresMemory, "shutdown_flush", short_flush)
        monkeypatch.setattr(PostgresMemory, "close", track_close)

        def enqueue():
            manager = environment.private_memory_manager if environment is not None else get_memory_manager()
            managers.append(manager)
            manager.add_nowait(item.spec.thread_id, ["native memory durable fact"], user_id=item.spec.user_id)

        original_start = gateway.start_services

        async def start_and_fail(*args, **kwargs):
            await original_start(*args, **kwargs)
            enqueue()
            assert await asyncio.to_thread(native_entered.wait, 5)
            raise ValueError("actual native memory build failure")

        async def stop_enqueue():
            effects.append("service-stop")
            enqueue()
            if entry == "native-memory-stop-observer-roundtrip":
                assert await asyncio.to_thread(observer_waiting.wait, 5)
                assert await asyncio.to_thread(native_done.wait, 5)

        if entry == "native-memory-build-failure":
            monkeypatch.setattr(gateway, "start_services", start_and_fail)
        if entry in {"native-memory-stop-enqueue", "native-memory-stop-observer-roundtrip"}:
            monkeypatch.setattr(item.service, "stop", stop_enqueue)
        environment, pending = None, None
        if entry == "native-memory-build-failure":
            closer = asyncio.create_task(host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant))
        else:
            environment = await host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant)
            if roundtrip:
                manager = environment.private_memory_manager

                async def observer():
                    observer_waiting.set()
                    await observer_ready.wait()
                    manager.add(item.spec.thread_id, ["roundtrip second fact"], user_id=item.spec.user_id)

                def extract(messages):
                    if messages[-1] == "native memory durable fact":
                        owner_loop.call_soon_threadsafe(observer_ready.clear)
                        assert notify.dispatch_system_model_observation(observer(), "native-memory-roundtrip", mutation_context=item.capability.context)
                        return "roundtrip first fact"
                    return messages[-1]

                manager._extract = extract
            if entry in {"native-memory-close", "native-memory-observer-roundtrip"}:
                with environment.mutation_scope():
                    enqueue()
            closer = asyncio.create_task(environment.close())
        try:
            if entry == "native-memory-stop-observer-roundtrip":
                assert await asyncio.to_thread(observer_waiting.wait, 5)
                assert await asyncio.to_thread(native_done.wait, 5)
                assert await asyncio.to_thread(native_entered.wait, 0.2), "Post-service drain waited for observer without flushing its native memory dependency"
            assert await asyncio.to_thread(native_entered.wait, 5)
            assert await asyncio.to_thread(flush_false.wait, 5)
            completed, _ = await asyncio.wait({closer}, timeout=0.05)
            assert not completed, "Host treated shutdown_flush(False) as native worker quiescence"
            assert any(worker.is_alive() for worker in managers[0]._threads)
            assert not any(effect in effects for effect in ("memory-close", "notify-reset", "failure-release", "store-exit", "checkpointer-exit", "async-engine-dispose", "sync-engine-dispose"))
            closer.cancel()
            with pytest.raises(host.AgentCleanupPending) as caught:
                await closer
            pending = caught.value
            deadline = pending._teardown.budget.deadline
            assert item.capability.context in host._pending_agent_teardowns
        finally:
            native_release.set()
            observer_ready.set()
            assert await asyncio.to_thread(native_done.wait, 5)
            if pending is not None:
                with pending.cleanup_scope(), pytest.raises(OwnershipRejected, match="memory worker drain is incomplete"):
                    await pending.retry_cleanup()
                assert pending._teardown.budget.deadline == deadline
            elif not closer.done():
                try:
                    await closer
                except ValueError:
                    pass
            else:
                try:
                    closer.result()
                except ValueError:
                    pass
        assert all(not worker.is_alive() for worker in managers[0]._threads)
        assert item.capability.context not in host._pending_agent_teardowns
        assert "memory-close" in effects and "store-exit" in effects and "checkpointer-exit" in effects and "async-engine-dispose" in effects
        async with item.engine.begin() as conn:
            expected = "roundtrip second fact" if roundtrip else "native memory durable fact"
            assert (await conn.execute(text("SELECT content FROM c06_memory"))).scalar_one() == expected
            await conn.execute(text("SELECT run_id FROM runs FOR UPDATE NOWAIT"))
        return

    if entry == "early-budget-failure":
        from dataclasses import replace

        from deerflow_ecs_fleet.worker.agent_cleanup import isolated_cleanup_policy
        from sqlalchemy.ext.asyncio import async_sessionmaker

        from deerflow.runtime.execution.mutation_context import _current_mutation_context, current_remote_mutation_context, remote_mutation_scope
        from deerflow.runtime.execution.mutation_transactions import BoundMutationTransactions

        original = ValueError("original early retained construction failure")
        observed, scope_matches = [], []

        def fail_checkpoint(*args, **kwargs):
            raise original

        async def held_dispose(engine, *args, **kwargs):
            if engine not in engines:
                return await original_async_dispose(engine, *args, **kwargs)
            scope_matches.append(current_remote_mutation_context() == item.capability.context)
            transactions = BoundMutationTransactions(item.capability, operation="extension.write", session_factory=async_sessionmaker(engine))
            try:
                with remote_mutation_scope(item.capability.context):
                    async with transactions.async_transaction() as session:
                        await session.execute(text("INSERT INTO c06_extension(value) VALUES ('early-cleanup-rollback')"))
                        entered.set()
                        try:
                            await release.wait()
                        finally:
                            raise asyncio.CancelledError("actual early resource rollback")
            finally:
                await original_async_dispose(engine, *args, **kwargs)
                effects.append("early-engine-dispose-done")
                done.set()

        monkeypatch.setattr(checkpoint_api, "make_checkpointer", fail_checkpoint)
        monkeypatch.setattr(AsyncEngine, "dispose", held_dispose)
        pending = None
        token = _current_mutation_context.set(None)
        try:
            with isolated_cleanup_policy(observed.append, lambda: pytest.fail("ordinary host must not abort")):
                builder = asyncio.create_task(host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant))
        finally:
            _current_mutation_context.reset(token)
        try:
            await asyncio.wait_for(entered.wait(), 5)
            assert len(observed) == 1, "Early construction cleanup bypassed first-teardown budget"
            assert scope_matches == [True], "Early resource cleanup lost its original private scope"
            builder.cancel()
            with pytest.raises(host.AgentCleanupPending) as caught:
                await builder
            pending = caught.value
            assert pending._original_error is original and not done.is_set()
            deadline = pending._teardown.budget.deadline
            for foreign in (None, replace(item.capability.context, attempt_id="foreign-early-retry")):
                token = _current_mutation_context.set(foreign)
                try:
                    with pytest.raises(OwnershipRejected, match="original private scope"):
                        await pending.retry_cleanup()
                finally:
                    _current_mutation_context.reset(token)
            assert pending._teardown.budget.deadline == deadline == observed[0]
        finally:
            release.set()
            await asyncio.wait_for(done.wait(), 5)
            if not builder.done():
                try:
                    await builder
                except (ValueError, host.AgentCleanupPending):
                    pass
            if pending is not None:
                with pending.cleanup_scope(), pytest.raises(asyncio.CancelledError):
                    await pending.retry_cleanup()
                assert pending._teardown.budget.deadline == observed[0]
        assert item.capability.context not in host._pending_agent_teardowns
        assert not any(row == "early-cleanup-rollback" for row in await extension_rows(item))
        async with item.engine.begin() as conn:
            await conn.execute(text("SELECT run_id FROM runs FOR UPDATE NOWAIT"))
        return

    if entry == "early-build-failure":
        original = ValueError("original early host construction failure")
        secondary = OwnershipRejected("secondary immediately settled engine disposal failure")

        def fail_checkpoint(*args, **kwargs):
            raise original

        async def fail_dispose(engine, *args, **kwargs):
            await dispose(engine, *args, **kwargs)
            if engine in engines:
                raise secondary

        monkeypatch.setattr(checkpoint_api, "make_checkpointer", fail_checkpoint)
        monkeypatch.setattr(AsyncEngine, "dispose", fail_dispose)
        with pytest.raises(ValueError) as caught:
            await host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant)
        assert caught.value is original and caught.value.__cause__ is secondary
        assert effects == ["async-engine-dispose", "sync-engine-dispose", "failure-release"]
        assert item.capability.context not in host._pending_agent_teardowns
        return

    if entry == "settled-build-failure":
        original = ValueError("original immediately settled host build failure")
        secondary = OwnershipRejected("secondary immediately settled stop failure")
        original_start = gateway.start_services

        async def fail_after_start(*args, **kwargs):
            await original_start(*args, **kwargs)
            raise original

        async def fail_stop():
            effects.append("service-stop")
            raise secondary

        monkeypatch.setattr(gateway, "start_services", fail_after_start)
        monkeypatch.setattr(item.service, "stop", fail_stop)
        with pytest.raises(ValueError) as caught:
            await host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant)
        assert caught.value is original and caught.value.__cause__ is secondary
        assert "service-stop" in effects and "store-exit" in effects and "checkpointer-exit" in effects and "async-engine-dispose" in effects
        assert item.capability.context not in host._pending_agent_teardowns
        return

    async def writer():
        try:
            async with item.service.deps.mutation_transactions.async_transaction() as session:
                await session.execute(text("INSERT INTO c06_extension(value) VALUES ('unsettled-host-observer')"))
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cleaning.set()
                    await release.wait()
        finally:
            done.set()

    awaited = None

    async def launch_writer():
        nonlocal awaited
        if dispatch_path == "detached":
            assert notify.dispatch_system_model_observation(writer(), "host-teardown-SQL", mutation_context=item.extensions.mutation_context)
        else:

            async def cross_loop():
                await notify._notify_each_on_extension_loop(item.extensions.task_lifecycle, "on_task_start", lambda contributor: writer(), "host-teardown-awaited", None)

            awaited = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(cross_loop())))
        await asyncio.wait_for(entered.wait(), 5)

    original_start = gateway.start_services

    async def fail_start(*args, **kwargs):
        await original_start(*args, **kwargs)
        await launch_writer()
        monkeypatch.setattr(asyncio, "timeout", lambda budget: original_timeout(0.01 if budget == 30 else budget))
        raise ValueError("actual host service bootstrap failure")

    if entry == "build-failure":
        monkeypatch.setattr(gateway, "start_services", fail_start)
    original_drain, original_timeout = notify.drain_extension_dispatches, asyncio.timeout

    async def short_drain(**kwargs):
        kwargs["timeout"] = min(0.01, kwargs.get("timeout") or 0.01)
        return await original_drain(**kwargs)

    monkeypatch.setattr(notify, "drain_extension_dispatches", short_drain)
    environment, rejected, early, bootstrap_task, bootstrap_returned = None, None, None, None, False
    try:
        if caller == "bootstrap":
            from dataclasses import asdict

            from deerflow_ecs_fleet.worker import agent_runner as runner

            async def build_environment(provider, **kwargs):
                return await host.build_agent_environment(**kwargs)

            async def run_agent(self, *args, **kwargs):
                await launch_writer()
                monkeypatch.setattr(asyncio, "timeout", lambda budget: original_timeout(0.01 if budget == 30 else budget))

            monkeypatch.setattr(runner, "build_environment", build_environment)
            monkeypatch.setattr(runner.AgentRunner, "run", run_agent)
            monkeypatch.setattr(runner.os, "readlink", lambda path: "pid:[test-owner-loop]")
            payload = {"bootstrap": {"schema_version": 1, "identity": asdict(item.identity), "operator_config": private.model_dump()}, "grant": item.grant}
            bootstrap_task = asyncio.create_task(runner._bootstrap(payload, "gateway"))
            await asyncio.wait_for(cleaning.wait(), 5)
            try:
                await asyncio.wait_for(asyncio.shield(bootstrap_task), 0.1)
                bootstrap_returned = True
            except TimeoutError:
                pass
            except OwnershipRejected as error:
                rejected = error
                bootstrap_returned = True
            assert not done.is_set()
            early = list(effects)
        elif entry == "close":
            environment = await host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant)
            await launch_writer()
            monkeypatch.setattr(asyncio, "timeout", lambda budget: original_timeout(0.01 if budget == 30 else budget))
            with pytest.raises(OwnershipRejected) as caught:
                await environment.close()
        else:
            with pytest.raises(OwnershipRejected) as caught:
                await host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant)
        if caller == "host":
            rejected = caught.value
            await asyncio.wait_for(cleaning.wait(), 5)
            assert not done.is_set()
            early = list(effects)
            from dataclasses import replace

            from deerflow.runtime.execution.mutation_context import _current_mutation_context

            assert host._pending_agent_teardowns[item.capability.context] is rejected._teardown
            deadline = rejected._teardown.budget.deadline
            for foreign in (None, replace(item.capability.context, attempt_id="foreign-cleanup-attempt")):
                token = _current_mutation_context.set(foreign)
                try:
                    with pytest.raises(OwnershipRejected, match="original private scope"):
                        await rejected.retry_cleanup()
                finally:
                    _current_mutation_context.reset(token)
            assert rejected._teardown.budget.deadline == deadline
            assert list(effects) == early
    finally:
        release.set()
        await asyncio.wait_for(done.wait(), 5)
        if bootstrap_task is not None:
            try:
                await asyncio.wait_for(bootstrap_task, 5)
            except OwnershipRejected:
                assert entry == "close"
            except ValueError as error:
                assert entry == "build-failure"
                assert str(error) == "actual host service bootstrap failure"
        if awaited is not None:
            try:
                await awaited
            except (OwnershipRejected, TimeoutError, asyncio.CancelledError):
                pass
        if environment is not None:
            try:
                await environment.close()
            except OwnershipRejected:
                pass
        elif rejected is not None and hasattr(rejected, "retry_cleanup"):
            try:
                await rejected.retry_cleanup()
            except OwnershipRejected:
                pass
        try:
            await original_drain(timeout=1)
        except OwnershipRejected:
            pass
        original_reset()
    assert item.capability.context not in host._pending_agent_teardowns
    assert not bootstrap_returned, "Actual bootstrap returned before original owner-loop SQL cleanup settled"
    assert early == [], "Host closed bound resources before actual SQL rollback: " + str(early)
    assert "service-stop" in effects and "store-exit" in effects and "checkpointer-exit" in effects and "async-engine-dispose" in effects
    assert not any(row[0] == "unsettled-host-observer" for row in await extension_rows(item))
    async with item.engine.begin() as conn:
        await conn.execute(text("SELECT run_id FROM runs FOR UPDATE NOWAIT"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry,dispatch_path",
    [(entry, path) for entry in ("close", "build-failure") for path in ("detached", "awaited")]
    + [("early-build-failure", "resource-close"), ("native-memory-close", "native-queue"), ("native-memory-build-failure", "native-queue"), ("stream-cleanup", "actual-graph")],
)
async def test_actual_isolated_bootstrap_deadline_aborts_before_sql_resource_unwind(extension_profile, tmp_path, entry, dispatch_path):
    import json
    import os
    import sys
    import time
    from dataclasses import asdict

    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.base import Base
    from deerflow.runtime.store.async_provider import make_store

    item = extension_profile
    async with item.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    private = AppConfig.model_validate({**item.private.model_dump(), "agent_storage": {"backend": "db"}, "run_events": {"backend": "db"}, "memory": {"enabled": True, "manager_class": "noop"}})
    async with make_store(private):
        pass
    if entry == "stream-cleanup":
        await item.writer.adelete_thread(item.spec.thread_id)
        async with item.engine.begin() as conn:
            await conn.execute(text("UPDATE runs SET status='pending'"))
    if entry.startswith("native-memory-"):
        async with item.engine.begin() as conn:
            await conn.execute(text("CREATE TABLE c06_memory(user_id text NOT NULL,agent_name text NOT NULL,fact_id text NOT NULL,content text NOT NULL,PRIMARY KEY(user_id,agent_name,fact_id))"))
        private = AppConfig.model_validate({**private.model_dump(), "memory": {"enabled": True, "manager_class": "deerflow_c04_fixture.memory:PostgresMemory"}})
    trace = tmp_path / "actual-cleanup-process.jsonl"
    payload = {"bootstrap": {"schema_version": 1, "identity": asdict(item.identity), "operator_config": private.model_dump(mode="json")}, "grant": item.grant}
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).with_name("c06_cleanup_process_fixture.py")),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": os.pathsep.join(
                [
                    str(Path(__file__).parent / "fixtures/c04-runtime-plugin"),
                    os.environ.get("PYTHONPATH", ""),
                    str(Path(__file__).resolve().parents[2]),
                    *(str(Path(__file__).resolve().parents[2] / "packages" / name) for name in ("harness", "ecs-fleet", "extension-api")),
                ]
            ),
        },
    )
    started = time.monotonic()
    communication = asyncio.create_task(process.communicate((json.dumps({"payload": payload, "trace": str(trace), "entry": entry, "dispatch": dispatch_path, "budget": 0.3}) + "\n").encode()))
    exceeded = False
    try:
        completed, _ = await asyncio.wait({communication}, timeout=5)
        if not completed:
            exceeded = True
            process.kill()
        stdout, stderr = await communication
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    events = [json.loads(line) for line in trace.read_text().splitlines()] if trace.exists() else []
    assert any(row["event"] == "writer-entered" for row in events), stderr.decode()
    assert any(row["event"] == "writer-cleaning" for row in events), stderr.decode()
    if entry == "stream-cleanup":
        assert any(row["event"] == "graph-custom-published" for row in events), stderr.decode()
        assert not any(row["event"] == "run-agent-returned" for row in events)
    assert not exceeded, "Actual isolated bootstrap exceeded the trusted total cleanup deadline"
    assert process.returncode == 70, (process.returncode, stdout.decode(), stderr.decode())
    assert time.monotonic() - started < 8
    assert not {"service-stop", "store-exit", "checkpointer-exit", "async-engine-dispose", "sync-engine-dispose", "writer-done"} & {row["event"] for row in events}
    writer_pid = next(row["database_pid"] for row in events if row["event"] == "writer-entered")
    async with item.engine.begin() as conn:
        assert not (await conn.execute(text("SELECT value FROM c06_extension WHERE value='cleanup-harddeadline-probe'"))).all()
        if entry.startswith("native-memory-"):
            assert not (await conn.execute(text("SELECT fact_id FROM c06_memory"))).all()
        assert not (await conn.execute(text("SELECT 1 FROM pg_stat_activity WHERE pid=:pid"), {"pid": writer_pid})).all()
        await conn.execute(text("SELECT run_id FROM runs FOR UPDATE NOWAIT"))


def test_cleanup_budget_first_deadline_is_immutable_and_host_has_no_abort(monkeypatch):
    from deerflow_ecs_fleet.worker import agent_cleanup as cleanup

    now, observed, aborted = [10.0], [], []
    monkeypatch.setattr(cleanup.time, "monotonic", lambda: now[0])
    assert cleanup.TOTAL_CLEANUP_SECONDS == 120
    budget = cleanup.CleanupBudget()
    assert budget._deadline is None
    cleanup.abort_isolated_cleanup()  # Ordinary host never installs self-exit.
    with cleanup.isolated_cleanup_policy(observed.append, lambda: aborted.append(True)):
        assert budget.start() == 130
        now[0] = 25
        assert budget.start() == 130
        assert budget.remaining() == 105
        now[0] = 140
        assert budget.remaining() == 0
    cleanup.abort_isolated_cleanup()
    assert observed == [130] and aborted == []


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["build-failure", "run-failure", "settled-run-failure", "settled-cancelled-run", "settled-close-only"])
async def test_actual_bootstrap_preserves_original_error_after_pending_cleanup(extension_profile, monkeypatch, entry):
    from contextlib import nullcontext
    from dataclasses import asdict
    from types import SimpleNamespace

    from deerflow_ecs_fleet.worker import agent_runner as runner
    from deerflow_ecs_fleet.worker.agent_cleanup import PendingAgentCleanup

    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    item, effects = extension_profile, []
    original = asyncio.CancelledError("original trusted bootstrap cancellation") if entry == "settled-cancelled-run" else ValueError("original trusted bootstrap failure")

    class Pending(PendingAgentCleanup):
        remaining_seconds = 1

        def cleanup_scope(self):
            return nullcontext()

        async def wait_for_cleanup(self):
            effects.append("settled")

        async def retry_cleanup(self):
            effects.append("unwound")
            raise OwnershipRejected("secondary settled cleanup failure")

    secondary = OwnershipRejected("secondary immediately settled close failure")

    async def close():
        if entry in {"settled-run-failure", "settled-cancelled-run", "settled-close-only"}:
            effects.append("unwound")
            raise secondary
        raise Pending("safe pending cleanup")

    async def build(*args, **kwargs):
        if entry == "build-failure":
            try:
                raise original
            except ValueError:
                try:
                    await close()
                except Pending as pending:
                    pending._original_error = original
                    raise
        return SimpleNamespace(close=close)

    async def run(*args, **kwargs):
        if entry != "settled-close-only":
            raise original

    monkeypatch.setattr(runner, "build_environment", build)
    monkeypatch.setattr(runner.AgentRunner, "run", run)
    monkeypatch.setattr(runner.os, "readlink", lambda path: "pid:[fixture]")
    payload = {"bootstrap": {"schema_version": 1, "identity": asdict(item.identity), "operator_config": item.private.model_dump()}, "grant": item.grant}
    expected = secondary if entry == "settled-close-only" else original
    with pytest.raises(type(expected)) as caught:
        await runner._bootstrap(payload, "gateway")
    assert caught.value is expected
    assert effects == (["unwound"] if entry in {"settled-run-failure", "settled-cancelled-run", "settled-close-only"} else ["settled", "unwound"])
    if entry in {"settled-run-failure", "settled-cancelled-run"}:
        assert caught.value.__cause__ is secondary


@pytest.mark.asyncio
async def test_host_cleanup_cancellation_retains_same_phase_and_original_error(extension_profile):
    from contextlib import AsyncExitStack, nullcontext

    from app.fleet import runner_context as host

    entered, release, effects = asyncio.Event(), asyncio.Event(), []
    stack = AsyncExitStack()
    stack.callback(lambda: effects.append("resources"))
    teardown = host._AgentResourceTeardown(stack, nullcontext, extension_profile.capability.context)

    async def stop():
        effects.append("stop-entered")
        entered.set()
        await release.wait()
        effects.append("stop-done")

    teardown.stop_plugins = stop
    closer = asyncio.create_task(teardown.close())
    await entered.wait()
    deadline = teardown.budget.deadline
    closer.cancel()
    with pytest.raises(host.AgentCleanupPending) as caught:
        await closer
    original = teardown.failure
    assert isinstance(original, asyncio.CancelledError)
    assert effects == ["stop-entered"]
    assert teardown.context in host._pending_agent_teardowns
    release.set()
    with pytest.raises(asyncio.CancelledError) as cancelled:
        await caught.value.retry_cleanup()
    assert cancelled.value is original
    assert effects == ["stop-entered", "stop-done", "resources"]
    assert teardown.closed and teardown.budget.deadline == deadline
    assert teardown.context not in host._pending_agent_teardowns


def test_isolated_watchdog_arms_only_at_teardown_and_finish_disarms():
    import time

    from deerflow_ecs_fleet.worker.agent_runner import _IsolatedCleanupWatchdog

    before = set(threading.enumerate())
    watchdog = _IsolatedCleanupWatchdog()
    assert watchdog._deadline is None and set(threading.enumerate()) == before
    deadline = time.monotonic() + 120
    watchdog.observe(deadline)
    watchdog.observe(deadline + 60)
    assert watchdog._deadline == deadline
    watchdog.finish()
    owned = [thread for thread in threading.enumerate() if thread not in before and thread.name == "agent-cleanup-deadline"]
    for thread in owned:
        thread.join(1)
        assert not thread.is_alive()


@pytest.mark.asyncio
async def test_actual_host_stream_phase_retains_original_scope_budget_and_sql_rollback(extension_profile, monkeypatch, tmp_path):
    from dataclasses import replace
    from types import SimpleNamespace

    import app.fleet.runner_context as host
    import deerflow.extensions as extension_api
    import deerflow.mcp.tools as mcp_tools
    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.base import Base
    from deerflow.runtime.execution.mutation_context import ExecutionCleanupPending, OwnershipRejected, _current_mutation_context
    from deerflow.runtime.store.async_provider import make_store

    item = extension_profile
    async with item.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    private = AppConfig.model_validate({**item.private.model_dump(), "agent_storage": {"backend": "db"}, "run_events": {"backend": "db"}, "memory": {"enabled": True, "manager_class": "noop"}})
    async with make_store(private):
        pass
    for name in ("validate_model_bindings", "validate_runtime_configuration"):
        monkeypatch.setattr(host, name, lambda *args: None)
    monkeypatch.setattr(host, "validate_secret_bindings", lambda *args: (set(), {}))
    from .c08_contract_fixture import install_empty_workspace_contract

    installed_bundle = install_empty_workspace_contract(monkeypatch, tmp_path, private)
    monkeypatch.setattr(host, "runtime_bundle", lambda: installed_bundle)
    monkeypatch.setattr(host, "installed_compatibility", lambda: SimpleNamespace(runtime_digest=item.spec.runtime_digest, skill_snapshot=item.spec.skill_snapshot, plugin_snapshot=item.spec.plugin_snapshot, workspace_contract_version=1))
    original_read = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda path: b"{}" if str(path) == "/opt/deerflow/model-bindings.json" else original_read(path))
    monkeypatch.setattr(extension_api, "load_extensions", lambda *args: (item.extensions, []))

    async def no_mcp():
        return []

    monkeypatch.setattr(mcp_tools, "get_mcp_tools", no_mcp)
    environment = await host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant)
    cleaning, release, done = (asyncio.Event() for _ in range(3))
    before = await extension_rows(item)
    effects = []
    original_stop = item.service.stop

    async def stop():
        assert done.is_set(), "Service stop preceded actual graph SQL rollback"
        effects.append("stop")
        await original_stop()

    monkeypatch.setattr(item.service, "stop", stop)

    async def writer():
        try:
            async with item.service.deps.mutation_transactions.async_transaction() as session:
                await session.execute(text("INSERT INTO c06_extension(value) VALUES ('owned-stream-rollback')"))
                try:
                    yield
                finally:
                    cleaning.set()
                    await release.wait()
                    raise asyncio.CancelledError("Actual stream SQL rollback")
        finally:
            done.set()

    stream = writer()
    pending, closing = None, None
    try:
        for context in (None, replace(item.capability.context, attempt_id="foreign")):
            token = _current_mutation_context.set(context)
            try:
                with pytest.raises(OwnershipRejected, match="original private scope"):
                    await environment.context.settle_stream(stream)
            finally:
                _current_mutation_context.reset(token)
        assert stream.ag_frame is not None and not cleaning.is_set()
        with environment.mutation_scope():
            await anext(stream)
            closing = asyncio.create_task(environment.context.settle_stream(stream))
        await asyncio.wait_for(cleaning.wait(), 5)
        closing.cancel()
        with pytest.raises(ExecutionCleanupPending) as caught:
            await closing
        pending = caught.value.__cause__
        assert isinstance(pending, host.AgentCleanupPending)
        deadline = pending._teardown.budget.deadline
        task = pending._teardown._phases["graph-stream"]
        assert not task.done() and not done.is_set() and not effects
        for context in (None, replace(item.capability.context, attempt_id="foreign")):
            token = _current_mutation_context.set(context)
            try:
                with pytest.raises(OwnershipRejected, match="original private scope"):
                    await pending.retry_cleanup()
            finally:
                _current_mutation_context.reset(token)
        assert pending._teardown.budget.deadline == deadline
        assert pending._teardown._phases["graph-stream"] is task
    finally:
        release.set()
        if pending is not None:
            wait_error = None
            with pending.cleanup_scope():
                try:
                    await pending.wait_for_cleanup()
                except BaseException as error:
                    wait_error = error
                finally:
                    with pytest.raises(asyncio.CancelledError):
                        await pending.retry_cleanup()
            assert wait_error is None, "Settled graph error bypassed the remaining bound resource cleanup"
        else:
            await asyncio.gather(stream.aclose(), return_exceptions=True)
            await environment.close()
    assert done.is_set() and stream.ag_frame is None and effects == ["stop"]
    assert item.capability.context not in host._pending_agent_teardowns
    rows = await extension_rows(item)
    assert "owned-stream-rollback" not in rows
    assert all(row in rows for row in before)
    async with item.engine.begin() as conn:
        await conn.execute(text("SELECT run_id FROM runs FOR UPDATE NOWAIT"))
