"""Real private Agent bootstrap process for bounded SQL cleanup acceptance."""

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine


def main():
    data = json.loads(sys.stdin.buffer.readline())
    trace = Path(data["trace"])

    def record(event, **extra):
        with trace.open("a") as sink:
            sink.write(json.dumps({"event": event, "process_pid": os.getpid(), **extra}) + "\n")
            sink.flush()

    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.worker import agent_runner as runner

    import app.fleet.runner_context as host
    import deerflow.extensions.gateway as gateway
    import deerflow.extensions.notify as notify
    import deerflow.runtime.checkpointer.async_provider as checkpoint_api
    import deerflow.runtime.store.async_provider as store_api
    from deerflow.extensions.registry import ExtensionRegistry

    spec = LaunchSpec.model_validate(data["payload"]["grant"]["launch_spec"])
    entered = asyncio.Event()
    awaited = None
    graph_stream_started = False
    graph_answer_started = False

    class Service:
        remote_state_mode = "transactional"

        async def start(self, deps):
            self.deps = deps

        async def stop(self):
            record("service-stop")

    service = Service()
    registry = ExtensionRegistry()
    with registry.attributed_to("actual-process-SQL"):
        registry.service(service)
        registry.task_lifecycle(service)

    async def writer():
        try:
            async with service.deps.mutation_transactions.async_transaction() as session:
                await session.execute(text("INSERT INTO c06_extension(value) VALUES ('cleanup-harddeadline-probe')"))
                pid = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                record("writer-entered", database_pid=pid)
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    record("writer-cleaning")
                    await asyncio.Event().wait()
        finally:
            record("writer-done")

    async def launch_writer(memory=None):
        nonlocal awaited
        if data["entry"].startswith("native-memory-"):
            from deerflow.agents.memory.manager import get_memory_manager

            (memory or get_memory_manager()).add_nowait(spec.thread_id, ["native subprocess durable memory"], user_id=spec.user_id)
            while not native_entered.is_set():
                await asyncio.sleep(0)
            return
        if data["dispatch"] == "detached":
            notify.dispatch_system_model_observation(writer(), "actual-process-cleanup", mutation_context=service.deps.mutation_transactions._capability.context)
        else:

            async def cross_loop():
                await notify._notify_each_on_extension_loop(registry.build().task_lifecycle, "on_task_start", lambda contributor: writer(), "actual-process-awaited", None)

            awaited = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(cross_loop())))
        await entered.wait()

    import threading
    from contextlib import contextmanager

    from deerflow.runtime.execution.mutation_transactions import BoundMutationTransactions

    native_entered = threading.Event()
    original_native_sync = BoundMutationTransactions.sync

    @contextmanager
    def native_sql_finally(transactions, **kwargs):
        with original_native_sync(transactions, **kwargs) as session:
            try:
                yield session
            finally:
                if transactions._operation == "memory.write":
                    pid = session.execute(text("SELECT pg_backend_pid()")).scalar_one()
                    record("writer-entered", database_pid=pid)
                    native_entered.set()
                    record("writer-cleaning")
                    threading.Event().wait()

    original_start = gateway.start_services

    async def start(*args, **kwargs):
        result = await original_start(*args, **kwargs)
        if data["entry"] in {"build-failure", "native-memory-build-failure"}:
            await launch_writer()
            raise ValueError("owned process bootstrap failure")
        return result

    async def no_mcp():
        return []

    async def run(self, *args, **kwargs):
        nonlocal graph_stream_started
        environment = kwargs["environment"]
        with environment.mutation_scope():
            if data["entry"] == "stream-cleanup":
                from dataclasses import replace

                from langchain_core.messages import HumanMessage
                from langgraph.config import get_stream_writer
                from langgraph.graph import StateGraph

                from deerflow.agents.thread_state import ThreadState
                from deerflow.runtime.execution.mutation_context import OwnershipRejected
                from deerflow.runtime.runs.worker import run_agent

                async def answer(state):
                    nonlocal graph_answer_started
                    graph_answer_started = True
                    record("graph-answer-started")
                    record("graph-custom-enqueued")
                    get_stream_writer()({"stream-cleanup": True})
                    from langchain_core.messages import AIMessage

                    return {"messages": [AIMessage(content="actual graph result")]}

                original_publish = environment.bridge.publish

                async def publish(*args, **kwargs):
                    if len(args) > 1 and args[1] == "custom":
                        await entered.wait()
                        record("graph-custom-published")
                        raise OwnershipRejected("Actual custom stream publication rejected")
                    return await original_publish(*args, **kwargs)

                graph = StateGraph(ThreadState)
                graph.add_node("answer", answer)
                graph.set_entry_point("answer")
                graph.set_finish_point("answer")
                compiled = graph.compile(checkpointer=environment.context.checkpointer)
                original_astream = compiled.astream

                def actual_stream(*args, **kwargs):
                    nonlocal graph_stream_started
                    graph_stream_started = True
                    record("graph-stream-created")
                    return original_astream(*args, **kwargs)

                compiled.astream = actual_stream
                environment.bridge.publish = publish
                current = await environment.manager.attach_existing_executor(spec.run_id, user_id=spec.user_id, thread_id=spec.thread_id, owner_worker_id=environment.identity.owner_worker_id, execution_backend="fleet")
                await run_agent(
                    environment.bridge,
                    environment.manager,
                    current,
                    ctx=replace(environment.context, event_store=None, thread_store=None),
                    agent_factory=lambda *, config: compiled,
                    graph_input={"messages": [HumanMessage(content="actual stream cleanup")]},
                    config={"configurable": {"thread_id": spec.thread_id}},
                    stream_modes=["custom"],
                )
                record("run-agent-returned")
                return
            await launch_writer(environment.private_memory_manager)

    async def build_environment(provider, **kwargs):
        return await host.build_agent_environment(**kwargs)

    original_read = Path.read_bytes
    original_drain = notify.drain_extension_dispatches

    async def short_drain(**kwargs):
        kwargs["timeout"] = min(0.01, kwargs.get("timeout") or 0.01)
        return await original_drain(**kwargs)

    from contextlib import ExitStack

    with ExitStack() as patches:
        if data["entry"] == "stream-cleanup":
            from deerflow.runtime.checkpointer.fenced_saver import FencedAsyncPostgresSaver

            original_cursor = FencedAsyncPostgresSaver._cursor
            holding = False

            @asynccontextmanager
            async def held_checkpoint_cursor(saver, **kwargs):
                nonlocal holding
                async with original_cursor(saver, **kwargs) as cursor:
                    if saver._mutation.get() is None or holding or not graph_answer_started:
                        yield cursor
                        return
                    holding = True
                    await cursor.execute("INSERT INTO c06_extension(value) VALUES ('cleanup-harddeadline-probe')")
                    await cursor.execute("SELECT pg_backend_pid() AS pid")
                    record("writer-entered", database_pid=(await cursor.fetchone())["pid"])
                    entered.set()
                    try:
                        yield cursor
                    finally:
                        record("writer-cleaning")
                        try:
                            await asyncio.Event().wait()
                        except asyncio.CancelledError:
                            await asyncio.Event().wait()

            patches.enter_context(patch.object(FencedAsyncPostgresSaver, "_cursor", held_checkpoint_cursor))
        if data["entry"].startswith("native-memory-"):
            patches.enter_context(patch.object(BoundMutationTransactions, "sync", native_sql_finally))
        for target, value in (
            ("app.fleet.runner_context.validate_model_bindings", lambda *args: None),
            ("app.fleet.runner_context.validate_runtime_configuration", lambda *args: None),
            ("app.fleet.runner_context.validate_secret_bindings", lambda *args: (set(), {})),
            ("app.fleet.runner_context.runtime_bundle", lambda: (b"{}", {})),
            ("app.fleet.runner_context.installed_compatibility", lambda: SimpleNamespace(runtime_digest=spec.runtime_digest, skill_snapshot=spec.skill_snapshot, plugin_snapshot=spec.plugin_snapshot)),
            ("deerflow.extensions.load_extensions", lambda *args: (registry.build(), [])),
            ("deerflow.extensions.gateway.start_services", start),
            ("deerflow.mcp.tools.get_mcp_tools", no_mcp),
            ("deerflow.extensions.notify.drain_extension_dispatches", short_drain),
            ("deerflow_ecs_fleet.worker.agent_runner.build_environment", build_environment),
            ("deerflow_ecs_fleet.worker.agent_runner.AgentRunner.run", run),
        ):
            patches.enter_context(patch(target, value))
        patches.enter_context(patch.object(Path, "read_bytes", lambda path: b"{}" if str(path) == "/opt/deerflow/model-bindings.json" else original_read(path)))
        original_link = os.readlink
        patches.enter_context(patch.object(os, "readlink", lambda path: "pid:[actual-native-process]" if str(path) == "/proc/self/ns/pid" else original_link(path)))
        for module, name, label in ((store_api, "make_store", "store-exit"), (checkpoint_api, "make_checkpointer", "checkpointer-exit")):
            original = getattr(module, name)

            def tracked_factory(*args, _original=original, _label=label, **kwargs):
                @asynccontextmanager
                async def tracked():
                    async with _original(*args, **kwargs) as value:
                        try:
                            yield value
                        finally:
                            record(_label)

                return tracked()

            patches.enter_context(patch.object(module, name, tracked_factory))
        original_async_dispose, original_sync_dispose = AsyncEngine.dispose, Engine.dispose

        async def dispose(engine, *args, **kwargs):
            if data["entry"] == "early-build-failure":
                from deerflow_ecs_fleet.worker.agent_runner import BootstrapV1
                from sqlalchemy.ext.asyncio import async_sessionmaker

                from app.fleet.mutation import FleetMutationCapability
                from deerflow.runtime.execution.mutation_context import current_remote_mutation_context, remote_mutation_scope
                from deerflow.runtime.execution.mutation_transactions import BoundMutationTransactions

                identity = BootstrapV1.from_private_payload(data["payload"]["bootstrap"]).identity
                capability = FleetMutationCapability(identity, spec)
                record("early-resource-scope", matches=current_remote_mutation_context() == capability.context)
                transactions = BoundMutationTransactions(capability, operation="extension.write", session_factory=async_sessionmaker(engine))
                with remote_mutation_scope(capability.context):
                    async with transactions.async_transaction() as session:
                        await session.execute(text("INSERT INTO c06_extension(value) VALUES ('cleanup-harddeadline-probe')"))
                        pid = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                        record("writer-entered", database_pid=pid)
                        try:
                            raise asyncio.CancelledError("early resource cancellation")
                        finally:
                            record("writer-cleaning")
                            await asyncio.Event().wait()
            record("async-engine-dispose")
            await original_async_dispose(engine, *args, **kwargs)

        if data["entry"] == "early-build-failure":

            def fail_checkpoint(*args, **kwargs):
                raise ValueError("actual early process construction failure")

            patches.enter_context(patch.object(checkpoint_api, "make_checkpointer", fail_checkpoint))

        def sync_dispose(engine, *args, **kwargs):
            record("sync-engine-dispose")
            return original_sync_dispose(engine, *args, **kwargs)

        patches.enter_context(patch.object(AsyncEngine, "dispose", dispose))
        patches.enter_context(patch.object(Engine, "dispose", sync_dispose))
        # Trusted fixture-only short budget; operator/model configuration has no key.
        try:
            from deerflow_ecs_fleet.worker import agent_cleanup
        except ImportError:
            pass
        else:
            patches.enter_context(patch.object(agent_cleanup, "TOTAL_CLEANUP_SECONDS", data["budget"]))
        entry = getattr(runner, "isolated_bootstrap_main", runner.bootstrap_main)
        entry(data["payload"], argv=["--provider", "actual-host-fixture"])


if __name__ == "__main__":
    main()
