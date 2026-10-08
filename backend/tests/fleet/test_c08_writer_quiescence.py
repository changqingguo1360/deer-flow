"""Actual native writer settlement; no runner replacement or cancellation proxy."""

import asyncio
import importlib
import threading
import time
from contextlib import nullcontext

import pytest

from deerflow.sandbox.local.local_sandbox import LocalSandbox


def boundary_module():
    try:
        return importlib.import_module("deerflow.runtime.execution.workspace_boundary")
    except ModuleNotFoundError:
        return None


@pytest.mark.asyncio
async def test_cancelled_awaiter_retains_real_native_writer_until_completion(tmp_path):
    module = boundary_module()
    controller = module.WorkspaceWriterController() if module else None
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    sandbox = LocalSandbox("owned-c08")
    original = sandbox.write_file

    def slow_write():
        entered.set()
        release.wait(5)
        try:
            original(str(tmp_path / "native.txt"), "actual native completion")
        finally:
            finished.set()

    async def dispatch():
        with module.workspace_writer_scope(controller) if module else nullcontext():
            if module:
                return await module.run_native_writer(slow_write)
            return await asyncio.to_thread(slow_write)

    task = asyncio.create_task(dispatch())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not finished.is_set()
        assert controller is not None and controller.active_count == 1, "cancelled awaiter lost real native writer ownership"
        with pytest.raises(TimeoutError):
            await controller.close_and_wait(deadline=time.monotonic() + 0.02)
        assert controller.active_count == 1
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)
    await controller.close_and_wait(deadline=time.monotonic() + 2)
    assert controller.active_count == 0
    assert (tmp_path / "native.txt").read_text() == "actual native completion"


@pytest.mark.asyncio
async def test_closed_scope_rejects_original_sync_writers_and_reopens_partial(tmp_path):
    module = boundary_module()
    controller = module.WorkspaceWriterController() if module else None
    sandbox = LocalSandbox("owned-c08")
    if controller:
        await controller.close_and_wait(deadline=time.monotonic() + 2)
    with module.workspace_writer_scope(controller) if module else nullcontext():
        with pytest.raises(Exception, match="closed"):
            sandbox.write_file(str(tmp_path / "blocked.txt"), "must reject")
    controller.reopen(controller.barrier_epoch)
    with module.workspace_writer_scope(controller):
        sandbox.update_file(str(tmp_path / "allowed.txt"), b"allowed partial reopen")
    await controller.close_and_wait(deadline=time.monotonic() + 2, final=True)
    with pytest.raises(Exception, match="final"):
        controller.reopen(controller.barrier_epoch)
    assert not (tmp_path / "blocked.txt").exists()
    assert (tmp_path / "allowed.txt").read_bytes() == b"allowed partial reopen"


@pytest.mark.asyncio
async def test_partial_barrier_does_not_start_original_cleanup_budget():
    from deerflow_ecs_fleet.worker.agent_cleanup import CleanupBudget

    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController

    budget = CleanupBudget()
    controller = WorkspaceWriterController()
    epoch = await controller.close_and_wait(deadline=time.monotonic() + 2)
    assert budget._deadline is None
    controller.reopen(epoch)
    await asyncio.sleep(0.02)
    assert budget._deadline is None


@pytest.mark.asyncio
async def test_gate_close_race_keeps_reserved_native_and_rejects_new_call(tmp_path):
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, run_native_writer, workspace_writer_scope

    controller = WorkspaceWriterController()
    entered, release = threading.Event(), threading.Event()

    def real_writer():
        entered.set()
        release.wait(5)
        LocalSandbox("reserved").update_file(str(tmp_path / "reserved.bin"), b"reserved")

    with workspace_writer_scope(controller):
        native = asyncio.create_task(run_native_writer(real_writer))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            closing = asyncio.create_task(controller.close_and_wait(deadline=time.monotonic() + 2))
            while controller.barrier_epoch == 0:
                await asyncio.sleep(0)
            with pytest.raises(Exception, match="closed"):
                await run_native_writer(LocalSandbox("rejected").write_file, str(tmp_path / "new.txt"), "reject")
            assert not closing.done()
        finally:
            release.set()
        await native
        epoch = await closing
    assert (tmp_path / "reserved.bin").read_bytes() == b"reserved"
    assert not (tmp_path / "new.txt").exists()
    controller.reopen(epoch)


def test_unbound_original_local_writer_remains_available(tmp_path):
    sandbox = LocalSandbox("unbound")
    sandbox.write_file(str(tmp_path / "local.txt"), "original behavior")
    sandbox.update_file(str(tmp_path / "local.bin"), b"binary")
    assert (tmp_path / "local.txt").read_text() == "original behavior"
    assert (tmp_path / "local.bin").read_bytes() == b"binary"


@pytest.mark.asyncio
async def test_original_host_teardown_keeps_resource_owner_until_cancelled_native_finishes(tmp_path):
    from contextlib import AsyncExitStack

    from app.fleet.runner_context import AgentCleanupPending, _AgentResourceTeardown, _pending_agent_teardowns
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, run_native_writer, workspace_writer_scope

    entered, release = threading.Event(), threading.Event()
    stack = AsyncExitStack()
    closed = []
    stack.callback(lambda: closed.append("actual resource close"))
    controller = WorkspaceWriterController()
    from contextlib import ExitStack, contextmanager

    from deerflow.runtime.execution.mutation_context import RemoteMutationContext, remote_mutation_scope

    identity = RemoteMutationContext("user", "thread", "run", "task", 1, "node", "session", "attempt", "fleet-agent:attempt", "a" * 64, "sha256:" + "b" * 64)

    @contextmanager
    def private_scope():
        with ExitStack() as scopes:
            scopes.enter_context(remote_mutation_scope(identity))
            scopes.enter_context(workspace_writer_scope(controller))
            yield

    teardown = _AgentResourceTeardown(stack, private_scope, identity)
    teardown.workspace_writers = controller

    def native():
        entered.set()
        release.wait(5)
        LocalSandbox("owned").write_file(str(tmp_path / "late.txt"), "late real write")

    with workspace_writer_scope(controller):
        writer = asyncio.create_task(run_native_writer(native))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            writer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await writer
            closing = asyncio.create_task(teardown.close())
            while "workspace-writers" not in teardown._phases:
                await asyncio.sleep(0)
            deadline = teardown.budget.deadline
            closing.cancel()
            with pytest.raises(AgentCleanupPending):
                await closing
            assert not closed and not teardown.closed
            assert _pending_agent_teardowns[teardown.context] is teardown
            assert controller.active_count == 1
            assert teardown.budget.deadline == deadline
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await teardown.close()
    assert closed == ["actual resource close"]
    assert teardown.budget.deadline == deadline
    assert teardown.context not in _pending_agent_teardowns
    assert (tmp_path / "late.txt").read_text() == "late real write"


@pytest.mark.asyncio
async def test_actual_async_activity_retained_until_cancellation_finally_has_joined():
    from deerflow.runtime.execution import workspace_boundary as module

    controller = module.WorkspaceWriterController()
    entered, settling, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    decorate = getattr(module, "settled_workspace_activity", lambda func: func)

    @decorate
    async def original_activity():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            settling.set()
            await release.wait()

    with module.workspace_writer_scope(controller):
        actual_task = asyncio.create_task(original_activity())
        try:
            await entered.wait()
            actual_task.cancel()
            await settling.wait()
            assert not actual_task.done()
            assert controller.active_count == 1, "real async/subagent activity disappeared during cancellation settlement"
            with pytest.raises(TimeoutError):
                await controller.close_and_wait(deadline=time.monotonic() + 0.02)
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await actual_task
    await controller.close_and_wait(deadline=time.monotonic() + 1)
    assert controller.active_count == 0


def test_private_process_registry_requires_original_execution_metadata():
    from deerflow_ecs_fleet.persistence import models
    from deerflow_ecs_fleet.persistence.base import FleetBase

    from deerflow.persistence.base import Base

    table = FleetBase.metadata.tables.get("fleet_workspace_processes")
    assert models.WorkspaceProcessRow.__table__ is table
    assert table is not None, "trusted tool process identities have no durable original-owner registry"
    assert table.name not in Base.metadata.tables
    assert {"attempt_id", "pid", "start_ticks", "role", "tool_execution_id", "start_nonce", "source_digest", "registered_at", "state"} <= set(table.c.keys())
    assert {"attempt_id", "pid", "start_ticks"} == set(table.primary_key.columns.keys())
    assert any("token_stamp" in fk.column_keys and "node_session_id" in fk.column_keys for fk in table.foreign_key_constraints)


def test_supervisor_launch_uses_fixed_installed_stdlib_only_source(monkeypatch):
    from pathlib import Path

    from deerflow.runtime.execution import workspace_process as process

    captured = []

    def failed_start(args, **kwargs):
        captured.append(args)
        raise OSError("real Popen construction failure")

    monkeypatch.setattr(process, "original_workspace_tool_id", lambda: "a" * 64)
    monkeypatch.setattr(process.subprocess, "Popen", failed_start)
    with pytest.raises(OSError, match="real Popen construction failure"):
        process.SupervisedCommand(args=["/bin/bash", "-c", "true"], env={}, stdout=None, stderr=None, register=None, pids_limit=32, retain=None, deadline=time.monotonic() + 1)
    assert captured[0][1:4] == ["-I", "-S", str(Path(process.workspace_supervisor.__file__).resolve())]


def test_supervisor_census_rejects_unverifiable_live_process(monkeypatch):
    from deerflow.runtime.execution import workspace_supervisor as supervisor

    class Entry:
        name = "123"

    class Entries:
        def __enter__(self):
            return iter([Entry()])

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(supervisor.os, "scandir", lambda _: Entries())
    monkeypatch.setattr(supervisor, "process_identity", lambda _: (_ for _ in ()).throw(PermissionError("unverifiable live proc")))
    with pytest.raises(PermissionError, match="unverifiable"):
        supervisor._descendants(1, 32)


def test_supervisor_error_cleanup_reaps_actual_children_before_control_exit(monkeypatch):
    from deerflow.runtime.execution import workspace_supervisor as supervisor

    stop_calls, reap_calls = [], []
    sequence = iter([(22, 9), (0, 0), ChildProcessError()])

    def reap(*args):
        reap_calls.append(args)
        value = next(sequence)
        if isinstance(value, BaseException):
            raise value
        return value

    monkeypatch.setattr(supervisor, "_stop_owned", lambda root, limit: stop_calls.append((root, limit)))
    monkeypatch.setattr(supervisor.os, "waitpid", reap)
    monkeypatch.setattr(supervisor.time, "sleep", lambda _: None)
    method = getattr(supervisor, "_stop_and_reap", None)
    assert method is not None, "supervisor error path abandons physical child handles"
    method(1, 32)
    assert stop_calls and len(reap_calls) == 3


@pytest.mark.asyncio
async def test_process_handles_prevent_reopen_until_exact_shell_and_supervisor_settlement():
    from types import SimpleNamespace

    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController

    settled = []
    controller = WorkspaceWriterController()
    controller.bind_process_registry(SimpleNamespace(settled=lambda record, *, deadline=None: settled.append(record)), pids_limit=32)
    handle = SimpleNamespace(_registered_supervisor=True, _registered_shell=True, supervisor="actual-supervisor", shell="actual-shell", join=lambda deadline: None)
    controller.retain_process(handle)
    epoch = await controller.close_and_wait(deadline=time.monotonic() + 1)
    with pytest.raises(Exception, match="unsettled"):
        controller.reopen(epoch)
    await controller.join_processes(deadline=time.monotonic() + 1)
    assert settled == ["actual-shell", "actual-supervisor"]
    controller.reopen(epoch)


@pytest.mark.asyncio
async def test_concurrent_process_join_retains_owner_after_timeout_then_joins_once():
    from types import SimpleNamespace

    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController

    controller = WorkspaceWriterController()
    entered, release = threading.Event(), threading.Event()
    calls, settled = [], []
    controller.bind_process_registry(SimpleNamespace(settled=lambda record, *, deadline=None: settled.append(record)), pids_limit=32)

    def physical_join(deadline):
        calls.append("actual-native-join")
        entered.set()
        if not release.wait(max(0, deadline - time.monotonic())):
            raise TimeoutError("Actual process handle remains owned")

    handle = SimpleNamespace(_registered_supervisor=True, _registered_shell=True, supervisor="supervisor", shell="shell", join=physical_join)
    controller.retain_process(handle)
    epoch = await controller.close_and_wait(deadline=time.monotonic() + 1)
    with pytest.raises(TimeoutError):
        await controller.join_processes(deadline=time.monotonic() + 0.01)
    assert handle in controller._process_handles and not settled
    with pytest.raises(Exception, match="unsettled"):
        controller.reopen(epoch)
    first = asyncio.create_task(controller.join_processes(deadline=time.monotonic() + 2))
    assert await asyncio.to_thread(entered.wait, 1)
    second = asyncio.create_task(controller.join_processes(deadline=time.monotonic() + 2))
    await asyncio.sleep(0.01)
    release.set()
    await asyncio.gather(first, second)
    assert calls == ["actual-native-join", "actual-native-join"]
    assert settled == ["shell", "supervisor"] and not controller._process_handles
    controller.reopen(epoch)


@pytest.mark.asyncio
async def test_original_teardown_retains_resource_owner_when_physical_process_join_times_out():
    from contextlib import AsyncExitStack, contextmanager
    from types import SimpleNamespace

    from app.fleet.runner_context import AgentCleanupPending, _AgentResourceTeardown, _pending_agent_teardowns
    from deerflow.runtime.execution.mutation_context import RemoteMutationContext, remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    context = RemoteMutationContext("user", "thread", "run", "task", 1, "node", "session", "attempt", "fleet-agent:attempt", "a" * 64, "sha256:" + "b" * 64)
    controller = WorkspaceWriterController()
    controller.bind_process_registry(SimpleNamespace(settled=lambda record, *, deadline=None: None), pids_limit=32)
    closed = []
    stack = AsyncExitStack()
    stack.callback(lambda: closed.append("original-resources"))

    @contextmanager
    def original_scope():
        with remote_mutation_scope(context), workspace_writer_scope(controller):
            yield

    def physical_timeout(deadline):
        raise TimeoutError("Actual process has not physically joined")

    handle = SimpleNamespace(_registered_shell=True, _registered_supervisor=True, shell="shell", supervisor="supervisor", stop_and_join=physical_timeout)
    controller.retain_process(handle)
    teardown = _AgentResourceTeardown(stack, original_scope, context)
    teardown.workspace_writers = controller
    try:
        with pytest.raises(AgentCleanupPending):
            await teardown.close()
        deadline = teardown.budget.deadline
        assert _pending_agent_teardowns[context] is teardown
        assert not teardown.closed and not closed and handle in controller._process_handles
    finally:
        handle.stop_and_join = lambda deadline: None
        cleanup_error = None
        try:
            await teardown.close()
        except TimeoutError as error:
            cleanup_error = error
    assert isinstance(cleanup_error, TimeoutError) and "physically joined" in str(cleanup_error)
    assert teardown.closed and closed == ["original-resources"]
    assert teardown.budget.deadline == deadline
    assert context not in _pending_agent_teardowns


@pytest.mark.asyncio
async def test_closed_gate_rejects_original_async_sandbox_initialization_before_mkdir(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    import deerflow.sandbox.tools as module
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    controller = WorkspaceWriterController()
    await controller.close_and_wait(deadline=time.monotonic() + 1)
    created = tmp_path / "unexpected-user-data"

    @asynccontextmanager
    async def authorized(runtime):
        yield

    async def initialize(runtime):
        created.mkdir()

    monkeypatch.setattr(module, "sandbox_authorization_scope_async", authorized)
    monkeypatch.setattr(module, "ensure_sandbox_initialized_async", initialize)
    with workspace_writer_scope(controller):
        try:
            await module._run_sync_tool_after_async_sandbox_init(lambda runtime: "OK", SimpleNamespace(state={}, context={}, config={}))
        except RuntimeError:
            pass
    assert not created.exists(), "Original initialization wrote user-data before writer admission"


@pytest.mark.asyncio
async def test_original_provider_async_acquire_cancel_retains_actual_native_writer(tmp_path):
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope
    from deerflow.sandbox.sandbox_provider import SandboxProvider

    entered, release = threading.Event(), threading.Event()
    target = tmp_path / "provider-created"

    class Provider(SandboxProvider):
        def acquire(self, thread_id=None, *, user_id=None):
            entered.set()
            assert release.wait(3)
            target.mkdir()
            return "original"

        def get(self, sandbox_id):
            return None

        def release(self, sandbox_id):
            pass

    controller = WorkspaceWriterController()
    with workspace_writer_scope(controller):
        waiter = asyncio.create_task(Provider().acquire_async("thread", user_id="user"))
        try:
            for _ in range(100):
                if entered.is_set():
                    break
                await asyncio.sleep(0.01)
            assert entered.is_set()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert controller.unsettled, "Canceling original provider awaiter hid its real native file writer"
            barrier = asyncio.create_task(controller.close_and_wait(deadline=time.monotonic() + 2))
            await asyncio.sleep(0.03)
            assert not barrier.done()
            release.set()
            await barrier
            assert target.exists() and not controller.unsettled
        finally:
            release.set()
            await asyncio.gather(waiter, return_exceptions=True)
            for _ in range(100):
                if target.exists():
                    break
                await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_actual_local_provider_acquire_cancellation_and_closed_admission(tmp_path, monkeypatch):
    import deerflow.config.paths as paths_module
    from deerflow.runtime.execution.mutation_context import OwnershipRejected
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider

    paths = paths_module.Paths(tmp_path / "original-private-data")
    monkeypatch.setattr(paths_module, "get_paths", lambda: paths)
    # Isolate managed-skill setup; the real Local acquire/Paths mkdir/cache remain.
    monkeypatch.setattr(LocalSandboxProvider, "_ensure_skills_projection", staticmethod(lambda *args, **kwargs: None))
    provider = LocalSandboxProvider()
    entered, release = threading.Event(), threading.Event()
    original_ensure = paths.ensure_thread_dirs

    def original_mkdir(thread, *, user_id=None):
        entered.set()
        assert release.wait(3)
        return original_ensure(thread, user_id=user_id)

    monkeypatch.setattr(paths, "ensure_thread_dirs", original_mkdir)
    controller = WorkspaceWriterController()
    with workspace_writer_scope(controller):
        waiter = asyncio.create_task(provider.acquire_async("original-thread", user_id="original-user"))
        try:
            for _ in range(100):
                if entered.is_set():
                    break
                await asyncio.sleep(0.01)
            assert entered.is_set()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert controller.unsettled
            barrier = asyncio.create_task(controller.close_and_wait(deadline=time.monotonic() + 2))
            await asyncio.sleep(0.03)
            assert not barrier.done()
            release.set()
            await barrier
            assert paths.sandbox_user_data_dir("original-thread", user_id="original-user").is_dir()
            assert len(provider._thread_sandboxes) == 1 and not controller.unsettled
            with pytest.raises(OwnershipRejected):
                provider.acquire("late-thread", user_id="original-user")
            with pytest.raises(OwnershipRejected):
                await provider.acquire_async("late-thread", user_id="original-user")
            assert not paths.sandbox_user_data_dir("late-thread", user_id="original-user").exists()
        finally:
            release.set()
            await asyncio.gather(waiter, return_exceptions=True)
    # Unbound Local keeps the actual cached instance and original return ABI.
    sandbox_id = provider.acquire("original-thread", user_id="original-user")
    assert provider.get(sandbox_id) is not None
