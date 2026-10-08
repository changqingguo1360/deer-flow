"""Actual cleanup caller fault coverage; no physical/runtime proof is inferred."""

import ast
import asyncio
import copy
import inspect
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


class OwnedAbort(BaseException):
    pass


class OwnedDriver:
    def __init__(self, visit):
        self.visit = visit
        self.operator_config = {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}

    async def command(self, action, *args, **kwargs):
        self.visit(action + ":" + str(args[-1]))
        return 0, "owned log", ""

    async def list_managed(self, node_id):
        self.visit("discover")
        return [("a", {}), ("b", {}), ("residual", {})]

    async def stop(self, ref):
        self.visit("stop:" + ref)
        return True


class OwnedLog:
    def __init__(self, visit):
        self.visit = visit

    def __truediv__(self, name):
        return self

    def write_text(self, value):
        self.visit("write")


class OwnedVersions:
    def __init__(self, visit):
        object.__setattr__(self, "visit", visit)
        object.__setattr__(self, "seal", "mutated seal")
        object.__setattr__(self, "recover", "mutated recover")

    def __setattr__(self, name, value):
        self.visit("restore-" + name)
        object.__setattr__(self, name, value)


def record_fault(surface, failing, error_type, visited, error, primary, **observed):
    directory = os.environ.get("C08_CALLER_CLEANUP_EVIDENCE_DIR")
    if directory:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        data = {
            "scope": "Caller cleanup fault only, not physical runtime proof",
            "surface": surface,
            "failing": failing,
            "fault_type": error_type.__name__,
            "visited": list(visited),
            "actual_error_type": type(error).__name__,
            "primary_first": isinstance(error, BaseExceptionGroup) and error.exceptions[0] is primary,
            "actual_group_errors": [type(value).__name__ + ":" + str(value) for value in error.exceptions] if isinstance(error, BaseExceptionGroup) else [],
            **observed,
        }
        target = path / (surface + "-" + error_type.__name__ + "-" + failing.replace(":", "-") + ".json")
        with target.open("x") as stream:
            json.dump(data, stream, indent=2)


async def invoke_under_primary(close, primary):
    try:
        try:
            raise primary
        finally:
            await close()
    except BaseException as error:
        return error
    raise AssertionError("Primary failure disappeared")


def visitor(failing, error_type):
    visited = []

    def visit(name):
        visited.append(name)
        if name == failing:
            raise error_type(name)

    def action(name):
        async def execute():
            visit(name)

        return execute

    return visited, visit, action


def assert_owned_error(error, primary, failing):
    assert isinstance(error, BaseExceptionGroup)
    assert error.exceptions[0] is primary
    assert any(str(value) == failing for value in error.exceptions[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["logs:owned", "credentials", "write", "release", "join", "stop:owned", "rm:owned"])
@pytest.mark.parametrize("error_type", [ValueError, OwnedAbort])
async def test_historical_c07_actual_close_settles_every_owner(monkeypatch, failing, error_type):
    from app.fleet import runner_context

    from .c07_installed_scenario import C07InstalledScenario

    visited, visit, action = visitor(failing, error_type)
    scenario = object.__new__(C07InstalledScenario)
    scenario.stop_release = asyncio.Event()
    scenario.grant = {"process_ref": "owned"}
    scenario.driver = OwnedDriver(visit)
    scenario.directory = OwnedLog(visit)
    scenario.publisher = SimpleNamespace(join_writers=action("join"))
    scenario.release_model_barrier = action("release")
    scenario.original_attempt = object()
    scenario.node = SimpleNamespace(attempt=object())

    def credentials(config):
        visit("credentials")
        return []

    monkeypatch.setattr(runner_context, "control_connection_credentials", credentials)

    async def execution():
        visit("execution")

    async def observation():
        await asyncio.Event().wait()

    scenario.execution_task = asyncio.create_task(execution())
    scenario.physical_observer = asyncio.create_task(observation())
    primary = RuntimeError("original historical assertion")
    error = await invoke_under_primary(scenario.close, primary)
    record_fault(
        "historical", failing, error_type, visited, error, primary, execution_done=scenario.execution_task.done(), observer_done=scenario.physical_observer.done(), original_rpc_restored=scenario.node.attempt is scenario.original_attempt
    )
    assert scenario.stop_release.is_set()
    assert scenario.execution_task.done() and scenario.physical_observer.done()
    assert scenario.node.attempt is scenario.original_attempt
    assert {"release", "execution", "join", "logs:owned", "stop:owned", "rm:owned"}.issubset(visited)
    assert_owned_error(error, primary, failing)


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["release", "join", "stop:owned", "rm:owned"])
@pytest.mark.parametrize("error_type", [ValueError, OwnedAbort])
async def test_stock_c07_actual_close_settles_every_owner(failing, error_type):
    from .test_c07_installed_remote_events import InstalledScenario

    visited, visit, action = visitor(failing, error_type)
    scenario = object.__new__(InstalledScenario)
    scenario.claim = object()
    scenario.grant = {"process_ref": "owned"}
    scenario.driver = OwnedDriver(visit)
    scenario.release_model_barrier = action("release")
    scenario.daemon = SimpleNamespace(workspace_publications=SimpleNamespace(join_writers=action("join")))

    async def execution():
        visit("execution")

    scenario.execution_task = asyncio.create_task(execution())
    primary = RuntimeError("original stock assertion")
    error = await invoke_under_primary(scenario.close, primary)
    settled_at_return = scenario.execution_task.done()
    record_fault("stock", failing, error_type, visited, error, primary, execution_done_at_return=settled_at_return)
    await asyncio.gather(scenario.execution_task, return_exceptions=True)
    assert settled_at_return
    assert {"release", "execution", "join", "stop:owned", "rm:owned"}.issubset(visited)
    assert_owned_error(error, primary, failing)


def actual_staged_finally(module, owners):
    """Execute the caller's exact finalbody, not a hand-written serial baseline."""
    source = ast.parse(Path(inspect.getfile(module)).read_text())
    target = next(node for node in source.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "test_actual_installed_original_daemon_http_stages_one_candidate_without_point")
    block = max((node for node in ast.walk(target) if isinstance(node, ast.Try) and node.finalbody), key=lambda node: len(node.finalbody))
    function = ast.AsyncFunctionDef(
        name="owned_finally", args=ast.arguments(posonlyargs=[], args=[ast.arg(arg=name) for name in owners], vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]), body=copy.deepcopy(block.finalbody), decorator_list=[]
    )
    tree = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    namespace = dict(vars(module))
    exec(compile(tree, inspect.getfile(module), "exec"), namespace)
    return lambda: namespace["owned_finally"](**owners)


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["discover", "stop:a", "stop:b", "join", "client", "http", "restore-seal", "restore-recover", "rm:a", "rm:b", "user", "config", "fleet"])
@pytest.mark.parametrize("error_type", [ValueError, OwnedAbort])
async def test_staged_c08_actual_finally_settles_every_owner(failing, error_type):
    from . import test_c08_linux_workspace as module

    visited, visit, action = visitor(failing, error_type)
    versions = OwnedVersions(visit)
    original_seal, original_recover = object(), object()

    async def execution():
        visit("execution")

    task = asyncio.create_task(execution())
    owners = {
        "refs": ["a", "b"],
        "node_id": "owned-node",
        "driver": OwnedDriver(visit),
        "execution": task,
        "stager": SimpleNamespace(join_writers=action("join")),
        "client": SimpleNamespace(close=action("client")),
        "http_client": SimpleNamespace(aclose=action("http")),
        "AgentWorkspaceVersions": versions,
        "original_seal": original_seal,
        "original_recover": original_recover,
        "reset_current_user": lambda token: visit("user"),
        "token": object(),
        "reset_app_config": lambda: visit("config"),
        "fleet": SimpleNamespace(stop=action("fleet")),
    }
    primary = RuntimeError("original staged assertion")
    error = await invoke_under_primary(actual_staged_finally(module, owners), primary)
    settled_at_return = task.done()
    record_fault(
        "staged",
        failing,
        error_type,
        visited,
        error,
        primary,
        execution_done_at_return=settled_at_return,
        seal_restored=versions.seal is original_seal,
        recover_restored=versions.recover is original_recover,
        actual_refs=list(owners["refs"]),
    )
    await asyncio.gather(task, return_exceptions=True)
    assert settled_at_return
    assert {"discover", "stop:a", "stop:b", "execution", "join", "client", "http", "restore-seal", "restore-recover", "rm:a", "rm:b", "user", "config", "fleet"}.issubset(visited)
    if failing != "discover":
        assert {"stop:residual", "rm:residual"}.issubset(visited)
    if failing != "restore-seal":
        assert versions.seal is original_seal
    if failing != "restore-recover":
        assert versions.recover is original_recover
    assert_owned_error(error, primary, failing)


@pytest.mark.asyncio
@pytest.mark.parametrize("discovery_failure", [False, True])
@pytest.mark.parametrize("pending_forever", [False, True])
async def test_staged_pending_execution_late_owned_container_is_stopped_and_removed(monkeypatch, discovery_failure, pending_forever):
    from . import test_c08_linux_workspace as module

    visited, visit, action = visitor(None, ValueError)
    managed = []
    inspected_deadlines = []

    class LateDriver(OwnedDriver):
        calls = 0

        async def list_managed(self, node_id):
            assert node_id == "unique-owned-node"
            self.calls += 1
            visited.append("discover:" + str(self.calls))
            if self.calls == 1 and discovery_failure:
                raise OwnedAbort("initial discovery")
            return [(ref, {}) for ref in managed]

    driver = LateDriver(visit)
    started = asyncio.Event()

    async def execution():
        await asyncio.sleep(0)
        managed.append("late")
        visited.append("late-created")
        started.set()
        if pending_forever:
            await asyncio.Event().wait()
        else:
            raise ValueError("owned execution failed after create")

    task = asyncio.create_task(execution())
    real_wait_for = asyncio.wait_for

    async def bounded_deadline(awaitable, timeout):
        if timeout == 125:
            inspected_deadlines.append(timeout)
            await started.wait()
            if pending_forever:
                raise TimeoutError("owned settlement deadline")
        return await real_wait_for(awaitable, timeout)

    monkeypatch.setattr(module.asyncio, "wait_for", bounded_deadline)
    versions = OwnedVersions(visit)
    owners = {
        "refs": [],
        "node_id": "unique-owned-node",
        "driver": driver,
        "execution": task,
        "stager": SimpleNamespace(join_writers=action("join")),
        "client": SimpleNamespace(close=action("client")),
        "http_client": SimpleNamespace(aclose=action("http")),
        "AgentWorkspaceVersions": versions,
        "original_seal": object(),
        "original_recover": object(),
        "reset_current_user": lambda token: visit("user"),
        "token": object(),
        "reset_app_config": lambda: visit("config"),
        "fleet": SimpleNamespace(stop=action("fleet")),
    }
    primary = RuntimeError("original before grant assertion")
    cleanup = asyncio.create_task(invoke_under_primary(actual_staged_finally(module, owners), primary))
    try:
        error = await real_wait_for(asyncio.shield(cleanup), 0.2)
    except TimeoutError:
        cleanup.cancel()
        error = await cleanup
    settled_at_return = task.done()
    record_fault(
        "late-staged",
        str(discovery_failure) + "-" + str(pending_forever),
        OwnedAbort if discovery_failure else ValueError,
        visited,
        error,
        primary,
        execution_done_at_return=settled_at_return,
        actual_refs=list(owners["refs"]),
        actual_wait_timeouts=inspected_deadlines,
    )
    await asyncio.gather(task, return_exceptions=True)
    assert {"late-created", "stop:late", "rm:late", "join", "client", "http", "restore-seal", "restore-recover", "user", "config", "fleet"}.issubset(visited)
    assert driver.calls >= 2 and settled_at_return
    assert inspected_deadlines == [125]
    assert isinstance(error, BaseExceptionGroup) and error.exceptions[0] is primary
    assert any(isinstance(value, (TimeoutError, ValueError)) for value in error.exceptions[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize("historical", [False, True])
@pytest.mark.parametrize("resists_cancel", [False, True])
async def test_c07_actual_close_late_grant_and_bounded_cancel_still_remove(monkeypatch, historical, resists_cancel):
    from .c07_installed_scenario import C07InstalledScenario
    from .test_c07_installed_remote_events import InstalledScenario

    visited, visit, action = visitor(None, ValueError)
    scenario = object.__new__(C07InstalledScenario if historical else InstalledScenario)
    scenario.grant = None
    scenario.claim = object()
    scenario.driver = OwnedDriver(visit)
    scenario.directory = OwnedLog(visit)
    scenario.publisher = SimpleNamespace(join_writers=action("join"))
    scenario.daemon = SimpleNamespace(workspace_publications=scenario.publisher)
    scenario.release_model_barrier = action("release")
    scenario.stop_release = asyncio.Event()
    scenario.original_attempt = object()
    scenario.node = SimpleNamespace(attempt=object())
    started, finish = asyncio.Event(), asyncio.Event()
    observed_timeouts = []

    async def execution():
        await asyncio.sleep(0)
        scenario.grant = {"process_ref": "owned"}
        visited.append("late-grant")
        started.set()
        if not resists_cancel:
            raise ValueError("original execution error after grant")
        try:
            await finish.wait()
        except asyncio.CancelledError:
            visited.append("cancel-resisted")
            await finish.wait()

    async def observation():
        await asyncio.Event().wait()

    scenario.execution_task = asyncio.create_task(execution())
    if historical:
        scenario.physical_observer = asyncio.create_task(observation())
    real_wait_for, real_wait = asyncio.wait_for, asyncio.wait

    async def bounded_deadline(awaitable, timeout):
        if timeout == 125:
            observed_timeouts.append(timeout)
            await started.wait()
            if resists_cancel:
                raise TimeoutError("owned settlement deadline")
        return await real_wait_for(awaitable, timeout)

    async def cancellation_wait(tasks, *, timeout, **kwargs):
        if timeout == 5 and scenario.execution_task in tasks:
            observed_timeouts.append(timeout)
            return await real_wait(tasks, timeout=0.01, **kwargs)
        return await real_wait(tasks, timeout=timeout, **kwargs)

    monkeypatch.setattr(asyncio, "wait_for", bounded_deadline)
    monkeypatch.setattr(asyncio, "wait", cancellation_wait)
    primary = RuntimeError("original assertion before grant")
    cleanup = asyncio.create_task(invoke_under_primary(scenario.close, primary))
    try:
        try:
            error = await real_wait_for(asyncio.shield(cleanup), 0.2)
        except TimeoutError:
            cleanup.cancel()
            error = await cleanup
        pending_at_return = not scenario.execution_task.done()
        record_fault(
            "late-historical" if historical else "late-stock",
            str(resists_cancel),
            TimeoutError if resists_cancel else ValueError,
            visited,
            error,
            primary,
            execution_pending_at_return=pending_at_return,
            actual_wait_timeouts=observed_timeouts,
            observer_done_at_return=scenario.physical_observer.done() if historical else None,
            original_rpc_restored=scenario.node.attempt is scenario.original_attempt if historical else None,
        )
        assert {"late-grant", "join", "stop:owned", "rm:owned"}.issubset(visited)
        assert isinstance(error, BaseExceptionGroup) and error.exceptions[0] is primary
        if resists_cancel:
            assert observed_timeouts == [125, 5] and pending_at_return
            assert "cancel-resisted" in visited
        else:
            assert observed_timeouts == [125] and not pending_at_return
        if historical:
            assert scenario.physical_observer.done() and scenario.node.attempt is scenario.original_attempt
    finally:
        finish.set()
        await real_wait_for(asyncio.gather(scenario.execution_task, return_exceptions=True), 1)
        if historical and not scenario.physical_observer.done():
            scenario.physical_observer.cancel()
            await real_wait_for(asyncio.gather(scenario.physical_observer, return_exceptions=True), 1)
