"""Actual outer-finally faults and owned scope; no Docker physical proof."""

import ast
import asyncio
import copy
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from .test_c08_cleanup_callers import OwnedAbort, OwnedVersions, invoke_under_primary, record_fault, visitor


def wired_finally(module, name, owners, *, inner=False):
    tree = ast.parse(Path(inspect.getfile(module)).read_text())
    function = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
    blocks = sorted((n for n in ast.walk(function) if isinstance(n, ast.Try) and n.finalbody), key=lambda n: n.end_lineno - n.lineno, reverse=True)
    block = blocks[1 if inner else 0]
    wrapper = ast.AsyncFunctionDef(
        name="actual_finally", args=ast.arguments(posonlyargs=[], args=[ast.arg(arg=n) for n in owners], vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]), body=copy.deepcopy(block.finalbody), decorator_list=[]
    )
    namespace = dict(vars(module))
    exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])), inspect.getfile(module), "exec"), namespace)
    return lambda: namespace["actual_finally"](**owners)


class Driver:
    def __init__(self, visit, refs):
        self.visit, self.refs = visit, refs
        self.calls = 0

    async def list_managed(self, node_id):
        assert node_id == "unique-stock-node"
        self.calls += 1
        self.visit("discover:" + str(self.calls))
        return [(ref, {}) for ref in self.refs]

    async def stop(self, ref):
        self.visit("stop:" + ref)

    async def command(self, action, *args, **kwargs):
        self.visit(action + ":" + args[-1])


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["diagnostics", "discover:1", "discover:2", "stop:a", "stop:late", "join", "rm:a", "rm:late"])
@pytest.mark.parametrize("error_type", [ValueError, OwnedAbort])
async def test_actual_stock_finally_discovers_late_owner_and_settles_every_action(monkeypatch, failing, error_type):
    from . import c08_stock_diagnostics
    from . import test_c08_stock_linux_workspace as module

    visited, visit, action = visitor(failing, error_type)
    managed = ["a"]
    driver = Driver(visit, managed)

    async def execution():
        await asyncio.sleep(0)
        managed.append("late")
        visit("execution")

    task = asyncio.create_task(execution())
    monkeypatch.setattr(c08_stock_diagnostics, "capture_precleanup", lambda **kw: visit("diagnostics"))
    versions = OwnedVersions(visit)
    owners = {
        "db": SimpleNamespace(session_factory=object()),
        "record": SimpleNamespace(run_id="stock-run"),
        "driver": driver,
        "refs": ["a"],
        "evidence": Path("/unused"),
        "node_id": "unique-stock-node",
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
    primary = RuntimeError("original stock assertion")
    error = await invoke_under_primary(wired_finally(module, "_run_actual_installed_stock", owners), primary)
    settled = task.done()
    await asyncio.gather(task, return_exceptions=True)
    record_fault("outer-stock", failing, error_type, visited, error, primary, execution_done_at_return=settled, refs=list(owners["refs"]))
    assert {"diagnostics", "execution", "join", "client", "http", "restore-seal", "restore-recover", "user", "config", "fleet", "rm:a"}.issubset(visited)
    if failing != "discover:2":
        assert {"stop:late", "rm:late"}.issubset(visited)
    assert isinstance(error, BaseExceptionGroup) and error.exceptions[0] is primary and settled


def journal_record(run_id, attempt):
    return {"claim": {"kind": "agent", "run_id": run_id, "attempt_id": attempt}, "grant": {"attempt_id": attempt, "process_ref": "fleet-" + attempt, "launch_spec": {"run_id": run_id}}}


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["discover:1", "discover:2", "stop:fleet-child", "join", "rm:fleet-child", "rm:fleet-late"])
@pytest.mark.parametrize("error_type", [ValueError, OwnedAbort])
async def test_actual_sequence_finally_cleans_only_current_child_attempts(failing, error_type):
    from . import c08_installed_sequence as module

    visited, visit, action = visitor(failing, error_type)
    rows = [journal_record("parent-run", "parent"), journal_record("other-run", "other"), journal_record("child-run", "child")]
    count = 0

    def records():
        nonlocal count
        count += 1
        visit("discover:" + str(count))
        return list(rows)

    started = asyncio.Event()

    async def execution():
        started.set()
        try:
            await asyncio.sleep(0)
        finally:
            rows.append(journal_record("child-run", "late"))
            visit("execution")

    task = asyncio.create_task(execution())
    await started.wait()  # real pending owner, already running before the actual finally
    threads = SimpleNamespace(get_paths=object())
    original_paths = object()
    owners = {
        "threads": threads,
        "original_paths": original_paths,
        "record": SimpleNamespace(run_id="child-run"),
        "daemon": SimpleNamespace(journal=SimpleNamespace(records=records)),
        "child_refs": ["fleet-child"],
        "run_task": task,
        "stager": SimpleNamespace(join_writers=action("join")),
        "driver": Driver(visit, []),
    }
    primary = RuntimeError("original child assertion")
    error = await invoke_under_primary(wired_finally(module, "run_sequence", owners), primary)
    settled = task.done()
    await asyncio.gather(task, return_exceptions=True)
    record_fault("outer-sequence", failing, error_type, visited, error, primary, execution_done_at_return=settled, refs=list(owners["child_refs"]), original_paths_restored=threads.get_paths is original_paths)
    assert threads.get_paths is original_paths and settled
    assert {"join", "stop:fleet-child", "rm:fleet-child"}.issubset(visited)
    if failing != "discover:2":
        assert {"stop:fleet-late", "rm:fleet-late"}.issubset(visited)
    assert not any(name in visited for name in ["stop:fleet-parent", "rm:fleet-parent", "stop:fleet-other", "rm:fleet-other"])
    assert isinstance(error, BaseExceptionGroup) and error.exceptions[0] is primary


@pytest.mark.asyncio
async def test_actual_sequence_before_child_admission_never_touches_parent():
    from . import c08_installed_sequence as module

    visited, visit, action = visitor(None, ValueError)
    threads = SimpleNamespace(get_paths=object())
    original_paths = object()
    owners = {
        "threads": threads,
        "original_paths": original_paths,
        "record": None,
        "daemon": SimpleNamespace(journal=SimpleNamespace(records=lambda: [journal_record("parent-run", "parent")])),
        "child_refs": [],
        "run_task": None,
        "stager": SimpleNamespace(join_writers=action("join")),
        "driver": Driver(visit, []),
    }
    primary = RuntimeError("failure before child admission")
    error = await invoke_under_primary(wired_finally(module, "run_sequence", owners), primary)
    assert error is primary and visited == ["join"] and threads.get_paths is original_paths


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["scenario", "node", "resources", "user", "config", "fleet", "node+resources", "scenario+node+resources"])
@pytest.mark.parametrize("error_type", [ValueError, OwnedAbort])
async def test_actual_c07_inner_outer_finally_preserves_primary_and_all_owners(failing, error_type):
    from . import test_c07_remote_agent_runtime as module

    visited = []

    def visit(name):
        visited.append(name)
        if name in failing.split("+"):
            raise error_type(name)

    def action(name):
        async def call():
            visit(name)

        return call

    owners = {
        "scenario": SimpleNamespace(close=action("scenario")),
        "node": SimpleNamespace(close=action("node")),
        "resources": SimpleNamespace(aclose=action("resources")),
        "reset_current_user": lambda token: visit("user"),
        "token": object(),
        "reset_app_config": lambda: visit("config"),
        "fleet": SimpleNamespace(stop=action("fleet")),
    }
    inner = wired_finally(module, "c07_scenario", owners, inner=True)
    outer = wired_finally(module, "c07_scenario", owners)

    async def both():
        try:
            await inner()
        finally:
            await outer()

    primary = RuntimeError("original C07 assertion")
    error = await invoke_under_primary(both, primary)
    record_fault("outer-c07", failing, error_type, visited, error, primary)
    assert visited == ["scenario", "node", "resources", "user", "config", "fleet"]
    assert isinstance(error, BaseExceptionGroup) and error.exceptions[0] is primary


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["stock", "sequence"])
async def test_actual_outer_finally_bounded_cancel_resistance_cannot_hide_late_owner(monkeypatch, surface):
    from . import c08_installed_sequence, c08_stock_diagnostics, test_c08_stock_linux_workspace

    visited, visit, action = visitor(None, ValueError)
    finish, started = asyncio.Event(), asyncio.Event()
    managed = []
    rows = [journal_record("parent-run", "parent")]
    deadlines = []

    async def execution():
        await asyncio.sleep(0)
        managed.append("late")
        rows.append(journal_record("child-run", "late"))
        visit("late-created")
        started.set()
        try:
            await finish.wait()
        except asyncio.CancelledError:
            visit("cancel-resisted")
            await finish.wait()

    task = asyncio.create_task(execution())
    real_wait_for, real_wait = asyncio.wait_for, asyncio.wait

    async def wait_for(awaitable, timeout):
        if timeout == 125:
            deadlines.append(timeout)
            await started.wait()
            raise TimeoutError("owned settlement deadline")
        return await real_wait_for(awaitable, timeout)

    async def wait(tasks, *, timeout, **kwargs):
        if timeout == 5 and task in tasks:
            deadlines.append(timeout)
            return await real_wait(tasks, timeout=0.01, **kwargs)
        return await real_wait(tasks, timeout=timeout, **kwargs)

    monkeypatch.setattr(asyncio, "wait_for", wait_for)
    monkeypatch.setattr(asyncio, "wait", wait)
    monkeypatch.setattr(c08_stock_diagnostics, "capture_precleanup", lambda **kw: visit("diagnostics"))
    if surface == "stock":
        owners = {
            "db": SimpleNamespace(session_factory=object()),
            "record": SimpleNamespace(run_id="stock-run"),
            "driver": Driver(visit, managed),
            "refs": [],
            "evidence": Path("/unused"),
            "node_id": "unique-stock-node",
            "execution": task,
            "stager": SimpleNamespace(join_writers=action("join")),
            "client": SimpleNamespace(close=action("client")),
            "http_client": SimpleNamespace(aclose=action("http")),
            "AgentWorkspaceVersions": OwnedVersions(visit),
            "original_seal": object(),
            "original_recover": object(),
            "reset_current_user": lambda token: visit("user"),
            "token": object(),
            "reset_app_config": lambda: visit("config"),
            "fleet": SimpleNamespace(stop=action("fleet")),
        }
        close = wired_finally(test_c08_stock_linux_workspace, "_run_actual_installed_stock", owners)
        late_ref = "late"
    else:
        owners = {
            "threads": SimpleNamespace(get_paths=object()),
            "original_paths": object(),
            "record": SimpleNamespace(run_id="child-run"),
            "daemon": SimpleNamespace(journal=SimpleNamespace(records=lambda: list(rows))),
            "child_refs": [],
            "run_task": task,
            "stager": SimpleNamespace(join_writers=action("join")),
            "driver": Driver(visit, managed),
        }
        close = wired_finally(c08_installed_sequence, "run_sequence", owners)
        late_ref = "fleet-late"
    primary = RuntimeError("original pending assertion")
    if surface == "sequence":
        await started.wait()  # original sequence cleanup must cancel an actually running owner
    cleanup = asyncio.create_task(invoke_under_primary(close, primary))
    try:
        try:
            error = await real_wait_for(asyncio.shield(cleanup), 0.2)
        except TimeoutError:
            cleanup.cancel()
            finish.set()  # only this test's owner, so the old unbounded join cannot hang pytest
            error = await real_wait_for(asyncio.shield(cleanup), 1)
        pending = not task.done()
        record_fault("outer-pending-" + surface, "cancel-resistance", TimeoutError, visited, error, primary, execution_pending_at_return=pending, actual_wait_timeouts=deadlines)
        assert {"stop:" + late_ref, "rm:" + late_ref, "join", "cancel-resisted"}.issubset(visited)
        assert deadlines == [125, 5] and pending
        assert isinstance(error, BaseExceptionGroup) and error.exceptions[0] is primary
    finally:
        finish.set()
        await real_wait_for(asyncio.gather(task, return_exceptions=True), 1)
