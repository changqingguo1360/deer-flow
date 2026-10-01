"""Real SQL admission participates with Fleet without starting a local agent."""

import asyncio
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import text

from app.gateway import services
from app.gateway.routers.thread_runs import RunCreateRequest
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.persistence.run import RunRepository, RunRow
from deerflow.runtime import RunManager
from deerflow.runtime.user_context import reset_current_user, set_current_user

from .test_b02_fleet_foundation import service_class, settings
from .test_c01_remote_agent_admission import c_config


@pytest_asyncio.fixture
async def admission(fleet_database, tmp_path):
    engine, sf, schema = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync: RunRow.__table__.create(sync))
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "memory": {"enabled": False}}))
    user = SimpleNamespace(id="user-c02", system_role="admin")
    token = set_current_user(user)
    try:
        yield engine, sf, schema, user
    finally:
        reset_current_user(token)
        reset_app_config()
        await fleet.stop()


def body(content="hello", **changes):
    return RunCreateRequest(
        input={"messages": [{"role": "user", "content": content}]}, config={"recursion_limit": 123}, stream_mode=["values", "messages-tuple", "custom"], stream_subgraphs=True, interrupt_before=["tools"], interrupt_after="*", **changes
    )


def request(manager, user):
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    store = InMemoryStore()
    req = SimpleNamespace(
        headers={},
        state=SimpleNamespace(),
        app=SimpleNamespace(
            state=SimpleNamespace(stream_bridge=SimpleNamespace(), run_manager=manager, checkpointer=InMemorySaver(), store=store, run_event_store=MemoryRunEventStore(), run_events_config=None, thread_store=MemoryThreadMetaStore(store))
        ),
    )
    req.state.user = user
    req.state.auth_source = "session"
    return req


def backend():
    from deerflow_ecs_fleet.config import FleetConfig

    from app.fleet.execution import FleetExecutionBackend

    return FleetExecutionBackend(
        config=FleetConfig.model_validate(c_config()),
        profile_name="remote",
        model_name="model-1",
        model_version="v1",
        skill_snapshot={"entries": []},
        plugin_snapshot={"entries": []},
        workspace_manifest_ref="workspace-1",
        secret_refs=[{"name": "MODEL_API_KEY", "reference_id": "opaque-model-key"}],
    )


@pytest.mark.asyncio
async def test_actual_start_run_honors_trusted_remote_backend_without_local_task(admission, monkeypatch):
    _, sf, _, user = admission
    from deerflow.config.run_ownership_config import RunOwnershipConfig

    manager = RunManager(store=RunRepository(sf), run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True))
    worker = AsyncMock()
    monkeypatch.setattr(services, "run_agent", worker)
    # Contract RED: the existing Gateway has no backend injection parameter.
    # This object need never execute before the parameter is wired.
    injected = backend()
    record = await services.start_run(body(), "thread-c02", request(manager, user), execution_backend=injected, idempotency_key="key-c02")
    await asyncio.sleep(0)
    assert record.task is None and record.store_only
    assert record.owner_worker_id is None and record.lease_expires_at is None
    worker.assert_not_awaited()
    reused = await services.start_run(body(), "thread-c02", request(manager, user), execution_backend=backend(), idempotency_key="key-c02")
    assert reused.run_id == record.run_id and reused.idempotency_reused and reused.task is None
    await manager._renew_leases()
    stored = await manager._store.get(record.run_id, user_id=user.id)
    assert stored["owner_worker_id"] is None and stored["lease_expires_at"] is None


@pytest.mark.asyncio
async def test_placement_fault_after_real_run_insert_rolls_back_entire_admission(admission, monkeypatch):
    engine, sf, _, user = admission
    from deerflow_ecs_fleet.persistence.placements import RunPlacements

    observed = []
    original = RunPlacements.create

    async def fail(self, session, **kwargs):
        observed.append((await session.execute(text("SELECT count(*) FROM runs"))).scalar_one())
        await original(self, session, **kwargs)
        raise RuntimeError("placement fault after real insert")

    monkeypatch.setattr(RunPlacements, "create", fail)
    manager = RunManager(store=RunRepository(sf))
    worker = AsyncMock()
    monkeypatch.setattr(services, "run_agent", worker)
    with pytest.raises(RuntimeError, match="placement fault"):
        await services.start_run(body(), "thread-fault", request(manager, user), execution_backend=backend(), idempotency_key="fault-key")
    assert observed == [1]
    async with engine.connect() as conn:
        for table in ("runs", "fleet_agent_tasks", "fleet_launch_specs", "fleet_run_placements"):
            assert (await conn.execute(text("SELECT count(*) FROM " + table))).scalar_one() == 0
    assert manager._runs == {}
    worker.assert_not_awaited()


@pytest.mark.asyncio
async def test_restart_retry_reuses_original_remote_admission_and_rejects_changed_input(admission, monkeypatch):
    engine, sf, _, user = admission
    worker = AsyncMock()
    monkeypatch.setattr(services, "run_agent", worker)
    for i in range(3):
        manager = RunManager(store=RunRepository(sf))
        record = await services.start_run(body(), "thread-retry", request(manager, user), execution_backend=backend(), idempotency_key="retry-key")
        if i == 0:
            first = record.run_id
        assert record.run_id == first and record.idempotency_reused == (i != 0)
        assert record.task is None and record.store_only
    with pytest.raises(ValueError, match="conflict"):
        await services.start_run(body("different input"), "thread-retry", request(manager, user), execution_backend=backend(), idempotency_key="retry-key")
    async with engine.connect() as conn:
        for table in ("runs", "fleet_agent_tasks", "fleet_launch_specs", "fleet_run_placements"):
            assert (await conn.execute(text("SELECT count(*) FROM " + table))).scalar_one() == 1
        payload = (await conn.execute(text("SELECT payload FROM fleet_launch_specs"))).scalar_one()
        assert payload["recursion_limit"] == 123 and payload["interrupt_before"] == ["tools"]
        assert payload["interrupt_after"] == "*" and payload["stream_subgraphs"]
        stored_kwargs = (await conn.execute(text("SELECT kwargs_json FROM runs"))).scalar_one()
        assert "opaque-model-key" not in json.dumps(stored_kwargs)
        assert "input" not in stored_kwargs and "config" not in stored_kwargs
    worker.assert_not_awaited()


@pytest.mark.asyncio
async def test_real_independent_processes_reuse_one_remote_run(admission, tmp_path):
    engine, _, schema, user = admission
    script = tmp_path / "admit.py"
    # Children use real RunRepository + Gateway start_run; no in-process lock
    # or substituted SQL implementation can resolve their race.
    script.write_text("""
import asyncio, json, os, sys
from types import SimpleNamespace
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from deerflow.persistence.run import RunRepository
from deerflow.runtime import RunManager
from deerflow.runtime.user_context import set_current_user
from deerflow.config.app_config import AppConfig, set_app_config
from fleet.test_c02_remote_agent_admission import backend, body, request
from app.gateway.services import start_run
async def main():
    engine = create_async_engine(os.environ['TEST_POSTGRES_URI'], connect_args={'server_settings': {'search_path': sys.argv[1]}})
    sf = async_sessionmaker(engine, expire_on_commit=False)
    set_app_config(AppConfig.model_validate({'sandbox': {'use': 'deerflow.sandbox.local:LocalSandboxProvider'}, 'memory': {'enabled': False}}))
    user = SimpleNamespace(id='user-c02', system_role='admin')
    set_current_user(user)
    manager = RunManager(store=RunRepository(sf))
    try:
        ready = __import__('pathlib').Path(sys.argv[2]) / ('ready-' + sys.argv[3])
        release = ready.parent / 'release'
        ready.write_text('ready')
        deadline = asyncio.get_running_loop().time()+20
        while not release.exists() or release.read_text() != 'released':
            if asyncio.get_running_loop().time()>deadline:
                raise TimeoutError('process release not delivered')
            await asyncio.sleep(.01)
        record = await start_run(body(), 'thread-process', request(manager,user), execution_backend=backend(), idempotency_key='process-key')
        print(json.dumps({'run_id': record.run_id, 'task': record.task is not None, 'owner': record.owner_worker_id}))
    finally:
        await engine.dispose()
asyncio.run(main())
""")
    env = os.environ | {"PYTHONPATH": os.pathsep.join([str(Path(__file__).parents[2]), str(Path(__file__).parents[1]), str(Path(__file__).parents[2] / "packages/ecs-fleet")])}
    children = []
    try:
        for slot in range(3):
            children.append(await asyncio.create_subprocess_exec(os.sys.executable, str(script), schema, str(tmp_path), str(slot), env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE))
        deadline = time.monotonic() + 20
        while not all((tmp_path / ("ready-" + str(slot))).exists() and (tmp_path / ("ready-" + str(slot))).read_text() == "ready" for slot in range(3)):
            if any(child.returncode is not None for child in children) or time.monotonic() > deadline:
                raise AssertionError("Independent admission processes failed to reach release barrier")
            await asyncio.sleep(0.01)
        (tmp_path / "release").write_text("released")
        results = await asyncio.gather(*(asyncio.wait_for(child.communicate(), timeout=45) for child in children))
        outcomes = []
        for child, (stdout, stderr) in zip(children, results, strict=True):
            assert child.returncode == 0, stderr.decode()
            outcomes.append(json.loads(stdout.decode().splitlines()[-1]))
        assert len({item["run_id"] for item in outcomes}) == 1
        assert all(item["task"] is False and item["owner"] is None for item in outcomes)
        async with engine.connect() as conn:
            for table in ("runs", "fleet_agent_tasks", "fleet_launch_specs", "fleet_run_placements"):
                assert (await conn.execute(text("SELECT count(*) FROM " + table))).scalar_one() == 1
            assert (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id "
                        "JOIN fleet_launch_specs s ON s.id=p.launch_spec_ref AND s.run_id=r.run_id "
                        "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id AND t.current_run_id=r.run_id "
                        "WHERE r.user_id=p.user_id AND t.user_id=r.user_id"
                    )
                )
            ).scalar_one() == 1
    finally:
        for child in children:
            if child.returncode is None:
                child.kill()
                await child.wait()


@pytest.mark.parametrize(
    "input",
    [
        {"messages": [{"role": "user", "content": "hello", "id": "msg-1", "name": "alice", "additional_kwargs": {"files": [{"filename": "file.txt"}]}}], "custom": {"keep": [1, 2]}},
        {"messages": ["plain accepted text", ["user", "accepted pair"]]},
    ],
)
def test_actual_gateway_normalized_input_json_roundtrip(input):
    from langchain_core.messages import BaseMessage

    from app.fleet.execution import decode_graph_input, encode_graph_input

    normalized = services.normalize_input(input)
    restored = decode_graph_input(json.loads(json.dumps(encode_graph_input(normalized))))
    assert restored == normalized
    for old, new in zip(normalized["messages"], restored["messages"], strict=True):
        if isinstance(old, BaseMessage):
            assert new.model_dump() == old.model_dump()


@pytest.mark.asyncio
async def test_actual_gateway_resume_command_is_durable_and_roundtrips(admission, monkeypatch):
    engine, sf, _, user = admission
    from langgraph.types import Command

    from app.fleet.execution import decode_graph_input

    worker = AsyncMock()
    monkeypatch.setattr(services, "run_agent", worker)
    record = await services.start_run(body(command={"resume": {"approval": True, "value": [1, 2]}}), "thread-resume", request(RunManager(store=RunRepository(sf)), user), execution_backend=backend())
    async with engine.connect() as conn:
        payload = (await conn.execute(text("SELECT payload FROM fleet_launch_specs"))).scalar_one()
    restored = decode_graph_input(payload["input"])
    assert isinstance(restored, Command) and restored.resume == {"approval": True, "value": [1, 2]}
    assert record.task is None
    worker.assert_not_awaited()


@pytest.mark.asyncio
async def test_remote_plan_rejects_memory_and_client_metadata_cannot_route(monkeypatch):
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    user = SimpleNamespace(id="user-c02", system_role="admin")
    token = set_current_user(user)
    worker = AsyncMock()
    monkeypatch.setattr(services, "run_agent", worker)
    manager = RunManager(store=MemoryRunStore())
    try:
        with pytest.raises(RuntimeError, match="participating SQL"):
            await services.start_run(body(), "thread-memory", request(manager, user), execution_backend=backend())
        assert manager._runs == {}
        local = await services.start_run(body(metadata={"execution_backend": "fleet", "store_only": True}), "thread-local", request(manager, user))
        assert local.task is not None and not local.store_only
        await local.task
        worker.assert_awaited_once()
        await manager.cancel(local.run_id)
    finally:
        reset_current_user(token)
        reset_app_config()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["thread", "user", "config", "streams", "profile", "runtime", "model_version", "secret_refs"])
async def test_remote_idempotency_rejects_changed_stable_identity(admission, monkeypatch, change):
    engine, sf, _, user = admission
    monkeypatch.setattr(services, "run_agent", AsyncMock())
    manager = RunManager(store=RunRepository(sf))
    await services.start_run(body(), "thread-bound", request(manager, user), execution_backend=backend(), idempotency_key="bound-key")
    selected = backend()
    incoming = body()
    thread = "thread-bound"
    token = None
    if change == "thread":
        thread = "thread-other"
    elif change == "user":
        user = SimpleNamespace(id="user-other", system_role="admin")
        token = set_current_user(user)
    elif change == "config":
        incoming.config["recursion_limit"] = 99
    elif change == "streams":
        incoming.stream_mode = ["updates"]
    elif change == "profile":
        selected.profile_name = "other-profile"
    elif change == "runtime":
        selected.profile = selected.profile.model_copy(update={"runtime_digest": "sha256:" + "b" * 64})
    elif change == "model_version":
        selected.model_version = "different-version"
    else:
        selected.secret_refs = [{"name": "MODEL_API_KEY", "reference_id": "other-ref"}]
    try:
        with pytest.raises(ValueError, match="conflict"):
            await services.start_run(incoming, thread, request(manager, user), execution_backend=selected, idempotency_key="bound-key")
    finally:
        if token is not None:
            reset_current_user(token)
    async with engine.connect() as conn:
        for table in ("runs", "fleet_agent_tasks", "fleet_launch_specs", "fleet_run_placements"):
            assert (await conn.execute(text("SELECT count(*) FROM " + table))).scalar_one() == 1


@pytest.mark.asyncio
async def test_real_sql_local_and_remote_admission_keep_same_thread_exclusive(admission, monkeypatch):
    from fastapi import HTTPException

    engine, sf, _, user = admission
    monkeypatch.setattr(services, "run_agent", AsyncMock())
    local_manager = RunManager(store=RunRepository(sf))
    local = await services.start_run(body(), "thread-exclusive", request(local_manager, user))
    await local.task
    with pytest.raises(HTTPException) as exc:
        await services.start_run(body(), "thread-exclusive", request(RunManager(store=RunRepository(sf)), user), execution_backend=backend())
    assert exc.value.status_code == 409
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM fleet_agent_tasks"))).scalar_one() == 0
    await local_manager.cancel(local.run_id)
    remote = await services.start_run(body(), "thread-exclusive", request(RunManager(store=RunRepository(sf)), user), execution_backend=backend())
    with pytest.raises(HTTPException) as exc:
        await services.start_run(body(), "thread-exclusive", request(RunManager(store=RunRepository(sf)), user))
    assert exc.value.status_code == 409
    assert remote.task is None


@pytest.mark.asyncio
async def test_cancelled_participation_rolls_back_and_never_registers_run(admission, monkeypatch):
    engine, sf, _, user = admission
    from deerflow_ecs_fleet.persistence.placements import RunPlacements

    inserted = asyncio.Event()
    original = RunPlacements.create

    async def pause_after_insert(self, session, **kwargs):
        await original(self, session, **kwargs)
        inserted.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(RunPlacements, "create", pause_after_insert)
    manager = RunManager(store=RunRepository(sf))
    task = asyncio.create_task(services.start_run(body(), "thread-cancel-admission", request(manager, user), execution_backend=backend()))
    try:
        await asyncio.wait_for(inserted.wait(), timeout=10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with engine.connect() as conn:
            for table in ("runs", "fleet_agent_tasks", "fleet_launch_specs", "fleet_run_placements"):
                assert (await conn.execute(text("SELECT count(*) FROM " + table))).scalar_one() == 0
        assert manager._runs == {}
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
