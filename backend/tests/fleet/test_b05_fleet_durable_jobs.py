"""Staged execution cannot race ahead of durable owner-scoped task tracking."""

import asyncio
import importlib
import importlib.util

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings
from .test_b04_fleet_foundation import scheduler_class


def job_service_class():
    assert importlib.util.find_spec("deerflow_ecs_fleet.job_service") is not None, "Staged Fleet submission missing"
    return importlib.import_module("deerflow_ecs_fleet.job_service").FleetJobService


async def setup_tracking(engine, sf):
    from sqlalchemy import select

    from deerflow.persistence.mcp_tasks.model import McpTaskRow

    async with engine.begin() as conn:
        await conn.run_sync(lambda sync: McpTaskRow.__table__.create(sync))

    async def read(session, task_id):
        row = (await session.execute(select(McpTaskRow).where(McpTaskRow.id == task_id).with_for_update())).scalar_one_or_none()
        return row.to_dict() if row else None

    return read


def spec():
    from deerflow_ecs_fleet.protocol import JobSpec

    return JobSpec(task_name="batch", profile="batch", argv=["true"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_b05_contract(fleet_database, tmp_path):
    cls = job_service_class()
    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    reader = await setup_tracking(engine, sf)
    jobs = cls(sf, service.config, tracking_reader=reader)
    handles = await asyncio.gather(*(jobs.submit(user_id="u", thread_id="t", source_run_id="run", tracking_task_id="tracking", idempotency_key="key", spec=spec()) for _ in range(5)))
    assert len({h["id"] for h in handles}) == 1
    job_id = handles[0]["id"]
    assert await jobs.reconcile(job_id) == "staged"
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,session_id,health,last_seen_at) VALUES ('n','n',1000,512,'s','online',clock_timestamp())"))
    assert await scheduler_class()(sf, service.config).claim_job("n", node_session_id="s") is None
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_jobs SET staged_deadline=clock_timestamp()-interval '1 second'"))
    assert await jobs.reconcile(job_id) == "failed"
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
    await service.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "owner,remote,driver,status,expected",
    [
        ("u", None, "fleet", "submitted", "queued"),
        ("other", None, "fleet", "submitted", "staged"),
        ("u", "wrong", "fleet", "submitted", "staged"),
        ("u", None, "ordinary_mcp", "submitted", "staged"),
        ("u", None, "fleet", "cancelled", "cancelled"),
    ],
)
async def test_tracking_handshake_is_owner_and_handle_scoped(fleet_database, tmp_path, owner, remote, driver, status, expected):
    cls = job_service_class()
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    reader = await setup_tracking(engine, sf)
    jobs = cls(sf, service.config, tracking_reader=reader)
    handle = await jobs.submit(user_id="u", thread_id="t", source_run_id="run", tracking_task_id="tracking", idempotency_key="key", spec=spec())
    repo = McpTaskRepository(sf)
    await repo.create(
        task_id="tracking",
        user_id=owner,
        thread_id="t",
        run_id="run",
        tool_call_id=None,
        server_name="fleet",
        driver_name=driver,
        remote_task_id=remote or handle["id"],
        task_name="batch",
        status=status,
        next_poll_at=None,
        driver_data={},
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
    )
    assert await jobs.reconcile(handle["id"]) == expected
    with pytest.raises(PermissionError):
        await jobs.get(handle["id"], user_id="other", thread_id="t")
    if expected == "queued":
        assert await jobs.reconcile(handle["id"]) == "queued"
    await service.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_submission_key_never_changes_payload(fleet_database, tmp_path):
    cls = job_service_class()
    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    reader = await setup_tracking(engine, sf)
    jobs = cls(sf, service.config, tracking_reader=reader)
    args = dict(user_id="u", thread_id="t", source_run_id="r", tracking_task_id="tracking", idempotency_key="key")
    await jobs.submit(**args, spec=spec())
    with pytest.raises(ValueError, match="different"):
        await jobs.submit(**args, spec=spec().model_copy(update={"argv": ["false"]}))
    with pytest.raises(ValueError, match="continuations"):
        await jobs.submit(**(args | {"tracking_task_id": "second", "idempotency_key": "second"}), spec=spec().model_copy(update={"link_mode": "awaited"}))
    await service.stop()
