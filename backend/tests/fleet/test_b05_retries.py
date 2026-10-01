"""Driver-owned idempotent task identity survives concurrent submission retries."""

import asyncio
import importlib
from dataclasses import replace

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import job_service_class, setup_tracking, spec


async def service_pair(fleet_database, tmp_path, repo_type=None, driver_type=None):
    from app.mcp_tasks.service import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSubmitRequest
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    reader = await setup_tracking(engine, sf)
    jobs = job_service_class()(sf, fleet.config, tracking_reader=reader)
    cls = driver_type or importlib.import_module("deerflow_ecs_fleet.mcp_driver").FleetTaskDriver
    drivers = McpTaskDriverRegistry()
    drivers.register("fleet", cls(jobs))
    service = McpTaskService(repository=(repo_type or McpTaskRepository)(sf), drivers=drivers, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    request = TaskSubmitRequest(user_id="u", thread_id="t", run_id="r", tool_call_id="same-provider-call", server_name="fleet", task_name="batch", arguments=spec().model_dump(), driver_data={"invocation_id": "durable-invocation"})
    return fleet, jobs, service, request


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retries_return_original_tracking_record(fleet_database, tmp_path):
    fleet, jobs, service, request = await service_pair(fleet_database, tmp_path)
    first = await service.submit(driver_name="fleet", request=request)
    second = await service.submit(driver_name="fleet", request=replace(request, local_task_id="different"))
    assert second["id"] == first["id"]
    assert second["remote_task_id"] == first["remote_task_id"]
    assert (await jobs.get(first["remote_task_id"], user_id="u", thread_id="t"))["state"] == "staged"
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_retries_never_cancel_tracked_job(fleet_database, tmp_path):
    fleet, jobs, service, request = await service_pair(fleet_database, tmp_path)
    results = await asyncio.gather(*(service.submit(driver_name="fleet", request=replace(request, local_task_id=f"candidate-{i}")) for i in range(6)))
    assert len({r["id"] for r in results}) == 1
    engine, _, _ = fleet_database
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT cancel_requested_at FROM fleet_jobs"))).scalar_one() is None
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_fails", [False, True])
async def test_persistence_failure_never_activates_job(fleet_database, tmp_path, cancel_fails):
    from deerflow_ecs_fleet.mcp_driver import FleetTaskDriver

    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    class FailingRepository(McpTaskRepository):
        async def create(self, **kwargs):
            raise RuntimeError("injected tracking commit failure")

    class FailingCancelDriver(FleetTaskDriver):
        async def cancel(self, task):
            raise ConnectionError("injected compensation outage")

    fleet, jobs, service, request = await service_pair(fleet_database, tmp_path, FailingRepository, FailingCancelDriver if cancel_fails else None)
    with pytest.raises(RuntimeError, match="tracking commit failure"):
        await service.submit(driver_name="fleet", request=request)
    engine, _, _ = fleet_database
    async with engine.begin() as conn:
        row = (await conn.execute(text("SELECT id,state FROM fleet_jobs"))).one()
        assert row.state == ("staged" if cancel_fails else "cancelled")
        assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
        await conn.execute(text("UPDATE fleet_jobs SET staged_deadline=clock_timestamp()-interval '1 second'"))
    if cancel_fails:
        assert await jobs.reconcile(row.id) == "failed"
    await fleet.stop()
