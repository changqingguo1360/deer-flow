"""Exercise the real generic task service against a staged Fleet driver."""

import importlib
import importlib.util

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import job_service_class, setup_tracking, spec


@pytest.mark.integration
@pytest.mark.asyncio
async def test_generic_task_service_commits_before_fleet_activation(fleet_database, tmp_path):
    assert importlib.util.find_spec("deerflow_ecs_fleet.mcp_driver") is not None, "Fleet task driver missing"
    from app.mcp_tasks.service import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskReference, TaskSubmitRequest
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    read = await setup_tracking(engine, sf)
    jobs = job_service_class()(sf, fleet.config, tracking_reader=read)
    driver = importlib.import_module("deerflow_ecs_fleet.mcp_driver").FleetTaskDriver(jobs)
    registry = McpTaskDriverRegistry()
    registry.register("fleet", driver)
    service = McpTaskService(repository=McpTaskRepository(sf), drivers=registry, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    request = TaskSubmitRequest(user_id="u", thread_id="t", run_id="run", tool_call_id="tool", server_name="fleet", task_name="batch", arguments=spec().model_dump(), local_task_id="tracking", driver_data={"invocation_id": "invocation"})
    created = await service.submit(driver_name="fleet", request=request)
    handle = await jobs.get(created["remote_task_id"], user_id="u", thread_id="t")
    assert handle["state"] == "staged"
    reference = TaskReference.from_record(created)
    snapshot = await driver.get_status(reference)
    assert snapshot.status.value == "submitted"
    assert (await jobs.get(handle["id"], user_id="u", thread_id="t"))["state"] == "queued"
    cancelled = await driver.cancel(reference)
    assert cancelled.status.value == "cancelled"
    assert (await jobs.get(handle["id"], user_id="u", thread_id="t"))["state"] == "cancelled"
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_host_registers_driver_and_reconciles_untracked_jobs(fleet_database, tmp_path):
    import asyncio
    from types import SimpleNamespace

    from sqlalchemy import text

    from deerflow.mcp.tasks import McpTaskDriverRegistry

    assert importlib.util.find_spec("app.fleet") is not None, "Fleet host package missing"
    assert importlib.util.find_spec("app.fleet.job_tracking") is not None, "Fleet driver startup binding missing"
    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    await setup_tracking(engine, sf)
    app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", fleet),))))
    registry = McpTaskDriverRegistry()
    importlib.import_module("app.fleet.job_tracking").register_fleet_driver(app, registry)
    assert registry.get("fleet") is not None
    job = await fleet.jobs.submit(user_id="u", thread_id="t", source_run_id="r", tracking_task_id="tracking", idempotency_key="key", spec=spec())
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_jobs SET staged_deadline=clock_timestamp()-interval '1 second'"))
    for _ in range(40):
        observed = await fleet.jobs.get(job["id"], user_id="u", thread_id="t")
        if observed["state"] == "failed":
            break
        await asyncio.sleep(0.05)
    assert observed["state"] == "failed"
    await fleet.stop()
    assert fleet.reconciler is None
