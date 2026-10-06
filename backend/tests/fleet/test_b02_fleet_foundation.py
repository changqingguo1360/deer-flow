"""Real Postgres schema isolation, migration concurrency and restart persistence."""

import asyncio
import importlib
import importlib.util

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def service_class():
    assert importlib.util.find_spec("deerflow_ecs_fleet.service") is not None, "Fleet startup migrations have not been implemented"
    return importlib.import_module("deerflow_ecs_fleet.service").FleetService


def settings(tmp_path):
    from deerflow_ecs_fleet.config import FleetConfig

    (tmp_path / ".deerflow-fleet-root").write_text("fleet-test\n")
    return FleetConfig(enabled=True, jobs_enabled=True, nas_root=tmp_path, nas_identity="fleet-test", profiles={"batch": {"image": "sha256:" + "a" * 64, "cpu_millis": 1000, "memory_mib": 512}})


@pytest.mark.integration
@pytest.mark.asyncio
async def test_b02_contract(fleet_database, tmp_path):
    engine, sf, _schema = fleet_database
    cls = service_class()
    first, second = cls(settings(tmp_path)), cls(settings(tmp_path))
    await asyncio.gather(first.start(ExtensionRuntimeDeps(session_factory=sf)), second.start(ExtensionRuntimeDeps(session_factory=sf)))
    assert first.ready and second.ready
    async with engine.begin() as conn:
        versions = (await conn.execute(text("SELECT version_num FROM fleet_alembic_version"))).scalars().all()
        assert versions == ["f0011_continuations"]
        tables = await conn.run_sync(lambda sync: inspect(sync).get_table_names())
        assert set(tables) == {
            "fleet_alembic_version",
            "fleet_nodes",
            "fleet_jobs",
            "fleet_attempts",
            "fleet_reservations",
            "fleet_scheduler_tickets",
            "fleet_credentials",
            "fleet_artifact_manifests",
            "fleet_input_manifests",
            "fleet_recovery_events",
            "fleet_job_invocations",
            "fleet_launch_specs",
            "fleet_agent_tasks",
            "fleet_run_placements",
            "fleet_event_outbox",
            "fleet_stream_seals",
            "fleet_workspace_requests",
            "fleet_workspace_manifests",
            "fleet_workspace_points",
            "fleet_workspace_processes",
            "fleet_job_links",
            "fleet_wait_groups",
        }
        await conn.execute(text("INSERT INTO fleet_nodes (id, name, cpu_millis, memory_mib) VALUES ('node-1', 'worker-1', 1000, 512)"))
    await first.stop()
    await second.stop()
    restarted = cls(settings(tmp_path))
    await restarted.start(ExtensionRuntimeDeps(session_factory=sf))
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT name FROM fleet_nodes WHERE id='node-1'"))).scalar_one() == "worker-1"
    await restarted.stop()
    assert not restarted.ready


def test_extension_metadata_is_not_registered_on_host():
    assert importlib.util.find_spec("deerflow_ecs_fleet.persistence") is not None, "Fleet private schema missing"
    from deerflow_ecs_fleet.persistence import models
    from deerflow_ecs_fleet.persistence.base import FleetBase

    from deerflow.persistence.base import Base

    assert models.NodeRow.metadata is FleetBase.metadata
    assert all(not name.startswith("fleet_") for name in Base.metadata.tables)


@pytest.mark.asyncio
async def test_service_rejects_sqlite_before_schema_changes(tmp_path):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        service = service_class()(settings(tmp_path))
        with pytest.raises(ValueError, match="Postgres"):
            await service.start(ExtensionRuntimeDeps(session_factory=async_sessionmaker(engine)))
        async with engine.connect() as conn:
            assert await conn.run_sync(lambda sync: inspect(sync).get_table_names()) == []
        assert not service.ready
    finally:
        await engine.dispose()
