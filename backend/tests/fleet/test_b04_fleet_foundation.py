"""Independent Postgres transactions compete for one shared resource budget."""

import asyncio
import importlib
import importlib.util

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings


def scheduler_class():
    assert importlib.util.find_spec("deerflow_ecs_fleet.scheduler") is not None, "Atomic Fleet scheduler missing"
    return importlib.import_module("deerflow_ecs_fleet.scheduler").FleetScheduler


async def seed(engine, count=3):
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,session_id,health,last_seen_at) VALUES ('n','n',1000,512,'s','online',clock_timestamp())"))
        for number in range(count):
            await conn.execute(
                text("INSERT INTO fleet_jobs (id,user_id,thread_id,tracking_task_id,idempotency_key,spec,state,queue_deadline,queued_at) VALUES (:id,'u','t',:id,:id,:spec,'queued',clock_timestamp()+interval '1 hour',clock_timestamp())"),
                {"id": str(number), "spec": '{"task_name":"batch","profile":"batch","argv":["true"]}'},
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_b04_contract(fleet_database, tmp_path):
    cls = scheduler_class()
    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    await seed(engine)
    scheduler = cls(sf, service.config)
    claims = await asyncio.gather(*(scheduler.claim_job("n", node_session_id="s") for _ in range(6)))
    won = [claim for claim in claims if claim is not None]
    assert len(won) == 1
    assert won[0].token not in repr(won[0])
    async with engine.begin() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT sum(cpu_millis) FROM fleet_reservations WHERE state!='released'"))).scalar_one() == 1000
        await conn.execute(text("UPDATE fleet_reservations SET state='quarantined'"))
        await conn.execute(text("UPDATE fleet_attempts SET state='unknown'"))
    assert await scheduler.claim_job("n", node_session_id="s") is None
    await service.nodes.set_admin_state("n", "draining")
    heartbeat = await service.nodes.heartbeat("n", node_session_id="s", protocol_version=1)
    assert heartbeat["admin_state"] == "draining"
    with pytest.raises(ValueError, match="unreleased"):
        await service.nodes.set_admin_state("n", "disabled")
    with pytest.raises(ValueError, match="history"):
        await service.nodes.delete("n")
    await service.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_release_requires_stop_and_allows_one_new_claim(fleet_database, tmp_path):
    cls = scheduler_class()
    from deerflow_ecs_fleet.persistence.models import AttemptRow
    from deerflow_ecs_fleet.persistence.reservations import release_stopped

    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    await seed(engine)
    scheduler = cls(sf, service.config)
    claim = await scheduler.claim_job("n", node_session_id="s")
    async with sf.begin() as session:
        attempt = await session.get(AttemptRow, claim.attempt_id, with_for_update=True)
        with pytest.raises(ValueError, match="stop"):
            await release_stopped(session, attempt)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET stopped_at=clock_timestamp(),state='cancelled' WHERE id=:id"), {"id": claim.attempt_id})
    async with sf.begin() as session:
        attempt = await session.get(AttemptRow, claim.attempt_id, with_for_update=True)
        assert await release_stopped(session, attempt)
        assert not await release_stopped(session, attempt)
    assert await scheduler.claim_job("n", node_session_id="s") is not None
    await service.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("update", ["memory_mib=511", "cpu_millis=999", "admin_state='draining'", "health='offline'", "last_seen_at=clock_timestamp()-interval '1 hour'"])
async def test_ineligible_node_never_reserves(fleet_database, tmp_path, update):
    cls = scheduler_class()
    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    await seed(engine)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_nodes SET " + update))
    assert await cls(sf, service.config).claim_job("n", node_session_id="s") is None
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations"))).scalar_one() == 0
    await service.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_old_session_and_untracked_jobs_cannot_claim(fleet_database, tmp_path):
    cls = scheduler_class()
    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    await seed(engine)
    scheduler = cls(sf, service.config)
    with pytest.raises(ValueError, match="session"):
        await scheduler.claim_job("n", node_session_id="old")
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_jobs SET state='staged'"))
    assert await scheduler.claim_job("n", node_session_id="s") is None
    await service.stop()
