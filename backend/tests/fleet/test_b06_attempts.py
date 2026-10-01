"""Durable start grants and leases fence worker attempts in real Postgres."""

import importlib
import importlib.util

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings
from .test_b04_fleet_foundation import scheduler_class, seed


def attempts_class():
    assert importlib.util.find_spec("deerflow_ecs_fleet.persistence.attempts") is not None, "Durable attempt protocol missing"
    return importlib.import_module("deerflow_ecs_fleet.persistence.attempts").JobAttempts


async def prepare(fleet_database, tmp_path):
    cls = attempts_class()
    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    await seed(engine)
    claim = await scheduler_class()(sf, fleet.config).claim_job("n", node_session_id="s")
    manager = cls(sf, fleet.config)
    args = {"node_id": "n", "node_session_id": "s", "attempt_id": claim.attempt_id, "token": claim.token}
    return fleet, claim, manager, args


@pytest.mark.integration
@pytest.mark.asyncio
async def test_start_is_durable_and_idempotent(fleet_database, tmp_path):
    fleet, claim, manager, args = await prepare(fleet_database, tmp_path)
    first = await manager.authorize_start(**args)
    second = await manager.authorize_start(**args)
    assert first["process_ref"] == second["process_ref"] == "fleet-" + claim.attempt_id
    assert first["launch_spec"]["profile"]["image"] == fleet.config.profiles["batch"].image
    engine, _, _ = fleet_database
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT state,start_authorized_at,execution_deadline,process_ref FROM fleet_attempts"))).one()
        assert row.state == "starting" and row.start_authorized_at is not None
        assert row.execution_deadline > row.start_authorized_at
        assert row.process_ref == first["process_ref"]
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"node_id": "foreign"}, {"token": "invalid"}, {"node_session_id": "old"}])
async def test_wrong_attempt_owner_cannot_start(fleet_database, tmp_path, change):
    fleet, claim, manager, args = await prepare(fleet_database, tmp_path)
    with pytest.raises((PermissionError, ValueError)):
        await manager.authorize_start(**(args | change))
    engine, _, _ = fleet_database
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT start_authorized_at FROM fleet_attempts"))).scalar_one() is None
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cancel_stops_renewal_without_fabricating_stop(fleet_database, tmp_path):
    fleet, claim, manager, args = await prepare(fleet_database, tmp_path)
    await manager.authorize_start(**args)
    engine, _, _ = fleet_database
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_jobs SET cancel_requested_at=clock_timestamp() WHERE id=:id"), {"id": claim.job_id})
    response = await manager.renew(**args, running=True)
    assert response["stop"] and response["reason"] == "cancel_requested"
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT stopped_at FROM fleet_attempts"))).scalar_one() is None
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "reserved"
    await manager.stopped(**args, reason="cancelled", exit_code=137)
    await manager.stopped(**args, reason="cancelled", exit_code=137)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT state FROM fleet_jobs WHERE id=:id"), {"id": claim.job_id})).scalar_one() == "cancelled"
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("started", [False, True])
async def test_expired_attempt_requeues_only_without_start_grant(fleet_database, tmp_path, started):
    fleet, claim, manager, args = await prepare(fleet_database, tmp_path)
    if started:
        await manager.authorize_start(**args)
    engine, _, _ = fleet_database
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=clock_timestamp()-interval '1 second'"))
    await manager.expire_pending()
    async with engine.connect() as conn:
        state = (await conn.execute(text("SELECT state FROM fleet_jobs WHERE id=:id"), {"id": claim.job_id})).scalar_one()
        assert state == ("unknown" if started else "queued")
        charged = (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one()
        assert charged == ("quarantined" if started else "released")
    with pytest.raises(ValueError, match="lease|active"):
        await manager.authorize_start(**args)
    await fleet.stop()
