"""Operator reconciliation cannot replace physical stop evidence or rewrite results."""

import asyncio
import importlib
import importlib.util

import pytest
from sqlalchemy import text

from .test_b06_attempts import prepare
from .test_b07_fleet_durable_jobs import sealed_attempt


def require_recovery():
    assert importlib.util.find_spec("deerflow_ecs_fleet.recovery") is not None, "Audited operator reconciliation missing"
    return importlib.import_module("deerflow_ecs_fleet.recovery").FleetRecovery


async def unknown_attempt(fleet_database, tmp_path, *, stopped=False):
    cls = require_recovery()
    fleet, claim, attempts, args = await prepare(fleet_database, tmp_path)
    await attempts.authorize_start(**args)
    await attempts.renew(**args, running=True)
    engine, sf, _ = fleet_database
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=clock_timestamp()-interval '1 second'"))
    await attempts.expire_pending()
    if stopped:
        assert (await attempts.stopped(**args, reason="lease_lost", exit_code=137))["state"] == "unknown"
    manager = cls(sf, attempts=attempts)
    command = dict(job_id=claim.job_id, expected_attempt_id=claim.attempt_id, operator_id="operator", note="Checked stopped process and external effects", side_effects_reviewed=True)
    return fleet, claim, args, manager, command


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unknown_without_stop_cannot_be_resolved(fleet_database, tmp_path):
    fleet, claim, args, manager, command = await unknown_attempt(fleet_database, tmp_path)
    listed = await manager.list_unresolved()
    assert listed[0]["job_id"] == claim.job_id and listed[0]["stop_confirmed"] is False
    with pytest.raises(ValueError, match="stop"):
        await manager.resolve(**command)
    engine, _, _ = fleet_database
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT state FROM fleet_jobs WHERE id=:id"), {"id": claim.job_id})).scalar_one() == "unknown"
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "quarantined"
        assert (await conn.execute(text("SELECT count(*) FROM fleet_recovery_events"))).scalar_one() == 0
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stopped_unknown_resolves_once_and_is_audited(fleet_database, tmp_path):
    fleet, claim, args, manager, command = await unknown_attempt(fleet_database, tmp_path, stopped=True)
    responses = await asyncio.gather(*(manager.resolve(**command) for _ in range(5)))
    assert all(row == responses[0] for row in responses)
    assert responses[0]["state"] == "failed"
    engine, sf, _ = fleet_database
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT operator_id,note,attempt_id,action FROM fleet_recovery_events"))).one()
        assert row.operator_id == "operator" and row.note == command["note"] and row.attempt_id == claim.attempt_id and row.action == "fail_stopped"
        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
    assert (await fleet.attempts.stopped(**args, reason="exit", exit_code=0))["state"] == "failed"
    with pytest.raises(ValueError):
        await fleet.manifests.complete(**args, manifest={"output_prefix": claim.output_prefix + "/sealed", "files": [], "total_bytes": 0})
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT outcome FROM fleet_attempts"))).scalar_one() == {"exit_code": 137, "stop_reason": "lease_lost"}
    assert await manager.list_unresolved() == []
    assert (await fleet.nodes.heartbeat("n", node_session_id="s", protocol_version=1))["health"] == "online"
    with pytest.raises(ValueError):
        await fleet.attempts.authorize_start(**args)
    recreated = require_recovery()(sf, attempts=fleet.attempts)
    assert await recreated.resolve(**command) == responses[0]
    with pytest.raises(ValueError):
        await recreated.resolve(**(command | {"note": "changed"}))
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["wrong_attempt", "not_reviewed", "blank_note", "bad_operator", "unreleased"])
async def test_invalid_operator_resolution_never_changes_unknown(fleet_database, tmp_path, fault):
    fleet, claim, args, manager, command = await unknown_attempt(fleet_database, tmp_path, stopped=True)
    engine, _, _ = fleet_database
    if fault == "wrong_attempt":
        command["expected_attempt_id"] = "old-attempt"
    elif fault == "not_reviewed":
        command["side_effects_reviewed"] = False
    elif fault == "blank_note":
        command["note"] = " "
    elif fault == "bad_operator":
        command["operator_id"] = "../operator"
    else:
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_reservations SET state='quarantined',released_at=NULL"))
    with pytest.raises((ValueError, PermissionError)):
        await manager.resolve(**command)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT state FROM fleet_jobs WHERE id=:id"), {"id": claim.job_id})).scalar_one() == "unknown"
        assert (await conn.execute(text("SELECT count(*) FROM fleet_recovery_events"))).scalar_one() == 0
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_operator_cannot_replace_accepted_success(fleet_database, tmp_path):
    cls = require_recovery()
    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path)
    accepted = await fleet.manifests.complete(**args, manifest=manifest)
    _, sf, _ = fleet_database
    manager = cls(sf, attempts=fleet.attempts)
    with pytest.raises(ValueError):
        await manager.resolve(job_id=claim.job_id, expected_attempt_id=claim.attempt_id, operator_id="operator", note="review", side_effects_reviewed=True)
    assert await fleet.manifests.complete(**args, manifest=manifest) == accepted
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_disabling_new_work_preserves_operator_recovery(fleet_database, tmp_path):
    from deerflow_extension_api import ExtensionRuntimeDeps

    from .test_b02_fleet_foundation import service_class

    fleet, claim, args, manager, command = await unknown_attempt(fleet_database, tmp_path, stopped=True)
    config = fleet.config.model_copy(update={"jobs_enabled": False})
    await fleet.stop()
    _, sf, _ = fleet_database
    restarted = service_class()(config)
    await restarted.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        assert restarted.ready and restarted.recovery is not None
        result = await restarted.recovery.resolve(**command)
        assert result["state"] == "failed"
        assert (await restarted.recovery.events(claim.job_id))[0]["operator_id"] == "operator"
    finally:
        await restarted.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_audit_insert_failure_rolls_back_resolution(fleet_database, tmp_path):
    from sqlalchemy.exc import IntegrityError

    fleet, claim, args, manager, command = await unknown_attempt(fleet_database, tmp_path, stopped=True)
    engine, _, _ = fleet_database
    async with engine.begin() as conn:
        await conn.execute(text("ALTER TABLE fleet_recovery_events ADD CONSTRAINT injected_audit_failure CHECK (operator_id != 'rejected')"))
    with pytest.raises(IntegrityError):
        await manager.resolve(**(command | {"operator_id": "rejected"}))
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT state FROM fleet_jobs WHERE id=:id"), {"id": claim.job_id})).scalar_one() == "unknown"
        assert (await conn.execute(text("SELECT state FROM fleet_attempts"))).scalar_one() == "unknown"
        assert (await conn.execute(text("SELECT count(*) FROM fleet_recovery_events"))).scalar_one() == 0
    assert (await manager.resolve(**command))["state"] == "failed"
    await fleet.stop()
