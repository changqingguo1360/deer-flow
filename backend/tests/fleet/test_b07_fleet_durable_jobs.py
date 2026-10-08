"""Only current stopped attempts can atomically accept a verified NAS manifest."""

import asyncio
import importlib
import importlib.util
from dataclasses import asdict

import pytest
from sqlalchemy import text

from .test_b06_attempts import prepare


def require_manifests():
    assert importlib.util.find_spec("deerflow_ecs_fleet.persistence.manifests") is not None, "Fleet accepted-manifest transaction missing"


async def sealed_attempt(fleet_database, tmp_path, *, stopped=True, exit_code=0):
    require_manifests()
    fleet, claim, attempts, args = await prepare(fleet_database, tmp_path)
    grant = await attempts.authorize_start(**args)
    await attempts.renew(**args, running=True)
    output = fleet.workspace.prepare(asdict(claim), grant)
    (output / "report.txt").write_text("report ready")
    manifest = fleet.workspace.seal(asdict(claim), grant, stopped=True, max_bytes=1024)
    if stopped:
        await attempts.stopped(**args, reason="exit", exit_code=exit_code)
    return fleet, claim, args, manifest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_b07_contract(fleet_database, tmp_path):
    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path)
    rows = await asyncio.gather(*(fleet.manifests.complete(**args, manifest=manifest) for _ in range(5)))
    assert len({row["manifest_id"] for row in rows}) == 1
    assert all(row["state"] == "succeeded" for row in rows)
    accepted = await fleet.manifests.get(rows[0]["manifest_id"], user_id="u", thread_id="t")
    assert accepted["files"] == manifest["files"]
    for owner, thread in [("other", "t"), ("u", "other-thread")]:
        with pytest.raises(LookupError):
            await fleet.manifests.get(rows[0]["manifest_id"], user_id=owner, thread_id=thread)
    engine, _, _ = fleet_database
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT state FROM fleet_jobs WHERE id=:id"), {"id": claim.job_id})).scalar_one() == "succeeded"
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=clock_timestamp()-interval '1 second'"))
    assert await fleet.manifests.complete(**args, manifest=manifest) == rows[0]
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "expired",
        "unknown",
        "unknown_before_stop",
        "cancelled",
        "old_session",
        "wrong_token",
        "wrong_node",
        "missing_stop",
        "foreign_prefix",
        "digest_mismatch",
        "size_mismatch",
        "symlink",
        "missing_nas",
        "nonzero_exit",
        "not_current",
        "output_budget",
    ],
)
async def test_invalid_completion_never_accepts_manifest(fleet_database, tmp_path, fault):
    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path, stopped=fault not in {"missing_stop", "unknown_before_stop"}, exit_code=1 if fault == "nonzero_exit" else 0)
    engine, _, _ = fleet_database
    async with engine.begin() as conn:
        if fault == "expired":
            await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=clock_timestamp()-interval '1 second'"))
        elif fault in {"unknown", "unknown_before_stop"}:
            await conn.execute(text("UPDATE fleet_jobs SET state='unknown'"))
            await conn.execute(text("UPDATE fleet_attempts SET state='unknown'"))
        elif fault == "cancelled":
            await conn.execute(text("UPDATE fleet_jobs SET cancel_requested_at=clock_timestamp()"))
        elif fault == "not_current":
            await conn.execute(text("UPDATE fleet_jobs SET active_attempt_id='another-attempt'"))
        elif fault == "output_budget":
            await conn.execute(text("UPDATE fleet_attempts SET launch_spec=jsonb_set(launch_spec, '{profile,max_output_bytes}', '1')"))
        elif fault == "old_session":
            await conn.execute(text("UPDATE fleet_nodes SET session_id='new-session'"))
    if fault == "unknown_before_stop":
        assert (await fleet.attempts.stopped(**args, reason="exit", exit_code=0))["state"] == "unknown"
    if fault == "wrong_token":
        args["token"] = "wrong"
    elif fault == "wrong_node":
        args["node_id"] = "other"
    elif fault == "foreign_prefix":
        manifest = manifest | {"output_prefix": "u/other/jobs/0/attempts/foreign/sealed"}
    elif fault == "digest_mismatch":
        manifest["files"][0]["sha256"] = "a" * 64
    elif fault == "size_mismatch":
        manifest["files"][0]["size"] = 1
        manifest["total_bytes"] = 1
    elif fault == "symlink":
        path = tmp_path / manifest["output_prefix"] / "report.txt"
        path.parent.chmod(0o700)
        path.unlink()
        outside = tmp_path / "outside"
        outside.write_text("report ready")
        path.symlink_to(outside)
    elif fault == "missing_nas":
        (tmp_path / ".deerflow-fleet-root").unlink()
    with pytest.raises((PermissionError, ValueError, OSError)):
        await fleet.manifests.complete(**args, manifest=manifest)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT accepted_manifest_id FROM fleet_jobs WHERE id=:id"), {"id": claim.job_id})).scalar_one() is None
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expiry_during_nas_validation_cannot_commit_success(fleet_database, tmp_path):
    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path)
    engine, _, _ = fleet_database
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=clock_timestamp()+interval '.15 second'"))
    original = fleet.workspace.verify_manifest

    def slow_verify(value):
        import time

        time.sleep(0.25)
        return original(value)

    fleet.workspace.verify_manifest = slow_verify
    with pytest.raises(ValueError, match="lease|deadline"):
        await fleet.manifests.complete(**args, manifest=manifest)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 0
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_closing_new_job_flag_keeps_accepted_work_completion_available(fleet_database, tmp_path):
    from deerflow_extension_api import ExtensionRuntimeDeps

    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path)
    _, sf, _ = fleet_database
    config = fleet.config.model_copy(update={"jobs_enabled": False})
    await fleet.stop()
    fleet.config = config
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    assert await fleet.scheduler.claim_job("n", node_session_id="s") is None
    completed = await fleet.manifests.complete(**args, manifest=manifest)
    assert completed["state"] == "succeeded"
    assert (await fleet.manifests.get(completed["manifest_id"], user_id="u", thread_id="t"))["files"] == manifest["files"]
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "wrong", "symlink"])
async def test_service_rejects_wrong_nas_before_migrations(fleet_database, tmp_path, fault):
    from deerflow_extension_api import ExtensionRuntimeDeps

    from .test_b02_fleet_foundation import service_class, settings

    engine, sf, _ = fleet_database
    config = settings(tmp_path)
    sentinel = tmp_path / ".deerflow-fleet-root"
    if fault == "missing":
        sentinel.unlink()
    elif fault == "wrong":
        sentinel.write_text("another-deployment\n")
    else:
        sentinel.unlink()
        outside = tmp_path / "elsewhere"
        outside.write_text("fleet-test\n")
        sentinel.symlink_to(outside)
    fleet = service_class()(config)
    with pytest.raises((ValueError, OSError)):
        await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    assert not fleet.ready
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name LIKE 'fleet_%'"))).scalar_one() == 0
    await fleet.stop()
