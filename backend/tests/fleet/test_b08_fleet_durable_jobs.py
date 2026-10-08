"""Cancellation and reconciliation retain real durable physical-stop boundaries."""

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings
from .test_b04_fleet_foundation import seed
from .test_b05_fleet_durable_jobs import setup_tracking
from .test_b06_attempts import prepare
from .test_b07_fleet_durable_jobs import sealed_attempt


async def bind(fleet, engine, sf):
    fleet.bind_tracking(await setup_tracking(engine, sf))
    # Explicit transactions in these tests decide order, not a periodic background poll.
    await fleet.reconciler.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cancel_after_proven_stop_before_complete_is_terminal(fleet_database, tmp_path):
    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path)
    engine, sf, _ = fleet_database
    await bind(fleet, engine, sf)
    result = await fleet.jobs.cancel(claim.job_id, user_id="u", thread_id="t")
    assert result["state"] == "cancelled"
    with pytest.raises(ValueError):
        await fleet.manifests.complete(**args, manifest=manifest)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT state FROM fleet_attempts"))).scalar_one() == "cancelled"
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
    assert await fleet.jobs.cancel(claim.job_id, user_id="u", thread_id="t") == result
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_accepted_complete_wins_over_later_cancel(fleet_database, tmp_path):
    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path)
    engine, sf, _ = fleet_database
    await bind(fleet, engine, sf)
    accepted = await fleet.manifests.complete(**args, manifest=manifest)
    result = await fleet.jobs.cancel(claim.job_id, user_id="u", thread_id="t")
    assert result["state"] == "succeeded" and result["accepted_manifest_id"] == accepted["manifest_id"]
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT cancel_requested_at FROM fleet_jobs WHERE id=:id"), {"id": claim.job_id})).scalar_one() is None
    assert await fleet.manifests.complete(**args, manifest=manifest) == accepted
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True])
async def test_cancel_is_not_stop_proof_even_when_unknown(fleet_database, tmp_path, unknown):
    fleet, claim, attempts, args = await prepare(fleet_database, tmp_path)
    engine, sf, _ = fleet_database
    await bind(fleet, engine, sf)
    await attempts.authorize_start(**args)
    await attempts.renew(**args, running=True)
    if unknown:
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=clock_timestamp()-interval '1 second'"))
        await attempts.expire_pending()
    result = await fleet.jobs.cancel(claim.job_id, user_id="u", thread_id="t")
    assert result["state"] == ("unknown" if unknown else "running")
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT stopped_at FROM fleet_attempts"))).scalar_one() is None
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == ("quarantined" if unknown else "reserved")
    with pytest.raises(ValueError):
        await attempts.authorize_start(**args)
    response = await attempts.stopped(**args, reason="cancelled", exit_code=137)
    assert response["state"] == "cancelled"
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_queue_timeout_reconciles_without_available_node(fleet_database, tmp_path):
    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    await bind(fleet, engine, sf)
    await seed(engine, count=1)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_jobs SET queue_deadline=clock_timestamp()-interval '1 second'"))
        await conn.execute(text("UPDATE fleet_nodes SET health='offline'"))
    assert await fleet.jobs.reconcile("0") == "failed"
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT finished_at,error FROM fleet_jobs"))).one()
        assert row.finished_at is not None and row.error
        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_background_queue_does_not_starve_staged_expiry(fleet_database, tmp_path):
    import asyncio

    from deerflow_ecs_fleet.reconcile import JobReconciler

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    await bind(fleet, engine, sf)
    await seed(engine, count=101)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO fleet_jobs (id,user_id,thread_id,tracking_task_id,idempotency_key,
                    spec,state,staged_deadline,queue_deadline)
                VALUES ('staged-z','u','t','track-z','key-z','{}','staged',
                    clock_timestamp()-interval '1 second',clock_timestamp()+interval '2 hours')
                """
            )
        )
    reconciler = JobReconciler(fleet.jobs, fleet.attempts)
    reconciler.start()
    try:
        for _ in range(40):
            async with engine.connect() as conn:
                state = (await conn.execute(text("SELECT state FROM fleet_jobs WHERE id='staged-z'"))).scalar_one()
            if state == "failed":
                break
            await asyncio.sleep(0.05)
        assert state == "failed"
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs WHERE state='queued'"))).scalar_one() == 101
    finally:
        await reconciler.stop()
        await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["complete", "cancel"])
async def test_concurrent_cancel_complete_respects_first_committed_transition(fleet_database, tmp_path, monkeypatch, winner):
    import asyncio
    import threading

    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path)
    engine, sf, _ = fleet_database
    await bind(fleet, engine, sf)
    active = []
    release_thread = threading.Event()
    release_cancel = asyncio.Event()
    try:
        if winner == "complete":
            entered = threading.Event()
            verify = fleet.workspace.verify_manifest

            def held_verify(value):
                entered.set()
                if not release_thread.wait(3):
                    raise TimeoutError("Test manifest barrier timed out")
                return verify(value)

            monkeypatch.setattr(fleet.workspace, "verify_manifest", held_verify)
            complete = asyncio.create_task(fleet.manifests.complete(**args, manifest=manifest))
            active.append(complete)
            assert await asyncio.to_thread(entered.wait, 2)
            cancel = asyncio.create_task(fleet.jobs.cancel(claim.job_id, user_id="u", thread_id="t"))
            active.append(cancel)
            await asyncio.sleep(0.05)
            assert not cancel.done()
            release_thread.set()
            accepted, cancelled = await asyncio.gather(complete, cancel)
            assert accepted["state"] == cancelled["state"] == "succeeded"
            assert accepted["manifest_id"] == cancelled["accepted_manifest_id"]
        else:
            import deerflow_ecs_fleet.job_service as module

            entered = asyncio.Event()
            apply_cancel = module.cancel_stopped_attempt

            async def held_cancel(session, job, now):
                entered.set()
                await release_cancel.wait()
                return await apply_cancel(session, job, now)

            monkeypatch.setattr(module, "cancel_stopped_attempt", held_cancel)
            cancel = asyncio.create_task(fleet.jobs.cancel(claim.job_id, user_id="u", thread_id="t"))
            active.append(cancel)
            await asyncio.wait_for(entered.wait(), timeout=2)
            complete = asyncio.create_task(fleet.manifests.complete(**args, manifest=manifest))
            active.append(complete)
            await asyncio.sleep(0.05)
            assert not complete.done()
            release_cancel.set()
            cancelled, rejected = await asyncio.gather(cancel, complete, return_exceptions=True)
            assert cancelled["state"] == "cancelled" and isinstance(rejected, ValueError)
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == (1 if winner == "complete" else 0)
            assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
    finally:
        release_thread.set()
        release_cancel.set()
        await asyncio.gather(*active, return_exceptions=True)
        await fleet.stop()
