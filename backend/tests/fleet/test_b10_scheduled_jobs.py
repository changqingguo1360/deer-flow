"""Scheduled named slots reuse durable unfinished work across Agent runs."""

import asyncio

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from pydantic import ValidationError
from sqlalchemy import text

from app.gateway.routers.mcp_tasks import _detail

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import setup_tracking, spec


def test_named_slots_are_operator_validated():
    from deerflow_ecs_fleet.config import FleetConfig

    assert FleetConfig(scheduled_job_slots={}).scheduled_job_slots == {}
    with pytest.raises(ValidationError):
        FleetConfig(scheduled_job_slots={"bad/name": "missing"})
    with pytest.raises(ValidationError):
        FleetConfig(scheduled_job_slots={"daily": "missing"})


def test_uncertainty_summary_is_specific_to_fleet_reason():
    record = dict(id="tracking", task_name="batch", status="input_required", created_at=None, updated_at=None, driver_name="fleet", input_required={"reason": "execution_unknown"})
    assert _detail(record, threshold=3)["execution_uncertain"] is True
    for patch in [{"driver_name": "ordinary"}, {"input_required": {"reason": "question"}}, {"status": "working"}]:
        assert _detail({**record, **patch}, threshold=3)["execution_uncertain"] is False


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["staged", "queued", "running", "unknown", "quarantined"])
async def test_unfinished_group_serializes_occurrences(fleet_database, tmp_path, state):
    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        from deerflow_ecs_fleet.job_service import FleetJobService

        jobs = FleetJobService(sf, fleet.config, tracking_reader=await setup_tracking(engine, sf))

        async def submit(run, thread="t", owner="u", args=None):
            return await jobs.submit(user_id=owner, thread_id=thread, source_run_id=run, tracking_task_id="tracking-" + run, idempotency_key=run, spec=args or spec(), dedupe_group="schedule-slot")

        first = await submit("r1")
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_jobs SET state=:state, cancel_requested_at=clock_timestamp()"), {"state": state})
        changed = spec().model_copy(update={"argv": ["echo", "new"]})
        repeated = await asyncio.gather(*(submit("r" + str(i), args=changed) for i in range(2, 8)))
        assert {row["id"] for row in repeated} == {first["id"]}
        assert all(row["tracking_task_id"] == first["tracking_task_id"] for row in repeated)
        with pytest.raises(ValueError, match="thread"):
            await submit("other-thread", thread="different")
        other_owner = await submit("other-owner", owner="different")
        assert other_owner["id"] != first["id"]
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_jobs SET state='failed' WHERE id=:id"), {"id": first["id"]})
        new = await submit("new-cycle")
        assert new["id"] != first["id"]
        retry = await submit("r1")
        assert retry["id"] == first["id"]
        async with engine.connect() as conn:
            original = (await conn.execute(text("SELECT source_run_id,spec,tracking_task_id FROM fleet_jobs WHERE id=:id"), {"id": first["id"]})).one()
        assert original.source_run_id == "r1"
        assert original.spec["argv"] == ["true"]
        assert original.tracking_task_id == first["tracking_task_id"]
    finally:
        await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_driver_reuses_original_tracking_across_runs_without_compensation(fleet_database, tmp_path):
    from app.mcp_tasks.service import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSubmitRequest
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    engine, sf, _ = fleet_database
    cfg = settings(tmp_path).model_copy(update={"scheduled_job_slots": {"daily": "batch"}})
    fleet = service_class()(cfg)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    repo = McpTaskRepository(sf)
    driver = fleet.bind_tracking(await setup_tracking(engine, sf))
    registry = McpTaskDriverRegistry()
    registry.register("fleet", driver)
    service = McpTaskService(repository=repo, drivers=registry, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)

    def request(run, invocation):
        return TaskSubmitRequest(
            user_id="u", thread_id="t", run_id=run, tool_call_id=None, server_name="fleet", task_name="batch", arguments=spec().model_dump(), driver_data={"invocation_id": invocation, "scheduled_task_id": "schedule", "job_slot": "daily"}
        )

    try:
        first = await service.submit(driver_name="fleet", request=request("r1", "i1"))
        second = await service.submit(driver_name="fleet", request=request("r2", "i2"))
        assert second.pop("reused_existing") is True
        assert second == first
        assert second["run_id"] == "r1"
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks"))).scalar_one() == 1
        # A staged Fleet row with no committed tracking must remain untouched.
        async with engine.begin() as conn:
            before = (await conn.execute(text("SELECT state FROM fleet_jobs"))).scalar_one()
            await conn.execute(text("DELETE FROM mcp_tasks"))
        with pytest.raises(RuntimeError, match="tracking.*pending"):
            await service.submit(driver_name="fleet", request=request("r3", "i3"))
        async with engine.connect() as conn:
            row = (await conn.execute(text("SELECT state,cancel_requested_at FROM fleet_jobs"))).one()
        assert row.state == before and row.cancel_requested_at is None
        # The original invocation can repair its own uncommitted tracking row;
        # cross-occurrence lookup never owns this recovery authority.
        repaired = await service.submit(driver_name="fleet", request=request("r1", "i1"))
        assert repaired["id"] == first["id"]
        assert repaired["run_id"] == "r1"
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks"))).scalar_one() == 1
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_jobs SET state='failed'"))
        fresh = await service.submit(driver_name="fleet", request=request("r4", "i4"))
        assert fresh["id"] != repaired["id"]
        replayed = await service.submit(driver_name="fleet", request=request("r2", "i2"))
        assert replayed.pop("reused_existing") is True
        assert replayed == repaired
        assert replayed["task_name"] == "batch" and replayed["run_id"] == "r1"
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 2
            assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks"))).scalar_one() == 2
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs WHERE cancel_requested_at IS NOT NULL"))).scalar_one() == 0
    finally:
        await fleet.stop()


def test_schedule_identity_is_scrubbed_before_explicit_host_injection():
    from app.fleet.scheduled_jobs import apply_scheduled_job_context

    config = {"context": {"scheduled_task_id": "forged"}, "configurable": {"scheduled_task_id": "forged"}, "metadata": {"scheduled_task_id": "forged"}}
    apply_scheduled_job_context(config, schedule_id=None)
    assert "scheduled_task_id" not in config["context"]
    assert "scheduled_task_id" not in config["configurable"]
    apply_scheduled_job_context(config, schedule_id="trusted")
    assert config["context"]["scheduled_task_id"] == "trusted"
    assert "scheduled_task_id" not in config["configurable"]


def test_model_can_select_only_named_job_slot():
    from deerflow.tools.builtins.fleet_jobs import submit_fleet_job

    assert "job_slot" in submit_fleet_job.tool_call_schema.model_json_schema()["properties"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "context,slot,profile",
    [
        ({"user_id": "u", "run_id": "r", "scheduled_task_id": "s", "scheduled_context_mode": "fresh_thread_per_run"}, "daily", "batch"),
        ({"user_id": "u", "run_id": "r"}, "daily", "batch"),
        ({"user_id": "u", "run_id": "r", "scheduled_task_id": "s"}, None, "batch"),
        ({"user_id": "u", "run_id": "r", "scheduled_task_id": "s"}, "arbitrary", "batch"),
        ({"user_id": "u", "run_id": "r", "scheduled_task_id": "s"}, "daily", "other"),
    ],
)
async def test_model_slot_requires_trusted_schedule_and_approved_binding(context, slot, profile):
    from types import SimpleNamespace
    from unittest.mock import patch

    from deerflow.mcp.tasks.fleet_runtime import set_fleet_job_submitter
    from deerflow.tools.builtins.fleet_jobs import submit_fleet_job

    class NeverSubmit:
        async def submit(self, **kwargs):
            raise AssertionError("Invalid slot reached host")

    set_fleet_job_submitter(NeverSubmit(), scheduled_job_slots={"daily": "batch"})
    runtime = SimpleNamespace(context=context, execution_info=SimpleNamespace(thread_id="t"), tool_call_id="call")
    try:
        with patch("deerflow.tools.builtins.fleet_jobs.durable_tool_invocation_id", return_value="durable-invocation"):
            with pytest.raises(ValueError, match="slot"):
                await submit_fleet_job.coroutine(runtime=runtime, task_name="batch", profile=profile, argv=["true"], job_slot=slot)
    finally:
        set_fleet_job_submitter(None)


def test_forged_schedule_mode_is_scrubbed():
    from app.fleet.scheduled_jobs import apply_scheduled_job_context

    config = {"context": {"scheduled_context_mode": "reuse_thread"}, "configurable": {"scheduled_context_mode": "reuse_thread"}}
    apply_scheduled_job_context(config, schedule_id=None)
    assert "scheduled_context_mode" not in config["context"]
    assert "scheduled_context_mode" not in config["configurable"]


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("new_active", [False, True])
async def test_reused_invocation_replay_keeps_original_job_after_terminal(fleet_database, tmp_path, new_active):
    from deerflow_ecs_fleet.job_service import FleetJobService

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        jobs = FleetJobService(sf, fleet.config, tracking_reader=await setup_tracking(engine, sf))

        async def submit(key, submitted_spec):
            return await jobs.submit(user_id="u", thread_id="t", source_run_id=key, tracking_task_id="tracking-" + key, idempotency_key=key, spec=submitted_spec, dedupe_group="schedule-slot")

        first = await submit("r1", spec())
        requested = spec().model_copy(update={"argv": ["echo", "occurrence-two"]})
        reused = await submit("r2", requested)
        assert reused["id"] == first["id"]
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_jobs SET state='failed' WHERE id=:id"), {"id": first["id"]})
        if new_active:
            third = await submit("r3", spec())
            assert third["id"] != first["id"]
        # Replay the invocation whose response/checkpoint was lost. Its first
        # decision remains immutable even once the original group is terminal.
        replayed = await submit("r2", requested)
        assert replayed["id"] == first["id"]
        assert replayed["tracking_task_id"] == first["tracking_task_id"]
        assert replayed["reused_existing"] is True
        with pytest.raises(ValueError, match="different input"):
            await submit("r2", spec())
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == (2 if new_active else 1)
    finally:
        await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("other_group", [None, "different-group"])
async def test_invocation_identity_serializes_alias_and_new_job_collisions(fleet_database, tmp_path, other_group):
    from deerflow_ecs_fleet.job_service import FleetJobService

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        jobs = FleetJobService(sf, fleet.config, tracking_reader=await setup_tracking(engine, sf))
        first = await jobs.submit(user_id="u", thread_id="t", source_run_id="first", tracking_task_id="track-first", idempotency_key="first", spec=spec(), dedupe_group="scheduled")

        async def submit(group):
            return await jobs.submit(user_id="u", thread_id="t", source_run_id="race", tracking_task_id="track-race", idempotency_key="shared-key", spec=spec(), dedupe_group=group)

        results = await asyncio.gather(submit("scheduled"), submit(other_group), return_exceptions=True)
        winner = [result for result in results if isinstance(result, dict)]
        failure = [result for result in results if isinstance(result, Exception)]
        assert len(winner) == len(failure) == 1
        assert isinstance(failure[0], ValueError) and "different input" in str(failure[0])
        async with engine.connect() as conn:
            receipt = (await conn.execute(text("SELECT job_id,dedupe_group FROM fleet_job_invocations WHERE user_id='u' AND idempotency_key='shared-key'"))).one()
            assert receipt.job_id == winner[0]["id"]
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == (1 if receipt.job_id == first["id"] else 2)
        assert (await submit(receipt.dedupe_group))["id"] == winner[0]["id"]
    finally:
        await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invocation_migration_backfills_existing_canonical_jobs(fleet_database, tmp_path):
    import json
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from deerflow_ecs_fleet import service as fleet_service

    engine, sf, _ = fleet_database

    def upgrade_old(connection):
        config = Config()
        config.set_main_option("script_location", str(Path(fleet_service.__file__).parent / "migrations"))
        config.attributes["connection"] = connection
        command.upgrade(config, "f0004_recovery")

    async with engine.begin() as conn:
        await conn.run_sync(upgrade_old)
        await conn.execute(
            text("""INSERT INTO fleet_jobs
            (id,user_id,thread_id,source_run_id,tracking_task_id,idempotency_key,dedupe_group,spec,state,staged_deadline,queue_deadline)
            VALUES ('old-job','u','t','r','old-tracking','old-key','old-group',cast(:spec as jsonb),'failed',clock_timestamp(),clock_timestamp())"""),
            {"spec": json.dumps(spec().model_dump())},
        )
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        from deerflow_ecs_fleet.job_service import FleetJobService

        jobs = FleetJobService(sf, fleet.config, tracking_reader=await setup_tracking(engine, sf))
        replayed = await jobs.submit(user_id="u", thread_id="t", source_run_id="r", tracking_task_id="new-local-id", idempotency_key="old-key", spec=spec(), dedupe_group="old-group")
        assert replayed["id"] == "old-job" and replayed["tracking_task_id"] == "old-tracking"
        assert not replayed.get("reused_existing")
        async with engine.connect() as conn:
            receipt = (await conn.execute(text("SELECT thread_id,source_run_id,spec,dedupe_group,job_id FROM fleet_job_invocations"))).one()
            assert receipt.thread_id == "t" and receipt.source_run_id == "r"
            assert receipt.spec == spec().model_dump()
            assert receipt.dedupe_group == "old-group" and receipt.job_id == "old-job"
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
    finally:
        await fleet.stop()
