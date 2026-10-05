"""Real PostgreSQL f0008 upgrade and private boundary foundation constraints."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import delete, insert, inspect, text, update
from sqlalchemy.exc import DBAPIError

from .test_b02_fleet_foundation import service_class, settings


@pytest.mark.asyncio
async def test_actual_f0008_upgrade_repeat_and_concurrent_startup(fleet_database, tmp_path):
    engine, sf, _ = fleet_database

    def baseline(conn):
        import deerflow_ecs_fleet.service as module

        cfg = Config()
        cfg.set_main_option("script_location", str(Path(module.__file__).parent / "migrations"))
        cfg.attributes["connection"] = conn
        command.upgrade(cfg, "f0008_event_outbox")

    async with engine.begin() as conn:
        await conn.run_sync(baseline)
    first, second = service_class()(settings(tmp_path)), service_class()(settings(tmp_path))
    await asyncio.gather(first.start(ExtensionRuntimeDeps(session_factory=sf)), second.start(ExtensionRuntimeDeps(session_factory=sf)))
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT version_num FROM fleet_alembic_version"))).scalar_one() == "f0009_workspace_points"
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
        assert {"fleet_workspace_requests", "fleet_workspace_manifests", "fleet_workspace_points"} <= set(names)
    await first.stop()
    await second.stop()
    await first.start(ExtensionRuntimeDeps(session_factory=sf))
    await first.stop()


def test_full_private_metadata_contract():
    from deerflow_ecs_fleet.persistence import models
    from deerflow_ecs_fleet.persistence.base import FleetBase

    from deerflow.persistence.base import Base

    for name in ("fleet_workspace_requests", "fleet_workspace_manifests", "fleet_workspace_points"):
        assert name in FleetBase.metadata.tables, "C workspace private schema missing"
        assert name not in Base.metadata.tables
    assert models.AGENT_TASK_ACTIVE == "state NOT IN ('succeeded','failed','cancelled','timed_out')"
    for name in ("fleet_agent_tasks", "fleet_run_placements"):
        checks = " ".join(str(c.sqltext) for c in FleetBase.metadata.tables[name].constraints if hasattr(c, "sqltext"))
        assert "finishing" in checks and "recovery_required" in checks


@pytest_asyncio.fixture
async def boundary_db(fleet_database, tmp_path):
    from deerflow_ecs_fleet.persistence import models as m

    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    async with engine.begin() as conn:
        for suffix in ("1", "2"):
            await conn.execute(insert(m.NodeRow).values(id="node" + suffix, name="node" + suffix, cpu_millis=1000, memory_mib=512))
            await conn.execute(insert(m.AgentTaskRow).values(id="task" + suffix, user_id="user" + suffix, thread_id="thread" + suffix, deadline=datetime.now(UTC) + timedelta(hours=1), continuation_budget=1))
            await conn.execute(
                insert(m.LaunchSpecRow).values(id="launch" + suffix, run_id="run" + suffix, agent_task_id="task" + suffix, generation=1, user_id="user" + suffix, thread_id="thread" + suffix, payload={}, payload_digest="sha256:" + "b" * 64)
            )
            await conn.execute(
                insert(m.RunPlacementRow).values(
                    run_id="run" + suffix,
                    agent_task_id="task" + suffix,
                    generation=1,
                    user_id="user" + suffix,
                    thread_id="thread" + suffix,
                    requested_backend="remote",
                    profile="remote",
                    launch_spec_ref="launch" + suffix,
                    queue_deadline=datetime.now(UTC) + timedelta(hours=1),
                )
            )
            await conn.execute(
                insert(m.AttemptRow).values(
                    id="attempt" + suffix,
                    kind="agent",
                    run_id="run" + suffix,
                    attempt_no=1,
                    node_id="node" + suffix,
                    node_session_id="session" + suffix,
                    token_hash="a" * 64,
                    process_ref="fleet-attempt" + suffix,
                    output_prefix="ignored",
                    lease_expires_at=datetime.now(UTC) + timedelta(hours=1),
                )
            )
    try:
        yield engine, sf, m
    finally:
        await service.stop()


def owner(suffix="1"):
    return dict(
        run_id="run" + suffix,
        agent_task_id="task" + suffix,
        generation=1,
        user_id="user" + suffix,
        thread_id="thread" + suffix,
        attempt_id="attempt" + suffix,
        launch_spec_digest="sha256:" + "b" * 64,
        node_id="node" + suffix,
        node_session_id="session" + suffix,
        token_stamp="a" * 64,
        process_ref="fleet-attempt" + suffix,
        owner_worker_id="fleet-agent:attempt" + suffix,
    )


def request(mid="c" * 64, kind="partial", **changes):
    outcomes = {} if kind == "partial" else dict(desired_core_status="success", desired_task_status="paused" if kind == "paused" else "succeeded", desired_placement_status="succeeded")
    return dict(id="request" + mid[:8], **owner(), request_digest=mid, checkpoint_id="checkpoint" + mid[:8], checkpoint_ns="", kind=kind, publication_key="pub", presented_paths=[], source_workspace_version="initial", **outcomes) | changes


async def prepared(conn, m, mid="c" * 64, kind="partial"):
    r = request(mid, kind)
    await conn.execute(insert(m.WorkspaceRequestRow).values(**r))
    manifest = dict(id=mid, **owner(), request_digest=mid, content_hash=mid, categories=["workspace", "uploads", "outputs"], directories=["workspace", "uploads", "outputs"], files=[], total_bytes=0, nas_prefix="prefix/" + mid)
    await conn.execute(insert(m.WorkspaceManifestRow).values(**manifest))
    await conn.execute(update(m.WorkspaceRequestRow).where(m.WorkspaceRequestRow.id == r["id"]).values(state="sealing"))
    await conn.execute(update(m.WorkspaceRequestRow).where(m.WorkspaceRequestRow.id == r["id"]).values(state="prepared", candidate_manifest_id=mid))
    point = {key: value for key, value in r.items() if key not in {"id", "presented_paths", "source_workspace_version"}}
    point.update(id="point" + mid[:8], request_id=r["id"], manifest_id=mid)
    return r, manifest, point


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("user_id", "user2"),
        ("thread_id", "thread2"),
        ("run_id", "run2"),
        ("attempt_id", "attempt2"),
        ("generation", 2),
        ("launch_spec_digest", "sha256:" + "d" * 64),
        ("node_id", "node2"),
        ("node_session_id", "session2"),
        ("token_stamp", "d" * 64),
        ("process_ref", "fleet-attempt2"),
    ],
)
async def test_real_request_composite_original_execution_rejects_crossassociation(boundary_db, field, value):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        with pytest.raises(DBAPIError):
            async with conn.begin_nested():
                await conn.execute(insert(m.WorkspaceRequestRow).values(**request(**{field: value})))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table,operation", [("WorkspaceManifestRow", "update"), ("WorkspaceManifestRow", "delete"), ("WorkspacePointRow", "update"), ("WorkspacePointRow", "delete"), ("WorkspaceRequestRow", "identity"), ("WorkspaceRequestRow", "delete")]
)
async def test_real_immutable_evidence_and_request_identity(boundary_db, table, operation):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        r, manifest, p = await prepared(conn, m)
        await conn.execute(insert(m.WorkspacePointRow).values(**p))
        model = getattr(m, table)
        statement = delete(model) if operation == "delete" else update(model).values(user_id="user2")
        with pytest.raises(DBAPIError):
            async with conn.begin_nested():
                await conn.execute(statement)
        await conn.execute(update(m.WorkspaceRequestRow).values(state="accepted"))
        with pytest.raises(DBAPIError):
            async with conn.begin_nested():
                await conn.execute(update(m.WorkspaceRequestRow).values(state="sealing"))


@pytest.mark.asyncio
async def test_real_points_partial_final_unique_and_owner_pointer_constraints(boundary_db):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        _, _, partial = await prepared(conn, m)
        await conn.execute(insert(m.WorkspacePointRow).values(**partial))
        _, _, final = await prepared(conn, m, "d" * 64, "final")
        await conn.execute(insert(m.WorkspacePointRow).values(**final))
        _, _, paused = await prepared(conn, m, "e" * 64, "paused")
        with pytest.raises(DBAPIError):
            async with conn.begin_nested():
                await conn.execute(insert(m.WorkspacePointRow).values(**paused))
        for model, where, values in [(m.AgentTaskRow, m.AgentTaskRow.id == "task2", dict(accepted_workspace_point_id=partial["id"])), (m.RunPlacementRow, m.RunPlacementRow.run_id == "run2", dict(final_workspace_point_id=final["id"]))]:
            with pytest.raises(DBAPIError):
                async with conn.begin_nested():
                    await conn.execute(update(model).where(where).values(**values))
                    await conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        await conn.execute(update(m.AgentTaskRow).where(m.AgentTaskRow.id == "task1").values(accepted_workspace_point_id=partial["id"], state="finishing"))
        await conn.execute(update(m.RunPlacementRow).where(m.RunPlacementRow.run_id == "run1").values(final_workspace_point_id=final["id"], state="recovery_required"))
        await conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", ["WorkspaceManifestRow", "WorkspacePointRow"])
async def test_real_manifest_and_point_full_chain_constraints(boundary_db, model_name):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        _, manifest, p = await prepared(conn, m)
        value = manifest if model_name == "WorkspaceManifestRow" else p
        for field, bad in [("attempt_id", "attempt2"), ("user_id", "user2"), ("run_id", "run2"), ("request_digest", "f" * 64)]:
            invalid = value | {field: bad, "id": "bad-" + field}
            if model_name == "WorkspaceManifestRow":
                invalid.update(content_hash="f" * 64, id="f" * 64, nas_prefix="bad/" + field)
            with pytest.raises(DBAPIError):
                async with conn.begin_nested():
                    await conn.execute(insert(getattr(m, model_name)).values(**invalid))


@pytest.mark.asyncio
async def test_real_request_same_identity_retry_and_changed_identity_conflict(boundary_db):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        await conn.execute(insert(m.WorkspaceRequestRow).values(**request()))
        with pytest.raises(DBAPIError):
            async with conn.begin_nested():
                await conn.execute(insert(m.WorkspaceRequestRow).values(**(request() | {"id": "changed", "request_digest": "d" * 64})))


@pytest.mark.asyncio
async def test_actual_downgrade_refuses_evidence(boundary_db):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        _, _, p = await prepared(conn, m)
        await conn.execute(insert(m.WorkspacePointRow).values(**p))

    def downgrade(sync):
        import deerflow_ecs_fleet.service as module

        cfg = Config()
        cfg.set_main_option("script_location", str(Path(module.__file__).parent / "migrations"))
        cfg.attributes["connection"] = sync
        command.downgrade(cfg, "f0008_event_outbox")

    async with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="downgrade"):
            await conn.run_sync(downgrade)
        assert (await conn.execute(text("SELECT count(*) FROM fleet_workspace_points"))).scalar_one() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("state,required", [("unknown", True), ("recovery_required", True), ("input_required", False), ("finishing", False)])
async def test_actual_summary_recovery_is_explicit(boundary_db, state, required):
    from deerflow_ecs_fleet.persistence.agent_tasks import AgentTasks

    engine, sf, m = boundary_db
    async with engine.begin() as conn:
        await conn.execute(update(m.AgentTaskRow).where(m.AgentTaskRow.id == "task1").values(state=state))
    async with sf() as session:
        summary = await AgentTasks().public_summary(session, task_id="task1", user_id="user1", thread_id="thread1")
    assert summary["recovery_required"] is required


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["finishing", "recovery_required"])
async def test_finishing_and_recovery_keep_thread_exclusive(boundary_db, state):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        await conn.execute(update(m.AgentTaskRow).where(m.AgentTaskRow.id == "task1").values(state=state))
        with pytest.raises(DBAPIError):
            async with conn.begin_nested():
                await conn.execute(insert(m.AgentTaskRow).values(id="replacement", user_id="user1", thread_id="thread1", deadline=datetime.now(UTC) + timedelta(hours=1), continuation_budget=0))


@pytest.mark.asyncio
async def test_final_without_desired_outcome_rejected(boundary_db):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        with pytest.raises(DBAPIError):
            async with conn.begin_nested():
                await conn.execute(insert(m.WorkspaceRequestRow).values(**(request() | {"kind": "final"})))


@pytest.mark.asyncio
async def test_final_placement_pointer_rejects_same_owner_partial(boundary_db):
    engine, _, m = boundary_db
    async with engine.begin() as conn:
        _, _, p = await prepared(conn, m)
        await conn.execute(insert(m.WorkspacePointRow).values(**p))
        with pytest.raises(DBAPIError):
            async with conn.begin_nested():
                await conn.execute(update(m.RunPlacementRow).where(m.RunPlacementRow.run_id == "run1").values(final_workspace_point_id=p["id"]))
                await conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
