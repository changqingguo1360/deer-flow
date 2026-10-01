"""Immutable input registration and authorization are persisted in real Postgres."""

import importlib.util
from io import BytesIO

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import setup_tracking


def require_inputs():
    assert importlib.util.find_spec("deerflow_ecs_fleet.persistence.inputs") is not None, "Immutable input registry missing"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_input_versions_are_pinned_and_owned(fleet_database, tmp_path):
    require_inputs()
    from deerflow_ecs_fleet.protocol import JobSpec

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    original = await fleet.inputs.register(user_id="u", thread_id="t", files=[("data.txt", BytesIO(b"original"))])
    later = await fleet.inputs.register(user_id="u", thread_id="t", files=[("data.txt", BytesIO(b"later"))])
    assert original["id"] != later["id"]
    reader = await setup_tracking(engine, sf)
    fleet.bind_tracking(reader)
    spec = JobSpec(task_name="analyze", profile="batch", argv=["true"], input_manifests=[original["id"]], code_artifact_id=original["id"])
    job = await fleet.jobs.submit(user_id="u", thread_id="t", source_run_id="r", tracking_task_id="track", idempotency_key="key", spec=spec)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_jobs SET state='queued',queued_at=clock_timestamp()"))
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,session_id,health,last_seen_at) VALUES ('n','n',1000,512,'s','online',clock_timestamp())"))
    claim = await fleet.scheduler.claim_job("n", node_session_id="s")
    grant = await fleet.attempts.authorize_start(node_id="n", node_session_id="s", attempt_id=claim.attempt_id, token=claim.token)
    assert job["id"] == claim.job_id
    assert [row["id"] for row in grant["launch_spec"]["inputs"]] == [original["id"]]
    assert grant["launch_spec"]["inputs"][0]["files"] == original["files"]
    for owner, thread in [("other", "t"), ("u", "other")]:
        with pytest.raises((PermissionError, ValueError)):
            await fleet.jobs.submit(user_id=owner, thread_id=thread, source_run_id="r", tracking_task_id="bad-track", idempotency_key="bad-key", spec=spec)
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["escape", "duplicate", "oversized", "missing"])
async def test_bad_input_registration_or_reference_never_creates_job(fleet_database, tmp_path, fault):
    require_inputs()
    from deerflow_ecs_fleet.protocol import JobSpec

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path).model_copy(update={"max_input_bytes": 8}))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    if fault != "missing":
        files = [("../outside", BytesIO(b"x"))] if fault == "escape" else ([("same", BytesIO(b"x")), ("same", BytesIO(b"y"))] if fault == "duplicate" else [("big", BytesIO(b"x" * 9))])
        with pytest.raises((ValueError, OSError)):
            await fleet.inputs.register(user_id="u", thread_id="t", files=files)
    else:
        reader = await setup_tracking(engine, sf)
        fleet.bind_tracking(reader)
        with pytest.raises((PermissionError, ValueError)):
            await fleet.jobs.submit(user_id="u", thread_id="t", source_run_id="r", tracking_task_id="track", idempotency_key="key", spec=JobSpec(task_name="bad", profile="batch", argv=["true"], input_manifests=["missing-version"]))
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_input_manifests"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 0
    await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["changed_bytes", "symlink", "foreign_prefix", "missing_snapshot", "extra_snapshot", "relabelled_version", "unlisted_file", "unlisted_directory"])
async def test_bad_pinned_snapshot_is_rejected_before_launch(fleet_database, tmp_path, fault):
    import json
    from dataclasses import asdict

    from .test_b04_fleet_foundation import seed

    require_inputs()
    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    version = await fleet.inputs.register(user_id="u", thread_id="t", files=[("data.txt", BytesIO(b"original"))])
    await seed(engine, count=1)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_jobs SET spec=:spec"), {"spec": json.dumps({"task_name": "batch", "profile": "batch", "argv": ["true"], "input_manifests": [version["id"]]})})
    claim = await fleet.scheduler.claim_job("n", node_session_id="s")
    grant = await fleet.attempts.authorize_start(node_id="n", node_session_id="s", attempt_id=claim.attempt_id, token=claim.token)
    source = tmp_path / grant["launch_spec"]["inputs"][0]["output_prefix"] / "data.txt"
    if fault == "changed_bytes":
        source.chmod(0o600)
        source.write_text("tampered")
    elif fault == "symlink":
        source.parent.chmod(0o700)
        source.unlink()
        outside = tmp_path / "outside"
        outside.write_text("original")
        source.symlink_to(outside)
    elif fault == "foreign_prefix":
        grant["launch_spec"]["inputs"][0]["output_prefix"] = "other/t/inputs/" + version["id"] + "/sealed"
    elif fault == "missing_snapshot":
        grant["launch_spec"]["inputs"] = []
    elif fault == "extra_snapshot":
        grant["launch_spec"]["inputs"].append(grant["launch_spec"]["inputs"][0])
    elif fault == "relabelled_version":
        other = await fleet.inputs.register(user_id="u", thread_id="t", files=[("data.txt", BytesIO(b"other"))])
        async with engine.connect() as conn:
            row = (await conn.execute(text("SELECT output_prefix,files,total_bytes FROM fleet_input_manifests WHERE id=:id"), {"id": other["id"]})).one()
        grant["launch_spec"]["inputs"][0].update(output_prefix=row.output_prefix, files=row.files, total_bytes=row.total_bytes)
    else:
        mount = fleet.workspace.prepare_inputs(asdict(claim), grant)[version["id"]]
        mount.chmod(0o700)
        if fault == "unlisted_file":
            (mount / "unlisted.txt").write_text("unexpected")
        else:
            (mount / "unlisted-dir").mkdir()
    with pytest.raises((ValueError, OSError)):
        fleet.workspace.prepare_inputs(asdict(claim), grant)
    await fleet.stop()
