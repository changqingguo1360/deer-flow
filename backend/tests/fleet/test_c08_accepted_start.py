"""New C admission freezes a legal original accepted source and verifies before start."""

import pytest
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c08_terminal_pair import participant
from .test_c08_terminal_pair import prepared_pair as prepared_pair


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", [dict(files={"outputs/result": b"accepted"})], indirect=True)
@pytest.mark.parametrize("mutation", ["none", "delete", "digest", "manifest", "private-root", "private-root-after-verify", "expired-fresh-fence"])
@pytest.mark.parametrize("selection", ["explicit", "implicit"])
async def test_actual_new_c_admission_freezes_original_source_point_and_grant(prepared_pair, tmp_path, mutation, selection, monkeypatch):
    from app.gateway import services
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime import RunManager

    from .test_c02_remote_agent_admission import backend, body, request
    from .test_c03_remote_agent_admission import claim

    p = prepared_pair
    from deerflow_ecs_fleet.launch_spec import LaunchSpec

    async with p.sf() as session:
        original_payload = await session.scalar(text("SELECT payload FROM fleet_launch_specs WHERE run_id=:id"), {"id": p.identity.run_id})
    assert "source_workspace_point_id" not in original_payload
    import importlib.util
    import sys
    from pathlib import Path

    frozen_path = Path(__file__).parent / "fixtures/c08-task4-launch-spec/launch_spec.py"
    frozen_module_spec = importlib.util.spec_from_file_location("deerflow_ecs_fleet._task4_frozen_launch_spec", frozen_path)
    frozen_module = importlib.util.module_from_spec(frozen_module_spec)
    sys.modules[frozen_module_spec.name] = frozen_module
    frozen_module_spec.loader.exec_module(frozen_module)
    historical = frozen_module.LaunchSpec.model_validate(original_payload)
    assert historical.model_dump(mode="json") == original_payload
    assert historical.payload_digest() == p.candidate.launch_spec_digest
    legacy = LaunchSpec.model_validate(original_payload)
    assert legacy.model_dump(mode="json") == historical.model_dump(mode="json") == original_payload
    assert legacy.payload_digest() == p.candidate.launch_spec_digest
    with p.scope():
        await RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p)).update_status(p.identity.run_id, "success")
    env = p.item.env
    ownership = env[4].state.fleet_ownership
    await ownership.stopped(
        reason="exit", exit_code=0, process_ref=p.identity.process_ref, physical_stopped=True, node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token
    )
    manager = RunManager(store=env[4].state.run_store)
    req = request(manager, env[2])
    req.app.state.checkpointer = p.item.writer
    new_body = body("new user turn")
    if selection == "explicit":
        new_body = new_body.model_copy(update={"checkpoint": {"checkpoint_id": p.identity.checkpoint_id}})
    result = await services.start_run(new_body, p.identity.thread_id, req, execution_backend=backend())
    async with p.sf() as session:
        payload = await session.scalar(text("SELECT payload FROM fleet_launch_specs WHERE run_id=:id"), {"id": result.run_id})
    assert payload["source_workspace_point_id"] == p.identity.request_id
    assert payload["normalized_config"]["configurable"].get("checkpoint_id") == (p.identity.checkpoint_id if selection == "explicit" else None)
    new_env = (*env[:5], result, *env[6:])
    accepted = await claim(new_env)
    assert accepted is not None and accepted.attempt_id != p.identity.attempt_id
    ownership.config = ownership.config.model_copy(update={"nas_root": p.versions.nas.root, "nas_identity": "task4"})
    import httpx
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers

    root = p.versions.nas.root / p.candidate.nas_prefix
    if mutation == "delete":
        (root / "outputs/result").unlink()
    elif mutation == "digest":
        (root / "outputs/result").write_bytes(b"ACCEPTED")
    elif mutation == "manifest":
        (root / "manifest.json").write_bytes(b"{}")
    elif mutation == "private-root":
        async with p.sf.begin() as session:
            await session.execute(
                text("UPDATE checkpoints SET metadata=jsonb_set(metadata::jsonb,'{deerflow_execution_run_id}','\"wrong-run\"') WHERE thread_id=:thread AND checkpoint_id=:checkpoint"),
                {"thread": p.identity.thread_id, "checkpoint": p.identity.checkpoint_id},
            )
    if mutation in {"expired-fresh-fence", "private-root-after-verify"}:
        import asyncio

        from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions

        loop = asyncio.get_running_loop()
        original_verify = AgentWorkspaceVersions.verify

        async def expire_original_lease():
            async with p.sf.begin() as session:
                if mutation == "expired-fresh-fence":
                    await session.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=:run"), {"run": result.run_id})
                else:
                    await session.execute(
                        text("UPDATE checkpoints SET metadata=jsonb_set(metadata::jsonb,'{deerflow_execution_run_id}','\"wrong-run\"') WHERE thread_id=:thread AND checkpoint_id=:checkpoint"),
                        {"thread": p.identity.thread_id, "checkpoint": p.identity.checkpoint_id},
                    )

        def verified_then_expired(self, manifest):
            verified = original_verify(self, manifest)
            asyncio.run_coroutine_threadsafe(expire_original_lease(), loop).result(timeout=3)
            return verified

        monkeypatch.setattr(AgentWorkspaceVersions, "verify", verified_then_expired)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=env[4]), base_url="http://test") as client:
        response = await client.post("/api/fleet/node/attempts/" + accepted.attempt_id + "/start", headers={"Authorization": "Bearer " + env[7].token}, json={"node_session_id": p.identity.node_session_id, "token": accepted.token})
    if mutation != "none":
        assert response.status_code == 409, response.text
        async with p.sf() as session:
            row = (
                await session.execute(
                    text("SELECT a.start_authorized_at,a.started_at,a.process_ref,t.state,p.state FROM fleet_attempts a JOIN fleet_run_placements p ON p.run_id=a.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id WHERE a.id=:id"),
                    {"id": accepted.attempt_id},
                )
            ).one()
            if mutation == "expired-fresh-fence":
                assert tuple(row[:3]) == (None, None, None)
                assert row[3] != "recovery_required" and row[4] != "recovery_required"
                return
            assert tuple(row) == (None, None, None, "recovery_required", "recovery_required")
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as recovery_conflict:
            await services.start_run(body("blocked new turn during recovery"), p.identity.thread_id, req, execution_backend=backend())
        assert recovery_conflict.value.status_code == 409
        return
    assert response.status_code == 200, response.text
    grant = response.json()
    assert grant["accepted_workspace"]["point_id"] == p.identity.request_id
    assert grant["accepted_workspace"]["manifest"]["manifest_id"] == p.candidate.manifest_id
    driver = AgentContainers(state_dir=tmp_path / "node-control", operator_config={})
    target = await driver.prepare_workspace(p.versions.nas.root, dict(attempt_id=accepted.attempt_id), grant)
    clone = target / ".deer-flow/users" / p.identity.user_id / "threads" / p.identity.thread_id / "user-data"
    assert (clone / "outputs/result").read_bytes() == b"accepted"
    assert (clone / "workspace").is_dir() and (clone / "uploads").is_dir()
    assert (clone / "outputs/result").stat().st_ino != (root / "outputs/result").stat().st_ino
    await driver.prepare_workspace(p.versions.nas.root, dict(attempt_id=accepted.attempt_id), grant)
    (root / "outputs/result").unlink()
    with pytest.raises((ValueError, OSError)):
        await driver.prepare_workspace(p.versions.nas.root, dict(attempt_id=accepted.attempt_id), grant)
