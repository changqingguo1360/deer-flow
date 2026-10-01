"""Remote run/attempt ownership and actual generic orphan recovery isolation."""

import asyncio
import importlib
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app.fleet.ownership import install_fleet_ownership
from app.gateway import services
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from deerflow.persistence.run import RunRepository
from deerflow.runtime import RunManager

from .test_b02_fleet_foundation import service_class, settings
from .test_b03_management_http import BODY, management_router
from .test_b03_management_http import management_environment as management_environment
from .test_c02_remote_agent_admission import admission as admission
from .test_c02_remote_agent_admission import backend, body, request


@pytest.mark.asyncio
async def test_queued_remote_survives_actual_gateway_hydration_and_startup_recovery(admission):
    engine, sf, _, user = admission
    record = await services.start_run(body(), "thread-c03-queued", request(RunManager(store=RunRepository(sf)), user), execution_backend=backend())
    restarted = RunManager(store=RunRepository(sf))
    hydrated = await restarted.get(record.run_id, user_id=user.id)
    assert hydrated.status.value == "pending" and hydrated.store_only
    recovered = await restarted.reconcile_orphaned_inflight_runs(error="Gateway restarted", stop_reason="orphan_recovered")
    assert recovered == []
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT status FROM runs WHERE run_id=:id"), {"id": record.run_id})).scalar_one() == "pending"
        assert (await conn.execute(text("SELECT state FROM fleet_run_placements WHERE run_id=:id"), {"id": record.run_id})).scalar_one() == "queued"


@pytest.mark.asyncio
async def test_actual_admin_csrf_registers_explicit_agent_capacity(management_environment):
    _, app, headers, _, _, engine = management_environment
    app.include_router(management_router())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/fleet/machines", headers=headers["admin"], json=BODY | {"agent_limit": 1})
        assert response.status_code == 201, response.text
        assert response.json()["node"]["agent_limit"] == 1
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT agent_limit FROM fleet_nodes WHERE id='machine-a'"))).scalar_one() == 1


def test_runtime_only_github_credential_requires_out_of_band_reference():
    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from pydantic import ValidationError

    from .test_c01_remote_agent_admission import wire

    payload = wire()
    payload["normalized_config"]["context"]["github_token"] = "ghs_" + "fixture-value"
    with pytest.raises(ValidationError, match="out-of-band"):
        LaunchSpec.model_validate(payload)


@pytest_asyncio.fixture
async def owner_environment(admission, tmp_path):
    engine, sf, _, user = admission
    cfg = backend().config
    nas = settings(tmp_path)
    cfg = cfg.model_copy(update={"nas_root": nas.nas_root, "nas_identity": nas.nas_identity, "profiles": cfg.profiles | nas.profiles})
    runtime = service_class()(cfg)
    await runtime.start(ExtensionRuntimeDeps(session_factory=sf))
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", runtime),))
    app.state.run_store = RunRepository(sf)
    install_fleet_ownership(app, sf)
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    await runtime.nodes.register(node_id="node-c03", name="node-c03", cpu_millis=1500, memory_mib=4096, agent_limit=1, profile_allowlist=["remote", "batch"])
    session = await runtime.nodes.open_session("node-c03", protocol_version=1)
    await runtime.nodes.heartbeat("node-c03", node_session_id=session["node_session_id"], protocol_version=1)
    credential = await runtime.credentials.issue("node-c03", lifetime_seconds=600)
    worker = WorkerCompatibility(runtime_digest=cfg.profiles["remote"].runtime_digest, skill_snapshot={"entries": []}, plugin_snapshot={"entries": []})
    record = await services.start_run(body(), "thread-c03-owned", request(RunManager(store=app.state.run_store), user), execution_backend=backend())
    try:
        yield engine, sf, user, runtime, app, record, session["node_session_id"], credential, worker
    finally:
        await runtime.stop()


async def claim(env):
    _, _, _, _, app, _, session_id, _, worker = env
    return await app.state.fleet_ownership.claim_agent("node-c03", node_session_id=session_id, worker=worker)


async def leases(engine):
    async with engine.connect() as conn:
        run = (await conn.execute(text("SELECT status,owner_worker_id,lease_expires_at FROM runs"))).one()
        attempt = (await conn.execute(text("SELECT state,id,lease_expires_at FROM fleet_attempts WHERE kind='agent'"))).one()
        return tuple(run), tuple(attempt)


@pytest.mark.asyncio
async def test_two_sessions_claim_one_agent_and_share_b_capacity(owner_environment):
    env = owner_environment
    engine, _, _, runtime, app, record, session_id, _, worker = env
    claims = await asyncio.gather(*(app.state.fleet_ownership.claim_agent("node-c03", node_session_id=session_id, worker=worker) for _ in range(2)))
    winner = [value for value in claims if value is not None]
    assert len(winner) == 1 and winner[0].run_id == record.run_id
    run, attempt = await leases(engine)
    assert run[1] == "fleet-agent:" + attempt[1] and run[2] == attempt[2]
    async with engine.begin() as conn:
        used = (await conn.execute(text("SELECT sum(cpu_millis),sum(memory_mib),sum(agent_units) FROM fleet_reservations WHERE state!='released'"))).one()
        assert tuple(used) == (1000, 2048, 1)
        await conn.execute(
            text(
                "INSERT INTO fleet_jobs(id,user_id,thread_id,tracking_task_id,idempotency_key,spec,state,queue_deadline,queued_at) "
                "VALUES ('job-c03','u','t','tracking','key','{\"task_name\":\"batch\",\"profile\":\"batch\",\"argv\":[\"true\"]}','queued',clock_timestamp()+interval '1 hour',clock_timestamp())"
            )
        )
    assert await runtime.scheduler.claim_job("node-c03", node_session_id=session_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["runtime", "skill", "plugin", "legacy_null", "allowlist", "agent_limit"])
async def test_actual_compatibility_and_capability_denial_never_reserve(owner_environment, change):
    env = owner_environment
    engine, _, _, _, app, _, session_id, _, worker = env
    payload = worker.model_dump(mode="json")
    if change == "runtime":
        payload["runtime_digest"] = "sha256:" + "b" * 64
    elif change in {"skill", "plugin"}:
        payload[change + "_snapshot"]["entries"] = [{"name": "changed", "version": "v1", "digest": "sha256:" + "b" * 64}]
    else:
        statement = {"legacy_null": "profile_allowlist=NULL", "allowlist": "profile_allowlist='[\"batch\"]'", "agent_limit": "agent_limit=0"}[change]
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_nodes SET " + statement))
    result = await app.state.fleet_ownership.claim_agent("node-c03", node_session_id=session_id, worker=WorkerCompatibility.model_validate(payload))
    assert result is None
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations"))).scalar_one() == 0


@pytest.mark.asyncio
async def test_node_bearer_claim_and_real_kind_renew_remain_available_when_closed(owner_environment):
    env = owner_environment
    engine, _, _, runtime, app, _, session_id, credential, worker = env
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        packet = {"node_session_id": session_id, "kind": "agent", "compatibility": worker.model_dump(mode="json")}
        assert (await client.post("/api/fleet/node/claims", json=packet)).status_code == 403
        headers = {"Authorization": "Bearer " + credential.token}
        response = await client.post("/api/fleet/node/claims", headers=headers, json=packet)
        assert response.status_code == 200, response.text
        accepted = response.json()
        assert accepted["kind"] == "agent"
        path = "/api/fleet/node/attempts/" + accepted["attempt_id"] + "/renew"
        identity = {"node_session_id": session_id, "token": accepted["token"]}
        before = await leases(engine)
        assert (await client.post(path, headers=headers, json=identity | {"token": "wrong"})).status_code == 403
        assert await leases(engine) == before
        # Closing new work and removing current profiles never changes the
        # already accepted immutable lease authority.
        app.state.fleet_ownership.config = runtime.config.model_copy(update={"agents_enabled": False, "jobs_enabled": False, "profiles": {}})
        await runtime.nodes.set_admin_state("node-c03", "draining")
        renewal = await client.post(path, headers=headers, json=identity | {"running": True})
        assert renewal.status_code == 200 and not renewal.json()["stop"]
        run, attempt = await leases(engine)
        assert run[0] == attempt[0] == "running" and run[2] == attempt[2]
        assert (await client.post("/api/fleet/node/claims", headers=headers, json=packet)).status_code == 204
        restarted = await runtime.nodes.open_session("node-c03", protocol_version=1)
        assert (await client.post(path, headers=headers, json=identity)).status_code == 409
        assert (await client.post(path, headers=headers, json=identity | {"node_session_id": restarted["node_session_id"]})).status_code == 409
        assert await leases(engine) == (run, attempt)


@pytest.mark.asyncio
async def test_renew_vs_session_rotation_and_local_takeover_are_atomic(owner_environment):
    env = owner_environment
    engine, _, user, runtime, app, record, session_id, _, _ = env
    accepted = await claim(env)
    identity = {"node_id": "node-c03", "node_session_id": session_id, "attempt_id": accepted.attempt_id, "token": accepted.token}
    before = await leases(engine)
    outcome, rotated = await asyncio.gather(app.state.fleet_ownership.renew(running=True, **identity), runtime.nodes.open_session("node-c03", protocol_version=1), return_exceptions=True)
    assert isinstance(rotated, dict) and rotated["node_session_id"] != session_id
    after = await leases(engine)
    if isinstance(outcome, ValueError):
        assert after == before
    else:
        assert not isinstance(outcome, BaseException) and outcome["stop"] is False
        assert after[0][2] == after[1][2]
    with pytest.raises(ValueError, match="Stale"):
        await app.state.fleet_ownership.renew(**identity)
    # Even deliberately expired remote rows remain Fleet-owned, rather than
    # a local orphan recovery or interrupt transferring only the core row.
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 hour'"))
        await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    expired = await leases(engine)
    assert await app.state.run_store.claim_for_takeover(record.run_id, grace_seconds=0, error="local takeover") is False
    assert await RunManager(store=app.state.run_store).reconcile_orphaned_inflight_runs(error="startup") == []
    for strategy in ("interrupt", "rollback"):
        with pytest.raises(HTTPException) as exc:
            await services.start_run(body(multitask_strategy=strategy), record.thread_id, request(RunManager(store=app.state.run_store), user))
        assert exc.value.status_code == 409
    assert await leases(engine) == expired


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["generation", "owner", "run_expired", "attempt_expired", "token"])
async def test_invalid_owner_fences_never_renew_one_side(owner_environment, change):
    env = owner_environment
    engine, _, _, _, app, _, session_id, _, _ = env
    accepted = await claim(env)
    statements = {
        "generation": "UPDATE fleet_agent_tasks SET generation=generation+1",
        "owner": "UPDATE runs SET owner_worker_id='foreign-owner'",
        "run_expired": "UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 hour'",
        "attempt_expired": "UPDATE fleet_attempts SET lease_expires_at=clock_timestamp()-interval '1 hour'",
        "token": "UPDATE fleet_attempts SET token_hash='changed-token-digest'",
    }
    async with engine.begin() as conn:
        await conn.execute(text(statements[change]))
    before = await leases(engine)
    with pytest.raises((ValueError, PermissionError)):
        await app.state.fleet_ownership.renew(node_id="node-c03", node_session_id=session_id, attempt_id=accepted.attempt_id, token=accepted.token)
    assert await leases(engine) == before


@pytest.mark.asyncio
async def test_actual_run_update_fault_rolls_back_attempt_renewal(owner_environment):
    env = owner_environment
    engine, _, _, _, app, _, session_id, _, _ = env
    accepted = await claim(env)
    before = await leases(engine)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE FUNCTION reject_run_lease() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'run lease fault'; END $$"))
        await conn.execute(text("CREATE TRIGGER reject_run_lease BEFORE UPDATE ON runs FOR EACH ROW EXECUTE FUNCTION reject_run_lease()"))
    with pytest.raises(DBAPIError):
        await app.state.fleet_ownership.renew(node_id="node-c03", node_session_id=session_id, attempt_id=accepted.attempt_id, token=accepted.token, running=True)
    assert await leases(engine) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
async def test_actual_scheduler_recovery_preserves_remote_owner(owner_environment, accepted):
    from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
    from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow
    from deerflow.persistence.scheduled_tasks.sql import ScheduledTaskRepository

    env = owner_environment
    engine, sf, user, _, app, record, _, _, _ = env
    if accepted:
        await claim(env)
    now = datetime.now(UTC)
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync: ScheduledTaskRow.__table__.create(sync))
        await conn.run_sync(lambda sync: ScheduledTaskRunRow.__table__.create(sync))
        if accepted:
            await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 hour'"))
            await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    async with sf.begin() as session:
        session.add(ScheduledTaskRow(id="schedule-c03", user_id=user.id, thread_id=record.thread_id, title="C03", prompt="test", schedule_type="once", schedule_spec={}, timezone="UTC", status="running", last_run_id=record.run_id))
        session.add(ScheduledTaskRunRow(id="occurrence-c03", task_id="schedule-c03", thread_id=record.thread_id, run_id=record.run_id, scheduled_for=now, trigger="schedule", status="running"))
    repo = ScheduledTaskRepository(sf, run_repository=app.state.run_store)
    assert await repo.reconcile_stuck_once_tasks(error="scheduler restarted", now=now, lease_grace_seconds=0) == 0
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT status FROM scheduled_tasks"))).scalar_one() == "running"
        assert (await conn.execute(text("SELECT status FROM runs"))).scalar_one() == "pending"


@pytest.mark.asyncio
async def test_private_placement_sql_guard_survives_missing_legacy_label(owner_environment):
    engine, _, _, _, app, record, _, _, _ = owner_environment
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET kwargs_json='{}'"))
    assert await app.state.run_store.list_inflight_with_expired_lease(grace_seconds=0) == []
    assert await app.state.run_store.claim_for_takeover(record.run_id, grace_seconds=0, error="local takeover") is False


@pytest.mark.asyncio
async def test_agent_unit_limit_is_independent_of_free_cpu_memory(owner_environment):
    env = owner_environment
    engine, _, user, _, app, _, _, _, _ = env
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_nodes SET cpu_millis=4000,memory_mib=8192"))
    assert await claim(env) is not None
    record = await services.start_run(body(), "thread-c03-second", request(RunManager(store=app.state.run_store), user), execution_backend=backend())
    assert await claim(env) is None
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT sum(cpu_millis),sum(memory_mib),sum(agent_units) FROM fleet_reservations"))).one() == (1000, 2048, 1)
        assert (await conn.execute(text("SELECT owner_worker_id FROM runs WHERE run_id=:id"), {"id": record.run_id})).scalar_one() is None


@pytest.mark.asyncio
async def test_actual_core_update_fault_rolls_back_claim_and_capacity(owner_environment):
    engine, _, _, _, _, _, _, _, _ = owner_environment
    async with engine.begin() as conn:
        await conn.execute(text("CREATE FUNCTION reject_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'claim owner fault'; END $$"))
        await conn.execute(text("CREATE TRIGGER reject_claim BEFORE UPDATE ON runs FOR EACH ROW EXECUTE FUNCTION reject_claim()"))
    with pytest.raises(DBAPIError):
        await claim(owner_environment)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT state,active_attempt_id FROM fleet_run_placements"))).one() == ("queued", None)
        assert (await conn.execute(text("SELECT state FROM fleet_agent_tasks"))).scalar_one() == "queued"
        assert (await conn.execute(text("SELECT owner_worker_id,lease_expires_at FROM runs"))).one() == (None, None)


@pytest.mark.asyncio
async def test_operator_registration_defaults_jobs_only_and_requires_explicit_agent_budget(owner_environment):
    _, _, _, runtime, _, _, _, _, _ = owner_environment
    await runtime.nodes.register(node_id="default-c03", name="default-c03", cpu_millis=4000, memory_mib=8192)
    status = await runtime.nodes.status("default-c03")
    assert "remote" not in status["node"]["profile_allowlist"]
    assert status["node"]["agent_limit"] == 0
    with pytest.raises(ValueError, match="positive agent capacity"):
        await runtime.nodes.register(node_id="invalid-c03", name="invalid-c03", cpu_millis=4000, memory_mib=8192, profile_allowlist=["remote"])


@pytest.mark.parametrize("label", [None, "", "local", "remote space", 123])
def test_remote_admission_requires_valid_server_backend_label(label):
    from deerflow.runtime.execution.contracts import ExecutionPlan

    with pytest.raises(ValueError, match="nonlocal backend label"):
        ExecutionPlan(public_kwargs={"execution_backend": label}, store_only=True, participant=object())


@pytest.mark.asyncio
async def test_session_admin_http_agent_allowlist_requires_positive_limit(management_environment):
    from .test_b06_fleet_durable_jobs import serve

    fleet, app, headers, _, _, engine = management_environment
    cfg = fleet.config.model_copy(update={"profiles": fleet.config.profiles | backend().config.profiles})
    runtime = service_class()(cfg)
    await runtime.start(ExtensionRuntimeDeps(session_factory=fleet.session_factory))
    app.state.extensions = SimpleNamespace(services=(("fleet", runtime),))
    app.include_router(management_router())
    server, server_task, sock, url = await serve(app)
    try:
        async with httpx.AsyncClient(base_url=url) as client:
            packet = BODY | {"profile_allowlist": ["remote"]}
            rejected = await client.post("/api/fleet/machines", headers=headers["admin"], json=packet)
            assert rejected.status_code == 422
            response = await client.post("/api/fleet/machines", headers=headers["admin"], json=packet | {"agent_limit": 1})
            assert response.status_code == 201, response.text
            assert response.json()["node"]["profile_allowlist"] == ["remote"]
            for value in (-1, True, 1000001):
                assert (await client.post("/api/fleet/machines", headers=headers["admin"], json=packet | {"agent_limit": value})).status_code == 422
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT profile_allowlist,agent_limit FROM fleet_nodes"))).one() == (["remote"], 1)
    finally:
        server.should_exit = True
        await server_task
        sock.close()
        await runtime.stop()


@pytest.mark.asyncio
async def test_attempt_token_rotation_serializes_against_renew(owner_environment):
    env = owner_environment
    engine, _, _, _, app, _, session_id, _, _ = env
    accepted = await claim(env)
    identity = {"node_id": "node-c03", "node_session_id": session_id, "attempt_id": accepted.attempt_id, "token": accepted.token}
    before = await leases(engine)

    async def rotate():
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_attempts SET token_hash='rotated'"))

    renewed, _ = await asyncio.gather(app.state.fleet_ownership.renew(**identity), rotate(), return_exceptions=True)
    after = await leases(engine)
    if isinstance(renewed, PermissionError):
        assert after == before
    else:
        assert isinstance(renewed, dict) and not renewed["stop"]
        assert after[0][2] == after[1][2]
    with pytest.raises(PermissionError):
        await app.state.fleet_ownership.renew(**identity)
    assert await leases(engine) == after


@pytest.mark.asyncio
async def test_concurrent_local_takeover_cannot_split_accepted_renew(owner_environment):
    env = owner_environment
    engine, _, _, _, app, record, session_id, _, _ = env
    accepted = await claim(env)
    renewal, takeover = await asyncio.gather(
        app.state.fleet_ownership.renew(node_id="node-c03", node_session_id=session_id, attempt_id=accepted.attempt_id, token=accepted.token), app.state.run_store.claim_for_takeover(record.run_id, grace_seconds=0, error="local recovery")
    )
    assert renewal["stop"] is False and takeover is False
    run, attempt = await leases(engine)
    assert run[1] == "fleet-agent:" + accepted.attempt_id
    assert run[2] == attempt[2]


@pytest.mark.asyncio
async def test_agent_attempt_dispatch_rejects_other_node_before_operation_kind(owner_environment):
    env = owner_environment
    _, _, _, runtime, app, _, session_id, _, _ = env
    accepted = await claim(env)
    await runtime.nodes.register(node_id="other-c03", name="other-c03", cpu_millis=1000, memory_mib=2048)
    other = await runtime.credentials.issue("other-c03", lifetime_seconds=600)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/fleet/node/attempts/" + accepted.attempt_id + "/start", headers={"Authorization": "Bearer " + other.token}, json={"node_session_id": session_id, "token": accepted.token})
        assert response.status_code == 403
