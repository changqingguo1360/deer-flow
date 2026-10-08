"""C10 uses the actual HTTP admission and durable scheduler/resource ledger."""

from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c02_remote_agent_admission import request


@pytest_asyncio.fixture
async def c10_environment(admission, monkeypatch, tmp_path):
    from app.gateway.auth_middleware import AuthMiddleware
    from app.gateway.csrf_middleware import CSRFMiddleware
    from app.gateway.internal_auth import create_internal_auth_headers
    from app.gateway.routers.thread_runs import router
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime import RunManager

    engine, sf, schema, user = admission
    import json
    import os
    from types import SimpleNamespace

    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_extension_api import ExtensionRuntimeDeps

    from app.fleet.ownership import install_fleet_ownership
    from app.gateway.routers.scheduled_tasks import router as scheduled_router
    from app.gateway.services import launch_scheduled_thread_run
    from app.scheduler.service import ScheduledTaskService
    from deerflow.config.paths import get_paths
    from deerflow.persistence.scheduled_task_runs import ScheduledTaskRunRepository
    from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow

    from .test_b02_fleet_foundation import service_class, settings

    compatibility_path = os.environ.get("C10_COMPATIBILITY_PATH")
    models_path = os.environ.get("C10_MODEL_BINDINGS_PATH")
    image = os.environ.get("C10_RUNNER_IMAGE")
    if not compatibility_path or not models_path or not image:
        pytest.skip("requires explicit C10 installed compatibility, model bindings and pinned runner image inputs")
    compatibility = json.loads(Path(compatibility_path).read_text())
    model = json.loads(Path(models_path).read_text())["model-1"]
    from uuid import UUID, uuid4

    from app.gateway.auth.models import User
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from deerflow.persistence.user.model import UserRow

    user.id = str(uuid4())
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: UserRow.__table__.create(sync))
    users = SQLiteUserRepository(sf)
    await users.create_user(User(id=UUID(user.id), email="c10-owner@example.com", system_role="user"))
    monkeypatch.setattr("app.gateway.deps._cached_repo", users)
    monkeypatch.setattr("app.gateway.deps._cached_local_provider", None)
    cfg = settings(tmp_path).model_dump(mode="json")
    cfg.update(
        agents_enabled=True,
        profiles=cfg["profiles"] | {"remote": {"kind": "agent", "image": image, "runtime_digest": compatibility["runtime_digest"], "cpu_millis": 1000, "memory_mib": 2048}},
        agent_bindings={
            "remote": {"allowed_user_ids": [user.id], "model_name": "model-1", "model_version": model["version"], "compatibility": compatibility, "secret_refs": [{"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"}]}
        },
    )
    runtime = service_class()(FleetConfig.model_validate(cfg))
    await runtime.start(ExtensionRuntimeDeps(session_factory=sf))
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: ScheduledTaskRow.__table__.create(sync))
        await connection.run_sync(lambda sync: ScheduledTaskRunRow.__table__.create(sync))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "host-home"))
    owned = get_paths().sandbox_user_data_dir("thread-c10-http", user_id=user.id) / "uploads"
    owned.mkdir(parents=True)
    (owned / "owner-input.txt").write_text("authenticated-owner-input")
    app = FastAPI()
    req = request(RunManager(store=RunRepository(sf)), user)
    for name, value in vars(req.app.state).items():
        setattr(app.state, name, value)
    await app.state.thread_store.create("thread-c10-http", user_id=user.id)
    app.state.extensions = SimpleNamespace(services=(("fleet", runtime),))
    app.state.run_store = app.state.run_manager._store
    install_fleet_ownership(app, sf)
    app.state.scheduled_task_repo = ScheduledTaskRepository(sf, run_repository=app.state.run_store)
    app.state.scheduled_task_run_repo = ScheduledTaskRunRepository(sf, run_repository=app.state.run_store)

    def scheduler():
        return ScheduledTaskService(
            task_repo=app.state.scheduled_task_repo,
            task_run_repo=app.state.scheduled_task_run_repo,
            launch_run=lambda **kwargs: launch_scheduled_thread_run(app=app, **kwargs),
            execution_admission=app.state.fleet_scheduler_tickets,
            poll_interval_seconds=60,
            lease_seconds=60,
            max_concurrent_runs=1,
            queue_timeout_seconds=3600,
        )

    app.state.scheduled_task_service = scheduler()
    app.include_router(scheduled_router)
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(router)
    headers = create_internal_auth_headers(owner_user_id=user.id)
    headers["X-CSRF-Token"] = "c10-csrf"
    try:
        yield SimpleNamespace(**locals())
    finally:
        await runtime.stop()
        evidence = os.environ.get("C10_EVIDENCE_DIR")
        if evidence:
            Path(evidence).mkdir(parents=True, exist_ok=True)
            with (Path(evidence) / "fixture-cleanup.jsonl").open("a") as record:
                record.write(json.dumps({"schema": schema, "fleet_runtime_ready_after_stop": runtime.ready, "runner_started": False, "fixture_scope": "native HTTP/PostgreSQL only"}) + "\n")


@pytest.mark.asyncio
async def test_http_remote_profile_and_scheduled_capacity_handoff(c10_environment):
    item = c10_environment
    app, sf, user, runtime, headers, tmp_path, compatibility, scheduler = (item.app, item.sf, item.user, item.runtime, item.headers, item.tmp_path, item.compatibility, item.scheduler)
    import asyncio
    from datetime import UTC, datetime, timedelta

    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility

    from app.gateway.services import launch_scheduled_thread_run

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://native", headers=headers, cookies={"csrf_token": "c10-csrf"}) as client:
        response = await client.post(
            "/api/threads/thread-c10-http/runs",
            json={
                "input": {"messages": [{"role": "user", "content": "hello"}]},
                "execution": {"preference": "remote", "profile": "remote"},
                "context": {"user_id": "forged-owner", "execution_backend": "local", "placement": "forged"},
            },
        )
        assert response.status_code == 200, response.text
        async with sf() as session:
            stored = (await session.execute(text("SELECT user_id,kwargs_json FROM runs"))).one()
            assert stored.user_id == user.id
            assert stored.kwargs_json["execution_backend"] == "fleet"
            observed_owner = stored.user_id

        from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest, AgentWorkspaceSnapshots

        async with sf() as session:
            payload = await session.scalar(text("SELECT payload FROM fleet_launch_specs"))
        snapshot_path = tmp_path / ".fleet-agent-inputs" / user.id / "thread-c10-http" / payload["workspace_manifest_ref"]
        manifest = AgentWorkspaceManifest.model_validate_json((snapshot_path / "manifest.json").read_text())
        assert manifest.user_id == user.id and len(manifest.files) == 1
        target = tmp_path / "prepared"
        target.mkdir()
        AgentWorkspaceSnapshots(tmp_path, state_dir=tmp_path.parent / ("control-" + tmp_path.name)).prepare(payload, target)
        assert (target / ".deer-flow" / "users" / user.id / "threads" / "thread-c10-http" / "user-data" / "uploads" / "owner-input.txt").read_text() == "authenticated-owner-input"

        created = await client.post(
            "/api/scheduled-tasks",
            json={
                "title": "C10",
                "prompt": "scheduled hello",
                "context_mode": "fresh_thread_per_run",
                "schedule_type": "once",
                "schedule_spec": {"run_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
                "timezone": "UTC",
                "execution": {"preference": "remote", "profile": "remote"},
            },
        )
        assert created.status_code == 200, created.text
        task_id = created.json()["id"]
        triggered = await client.post("/api/scheduled-tasks/" + task_id + "/trigger")
        assert triggered.status_code == 200, triggered.text
        async with sf() as session:
            occurrence = (await session.execute(text("SELECT id,thread_id,status,created_at FROM scheduled_task_runs"))).one()
            assert occurrence.status == "queued"
            queued_budget = await session.scalar(text("SELECT count(*) FROM scheduled_task_runs WHERE status IN ('launching','running')"))
            assert queued_budget == 0
            assert await session.scalar(text("SELECT count(*) FROM runs WHERE idempotency_key=:key"), {"key": "scheduled-task:" + occurrence.id}) == 0
            assert await session.scalar(text("SELECT count(*) FROM fleet_scheduler_tickets")) == 0

        await runtime.nodes.register(node_id="node-c10", name="node-c10", cpu_millis=1000, memory_mib=2048, agent_limit=1, profile_allowlist=["remote"])
        node_session = await runtime.nodes.open_session("node-c10", protocol_version=1)
        assert await app.state.fleet_ownership.claim_agent("node-c10", node_session_id=node_session["node_session_id"], worker=WorkerCompatibility.model_validate(compatibility)) is None
        await runtime.nodes.heartbeat("node-c10", node_session_id=node_session["node_session_id"], protocol_version=1)
        task = await app.state.scheduled_task_repo.get_internal(task_id)
        queued = await app.state.scheduled_task_run_repo.get_active_run(task_id)
        peers = [scheduler(), scheduler()]
        outcomes = await asyncio.gather(*(peer._attempt_queued_run(task, queued, now=datetime.now(UTC)) for peer in peers))
        assert sorted(item["outcome"] for item in outcomes) == ["launched", "queued"], outcomes
        async with sf() as session:
            ticket = (await session.execute(text("SELECT id,node_id,run_id,lease_owner,state FROM fleet_scheduler_tickets"))).one()
            reserved = (await session.execute(text("SELECT id,attempt_id,ticket_id,cpu_millis,memory_mib,agent_units FROM fleet_reservations"))).one()
            assert ticket.state == "consumed" and reserved.attempt_id is None and reserved.ticket_id == ticket.id
            assert await session.scalar(text("SELECT count(*) FROM runs WHERE idempotency_key=:key"), {"key": "scheduled-task:" + occurrence.id}) == 1
            assert await session.scalar(text("SELECT created_at FROM scheduled_task_runs")) == occurrence.created_at
        repeated = await launch_scheduled_thread_run(
            app=app,
            thread_id=occurrence.thread_id,
            assistant_id=task["assistant_id"],
            prompt=task["prompt"],
            owner_user_id=user.id,
            execution=task["execution"],
            execution_ticket=ticket.id,
            execution_lease_owner=ticket.lease_owner,
            metadata={"scheduled_task_id": task_id, "scheduled_task_run_id": occurrence.id, "scheduled_trigger": "manual", "scheduled_context_mode": "fresh_thread_per_run"},
        )
        assert repeated["run_id"] == ticket.run_id
        accepted = await app.state.fleet_ownership.claim_agent("node-c10", node_session_id=node_session["node_session_id"], worker=WorkerCompatibility.model_validate(compatibility))
        assert accepted is not None and accepted.run_id == ticket.run_id
        async with sf() as session:
            transferred = (await session.execute(text("SELECT id,attempt_id,ticket_id,cpu_millis,memory_mib,agent_units FROM fleet_reservations"))).one()
            assert transferred.id == reserved.id and transferred.attempt_id == accepted.attempt_id
            assert tuple(transferred[2:]) == tuple(reserved[2:])
            assert await session.scalar(text("SELECT count(*) FROM fleet_attempts")) == 1
            assert await session.scalar(text("SELECT count(*) FROM fleet_reservations")) == 1
            assert await session.scalar(text("SELECT count(*) FROM scheduled_task_runs WHERE status IN ('launching','running')")) == 1
        import json
        import os

        evidence = os.environ.get("C10_EVIDENCE_DIR")
        if evidence:
            destination = Path(evidence)
            destination.mkdir(parents=True, exist_ok=True)
            async with sf() as session:
                observations = {
                    "forged_owner_accepted": observed_owner == "forged-owner",
                    "actual_owner": observed_owner,
                    "queued_budget_usage": queued_budget,
                    "scheduler_outcomes": [value["outcome"] for value in outcomes],
                    "runs_for_occurrence_key": await session.scalar(text("SELECT count(*) FROM runs WHERE idempotency_key=:key"), {"key": "scheduled-task:" + occurrence.id}),
                    "reservation_count": await session.scalar(text("SELECT count(*) FROM fleet_reservations")),
                    "reservation_id_before_claim": reserved.id,
                    "reservation_id_after_claim": transferred.id,
                    "actual_attempt_id": accepted.attempt_id,
                    "installed_runtime_digest": compatibility["runtime_digest"],
                    "runner_image": item.image,
                }
            (destination / "main-observations.json").write_text(json.dumps(observations, indent=2))


@pytest.mark.asyncio
async def test_external_permissions_local_default_and_unassigned_cancel(c10_environment, monkeypatch):
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from app.gateway.auth.jwt import create_access_token
    from app.gateway.auth.models import User
    from deerflow.runtime import RunManager
    from deerflow.runtime.runs.manager import CancelOutcome

    item = c10_environment
    app, sf, user = item.app, item.sf, item.user
    app.state.run_manager = RunManager(store=app.state.run_store)
    headers = {"X-CSRF-Token": "c10-csrf"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://native", headers=headers, cookies={"csrf_token": "c10-csrf", "access_token": create_access_token(user.id)}) as client:
        response = await client.post(
            "/api/threads/thread-c10-http/runs",
            json={
                "input": {"messages": [{"role": "user", "content": "hello"}]},
                "execution": {"preference": "remote", "profile": "remote"},
                "context": {"user_id": "forged-owner", "non_interactive": True},
                "config": {"context": {"owner_worker_id": "forged", "token": "forged", "placement": "forged"}},
            },
        )
        assert response.status_code == 200, response.text
        remote_id = response.json()["run_id"]
        async with sf() as session:
            row = (await session.execute(text("SELECT user_id,kwargs_json FROM runs"))).one()
            payload = await session.scalar(text("SELECT payload FROM fleet_launch_specs"))
            assert row.user_id == user.id
            assert payload["normalized_config"]["context"]["user_id"] == user.id
            assert "non_interactive" not in payload["normalized_config"]["context"]
            assert not ({"token", "placement", "owner_worker_id"} & payload["normalized_config"]["context"].keys())
        cancelled = await app.state.run_manager.cancel(remote_id)
        assert cancelled == CancelOutcome.cancelled
        refreshed = await client.get("/api/threads/thread-c10-http/runs/" + remote_id)
        assert refreshed.status_code == 200 and refreshed.json()["status"] == "interrupted", refreshed.text
        repeated_cancel = await client.post("/api/threads/thread-c10-http/runs/" + remote_id + "/cancel?wait=true")
        assert repeated_cancel.status_code == 204, repeated_cancel.text
        async with sf() as session:
            assert await session.scalar(text("SELECT status FROM runs WHERE run_id=:id"), {"id": remote_id}) == "interrupted"
            assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "cancelled"
            assert await session.scalar(text("SELECT count(*) FROM fleet_attempts")) == 0
        # Existing thread remains bound to Fleet; default Local regression uses a fresh thread.
        await app.state.thread_store.create("thread-c10-local", user_id=user.id)
        worker = AsyncMock()
        monkeypatch.setattr("app.gateway.services.run_agent", worker)
        local = await client.post("/api/threads/thread-c10-local/runs", json={"input": {"messages": [{"role": "user", "content": "local"}]}})
        assert local.status_code == 200, local.text
        record = await app.state.run_manager.get(local.json()["run_id"], user_id=user.id)
        assert record.task is not None and not record.store_only
        await record.task
        worker.assert_awaited_once()
        await app.state.run_manager.cancel(record.run_id)
        outsider = User(id=uuid4(), email="c10-other@example.com", system_role="user")
        await item.users.create_user(outsider)
        await app.state.thread_store.create("thread-c10-other", user_id=str(outsider.id))
        client.cookies.set("access_token", create_access_token(str(outsider.id)))
        denied = await client.post("/api/threads/thread-c10-other/runs", json={"input": {"messages": [{"role": "user", "content": "denied"}]}, "execution": {"preference": "remote", "profile": "remote"}, "context": {"user_id": user.id}})
        assert denied.status_code == 403, denied.text
        async with sf() as session:
            assert await session.scalar(text("SELECT count(*) FROM runs WHERE thread_id='thread-c10-other'")) == 0

        # Cancel read the unassigned placement, but real claim wins before the
        # cancellation path acquires any parent/task/run/placement row locks.
        import asyncio
        import json
        import os

        from deerflow_ecs_fleet.launch_spec import WorkerCompatibility

        from app.fleet.scheduler_tickets import FleetSchedulerTickets

        client.cookies.set("access_token", create_access_token(user.id))
        await app.state.thread_store.create("thread-c10-cancel-handoff", user_id=user.id)
        handoff = await client.post(
            "/api/threads/thread-c10-cancel-handoff/runs",
            json={
                "input": {"messages": [{"role": "user", "content": "cancel handoff"}]},
                "execution": {"preference": "remote", "profile": "remote"},
            },
        )
        assert handoff.status_code == 200, handoff.text
        handoff_run = handoff.json()["run_id"]
        await item.runtime.nodes.register(node_id="node-c10-cancel-handoff", name="node-c10-cancel-handoff", cpu_millis=1000, memory_mib=2048, agent_limit=1, profile_allowlist=["remote"])
        node_session = await item.runtime.nodes.open_session("node-c10-cancel-handoff", protocol_version=1)
        await item.runtime.nodes.heartbeat("node-c10-cancel-handoff", node_session_id=node_session["node_session_id"], protocol_version=1)
        entered, release = asyncio.Event(), asyncio.Event()
        original_cancel_unassigned = FleetSchedulerTickets.cancel_unassigned

        async def gated_unassigned_cancel(tickets, session, *, run_id, action):
            if run_id == handoff_run:
                entered.set()
                await asyncio.wait_for(release.wait(), timeout=5)
            return await original_cancel_unassigned(tickets, session, run_id=run_id, action=action)

        monkeypatch.setattr(FleetSchedulerTickets, "cancel_unassigned", gated_unassigned_cancel)
        cancellation = asyncio.create_task(client.post("/api/threads/thread-c10-cancel-handoff/runs/" + handoff_run + "/cancel"))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            accepted = await asyncio.wait_for(app.state.fleet_ownership.claim_agent("node-c10-cancel-handoff", node_session_id=node_session["node_session_id"], worker=WorkerCompatibility.model_validate(item.compatibility)), timeout=5)
            assert accepted is not None and accepted.run_id == handoff_run
            # claim_agent's own transaction has committed before releasing cancel.
            async with sf() as session:
                assert await session.scalar(text("SELECT active_attempt_id FROM fleet_run_placements WHERE run_id=:id"), {"id": handoff_run}) == accepted.attempt_id
            release.set()
            outcome = await asyncio.wait_for(cancellation, timeout=5)
            assert outcome.status_code == 202, outcome.text
            async with sf() as session:
                state = (
                    await session.execute(
                        text("SELECT r.cancel_action,a.stopped_at,v.state,v.attempt_id FROM runs r JOIN fleet_attempts a ON a.run_id=r.run_id JOIN fleet_reservations v ON v.attempt_id=a.id WHERE r.run_id=:id"), {"id": handoff_run}
                    )
                ).one()
                assert state.cancel_action == "interrupt"
                assert state.stopped_at is None and state.state == "reserved" and state.attempt_id == accepted.attempt_id
                evidence = os.environ.get("C10_EVIDENCE_DIR")
                if evidence:
                    (Path(evidence) / "cancel-handoff-observations.json").write_text(json.dumps(dict(state._mapping), indent=2))
        finally:
            release.set()
            if not cancellation.done():
                cancellation.cancel()
            await asyncio.gather(cancellation, return_exceptions=True)
            monkeypatch.setattr(FleetSchedulerTickets, "cancel_unassigned", original_cancel_unassigned)


@pytest.mark.asyncio
async def test_ticket_crash_expiry_original_run_retry_and_retirement(c10_environment, monkeypatch):
    import asyncio
    from datetime import UTC, datetime, timedelta

    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility

    from app.fleet.execution import FleetRunAdmission

    item = c10_environment
    app, sf, user, runtime = item.app, item.sf, item.user, item.runtime
    tickets = app.state.fleet_scheduler_tickets
    tickets.config = tickets.config.model_copy(update={"ticket_seconds": 1})
    await runtime.nodes.register(node_id="node-c10-recovery", name="node-c10-recovery", cpu_millis=1000, memory_mib=2048, agent_limit=1, profile_allowlist=["remote"])
    node_session = await runtime.nodes.open_session("node-c10-recovery", protocol_version=1)
    assert await app.state.fleet_ownership.claim_agent("node-c10-recovery", node_session_id=node_session["node_session_id"], worker=WorkerCompatibility.model_validate(item.compatibility)) is None
    await runtime.nodes.heartbeat("node-c10-recovery", node_session_id=node_session["node_session_id"], protocol_version=1)
    task = await app.state.scheduled_task_repo.create(
        task_id="task-c10-recovery",
        user_id=user.id,
        thread_id=None,
        context_mode="fresh_thread_per_run",
        assistant_id="lead_agent",
        title="recovery",
        prompt="hello",
        schedule_type="once",
        schedule_spec={"run_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
        timezone="UTC",
        next_run_at=datetime.now(UTC) + timedelta(hours=1),
        execution={"preference": "remote", "profile": "remote"},
    )
    original = FleetRunAdmission.insert

    async def crash_after_original_insert(participant, session, admitted):
        await original(participant, session, admitted)
        raise asyncio.CancelledError("original admission transaction crashed")

    monkeypatch.setattr(FleetRunAdmission, "insert", crash_after_original_insert)
    with pytest.raises(asyncio.CancelledError):
        await item.scheduler().dispatch_task(task, now=datetime.now(UTC), trigger="manual")
    monkeypatch.setattr(FleetRunAdmission, "insert", original)
    async with sf() as session:
        assert await session.scalar(text("SELECT count(*) FROM runs")) == 0
        before = (await session.execute(text("SELECT id,thread_id,status,created_at FROM scheduled_task_runs"))).one()
        assert before.status == "launching"
        assert await session.scalar(text("SELECT count(*) FROM fleet_scheduler_tickets WHERE state='held'")) == 1
    await asyncio.sleep(1.1)
    await tickets.reconcile()
    queued = await app.state.scheduled_task_run_repo.get_active_run(task["id"])
    assert queued["status"] == "queued"
    launched = await item.scheduler()._attempt_queued_run(task, queued, now=datetime.now(UTC))
    assert launched["outcome"] == "launched", launched
    run_id = launched["run_id"]
    async with sf() as session:
        payload = await session.scalar(text("SELECT payload FROM fleet_launch_specs"))
    # Later host edits cannot redefine the accepted original input snapshot.
    from deerflow.config.paths import get_paths

    new_input = get_paths().sandbox_user_data_dir(before.thread_id, user_id=user.id) / "uploads"
    new_input.mkdir(parents=True)
    (new_input / "later.txt").write_text("must not replace accepted inputs")
    await asyncio.sleep(1.1)
    await tickets.reconcile()
    queued = await app.state.scheduled_task_run_repo.get_active_run(task["id"])
    assert queued["status"] == "queued" and queued["run_id"] == run_id
    async with sf() as session:
        assert await session.scalar(text("SELECT count(*) FROM scheduled_task_runs WHERE status IN ('launching','running')")) == 0
        assert await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE state!='released'")) == 0
    retried = await item.scheduler()._attempt_queued_run(task, queued, now=datetime.now(UTC))
    assert retried["outcome"] == "launched" and retried["run_id"] == run_id, retried
    async with sf() as session:
        assert await session.scalar(text("SELECT count(*) FROM runs")) == 1
        assert await session.scalar(text("SELECT created_at FROM scheduled_task_runs")) == before.created_at
        assert (await session.scalar(text("SELECT payload FROM fleet_launch_specs")))["workspace_manifest_ref"] == payload["workspace_manifest_ref"]
        assert await session.scalar(text("SELECT run_count FROM scheduled_tasks")) == 1
        replacement = (await session.execute(text("SELECT t.state,r.attempt_id FROM fleet_scheduler_tickets t JOIN fleet_reservations r ON r.ticket_id=t.id WHERE r.state!='released'"))).one()
        assert replacement.state == "consumed" and replacement.attempt_id is None
    await asyncio.sleep(1.1)
    await tickets.reconcile()
    paused = await app.state.scheduled_task_repo.pause_with_queue_cancellation(task["id"], user_id=user.id, error="paused", now=datetime.now(UTC))
    assert paused == "paused"
    async with sf() as session:
        assert await session.scalar(text("SELECT status FROM runs")) == "interrupted"
        assert await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE state!='released'")) == 0
        assert await session.scalar(text("SELECT count(*) FROM fleet_attempts")) == 0

    # The remaining two retirement entry points reuse the same never-started protocol.
    async def waiting_parent(task_id):
        definition = await app.state.scheduled_task_repo.create(
            task_id=task_id,
            user_id=user.id,
            thread_id=None,
            context_mode="fresh_thread_per_run",
            assistant_id="lead_agent",
            title=task_id,
            prompt="hello",
            schedule_type="once",
            schedule_spec={"run_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
            timezone="UTC",
            next_run_at=datetime.now(UTC) + timedelta(hours=1),
            execution={"preference": "remote", "profile": "remote"},
        )
        result = await item.scheduler().dispatch_task(definition, now=datetime.now(UTC), trigger="manual")
        assert result["outcome"] == "launched", result
        await asyncio.sleep(1.1)
        await tickets.reconcile()
        return definition, result

    deleted_task, deleted_run = await waiting_parent("task-c10-delete")
    assert await app.state.scheduled_task_repo.delete_with_queue_cancellation(deleted_task["id"], user_id=user.id, error="deleted", now=datetime.now(UTC)) == "deleted"
    timeout_task, timeout_run = await waiting_parent("task-c10-timeout")
    expired = await app.state.scheduled_task_run_repo.expire_queued_runs(created_before=datetime.now(UTC) - timedelta(seconds=1), error="queue timeout", now=datetime.now(UTC))
    assert any(value["task_id"] == timeout_task["id"] for value in expired)
    async with sf() as session:
        assert await session.scalar(text("SELECT status FROM runs WHERE run_id=:id"), {"id": deleted_run["run_id"]}) == "interrupted"
        assert await session.scalar(text("SELECT status FROM runs WHERE run_id=:id"), {"id": timeout_run["run_id"]}) == "interrupted"
        leaks = await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE state!='released' AND attempt_id IS NULL"))
        assert leaks == 0
    # Assignment changes release authority: expiry alone cannot free this capacity.
    assigned_definition = await app.state.scheduled_task_repo.create(
        task_id="task-c10-assigned",
        user_id=user.id,
        thread_id=None,
        context_mode="fresh_thread_per_run",
        assistant_id="lead_agent",
        title="assigned",
        prompt="hello",
        schedule_type="once",
        schedule_spec={"run_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
        timezone="UTC",
        next_run_at=datetime.now(UTC) + timedelta(hours=1),
        execution={"preference": "remote", "profile": "remote"},
    )
    assigned_run = await item.scheduler().dispatch_task(assigned_definition, now=datetime.now(UTC), trigger="manual")
    accepted = await app.state.fleet_ownership.claim_agent("node-c10-recovery", node_session_id=node_session["node_session_id"], worker=WorkerCompatibility.model_validate(item.compatibility))
    assert accepted is not None and accepted.run_id == assigned_run["run_id"]
    await asyncio.sleep(1.1)
    await tickets.reconcile()
    async with sf() as session:
        held = (await session.execute(text("SELECT attempt_id,state FROM fleet_reservations WHERE state!='released'"))).one()
        assert held.attempt_id == accepted.attempt_id and held.state == "reserved"
        actual_stopped_at = await session.scalar(text("SELECT stopped_at FROM fleet_attempts WHERE id=:id"), {"id": accepted.attempt_id})
        assert actual_stopped_at is None
    import json
    import os

    evidence = os.environ.get("C10_EVIDENCE_DIR")
    if evidence:
        Path(evidence).mkdir(parents=True, exist_ok=True)
        (Path(evidence) / "recovery-observations.json").write_text(
            json.dumps(
                {"ticket_leaks_after_reconcile": leaks, "original_run_id": run_id, "retried_run_id": retried["run_id"], "assigned_reservation_after_expiry": dict(held._mapping), "assigned_attempt_stopped_at": actual_stopped_at}, indent=2
            )
        )


def test_initial_capture_enumeration_and_destination_failure_bounds(monkeypatch, tmp_path):
    """Small real inventories and deterministic destination faults expose ownership."""
    import errno
    import os
    from contextlib import contextmanager

    from app.fleet import initial_workspace
    from deerflow.config.paths import get_paths

    from .test_b02_fleet_foundation import settings

    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "host"))
    original_open, original_close = os.open, os.close
    original_listdir, original_scandir, original_mkdir = os.listdir, os.scandir, os.mkdir
    problems, observations = [], {}
    for fault in ("enumeration", "category-open", "directory-mkdir", "leaf-open"):
        nas = tmp_path / fault
        nas.mkdir()
        cfg = settings(nas)
        source = get_paths().sandbox_user_data_dir("thread-" + fault, user_id="owner")
        uploads = source / "uploads"
        uploads.mkdir(parents=True)
        if fault == "enumeration":
            for index in range(8):
                (uploads / str(index)).write_text("x")
        else:
            (uploads / "nested").mkdir()
            (uploads / "leaf.txt").write_text("x")
        source_inodes = {os.stat(path).st_ino for path in (uploads, *uploads.iterdir())}
        source_fds, closed = set(), []
        read_count = [0]
        upload_inode = uploads.stat().st_ino

        def source_directory(fd):
            return isinstance(fd, int) and os.fstat(fd).st_ino == upload_inode

        def counted_listdir(fd):
            names = original_listdir(fd)
            if source_directory(fd):
                read_count[0] += len(names)
            return names

        @contextmanager
        def counted_scandir(fd):
            with original_scandir(fd) as entries:
                if source_directory(fd):

                    def counted():
                        for entry in entries:
                            read_count[0] += 1
                            yield entry

                    yield counted()
                else:
                    yield entries

        def tracked_open(path, flags, *args, **kwargs):
            if fault == "leaf-open" and path == "leaf.txt" and flags & os.O_WRONLY:
                raise OSError(errno.ENOSPC, "destination full")
            if fault == "category-open" and path == "uploads" and source_fds:
                raise OSError(errno.EMFILE, "destination unavailable")
            fd = original_open(path, flags, *args, **kwargs)
            if os.fstat(fd).st_ino in source_inodes:
                source_fds.add(fd)
            return fd

        def tracked_close(fd):
            if fd in source_fds:
                source_fds.remove(fd)
                closed.append(fd)
            return original_close(fd)

        def failing_mkdir(path, *args, **kwargs):
            if fault == "directory-mkdir" and path == "nested":
                raise OSError(errno.EACCES, "destination denied")
            return original_mkdir(path, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(os, "open", tracked_open)
            patch.setattr(os, "close", tracked_close)
            patch.setattr(os, "mkdir", failing_mkdir)
            if fault == "enumeration":
                patch.setattr(initial_workspace, "MAX_FILES", 2)
                patch.setattr(os, "listdir", counted_listdir)
                patch.setattr(os, "scandir", counted_scandir)
            try:
                with pytest.raises(ValueError if fault == "enumeration" else OSError):
                    initial_workspace.capture(cfg, user_id="owner", thread_id="thread-" + fault)
                observations[fault] = {"enumerated_entries": read_count[0], "source_descriptors_closed": len(closed), "source_descriptors_leaked": len(source_fds)}
                if fault == "enumeration" and read_count[0] != 3:
                    problems.append(f"enumeration consumed {read_count[0]} entries instead of bounded limit+1")
                if source_fds:
                    problems.append(f"{fault} leaked {len(source_fds)} source descriptors")
            finally:
                # RED must not leak the descriptors it proved were abandoned.
                for fd in tuple(source_fds):
                    original_close(fd)
        if list(nas.glob(".fleet-agent-inputs/owner/thread-*/*")):
            problems.append(f"{fault} left capture staging content")
    import json

    evidence = os.environ.get("C10_EVIDENCE_DIR")
    if evidence:
        (Path(evidence) / "capture-boundary-observations.json").write_text(json.dumps(observations, indent=2))
    assert not problems, problems
