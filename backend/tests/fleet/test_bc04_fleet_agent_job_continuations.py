"""Native exclusive delivery ownership with the original SQL runtimes."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text

from .test_bc03_fleet_agent_job_continuations import admission as admission
from .test_bc03_fleet_agent_job_continuations import checkpoint_owner as checkpoint_owner
from .test_bc03_fleet_agent_job_continuations import fleet_database as fleet_database
from .test_bc03_fleet_agent_job_continuations import owner_environment as owner_environment


async def prepare_delivery(item, tmp_path, monkeypatch):
    from app.fleet.job_tracking import register_fleet_driver
    from deerflow.mcp.tasks import McpTaskDriverRegistry
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop

    app, sf = item.env[4], item.env[1]
    app.state.mcp_task_repo = McpTaskRepository(sf)
    drivers = McpTaskDriverRegistry()
    register_fleet_driver(app, drivers)
    from app.fleet.continuations import FleetContinuations

    original_dispatch = FleetContinuations.dispatch
    gate = asyncio.Event()

    async def delayed_dispatch(coordinator, group_id):
        await asyncio.wait_for(gate.wait(), timeout=15)
        return await original_dispatch(coordinator, group_id)

    monkeypatch.setattr(FleetContinuations, "dispatch", delayed_dispatch)
    try:
        observed = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path, monkeypatch, continuation_case="before_seal")
        from app.gateway.services import launch_mcp_task_notification_run
        from app.mcp_tasks import McpTaskService

        services = [
            McpTaskService(
                repository=app.state.mcp_task_repo,
                drivers=drivers,
                poll_interval_seconds=1,
                lease_seconds=30,
                max_concurrent_polls=1,
                launch_notification=lambda **kwargs: launch_mcp_task_notification_run(app=app, **kwargs),
                get_run=lambda run_id, **kwargs: app.state.run_manager.get(run_id, **kwargs),
            )
            for _ in range(2)
        ]
        await configure_notification_host(app, sf, item.env[2], monkeypatch)
        detached = await complete_detached(item, services[0])
        return services, detached, observed
    finally:
        gate.set()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc04_exclusive_result_delivery(checkpoint_owner, tmp_path, monkeypatch):
    from app.fleet.continuations import FleetContinuations

    item = checkpoint_owner
    services, detached, observed = await prepare_delivery(item, tmp_path, monkeypatch)
    app, sf = item.env[4], item.env[1]
    coordinator = FleetContinuations(sf, item.env[3].config, app.state.run_manager)
    await asyncio.gather(*(service.run_once(now=datetime.now(UTC) + timedelta(seconds=2)) for service in services), coordinator.dispatch(observed["group_id"]))
    for _ in range(3):
        await asyncio.gather(*(service.run_once(now=datetime.now(UTC) + timedelta(seconds=2)) for service in services))
        await settle_workers(app)
    evidence = await delivery_observation(item, detached)
    save_evidence("main-sql.json", evidence)
    assert evidence["detached_delivered"]
    assert evidence["awaited_generic_notifications"] == evidence["duplicate_modifying_runs"] == 0
    assert evidence["continuation_count"] == 1 and len(evidence["runs"]) == 3
    assert len(evidence["awaited"]) == 2 and all(row["status"] == "completed" and row["dispatch_version"] is None for row in evidence["awaited"])


async def settle_workers(app):
    for record in list(app.state.run_manager._runs.values()):
        if record.task is not None:
            await record.task


def save_evidence(name, value):
    if directory := os.environ.get("BC04_EVIDENCE_DIR"):
        Path(directory, name).write_text(json.dumps(value, default=str, indent=2))


async def delivery_observation(item, detached):
    async with item.engine.connect() as connection:
        awaited = list(
            (
                await connection.execute(
                    text(
                        "SELECT m.id,m.status,m.event_version,m.notified_version,m.dispatch_version,m.notification_status,m.notification_run_id,m.notification_lease_owner FROM mcp_tasks m JOIN "
                        "fleet_jobs j ON j.tracking_task_id=m.id JOIN fleet_job_links l ON l.job_id=j.id WHERE l.link_mode='awaited'"
                    )
                )
            ).mappings()
        )
        runs = list((await connection.execute(text("SELECT run_id,thread_id,status,metadata_json,created_at,updated_at FROM runs ORDER BY created_at"))).mappings())
        delivery = (
            (
                await connection.execute(
                    text(
                        "SELECT m.id,m.notification_status,m.event_version,m.notified_version,m.dispatch_attempt,m.notification_attempt_count,r.run_id,r.status FROM mcp_tasks m JOIN runs r ON "
                        "r.metadata_json->'mcp_task_notification'->>'task_id'=m.id WHERE m.id=:id"
                    ),
                    {"id": detached["id"]},
                )
            )
            .mappings()
            .one()
        )
        groups = list((await connection.execute(text("SELECT id,state,continuation_run_id,dispatched_at FROM fleet_wait_groups"))).mappings())
        duplicates = await connection.scalar(
            text(
                "SELECT count(*) FROM runs a JOIN runs b ON a.run_id<b.run_id AND a.thread_id=b.thread_id WHERE a.created_at<CASE WHEN b.status IN "
                "('success','error','timeout','interrupted') THEN b.updated_at ELSE 'infinity'::timestamptz END AND b.created_at<CASE WHEN a.status IN "
                "('success','error','timeout','interrupted') THEN a.updated_at ELSE 'infinity'::timestamptz END"
            )
        )
    awaited_ids = {row["id"] for row in awaited}
    notifications = [row for row in runs if row["metadata_json"].get("mcp_task_notification", {}).get("task_id") in awaited_ids]
    return {
        "awaited": [dict(row) for row in awaited],
        "runs": [dict(row) for row in runs],
        "groups": [dict(row) for row in groups],
        "delivery": dict(delivery),
        "awaited_generic_notifications": len(notifications),
        "duplicate_modifying_runs": duplicates,
        "continuation_count": sum(row["continuation_run_id"] is not None for row in groups),
        "detached_delivered": delivery["notification_status"] == "delivered" and delivery["status"] == "success" and delivery["notified_version"] == delivery["event_version"],
    }


async def configure_notification_host(app, sf, user, monkeypatch):
    from langchain_core.messages import AIMessage
    from langgraph.graph import END, START, MessagesState, StateGraph

    from app.gateway import services
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime import MemoryStreamBridge

    from .test_c02_remote_agent_admission import request

    template = request(app.state.run_manager, user).app.state
    for name in ("checkpointer", "store", "run_event_store", "run_events_config"):
        setattr(app.state, name, getattr(template, name))
    from deerflow_extension_api import ExtensionData

    from deerflow.extensions.registry import LoadedExtensions

    app.state.extensions = LoadedExtensions(app_store=ExtensionData("bc04-app"), services=app.state.extensions.services)
    from app.gateway.auth.local_provider import LocalAuthProvider
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from deerflow.persistence.user.model import UserRow

    async with sf.begin() as session:
        await session.connection()
        await session.run_sync(lambda sync: UserRow.__table__.create(sync.connection(), checkfirst=True))
    provider = LocalAuthProvider(SQLiteUserRepository(sf))
    monkeypatch.setattr(services, "get_local_provider", lambda: provider)
    app.state.thread_store = ThreadMetaRepository(sf)
    app.state.stream_bridge = MemoryStreamBridge()

    app.state.delivery_hold = asyncio.Event()
    app.state.delivery_hold.set()
    app.state.delivery_holding = asyncio.Event()

    async def deliver(state):
        if any(message.content == "hold-bc04" for message in state["messages"]):
            app.state.delivery_holding.set()
            await app.state.delivery_hold.wait()
        return {"messages": [AIMessage(content="Background result received.")]}

    def factory(**kwargs):
        graph = StateGraph(MessagesState)
        graph.add_node("deliver", deliver)
        graph.add_edge(START, "deliver")
        graph.add_edge("deliver", END)
        return graph.compile()

    # Deterministic graph fixture; original start_run/run_agent/SQL remain real.
    monkeypatch.setattr(services, "resolve_agent_factory", lambda assistant_id: factory)
    await app.state.thread_store.create("bc04-detached", user_id=user.id)
    return app


async def complete_detached(item, service):
    from dataclasses import asdict

    from deerflow_ecs_fleet.persistence.attempts import JobAttempts
    from deerflow_ecs_fleet.persistence.manifests import FleetManifests
    from deerflow_ecs_fleet.workspace import NASWorkspace

    from deerflow.mcp.tasks import TaskSubmitRequest

    task = await service.submit(
        driver_name="fleet",
        request=TaskSubmitRequest(
            user_id=item.spec.user_id,
            thread_id="bc04-detached",
            run_id=None,
            tool_call_id=None,
            server_name="fleet",
            task_name="detached",
            arguments={"task_name": "detached", "profile": "batch", "argv": ["/bin/sh", "-c", "printf detached-result > report.txt"]},
            driver_data={"invocation_id": "bc04-detached"},
        ),
    )
    jobs, runtime = service.drivers.get("fleet").jobs, item.env[3]
    await jobs.reconcile(task["remote_task_id"])
    claim = await runtime.scheduler.claim_job(item.identity.node_id, node_session_id=item.identity.node_session_id)
    assert claim is not None and claim.job_id == task["remote_task_id"]
    attempts = JobAttempts(item.env[1], runtime.config)
    identity = dict(node_id=item.identity.node_id, node_session_id=item.identity.node_session_id, attempt_id=claim.attempt_id, token=claim.token)
    grant = await attempts.authorize_start(**identity)
    await attempts.renew(**identity, running=True)
    workspace = NASWorkspace(runtime.config.nas_root, identity=runtime.config.nas_identity)
    output = workspace.prepare(asdict(claim), grant)
    child = await asyncio.create_subprocess_exec(*claim.spec["argv"], cwd=output)
    await child.wait()
    assert child.returncode == 0
    manifest = workspace.seal(asdict(claim), grant, stopped=True, max_bytes=1024)
    await attempts.stopped(**identity, reason="exit", exit_code=child.returncode)
    await FleetManifests(item.env[1], attempts=attempts, workspace=workspace).complete(**identity, manifest=manifest)
    save_evidence("detached-process-nas.json", {"job_id": claim.job_id, "pid": child.pid, "exit": child.returncode, "output_prefix": str(output), "manifest": manifest})
    return task


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc04_busy_lost_reply_restart_recovers_original_receipts(checkpoint_owner, tmp_path, monkeypatch):
    from app.fleet.continuations import FleetContinuations
    from app.fleet.delivery import FleetDeliveryPolicy
    from app.gateway import services as gateway
    from app.mcp_tasks import McpTaskService
    from app.mcp_tasks.errors import PermanentNotificationError
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime import RunManager

    from .test_c02_remote_agent_admission import body, request

    item = checkpoint_owner
    pollers, detached, observed = await prepare_delivery(item, tmp_path, monkeypatch)
    app, sf = item.env[4], item.env[1]
    app.state.delivery_hold.clear()
    req = request(app.state.run_manager, item.env[2])
    req.app = app
    blocker = await gateway.start_run(body("hold-bc04").model_copy(update={"interrupt_before": None, "interrupt_after": None, "stream_subgraphs": False, "stream_mode": ["values"]}), "bc04-detached", req)
    try:
        await asyncio.wait_for(app.state.delivery_holding.wait(), timeout=10)
        for _ in range(2):
            await asyncio.gather(*(poller.run_once(now=datetime.now(UTC) + timedelta(seconds=2)) for poller in pollers))
        busy = await app.state.mcp_task_repo.get(detached["id"], user_id=item.spec.user_id)
        save_evidence("busy-before-receipt.json", busy)
        assert busy["notification_status"] == "pending" and busy["notification_attempt_count"] == busy["dispatch_attempt"] == 0
        assert busy["notification_error"] and busy["notification_run_id"] is None
    finally:
        app.state.delivery_hold.set()
        await blocker.task

    # Both an already-held claim and direct internal launch consult DB ownership.
    legacy_claims = await McpTaskRepository(sf).claim_notification_work(now=datetime.now(UTC), lease_owner="legacy-bc04", lease_seconds=30, limit=2, tracking_degraded_after_errors=3)
    awaited_claims = [row for row in legacy_claims if row["thread_id"] == item.spec.thread_id]
    assert len(awaited_claims) == 2
    awaited = awaited_claims[0]
    save_evidence("already-held-awaited-claims.json", awaited_claims)
    await pollers[0]._notify_one(awaited, now=datetime.now(UTC))
    with pytest.raises(PermanentNotificationError, match="durable coordinator"):
        await gateway.launch_mcp_task_notification_run(app=app, thread_id=awaited["thread_id"], assistant_id=None, owner_user_id=awaited["user_id"], task_id=awaited["id"], dispatch_version=1, dispatch_attempt=0, event={})

    launched = []

    async def lose_reply(**kwargs):
        receipt = await gateway.launch_mcp_task_notification_run(app=app, **kwargs)
        launched.append(receipt)
        async with item.engine.connect() as connection:
            committed = (await connection.execute(text("SELECT run_id,status,idempotency_key,metadata_json FROM runs WHERE run_id=:id"), {"id": receipt["run_id"]})).mappings().one()
        save_evidence("committed-run-before-lost-reply.json", dict(committed))
        # Actual run admission committed; only the transport reply is lost.
        raise RuntimeError("fixture lost reply after committed run")

    pollers[0]._launch_notification = lose_reply
    await pollers[0]._run_notifications(now=datetime.now(UTC) + timedelta(seconds=10))
    await settle_workers(app)
    assert len(launched) == 1
    lost = await app.state.mcp_task_repo.get(detached["id"], user_id=item.spec.user_id)
    save_evidence("lost-reply-before-tracking-receipt.json", {"launch": launched, "tracking": lost})
    assert lost["notification_run_id"] is None and lost["notification_attempt_count"] == 1 and lost["dispatch_attempt"] == 0

    # Reconstruct the actual host participants against the original database.
    app.state.mcp_task_repo = McpTaskRepository(sf)
    app.state.mcp_task_repo.bind_notification_policy(FleetDeliveryPolicy())
    app.state.run_manager = RunManager(store=RunRepository(sf))
    recovered = McpTaskService(
        repository=app.state.mcp_task_repo,
        drivers=pollers[0].drivers,
        poll_interval_seconds=1,
        lease_seconds=30,
        max_concurrent_polls=1,
        launch_notification=lambda **kwargs: gateway.launch_mcp_task_notification_run(app=app, **kwargs),
        get_run=lambda run_id, **kwargs: app.state.run_manager.get(run_id, **kwargs),
    )
    await recovered._run_notifications(now=datetime.now(UTC) + timedelta(seconds=20))
    receipt = await app.state.mcp_task_repo.get(detached["id"], user_id=item.spec.user_id)
    save_evidence("recovered-tracking-receipt.json", receipt)
    assert receipt["notification_run_id"] == launched[0]["run_id"] and receipt["notification_status"] == "dispatched"
    await recovered._run_notifications(now=datetime.now(UTC) + timedelta(seconds=20))
    config = item.env[3].config.model_copy(update={"enabled": False, "agents_enabled": False, "continuations_enabled": False})
    continuation = await FleetContinuations(sf, config, app.state.run_manager).dispatch(observed["group_id"])
    evidence = await delivery_observation(item, detached)
    save_evidence("boundary-sql.json", evidence)
    assert evidence["detached_delivered"] and evidence["delivery"]["run_id"] == launched[0]["run_id"]
    assert evidence["awaited_generic_notifications"] == evidence["duplicate_modifying_runs"] == 0
    assert evidence["continuation_count"] == 1 and len(evidence["runs"]) == 4
    assert continuation.run_id == evidence["groups"][0]["continuation_run_id"]
