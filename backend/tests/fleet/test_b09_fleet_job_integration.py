"""Real lead-agent submission, worker publication and restart-safe notification."""

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import httpx
import pytest
import test_runtime_lifecycle_e2e as runtime_e2e
from _agent_e2e_helpers import FakeToolCallingModel
from deerflow_extension_api import ExtensionRuntimeDeps
from langchain_core.messages import AIMessage
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import setup_tracking
from .test_b06_containers import docker
from .test_b06_fleet_durable_jobs import serve

isolated_app = runtime_e2e.isolated_app
isolated_deer_flow_home = runtime_e2e.isolated_deer_flow_home

pytestmark = [pytest.mark.no_auto_user, pytest.mark.integration, pytest.mark.asyncio]


async def wait_run(manager, run_id, owner):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 15
    while loop.time() < deadline:
        record = await manager.get(run_id, user_id=owner)
        if record.status.value in {"success", "error", "timeout", "interrupted"}:
            # Local success can precede the original worker's durable terminal write.
            assert record.task is not None, "Expected the original locally owned task"
            await asyncio.wait_for(asyncio.shield(record.task), timeout=max(0, deadline - loop.time()))
            assert record.status.value == "success", record.error
            assert not record.finalizing
            return record
        await asyncio.sleep(min(0.05, max(0, deadline - loop.time())))
    raise TimeoutError("Real Agent run did not settle within 15 seconds")


async def test_real_agent_job_busy_restart_notification_once(fleet_database, isolated_app, tmp_path):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit local Docker gate")
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.containers import DockerContainers
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon

    from app.fleet.runtime import install_fleet_tools
    from app.gateway.services import launch_mcp_task_notification_run
    from app.mcp_tasks.service import McpTaskService
    from deerflow.config.app_config import get_app_config
    from deerflow.mcp.tasks import McpTaskDriverRegistry
    from deerflow.mcp.tasks.fleet_runtime import set_fleet_job_submitter
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository
    from deerflow.runtime import ThreadOperationKind

    get_app_config().run_events.backend = "db"
    engine, sf, _ = fleet_database
    nas = tmp_path / "fleet-nas"
    nas.mkdir()
    cfg = settings(nas)
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    cfg = cfg.model_copy(update={"profiles": {"batch": cfg.profiles["batch"].model_copy(update={"image": image, "execution_timeout_seconds": 15})}})
    fleet = service_class()(cfg)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    registry = McpTaskDriverRegistry()
    registry.register("fleet", fleet.bind_tracking(await setup_tracking(engine, sf)))
    repo = McpTaskRepository(sf)
    services = []
    accepted_notifications = []
    launch_requests = []
    drop_launch_reply = True
    node_client = None
    server = server_task = sock = None
    model = FakeToolCallingModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "submit_fleet_job",
                        "args": {"task_name": "count", "profile": "batch", "argv": ["sh", "-c", "echo start >> /output/count; echo report-ready > /output/report.txt"], "execution_timeout_seconds": 15},
                        "id": "scripted-fleet-call",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Your computation is running in the background."),
            AIMessage(content="Your computation completed; the report is ready."),
        ]
    )
    try:
        with patch("deerflow.agents.lead_agent.agent.create_chat_model", return_value=model):
            async with isolated_app.router.lifespan_context(isolated_app):
                app = isolated_app
                app.state.extensions = replace(app.state.extensions, services=(("fleet", fleet),))
                manager = app.state.run_manager

                async def launch(**kwargs):
                    nonlocal drop_launch_reply
                    result = await launch_mcp_task_notification_run(app=app, **kwargs)
                    accepted_notifications.append(result)
                    launch_requests.append(kwargs)
                    if drop_launch_reply:
                        drop_launch_reply = False
                        raise ConnectionError("Lost response after real notification run admission")
                    return result

                def new_service():
                    service = McpTaskService(
                        repository=repo,
                        drivers=registry,
                        poll_interval_seconds=1,
                        lease_seconds=30,
                        max_concurrent_polls=1,
                        launch_notification=launch,
                        get_run=lambda run_id, **kwargs: manager.get(run_id, raise_on_store_error=True, **kwargs),
                    )
                    services.append(service)
                    install_fleet_tools(app, service)
                    app.state.mcp_task_repo = repo
                    app.state.mcp_task_service = service
                    app.state.mcp_tasks_available = True
                    return service

                tasks = new_service()
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
                    response = await http.post("/api/v1/auth/register", json={"email": "fleet-b09@example.com", "password": "StrongPass123!", "name": "Fleet B09"})
                    assert response.status_code == 201, response.text
                    owner = (await http.get("/api/v1/auth/me")).json()["id"]
                    csrf = {"X-CSRF-Token": http.cookies.get("csrf_token")}
                    thread = str(uuid.uuid4())
                    response = await http.post("/api/threads", json={"thread_id": thread}, headers=csrf)
                    assert response.status_code == 200, response.text
                    response = await http.post(
                        f"/api/threads/{thread}/runs",
                        headers=csrf,
                        json={
                            "assistant_id": "lead_agent",
                            "input": {"messages": [{"role": "user", "content": "Run the approved batch job."}]},
                            "context": {"thinking_enabled": False, "subagent_enabled": False},
                            "config": {"recursion_limit": 50},
                        },
                    )
                    assert response.status_code == 200, response.text
                    source = await wait_run(manager, response.json()["run_id"], owner)
                    async with engine.connect() as conn:
                        tracked = (await conn.execute(text("SELECT id,remote_task_id,user_id,thread_id,run_id FROM mcp_tasks"))).one()
                        assert (tracked.user_id, tracked.thread_id, tracked.run_id) == (owner, thread, source.run_id)
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
                    await fleet.jobs.reconcile(tracked.remote_task_id)
                    async with engine.begin() as conn:
                        await conn.execute(text("INSERT INTO fleet_nodes(id,name,cpu_millis,memory_mib) VALUES ('n','worker',1000,512)"))
                    credential = await fleet.credentials.issue("n", lifetime_seconds=600)
                    server, server_task, sock, url = await serve(app)
                    node_client = NodeClient(gateway_url=url, credential=credential.token, timeout_seconds=1)
                    state = tmp_path / "private-worker"
                    daemon = NodeDaemon(client=node_client, containers=DockerContainers(state_dir=state), workspace=fleet.workspace, state_dir=state, renew_seconds=0.2, safety_margin_seconds=0.1, poll_seconds=0.03)
                    await daemon.bootstrap()
                    async with manager.reserve_thread_operation(thread, kind=ThreadOperationKind.checkpoint_write, user_id=owner):
                        outcome = await asyncio.wait_for(daemon.execute_one(), timeout=15)
                        assert outcome["state"] == "succeeded" and not outcome["report_pending"]
                        await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
                        row = await repo.get(tracked.id, user_id=owner)
                        assert row["status"] == "completed"
                        assert row["notification_status"] == "pending"
                        assert row["notification_run_id"] is None
                        assert row["notified_version"] == 0
                        assert row["notification_attempt_count"] == 0
                        await tasks.stop()
                        tasks = new_service()
                    restarted_after_lost_reply = False
                    for _ in range(160):
                        await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
                        row = await repo.get(tracked.id, user_id=owner)
                        if accepted_notifications and not restarted_after_lost_reply:
                            assert row["notification_status"] == "pending"
                            assert row["notified_version"] == 0
                            assert row["notification_attempt_count"] == 1
                            await tasks.stop()
                            tasks = new_service()
                            restarted_after_lost_reply = True
                        if row["notification_status"] == "delivered":
                            break
                        await asyncio.sleep(0.05)
                    assert row["notification_status"] == "delivered", row
                    assert row["notified_version"] == row["event_version"] == 1
                    assert restarted_after_lost_reply
                    assert len(accepted_notifications) == 2
                    assert accepted_notifications[0] == accepted_notifications[1]
                    notified = await wait_run(manager, accepted_notifications[0]["run_id"], owner)
                    duplicate = await launch_mcp_task_notification_run(app=app, **launch_requests[0])
                    assert duplicate["run_id"] == notified.run_id
                    await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
                    unchanged = await repo.get(tracked.id, user_id=owner)
                    assert unchanged["notified_version"] == row["notified_version"]
                    runs = await manager.list_by_thread(thread, user_id=owner)
                    notification_runs = [run for run in runs if run.metadata.get("mcp_task_notification")]
                    assert len(notification_runs) == 1
                    from deerflow.persistence.engine import get_engine

                    async with get_engine().connect() as host_conn:
                        durable_run = (
                            await host_conn.execute(text("SELECT run_id,status FROM runs WHERE idempotency_key=:key"), {"key": f"mcp-task:{tracked.id}:{launch_requests[0]['dispatch_version']}:{launch_requests[0]['dispatch_attempt']}"})
                        ).one()
                        assert durable_run.run_id == notified.run_id and durable_run.status == "success"
                        receipts = (await host_conn.execute(text("SELECT user_id,content FROM run_events WHERE run_id=:run AND event_type='run.delivery'"), {"run": notified.run_id})).all()
                        assert len(receipts) == 1 and receipts[0].user_id == owner
                        assert receipts[0].content
                    accepted = await fleet.manifests.get(row["result"]["manifest_id"], user_id=owner, thread_id=thread)
                    assert (nas / accepted["output_prefix"] / "count").read_text().splitlines() == ["start"]
                    assert (nas / accepted["output_prefix"] / "report.txt").read_text().strip() == "report-ready"
                    async with engine.connect() as conn:
                        assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks WHERE notified_version=event_version AND notification_status='delivered'"))).scalar_one() == 1
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 1
                        process_ref = (await conn.execute(text("SELECT process_ref FROM fleet_attempts"))).scalar_one()
                    assert not json.loads(await docker("inspect", process_ref))[0]["State"]["Running"]
                    detail = await http.get(f"/api/threads/{thread}/mcp-tasks/{tracked.id}")
                    assert detail.status_code == 200, detail.text
                    snapshot = await http.get(f"/api/threads/{thread}")
                    assert snapshot.status_code == 200, snapshot.text
                    exposed = detail.text + snapshot.text + json.dumps(launch_requests[0]["event"])
                    for secret in (credential.token, accepted["output_prefix"], str(nas), tracked.remote_task_id):
                        assert secret not in exposed
    finally:
        try:
            set_fleet_job_submitter(None)
            for service in services:
                await service.stop()
            if node_client:
                await node_client.close()
            if server:
                server.should_exit = True
                try:
                    await asyncio.wait_for(server_task, timeout=5)
                finally:
                    sock.close()
            async with engine.connect() as conn:
                refs = (await conn.execute(text("SELECT process_ref FROM fleet_attempts WHERE process_ref IS NOT NULL"))).scalars().all()
            for ref in refs:
                await docker("rm", "-f", ref)
        finally:
            await fleet.stop()


async def test_real_scheduled_slots_http_boundary_and_one_docker_execution(fleet_database, isolated_app, tmp_path):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit local Docker gate")
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.containers import DockerContainers
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon

    from app.fleet.runtime import install_fleet_tools
    from app.gateway.services import launch_mcp_task_notification_run, launch_scheduled_thread_run
    from app.mcp_tasks.service import McpTaskService
    from deerflow.config.app_config import get_app_config
    from deerflow.mcp.tasks import McpTaskDriverRegistry
    from deerflow.mcp.tasks.fleet_runtime import set_fleet_job_submitter
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository
    from deerflow.runtime import ThreadOperationKind

    get_app_config().run_events.backend = "db"
    engine, sf, _ = fleet_database
    nas = tmp_path / "fleet-nas"
    nas.mkdir()
    cfg = settings(nas).model_copy(update={"scheduled_job_slots": {"daily": "batch"}})
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    cfg = cfg.model_copy(update={"profiles": {"batch": cfg.profiles["batch"].model_copy(update={"image": image, "execution_timeout_seconds": 15})}})
    fleet = service_class()(cfg)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    registry = McpTaskDriverRegistry()
    registry.register("fleet", fleet.bind_tracking(await setup_tracking(engine, sf)))
    repo = McpTaskRepository(sf)
    services = []
    accepted_notifications = []
    launch_requests = []
    drop_launch_reply = True
    node_client = None
    server = server_task = sock = None
    actual_configs = {}
    from app.gateway import services as gateway_services

    original_run_agent = gateway_services.run_agent

    async def observe_real_run(*args, **kwargs):
        actual_configs[args[2].run_id] = kwargs["config"]
        return await original_run_agent(*args, **kwargs)

    model = FakeToolCallingModel(
        responses=[
            *[AIMessage(content="External request finished.") for _ in range(4)],
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "submit_fleet_job",
                        "args": {"task_name": "count", "profile": "batch", "job_slot": "daily", "argv": ["sh", "-c", "echo start >> /output/count; echo report-ready > /output/report.txt"], "execution_timeout_seconds": 15},
                        "id": "scripted-fleet-call",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Your computation is running in the background."),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "submit_fleet_job",
                        "args": {"task_name": "new-name", "profile": "batch", "job_slot": "daily", "argv": ["sh", "-c", "echo different >> /output/count"], "execution_timeout_seconds": 15},
                        "id": "second-scheduled-call",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="The original job is still running."),
            AIMessage(content="Your computation completed; the report is ready."),
        ]
    )
    try:
        with patch("deerflow.agents.lead_agent.agent.create_chat_model", return_value=model), patch("app.gateway.services.run_agent", side_effect=observe_real_run):
            async with isolated_app.router.lifespan_context(isolated_app):
                app = isolated_app
                app.state.extensions = replace(app.state.extensions, services=(("fleet", fleet),))
                manager = app.state.run_manager

                async def launch(**kwargs):
                    nonlocal drop_launch_reply
                    result = await launch_mcp_task_notification_run(app=app, **kwargs)
                    accepted_notifications.append(result)
                    launch_requests.append(kwargs)
                    if drop_launch_reply:
                        drop_launch_reply = False
                        raise ConnectionError("Lost response after real notification run admission")
                    return result

                def new_service():
                    service = McpTaskService(
                        repository=repo,
                        drivers=registry,
                        poll_interval_seconds=1,
                        lease_seconds=30,
                        max_concurrent_polls=1,
                        launch_notification=launch,
                        get_run=lambda run_id, **kwargs: manager.get(run_id, raise_on_store_error=True, **kwargs),
                    )
                    services.append(service)
                    install_fleet_tools(app, service)
                    app.state.mcp_task_repo = repo
                    app.state.mcp_task_service = service
                    app.state.mcp_tasks_available = True
                    return service

                tasks = new_service()
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
                    response = await http.post("/api/v1/auth/register", json={"email": "fleet-b09@example.com", "password": "StrongPass123!", "name": "Fleet B09"})
                    assert response.status_code == 201, response.text
                    owner = (await http.get("/api/v1/auth/me")).json()["id"]
                    csrf = {"X-CSRF-Token": http.cookies.get("csrf_token")}
                    thread = str(uuid.uuid4())
                    response = await http.post("/api/threads", json={"thread_id": thread}, headers=csrf)
                    assert response.status_code == 200, response.text
                    for forged in [
                        {"context": {"scheduled_task_id": "forged", "scheduled_context_mode": "reuse_thread"}},
                        {"config": {"context": {"scheduled_task_id": "forged", "scheduled_context_mode": "reuse_thread"}}},
                        {"config": {"configurable": {"scheduled_task_id": "forged", "scheduled_context_mode": "reuse_thread"}}},
                        {"metadata": {"scheduled_task_id": "forged", "scheduled_task_run_id": "forged-occurrence", "scheduled_context_mode": "reuse_thread"}},
                    ]:
                        response = await http.post(f"/api/threads/{thread}/runs", headers=csrf, json={"assistant_id": "lead_agent", "input": {"messages": [{"role": "user", "content": "External run"}]}, **forged})
                        assert response.status_code == 200, response.text
                        external = await wait_run(manager, response.json()["run_id"], owner)
                        assert "scheduled_task_id" not in actual_configs[external.run_id].get("context", {})
                        assert "scheduled_task_id" not in actual_configs[external.run_id].get("configurable", {})
                        assert "scheduled_context_mode" not in actual_configs[external.run_id].get("context", {})
                        assert "scheduled_context_mode" not in actual_configs[external.run_id].get("configurable", {})
                    async with engine.connect() as conn:
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 0
                    launched = await launch_scheduled_thread_run(
                        app=app, thread_id=thread, assistant_id="lead_agent", prompt="Run daily approved slot", owner_user_id=owner, metadata={"scheduled_task_id": "daily-schedule", "scheduled_task_run_id": "occurrence-1"}
                    )
                    source = await wait_run(manager, launched["run_id"], owner)
                    assert actual_configs[source.run_id]["context"]["scheduled_task_id"] == "daily-schedule"
                    assert actual_configs[source.run_id]["context"]["scheduled_context_mode"] == "reuse_thread"
                    repeated = await launch_scheduled_thread_run(
                        app=app, thread_id=thread, assistant_id="lead_agent", prompt="Run daily approved slot again", owner_user_id=owner, metadata={"scheduled_task_id": "daily-schedule", "scheduled_task_run_id": "occurrence-2"}
                    )
                    second = await wait_run(manager, repeated["run_id"], owner)
                    assert second.run_id != source.run_id
                    async with engine.connect() as conn:
                        tracked = (await conn.execute(text("SELECT id,remote_task_id,user_id,thread_id,run_id FROM mcp_tasks"))).one()
                        assert (tracked.user_id, tracked.thread_id, tracked.run_id) == (owner, thread, source.run_id)
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
                    original_job = await fleet.jobs.get(tracked.remote_task_id, user_id=owner, thread_id=thread)
                    assert original_job["state"] in {"staged", "queued"}
                    # Both scheduler Agent occurrences are already successful;
                    # the detached computation has not executed yet.
                    assert source.status.value == second.status.value == "success"
                    original_tracking = await repo.get(tracked.id, user_id=owner)
                    assert original_tracking["status"] == "submitted"
                    assert original_tracking["task_name"] == "count"
                    await fleet.jobs.reconcile(tracked.remote_task_id)
                    async with engine.begin() as conn:
                        await conn.execute(text("INSERT INTO fleet_nodes(id,name,cpu_millis,memory_mib) VALUES ('n','worker',1000,512)"))
                    credential = await fleet.credentials.issue("n", lifetime_seconds=600)
                    server, server_task, sock, url = await serve(app)
                    node_client = NodeClient(gateway_url=url, credential=credential.token, timeout_seconds=1)
                    state = tmp_path / "private-worker"
                    daemon = NodeDaemon(client=node_client, containers=DockerContainers(state_dir=state), workspace=fleet.workspace, state_dir=state, renew_seconds=0.2, safety_margin_seconds=0.1, poll_seconds=0.03)
                    await daemon.bootstrap()
                    async with manager.reserve_thread_operation(thread, kind=ThreadOperationKind.checkpoint_write, user_id=owner):
                        outcome = await asyncio.wait_for(daemon.execute_one(), timeout=15)
                        assert outcome["state"] == "succeeded" and not outcome["report_pending"]
                        await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
                        row = await repo.get(tracked.id, user_id=owner)
                        assert row["status"] == "completed"
                        assert row["notification_status"] == "pending"
                        assert row["notification_run_id"] is None
                        assert row["notified_version"] == 0
                        assert row["notification_attempt_count"] == 0
                        await tasks.stop()
                        tasks = new_service()
                    restarted_after_lost_reply = False
                    for _ in range(160):
                        await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
                        row = await repo.get(tracked.id, user_id=owner)
                        if accepted_notifications and not restarted_after_lost_reply:
                            assert row["notification_status"] == "pending"
                            assert row["notified_version"] == 0
                            assert row["notification_attempt_count"] == 1
                            await tasks.stop()
                            tasks = new_service()
                            restarted_after_lost_reply = True
                        if row["notification_status"] == "delivered":
                            break
                        await asyncio.sleep(0.05)
                    assert row["notification_status"] == "delivered", row
                    assert row["notified_version"] == row["event_version"] == 1
                    assert restarted_after_lost_reply
                    assert len(accepted_notifications) == 2
                    assert accepted_notifications[0] == accepted_notifications[1]
                    notified = await wait_run(manager, accepted_notifications[0]["run_id"], owner)
                    duplicate = await launch_mcp_task_notification_run(app=app, **launch_requests[0])
                    assert duplicate["run_id"] == notified.run_id
                    await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
                    unchanged = await repo.get(tracked.id, user_id=owner)
                    assert unchanged["notified_version"] == row["notified_version"]
                    runs = await manager.list_by_thread(thread, user_id=owner)
                    notification_runs = [run for run in runs if run.metadata.get("mcp_task_notification")]
                    assert len(notification_runs) == 1
                    from deerflow.persistence.engine import get_engine

                    async with get_engine().connect() as host_conn:
                        durable_run = (
                            await host_conn.execute(text("SELECT run_id,status FROM runs WHERE idempotency_key=:key"), {"key": f"mcp-task:{tracked.id}:{launch_requests[0]['dispatch_version']}:{launch_requests[0]['dispatch_attempt']}"})
                        ).one()
                        assert durable_run.run_id == notified.run_id and durable_run.status == "success"
                        receipts = (await host_conn.execute(text("SELECT user_id,content FROM run_events WHERE run_id=:run AND event_type='run.delivery'"), {"run": notified.run_id})).all()
                        assert len(receipts) == 1 and receipts[0].user_id == owner
                        assert receipts[0].content
                    accepted = await fleet.manifests.get(row["result"]["manifest_id"], user_id=owner, thread_id=thread)
                    assert (nas / accepted["output_prefix"] / "count").read_text().splitlines() == ["start"]
                    assert (nas / accepted["output_prefix"] / "report.txt").read_text().strip() == "report-ready"
                    async with engine.connect() as conn:
                        assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks WHERE notified_version=event_version AND notification_status='delivered'"))).scalar_one() == 1
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
                        assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 1
                        process_ref = (await conn.execute(text("SELECT process_ref FROM fleet_attempts"))).scalar_one()
                    assert not json.loads(await docker("inspect", process_ref))[0]["State"]["Running"]
                    detail = await http.get(f"/api/threads/{thread}/mcp-tasks/{tracked.id}")
                    assert detail.status_code == 200, detail.text
                    snapshot = await http.get(f"/api/threads/{thread}")
                    assert snapshot.status_code == 200, snapshot.text
                    exposed = detail.text + snapshot.text + json.dumps(launch_requests[0]["event"])
                    for secret in (credential.token, accepted["output_prefix"], str(nas), tracked.remote_task_id):
                        assert secret not in exposed
    finally:
        try:
            set_fleet_job_submitter(None)
            for service in services:
                await service.stop()
            if node_client:
                await node_client.close()
            if server:
                server.should_exit = True
                try:
                    await asyncio.wait_for(server_task, timeout=5)
                finally:
                    sock.close()
            async with engine.connect() as conn:
                refs = (await conn.execute(text("SELECT process_ref FROM fleet_attempts WHERE process_ref IS NOT NULL"))).scalars().all()
            for ref in refs:
                await docker("rm", "-f", ref)
        finally:
            await fleet.stop()


@asynccontextmanager
async def held_real_terminal_persist(app, monkeypatch):
    """Pause the original worker before durable success, without changing state."""
    from deerflow.config.app_config import get_app_config

    get_app_config().run_events.backend = "db"
    model = FakeToolCallingModel(responses=[AIMessage(content="Settlement control complete.")])
    release = asyncio.Event()
    entered = asyncio.Event()
    terminal_observed = asyncio.Event()
    record = None
    waiter = None
    with patch("deerflow.agents.lead_agent.agent.create_chat_model", return_value=model):
        async with app.router.lifespan_context(app):
            manager = app.state.run_manager
            original_status = manager.set_status_if_not_cancelled
            original_get = manager.get

            async def hold_terminal(run_id, *args, **kwargs):
                current = manager._runs.get(run_id)
                if kwargs.get("persist", True) and current is not None and current.status.value == "success":
                    entered.set()
                    await release.wait()
                return await original_status(run_id, *args, **kwargs)

            async def observe_terminal(run_id, **kwargs):
                current = await original_get(run_id, **kwargs)
                if current.status.value == "success":
                    terminal_observed.set()
                return current

            monkeypatch.setattr(manager, "set_status_if_not_cancelled", hold_terminal)
            monkeypatch.setattr(manager, "get", observe_terminal)
            try:
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
                    response = await http.post("/api/v1/auth/register", json={"email": "settlement@example.com", "password": "StrongPass123!", "name": "Settlement"})
                    assert response.status_code == 201, response.text
                    owner = (await http.get("/api/v1/auth/me")).json()["id"]
                    csrf = {"X-CSRF-Token": http.cookies.get("csrf_token")}
                    thread = str(uuid.uuid4())
                    response = await http.post("/api/threads", json={"thread_id": thread}, headers=csrf)
                    assert response.status_code == 200, response.text
                    response = await http.post(f"/api/threads/{thread}/runs", headers=csrf, json={"assistant_id": "lead_agent", "input": {"messages": [{"role": "user", "content": "Finish this run"}]}})
                    assert response.status_code == 200, response.text
                    await asyncio.wait_for(entered.wait(), 5)
                    record = manager._runs[response.json()["run_id"]]
                    assert record.task is not None and not record.task.done()
                    assert record.status.value == "success"
                    assert (await manager._store.get(record.run_id, user_id=owner))["status"] == "running"
                    waiter = asyncio.create_task(wait_run(manager, record.run_id, owner))
                    yield manager, record, waiter, release, terminal_observed
            finally:
                release.set()
                if record is not None and record.task is not None:
                    await asyncio.wait_for(asyncio.shield(record.task), 5)
                if waiter is not None:
                    await asyncio.gather(waiter, return_exceptions=True)


async def test_wait_run_waits_for_original_task_and_durable_success(isolated_app, monkeypatch):
    async with held_real_terminal_persist(isolated_app, monkeypatch) as (manager, record, waiter, release, terminal_observed):
        await asyncio.wait_for(terminal_observed.wait(), 5)
        # A scheduler turn exposes an early return; the persist boundary stays held.
        await asyncio.sleep(0)
        assert not waiter.done(), "Terminal memory status is not owned-task settlement"
        assert not record.task.done()
        assert (await manager._store.get(record.run_id, user_id=record.user_id))["status"] == "running"
        release.set()
        assert await asyncio.wait_for(waiter, 5) is record
        assert record.task.done() and not record.finalizing
        assert (await manager._store.get(record.run_id, user_id=record.user_id))["status"] == "success"


async def test_wait_run_timeout_preserves_original_owned_task(isolated_app, monkeypatch):
    async with held_real_terminal_persist(isolated_app, monkeypatch) as (manager, record, waiter, release, terminal_observed):
        with pytest.raises(TimeoutError):
            await waiter
        assert not record.task.done() and not record.task.cancelled()
        assert (await manager._store.get(record.run_id, user_id=record.user_id))["status"] == "running"
        release.set()
        await asyncio.wait_for(asyncio.shield(record.task), 5)
        assert record.status.value == "success" and not record.finalizing
        assert (await manager._store.get(record.run_id, user_id=record.user_id))["status"] == "success"
