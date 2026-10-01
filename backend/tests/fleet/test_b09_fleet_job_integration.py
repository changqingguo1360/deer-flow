"""Real lead-agent submission, worker publication and restart-safe notification."""

import asyncio
import json
import os
import uuid
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
    for _ in range(300):
        record = await manager.get(run_id, user_id=owner)
        if record.status.value in {"success", "error", "timeout", "interrupted"}:
            assert record.status.value == "success", record.error
            return record
        await asyncio.sleep(0.05)
    raise AssertionError("Real Agent run did not finish")


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
