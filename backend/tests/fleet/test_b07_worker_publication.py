"""A real Docker job publishes an accepted NAS result through real host HTTP."""

import asyncio
import inspect
import json
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import setup_tracking
from .test_b06_containers import docker
from .test_b06_fleet_durable_jobs import serve


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["normal", "completion_reply_lost"])
async def test_real_worker_accepts_manifest_and_replays_lost_completion(fleet_database, tmp_path, fault):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit local Docker gate")
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.containers import DockerContainers
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon

    assert "workspace" in inspect.signature(NodeDaemon.__init__).parameters, "Worker does not publish accepted artifacts"
    from app.gateway.routers.fleet_nodes import router
    from app.mcp_tasks.service import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSubmitRequest
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    engine, sf, _ = fleet_database
    config = settings(tmp_path)
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    config = config.model_copy(update={"profiles": {"batch": config.profiles["batch"].model_copy(update={"image": image, "execution_timeout_seconds": 15})}})
    fleet = service_class()(config)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    reader = await setup_tracking(engine, sf)
    registry = McpTaskDriverRegistry()
    registry.register("fleet", fleet.bind_tracking(reader))
    repo = McpTaskRepository(sf)
    tasks = McpTaskService(repository=repo, drivers=registry, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib) VALUES ('n','worker',1000,512)"))
    credential = await fleet.credentials.issue("n", lifetime_seconds=600)
    tracked = await tasks.submit(
        driver_name="fleet",
        request=TaskSubmitRequest(
            user_id="u",
            thread_id="t",
            run_id="r",
            server_name="fleet",
            task_name="report",
            tool_call_id="call",
            driver_data={"invocation_id": "invocation"},
            arguments={"task_name": "report", "profile": "batch", "argv": ["sh", "-c", "echo start >> /output/count; printf 'report ready' > /output/report.txt"], "execution_timeout_seconds": 15},
        ),
    )
    await fleet.jobs.reconcile(tracked["remote_task_id"])
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(router)

    class DropCompletionReply:
        async def __call__(self, scope, receive, send):
            if scope.get("path", "").endswith("/complete"):

                async def drop(message):
                    if message["type"] == "http.response.start":
                        raise RuntimeError("Test dropped reply after accepted-manifest commit")
                    await send(message)

                await app(scope, receive, drop)
            else:
                await app(scope, receive, send)

    server, task, sock, url = await serve(DropCompletionReply() if fault == "completion_reply_lost" else app)
    state = tmp_path / "private-worker"
    client = NodeClient(gateway_url=url, credential=credential.token, timeout_seconds=0.5)
    clients = [client]
    ref = None
    try:
        daemon = NodeDaemon(client=client, containers=DockerContainers(state_dir=state), workspace=fleet.workspace, state_dir=state, renew_seconds=0.2, safety_margin_seconds=0.1, poll_seconds=0.03)
        await daemon.bootstrap()
        result = await asyncio.wait_for(daemon.execute_one(), timeout=10)
        assert result["report_pending"] == (fault == "completion_reply_lost")
        async with engine.connect() as conn:
            ref = (await conn.execute(text("SELECT process_ref FROM fleet_attempts"))).scalar_one()
            assert (await conn.execute(text("SELECT state FROM fleet_jobs"))).scalar_one() == "succeeded"
            assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 1
        assert not json.loads(await docker("inspect", ref))[0]["State"]["Running"]
        if fault == "completion_reply_lost":
            server.should_exit = True
            await task
            sock.close()
            server, task, sock, url = await serve(app)
            client = NodeClient(gateway_url=url, credential=credential.token, timeout_seconds=0.5)
            clients.append(client)
            restarted = NodeDaemon(client=client, containers=DockerContainers(state_dir=state), workspace=fleet.workspace, state_dir=state, renew_seconds=0.2, safety_margin_seconds=0.1, poll_seconds=0.03)
            assert (await restarted.bootstrap())["health"] == "online"
            assert await restarted.execute_one() is None
        await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
        row = await repo.get(tracked["id"], user_id="u")
        assert row["status"] == "completed"
        assert row["result"]["manifest_id"]
        accepted = await fleet.manifests.get(row["result"]["manifest_id"], user_id="u", thread_id="t")
        with fleet.workspace.open_artifact({key: accepted[key] for key in ("schema_version", "output_prefix", "files", "total_bytes")}, "report.txt") as file:
            assert file.read() == b"report ready"
        assert (tmp_path / accepted["output_prefix"] / "count").read_text().splitlines() == ["start"]
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 1
    finally:
        for client in clients:
            await client.close()
        server.should_exit = True
        await task
        sock.close()
        if ref:
            await docker("rm", "-f", ref)
        await tasks.stop()
        await fleet.stop()
