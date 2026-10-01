"""A real HTTP/Docker/Postgres chain stops locally when its Gateway disappears."""

import asyncio
import importlib
import importlib.util
import json
import os
import socket
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import setup_tracking
from .test_b06_containers import docker


def require_worker():
    assert importlib.util.find_spec("deerflow_ecs_fleet.worker.daemon") is not None, "Fleet worker daemon missing"
    return importlib.import_module("deerflow_ecs_fleet.worker.daemon")


async def serve(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", lifespan="off", timeout_graceful_shutdown=2))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    for _ in range(100):
        if server.started:
            return server, task, sock, f"http://127.0.0.1:{port}"
        if task.done():
            await task
        await asyncio.sleep(0.01)
    raise AssertionError("Local test Gateway did not become ready")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["gateway_loss", "start_reply_lost"])
async def test_b06_contract(fleet_database, tmp_path, fault):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit local Docker gate")
    module = require_worker()
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.containers import DockerContainers

    from app.mcp_tasks.service import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSubmitRequest
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    engine, sf, _ = fleet_database
    cfg = settings(tmp_path)
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    cfg = cfg.model_copy(update={"profiles": {"batch": cfg.profiles["batch"].model_copy(update={"image": image, "execution_timeout_seconds": 2})}})
    fleet = service_class()(cfg)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    reader = await setup_tracking(engine, sf)
    registry = McpTaskDriverRegistry()
    registry.register("fleet", fleet.bind_tracking(reader))
    tasks = McpTaskService(repository=McpTaskRepository(sf), drivers=registry, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib) VALUES ('n','worker',1000,512)"))
    credential = await fleet.credentials.issue("n", lifetime_seconds=600)
    request = TaskSubmitRequest(
        user_id="u",
        thread_id="t",
        run_id="r",
        tool_call_id="call",
        server_name="fleet",
        task_name="count",
        driver_data={"invocation_id": "invocation"},
        arguments={"task_name": "count", "profile": "batch", "argv": ["sh", "-c", "echo start >> /output/count; while true; do echo tick >> /output/ticks; sleep .1; done"], "execution_timeout_seconds": 2},
    )
    tracked = await tasks.submit(driver_name="fleet", request=request)
    await fleet.jobs.reconcile(tracked["remote_task_id"])
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    output = tmp_path / "output"
    output.mkdir(mode=0o777)
    output.chmod(0o777)

    async def workspace(claim, grant):
        return output

    class DropStartReply:
        async def __call__(self, scope, receive, send):
            if scope.get("path", "").endswith("/start"):

                async def drop(message):
                    if message["type"] == "http.response.start":
                        raise RuntimeError("Test connection dropped after durable start grant")
                    await send(message)

                await app(scope, receive, drop)
            else:
                await app(scope, receive, send)

    served_app = DropStartReply() if fault == "start_reply_lost" else app
    server, server_task, sock, url = await serve(served_app)
    client = NodeClient(gateway_url=url, credential=credential.token, timeout_seconds=0.3)
    state = tmp_path / "private-state"
    containers = DockerContainers(state_dir=state)
    daemon = module.NodeDaemon(client=client, containers=containers, state_dir=state, prepare_workspace=workspace, renew_seconds=0.15, safety_margin_seconds=0.1, poll_seconds=0.03)
    execution = None
    ref = None
    try:
        await daemon.bootstrap()
        execution = asyncio.create_task(daemon.execute_one())
        if fault == "start_reply_lost":
            with pytest.raises(httpx.HTTPStatusError):
                await execution
            async with engine.connect() as conn:
                attempt = (await conn.execute(text("SELECT start_authorized_at,stopped_at,process_ref FROM fleet_attempts"))).one()
                assert attempt.start_authorized_at is not None and attempt.stopped_at is not None
                assert (await conn.execute(text("SELECT state FROM fleet_jobs"))).scalar_one() == "unknown"
                assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
                assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
            assert await containers.inspect(attempt.process_ref) is None
            assert not (output / "count").exists()
            with pytest.raises(module.RecoveryRequired):
                await daemon.execute_one()
            return
        for _ in range(100):
            if (output / "ticks").exists():
                break
            if execution.done():
                await execution
            await asyncio.sleep(0.02)
        assert (output / "count").read_text().splitlines() == ["start"]
        async with engine.connect() as conn:
            ref = (await conn.execute(text("SELECT process_ref FROM fleet_attempts"))).scalar_one()
        assert json.loads(await docker("inspect", ref))[0]["State"]["Running"]
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=4)
        sock.close()
        outcome = await asyncio.wait_for(execution, timeout=6)
        assert outcome["report_pending"]
        assert not json.loads(await docker("inspect", ref))[0]["State"]["Running"]
        ticks = (output / "ticks").read_text()
        await asyncio.sleep(0.2)
        assert (output / "ticks").read_text() == ticks
        assert all(p.stat().st_mode & 0o077 == 0 for p in state.glob("*.json"))
        await fleet.attempts.expire_pending()
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT state FROM fleet_jobs"))).scalar_one() == "unknown"
            assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "quarantined"
        await client.close()
        server, server_task, sock, url = await serve(app)
        client = NodeClient(gateway_url=url, credential=credential.token, timeout_seconds=0.5)
        restarted = module.NodeDaemon(client=client, containers=DockerContainers(state_dir=state), state_dir=state, prepare_workspace=workspace, renew_seconds=0.15, safety_margin_seconds=0.1, poll_seconds=0.03)
        with pytest.raises(module.RecoveryRequired):
            await restarted.bootstrap()
        with pytest.raises(module.RecoveryRequired):
            await restarted.execute_one()
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
        assert (output / "count").read_text().splitlines() == ["start"]
    finally:
        if execution is not None and not execution.done():
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
        await client.close()
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=4)
        sock.close()
        if ref:
            await docker("rm", "-f", ref)
        await fleet.stop()
