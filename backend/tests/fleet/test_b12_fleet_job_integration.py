"""B fault acceptance observes real file and external HTTP effects through a TCP cut."""

import asyncio
import importlib
import json
import os
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware

from .fault_proxy import FaultProxy
from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import setup_tracking
from .test_b06_containers import docker
from .test_b06_fleet_durable_jobs import serve


@pytest.mark.integration
@pytest.mark.asyncio
async def test_b12_contract(fleet_database, tmp_path):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1" or not os.environ.get("FLEET_TEST_WORKER_IMAGE"):
        pytest.skip("requires explicit local Docker gate")
    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.containers import DockerContainers
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon, RecoveryRequired

    from app.mcp_tasks.service import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSubmitRequest
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    engine, sf, _ = fleet_database
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    # Validation is deliberate: do not shorten leases with model_copy for this gate.
    raw = settings(tmp_path).model_dump()
    raw.update(lease_seconds=30, renew_seconds=1)
    raw["profiles"]["batch"].update(image=image, network="bridge", execution_timeout_seconds=90)
    cfg = FleetConfig.model_validate(raw)
    fleet = service_class()(cfg)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    reader = await setup_tracking(engine, sf)
    registry = McpTaskDriverRegistry()
    registry.register("fleet", fleet.bind_tracking(reader))
    tasks = McpTaskService(repository=McpTaskRepository(sf), drivers=registry, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    node = "b12-" + uuid4().hex
    await fleet.nodes.register(node_id=node, name=node, cpu_millis=1000, memory_mib=512)
    credential = await fleet.credentials.issue(node, lifetime_seconds=600)
    mock_name = "b12-http-" + uuid4().hex
    mock_dir = tmp_path / "external-http"
    mock_dir.mkdir(mode=0o777)
    mock_dir.chmod(0o777)
    endpoint = mock_dir / "server.py"
    endpoint.write_text(
        "from http.server import BaseHTTPRequestHandler, HTTPServer\n"
        "class Handler(BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        with open('/www/count','a') as count: count.write('effect\\n')\n"
        "        self.send_response(200)\n"
        "        self.end_headers()\n"
        "        self.wfile.write(b'ok')\n"
        "HTTPServer(('0.0.0.0',8080),Handler).serve_forever()\n"
    )
    output = tmp_path / "output"
    output.mkdir(mode=0o777)
    output.chmod(0o777)
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    server, server_task, sock, upstream = await serve(app)
    parsed = urlsplit(upstream)
    proxy = FaultProxy(parsed.hostname, parsed.port)
    url = await proxy.start()
    client = NodeClient(gateway_url=url, credential=credential.token, timeout_seconds=0.3)
    state = tmp_path / "worker-state"
    containers = DockerContainers(state_dir=state)

    async def workspace(claim, grant):
        return output

    daemon = NodeDaemon(client=client, containers=containers, state_dir=state, prepare_workspace=workspace, renew_seconds=0.2, safety_margin_seconds=2, poll_seconds=0.03)
    execution = None
    ref = None
    mock_created = False
    try:
        mock_created = True
        await docker(
            "run",
            "-d",
            "--pull=never",
            "--name",
            mock_name,
            "--network",
            "bridge",
            "--user",
            "65534:65534",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--mount",
            f"type=bind,src={mock_dir},dst=/www",
            "--entrypoint",
            "python",
            os.environ["FLEET_TEST_WORKER_IMAGE"],
            "/www/server.py",
        )
        mock = json.loads(await docker("inspect", mock_name))[0]
        assert mock["HostConfig"]["PortBindings"] in (None, {})
        address = mock["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
        assert address
        await docker(
            "exec",
            mock_name,
            "python",
            "-c",
            "import socket,time; deadline=time.monotonic()+5;\n"
            "while time.monotonic()<deadline:\n"
            " try:\n"
            "  socket.create_connection(('127.0.0.1',8080),timeout=.1).close(); break\n"
            " except OSError: time.sleep(.05)\n"
            "else: raise RuntimeError('HTTP mock did not become ready')",
        )
        script = f"echo first >> /output/count; wget -q -O /dev/null http://{address}:8080/cgi-bin/effect || exit 9; while [ ! -f /output/allow-second ]; do sleep .1; done; echo second >> /output/count; wget -q -O /dev/null http://{address}:8080/cgi-bin/effect"
        request = TaskSubmitRequest(
            user_id="u",
            thread_id="t",
            run_id="r",
            tool_call_id="call",
            server_name="fleet",
            task_name="effects",
            driver_data={"invocation_id": "b12-invocation"},
            arguments={"task_name": "effects", "profile": "batch", "argv": ["sh", "-c", script], "execution_timeout_seconds": 90},
        )
        tracked = await tasks.submit(driver_name="fleet", request=request)
        await fleet.jobs.reconcile(tracked["remote_task_id"])
        await daemon.bootstrap()
        execution = asyncio.create_task(daemon.execute_one())
        for _ in range(200):
            if (mock_dir / "count").exists():
                break
            if execution.done():
                await execution
            await asyncio.sleep(0.03)
        assert (output / "count").read_text().splitlines() == ["first"]
        assert (mock_dir / "count").read_text().splitlines() == ["effect"]
        async with engine.connect() as conn:
            before = (await conn.execute(text("SELECT a.process_ref,a.start_authorized_at,j.tracking_task_id FROM fleet_attempts a JOIN fleet_jobs j ON j.id=a.job_id"))).one()
        ref = before.process_ref
        assert before.start_authorized_at is not None and before.tracking_task_id == tracked["id"]
        assert json.loads(await docker("inspect", ref))[0]["State"]["Running"]
        proxy.cut()
        # The control path is cut while the independent job process really remains alive.
        await asyncio.sleep(0.3)
        assert json.loads(await docker("inspect", ref))[0]["State"]["Running"]
        outcome = await asyncio.wait_for(execution, timeout=35)
        assert outcome["report_pending"]
        assert not json.loads(await docker("inspect", ref))[0]["State"]["Running"]
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT lease_expires_at > clock_timestamp() FROM fleet_attempts"))).scalar_one()
        # Permit the second effect only after observed physical stop; it must never happen.
        (output / "allow-second").write_text("go")
        await asyncio.sleep(0.3)
        assert (output / "count").read_text().splitlines() == ["first"]
        assert (mock_dir / "count").read_text().splitlines() == ["effect"]
        for _ in range(60):
            async with engine.connect() as conn:
                expired = (await conn.execute(text("SELECT lease_expires_at <= clock_timestamp() FROM fleet_attempts"))).scalar_one()
            if expired:
                break
            await asyncio.sleep(0.1)
        assert expired
        await fleet.attempts.expire_pending()
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT state FROM fleet_jobs"))).scalar_one() == "unknown"
            assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "quarantined"
        proxy.restore()
        restarted = NodeDaemon(client=client, containers=containers, state_dir=state, prepare_workspace=workspace, renew_seconds=0.2, safety_margin_seconds=2, poll_seconds=0.03)
        with pytest.raises(RecoveryRequired):
            await restarted.bootstrap()
        with pytest.raises(RecoveryRequired):
            await restarted.execute_one()
        replay = await tasks.submit(driver_name="fleet", request=request)
        assert replay["id"] == tracked["id"] and replay["remote_task_id"] == tracked["remote_task_id"]
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts a JOIN fleet_jobs j ON j.id=a.job_id WHERE a.start_authorized_at IS NOT NULL AND j.tracking_task_id IS NULL"))).scalar_one() == 0
            assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
            assert (await conn.execute(text("SELECT stopped_at FROM fleet_attempts"))).scalar_one() is not None
        assert (output / "count").read_text().splitlines() == ["first"]
        assert (mock_dir / "count").read_text().splitlines() == ["effect"]
    finally:
        cleanup_errors = []

        async def cleanup(operation):
            try:
                await operation()
            except BaseException as error:
                cleanup_errors.append(error)

        async def stop_execution():
            if execution is not None and not execution.done():
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)

        async def stop_server():
            server.should_exit = True
            try:
                await asyncio.wait_for(server_task, timeout=4)
            finally:
                if not server_task.done():
                    server_task.cancel()
                    await asyncio.gather(server_task, return_exceptions=True)
                sock.close()

        async def remove_job():
            owned_ref = ref
            if owned_ref is None:
                async with engine.connect() as conn:
                    owned_ref = (await conn.execute(text("SELECT process_ref FROM fleet_attempts"))).scalar_one_or_none()
            if owned_ref:
                await docker("rm", "-f", owned_ref)

        async def remove_mock():
            if mock_created:
                await docker("rm", "-f", mock_name)

        # Every owned resource is cleaned even if an earlier shutdown operation fails.
        await cleanup(stop_execution)
        await cleanup(client.close)
        await cleanup(proxy.close)
        await cleanup(stop_server)
        await cleanup(remove_job)
        await cleanup(remove_mock)
        await cleanup(fleet.stop)
        if cleanup_errors:
            raise cleanup_errors[0]
