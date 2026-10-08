"""Two production worker processes against real HTTP/Postgres/Docker/NAS."""

import asyncio
import importlib
import json
import os
import signal
import sys
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.mcp_tasks.service import McpTaskService
from deerflow.mcp.tasks import McpTaskDriverRegistry, TaskSubmitRequest
from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import setup_tracking
from .test_b06_containers import docker
from .test_b06_fleet_durable_jobs import serve
from .test_b11_worker_entry import PACKAGE

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Fleet worker/operator require a POSIX node host")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["complete", "signal_stop"])
async def test_b11_contract(fleet_database, tmp_path, mode):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit isolated local Docker gate")
    engine, sf, _ = fleet_database
    nas = tmp_path / "nas"
    nas.mkdir()
    cfg = settings(nas)
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    cfg = cfg.model_copy(update={"profiles": {"batch": cfg.profiles["batch"].model_copy(update={"image": image, "execution_timeout_seconds": 15})}})
    fleet = service_class()(cfg)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    registry = McpTaskDriverRegistry()
    registry.register("fleet", fleet.bind_tracking(await setup_tracking(engine, sf)))
    tasks = McpTaskService(repository=McpTaskRepository(sf), drivers=registry, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    server = server_task = sock = None
    processes = []
    credentials = []
    try:
        for node in ("b11-drained", "b11-active"):
            await fleet.nodes.register(node_id=node, name=node, cpu_millis=1000, memory_mib=512)
            credentials.append(await fleet.credentials.issue(node, lifetime_seconds=600))
        await fleet.nodes.set_admin_state("b11-drained", "draining")
        server, server_task, sock, url = await serve(app)
        async with httpx.AsyncClient(base_url=url) as http:
            reply = await http.post("/api/fleet/node/session", headers={"Authorization": "Bearer " + credentials[0].token}, json={"protocol_version": 0})
            assert reply.status_code == 422
            reply = await http.post("/api/fleet/node/claims", headers={"Authorization": "Bearer " + credentials[0].token}, json={"node_session_id": "old-protocol"})
            assert reply.status_code == 409
        for credential in credentials:
            secret = tmp_path / (credential.node_id + ".credential")
            secret.write_text(credential.token + "\n")
            secret.chmod(0o600)
            config = tmp_path / (credential.node_id + ".json")
            config.write_text(
                json.dumps(
                    {
                        "gateway_url": url,
                        "credential_file": str(secret),
                        "state_dir": str(tmp_path / (credential.node_id + "-state")),
                        "nas_root": str(nas),
                        "nas_identity": "fleet-test",
                        "renew_seconds": 0.2,
                        "poll_seconds": 0.03,
                        "safety_margin_seconds": 0.1,
                    }
                )
            )
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "deerflow_ecs_fleet.worker", "--settings", str(config), env=os.environ | {"PYTHONPATH": str(PACKAGE)}, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            processes.append(proc)
        for _ in range(150):
            status = [await fleet.nodes.status(c.node_id) for c in credentials]
            if all(s["node"]["health"] == "online" for s in status):
                break
            assert all(p.returncode is None for p in processes)
            await asyncio.sleep(0.03)
        assert all(s["node"]["health"] == "online" for s in status)
        request = TaskSubmitRequest(
            user_id="u",
            thread_id="t",
            run_id="r",
            tool_call_id="b11-call",
            server_name="fleet",
            task_name="b11",
            driver_data={"invocation_id": "b11-invocation"},
            arguments={"task_name": "b11", "profile": "batch", "argv": ["sh", "-c", "echo start >> /output/count; sleep " + ("10" if mode == "signal_stop" else "1") + "; echo ready > /output/result"], "execution_timeout_seconds": 15},
        )
        tracked = await tasks.submit(driver_name="fleet", request=request)
        job_id = tracked["remote_task_id"]
        await fleet.jobs.reconcile(job_id)
        for _ in range(200):
            async with engine.connect() as conn:
                attempts = (await conn.execute(text("SELECT node_id,state,process_ref FROM fleet_attempts"))).mappings().all()
            if attempts and attempts[0]["state"] == "running":
                break
            await asyncio.sleep(0.03)
        assert attempts and attempts[0]["node_id"] == "b11-active"
        if mode == "signal_stop":
            ref = attempts[0]["process_ref"]
            for _ in range(100):
                observation = json.loads(await docker("inspect", ref))[0]
                if observation["State"]["Running"]:
                    break
                await asyncio.sleep(0.03)
            assert observation["State"]["Running"]
            processes[1].send_signal(signal.SIGTERM)
            await asyncio.wait_for(processes[1].wait(), 10)
            assert processes[1].returncode == 0
            assert not json.loads(await docker("inspect", ref))[0]["State"]["Running"]
            retained = await fleet.nodes.status("b11-active")
            assert retained["attempts"][0]["state"] == "unknown"
            assert retained["attempts"][0]["stopped_at"] is not None
            assert retained["unreleased_reservations"] == []
            async with engine.connect() as conn:
                assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations WHERE state='released' AND released_at IS NOT NULL"))).scalar_one() == 1
            records = list((tmp_path / "b11-active-state").glob("*.json"))
            assert len(records) == 1 and json.loads(records[0].read_text())["reported"]
            await fleet.nodes.set_admin_state("b11-active", "disabled")
            assert (await fleet.nodes.status("b11-active"))["attempts"][0]["state"] == "unknown"
            return
        # Stop admission while accepted work remains charged and queryable.
        closed = cfg.model_copy(update={"jobs_enabled": False})
        fleet.config = fleet.jobs.config = fleet.scheduler.config = closed
        with pytest.raises(ValueError, match="disabled"):
            await tasks.submit(driver_name="fleet", request=replace(request, driver_data={"invocation_id": "disabled-invocation"}, tool_call_id="disabled-call"))
        charged = await fleet.nodes.status("b11-active")
        assert len(charged["unreleased_reservations"]) == 1
        with pytest.raises(ValueError, match="unreleased"):
            await fleet.nodes.set_admin_state("b11-active", "disabled")
        for _ in range(200):
            job = await fleet.jobs.get(job_id, user_id="u", thread_id="t")
            if job["state"] == "succeeded":
                break
            await asyncio.sleep(0.03)
        assert job["state"] == "succeeded", job
        manifest = await fleet.manifests.get(job["accepted_manifest_id"], user_id="u", thread_id="t")
        assert (nas / manifest["output_prefix"] / "count").read_text().splitlines() == ["start"]
        assert (nas / manifest["output_prefix"] / "result").read_text().strip() == "ready"
        await fleet.nodes.set_admin_state("b11-active", "draining")
        processes[1].send_signal(signal.SIGTERM)
        await asyncio.wait_for(processes[1].wait(), 10)
        assert processes[1].returncode == 0
        await fleet.nodes.set_admin_state("b11-active", "disabled")
        retained = await fleet.nodes.status("b11-active")
        assert len(retained["attempts"]) == 1
        assert retained["unreleased_reservations"] == []
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts WHERE node_id='b11-drained'"))).scalar_one() == 0
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 1
        observation = json.loads(await docker("inspect", retained["attempts"][0]["process_ref"]))[0]
        assert not observation["State"]["Running"]
        assert observation["HostConfig"]["PortBindings"] in ({}, None)
        assert all(m["Destination"] not in {"/var/run/docker.sock", str(tmp_path)} for m in observation["Mounts"])
    finally:
        try:
            for proc in processes:
                if proc.returncode is None:
                    proc.send_signal(signal.SIGTERM)
            for proc in processes:
                try:
                    out, err = await asyncio.wait_for(proc.communicate(), 10)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
                    raise
                for credential in credentials:
                    assert credential.token.encode() not in out + err
        finally:
            try:
                await tasks.stop()
            finally:
                try:
                    if server:
                        server.should_exit = True
                        try:
                            await asyncio.wait_for(server_task, 5)
                        finally:
                            sock.close()
                finally:
                    try:
                        async with engine.connect() as conn:
                            refs = (await conn.execute(text("SELECT process_ref FROM fleet_attempts WHERE process_ref IS NOT NULL"))).scalars().all()
                        for ref in refs:
                            await docker("rm", "-f", ref)
                    finally:
                        await fleet.stop()
