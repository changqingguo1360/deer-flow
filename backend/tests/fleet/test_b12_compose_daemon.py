"""Two built Compose daemons through real HTTPS/Postgres/Docker/NAS."""

import asyncio
import importlib
import json
import os
import socket
import ssl
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import uvicorn
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

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Fleet worker/operator require a POSIX node host")


async def compose(project, *args):
    name, override, env = project
    root = Path(__file__).resolve().parents[3]
    proc = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "--project-name",
        name,
        "--profile",
        "fleet-worker",
        "-f",
        str(root / "docker/fleet/compose.yaml"),
        "-f",
        str(override),
        *args,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), 45)
    except BaseException:
        if proc.returncode is None:
            proc.kill()
        await proc.wait()
        raise
    assert proc.returncode == 0, err.decode()
    return out.decode().strip()


async def terminate(ref):
    await docker("kill", "--signal", "TERM", ref)
    assert await docker("wait", ref) == "0"


async def serve_https(app, directory):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "host.docker.internal")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("host.docker.internal"), x509.DNSName("localhost")]), critical=False)
        .sign(key, hashes.SHA256())
    )
    ca, private = directory / "ca.pem", directory / "key.pem"
    ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    private.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    private.chmod(0o600)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", ws="none", lifespan="off", timeout_graceful_shutdown=2, ssl_certfile=str(ca), ssl_keyfile=str(private)))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        for _ in range(100):
            if server.started:
                host = "host.docker.internal" if sys.platform == "darwin" else "localhost"
                return server, task, sock, f"https://{host}:{sock.getsockname()[1]}"
            if task.done():
                await task
            await asyncio.sleep(0.01)
        raise AssertionError("Local HTTPS Gateway did not become ready")
    except BaseException:
        server.should_exit = True
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        sock.close()
        raise


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["complete", "signal_stop"])
async def test_b12_compose_daemons(fleet_database, tmp_path, mode):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit isolated local Docker gate")
    image_id = os.environ.get("FLEET_TEST_WORKER_IMAGE")
    if not image_id:
        pytest.skip("requires an explicitly retained immutable worker image")
    engine, sf, _ = fleet_database
    nas = tmp_path / "nas"
    nas.mkdir()
    cfg = settings(nas)
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    cfg = cfg.model_copy(update={"profiles": {"batch": cfg.profiles["batch"].model_copy(update={"image": image, "execution_timeout_seconds": 60})}})
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
    projects = []
    worker_refs = []
    drained, active = str(uuid4()), str(uuid4())
    credentials = []
    try:
        for node in (drained, active):
            await fleet.nodes.register(node_id=node, name=node, cpu_millis=1000, memory_mib=512)
            credentials.append(await fleet.credentials.issue(node, lifetime_seconds=600))
        await fleet.nodes.set_admin_state(drained, "draining")
        server, server_task, sock, url = await serve_https(app, tmp_path)
        async with httpx.AsyncClient(base_url=url.replace("host.docker.internal", "localhost"), verify=ssl.create_default_context(cafile=tmp_path / "ca.pem")) as http:
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
                        "credential_file": "/run/fleet/credential",
                        "state_dir": "/var/lib/deerflow-fleet",
                        "nas_root": str(nas),
                        "nas_identity": "fleet-test",
                        "renew_seconds": 0.2,
                        "poll_seconds": 0.03,
                        "safety_margin_seconds": 0.1,
                    }
                )
            )
            state = tmp_path / (credential.node_id + "-state")
            state.mkdir(mode=0o700)
            override = tmp_path / (credential.node_id + "-compose.json")
            override.write_text(
                json.dumps(
                    {
                        "services": {
                            "worker": {
                                "user": f"{os.getuid()}:{os.getgid()}",
                                "group_add": [str(0 if sys.platform == "darwin" else Path(os.environ.get("FLEET_TEST_DOCKER_SOCKET", "/var/run/docker.sock")).stat().st_gid)],
                                "environment": {"SSL_CERT_FILE": "/run/fleet/ca.pem"},
                                "volumes": [{"type": "bind", "source": str(tmp_path / "ca.pem"), "target": "/run/fleet/ca.pem", "read_only": True, "bind": {"create_host_path": False}}],
                                **({"network_mode": "host"} if sys.platform != "darwin" else {}),
                            }
                        }
                    }
                )
            )
            env = os.environ | {
                "FLEET_WORKER_IMAGE": image_id,
                "FLEET_WORKER_SETTINGS": str(config),
                "FLEET_WORKER_CREDENTIAL": str(secret),
                "FLEET_WORKER_STATE": str(state),
                "FLEET_NAS_ROOT": str(nas),
                "FLEET_DOCKER_SOCKET": os.environ.get("FLEET_TEST_DOCKER_SOCKET", "/var/run/docker.sock"),
            }
            project = ("fleet-b12-" + uuid4().hex, override, env)
            projects.append(project)
            await compose(project, "up", "--detach", "--no-build", "--pull", "never")
            ref = await compose(project, "ps", "--quiet", "worker")
            worker_refs.append(ref)
            inspection = json.loads(await docker("inspect", ref))[0]
            assert inspection["Image"] == image_id
            assert inspection["HostConfig"]["PortBindings"] in ({}, None)
            assert not inspection["Config"].get("ExposedPorts")
            assert inspection["HostConfig"]["ReadonlyRootfs"]
            assert inspection["HostConfig"]["CapDrop"] == ["ALL"]
            assert inspection["Config"]["User"] == f"{os.getuid()}:{os.getgid()}"
            observed_private = await docker("exec", ref, "python", "-c", "import os,stat; s=os.stat('/run/fleet/credential'); print(s.st_uid,s.st_gid,oct(stat.S_IMODE(s.st_mode)))")
            assert observed_private == f"{os.getuid()} {os.getgid()} 0o600"
        for _ in range(500):
            status = [await fleet.nodes.status(c.node_id) for c in credentials]
            if all(s["node"]["health"] == "online" for s in status):
                break
            assert all([json.loads(await docker("inspect", ref))[0]["State"]["Running"] for ref in worker_refs])
            await asyncio.sleep(0.03)
        assert all(s["node"]["health"] == "online" for s in status)
        async with engine.connect() as conn:
            sessions = (await conn.execute(text("SELECT session_id FROM fleet_nodes WHERE id IN (:drained, :active)"), {"drained": drained, "active": active})).scalars().all()
        assert len(sessions) == 2 and None not in sessions and len(set(sessions)) == 2
        request = TaskSubmitRequest(
            user_id="u",
            thread_id="t",
            run_id="r",
            tool_call_id="b12-compose-call",
            server_name="fleet",
            task_name="b12-compose",
            driver_data={"invocation_id": "b12-compose-invocation"},
            arguments={
                "task_name": "b12-compose",
                "profile": "batch",
                "argv": ["sh", "-c", "echo start >> /output/count; while [ ! -f /output/release ]; do sleep .1; done; echo ready > /output/result"],
                "execution_timeout_seconds": 60,
            },
        )
        tracked = await tasks.submit(driver_name="fleet", request=request)
        job_id = tracked["remote_task_id"]
        await fleet.jobs.reconcile(job_id)
        for _ in range(200):
            async with engine.connect() as conn:
                attempts = (await conn.execute(text("SELECT node_id,state,process_ref,output_prefix FROM fleet_attempts"))).mappings().all()
            if attempts and attempts[0]["state"] == "running":
                break
            await asyncio.sleep(0.03)
        assert attempts and attempts[0]["node_id"] == active
        output = nas / attempts[0]["output_prefix"] / "outputs"
        for _ in range(100):
            if (output / "count").exists():
                break
            await asyncio.sleep(0.03)
        assert (output / "count").read_text().splitlines() == ["start"]
        if mode == "signal_stop":
            ref = attempts[0]["process_ref"]
            for _ in range(100):
                observation = json.loads(await docker("inspect", ref))[0]
                if observation["State"]["Running"]:
                    break
                await asyncio.sleep(0.03)
            assert observation["State"]["Running"]
            await terminate(worker_refs[1])
            assert not json.loads(await docker("inspect", ref))[0]["State"]["Running"]
            assert (output / "count").read_text().splitlines() == ["start"]
            assert not (output / "result").exists()
            retained = await fleet.nodes.status(active)
            assert retained["attempts"][0]["state"] == "unknown"
            assert retained["attempts"][0]["stopped_at"] is not None
            assert retained["unreleased_reservations"] == []
            async with engine.connect() as conn:
                assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations WHERE state='released' AND released_at IS NOT NULL"))).scalar_one() == 1
                assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
                assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts WHERE node_id=:node"), {"node": drained})).scalar_one() == 0
                assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 0
            await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
            tracking = await tasks.list_tasks(user_id="u", thread_id="t")
            assert len(tracking) == 1 and tracking[0]["id"] == tracked["id"] and tracking[0]["status"] == "input_required"
            records = list((tmp_path / (active + "-state")).glob("*.json"))
            assert len(records) == 1 and json.loads(records[0].read_text())["reported"]
            await fleet.nodes.set_admin_state(active, "disabled")
            assert (await fleet.nodes.status(active))["attempts"][0]["state"] == "unknown"
            return
        # Stop admission while accepted work remains charged and queryable.
        closed = cfg.model_copy(update={"jobs_enabled": False})
        fleet.config = fleet.jobs.config = fleet.scheduler.config = closed
        with pytest.raises(ValueError, match="disabled"):
            await tasks.submit(driver_name="fleet", request=replace(request, driver_data={"invocation_id": "disabled-invocation"}, tool_call_id="disabled-call"))
        charged = await fleet.nodes.status(active)
        assert len(charged["unreleased_reservations"]) == 1
        with pytest.raises(ValueError, match="unreleased"):
            await fleet.nodes.set_admin_state(active, "disabled")
        (output / "release").touch()
        for _ in range(500):
            job = await fleet.jobs.get(job_id, user_id="u", thread_id="t")
            if job["state"] == "succeeded":
                break
            await asyncio.sleep(0.03)
        assert job["state"] == "succeeded", job
        manifest = await fleet.manifests.get(job["accepted_manifest_id"], user_id="u", thread_id="t")
        assert (nas / manifest["output_prefix"] / "count").read_text().splitlines() == ["start"]
        assert (nas / manifest["output_prefix"] / "result").read_text().strip() == "ready"
        await tasks.run_once(now=datetime.now(UTC) + timedelta(seconds=2))
        records = await tasks.list_tasks(user_id="u", thread_id="t")
        assert len(records) == 1 and records[0]["id"] == tracked["id"] and records[0]["status"] == "completed"
        # Published output is a sealed copy, independent of the writable attempt.
        (output / "result").write_text("changed after publication")
        assert (nas / manifest["output_prefix"] / "result").read_text().strip() == "ready"
        assert await fleet.manifests.get(job["accepted_manifest_id"], user_id="u", thread_id="t") == manifest
        await fleet.nodes.set_admin_state(active, "draining")
        await terminate(worker_refs[1])
        await fleet.nodes.set_admin_state(active, "disabled")
        retained = await fleet.nodes.status(active)
        assert len(retained["attempts"]) == 1
        assert retained["unreleased_reservations"] == []
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts WHERE node_id=:node"), {"node": drained})).scalar_one() == 0
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM fleet_artifact_manifests"))).scalar_one() == 1
        observation = json.loads(await docker("inspect", retained["attempts"][0]["process_ref"]))[0]
        assert not observation["State"]["Running"]
        assert observation["HostConfig"]["PortBindings"] in ({}, None)
        assert [m["Destination"] for m in observation["Mounts"]] == ["/output"]
    finally:
        try:
            errors = []
            for project in reversed(projects):
                try:
                    logs = await compose(project, "logs", "--no-color")
                    for credential in credentials:
                        assert credential.token not in logs
                except Exception as error:
                    errors.append(error)
                finally:
                    try:
                        await compose(project, "down", "--timeout", "10", "--remove-orphans")
                    except Exception as error:
                        errors.append(error)
            if errors:
                raise ExceptionGroup("Compose cleanup failed", errors)
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
