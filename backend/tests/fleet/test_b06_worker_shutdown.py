"""Real Docker residuals and a live worker loop establish shutdown stop barriers."""

import asyncio
import importlib
import json
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware

from .test_b02_fleet_foundation import service_class, settings
from .test_b04_fleet_foundation import seed
from .test_b06_containers import docker
from .test_b06_fleet_durable_jobs import serve


def require_docker():
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit local Docker gate")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing_journal", "engine_stop_failure"])
async def test_bootstrap_stops_all_orphans_and_preserves_other_nodes(tmp_path, fault):
    require_docker()
    from deerflow_ecs_fleet.worker.containers import DockerContainers
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon, RecoveryRequired

    node_id = "test-" + uuid4().hex
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    owned = [str(uuid4()) for _ in range(3)]
    foreign = str(uuid4())
    created = []

    class Client:
        async def open_session(self):
            return None

        async def heartbeat(self):
            raise AssertionError("Cannot advertise healthy capacity with missing journals")

        async def claim(self):
            raise AssertionError("Cannot claim until all residuals are reconciled")

    client = Client()
    client.node_id = node_id
    try:
        for attempt in [*owned, foreign]:
            ref = "fleet-" + attempt
            await docker("run", "-d", "--name", ref, "--network=none", "--label", "deerflow.fleet.attempt=" + attempt, "--label", "deerflow.fleet.node=" + (node_id if attempt in owned else "another-" + node_id), image, "sleep", "30")
            created.append(ref)
        containers = DockerContainers(state_dir=tmp_path / "state")
        if fault == "engine_stop_failure":
            stop_real = containers.stop

            async def stop_with_failure(ref):
                if ref == "fleet-" + owned[0]:
                    raise RuntimeError("Test engine stop RPC failure")
                return await stop_real(ref)

            containers.stop = stop_with_failure
        daemon = NodeDaemon(client=client, containers=containers, state_dir=tmp_path / "state", prepare_workspace=None)
        with pytest.raises(RecoveryRequired, match="journal|stopped"):
            await daemon.bootstrap()
        for attempt in owned:
            assert json.loads(await docker("inspect", "fleet-" + attempt))[0]["State"]["Running"] == (fault == "engine_stop_failure" and attempt == owned[0])
        assert json.loads(await docker("inspect", "fleet-" + foreign))[0]["State"]["Running"]
        with pytest.raises(RecoveryRequired):
            await daemon.execute_one()
    finally:
        for ref in created:
            await docker("rm", "-f", ref)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_daemon_shutdown_proves_all_parallel_executions_stopped(fleet_database, tmp_path):
    require_docker()
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.containers import DockerContainers
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_ecs_fleet.worker.journal import AttemptJournal

    engine, sf, _ = fleet_database
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    config = settings(tmp_path)
    config = config.model_copy(update={"profiles": {"batch": config.profiles["batch"].model_copy(update={"image": image, "execution_timeout_seconds": 30})}})
    fleet = service_class()(config)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    await seed(engine, count=3)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_nodes SET cpu_millis=2000,memory_mib=1024"))
        await conn.execute(
            text("UPDATE fleet_jobs SET spec=:spec"),
            {"spec": json.dumps({"task_name": "batch", "profile": "batch", "argv": ["sh", "-c", "echo start >> /output/count; while true; do echo tick >> /output/ticks; sleep .1; done"], "execution_timeout_seconds": 30})},
        )
    credential = await fleet.credentials.issue("n", lifetime_seconds=600)
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    server, server_task, sock, url = await serve(app)
    client = NodeClient(gateway_url=url, credential=credential.token, timeout_seconds=0.5)
    state = tmp_path / "private-state"

    async def workspace(claim, grant):
        output = tmp_path / "outputs" / claim["attempt_id"]
        output.mkdir(mode=0o777, parents=True)
        output.chmod(0o777)
        return output

    daemon = NodeDaemon(client=client, containers=DockerContainers(state_dir=state), state_dir=state, prepare_workspace=workspace, renew_seconds=0.2, safety_margin_seconds=0.1, poll_seconds=0.03)
    stop = asyncio.Event()
    run = asyncio.create_task(daemon.run(stop=stop, max_parallel=2))
    refs = []
    try:
        for _ in range(150):
            if run.done():
                await run
            ticks = list((tmp_path / "outputs").glob("*/ticks"))
            if len(ticks) == 2:
                break
            await asyncio.sleep(0.03)
        assert len(list((tmp_path / "outputs").glob("*/ticks"))) == 2
        async with engine.connect() as conn:
            refs = (await conn.execute(text("SELECT process_ref FROM fleet_attempts"))).scalars().all()
        assert len(refs) == 2
        assert all([json.loads(await docker("inspect", ref))[0]["State"]["Running"] for ref in refs])
        stop.set()
        await asyncio.wait_for(run, timeout=8)
        for ref in refs:
            assert not json.loads(await docker("inspect", ref))[0]["State"]["Running"]
        counts = list((tmp_path / "outputs").glob("*/count"))
        assert len(counts) == 2 and all(p.read_text().splitlines() == ["start"] for p in counts)
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts WHERE stopped_at IS NOT NULL"))).scalar_one() == 2
            assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations WHERE state='released'"))).scalar_one() == 2
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs WHERE state='unknown'"))).scalar_one() == 2
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs WHERE state='queued'"))).scalar_one() == 1
        records = AttemptJournal(state).records()
        assert len(records) == 2 and all(row["reported"] for row in records)
    finally:
        if not run.done():
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)
        async with engine.connect() as conn:
            refs = (await conn.execute(text("SELECT process_ref FROM fleet_attempts WHERE process_ref IS NOT NULL"))).scalars().all()
        for ref in refs:
            await docker("rm", "-f", ref)
        await client.close()
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=4)
        sock.close()
        await fleet.stop()
