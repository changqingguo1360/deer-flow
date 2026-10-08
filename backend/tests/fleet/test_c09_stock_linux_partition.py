"""ONE installed partition main; original SQL/graph/Node/STOP, actual TCP loss."""

import asyncio
import json
import socket
import time
from contextlib import AsyncExitStack
from datetime import datetime
from urllib.parse import urlsplit

import httpx
import pytest
from sqlalchemy import text

from .test_c09_stock_linux_operations import control_database, owned_setup, state


class OwnedPartitionEndpoint:
    """A real TCP forwarder; disconnect closes original socket/connection owners."""

    def __init__(self, upstream):
        self.upstream = urlsplit(upstream)
        self.port = 0
        self.server = None
        self.tasks = set()
        self.writers = set()
        self._closers = {}
        self.rows = []

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    async def connect(self):
        assert self.server is None and not self.tasks and not self.writers and not self._closers
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", self.port))
            sock.listen()
            sock.setblocking(False)
            self.port = sock.getsockname()[1]
            self.server = await asyncio.start_server(self.accept, sock=sock)
        except BaseException:
            sock.close()
            raise
        self.rows.append(
            {
                "action": "connected",
                "port": self.port,
                "host_monotonic": time.monotonic(),
            }
        )

    def retain_writer(self, writer):
        self.writers.add(writer)
        self._closers.setdefault(writer, None)  # Registry ownership now, not early close.

    def start_close(self, writer):
        writer.close()
        task = self._closers.get(writer)
        if task is None:

            async def actual_close():
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass

            task = asyncio.create_task(actual_close(), name="owned-tcp-writer-close")
            self._closers[writer] = task
        return task

    async def close_writer(self, writer):
        task = self.start_close(writer)
        # Both forward-finally and disconnect join the SAME close owner. A
        # cancelled forward awaiter cannot cancel StreamWriter's shared waiter.
        await asyncio.shield(task)
        task.result()
        self.writers.discard(writer)
        self._closers.pop(writer, None)

    def accept(self, reader, writer):
        self.retain_writer(writer)
        task = asyncio.create_task(self.forward(reader, writer), name="owned-node-tcp-forward")
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def forward(self, reader, writer):
        upstream = None
        pumps = []
        try:
            remote, upstream = await asyncio.open_connection(self.upstream.hostname, self.upstream.port)
            self.retain_writer(upstream)

            async def pump(source, target):
                while data := await source.read(65536):
                    target.write(data)
                    await target.drain()

            pumps = [
                asyncio.create_task(pump(reader, upstream)),
                asyncio.create_task(pump(remote, writer)),
            ]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except (ConnectionError, OSError):
            pass  # real peer/partition transport loss; never fabricate protocol replies
        finally:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            for owner in (writer, upstream):
                if owner is not None:
                    await self.close_writer(owner)

    async def disconnect(self):
        server = self.server
        if server is not None:
            server.close()  # Stop accepts, but wait_closed also waits for clients.
        # Close accepted connections BEFORE joining the server. Register close
        # Tasks synchronously before cancellation, including unstarted forward.
        for owner in tuple(self.writers):
            self.start_close(owner)
        pending = tuple(self.tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), 5)
        for owner in tuple(self.writers):
            await self.close_writer(owner)
        if server is not None:
            await server.wait_closed()
            if self.server is server:
                self.server = None
        assert not self.tasks and not self.writers and not self._closers
        self.rows.append({"action": "disconnected", "port": self.port, "host_monotonic": time.monotonic()})


async def restart_original_worker(*, tmp_path, endpoint, private, credential, image, nas, evidence):
    """Actual independent stock entrypoint; retain its process until natural/forced exit."""
    import os
    import sys

    settings_path = tmp_path / "restart-settings.json"
    operator_path = tmp_path / "restart-operator.json"
    credential_path = tmp_path / "restart-credential"
    operator_path.write_text(json.dumps(private))
    credential_path.write_text(credential.token)
    settings_path.write_text(
        json.dumps(
            {
                "kind": "agent",
                "agent_image": image,
                "agent_config_file": str(operator_path),
                "gateway_url": endpoint.url,
                "credential_file": str(credential_path),
                "state_dir": str(tmp_path / "agent-state"),
                "nas_root": str(nas.nas_root),
                "nas_identity": nas.nas_identity,
                "renew_seconds": 1,
                "safety_margin_seconds": 0.25,
                "poll_seconds": 0.05,
            }
        )
    )
    for path in (operator_path, credential_path, settings_path):
        path.chmod(0o600)
    with (evidence / "stock-restart.log").open("w") as log:
        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join(str(__import__("pathlib").Path(path).resolve()) for path in sys.path if path)
        env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost,::1"
        process = await asyncio.create_subprocess_exec(sys.executable, "-m", "deerflow_ecs_fleet.worker", "--settings", str(settings_path), env=env, stdout=log, stderr=log)
        waiter = asyncio.create_task(process.wait(), name="retained-stock-restart-process")
        try:
            async with asyncio.timeout(45):
                code = await asyncio.shield(waiter)
            receipt = {
                "pid": process.pid,
                "exit_code": code,
                "natural_exit": True,
                "entrypoint": "python -m deerflow_ecs_fleet.worker --settings PRIVATE",
                "node_cli_origin": "current host source paths; not installed Runner wheel",
                "runner_image": image,
            }
            (evidence / "stock-restart-process.json").write_text(json.dumps(receipt, indent=2))
            return receipt
        finally:
            if process.returncode is None:
                process.terminate()
            while not waiter.done():
                try:
                    await asyncio.shield(waiter)
                except asyncio.CancelledError:
                    continue
            waiter.result()


@pytest.mark.live
@pytest.mark.asyncio
async def test_c09_partition_installed_actual_main(tmp_path, monkeypatch):
    mode = "full"
    import importlib
    import os
    from pathlib import Path
    from types import SimpleNamespace

    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_extension_api import ExtensionRuntimeDeps
    from fastapi import FastAPI

    import deerflow.persistence.models  # noqa: F401 - actual complete host ORM registration
    from app.fleet.execution import FleetExecutionBackend
    from app.fleet.ownership import install_fleet_ownership
    from app.fleet.runner_context import execution_configuration
    from app.gateway import services
    from app.gateway.auth_middleware import AuthMiddleware
    from app.gateway.csrf_middleware import CSRFMiddleware
    from app.gateway.routers.thread_runs import RunCreateRequest
    from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
    from deerflow.persistence.base import Base
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime import RunManager
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    from .c04_integration_fixture import node_server
    from .test_b02_fleet_foundation import service_class, settings
    from .test_c01_remote_agent_admission import c_config
    from .test_c02_remote_agent_admission import request

    image = os.environ.get("FLEET_AGENT_TEST_IMAGE")
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1" or not image:
        pytest.fail("required installed gate needs FLEET_TEST_CONTAINERS=1 and immutable FLEET_AGENT_TEST_IMAGE")
    async with control_database(tmp_path) as db, owned_setup() as setup:
        async with db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        private = {
            "sandbox": {
                "use": "deerflow.sandbox.local:LocalSandboxProvider",
                "allow_host_bash": True,
            },
            "database": {
                "backend": "postgres",
                "postgres_url": db.runner_url,
                "postgres_schema": db.schema,
                "checkpoint_channel_mode": mode,
            },
            "run_events": {"backend": "db"},
            "agent_storage": {"backend": "db"},
            "plugins": [],
            "memory": {
                "enabled": False,
                "mode": "tool",
                "manager_class": "deerflow_c04_fixture.memory:PostgresMemory",
            },
            "mcp_tasks": {"enabled": False},
            "title": {"enabled": False},
            "summarization": {"enabled": False},
            "models": [
                {
                    "name": "model-1",
                    "use": "fleet.c09_stock_linux_partition_fixture:PartitionModel",
                    "model": "parent",
                    "api_key": "c04-scripted-provider-credential",
                }
            ],
            "tools": [
                {
                    "name": "bash",
                    "group": "bash",
                    "use": "deerflow.sandbox.tools:bash_tool",
                }
            ],
            "tool_groups": [{"name": "bash", "description": "Bounded original supervised shell"}],
            "extensions": {
                "skills": {
                    "c04-enabled": {"enabled": True},
                    "c04-disabled": {"enabled": False},
                },
                "mcpServers": {},
            },
        }
        # Trusted Gateway setup precedes remote readiness; remote saver never migrates.
        from deerflow.runtime.checkpointer.async_provider import make_checkpointer

        native_private = AppConfig.model_validate(
            {
                **private,
                "database": {**private["database"], "postgres_url": db.host_url},
            }
        )
        host_writer = await setup.enter_async_context(make_checkpointer(native_private))
        from deerflow.runtime.store.async_provider import make_store

        host_store = await setup.enter_async_context(make_store(native_private))
        public, _ = execution_configuration(AppConfig.model_validate(private))
        import shutil

        host_modules = tmp_path / "host-modules"
        (host_modules / "fleet").mkdir(parents=True)
        (host_modules / "fleet" / "__init__.py").touch()
        for name in (
            "c04_worker_fixture.py",
            "c08_installed_bytes.py",
            "c09_stock_linux_fixture.py",
            "c09_stock_linux_partition_fixture.py",
        ):
            shutil.copyfile(Path(__file__).with_name(name), host_modules / "fleet" / name)
        monkeypatch.syspath_prepend(str(host_modules))
        host_skills = host_modules / "skills"
        shutil.copytree(Path(__file__).parent / "fixtures/c04-runner/skills", host_skills)
        public = public.model_copy(update={"skills": public.skills.model_copy(update={"path": str(host_skills)})})
        set_app_config(public)
        setup.callback(reset_app_config)
        user = SimpleNamespace(id="user-c09", system_role="admin")
        token = set_current_user(user)
        setup.callback(reset_current_user, token)
        driver = AgentContainers(
            state_dir=tmp_path / "agent-state",
            operator_config=private,
            provider="gateway",
        )
        inspected_image = __import__("json").loads(await driver.checked("image", "inspect", image))[0]
        image = inspected_image["Id"]
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        (tmp_path / "execution-nas").mkdir()
        nas = settings(tmp_path / "execution-nas")
        config_data = c_config()
        config_data.update(
            nas_root=str(nas.nas_root),
            nas_identity=nas.nas_identity,
            lease_seconds=30,
            renew_seconds=5,
        )
        config_data["profiles"]["remote"].update(
            image=image,
            runtime_digest=actual.runtime_digest,
            user=str(os.getuid()) + ":" + str(os.getgid()),
            network="bridge",
            pids_limit=256,
            execution_timeout_seconds=240,
        )
        config = FleetConfig.model_validate(config_data)
        fleet = service_class()(config)
        setup.push_async_callback(fleet.stop)
        await fleet.start(ExtensionRuntimeDeps(session_factory=db.session_factory))
        app = FastAPI()
        app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
        app.state.run_store = RunRepository(db.session_factory)
        app.state.thread_store = ThreadMetaRepository(db.session_factory)
        install_fleet_ownership(app, db.session_factory)
        app.state.run_manager = RunManager(store=app.state.run_store)
        from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
        from deerflow.runtime.events.store.db import DbRunEventStore
        from deerflow.runtime.stream_bridge import MemoryStreamBridge

        app.state.checkpointer = host_writer
        app.state.store = host_store
        app.state.run_event_store = DbRunEventStore(db.session_factory)
        app.state.run_events_config = None
        app.state.stream_bridge = MemoryStreamBridge()
        app.add_middleware(AuthMiddleware)
        app.add_middleware(CSRFMiddleware)
        app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
        app.include_router(importlib.import_module("app.gateway.routers.artifacts").router)
        app.include_router(importlib.import_module("app.gateway.routers.thread_runs").router)
        node_id = "node-c09-" + __import__("uuid").uuid4().hex
        await fleet.nodes.register(
            node_id=node_id,
            name=node_id,
            cpu_millis=1000,
            memory_mib=2048,
            agent_limit=1,
            profile_allowlist=["remote"],
        )
        credential = await fleet.credentials.issue(node_id, lifetime_seconds=600)
        import hashlib

        from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest

        source_bytes = {"uploads": b"artifact\n", "workspace": b"c04-"}
        manifest = AgentWorkspaceManifest(
            user_id=user.id,
            thread_id="thread-c04",
            files=[
                {
                    "category": category,
                    "path": "source.txt",
                    "size": len(value),
                    "sha256": hashlib.sha256(value).hexdigest(),
                }
                for category, value in sorted(source_bytes.items())
            ],
            total_bytes=sum(map(len, source_bytes.values())),
        )
        snapshot = nas.nas_root / ".fleet-agent-inputs" / user.id / "thread-c04" / manifest.reference
        for category, value in source_bytes.items():
            (snapshot / category).mkdir(parents=True)
            (snapshot / category / "source.txt").write_bytes(value)
        (snapshot / "manifest.json").write_bytes(manifest.canonical_bytes())
        backend = FleetExecutionBackend(
            config=config,
            profile_name="remote",
            model_name="model-1",
            model_version="v1",
            skill_snapshot=actual.skill_snapshot,
            plugin_snapshot=actual.plugin_snapshot,
            workspace_manifest_ref=manifest.reference,
            secret_refs=[{"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"}],
        )
        core = app.state.run_manager
        body = RunCreateRequest(
            input={
                "messages": [
                    {
                        "role": "user",
                        "content": "Write the original supervised artifact, hold c09-shell-barrier c04-control-port=" + str(db.port),
                    }
                ]
            },
            config={
                "recursion_limit": 1000,
                "context": {"model_name": "model-1", "subagent_enabled": False},
            },
            stream_mode=["values", "messages-tuple", "custom"],
            stream_subgraphs=True,
        )
        initial_request = request(core, user)
        initial_request.app = app
        record = await services.start_run(body, "thread-c04", initial_request, execution_backend=backend)
        await app.state.thread_store.create(record.thread_id, user_id=user.id)
        from deerflow_ecs_fleet.worker.workspace_publication import (
            AgentWorkspacePublication,
        )
        from deerflow_ecs_fleet.workspace import NASWorkspace
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from app.gateway.internal_auth import create_internal_auth_headers

        from .c08_installed_cleanup import settle_owned_containers
        from .c08_stock_diagnostics import NodeProtocolObservation, capture_precleanup

        # Fresh Gateway has its own engine/session factory, service/repository and
        # RunManager cache. Only the original durable database/binding is shared.
        fresh_engine = create_async_engine(db.host_url)
        setup.push_async_callback(fresh_engine.dispose)
        fresh_sf = async_sessionmaker(fresh_engine, expire_on_commit=False)
        fresh_fleet = service_class()(config)
        setup.push_async_callback(fresh_fleet.stop)
        await fresh_fleet.start(ExtensionRuntimeDeps(session_factory=fresh_sf))
        fresh_app = FastAPI()
        fresh_app.state.extensions = SimpleNamespace(services=(("fleet", fresh_fleet),))
        fresh_app.state.run_store = RunRepository(fresh_sf)
        fresh_app.state.thread_store = ThreadMetaRepository(fresh_sf)
        install_fleet_ownership(fresh_app, fresh_sf)
        fresh_app.state.run_manager = RunManager(store=fresh_app.state.run_store)
        monkeypatch.setattr("app.gateway.deps._cached_repo", SQLiteUserRepository(fresh_sf))
        monkeypatch.setattr("app.gateway.deps._cached_local_provider", None)
        fresh_app.state.checkpointer = await setup.enter_async_context(make_checkpointer(native_private))
        fresh_app.state.store = await setup.enter_async_context(make_store(native_private))
        fresh_app.state.run_event_store = DbRunEventStore(fresh_sf)
        fresh_app.state.run_events_config = None
        fresh_app.state.stream_bridge = MemoryStreamBridge()
        fresh_app.add_middleware(AuthMiddleware)
        fresh_app.add_middleware(CSRFMiddleware)
        fresh_app.include_router(importlib.import_module("app.gateway.routers.thread_runs").router)
        assert fresh_sf is not db.session_factory and fresh_app.state.run_manager is not core

        evidence = Path(os.environ["C09_LINUX_EVIDENCE_DIR"])
        refs, stops, samples, admissions = [], [], [], []
        output_dirs = {}
        execution = client = node_http = public_http = stager = daemon = endpoint = None
        servers = AsyncExitStack()
        original_stop = driver.stop
        business_complete = False
        handled_execution_error = None
        execution_error_receipt = None
        result = None
        restart_receipt = None
        restart_mode = os.environ.get("C09_PARTITION_RESTART") == "1"

        async def observed_stop(ref):
            result = await original_stop(ref)
            observed = await driver.inspect(ref)
            stops.append(
                {
                    "ref": ref,
                    "physical_result": result,
                    "host_monotonic": time.monotonic(),
                    "state": observed["State"] if observed else None,
                }
            )
            return result

        driver.stop = observed_stop

        async def logs():
            rows = []
            for ref in refs:
                _, stdout, stderr = await driver.command("logs", ref, timeout=5)
                output = stdout + stderr
                for secret in (db.host_url, db.runner_url, db.password):
                    if secret:
                        output = output.replace(secret, "[private database credential redacted]")
                (evidence / (ref + ".log")).write_text(output)
                for line in stdout.splitlines():
                    try:
                        value = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(value, dict) and "event" in value:
                        rows.append(value)
            return rows

        def writer_receipts():
            rows = []
            for attempt, directory in output_dirs.items():
                for path in Path(directory).rglob("c09-partition-writers.jsonl"):
                    # Snapshot only fsynced complete lines; the next observation
                    # will receive any concurrent append's remaining bytes.
                    for line in path.read_bytes().splitlines(keepends=True):
                        if line.endswith(b"\n"):
                            rows.append({"attempt_id": attempt, **json.loads(line)})
            return rows

        async def sample(phase):
            discovered = await driver.list_managed(node_id)
            for ref, _ in discovered:
                if ref not in refs:
                    refs.append(ref)
            receipts = await asyncio.to_thread(writer_receipts)
            containers = []
            for ref in refs:
                observed = await driver.inspect(ref)
                assert observed is not None
                row = {
                    "ref": ref,
                    "id": observed["Id"],
                    "image": observed["Image"],
                    "state": observed["State"],
                }
                if observed["State"]["Running"]:
                    probe = (
                        "import json;from pathlib import Path;out=[]\nfor p in Path('/proc').glob('[0-9]*/stat'):\n try:\n"
                        "  raw=p.read_text();f=raw[raw.rfind(')')+2:].split();out.append({'pid':int(p.parent.name),'ppid':int(f[1]),'start_ticks':int(f[19]),'state':f[0]})\n"
                        " except (FileNotFoundError,ProcessLookupError):pass\nprint(json.dumps(out))"
                    )
                    code, stdout, _ = await driver.command("exec", ref, "python", "-c", probe, timeout=3)
                    if code == 0:
                        row["processes"] = json.loads(stdout)
                    else:
                        # A racing physical stop is recorded by fresh inspect,
                        # never represented as an invented empty live census.
                        latest = await driver.inspect(ref)
                        assert latest is not None and not latest["State"]["Running"]
                        row["state"] = latest["State"]
                containers.append(row)
            value = {
                "phase": phase,
                "host_monotonic": time.monotonic(),
                "receipts": receipts,
                "containers": containers,
                "sql": await state(fresh_sf, record.run_id),
            }
            samples.append(value)
            (evidence / "partition-observations.json").write_text(json.dumps(samples, indent=2, default=str))
            return value

        async def durable_identity():
            async with fresh_sf() as session:
                row = await session.execute(
                    text(
                        "SELECT t.id,t.current_run_id,t.generation,p.active_attempt_id,a.node_session_id,n.session_id AS current_node_session,res.state AS reservation,res.released_at, "
                        "b.backend,p.profile,b.source_workspace FROM fleet_agent_tasks t JOIN fleet_run_placements p ON p.agent_task_id=t.id JOIN fleet_attempts a ON a.id=p.active_attempt_id "
                        "JOIN fleet_nodes n ON n.id=a.node_id JOIN fleet_reservations res ON res.attempt_id=a.id JOIN thread_execution_bindings b ON b.user_id=t.user_id AND b.thread_id=t.thread_id WHERE p.run_id=:run"
                    ),
                    {"run": record.run_id},
                )
                return dict(row.mappings().one())

        async def rejected_admission(phase):
            response = await public_http.post(
                f"/api/threads/{record.thread_id}/runs",
                json={
                    "input": {
                        "messages": [
                            {
                                "role": "user",
                                "content": "A second physical writer must not start",
                            }
                        ]
                    },
                    "config": {"context": {"model_name": "model-1", "subagent_enabled": False}},
                    "stream_mode": ["values"],
                },
            )
            assert response.status_code == 409, response.text
            async with fresh_sf() as session:
                counts = dict(
                    (
                        await session.execute(
                            text(
                                "SELECT (SELECT count(*) FROM runs WHERE thread_id=:thread) AS runs,(SELECT count(*) FROM fleet_run_placements WHERE thread_id=:thread) AS placements,"
                                "(SELECT count(*) FROM fleet_attempts WHERE node_id=:node) AS attempts"
                            ),
                            {"thread": record.thread_id, "node": node_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            assert counts == {"runs": 1, "placements": 1, "attempts": 1}
            observed = await sample(phase)
            assert len(observed["containers"]) == 1
            admissions.append(
                {
                    "phase": phase,
                    "status_code": response.status_code,
                    "host_monotonic": time.monotonic(),
                    "durable_counts": counts,
                    "identity": await durable_identity(),
                }
            )
            return observed

        try:
            upstream_url = await servers.enter_async_context(node_server(app))
            fresh_url = await servers.enter_async_context(node_server(fresh_app))
            endpoint = OwnedPartitionEndpoint(upstream_url)
            # Register owner before creation, so a failed connect also settles.
            servers.push_async_callback(endpoint.disconnect)
            await endpoint.connect()
            node_http = httpx.AsyncClient(base_url=endpoint.url + "/", timeout=2)
            client = NodeClient(
                gateway_url=endpoint.url,
                credential=credential.token,
                claim_kind="agent",
                compatibility=actual.model_dump(mode="json"),
                http_client=node_http,
            )
            protocol = NodeProtocolObservation(client, evidence)
            public_http = httpx.AsyncClient(
                base_url=fresh_url,
                headers={
                    **create_internal_auth_headers(owner_user_id=user.id),
                    "X-CSRF-Token": "c09-partition-csrf",
                },
                cookies={"csrf_token": "c09-partition-csrf"},
                timeout=15,
            )

            async def prepare(claim, grant):
                output = await driver.prepare_workspace(nas.nas_root, claim, grant)
                output_dirs[claim["attempt_id"]] = output
                return output

            daemon = NodeDaemon(
                client=client,
                containers=driver,
                state_dir=tmp_path / "agent-state",
                prepare_workspace=prepare,
                renew_seconds=1,
                safety_margin_seconds=0.25,
                poll_seconds=0.05,
            )
            stager = AgentWorkspacePublication(
                client=client,
                containers=driver,
                nas=NASWorkspace(nas.nas_root, identity=nas.nas_identity),
                journal=daemon.journal,
            )
            daemon.workspace_publications = stager
            await daemon.bootstrap()
            original_session = client.session_id
            execution = asyncio.create_task(daemon.execute_one(), name="original-partition-node-execution")
            async with asyncio.timeout(45):
                while len(await asyncio.to_thread(writer_receipts)) < 2:
                    if execution.done():
                        await execution
                        raise AssertionError("Original installed shell exited before append receipts")
                    await asyncio.sleep(0.05)
            live = await sample("before-partition")
            before = await durable_identity()
            assert live["sql"]["cancel_action"] is None and live["sql"]["stopped_at"] is None
            assert before["node_session_id"] == before["current_node_session"] == original_session
            assert live["sql"]["reservation"] in {"reserved", "active"}
            baseline_sequence = max(row["sequence"] for row in live["receipts"])
            await endpoint.disconnect()  # Actual TCP loss, original NodeClient unchanged.
            partition_at = endpoint.rows[-1]["host_monotonic"]
            # The admission RPC runs while the SAME original bash keeps appending.
            concurrent_admission = asyncio.create_task(rejected_admission("partition-live-admission"))
            async with asyncio.timeout(5):
                while max(row["sequence"] for row in await asyncio.to_thread(writer_receipts)) < baseline_sequence + 3:
                    await asyncio.sleep(0.05)
                await concurrent_admission
            continuing = await sample("shell-continues-after-partition")
            assert continuing["host_monotonic"] > partition_at
            assert continuing["containers"][0]["state"]["Running"]
            assert max(row["sequence"] for row in continuing["receipts"]) > baseline_sequence
            original_writer = continuing["receipts"][-1]
            assert len(original_writer["process_chain"]) >= 3
            assert any(row["pid"] == original_writer["pid"] and row["start_ticks"] == original_writer["process_chain"][0]["start_ticks"] for row in continuing["containers"][0]["processes"])
            assert continuing["sql"]["stopped_at"] is None and continuing["sql"]["reservation"] in {"reserved", "active", "quarantined"}
            cancel_path = f"/api/threads/{record.thread_id}/runs/{record.run_id}/cancel"
            response = await public_http.post(cancel_path + "?wait=false&action=interrupt")
            assert response.status_code == 202, response.text
            intent = await state(fresh_sf, record.run_id)
            assert intent["cancel_action"] == "interrupt" and intent["cancel_requested_at"] is not None
            assert intent["generation"] == live["sql"]["generation"] and intent["active_attempt_id"] == live["sql"]["active_attempt_id"]
            assert intent["stopped_at"] is None and intent["reservation"] in {
                "reserved",
                "active",
                "quarantined",
            }
            async with asyncio.timeout(145):
                try:
                    result = await asyncio.shield(execution)
                except TimeoutError as error:
                    if str(error) != "Original publication deadline elapsed" or not execution.done() or execution.exception() is not error:
                        raise
                    handled_execution_error = error
                    execution_error_receipt = {"type": type(error).__name__, "message": str(error), "origin": "original AgentWorkspacePublication.step local safety deadline"}
                    (evidence / "execution-error.json").write_text(json.dumps(execution_error_receipt, indent=2))
            # Settle retained writers without closing original journal save admission
            # until the same-session STOP receipt is durably saved.
            await asyncio.gather(*stager.pending_copies, *stager.pending_saves)
            stopped_pending = await rejected_admission("physical-stop-before-ACK")
            assert not daemon._ready and not daemon._active
            if handled_execution_error is None:
                assert result["report_pending"]
            assert stops and all(row["physical_result"] for row in stops)
            assert not stopped_pending["containers"][0]["state"]["Running"]
            assert stopped_pending["sql"]["stopped_at"] is None and stopped_pending["sql"]["reservation"] in {"reserved", "active", "quarantined"}
            assert stopped_pending["sql"]["accepted_workspace_point_id"] is None and stopped_pending["sql"]["final_workspace_point_id"] is None
            pending_journal = await asyncio.to_thread(daemon.journal.records)
            assert len(pending_journal) == 1 and not pending_journal[0]["reported"]
            assert pending_journal[0]["stop_reason"] == "lease_lost" and isinstance(pending_journal[0]["exit_code"], int)
            journal = pending_journal[0]
            pending_journal_receipt = {key: journal.get(key) for key in ("reported", "stop_reason", "exit_code", "server_state")}
            assert journal["grant"]["node_session_id"] == original_session and journal["claim"]["attempt_id"] == before["active_attempt_id"]
            unchanged = await durable_identity()
            assert (
                unchanged["id"],
                unchanged["current_run_id"],
                unchanged["generation"],
                unchanged["active_attempt_id"],
                unchanged["node_session_id"],
                unchanged["current_node_session"],
                unchanged["backend"],
                unchanged["profile"],
                unchanged["source_workspace"],
            ) == (
                before["id"],
                before["current_run_id"],
                before["generation"],
                before["active_attempt_id"],
                original_session,
                original_session,
                before["backend"],
                before["profile"],
                before["source_workspace"],
            )
            assert any(row.get("error_type") in {"ConnectError", "ReadError", "RemoteProtocolError", "ReadTimeout", "ConnectTimeout"} and row["operation"].endswith("stopped") for row in protocol.rows)

            await endpoint.connect()
            if restart_mode:
                await stager.join_writers()  # Original owners settle before stock child acquires journal lock.
                restart_receipt = await restart_original_worker(tmp_path=tmp_path, endpoint=endpoint, private=private, credential=credential, image=image, nas=nas, evidence=evidence)
                after_restart = await durable_identity()
                journal = (await asyncio.to_thread(daemon.journal.records))[0]
                observed_restart = await sample("stock-new-session-restart")
                restart_state = {"identity": after_restart, "journal": {key: journal.get(key) for key in ("reported", "stop_reason", "exit_code", "server_state")}, "sql": observed_restart["sql"]}
                (evidence / "stock-restart-state.json").write_text(json.dumps(restart_state, indent=2, default=str))
                assert restart_receipt["exit_code"] == 1  # Missing exact source retains non-ready recovery; stock entry exits.
                assert after_restart["current_node_session"] != original_session
                assert after_restart["node_session_id"] == original_session
                assert journal["reported"] is True  # RED until dedicated reconciliation exists.
                replay = {"state": journal["server_state"], "stopped": observed_restart["sql"]["stopped_at"] is not None}
            else:
                assert client.session_id == original_session
                ref = "fleet-" + journal["claim"]["attempt_id"]
                assert await driver.stop(ref) is True  # Fresh original physical proof before exact journal replay.
                physical = await driver.inspect(ref)
                assert physical is not None and not physical["State"]["Running"]
                replay = await client.attempt(
                    journal["claim"],
                    "stopped",
                    reason=journal["stop_reason"],
                    exit_code=journal["exit_code"],
                    process_ref=ref,
                    physical_stopped=True,
                )
                journal["reported"], journal["server_state"] = True, replay["state"]
                await daemon._save_record(journal)  # Only original HTTP success marks actual journal reported.
            final_observed = await rejected_admission("replayed-STOP-missing-exact-source")
            final = final_observed["sql"]
            assert replay == {"state": "recovery_required", "stopped": True}
            assert final["stopped_at"] is not None and final["reservation"] == "released"
            assert final["task_state"] == final["placement_state"] == "recovery_required"
            assert final["accepted_workspace_point_id"] is None and final["final_workspace_point_id"] is None
            async with fresh_sf() as session:
                assert (
                    await session.scalar(
                        text("SELECT state FROM fleet_attempts WHERE id=:attempt"),
                        {"attempt": before["active_attempt_id"]},
                    )
                    == "unknown"
                )
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM fleet_reservations WHERE attempt_id=:attempt AND state='released' AND released_at IS NOT NULL"),
                        {"attempt": before["active_attempt_id"]},
                    )
                    == 1
                )
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM fleet_workspace_points WHERE run_id=:run"),
                        {"run": record.run_id},
                    )
                    == 0
                )
                bytes_before = await session.scalar(
                    text("SELECT row_to_json(r)::text FROM runs r WHERE run_id=:run"),
                    {"run": record.run_id},
                )

            # Exercise original old-context SQL writer and renew/cleanup endpoints;
            # rejection must roll back without any stale persisted core change.
            import hashlib

            from deerflow_ecs_fleet.launch_spec import LaunchSpec
            from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity

            from app.fleet.mutation import FleetMutationCapability
            from deerflow.runtime.execution.mutation_context import (
                OwnershipRejected,
                remote_mutation_scope,
            )

            grant = journal["grant"]
            identity = ExecutionIdentity(
                **{
                    key: grant[key]
                    for key in (
                        "node_id",
                        "node_session_id",
                        "agent_task_id",
                        "generation",
                        "attempt_id",
                        "owner_worker_id",
                    )
                },
                token_stamp=hashlib.sha256(journal["claim"]["token"].encode()).hexdigest(),
            )
            capability = FleetMutationCapability(identity, LaunchSpec.model_validate(grant["launch_spec"]))
            stale_runs = RunRepository(fresh_sf, mutation_capability=capability)
            with (
                remote_mutation_scope(capability.context),
                pytest.raises(OwnershipRejected),
            ):
                await stale_runs.update_model_name(record.run_id, "forbidden-old-writer")
            stale_http = []
            for operation in ("renew", "workspace/poll"):
                with pytest.raises(httpx.HTTPStatusError) as rejected:
                    await client.attempt(journal["claim"], operation)
                assert rejected.value.response.status_code == 409
                stale_http.append(
                    {
                        "operation": operation,
                        "status_code": rejected.value.response.status_code,
                    }
                )
            async with fresh_sf() as session:
                assert (
                    await session.scalar(
                        text("SELECT row_to_json(r)::text FROM runs r WHERE run_id=:run"),
                        {"run": record.run_id},
                    )
                    == bytes_before
                )
            await stager.join_writers()
            assert (await asyncio.to_thread(daemon.journal.records))[0]["reported"] is True
            assert client.session_id == original_session
            expected_current_session = after_restart["current_node_session"] if restart_mode else original_session
            assert (await durable_identity())["current_node_session"] == expected_current_session

            # Count actual writer intervals, keyed by container identity and actual
            # PID/start ticks. We never assign the target count as an observation.
            intervals = []
            for container in final_observed["containers"]:
                attempt = container["ref"].removeprefix("fleet-")
                grouped = {}
                for row in final_observed["receipts"]:
                    if row["attempt_id"] == attempt:
                        grouped.setdefault((row["pid"], row["process_chain"][0]["start_ticks"]), []).append(row)
                for (pid, ticks), rows in grouped.items():
                    finished = datetime.fromisoformat(container["state"]["FinishedAt"].replace("Z", "+00:00")).timestamp()
                    assert finished > max(row["unix_ns"] / 1e9 for row in rows)
                    assert finished - min(row["unix_ns"] / 1e9 for row in rows) < rows[0]["natural_seconds"], "Original supervised shell ended naturally instead of safety STOP"
                    intervals.append(
                        {
                            "container_id": container["id"],
                            "pid": pid,
                            "start_ticks": ticks,
                            "thread_id": record.thread_id,
                            "run_id": record.run_id,
                            "first_receipt_unix": min(row["unix_ns"] / 1e9 for row in rows),
                            "last_receipt_unix": max(row["unix_ns"] / 1e9 for row in rows),
                            "physical_finished_unix": finished,
                            "receipt_count": len(rows),
                        }
                    )
            edges = [(row["first_receipt_unix"], 1) for row in intervals] + [(row["physical_finished_unix"], -1) for row in intervals]
            writers = maximum = 0
            for _, change in sorted(edges):
                writers += change
                maximum = max(maximum, writers)
            assert intervals and maximum == 1 and writers == 0
            rows = await logs()
            assert any(row["event"] == "original-installed-complete-wheel-bytes" for row in rows)
            origins = next(row for row in rows if row["event"] == "c09-partition-installed-origins")
            installed_fixture = origins["modules"]["fleet.c09_stock_linux_partition_fixture"]
            assert installed_fixture["sha256"] == hashlib.sha256(Path(__file__).with_name("c09_stock_linux_partition_fixture.py").read_bytes()).hexdigest()
            assert execution.done() and not daemon._active and not stager.pending_copies and not stager.pending_saves
            business_complete = True
            (evidence / "actual-partition-main.json").write_text(
                json.dumps(
                    {
                        "scope": "Actual stock worker process restart/new-session STOP reconciliation"
                        if restart_mode
                        else "Actual same-session transport recovery, not daemon process-restart recovery; original independent observer/Node safety wins as observed",
                        "image": image,
                        "before": before,
                        "intent": intent,
                        "final": final,
                        "node_session_preserved": client.session_id == original_session,
                        "journal_pending": pending_journal_receipt,
                        "journal_replay": replay,
                        "tcp_partition": endpoint.rows,
                        "admissions": admissions,
                        "physical_stop_observations": stops,
                        "node_protocol": protocol.rows,
                        "original_stop_result": result,
                        "execution_error": execution_error_receipt,
                        "stock_restart": restart_receipt,
                        "writer_intervals": intervals,
                        "concurrent_thread_writers": maximum,
                        "unconfirmed_resources_released": any(value["sql"]["stopped_at"] is None and value["sql"]["reservation"] == "released" for value in samples),
                        "runner_receipts": rows,
                        "stale_http": stale_http,
                        "resume_new_run_evidence": "Task3 accepted actual public keyed resume; Root assembles unchanged exact-source evidence",
                    },
                    indent=2,
                    default=str,
                )
            )
        finally:
            import sys

            primary = sys.exception()
            owned_tasks = [task for task in (execution, locals().get("concurrent_admission")) if task is not None]

            async def discover_owned():
                return [ref for ref, _ in await driver.list_managed(node_id)]

            async def cleanup():
                from .c08_installed_cleanup import cancel_owned_task

                before_cleanup = [("diagnostics", lambda: capture_precleanup(session_factory=db.session_factory, run_id=record.run_id, driver=driver, refs=refs, evidence=evidence)), ("runner-logs", logs)]
                for task in owned_tasks:
                    if task is not execution and not (task.done() and not task.cancelled() and task.exception() is primary):
                        before_cleanup.append(("concurrent-admission", lambda task=task: cancel_owned_task(task)))

                async def owner_gate():
                    # Same retained ownership gate as accepted Task3: even if
                    # the bounded helper reports failure, live original owners
                    # retain HTTP/saver/PG scope until real settlement.
                    while any(not task.done() for task in owned_tasks):
                        pending = [task for task in owned_tasks if not task.done()]
                        (evidence / "retained-owner-pending.json").write_text(json.dumps({"pending_tasks": len(pending), "scope_retained": True}))
                        await asyncio.wait(pending, timeout=30)
                    if stager is not None:
                        await stager.join_writers()
                    if endpoint is not None:
                        # A bounded proxy settlement error cannot let the helper
                        # continue closing HTTP/PG around still-live pumps.
                        proxy_errors = []
                        while True:
                            try:
                                await endpoint.disconnect()
                            except BaseException as error:
                                if not proxy_errors or (type(proxy_errors[-1]), str(proxy_errors[-1])) != (type(error), str(error)):
                                    proxy_errors.append(error)
                            if not endpoint.tasks and not endpoint.writers and not endpoint._closers and endpoint.server is None:
                                break
                            (evidence / "retained-proxy-pending.json").write_text(json.dumps({"tasks": len(endpoint.tasks), "writers": len(endpoint.writers), "scope_retained": True}))
                            if endpoint.tasks:
                                await asyncio.wait(tuple(endpoint.tasks), timeout=30)
                            else:
                                await asyncio.sleep(0.05)
                        if proxy_errors:
                            raise BaseExceptionGroup("Settled original TCP proxy cleanup errors", proxy_errors)

                after = [("retained-owner-gate", owner_gate)]
                for name, owner in (("node-client", client), ("node-http", node_http), ("public-http", public_http)):
                    if owner is not None:
                        after.append((name, owner.close if name == "node-client" else owner.aclose))
                after.append(("tcp-owners", servers.aclose))
                completed_primary = (
                    execution is not None and execution.done() and not execution.cancelled() and (execution.exception() is primary or (handled_execution_error is not None and execution.exception() is handled_execution_error))
                )
                cleanup_completed = False
                try:
                    await settle_owned_containers(refs=refs, discover=discover_owned, driver=driver, execution=None if completed_primary else execution, before=before_cleanup, after=after, original_error=primary)
                    cleanup_completed = True
                finally:
                    remaining = await driver.list_managed(node_id)
                    handles = {
                        "business_main_completed_before_cleanup": business_complete,
                        "owned_tasks_done": all(task.done() for task in owned_tasks),
                        "execution_done": execution is None or execution.done(),
                        "clients_closed": all(owner is None or owner.is_closed for owner in (node_http, public_http)),
                        "remaining_owned_containers": [ref for ref, _ in remaining],
                        "stager_pending": 0 if stager is None else len(stager.pending_copies) + len(stager.pending_saves),
                        "driver_attachments": len(driver._attachments),
                        "driver_drains": len(driver._drains),
                        "tcp_forward_tasks": 0 if endpoint is None else len(endpoint.tasks),
                        "tcp_forward_writers": 0 if endpoint is None else len(endpoint.writers),
                        "tcp_close_owners": 0 if endpoint is None else len(endpoint._closers),
                        "tcp_listener_closed": endpoint is None or endpoint.server is None,
                        "scoped_cleanup_completed": cleanup_completed,
                    }
                    (evidence / "owned-handles.json").write_text(json.dumps(handles, indent=2))

            retained = asyncio.create_task(cleanup(), name="retained-original-partition-cleanup")
            cancellations = []
            while not retained.done():
                try:
                    await asyncio.shield(retained)
                except asyncio.CancelledError as error:
                    cancellations.append(error)
                    continue
                except BaseException:
                    break
            try:
                retained.result()
            except BaseException as cleanup_error:
                if primary is not None and cleanup_error is not primary:
                    raise BaseExceptionGroup("Original installed partition main and retained cleanup", [primary, cleanup_error])
                raise
            if primary is None and cancellations:
                raise cancellations[0]
