"""C07 installed Linux proof, run only after the frozen image review gate."""

import asyncio
import importlib
import json
import os
import sys
from contextlib import AsyncExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text

from .c04_integration_fixture import control_database, node_server
from .c07_integration_fixture import C07Scenario, RedisFaultProxy, bounded_wait, owned_redis
from .test_b02_fleet_foundation import service_class, settings
from .test_c01_remote_agent_admission import c_config
from .test_c02_remote_agent_admission import request as trusted_request


class InstalledScenario(C07Scenario):
    async def start_once(self):
        assert not hasattr(self, "execution_task")
        self.execution_task = asyncio.create_task(self.daemon.execute_one())

    async def wait_for_committed_model_event(self):
        async def committed():
            if self.execution_task.done():
                await self.execution_task
                raise AssertionError("Installed original Agent exited before its model gate")
            if self.claim is None or not (self.directory / "process.jsonl").exists():
                return False
            if '"barrier-entered"' not in (self.directory / "process.jsonl").read_text():
                return False
            async with self.engine.connect() as conn:
                rows = (await conn.execute(text("SELECT event_type,content,seq FROM run_events WHERE run_id=:run ORDER BY seq"), {"run": self.record.run_id})).all()
            return any(row[0] == "llm.ai.response" for row in rows)

        await bounded_wait(committed, seconds=30)

    async def wait_for_runner_exit(self):
        self.result = await asyncio.wait_for(asyncio.shield(self.execution_task), 60)
        self.inspected = await self.driver.inspect(self.grant["process_ref"])
        assert not self.inspected["State"]["Running"]
        assert self.inspected["State"]["ExitCode"] == 0
        assert self.result["state"] == "succeeded" and not self.result["report_pending"]

    async def acknowledge_actual_stop(self):
        # NodeDaemon already submitted its actual container stop observation.
        assert self.execution_task.done() and not self.inspected["State"]["Running"]
        return self.result

    async def start_receipts(self):
        rows = await self.driver.list_managed(self.node.node_id)
        assert len(rows) == 1
        observation = await self.driver.inspect(rows[0][0])
        assert observation["RestartCount"] == 0
        ready = self.driver.ready[rows[0][0]]
        return [
            {
                "container_id": observation["Id"],
                "started_at": observation["State"]["StartedAt"],
                "restart_count": observation["RestartCount"],
                "ready_pid": ready["pid"],
                "ready_host": ready["host"],
                "entrypoint": observation["Config"]["Entrypoint"],
                "cmd": observation["Config"]["Cmd"],
            }
        ]

    async def durable_receipt(self):
        async with self.engine.connect() as conn:
            run = (await conn.execute(text("SELECT status,error FROM runs WHERE run_id=:run"), {"run": self.record.run_id})).one()
            events = (await conn.execute(text("SELECT event_type,content,seq FROM run_events WHERE run_id=:run ORDER BY seq"), {"run": self.record.run_id})).all()
            attempt = (await conn.execute(text("SELECT state,stopped_at,outcome FROM fleet_attempts WHERE id=:id"), {"id": self.claim["attempt_id"]})).one()
            seal = (await conn.execute(text("SELECT source,last_seq,core_status FROM fleet_stream_seals WHERE run_id=:run"), {"run": self.record.run_id})).one()
        return {"run": list(run), "events": [list(row) for row in events], "attempt": list(attempt), "seal": list(seal), "frames": self.frames, "http": self.http_receipts, "starts": await self.start_receipts(), "container": self.inspected}

    async def close(self):
        from .c08_installed_cleanup import settle_owned_cleanup, settle_owned_execution

        original_error = sys.exc_info()[1]

        async def execution():
            if hasattr(self, "execution_task"):
                await settle_owned_execution(self.execution_task)

        async def stop():
            if self.grant is not None:
                await self.driver.stop(self.grant["process_ref"])

        async def remove():
            if self.grant is not None:
                await self.driver.command("rm", self.grant["process_ref"])

        actions = []
        if hasattr(self, "execution_task") and not self.execution_task.done() and self.claim is not None:
            actions.append(("release model barrier", self.release_model_barrier))
        actions.append(("execution", execution))
        if getattr(self.daemon, "workspace_publications", None) is not None:
            actions.append(("writers", self.daemon.workspace_publications.join_writers))
        actions.extend([("stop", stop), ("remove", remove)])
        await settle_owned_cleanup(actions, original_error=original_error)


@pytest.mark.live
@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["cached", "hydrated"])
@pytest.mark.parametrize("seal_fault", [False, True])
async def test_installed_original_agent_replays_committed_tail_after_redis_loss(tmp_path, reader, seal_fault):
    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers
    from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_extension_api import ExtensionRuntimeDeps
    from fastapi import FastAPI

    import deerflow.persistence.models  # noqa: F401
    from app.fleet.events import install_fleet_events
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
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.store.async_provider import make_store
    from deerflow.runtime.stream_bridge.redis import RedisStreamBridge
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    image = os.environ.get("FLEET_C07_TEST_IMAGE")
    if os.environ.get("FLEET_TEST_CONTAINERS") == "1" and not image:
        pytest.fail("required explicit installed gate needs immutable FLEET_C07_TEST_IMAGE")
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1" or not image:
        pytest.skip("explicit installed C07 gate requires FLEET_TEST_CONTAINERS=1 and FLEET_C07_TEST_IMAGE")
    evidence = Path(os.environ.get("C07_EVIDENCE_DIR", str(tmp_path))) / ("installed-" + reader + ("-omit-seal" if seal_fault else "-writer-seal"))
    evidence.mkdir(parents=True, exist_ok=False)
    async with control_database(tmp_path) as db, AsyncExitStack() as resources:
        async with db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text("CREATE TABLE c06_extension(value text)"))
        private = {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider", "allow_host_bash": True},
            "database": {"backend": "postgres", "postgres_url": db.runner_url, "postgres_schema": db.schema},
            "run_events": {"backend": "db"},
            "agent_storage": {"backend": "db"},
            "plugins": [{"name": "c04-plugin", "package": "deerflow-c04-runtime-fixture", "use": "deerflow_c04_fixture:install", "required": True}],
            "skills": {"path": "/opt/deerflow/skills"},
            "extensions": {
                "skills": {"c04-enabled": {"enabled": True}, "c04-disabled": {"enabled": False}},
                "mcpServers": {"c04": {"command": "/usr/local/bin/python", "args": ["-m", "fleet.c04_mcp_fixture"], "env": {"ERP_AUTH": "c04-target-access"}}},
            },
            "memory": {"enabled": False, "manager_class": "noop"},
            "title": {"enabled": False},
            "summarization": {"enabled": False},
            "models": [
                {"name": "model-1", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent", "api_key": "c04-scripted-provider-credential", "c07_gate": True},
                {"name": "child", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "child", "api_key": "c04-scripted-provider-credential"},
            ],
            "tools": [{"name": "c07_barrier", "group": "c07", "use": "fleet.c07_integration_fixture:c07_barrier"}],
            "tool_groups": [{"name": "c07", "description": "Original installed model gate"}],
        }
        native = AppConfig.model_validate({**private, "database": {**private["database"], "postgres_url": db.host_url}})
        await resources.enter_async_context(make_checkpointer(native))
        await resources.enter_async_context(make_store(native))
        driver = AgentContainers(provider="c07-omit-seal" if seal_fault else "c07-stock", state_dir=tmp_path / "agent-state", operator_config=private)
        image_observation = json.loads(await driver.checked("image", "inspect", image))[0]
        image = image_observation["Id"]
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        nas_dir = tmp_path / "execution-nas"
        nas_dir.mkdir()
        nas = settings(nas_dir)
        data = c_config()
        data.update(nas_root=str(nas.nas_root), nas_identity=nas.nas_identity)
        data["profiles"]["remote"].update(image=image, runtime_digest=actual.runtime_digest, user=f"{os.getuid()}:{os.getgid()}", network="bridge", pids_limit=128)
        config = FleetConfig.model_validate(data)
        fleet = service_class()(config)
        await fleet.start(ExtensionRuntimeDeps(session_factory=db.session_factory))
        resources.push_async_callback(fleet.stop)
        public, _ = execution_configuration(AppConfig.model_validate(private))
        set_app_config(public)
        resources.callback(reset_app_config)
        user = SimpleNamespace(id="user-c07-installed", system_role="admin")
        token = set_current_user(user)
        resources.callback(reset_current_user, token)
        app = FastAPI()
        app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
        app.state.run_store = RunRepository(db.session_factory)
        app.state.run_manager = RunManager(store=app.state.run_store)
        app.state.thread_store = ThreadMetaRepository(db.session_factory)
        install_fleet_ownership(app, db.session_factory)
        app.add_middleware(AuthMiddleware)
        app.add_middleware(CSRFMiddleware)
        app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
        app.include_router(importlib.import_module("app.gateway.routers.thread_runs").router)
        node_id = "c07-node-" + uuid4().hex
        await fleet.nodes.register(node_id=node_id, name=node_id, cpu_millis=1500, memory_mib=4096, agent_limit=1, profile_allowlist=["remote"])
        credential = await fleet.credentials.issue(node_id, lifetime_seconds=600)
        redis, port, _ = await resources.enter_async_context(owned_redis(evidence / "redis"))
        proxy = RedisFaultProxy(port)
        await proxy.start()
        resources.push_async_callback(proxy.close)
        prefix = "c07:" + uuid4().hex
        local = RedisStreamBridge(redis_url=f"redis://127.0.0.1:{proxy.port}/0", key_prefix=prefix, heartbeat_interval=0.1)
        resources.push_async_callback(local.close)
        app.state.stream_bridge = local
        bridge = install_fleet_events(app, db.session_factory)
        resources.push_async_callback(bridge.close)
        await bridge.start()
        thread_id = "thread-c07-installed"
        manifest = AgentWorkspaceManifest(user_id=user.id, thread_id=thread_id, files=[], total_bytes=0)
        snapshot = nas.nas_root / ".fleet-agent-inputs" / user.id / thread_id / manifest.reference
        snapshot.mkdir(parents=True)
        (snapshot / "manifest.json").write_bytes(manifest.canonical_bytes())
        backend = FleetExecutionBackend(
            config=config,
            profile_name="remote",
            model_name="model-1",
            model_version="v1",
            skill_snapshot=actual.skill_snapshot,
            plugin_snapshot=actual.plugin_snapshot,
            workspace_manifest_ref=manifest.reference,
            secret_refs=[
                {"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"},
                {"name": "CHILD_API_KEY", "reference_id": "operator-child-binding"},
                {"name": "MCP_TARGET_TOKEN", "reference_id": "operator-mcp-binding"},
            ],
        )
        body = RunCreateRequest(
            input={"messages": [{"role": "user", "content": "Run the original installed C07 gate and answer."}]},
            config={"recursion_limit": 50, "context": {"model_name": "model-1", "subagent_enabled": False}},
            stream_mode=["values", "messages-tuple", "updates"],
            stream_subgraphs=True,
        )
        record = await services.start_run(body, thread_id, trusted_request(app.state.run_manager, user), execution_backend=backend)
        async with node_server(app) as url:
            client = NodeClient(gateway_url=url, credential=credential.token, claim_kind="agent", compatibility=actual.model_dump(mode="json"))
            scenario = InstalledScenario(directory=evidence, engine=db.engine, record=record, claim=None, grant=None, bootstrap=None, app=app, url=url, node=client, proxy=proxy, redis=redis, prefix=prefix)
            scenario.driver = driver

            async def prepare(claim, grant):
                scenario.claim, scenario.grant = claim, grant
                scenario.directory = await driver.prepare_workspace(nas.nas_root, claim, grant)
                return scenario.directory

            scenario.daemon = NodeDaemon(client=client, containers=driver, state_dir=tmp_path / "agent-state", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.25, poll_seconds=0.05)
            from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication
            from deerflow_ecs_fleet.workspace import NASWorkspace

            scenario.daemon.workspace_publications = AgentWorkspacePublication(client=client, containers=driver, nas=NASWorkspace(nas.nas_root, identity=nas.nas_identity), journal=scenario.daemon.journal)
            try:
                await scenario.daemon.bootstrap()
                await scenario.start_once()
                await scenario.wait_for_committed_model_event()
                cursor = await scenario.initial_cursor()
                await scenario.disconnect_redis()
                await scenario.release_model_barrier()
                await scenario.wait_for_runner_exit()
                assert (await scenario.acknowledge_actual_stop())["state"] == "succeeded"
                if reader == "hydrated":
                    app.state.run_manager = RunManager(store=app.state.run_store)
                await bridge.publisher.stop()
                await scenario.restore_redis()
                await scenario.delete_owned_redis_keys()
                frames = await scenario.join(cursor=cursor)
                assert frames[-1]["event"] == "end"
                assert any(scenario.expected_tail in json.dumps(frame.get("data")) for frame in frames)
                ids = [frame["id"] for frame in frames if frame.get("id")]
                seqs = [int(value.rsplit(".", 1)[1]) for value in ids]
                assert seqs == sorted(set(seqs))
                receipt = await scenario.durable_receipt()
                assert len(receipt["starts"]) == 1
                receipt["image_id"] = image
                expected_source = "physical_stop" if seal_fault else "writer"
                assert receipt["seal"][0] == expected_source
                process_receipts = [json.loads(line) for line in (scenario.directory / "process.jsonl").read_text().splitlines()]
                if seal_fault:
                    assert len([item for item in process_receipts if item["event"] == "safe-seal-omitted"]) == 1
                    assert len([item for item in process_receipts if item["event"] == "original-environment-settled"]) == 1
                receipt["process_receipts"] = process_receipts
                receipt["fault"] = "safe seal omission after actual closure" if seal_fault else None
                receipt["observed"] = {"ordered_unique_events": seqs == sorted(set(seqs)), "runner_starts": len(receipt["starts"]), "terminal_end_recovered": frames[-1]["event"] == "end"}
                (evidence / "installed-receipt.json").write_text(json.dumps(receipt, indent=2, default=str))
            finally:
                try:
                    await scenario.close()
                finally:
                    await client.close()
