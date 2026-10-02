"""Actual Agent start/stop, existing SQL run attachment and Linux runner parity."""

import httpx
import pytest
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import claim, leases
from .test_c03_remote_agent_admission import owner_environment as owner_environment


@pytest.mark.asyncio
async def test_actual_node_start_freezes_agent_authorization_without_starting_core_run(owner_environment):
    env = owner_environment
    engine, _, _, _, app, _, session_id, credential, _ = env
    accepted = await claim(env)
    identity = {"node_session_id": session_id, "token": accepted.token}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        reply = await client.post("/api/fleet/node/attempts/" + accepted.attempt_id + "/start", headers={"Authorization": "Bearer " + credential.token}, json=identity)
        assert reply.status_code == 200, "actual Agent start protocol must return frozen authorization"
        grant = reply.json()
        assert grant["authorized"] and grant["process_ref"] == "fleet-" + accepted.attempt_id
        assert grant["execution_profile"]["kind"] == "agent"
    run, attempt = await leases(engine)
    assert run[0] == "pending" and attempt[0] == "starting" and run[2] == attempt[2]


@pytest.mark.asyncio
async def test_trusted_attach_uses_one_existing_sql_run_and_owned_start_rechecks_lease(owner_environment):
    from deerflow.runtime import RunManager

    env = owner_environment
    engine, _, user, _, app, record, session_id, _, _ = env
    accepted = await claim(env)
    await app.state.fleet_ownership.authorize_start(node_id="node-c03", node_session_id=session_id, attempt_id=accepted.attempt_id, token=accepted.token)
    manager = RunManager(store=app.state.run_store, worker_id=accepted.owner_worker_id)
    attached = await manager.attach_existing_executor(record.run_id, user_id=user.id, thread_id=record.thread_id, owner_worker_id=accepted.owner_worker_id, execution_backend="fleet")
    assert attached.run_id == record.run_id and attached.owner_worker_id == accepted.owner_worker_id
    async with engine.begin() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 1
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 hour'"))
    outcome = await manager.try_start(record.run_id)
    assert outcome.value == "cancelled"
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT status FROM runs"))).scalar_one() == "pending"


@pytest.mark.asyncio
async def test_private_model_scope_reaches_thread_and_is_cleaned_on_exit():
    import asyncio

    from deerflow.config.app_config import AppConfig
    from deerflow.models.credentials import model_credential_scope
    from deerflow.models.factory import create_chat_model

    cfg = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "models": [{"name": "private", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent"}]})

    def construct():
        return create_chat_model("private", app_config=cfg, attach_tracing=False)

    with model_credential_scope(lambda name, use: {"api_key": "scope-fixture"}):
        model = await asyncio.to_thread(construct)
        assert model.api_key.get_secret_value() == "scope-fixture"
        assert "scope-fixture" not in cfg.model_dump_json()
    assert construct().api_key is None


def test_execution_config_keeps_model_credentials_private_and_rejects_unsupported_auth():
    from app.fleet.runner_context import execution_configuration
    from deerflow.config.app_config import AppConfig

    cfg = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "database": {"backend": "postgres", "postgres_url": "postgresql://private:control-password@localhost/test"},
            "models": [{"name": "parent", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent", "api_key": "private-model-credential"}],
        }
    )
    execution, resolver = execution_configuration(cfg)
    assert "control-password" not in execution.model_dump_json()
    assert "private-model-credential" not in execution.model_dump_json()
    assert resolver("parent", "fleet.c04_worker_fixture:ScriptedModel") == {"api_key": "private-model-credential"}
    with pytest.raises(ValueError, match="operator-approved"):
        resolver("client-chosen", "fleet.c04_worker_fixture:ScriptedModel")
    cfg.models[0].model_extra["default_headers"] = {"Authorization": "private-header-credential"}
    with pytest.raises(ValueError, match="authentication"):
        execution_configuration(cfg)


def test_native_entry_fails_before_consuming_bootstrap_or_loading_provider():
    import subprocess
    import sys
    from pathlib import Path

    import deerflow_ecs_fleet

    if sys.platform == "linux":
        pytest.skip("native rejection test is for non-Linux; Docker verifies Linux entry")
    wrapper = Path(deerflow_ecs_fleet.__file__).parent / "worker/libexec_bootstrap.py"
    result = subprocess.run([sys.executable, "-I", "-S", str(wrapper), "--provider", "not-installed"], input=b"private-bootstrap-not-consumed\n", capture_output=True, timeout=10)
    assert result.returncode == 1
    assert result.stdout == b""
    assert result.stderr == b"Agent trusted bootstrap failed\n"


@pytest.mark.asyncio
async def test_model_binding_drift_is_rejected_before_execution():
    from deerflow_ecs_fleet.launch_spec import LaunchSpec

    from app.fleet.runner_context import validate_model_bindings
    from deerflow.config.app_config import AppConfig

    from .test_c01_remote_agent_admission import wire

    spec = LaunchSpec.model_validate(wire())
    cfg = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "models": [{"name": spec.model_name, "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent"}]})
    binding = {spec.model_name: {"provider_use": "fleet.c04_worker_fixture:ScriptedModel", "target_model": "parent", "version": spec.model_version}}
    validate_model_bindings(cfg, spec, binding)
    cfg.models[0].model = "changed"
    with pytest.raises(ValueError, match="drifted"):
        validate_model_bindings(cfg, spec, binding)
    cfg.models[0].model = "parent"
    binding[spec.model_name]["version"] = "changed"
    with pytest.raises(ValueError, match="version"):
        validate_model_bindings(cfg, spec, binding)


@pytest.mark.asyncio
async def test_agent_stop_requires_matching_physical_proof_before_capacity_release(owner_environment):
    env = owner_environment
    engine, _, _, _, app, _, session_id, _, _ = env
    accepted = await claim(env)
    identity = {"node_id": "node-c03", "node_session_id": session_id, "attempt_id": accepted.attempt_id, "token": accepted.token}
    grant = await app.state.fleet_ownership.authorize_start(**identity)
    with pytest.raises(ValueError, match="physical"):
        await app.state.fleet_ownership.stopped(**identity, process_ref=grant["process_ref"], reason="lease_lost", exit_code=137)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "reserved"
    with pytest.raises(ValueError, match="identity"):
        await app.state.fleet_ownership.stopped(**identity, process_ref="fleet-wrong", physical_stopped=True, reason="lease_lost", exit_code=137)
    result = await app.state.fleet_ownership.stopped(**identity, process_ref=grant["process_ref"], physical_stopped=True, reason="lease_lost", exit_code=137)
    assert result["state"] == "unknown"
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"


@pytest.mark.asyncio
@pytest.mark.parametrize("memory_mode", ["tool", "middleware"])
@pytest.mark.parametrize("bootstrap_failure,interrupted", [(False, False), (True, False), (False, True)], ids=["parity", "failed-one-shot", "interrupt-before-tools"])
async def test_actual_daemon_runs_real_lead_graph_in_independent_linux_container(tmp_path, bootstrap_failure, interrupted, memory_mode):
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
    from deerflow.runtime import RunManager
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    from .c04_integration_fixture import control_database, node_server
    from .test_b02_fleet_foundation import service_class, settings
    from .test_c01_remote_agent_admission import c_config
    from .test_c02_remote_agent_admission import request

    image = os.environ.get("FLEET_AGENT_TEST_IMAGE")
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1" or not image:
        pytest.skip("requires FLEET_TEST_CONTAINERS=1 and actual installed FLEET_AGENT_TEST_IMAGE")
    async with control_database(tmp_path) as db:
        async with db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text("CREATE TABLE c06_extension(value text)"))
            for table in ("c06_memory", "c06_local_memory"):
                await conn.execute(text("CREATE TABLE " + table + "(user_id text NOT NULL,agent_name text NOT NULL,fact_id text NOT NULL,content text NOT NULL,PRIMARY KEY(user_id,agent_name,fact_id))"))
        private = {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider", "allow_host_bash": True},
            "database": {"backend": "postgres", "postgres_url": db.runner_url, "postgres_schema": db.schema},
            "run_events": {"backend": "db"},
            "mcp_tasks": {"enabled": True},
            "agent_storage": {"backend": "db"},
            "plugins": [{"name": "c04-plugin", "package": "deerflow-c04-runtime-fixture", "use": "deerflow_c04_fixture:install", "required": True}],
            "memory": {"enabled": True, "mode": memory_mode, "manager_class": "deerflow_c04_fixture.memory:PostgresMemory"},
            "title": {"enabled": False},
            "summarization": {"enabled": False},
            "models": [
                {"name": "model-1", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent", "api_key": "c04-scripted-provider-credential"},
                {"name": "child", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "child", "api_key": "c04-scripted-provider-credential"},
            ],
            "tools": [
                {"name": "bash", "group": "bash", "use": "deerflow.sandbox.tools:bash_tool"},
                *([{"name": "memory_add", "group": "memory", "use": "deerflow.agents.memory.tools:memory_add_tool"}] if memory_mode == "tool" else []),
            ],
            "tool_groups": [{"name": "bash", "description": "Bounded container execution"}, {"name": "memory", "description": "Configured PostgreSQL memory mutation"}],
            "subagents": {"agents": {"general-purpose": {"model": "child"}}},
            "extensions": {
                "skills": {"c04-enabled": {"enabled": True}, "c04-disabled": {"enabled": False}},
                "mcpServers": {
                    "c04": {
                        "command": "/usr/local/bin/python",
                        "args": ["-m", "fleet.c04_mcp_fixture"],
                        "env": {"ERP_AUTH": "c04-target-access"},
                        "task_toolsets": [{"name": "c04-job", "submit_tool": "submit_job", "status_tool": "job_status", "cancel_tool": "cancel_job"}],
                    }
                },
            },
        }
        # Trusted Gateway setup precedes remote readiness; remote saver never migrates.
        from deerflow.runtime.checkpointer.async_provider import make_checkpointer

        native_private = AppConfig.model_validate({**private, "database": {**private["database"], "postgres_url": db.host_url}})
        async with make_checkpointer(native_private):
            pass
        from deerflow.runtime.store.async_provider import make_store

        async with make_store(native_private):
            pass
        public, _ = execution_configuration(AppConfig.model_validate(private))
        set_app_config(public)
        user = SimpleNamespace(id="user-c04", system_role="admin")
        token = set_current_user(user)
        driver = AgentContainers(state_dir=tmp_path / "agent-state", operator_config=private, provider="gateway")
        inspected_image = __import__("json").loads(await driver.checked("image", "inspect", image))[0]
        image = inspected_image["Id"]
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        if bootstrap_failure:
            driver.provider = "not-installed"
        (tmp_path / "execution-nas").mkdir()
        nas = settings(tmp_path / "execution-nas")
        config_data = c_config()
        config_data.update(nas_root=str(nas.nas_root), nas_identity=nas.nas_identity)
        config_data["profiles"]["remote"].update(image=image, runtime_digest=actual.runtime_digest, user=str(os.getuid()) + ":" + str(os.getgid()), network="bridge", pids_limit=128)
        config = FleetConfig.model_validate(config_data)
        fleet = service_class()(config)
        await fleet.start(ExtensionRuntimeDeps(session_factory=db.session_factory))
        app = FastAPI()
        app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
        app.state.run_store = RunRepository(db.session_factory)
        install_fleet_ownership(app, db.session_factory)
        app.add_middleware(AuthMiddleware)
        app.add_middleware(CSRFMiddleware)
        app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
        node_id = "node-c04-" + __import__("uuid").uuid4().hex
        await fleet.nodes.register(node_id=node_id, name=node_id, cpu_millis=1000, memory_mib=2048, agent_limit=1, profile_allowlist=["remote"])
        credential = await fleet.credentials.issue(node_id, lifetime_seconds=600)
        import hashlib

        from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest

        source_bytes = {"uploads": b"artifact\n", "workspace": b"c04-"}
        manifest = AgentWorkspaceManifest(
            user_id=user.id,
            thread_id="thread-c04",
            files=[{"category": category, "path": "source.txt", "size": len(value), "sha256": hashlib.sha256(value).hexdigest()} for category, value in sorted(source_bytes.items())],
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
            secret_refs=[{"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"}, {"name": "CHILD_API_KEY", "reference_id": "operator-child-binding"}, {"name": "MCP_TARGET_TOKEN", "reference_id": "operator-mcp-binding"}],
        )
        core = RunManager(store=app.state.run_store)
        body = RunCreateRequest(
            input={"messages": [{"role": "user", "content": "Create deterministic parent and child artifacts c04-control-port=" + str(db.port)}]},
            config={"recursion_limit": 100, "context": {"model_name": "model-1", "subagent_enabled": True}},
            stream_mode=["values", "messages-tuple", "custom"],
            stream_subgraphs=True,
            interrupt_before=["tools"] if interrupted else None,
        )
        record = await services.start_run(body, "thread-c04", request(core, user), execution_backend=backend)
        refs = []
        try:
            async with node_server(app) as url:
                client = NodeClient(gateway_url=url, credential=credential.token, claim_kind="agent", compatibility=actual.model_dump(mode="json"))

                async def prepare(claim, grant):
                    return await driver.prepare_workspace(nas.nas_root, claim, grant)

                daemon = NodeDaemon(client=client, containers=driver, state_dir=tmp_path / "agent-state", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.25, poll_seconds=0.05)
                try:
                    await daemon.bootstrap()
                    if bootstrap_failure:
                        from deerflow_ecs_fleet.worker.containers import DockerError

                        with pytest.raises(DockerError, match="readiness"):
                            await daemon.execute_one()
                        refs = [ref for ref, _ in await driver.list_managed(node_id)]
                        assert len(refs) == 1
                        observed = await driver.inspect(refs[0])
                        assert not observed["State"]["Running"]
                        journal = daemon.journal.records()[0]
                        async with db.engine.connect() as conn:
                            assert (await conn.execute(text("SELECT status FROM runs"))).scalar_one() == "pending"
                            assert (await conn.execute(text("SELECT state FROM fleet_run_placements"))).scalar_one() == "unknown"
                            assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
                        output = await prepare(journal["claim"], journal["grant"])
                        started_at = observed["State"]["StartedAt"]
                        repeated = await driver.launch(journal["grant"], output_dir=output, input_dirs={})
                        assert not repeated["State"]["Running"] and repeated["State"]["StartedAt"] == started_at
                        assert len(await driver.list_managed(node_id)) == 1
                        assert await client.claim() is None
                        return
                    result = await daemon.execute_one()
                    refs = [ref for ref, _ in await driver.list_managed(node_id)]
                    diagnostic = Path(f"/private/tmp/c04-runner-{memory_mode}-{bootstrap_failure}-{interrupted}-diagnostic.log")
                    for diagnostic_ref in refs:
                        _, stdout, stderr = await driver.command("logs", diagnostic_ref)
                        diagnostic.write_text((stdout + stderr).replace(db.password, "[control credential redacted]"))
                        diagnostic.chmod(0o600)
                    async with db.engine.connect() as diagnostic_conn:
                        diagnostics = (await diagnostic_conn.execute(text("SELECT status,error FROM runs"))).all()
                        with diagnostic.open("a") as sink:
                            sink.write(str(diagnostics).replace(db.password, "[control credential redacted]"))
                    assert result["state"] == "succeeded" and not result["report_pending"]
                    assert len(refs) == 1
                    ready = driver.ready[refs[0]]
                    assert ready["pid"] != os.getpid() and ready["host"] != __import__("socket").gethostname()
                    observed = await driver.inspect(refs[0])
                    assert not observed["State"]["Running"] and observed["HostConfig"]["PidsLimit"] == 128
                    if interrupted:
                        from deerflow.runtime.checkpointer.async_provider import make_checkpointer

                        native_private = AppConfig.model_validate({**private, "database": {**private["database"], "postgres_url": db.host_url}})
                        async with make_checkpointer(native_private) as checkpointer:
                            snapshot = await checkpointer.aget_tuple({"configurable": {"thread_id": "thread-c04"}})
                            assert snapshot is not None
                            messages = snapshot.checkpoint["channel_values"]["messages"]
                            assert any(getattr(message, "tool_calls", None) for message in messages)
                            assert not any(message.type == "tool" for message in messages)
                        assert not list(nas.nas_root.rglob("parent.txt")) and not list(nas.nas_root.rglob("child.txt"))
                        return
                    async with db.engine.connect() as conn:
                        lifecycle = list((await conn.execute(text("SELECT value FROM c06_extension ORDER BY value"))).scalars())
                        assert lifecycle == ["lead-start", "start", "subagent-start", "subagent-stop"]
                        receipt = (await conn.execute(text("SELECT content FROM run_events WHERE event_type='run.extension.task_stop'"))).scalar_one()
                        assert __import__("json").loads(receipt) == {"task_id": record.run_id, "outcome": "completed"}
                        memories = list((await conn.execute(text("SELECT content FROM c06_memory ORDER BY content"))).scalars())
                        tracked = (await conn.execute(text("SELECT user_id,thread_id,run_id,remote_task_id FROM mcp_tasks"))).one()
                        assert tracked.user_id == user.id and tracked.thread_id == record.thread_id and tracked.run_id == record.run_id
                        assert __import__("re").fullmatch(r"c04-external-job-c04-job-input-[0-9a-f]{32}", tracked.remote_task_id)
                        if memory_mode == "tool":
                            assert "c04-parent-memory" in memories and "c04-child-memory" in memories
                        else:
                            assert memories and any("c04-result" in fact for fact in memories)
                    assert not list(nas.nas_root.rglob("plugin-lifecycle.json"))
                    outputs = list(nas.nas_root.rglob("parent.txt"))
                    assert len(outputs) == 1 and outputs[0].read_bytes() == b"c04-artifact\n"
                    children = list(nas.nas_root.rglob("child.txt"))
                    assert len(children) == 1 and children[0].read_bytes() == b"c04-artifact\n"
                    for role in ("parent", "child"):
                        probe_paths = list(nas.nas_root.rglob(role + "-probe.json"))
                        assert len(probe_paths) == 1
                        probe = __import__("json").loads(probe_paths[0].read_text())
                        assert probe["pid"] != ready["pid"] and probe["host"] == ready["host"]
                        assert probe["cgroup"] == probe["runner_cgroup"]
                        assert probe["pid_namespace"] == ready["pid_namespace"]
                        assert probe["uid"] == ready["uid"] == os.getuid() and probe["uid"] != 0
                        assert all(probe[part] == "denied" for part in ("environ", "fd", "mem", "ptrace", "db_without_password", "db_wrong_password"))
                        assert probe["raw_config_absent"] and probe["control_env_absent"]
                    import asyncio

                    for role in ("parent", "child"):
                        marker = next(nas.nas_root.rglob(role + "-probe.ticks"))
                        child_identity = __import__("json").loads(marker.with_suffix(".child.json").read_text())
                        assert child_identity["uid"] == ready["uid"] and child_identity["cgroup"] == probe["runner_cgroup"]
                        before_ticks = marker.read_bytes()
                        assert before_ticks.strip()
                        await asyncio.sleep(0.15)
                        assert marker.read_bytes() == before_ticks
                    recovered = daemon.journal.records()
                    assert len(recovered) == 1
                    journal = recovered[0]
                    # Recovery bearer remains only in the trusted private daemon journal.
                    assert journal["claim"]["token"] and (daemon.journal.root / (journal["claim"]["attempt_id"] + ".json")).stat().st_mode & 0o077 == 0
                    started_at = observed["State"]["StartedAt"]
                    output_root = await prepare(journal["claim"], journal["grant"])
                    repeated = await driver.launch(journal["grant"], output_dir=output_root, input_dirs={})
                    assert not repeated["State"]["Running"] and repeated["State"]["StartedAt"] == started_at
                    assert len(await driver.list_managed(node_id)) == 1
                    from .c04_integration_fixture import local_parity_subprocess

                    parity = await local_parity_subprocess(db, private, body, user, tmp_path / "local-parity")
                    assert parity["pid"] != os.getpid()
                    assert parity["local"] == parity["remote"]
                    assert parity["artifacts"] == {"parent.txt": outputs[0].read_bytes().hex(), "child.txt": children[0].read_bytes().hex()}
                    assert parity["todos_equal"] and parity["different_checkpoint_ids"] and parity["has_parent_refs"]
                    async with db.engine.connect() as conn:
                        assert (await conn.execute(text("SELECT status FROM runs WHERE run_id=:id"), {"id": record.run_id})).scalar_one() == "success"
                        assert (await conn.execute(text("SELECT count(*) FROM run_events"))).scalar_one() > 0
                        assert (await conn.execute(text("SELECT count(*) FROM checkpoints"))).scalar_one() > 0
                        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "released"
                finally:
                    await client.close()
        finally:
            for ref, _ in await driver.list_managed(node_id):
                await driver.command("rm", "--force", ref)
            await fleet.stop()
            reset_current_user(token)
            reset_app_config()


@pytest.mark.asyncio
async def test_terminal_core_cleanup_keeps_same_live_physical_owner_until_stop(owner_environment):
    env = owner_environment
    engine, _, _, _, app, _, session_id, _, _ = env
    accepted = await claim(env)
    identity = {"node_id": "node-c03", "node_session_id": session_id, "attempt_id": accepted.attempt_id, "token": accepted.token}
    await app.state.fleet_ownership.authorize_start(**identity)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
    reply = await app.state.fleet_ownership.renew(**identity, running=False)
    assert reply["stop"] is False
    async with engine.connect() as conn:
        run = (await conn.execute(text("SELECT status,owner_worker_id,lease_expires_at FROM runs"))).one()
        attempt = (await conn.execute(text("SELECT state,lease_expires_at FROM fleet_attempts"))).one()
        assert run[0] == "success" and run[1] == accepted.owner_worker_id
        assert attempt[0] == "starting" and run[2] == attempt[1]
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "reserved"
    with pytest.raises(ValueError, match="active"):
        await app.state.fleet_ownership.renew(**identity, running=True)
    with pytest.raises(ValueError, match="active"):
        await app.state.fleet_ownership.authorize_start(**identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["session", "token", "generation", "expired"])
async def test_terminal_cleanup_renewal_rejects_stale_identity_without_extending_rows(owner_environment, mutation):
    env = owner_environment
    engine, _, _, _, app, _, session_id, _, _ = env
    accepted = await claim(env)
    identity = {"node_id": "node-c03", "node_session_id": session_id, "attempt_id": accepted.attempt_id, "token": accepted.token}
    await app.state.fleet_ownership.authorize_start(**identity)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
        if mutation == "session":
            await conn.execute(text("UPDATE fleet_nodes SET session_id='replaced-session'"))
        elif mutation == "token":
            identity["token"] = "not-the-original-token"
        elif mutation == "generation":
            await conn.execute(text("UPDATE fleet_agent_tasks SET generation=generation+1"))
        else:
            await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 hour'"))
            await conn.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
        before = (await conn.execute(text("SELECT lease_expires_at FROM runs UNION ALL SELECT lease_expires_at FROM fleet_attempts"))).scalars().all()
    with pytest.raises((PermissionError, ValueError)):
        await app.state.fleet_ownership.renew(**identity, running=False)
    async with engine.connect() as conn:
        after = (await conn.execute(text("SELECT lease_expires_at FROM runs UNION ALL SELECT lease_expires_at FROM fleet_attempts"))).scalars().all()
        assert after == before
        assert (await conn.execute(text("SELECT state FROM fleet_reservations"))).scalar_one() == "reserved"


@pytest.mark.asyncio
async def test_private_contexts_are_concurrent_thread_safe_and_reset_after_exception():
    import asyncio

    from langchain_core.tools import tool

    from deerflow.config.extensions_config import ExtensionsConfig, extensions_config_scope, get_scoped_extensions_config
    from deerflow.mcp.cache import _scoped_mcp_tools, get_cached_mcp_tools, mcp_tools_scope
    from deerflow.models.credentials import model_credential_scope, resolve_model_credentials
    from deerflow.persistence.agent_definition_context import agent_definition_store_scope, get_scoped_definition_stores
    from deerflow.persistence.agents import get_agent_store
    from deerflow.persistence.agents.file import FileAgentStore
    from deerflow.persistence.managed_subagents.file import FileManagedSubagentStore

    @tool
    def scoped_echo(value: str) -> str:
        """Return a value through the actual private tool binding."""
        return value

    async def run(label):
        config = ExtensionsConfig.model_validate({"skills": {label: {"enabled": False}}})
        definitions = (FileAgentStore(), FileManagedSubagentStore())
        with model_credential_scope(lambda name, use: {"api_key": label}), extensions_config_scope(config), agent_definition_store_scope(*definitions), mcp_tools_scope([scoped_echo]):
            await asyncio.sleep(0)

            def check():
                assert resolve_model_credentials("model", "provider") == {"api_key": label}
                assert get_scoped_extensions_config() is config
                assert get_agent_store() is definitions[0]
                assert get_scoped_definition_stores() == definitions
                assert get_cached_mcp_tools() == [scoped_echo]

            await asyncio.to_thread(check)
        assert resolve_model_credentials("model", "provider") is None
        assert get_scoped_extensions_config() is None and get_scoped_definition_stores() is None and _scoped_mcp_tools.get() is None

    await asyncio.gather(run("first"), run("second"))
    with pytest.raises(RuntimeError):
        with extensions_config_scope(ExtensionsConfig()):
            raise RuntimeError("scope exit fixture")
    assert get_scoped_extensions_config() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", [None, "wrong-target-access", "c04-target-access"])
async def test_actual_authenticated_stdio_mcp_tool_requires_private_target_credential(credential):
    import sys

    from deerflow.config.extensions_config import ExtensionsConfig, extensions_config_scope
    from deerflow.mcp.cache import get_cached_mcp_tools, mcp_tools_scope
    from deerflow.mcp.session_pool import get_session_pool
    from deerflow.mcp.tools import get_mcp_tools

    cfg = ExtensionsConfig.model_validate(
        {"mcpServers": {"c04": {"command": sys.executable, "args": [str(__import__("pathlib").Path(__file__).with_name("c04_mcp_fixture.py"))], "env": {} if credential is None else {"ERP_AUTH": credential}}}}
    )
    try:
        with extensions_config_scope(cfg):
            tools = await get_mcp_tools()
            with mcp_tools_scope(tools):
                target = next(tool for tool in get_cached_mcp_tools() if tool.name == "c04_echo")
                if credential == "c04-target-access":
                    reply = await target.ainvoke({"value": "actual-authenticated-result"})
                    assert "actual-authenticated-result" in str(reply)
                else:
                    with pytest.raises(Exception, match="authentication rejected"):
                        await target.ainvoke({"value": "must-not-be-returned"})
    finally:
        await get_session_pool().close_all()


@pytest.mark.asyncio
async def test_actual_stdio_task_fixture_returns_structured_driver_payloads():
    import os
    import sys
    from pathlib import Path

    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(
        command=sys.executable,
        args=[str(Path(__file__).with_name("c04_mcp_fixture.py"))],
        env={**os.environ, "ERP_AUTH": "c04-target-access"},
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            submitted = await session.call_tool("submit_job", {"value": "structured-proof"})
            assert submitted.structuredContent is not None
            remote_id = submitted.structuredContent["task_id"]
            assert __import__("re").fullmatch(r"c04-external-job-structured-proof-[0-9a-f]{32}", remote_id)
            assert submitted.structuredContent == {"task_id": remote_id, "status": "running"}
            repeated = await session.call_tool("submit_job", {"value": "structured-proof"})
            assert repeated.structuredContent is not None
            assert repeated.structuredContent["task_id"] != remote_id
            status = await session.call_tool("job_status", {"task_id": remote_id})
            assert status.structuredContent == {"task_id": remote_id, "status": "completed", "result": "finished"}
            cancelled = await session.call_tool("cancel_job", {"task_id": remote_id})
            assert cancelled.structuredContent == {"task_id": remote_id, "status": "cancelled"}


@pytest.mark.parametrize("extra", [{"token": "fake-raw-auth"}, {"authorization": "fake-raw-auth"}, {"base_url": "https://credential@example.invalid/v1"}, {"api_base": "https://example.invalid/v1?token=fake-raw-auth"}])
def test_execution_view_rejects_raw_model_authentication_shapes(extra):
    from app.fleet.runner_context import execution_configuration
    from deerflow.config.app_config import AppConfig

    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "models": [{"name": "parent", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent", **extra}]})
    with pytest.raises(ValueError, match="credential|authentication"):
        execution_configuration(config)


@pytest.mark.parametrize("branch", ["when_thinking_enabled", "when_thinking_disabled"])
def test_model_binding_rejects_thinking_target_override(branch):
    from deerflow_ecs_fleet.launch_spec import LaunchSpec

    from app.fleet.runner_context import validate_model_bindings
    from deerflow.config.app_config import AppConfig

    from .test_c01_remote_agent_admission import wire

    spec = LaunchSpec.model_validate(wire())
    cfg = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "models": [{"name": spec.model_name, "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent", "supports_thinking": True, branch: {"model": "unapproved-target"}}],
        }
    )
    binding = {spec.model_name: {"provider_use": "fleet.c04_worker_fixture:ScriptedModel", "target_model": "parent", "version": spec.model_version}}
    with pytest.raises(ValueError, match="target"):
        validate_model_bindings(cfg, spec, binding)


@pytest.mark.parametrize("selection", ["default", "unknown-fallback", "custom", "authorization-fallback", "bootstrap"])
def test_actual_assembly_rejects_effective_model_drift(selection, tmp_path, monkeypatch):
    from deerflow.agents.lead_agent.agent import assemble_lead_agent
    from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
    from deerflow.persistence.agent_definition_context import agent_definition_store_scope
    from deerflow.persistence.agents.file import FileAgentStore
    from deerflow.persistence.managed_subagents.file import FileManagedSubagentStore

    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "definitions"))
    cfg = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "memory": {"enabled": False, "manager_class": "noop"},
            "models": [{"name": "first", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent"}, {"name": "second", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "child"}],
        }
    )
    config = {"configurable": {"thread_id": "assembly-c04"}, "context": {"user_id": "user-c04"}}
    expected = "second"
    if selection == "unknown-fallback":
        config["context"]["model_name"] = "not-configured"
    elif selection == "bootstrap":
        config["context"]["is_bootstrap"] = True
    elif selection == "authorization-fallback":
        cfg.authorization.enabled = True
        from deerflow.config.authorization_config import AuthorizationProviderConfig

        cfg.authorization.provider = AuthorizationProviderConfig(use="deerflow.authz.rbac:RbacAuthorizationProvider", config={"roles": {"user": {"models": {"allow": ["first"]}}}})
        config["context"].update(model_name="second", user_role="user")
    stores = (FileAgentStore(), FileManagedSubagentStore())
    set_app_config(cfg)
    try:
        with agent_definition_store_scope(*stores):
            if selection == "custom":
                stores[0].create("custom-c04", {"model": "first"}, "Custom runtime binding", user_id="user-c04")
                config["context"]["agent_name"] = "custom-c04"
            with pytest.raises(ValueError, match="trusted launch binding"):
                assemble_lead_agent(config, app_config=cfg, expected_model_name=expected)
    finally:
        reset_app_config()


@pytest.mark.asyncio
async def test_agent_workspace_ref_must_resolve_before_execution(tmp_path):
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers

    from .test_c01_remote_agent_admission import wire

    nas = tmp_path / "nas"
    nas.mkdir()
    driver = AgentContainers(state_dir=tmp_path / "private", operator_config={})
    grant = {"launch_spec": wire(), "input_limits": {"max_input_bytes": 64 * 1024 * 1024}}
    claim = {"attempt_id": "attempt-c04-workspace"}
    with pytest.raises(ValueError, match="workspace|manifest"):
        await driver.prepare_workspace(nas, claim, grant)


@pytest.mark.parametrize("mutation", ["valid", "empty", "owner", "digest", "hash", "symlink", "hardlink", "target-symlink", "budget"])
def test_real_workspace_manifest_copy_is_owner_hash_bound_and_no_follow(tmp_path, mutation):
    import hashlib
    import os

    from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest, AgentWorkspaceSnapshots

    from .test_c01_remote_agent_admission import wire

    spec = wire()
    data = b"workspace-input"
    files = [] if mutation == "empty" else [{"category": "uploads", "path": "source.txt", "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}]
    manifest = AgentWorkspaceManifest(user_id=spec["user_id"] if mutation != "owner" else "other-user", thread_id=spec["thread_id"], files=files, total_bytes=0 if not files else len(data))
    spec["workspace_manifest_ref"] = manifest.reference
    nas = tmp_path / "nas"
    root = nas / ".fleet-agent-inputs" / spec["user_id"] / spec["thread_id"] / manifest.reference
    (root / "uploads").mkdir(parents=True)
    source = root / "uploads/source.txt"
    source.write_bytes(data)
    (root / "manifest.json").write_bytes(manifest.canonical_bytes())
    target = tmp_path / "execution"
    target.mkdir()
    if mutation == "digest":
        changed = __import__("json").loads(manifest.canonical_bytes())
        changed["files"][0]["sha256"] = "b" * 64
        (root / "manifest.json").write_text(__import__("json").dumps(changed))
    elif mutation == "hash":
        source.write_bytes(b"changed-content")
    elif mutation == "symlink":
        source.unlink()
        source.symlink_to(tmp_path / "outside-control")
    elif mutation == "hardlink":
        os.link(source, tmp_path / "second-link")
    elif mutation == "target-symlink":
        (tmp_path / "outside").mkdir()
        (target / ".deer-flow").symlink_to(tmp_path / "outside")
    resolver = AgentWorkspaceSnapshots(nas, state_dir=tmp_path / "private-prepared", max_input_bytes=1 if mutation == "budget" else 1024)
    if mutation not in {"valid", "empty"}:
        with pytest.raises((ValueError, OSError)):
            resolver.prepare(spec, target)
        assert not list((tmp_path / "private-prepared").glob("*.json"))
    else:
        resolver.prepare(spec, target)
        if files:
            copied = target / ".deer-flow/users" / spec["user_id"] / "threads" / spec["thread_id"] / "user-data/uploads/source.txt"
            assert copied.read_bytes() == data and copied.stat().st_ino != source.stat().st_ino
            copied.write_bytes(b"accepted-runtime-change")
            resolver.prepare(spec, target)
            assert copied.read_bytes() == b"accepted-runtime-change"
        else:
            resolver.prepare(spec, target)


def test_workspace_manifest_rejects_duplicate_paths_total_and_traversal():
    from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest

    row = {"category": "workspace", "path": "source.txt", "size": 1, "sha256": "a" * 64}
    base = {"user_id": "owner", "thread_id": "thread", "files": [row], "total_bytes": 1}
    for changes in ({"files": [row, row], "total_bytes": 2}, {"total_bytes": 2}, {"files": [{**row, "path": "../control"}]}):
        with pytest.raises(ValueError):
            AgentWorkspaceManifest.model_validate({**base, **changes})


@pytest.mark.parametrize("mutation", ["unknown", "name", "target", "skill", "mcp-not-declared"])
def test_launch_secret_references_require_actual_approved_target_binding(mutation):
    from deerflow_ecs_fleet.launch_spec import LaunchSpec

    from app.fleet.runner_context import validate_secret_bindings
    from deerflow.config.app_config import AppConfig

    from .test_c01_remote_agent_admission import wire

    raw = wire()
    raw["secret_refs"] = [{"name": "MODEL_KEY", "reference_id": "model-reference"}]
    private = AppConfig.model_validate(
        {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "models": [{"name": "parent", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent", "api_key": "private-provider-key"}]}
    )
    bundle = {"secret_bindings": {"model-reference": {"name": "MODEL_KEY", "kind": "model", "target": "parent"}}, "mcp_servers": {}}
    if mutation == "unknown":
        raw["secret_refs"][0]["reference_id"] = "not-approved"
    elif mutation == "name":
        raw["secret_refs"][0]["name"] = "DIFFERENT_ALIAS"
    elif mutation == "target":
        bundle["secret_bindings"]["model-reference"]["target"] = "not-a-model"
    elif mutation == "skill":
        bundle["secret_bindings"]["model-reference"]["kind"] = "skill"
    else:
        private.extensions = __import__("deerflow.config.extensions_config", fromlist=["ExtensionsConfig"]).ExtensionsConfig.model_validate(
            {"mcpServers": {"target": {"command": "/usr/bin/example", "env": {"TARGET_TOKEN": "private-target-key"}}}}
        )
    with pytest.raises(ValueError, match="secret|target"):
        validate_secret_bindings(private, LaunchSpec.model_validate(raw), bundle)


def test_secret_binding_allows_only_declared_actual_model_and_mcp_targets():
    from deerflow_ecs_fleet.launch_spec import LaunchSpec

    from app.fleet.runner_context import validate_secret_bindings
    from deerflow.config.app_config import AppConfig

    from .test_c01_remote_agent_admission import wire

    raw = wire()
    raw["secret_refs"] = [{"name": "MODEL_KEY", "reference_id": "model-reference"}, {"name": "TARGET_TOKEN", "reference_id": "mcp-reference"}]
    private = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "models": [{"name": "parent", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent", "api_key": "private-provider-key"}],
            "extensions": {"mcpServers": {"target": {"command": "/usr/bin/example", "env": {"TARGET_TOKEN": "private-target-key"}}}},
        }
    )
    bundle = {"secret_bindings": {"model-reference": {"name": "MODEL_KEY", "kind": "model", "target": "parent"}, "mcp-reference": {"name": "TARGET_TOKEN", "kind": "mcp", "target": "target"}}, "mcp_servers": {"target": {}}}
    assert validate_secret_bindings(private, LaunchSpec.model_validate(raw), bundle) == ({"parent"}, {"target"})


@pytest.mark.parametrize("transport", ["custom-header", "custom-env"])
def test_private_mcp_payload_needs_target_ref_independent_of_auth_field_spelling(transport):
    from deerflow_ecs_fleet.launch_spec import LaunchSpec

    from app.fleet.runner_context import validate_secret_bindings
    from deerflow.config.app_config import AppConfig

    from .test_c01_remote_agent_admission import wire

    raw = wire()
    raw["secret_refs"] = []
    server = {"type": "http", "url": "https://example.invalid/mcp", "headers": {"X-Client-Key": "private-target-key"}} if transport == "custom-header" else {"command": "/usr/bin/example", "env": {"ERP_AUTH": "private-target-key"}}
    private = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "extensions": {"mcpServers": {"target": server}}})
    bundle = {"secret_bindings": {"target-reference": {"name": "TARGET_KEY", "kind": "mcp", "target": "target"}}, "mcp_servers": {"target": {}}}
    with pytest.raises(ValueError, match="declared launch secret"):
        validate_secret_bindings(private, LaunchSpec.model_validate(raw), bundle)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["skill-version", "provider-code", "skill-metadata"])
async def test_actual_installed_runtime_digest_and_skill_metadata_detect_bundle_drift(tmp_path, mutation):
    import json
    import os
    from pathlib import Path

    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers
    from deerflow_ecs_fleet.worker.containers import DockerError

    image = os.environ.get("FLEET_AGENT_TEST_IMAGE")
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1" or not image:
        pytest.skip("requires actual installed C04 acceptance image")
    driver = AgentContainers(state_dir=tmp_path / "private", operator_config={})
    baseline = await driver.compatibility(image)
    fixtures = Path(__file__).parent / "fixtures/c04-runner"
    if mutation == "skill-version":
        bundle = json.loads((fixtures / "runtime-bundle.json").read_text())
        bundle["skills"][0]["version"] = "operator-binding-v2"
        changed = tmp_path / "runtime-bundle.json"
        changed.write_text(json.dumps(bundle))
        destination = "/opt/deerflow/runtime-bundle.json"
    elif mutation == "provider-code":
        changed = tmp_path / "provider.py"
        changed.write_bytes(Path(__file__).with_name("c04_worker_fixture.py").read_bytes() + b"\n# actual provider code changed\n")
        destination = "/usr/local/lib/python3.12/site-packages/fleet/c04_worker_fixture.py"
    else:
        changed = tmp_path / "SKILL.md"
        changed.write_text("---\nname: mismatched-metadata\ndescription: Actual skill drift\n---\nChanged metadata.\n")
        destination = "/opt/deerflow/skills/public/c04-enabled/SKILL.md"
    changed.chmod(0o644)
    arguments = (
        "run",
        "--rm",
        "--read-only",
        "--network",
        "none",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--mount",
        "type=bind,src=" + str(changed) + ",dst=" + destination + ",readonly",
        "--entrypoint",
        "python",
        image,
        "-c",
        "from deerflow_ecs_fleet.worker.agent_environment import worker_compatibility; print(worker_compatibility('gateway').model_dump_json())",
    )
    if mutation == "skill-metadata":
        with pytest.raises(DockerError):
            await driver.checked(*arguments, timeout=30)
    else:
        actual = json.loads(await driver.checked(*arguments, timeout=30))
        assert actual["runtime_digest"] != baseline["runtime_digest"]
        if mutation == "skill-version":
            assert actual["skill_snapshot"] != baseline["skill_snapshot"]


@pytest.mark.parametrize("mutation", ["fifo", "symlink", "hardlink"])
def test_private_prepared_marker_rejects_nonregular_or_linked_state(tmp_path, mutation):
    import os

    from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest, AgentWorkspaceSnapshots

    from .test_c01_remote_agent_admission import wire

    spec = wire()
    manifest = AgentWorkspaceManifest(user_id=spec["user_id"], thread_id=spec["thread_id"], files=[], total_bytes=0)
    spec["workspace_manifest_ref"] = manifest.reference
    nas = tmp_path / "nas"
    source = nas / ".fleet-agent-inputs" / spec["user_id"] / spec["thread_id"] / manifest.reference
    source.mkdir(parents=True)
    (source / "manifest.json").write_bytes(manifest.canonical_bytes())
    target = tmp_path / "attempt"
    target.mkdir()
    private = tmp_path / "private"
    resolver = AgentWorkspaceSnapshots(nas, state_dir=private, max_input_bytes=1024)
    resolver.prepare(spec, target)
    marker = next(private.glob("*.json"))
    if mutation == "hardlink":
        os.link(marker, tmp_path / "extra-link")
    else:
        marker.unlink()
        if mutation == "fifo":
            os.mkfifo(marker, 0o600)
        else:
            marker.symlink_to(source / "manifest.json")
    with pytest.raises((ValueError, OSError)):
        resolver.prepare(spec, target)


def test_scoped_model_auth_cannot_be_replaced_with_another_caller_auth_field():
    from deerflow.config.app_config import AppConfig
    from deerflow.models.credentials import model_credential_scope
    from deerflow.models.factory import create_chat_model

    cfg = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "models": [{"name": "private", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent"}]})
    with model_credential_scope(lambda name, use: {}):
        with pytest.raises(ValueError, match="cannot be overridden"):
            create_chat_model("private", app_config=cfg, attach_tracing=False, api_key="unapproved-caller-auth")


@pytest.mark.parametrize("legacy", [None, "shared", "memory", "sqlite", "other-dsn", "other-schema"])
def test_actual_runner_rejects_nonshared_effective_legacy_storage(legacy):
    from app.fleet.runtime import validate_remote_agent_shared_storage
    from deerflow.config.app_config import AppConfig

    cfg = {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "database": {"backend": "postgres", "postgres_url": "postgresql+asyncpg://runner:private@localhost:5432/control", "postgres_schema": "own_scope"}}
    if legacy in {"memory", "sqlite"}:
        cfg["checkpointer"] = {"type": legacy}
    elif legacy is not None:
        cfg["checkpointer"] = {
            "type": "postgres",
            "connection_string": "postgresql://runner:private@localhost:5432/" + ("other" if legacy == "other-dsn" else "control"),
            "postgres_schema": "different_scope" if legacy == "other-schema" else "own_scope",
        }
    private = AppConfig.model_validate(cfg)
    if legacy in {None, "shared"}:
        validate_remote_agent_shared_storage(private)
    else:
        with pytest.raises(RuntimeError, match="shared PostgreSQL|matching PostgreSQL"):
            validate_remote_agent_shared_storage(private)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", ["memory", "sqlite"])
async def test_bootstrap_refuses_nonshared_checkpoint_before_loading_model_or_plugins(legacy):
    from types import SimpleNamespace

    from app.fleet.runner_context import build_agent_environment

    private = {
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
        "database": {"backend": "postgres", "postgres_url": "postgresql://runner:private@localhost/control"},
        "agent_storage": {"backend": "db"},
        "checkpointer": {"type": legacy},
        "run_events": {"backend": "db"},
    }
    with pytest.raises(RuntimeError, match="shared PostgreSQL"):
        await build_agent_environment(bootstrap=SimpleNamespace(operator_config=private), spec=None, grant={})


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["alternate", "not-installed", "ambiguous", "unfit", "invalid", "noncallable"])
async def test_selected_installed_provider_preflight_without_gateway_or_harness(tmp_path, provider):
    import os

    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers
    from deerflow_ecs_fleet.worker.containers import DockerError

    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires built standalone provider acceptance image")
    driver = AgentContainers(state_dir=tmp_path / "private", operator_config={}, provider=provider)
    image = os.environ.get("FLEET_AGENT_PROVIDER_TEST_IMAGE", "deerflow-c04-provider:local")
    image = __import__("json").loads(await driver.checked("image", "inspect", image))[0]["Id"]
    assert (
        await driver.checked("run", "--rm", "--network", "none", "--entrypoint", "python", image, "-c", "import importlib.util; assert importlib.util.find_spec('app') is None; assert importlib.util.find_spec('deerflow') is None")
    ).strip() == ""
    if provider == "alternate":
        actual = await driver.compatibility(image)
        expected = await driver.checked("run", "--rm", "--network", "none", "--entrypoint", "python", image, "-c", "from deerflow_c04_alternate import compatibility; print(compatibility().model_dump_json())")
        assert actual == __import__("json").loads(expected)
    else:
        with pytest.raises(DockerError):
            await driver.compatibility(image)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["attach", "start"])
async def test_owned_executor_rejects_expiry_after_actual_unchanged_row_lock_wait(owner_environment, operation):
    import asyncio

    from deerflow.runtime import RunManager
    from deerflow.runtime.runs.manager import RunStartupError

    env = owner_environment
    engine, sf, user, _, app, record, session_id, _, _ = env
    accepted = await claim(env)
    await app.state.fleet_ownership.authorize_start(node_id="node-c03", node_session_id=session_id, attempt_id=accepted.attempt_id, token=accepted.token)
    manager = RunManager(store=app.state.run_store, worker_id=accepted.owner_worker_id)
    identity = dict(user_id=user.id, thread_id=record.thread_id, owner_worker_id=accepted.owner_worker_id, execution_backend="fleet")
    if operation == "start":
        await manager.attach_existing_executor(record.run_id, **identity)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '2 seconds'"))
        before = (await conn.execute(text("SELECT status,owner_worker_id,lease_expires_at,updated_at FROM runs"))).one()
    candidate = None
    try:
        async with sf.begin() as locker:
            locker_pid = (await locker.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            await locker.execute(text("SELECT run_id FROM runs WHERE run_id=:id FOR UPDATE"), {"id": record.run_id})
            candidate = asyncio.create_task(manager.attach_existing_executor(record.run_id, **identity) if operation == "attach" else manager.try_start(record.run_id))
            deadline = asyncio.get_running_loop().time() + 5
            async with engine.connect() as observer:
                while not (await observer.execute(text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid)))"), {"pid": locker_pid})).scalar_one():
                    assert not candidate.done(), "the executor must actually wait on the unchanged row lock"
                    assert asyncio.get_running_loop().time() < deadline
                    await asyncio.sleep(0.01)
                while not (await observer.execute(text("SELECT clock_timestamp()>lease_expires_at FROM runs"))).scalar_one():
                    assert asyncio.get_running_loop().time() < deadline
                    await asyncio.sleep(0.01)
                assert not candidate.done()
        if operation == "attach":
            with pytest.raises(RunStartupError, match="no longer owns"):
                await asyncio.wait_for(candidate, 5)
        else:
            assert (await asyncio.wait_for(candidate, 5)).value == "cancelled"
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT status,owner_worker_id,lease_expires_at,updated_at FROM runs"))).one() == before
    finally:
        if candidate is not None and not candidate.done():
            candidate.cancel()
            await asyncio.gather(candidate, return_exceptions=True)


@pytest.mark.parametrize("source", ["pg-encoded", "pg-query", "legacy-encoded", "legacy-libpq", "stream-encoded", "stream-query", "ownership-encoded", "public-username"])
@pytest.mark.parametrize("destination", ["env", "args"])
def test_mcp_process_rejects_actual_decoded_control_credentials(monkeypatch, source, destination):
    import sys

    from app.fleet import runner_context
    from deerflow.config.app_config import AppConfig

    secret = "control@credential"
    cfg = {
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
        "database": {"backend": "postgres", "postgres_url": "postgresql://public-user@localhost/control"},
        "extensions": {"mcpServers": {"target": {"command": sys.executable, "env": {"ERP_AUTH": "public-user" if source == "public-username" else secret}}}},
    }
    if source == "pg-encoded":
        cfg["database"]["postgres_url"] = "postgresql://public-user:control%40credential@localhost/control"
    elif source == "pg-query":
        cfg["database"]["postgres_url"] += "?password=control%40credential"
    elif source.startswith("legacy"):
        cfg["checkpointer"] = {
            "type": "postgres",
            "connection_string": "postgresql://public-user:control%40credential@localhost/control" if source == "legacy-encoded" else "host=localhost dbname=control user=public-user password=control@credential",
        }
    elif source.startswith("stream"):
        cfg["stream_bridge"] = {"type": "redis", "redis_url": "redis://public-user:control%40credential@localhost/0" if source == "stream-encoded" else "redis://localhost/0?password=control%40credential"}
    elif source == "ownership-encoded":
        cfg["sandbox"]["ownership"] = {"type": "redis", "redis_url": "redis://public-user:control%40credential@localhost/0"}
    if destination == "args":
        server = cfg["extensions"]["mcpServers"]["target"]
        server["args"] = [server["env"].pop("ERP_AUTH")]
    server = cfg["extensions"]["mcpServers"]["target"]
    bundle = {"skills": [], "plugins": [], "mcp_servers": {"target": {"transport": "stdio", "command": sys.executable, "args": server.get("args", []), "allowed_env_keys": list(server["env"])}}, "secret_bindings": {}}
    monkeypatch.setattr(runner_context, "runtime_bundle", lambda: (b"", bundle))
    private = AppConfig.model_validate(cfg)
    if source == "public-username":
        runner_context.validate_runtime_configuration(private)
    else:
        with pytest.raises(ValueError, match="Control credentials"):
            runner_context.validate_runtime_configuration(private)


@pytest.mark.parametrize("connection", ["postgresql://public-user:raw%40credential@localhost/control", "redis://public-user:raw%40credential@localhost/0"])
def test_control_parser_preserves_encoded_and_decoded_secret_forms(connection):
    from app.fleet.runner_context import control_connection_credentials
    from deerflow.config.app_config import AppConfig

    cfg = {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}
    if connection.startswith("postgresql"):
        cfg["database"] = {"backend": "postgres", "postgres_url": connection}
    else:
        cfg["stream_bridge"] = {"type": "redis", "redis_url": connection}
    values = control_connection_credentials(AppConfig.model_validate(cfg))
    assert {"raw%40credential", "raw@credential"} <= values
    assert "public-user" not in values


@pytest.mark.parametrize("connection", ["postgresql://public-user:invalid%ZZ@localhost/control", "redis://localhost:invalid/0"])
def test_control_parser_fails_closed_without_echoing_malformed_private_connection(connection):
    from app.fleet.runner_context import control_connection_credentials
    from deerflow.config.app_config import AppConfig

    cfg = {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}
    if connection.startswith("postgresql"):
        cfg["database"] = {"backend": "postgres", "postgres_url": connection}
    else:
        cfg["stream_bridge"] = {"type": "redis", "redis_url": connection}
    with pytest.raises(ValueError, match="Invalid private control") as error:
        control_connection_credentials(AppConfig.model_validate(cfg))
    assert connection not in str(error.value)
