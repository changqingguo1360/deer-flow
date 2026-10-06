"""Single installed original stock lead interrupt, real TCP/Node/Docker/PG."""

import asyncio
import json
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text


@asynccontextmanager
async def owned_setup():
    import sys

    from .c08_installed_cleanup import settle_owned_cleanup

    stack = AsyncExitStack()
    try:
        yield stack
    finally:
        await settle_owned_cleanup([("setup-owners", stack.aclose)], original_error=sys.exception())


@asynccontextmanager
async def control_database(tmp_path):
    import os
    import uuid

    import psycopg
    from psycopg import sql
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from .c04_integration_fixture import ControlDatabase

    base = make_url(os.environ["DEERFLOW_TEST_POSTGRES_URL"])
    admin = base.set(drivername="postgresql").render_as_string(hide_password=False)
    name = "c09_runtime_" + uuid.uuid4().hex
    proof = {"owned_database": name, "created": False, "dropped": False}
    evidence = Path(os.environ["C09_LINUX_EVIDENCE_DIR"])
    evidence.mkdir(parents=True, exist_ok=True)
    engine = None

    def create_or_drop(operation):
        with psycopg.connect(admin, autocommit=True, connect_timeout=5) as conn:
            conn.execute(sql.SQL(operation + " DATABASE {}").format(sql.Identifier(name)))

    try:
        await asyncio.to_thread(create_or_drop, "CREATE")
        proof["created"] = True
        owned = base.set(database=name, drivername="postgresql+asyncpg")
        host_url = owned.render_as_string(hide_password=False)
        runner_url = owned.set(host="host.docker.internal").render_as_string(hide_password=False)
        engine = create_async_engine(host_url)
        yield ControlDatabase(engine, async_sessionmaker(engine, expire_on_commit=False), "public", runner_url, host_url, base.password or "", base.port or 5432)
    finally:
        import sys

        from .c08_installed_cleanup import settle_owned_cleanup

        async def drop():
            await asyncio.to_thread(create_or_drop, "DROP")
            proof["dropped"] = True

        actions = []
        if engine is not None:
            actions.append(("owned-engine", engine.dispose))
        if proof["created"]:
            actions.append(("owned-database", drop))
        try:
            await settle_owned_cleanup(actions, original_error=sys.exception())
        finally:
            (evidence / "owned-database.json").write_text(json.dumps(proof, indent=2))


async def state(sf, run):
    async with sf() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT r.status,r.cancel_action,r.cancel_requested_at,t.cancel_requested_at AS task_cancel,t.generation,p.active_attempt_id,a.stopped_at,res.state AS reservation,t.state AS "
                        "task_state,p.state AS placement_state,t.accepted_workspace_point_id,p.final_workspace_point_id FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks "
                        "t ON t.id=p.agent_task_id LEFT JOIN fleet_attempts a ON a.id=p.active_attempt_id LEFT JOIN fleet_reservations res ON res.attempt_id=a.id WHERE r.run_id=:run"
                    ),
                    {"run": run},
                )
            )
            .mappings()
            .one()
        )
        return dict(row)


@pytest.mark.live
@pytest.mark.asyncio
async def test_c09_rollback_installed_actual_main(tmp_path):
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
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider", "allow_host_bash": True},
            "database": {"backend": "postgres", "postgres_url": db.runner_url, "postgres_schema": db.schema, "checkpoint_channel_mode": mode},
            "run_events": {"backend": "db"},
            "agent_storage": {"backend": "db"},
            "plugins": [],
            "memory": {"enabled": False, "mode": "tool", "manager_class": "deerflow_c04_fixture.memory:PostgresMemory"},
            "mcp_tasks": {"enabled": False},
            "title": {"enabled": False},
            "summarization": {"enabled": False},
            "models": [{"name": "model-1", "use": "fleet.c09_stock_linux_fixture:BarrierModel", "model": "parent", "api_key": "c04-scripted-provider-credential"}],
            "tools": [{"name": "bash", "group": "bash", "use": "deerflow.sandbox.tools:bash_tool"}],
            "tool_groups": [{"name": "bash", "description": "Bounded original supervised shell"}],
            "extensions": {"skills": {"c04-enabled": {"enabled": True}, "c04-disabled": {"enabled": False}}, "mcpServers": {}},
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
        setup.callback(reset_app_config)
        user = SimpleNamespace(id="user-c09", system_role="admin")
        token = set_current_user(user)
        setup.callback(reset_current_user, token)
        driver = AgentContainers(state_dir=tmp_path / "agent-state", operator_config=private, provider="gateway")
        inspected_image = __import__("json").loads(await driver.checked("image", "inspect", image))[0]
        image = inspected_image["Id"]
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        (tmp_path / "execution-nas").mkdir()
        nas = settings(tmp_path / "execution-nas")
        config_data = c_config()
        config_data.update(nas_root=str(nas.nas_root), nas_identity=nas.nas_identity)
        config_data["profiles"]["remote"].update(image=image, runtime_digest=actual.runtime_digest, user=str(os.getuid()) + ":" + str(os.getgid()), network="bridge", pids_limit=256, execution_timeout_seconds=240)
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
        app.add_middleware(AuthMiddleware)
        app.add_middleware(CSRFMiddleware)
        app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
        app.include_router(importlib.import_module("app.gateway.routers.artifacts").router)
        app.include_router(importlib.import_module("app.gateway.routers.thread_runs").router)
        node_id = "node-c09-" + __import__("uuid").uuid4().hex
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
            config={"recursion_limit": 1000, "context": {"model_name": "model-1", "subagent_enabled": False}},
            stream_mode=["values", "messages-tuple", "custom"],
            stream_subgraphs=True,
        )
        record = await services.start_run(body, "thread-c04", request(core, user), execution_backend=backend)
        await app.state.thread_store.create(record.thread_id, user_id=user.id)
        # A real trusted pre-run checkpoint, through the original graph/saver.
        from langchain_core.messages import HumanMessage

        from deerflow.runtime.checkpoint_state import CheckpointStateAccessor, build_state_mutation_graph

        async with make_checkpointer(native_private) as seed_saver:
            seed_graph = build_state_mutation_graph("rollback_seed", mode)
            seed_accessor = CheckpointStateAccessor.bind(seed_graph, seed_saver, mode=mode)
            seed_config = await seed_accessor.aupdate(
                {"configurable": {"thread_id": record.thread_id}}, {"messages": [HumanMessage(content="Retain the earlier conversation", id="c09-before-run")], "title": "Pre-run title"}, as_node="rollback_seed"
            )
            seeded = await seed_accessor.aget(seed_config)
            assert seeded.config["configurable"]["checkpoint_id"] and not seeded.next

        from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication
        from deerflow_ecs_fleet.workspace import NASWorkspace

        from app.gateway.internal_auth import create_internal_auth_headers

        from .c08_installed_cleanup import cancel_owned_task, settle_owned_containers
        from .c08_stock_diagnostics import NodeProtocolObservation, capture_precleanup

        evidence = Path(os.environ["C09_LINUX_EVIDENCE_DIR"])
        refs, stops, censuses = [], [], []
        execution = wait_request = None
        client = node_http = public_http = stager = None
        server = AsyncExitStack()
        original_stop, original_census = driver.stop, driver.quiesce_workspace

        async def observed_stop(ref):
            result = await original_stop(ref)
            observed = await driver.inspect(ref)
            stops.append({"ref": ref, "physical_result": result, "host_monotonic": time.monotonic(), "state": observed["State"] if observed else None})
            return result

        async def observed_census(*args, **kwargs):
            observed = await original_census(*args, **kwargs)
            censuses.append({"host_monotonic": time.monotonic(), "actual": observed})
            return observed

        driver.stop, driver.quiesce_workspace = observed_stop, observed_census

        async def logs():
            _, stdout, stderr = await driver.command("logs", refs[0], timeout=5)
            output = stdout + stderr
            for secret in (db.host_url, db.runner_url, db.password):
                if secret:
                    output = output.replace(secret, "[private database credential redacted]")
            (evidence / "runner.log").write_text(output)
            rows = []
            for line in stdout.splitlines():
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if isinstance(value, dict) and "event" in value:
                    rows.append(value)
            return rows

        try:
            url = await server.enter_async_context(node_server(app))
            node_http = httpx.AsyncClient(base_url=url + "/", timeout=10)
            client = NodeClient(gateway_url=url, credential=credential.token, claim_kind="agent", compatibility=actual.model_dump(mode="json"), http_client=node_http)
            protocol = NodeProtocolObservation(client, evidence)
            public_http = httpx.AsyncClient(base_url=url, headers={**create_internal_auth_headers(owner_user_id=user.id), "X-CSRF-Token": "c09-main-csrf"}, cookies={"csrf_token": "c09-main-csrf"}, timeout=150)

            async def prepare(claim, grant):
                return await driver.prepare_workspace(nas.nas_root, claim, grant)

            daemon = NodeDaemon(client=client, containers=driver, state_dir=tmp_path / "agent-state", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.25, poll_seconds=0.05)
            stager = AgentWorkspacePublication(client=client, containers=driver, nas=NASWorkspace(nas.nas_root, identity=nas.nas_identity), journal=daemon.journal)
            daemon.workspace_publications = stager
            await daemon.bootstrap()
            execution = asyncio.create_task(daemon.execute_one())
            barrier = None
            async with asyncio.timeout(45):
                while barrier is None:
                    refs[:] = [ref for ref, _ in await driver.list_managed(node_id)]
                    if refs:
                        code, stdout, _ = await driver.command("exec", refs[0], "cat", "/tmp/c09-shell-barrier.json", timeout=3)
                        if code == 0:
                            barrier = json.loads(stdout)
                            break
                        await logs()
                    if execution.done():
                        await execution
                        raise AssertionError("Actual runner exited before supervised shell barrier")
                    await asyncio.sleep(0.05)
            assert len(refs) == 1 and len(barrier["process_chain"]) >= 3
            assert (await driver.inspect(refs[0]))["State"]["Running"]
            # The running graph has changed checkpoint state and current files.
            async with make_checkpointer(native_private) as current_saver:
                running = await CheckpointStateAccessor.bind(seed_graph, current_saver, mode=mode).aget({"configurable": {"thread_id": record.thread_id}})
                assert len(running.values["messages"]) > len(seeded.values["messages"])
            code, contents, _ = await driver.command(
                "exec", refs[0], "python", "-c", "from deerflow.config.paths import get_paths; import sys; sys.stdout.write(get_paths().sandbox_outputs_dir('thread-c04', user_id='user-c09').joinpath('parent.txt').read_text())", timeout=3
            )
            assert code == 0 and contents == "c04-artifact\n"
            before = await state(db.session_factory, record.run_id)
            assert before["cancel_action"] is None and before["stopped_at"] is None
            cancel_path = f"/api/threads/{record.thread_id}/runs/{record.run_id}/cancel"
            cancelled_at = time.monotonic()
            response = await public_http.post(cancel_path + "?wait=false&action=rollback")
            assert response.status_code == 202, response.text
            intent = await state(db.session_factory, record.run_id)
            assert intent["cancel_action"] == "rollback" and intent["cancel_requested_at"] is not None
            assert intent["generation"] == before["generation"] and intent["active_attempt_id"] == before["active_attempt_id"]
            assert intent["task_cancel"] is None and intent["reservation"] in {"reserved", "active"} and intent["stopped_at"] is None
            wait_request = asyncio.create_task(public_http.post(cancel_path + "?wait=true&action=rollback"))
            await asyncio.sleep(0.01)
            if (await state(db.session_factory, record.run_id))["stopped_at"] is None:
                assert not wait_request.done(), "HTTP204 escaped before original physical STOP acknowledgement"
            async with asyncio.timeout(130):
                result = await asyncio.shield(execution)
                waited = await asyncio.shield(wait_request)
            assert waited.status_code == 204, waited.text
            elapsed = time.monotonic() - cancelled_at
            assert elapsed < barrier["bounded_tool_seconds"], "Tool ended naturally instead of original cancellation"
            await stager.join_writers()
            observation = await driver.inspect(refs[0])
            assert not observation["State"]["Running"] and observation["State"]["ExitCode"] == 0
            assert observation["Image"] == image and result["stop_reason"] == "exit" and result["state"] == "cancelled" and not result["report_pending"]
            final = await state(db.session_factory, record.run_id)
            assert final["status"] == "error" and final["task_state"] == "paused" and final["placement_state"] == "cancelled"
            assert final["stopped_at"] is not None and final["reservation"] == "released"
            artifact = await public_http.get(f"/api/threads/{record.thread_id}/artifacts/mnt/user-data/outputs/parent.txt")
            assert artifact.status_code == 200 and artifact.content == b"c04-artifact\n"
            assert final["generation"] == before["generation"] and final["active_attempt_id"] == before["active_attempt_id"]
            assert final["accepted_workspace_point_id"] == final["final_workspace_point_id"] and final["accepted_workspace_point_id"] is not None
            async with db.session_factory() as session:
                points = [
                    dict(row)
                    for row in (
                        await session.execute(
                            text(
                                "SELECT p.id,p.request_id,p.kind,p.checkpoint_id,p.manifest_id,p.desired_core_status,p.desired_task_status,r.state FROM fleet_workspace_points p JOIN fleet_workspace_requests r ON "
                                "r.id=p.request_id WHERE p.run_id=:run"
                            ),
                            {"run": record.run_id},
                        )
                    ).mappings()
                ]
                processes = [dict(row) for row in (await session.execute(text("SELECT pid,start_ticks,role,state,settled_at FROM fleet_workspace_processes WHERE run_id=:run"), {"run": record.run_id})).mappings()]
                assert await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE attempt_id=:attempt AND state='released' AND released_at IS NOT NULL"), {"attempt": before["active_attempt_id"]}) == 1
            assert len(points) == 1 and points[0]["kind"] == "paused" and points[0]["state"] == "accepted" and points[0]["desired_core_status"] == "error" and points[0]["desired_task_status"] == "paused"
            assert processes and all(row["state"] == "settled" and row["settled_at"] is not None for row in processes)
            assert stops and all(row["physical_result"] and row["state"] is not None and not row["state"]["Running"] for row in stops)
            stopped_calls = [row for row in protocol.rows if row["event"] == "original_client_call" and row["operation"].endswith("stopped")]
            assert len(stopped_calls) == 1 and stops[0]["host_monotonic"] <= stopped_calls[0]["host_monotonic_started"]
            async with make_checkpointer(native_private) as restored_saver:
                restored = await CheckpointStateAccessor.bind(seed_graph, restored_saver, mode=mode).aget({"configurable": {"thread_id": record.thread_id}})
                assert [message.id for message in restored.values["messages"]] == ["c09-before-run"]
                assert restored.values["title"] == "Pre-run title" and not restored.next
                assert restored.config["configurable"]["checkpoint_id"] == points[0]["checkpoint_id"]
            async with db.session_factory() as session:
                assert await session.scalar(text("SELECT error FROM runs WHERE run_id=:run"), {"run": record.run_id}) == "Rolled back by user"
            rows = await logs()
            assert any(row["event"] == "original-installed-complete-wheel-bytes" for row in rows)
            assert any(row["event"] == "c09-original-installed-origins" for row in rows)
            assert execution.done() and wait_request.done() and not daemon._active
            (evidence / "actual-main.json").write_text(
                json.dumps(
                    {
                        "scope": "Original gateway provider/stock lead/bash, TCP public cancel and NodeDaemon/physical Docker STOP; no MCP/plugin/subagent execution claimed",
                        "image": image,
                        "container": {"Id": observation["Id"], "State": observation["State"]},
                        "compatibility": actual.model_dump(mode="json"),
                        "barrier": barrier,
                        "before": before,
                        "intent": intent,
                        "final": final,
                        "points": points,
                        "registered_processes": processes,
                        "physical_stop_observations": stops,
                        "collector_censuses": censuses,
                        "node_protocol": protocol.rows,
                        "original_stop_result": result,
                        "rollback_elapsed_seconds": elapsed,
                        "pre_run_checkpoint": seed_config,
                        "restored_checkpoint": restored.config,
                        "current_file_contents": contents,
                        "runner_receipts": rows,
                        "owned_handles_before_cleanup": {"execution_done": execution.done(), "wait_done": wait_request.done(), "daemon_active": len(daemon._active)},
                    },
                    indent=2,
                    default=str,
                )
            )
        finally:
            import sys

            original_error = sys.exception()

            async def discover_owned():
                return [ref for ref, _ in await driver.list_managed(node_id)]

            before_cleanup = [("diagnostics", lambda: capture_precleanup(session_factory=db.session_factory, run_id=record.run_id, driver=driver, refs=refs, evidence=evidence))]
            if refs:
                before_cleanup.append(("runner-log", logs))
            if wait_request is not None:
                before_cleanup.append(("wait-request", lambda: cancel_owned_task(wait_request)))
            after = []
            if stager is not None:
                after.append(("stager-writers", stager.join_writers))
            for name, owner in (("node-client", client), ("node-http", node_http), ("public-http", public_http)):
                if owner is not None:
                    after.append((name, owner.close if name == "node-client" else owner.aclose))
            after.append(("tcp-server", server.aclose))
            tail = []
            cleanup_completed = False
            completed_primary = execution is not None and execution.done() and not execution.cancelled() and execution.exception() is original_error
            try:
                # The original failed task was already awaited. Retrieve no
                # second copy of the same primary as a fake cleanup failure.
                await settle_owned_containers(refs=refs, discover=discover_owned, driver=driver, execution=None if completed_primary else execution, before=before_cleanup, after=after, tail=tail, original_error=original_error)
                cleanup_completed = True
            finally:
                remaining = await driver.list_managed(node_id)
                handles = {
                    "execution_done": execution is None or execution.done(),
                    "primary_failed_execution_already_observed": completed_primary,
                    "wait_done": wait_request is None or wait_request.done(),
                    "clients_closed": all(owner is None or owner.is_closed for owner in (node_http, public_http)),
                    "remaining_owned_containers": [ref for ref, _ in remaining],
                    "stager_pending": 0 if stager is None else len(stager.pending_copies) + len(stager.pending_saves),
                    "driver_attachments": len(driver._attachments),
                    "driver_drains": len(driver._drains),
                    "scoped_cleanup_completed": cleanup_completed,
                }
                (evidence / "owned-handles.json").write_text(json.dumps(handles, indent=2))
