"""One actual installed keyed stock graph resume main; no core substitutions."""

import asyncio
import json
import time
from contextlib import AsyncExitStack
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text

from .test_c09_stock_linux_operations import control_database, owned_setup, state


@pytest.mark.live
@pytest.mark.asyncio
async def test_c09_public_keyed_resume_installed_main(tmp_path, monkeypatch):
    mode = "full"
    import importlib
    import os
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
            "models": [{"name": "model-1", "use": "fleet.c09_stock_linux_resume_fixture:ResumeModel", "model": "parent", "api_key": "c04-scripted-provider-credential"}],
            "tools": [{"name": "bash", "group": "bash", "use": "deerflow.sandbox.tools:bash_tool"}, {"name": "request_original_input", "group": "bash", "use": "fleet.c09_stock_linux_resume_fixture:request_original_input"}],
            "tool_groups": [{"name": "bash", "description": "Bounded original supervised shell"}],
            "extensions": {"skills": {"c04-enabled": {"enabled": True}, "c04-disabled": {"enabled": False}}, "mcpServers": {}},
        }
        # Host accessor uses the exact scripted provider/tool bytes, never a
        # replacement graph factory. The namespace is private to this fixture.
        host_modules = tmp_path / "host-fixture"
        (host_modules / "fleet").mkdir(parents=True)
        (host_modules / "fleet" / "__init__.py").write_text("")
        import shutil

        for name in ("c04_worker_fixture.py", "c08_installed_bytes.py", "c09_stock_linux_resume_fixture.py"):
            shutil.copyfile(Path(__file__).with_name(name), host_modules / "fleet" / name)
        monkeypatch.syspath_prepend(str(host_modules))
        # Trusted Gateway setup precedes remote readiness; remote saver never migrates.
        from deerflow.runtime.checkpointer.async_provider import make_checkpointer

        native_private = AppConfig.model_validate({**private, "database": {**private["database"], "postgres_url": db.host_url}})
        host_writer = await setup.enter_async_context(make_checkpointer(native_private))
        from deerflow.runtime.store.async_provider import make_store

        host_store = await setup.enter_async_context(make_store(native_private))
        public, _ = execution_configuration(AppConfig.model_validate(private))
        # Read-only host materialization receives the same bundled skill bytes
        # through an actual host path; containers keep their original /opt root.
        host_skills = host_modules / "skills"
        shutil.copytree(Path(__file__).parent / "fixtures/c04-runner/skills", host_skills)
        public = public.model_copy(update={"skills": public.skills.model_copy(update={"path": str(host_skills)})})
        provider_origin = Path(importlib.import_module("fleet.c09_stock_linux_resume_fixture").__file__)
        assert provider_origin.read_bytes() == Path(__file__).with_name("c09_stock_linux_resume_fixture.py").read_bytes()
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
        # Original service context uses actual PostgreSQL checkpoint/store;
        # no ephemeral fallback or agent-factory override is installed.
        from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
        from deerflow.runtime.events.store.db import DbRunEventStore
        from deerflow.runtime.stream_bridge import MemoryStreamBridge

        monkeypatch.setattr("app.gateway.deps._cached_repo", SQLiteUserRepository(db.session_factory))
        monkeypatch.setattr("app.gateway.deps._cached_local_provider", None)
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
                        "content": "Run the original parallel bash and graph interrupt; c04-control-port=" + str(db.port),
                    }
                ]
            },
            config={"recursion_limit": 1000, "context": {"model_name": "model-1", "subagent_enabled": False}},
            stream_mode=["values", "messages-tuple", "custom"],
            stream_subgraphs=True,
        )
        initial_request = request(core, user)
        initial_request.app = app
        record = await services.start_run(body, "thread-c04", initial_request, execution_backend=backend)
        await app.state.thread_store.create(record.thread_id, user_id=user.id)
        from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication
        from deerflow_ecs_fleet.workspace import NASWorkspace

        from app.gateway.internal_auth import create_internal_auth_headers

        from .c08_installed_cleanup import settle_owned_containers
        from .c08_stock_diagnostics import NodeProtocolObservation, capture_precleanup

        evidence = Path(os.environ["C09_LINUX_EVIDENCE_DIR"])
        refs, stops, censuses = [], [], []
        execution = None
        executions = []
        client = node_http = public_http = stager = None
        stagers = []
        daemon = None
        diagnostic_errors = []
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

        async def logs(ref=None):
            ref = ref or refs[-1]
            _, stdout, stderr = await driver.command("logs", ref, timeout=5)
            output = stdout + stderr
            for secret in (db.host_url, db.runner_url, db.password):
                if secret:
                    output = output.replace(secret, "[private database credential redacted]")
            (evidence / ("runner-" + ref + ".log")).write_text(output)
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
            stagers.append(stager)
            daemon.workspace_publications = stager
            await daemon.bootstrap()
            execution = asyncio.create_task(daemon.execute_one())
            executions.append(execution)

            async def finish_attempt(task):
                async with asyncio.timeout(60):
                    result = await asyncio.shield(task)
                assert result is not None and not result["report_pending"] and result["stop_reason"] == "exit"
                await stager.join_writers()
                rows = await asyncio.to_thread(daemon.journal.records)
                journal = next(row for row in rows if row["claim"]["attempt_id"] == result["attempt_id"])
                ref = journal["grant"]["process_ref"]
                if ref not in refs:
                    refs.append(ref)
                # Capture original failure data before any business assertion
                # or owned container removal/UUID database drop.
                try:
                    await logs(ref)
                except Exception as error:
                    diagnostic_errors.append(error)
                try:
                    async with db.session_factory() as session:
                        core_outcome = dict((await session.execute(text("SELECT run_id,status,error,stop_reason FROM runs WHERE run_id=:run"), {"run": journal["claim"]["run_id"]})).mappings().one())
                    diagnostic = json.dumps(core_outcome, indent=2, default=str)
                    for secret in (db.host_url, db.runner_url, db.password):
                        if secret:
                            diagnostic = diagnostic.replace(secret, "[private database credential redacted]")
                    (evidence / ("core-" + result["attempt_id"] + ".json")).write_text(diagnostic)
                except Exception as error:
                    diagnostic_errors.append(error)
                observed = await driver.inspect(ref)
                assert observed is not None and observed["Image"] == image
                physical = observed["State"]
                assert not physical["Running"] and physical["Pid"] == 0 and physical["ExitCode"] == 0
                assert journal["reported"] and journal["stop_reason"] == "exit" and journal["exit_code"] == 0
                assert not daemon._active
                return result, {"attempt_id": result["attempt_id"], "reported": journal["reported"], "stop_reason": journal["stop_reason"], "exit_code": journal["exit_code"], "server_state": journal["server_state"]}, observed

            first_result, first_journal, first_container = await finish_attempt(execution)
            paused = await state(db.session_factory, record.run_id)
            assert paused["status"] == "interrupted" and paused["task_state"] == "input_required"
            assert paused["stopped_at"] is not None and paused["reservation"] == "released"
            point_id = paused["accepted_workspace_point_id"]
            assert point_id and point_id == paused["final_workspace_point_id"]
            async with db.session_factory() as session:
                source = dict((await session.execute(text("SELECT p.*,r.state AS request_state FROM fleet_workspace_points p JOIN fleet_workspace_requests r ON r.id=p.request_id WHERE p.id=:point"), {"point": point_id})).mappings().one())
                original = dict((await session.execute(text("SELECT id,generation,deadline AS execution_deadline,continuation_budget FROM fleet_agent_tasks"))).mappings().one())
            assert source["kind"] == "paused" and source["request_state"] == "accepted" and source["checkpoint_id"]
            from starlette.requests import Request

            from deerflow.runtime.checkpoint_state import CheckpointStateAccessor

            host_request = Request({"type": "http", "app": app, "headers": []})
            accessor, read_config = await services.build_thread_checkpoint_state_accessor(host_request, thread_id=record.thread_id, checkpoint_id=source["checkpoint_id"], fail_closed=True)
            assert isinstance(accessor, CheckpointStateAccessor), "Original stock graph readiness failed; raw accessor fallback is not proof"
            snapshot = await accessor.aget(read_config)
            assert snapshot.interrupts and len(snapshot.interrupts) == 1 and snapshot.next
            interrupt_id = snapshot.interrupts[0].id
            from langchain_core.messages import ToolMessage

            # Materialized pending writes must include the actual completed
            # parallel bash result even while the original input tool paused.
            raw = await host_writer.aget_tuple(snapshot.config)

            def tool_messages(value):
                if isinstance(value, ToolMessage):
                    yield value
                elif isinstance(value, dict):
                    for child in value.values():
                        yield from tool_messages(child)
                elif isinstance(value, (list, tuple)):
                    for child in value:
                        yield from tool_messages(child)

            pending_tools = [message for _, _, value in raw.pending_writes for message in tool_messages(value)]
            assert any(message.name == "bash" and message.tool_call_id == "c09-original-bash-once" for message in pending_tools)
            artifact_path = f"/api/threads/{record.thread_id}/artifacts/mnt/user-data/outputs/c09-sideeffects.jsonl"
            first_receipt = await public_http.get(artifact_path, params={"workspace_point_id": point_id})
            assert first_receipt.status_code == 200, first_receipt.text
            sideeffects = [json.loads(line) for line in first_receipt.content.splitlines()]
            assert len(sideeffects) == 1 and len(sideeffects[0]["process_chain"]) >= 3
            assert len(executions) == 1 and execution.done()  # no second writer before durable STOP
            resume_response = await public_http.post(f"/api/threads/{record.thread_id}/runs", json={"command": {"resume": {interrupt_id: "approved"}}, "stream_mode": ["values", "messages-tuple", "custom"], "stream_subgraphs": True})
            assert resume_response.status_code == 200, resume_response.text
            new_run_id = resume_response.json()["run_id"]
            assert new_run_id != record.run_id
            async with db.session_factory() as session:
                resumed = dict(
                    (
                        await session.execute(
                            text(
                                "SELECT t.id,t.generation,t.current_run_id,t.deadline AS execution_deadline,t.continuation_budget,l.payload FROM fleet_agent_tasks t "
                                "JOIN fleet_run_placements p ON p.run_id=t.current_run_id JOIN fleet_launch_specs l ON l.id=p.launch_spec_ref"
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
            assert resumed["id"] == original["id"] and resumed["generation"] == original["generation"] + 1 and resumed["current_run_id"] == new_run_id
            assert resumed["execution_deadline"] == original["execution_deadline"] and resumed["continuation_budget"] == original["continuation_budget"]
            assert resumed["payload"]["source_workspace_point_id"] == point_id and resumed["payload"]["source_workspace_checkpoint_id"] == source["checkpoint_id"]
            # join_writers closes admission on the first publication owner;
            # the second original owner uses the same durable daemon journal.
            stager = AgentWorkspacePublication(client=client, containers=driver, nas=NASWorkspace(nas.nas_root, identity=nas.nas_identity), journal=daemon.journal)
            stagers.append(stager)
            daemon.workspace_publications = stager
            execution = asyncio.create_task(daemon.execute_one())
            executions.append(execution)
            second_result, second_journal, second_container = await finish_attempt(execution)
            final = await state(db.session_factory, new_run_id)
            assert final["status"] == "success" and final["task_state"] == "succeeded"
            assert final["stopped_at"] is not None and final["reservation"] == "released"
            final_point = final["accepted_workspace_point_id"]
            assert final_point and final_point == final["final_workspace_point_id"] and final_point != point_id
            final_receipt = await public_http.get(artifact_path, params={"workspace_point_id": final_point})
            assert final_receipt.status_code == 200, final_receipt.text
            assert final_receipt.content == first_receipt.content
            assert len([json.loads(line) for line in final_receipt.content.splitlines()]) == 1
            final_accessor, final_config = await services.build_thread_checkpoint_state_accessor(host_request, thread_id=record.thread_id, fail_closed=True)
            assert isinstance(final_accessor, CheckpointStateAccessor)
            final_snapshot = await final_accessor.aget(final_config)
            assert not final_snapshot.next and not final_snapshot.interrupts
            assert any(isinstance(message, ToolMessage) and message.name == "request_original_input" and message.content == "approved" for message in final_snapshot.values["messages"])
            rows = await logs()
            assert any(row["event"] == "c09-original-resumed-model-complete" for row in rows)
            assert first_container["Id"] != second_container["Id"]
            assert len([row for row in protocol.rows if row["event"] == "original_client_call" and row["operation"].endswith("stopped")]) == 2
            (evidence / "actual-resume-main.json").write_text(
                json.dumps(
                    {
                        "scope": "Original public keyed create, stock lead/ToolNode, actual Node/Docker STOP and durable journal ACK; stream route not executed",
                        "image": image,
                        "original_run_id": record.run_id,
                        "new_run_id": new_run_id,
                        "source_point_id": point_id,
                        "source_checkpoint_id": source["checkpoint_id"],
                        "keyed_interrupt_id": interrupt_id,
                        "original_task": original,
                        "resumed_task": resumed,
                        "paused": paused,
                        "final": final,
                        "actual_sideeffects": sideeffects,
                        "first_container": {"Id": first_container["Id"], "State": first_container["State"]},
                        "second_container": {"Id": second_container["Id"], "State": second_container["State"]},
                        "journals": [first_journal, second_journal],
                        "original_results": [first_result, second_result],
                        "physical_stops": stops,
                        "censuses": censuses,
                        "node_protocol": protocol.rows,
                        "runner_receipts": rows,
                    },
                    indent=2,
                    default=str,
                )
            )
        finally:
            import sys

            primary = sys.exception()

            async def discover_owned():
                return [ref for ref, _ in await driver.list_managed(node_id)]

            async def finish_executions():
                from .c08_installed_cleanup import settle_owned_execution

                for task in executions:
                    if task.done() and not task.cancelled() and task.exception() is primary:
                        continue
                    await settle_owned_execution(task)

            async def cleanup():
                before = [("runner-log-" + ref, lambda ref=ref: logs(ref)) for ref in refs]
                before.append(("diagnostics", lambda: capture_precleanup(session_factory=db.session_factory, run_id=record.run_id, driver=driver, refs=refs, evidence=evidence)))

                async def owner_gate():
                    # The audited helper reports a bounded settlement failure;
                    # a still-live owner must retain HTTP/saver/PG scope. This
                    # gate does not alter daemon/runner cancellation or STOP.
                    while any(not task.done() for task in executions):
                        pending = [task for task in executions if not task.done()]
                        (evidence / "retained-owner-pending.json").write_text(json.dumps({"pending_executions": len(pending), "scope_retained": True}))
                        await asyncio.wait(pending, timeout=30)
                    for owner in stagers:
                        await owner.join_writers()

                after = [("execution-owners", finish_executions), ("retained-owner-gate", owner_gate)]
                for name, owner in (("node-client", client), ("node-http", node_http), ("public-http", public_http)):
                    if owner is not None:
                        after.append((name, owner.close if name == "node-client" else owner.aclose))
                after.append(("tcp-server", server.aclose))

                async def diagnostic_failures():
                    if diagnostic_errors:
                        raise ExceptionGroup("Original installed diagnostic capture failures", diagnostic_errors)

                try:
                    await settle_owned_containers(
                        refs=refs,
                        discover=discover_owned,
                        driver=driver,
                        execution=execution if execution is not None and not (execution.done() and not execution.cancelled() and execution.exception() is primary) else None,
                        before=before,
                        after=after,
                        tail=[("diagnostic-failures", diagnostic_failures)],
                        original_error=primary,
                    )
                finally:
                    # All original owners remain inside setup/PG scope until
                    # retained cleanup settles, including repeated cancellation.
                    remaining = await driver.list_managed(node_id)
                    proof = {
                        "executions_done": all(task.done() for task in executions),
                        "daemon_active": None if daemon is None else len(daemon._active),
                        "clients_closed": all(owner is None or owner.is_closed for owner in (node_http, public_http)),
                        "remaining_owned_containers": [ref for ref, _ in remaining],
                        "stager_pending": sum(len(owner.pending_copies) + len(owner.pending_saves) for owner in stagers),
                        "driver_attachments": len(driver._attachments),
                        "driver_drains": len(driver._drains),
                    }
                    (evidence / "owned-handles.json").write_text(json.dumps(proof, indent=2))

            retained = asyncio.create_task(cleanup())
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
                    raise BaseExceptionGroup("Original installed main and retained cleanup", [primary, cleanup_error])
                raise
            if primary is None and cancellations:
                raise cancellations[0]
