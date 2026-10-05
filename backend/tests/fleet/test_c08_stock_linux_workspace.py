"""Required fresh installed original stock worker lifecycle, TCP/Node/PG/NAS."""

import asyncio
import json
import time

import httpx
import pytest
from sqlalchemy import text


async def _run_actual_installed_stock(tmp_path, mode, pause, *, sequence=None, fault=None):
    bootstrap_failure = interrupted = False
    memory_mode = "tool"
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

    from .c04_integration_fixture import control_database, node_server
    from .test_b02_fleet_foundation import service_class, settings
    from .test_c01_remote_agent_admission import c_config
    from .test_c02_remote_agent_admission import request

    image = os.environ.get("FLEET_AGENT_TEST_IMAGE")
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1" or not image:
        pytest.fail("required installed gate needs FLEET_TEST_CONTAINERS=1 and immutable FLEET_AGENT_TEST_IMAGE")
    async with control_database(tmp_path) as db:
        async with db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text("CREATE TABLE c06_extension(value text)"))
            for table in ("c06_memory", "c06_local_memory"):
                await conn.execute(text("CREATE TABLE " + table + "(user_id text NOT NULL,agent_name text NOT NULL,fact_id text NOT NULL,content text NOT NULL,PRIMARY KEY(user_id,agent_name,fact_id))"))
        private = {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider", "allow_host_bash": True},
            "database": {"backend": "postgres", "postgres_url": db.runner_url, "postgres_schema": db.schema, "checkpoint_channel_mode": mode, "checkpoint_delta": {"snapshot_frequency": 3}},
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
        driver = AgentContainers(state_dir=tmp_path / "agent-state", operator_config=private, provider="c08-stock")
        inspected_image = __import__("json").loads(await driver.checked("image", "inspect", image))[0]
        image = inspected_image["Id"]
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        if bootstrap_failure:
            driver.provider = "not-installed"
        (tmp_path / "execution-nas").mkdir()
        nas = settings(tmp_path / "execution-nas")
        config_data = c_config()
        config_data.update(nas_root=str(nas.nas_root), nas_identity=nas.nas_identity)
        config_data["profiles"]["remote"].update(image=image, runtime_digest=actual.runtime_digest, user=str(os.getuid()) + ":" + str(os.getgid()), network="bridge", pids_limit=256, execution_timeout_seconds=240)
        config = FleetConfig.model_validate(config_data)
        fleet = service_class()(config)
        await fleet.start(ExtensionRuntimeDeps(session_factory=db.session_factory))
        app = FastAPI()
        app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
        app.state.run_store = RunRepository(db.session_factory)
        app.state.run_manager = RunManager(store=app.state.run_store)
        app.state.thread_store = ThreadMetaRepository(db.session_factory)
        install_fleet_ownership(app, db.session_factory)
        app.add_middleware(AuthMiddleware)
        app.add_middleware(CSRFMiddleware)
        app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
        app.include_router(importlib.import_module("app.gateway.routers.artifacts").router)
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
            input={
                "messages": [
                    {
                        "role": "user",
                        "content": "Create deterministic parent and child artifacts c04-control-port="
                        + str(db.port)
                        + (" c08-stock-pause" if pause else "")
                        + (" c08-stock-failed" if fault == "failed" else "")
                        + (" c08-stock-partial-barrier" if fault in {"partial-read", "partial-kill"} else ""),
                    }
                ]
            },
            config={"recursion_limit": 1000, "context": {"model_name": "model-1", "subagent_enabled": True}},
            stream_mode=["values", "messages-tuple", "custom"],
            stream_subgraphs=True,
            interrupt_before=["tools"] if interrupted else None,
        )
        record = await services.start_run(body, "thread-c04", request(core, user), execution_backend=backend)
        await app.state.thread_store.create(record.thread_id, user_id=user.id)
        refs = []
        execution = None
        client = http_client = None
        stager = None
        lost, renewals, seals, recoveries, censuses = [], [], [], [], []
        evidence = Path(os.environ["C08_LINUX_EVIDENCE_DIR"]) / (mode + ("-pause" if pause else "-success") + ("-" + sequence if sequence else "") + ("-" + fault if fault else ""))
        evidence.mkdir(parents=True, exist_ok=False)
        try:
            async with node_server(app) as url:
                fault_transport = None
                if fault == "lost-reply":
                    from .c08_installed_faults import LostPreparedReply

                    fault_transport = LostPreparedReply(evidence)
                http_client = httpx.AsyncClient(base_url=url + "/", timeout=10, **({"transport": fault_transport} if fault_transport is not None else {}))
                client = NodeClient(gateway_url=url, credential=credential.token, claim_kind="agent", compatibility=actual.model_dump(mode="json"), http_client=http_client)

                from .c08_stock_diagnostics import NodeProtocolObservation

                NodeProtocolObservation(client, evidence)

                async def prepare(claim, grant):
                    return await driver.prepare_workspace(nas.nas_root, claim, grant)

                from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions
                from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication
                from deerflow_ecs_fleet.workspace import NASWorkspace

                original_seal, original_recover = AgentWorkspaceVersions.seal, AgentWorkspaceVersions.recover

                def observed_seal(versions, *args):
                    seals.append(time.monotonic())
                    return original_seal(versions, *args)

                def observed_recover(versions, *args):
                    recoveries.append(time.monotonic())
                    return original_recover(versions, *args)

                AgentWorkspaceVersions.seal, AgentWorkspaceVersions.recover = observed_seal, observed_recover
                original_census = driver.quiesce_workspace

                async def observed_census(*args, **kwargs):
                    observed = await original_census(*args, **kwargs)
                    observed["observed_monotonic"] = time.monotonic()
                    censuses.append(observed)
                    (evidence / "collector-censuses.json").write_text(json.dumps(censuses, indent=2))
                    if fault == "conflicting-claims" and not (evidence / "actual-concurrent-claim-conflict.json").exists():
                        from .c08_installed_faults import observe_conflicting_claims

                        await observe_conflicting_claims(client=client, http_client=http_client, credential=credential, daemon=daemon, request=kwargs["request"], evidence=evidence)
                    if fault == "preseal-kill":
                        from .c08_installed_faults import kill_before_second_seal

                        await kill_before_second_seal(driver=driver, grant=args[0], request=kwargs["request"], db=db, evidence=evidence, seals=seals)
                    return observed

                driver.quiesce_workspace = observed_census
                daemon = NodeDaemon(client=client, containers=driver, state_dir=tmp_path / "agent-state", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.25, poll_seconds=0.05)
                stager = AgentWorkspacePublication(client=client, containers=driver, nas=NASWorkspace(nas.nas_root, identity=nas.nas_identity), journal=daemon.journal)
                daemon.workspace_publications = stager
                await daemon.bootstrap()
                execution = asyncio.create_task(daemon.execute_one())
                rows = []
                deadline = time.monotonic() + 260
                while time.monotonic() < deadline:
                    refs = [ref for ref, _ in await driver.list_managed(node_id)]
                    if refs:
                        _, stdout, stderr = await driver.command("logs", refs[0])
                        (evidence / "runner.log").write_text((stdout + stderr).replace(db.password, "[private credential redacted]"))
                        rows = []
                        for line in stdout.splitlines():
                            try:
                                item = json.loads(line)
                            except ValueError:
                                continue
                            if isinstance(item, dict) and "event" in item:
                                rows.append(item)
                    if fault in {"partial-read", "partial-kill"} and any(row["event"] == "original-stock-first-partial-barrier" for row in rows) and not (evidence / "actual-first-partial-read.json").exists():
                        from .c08_installed_faults import observe_first_partial

                        await observe_first_partial(app=app, db=db, driver=driver, ref=refs[0], record=record, rows=rows, evidence=evidence, kill=fault == "partial-kill")
                    if execution.done():
                        result = await execution
                        break
                    await asyncio.sleep(0.05)
                else:
                    raise AssertionError("Original stock lifecycle deadline elapsed; inspect owned runner.log")
                assert len(refs) == 1
                observation = await driver.inspect(refs[0])
                if fault == "failed":
                    from .c08_installed_faults import observe_failed_stock

                    await stager.join_writers()
                    await observe_failed_stock(app=app, db=db, record=record, result=result, rows=rows, observation=observation, evidence=evidence)
                    return
                if fault in {"preseal-kill", "partial-kill"}:
                    from .c08_installed_faults import observe_restarts_after_kill

                    await stager.join_writers()
                    await observe_restarts_after_kill(app=app, db=db, driver=driver, daemon=daemon, client=client, http_client=http_client, credential=credential, actual=actual, record=record, result=result, rows=rows, evidence=evidence)
                    return
                if fault == "lost-reply":
                    assert len(fault_transport.observations) == 1
                assert not observation["State"]["Running"] and observation["State"]["ExitCode"] == 0
                (evidence / "docker-stopped.json").write_text(json.dumps({"Id": observation["Id"], "Image": observation["Image"], "State": observation["State"], "Config": {"User": observation["Config"]["User"]}}, indent=2))
                assert result["stop_reason"] == "exit" and not result["report_pending"]
                assert result["state"] == ("cancelled" if pause else "succeeded")
                await stager.join_writers()
                joined = next(row for row in rows if row["event"] == "original-writers-joined-before-publication-sf-begin")
                assert len(joined["ancestry"]) == len(joined["physically_absent"]) == 6
                assert joined["actual_pipe_threads_joined"] and not joined["budget_started"]
                assert all(code == 0 for code in joined["original_popen_returncodes"])
                closed = [row for row in rows if row["event"] == "original-stock-prepared-gate-closed"]
                accepted = [row for row in rows if row["event"] == "original-stock-partial-accepted-and-reopened"]
                assert len(closed) == len(accepted) == 2
                protocol = [row for row in rows if row["event"] == "original-stock-finite-protocol-counts"][-1]
                assert protocol["model_calls"] == {"parent": 10, "child": 3}
                assert protocol["tool_calls"] == {"parent": 10 if pause else 9, "child": 2}
                assert all(not row["budget_started"] for row in closed + accepted)
                assert len({row["manifest_id"] for row in accepted}) == len({row["checkpoint_id"] for row in accepted}) == 2
                assert next(row for row in rows if row["event"] == "original-mcp-reconnected-after-accepted-partial")["new_owner_distinct"]
                final = next(row for row in rows if row["event"] == "original-stock-final-prepared-gate-closed")
                assert final["kind"] == ("paused" if pause else "final")
                assert final["budget_started"] and 0 < final["remaining_seconds"] <= 120
                cleanup = next(row for row in rows if row["event"] == "original-stock-final-cleanup-deadline-preserved")
                assert cleanup["deadline_before"] == cleanup["deadline_after"] and 0 <= cleanup["remaining_seconds"] <= final["remaining_seconds"]
                committed = next(row for row in rows if row["event"] == "original-stock-terminal-committed-finishing")
                assert committed["both_pointers_equal"] and committed["task_state"] == committed["placement_state"] == "finishing"
                assert committed["checkpoint_id"] == final["checkpoint_id"]
                assert len(seals) == 3 and len(censuses) >= 6
                assert all(
                    item["container_id"] == observation["Id"]
                    and item["started_at"] == observation["State"]["StartedAt"]
                    and item["receipt"]["remaining_pids"] == sorted([1, item["receipt"]["collector"]["pid"]])
                    and item["receipt"]["collector"]["uid"] != item["receipt"]["runner"]["uid"]
                    for item in censuses
                )
                output = await prepare(daemon.journal.records()[0]["claim"], daemon.journal.records()[0]["grant"])
                assert next(Path(output).rglob("parent.txt")).read_bytes() == b"c04-artifact\naccepted-second-turn\n"
                assert next(Path(output).rglob("child.txt")).read_bytes() == b"c04-artifact\n"
                for role in ("parent", "child"):
                    probe = json.loads(next(Path(output).rglob(role + "-probe.json")).read_text())
                    assert probe["environ"] == probe["fd"] == probe["mem"] == probe["ptrace"] == "denied"
                    assert probe["raw_config_absent"] and probe["control_env_absent"]
                async with db.engine.connect() as conn:
                    points = [
                        dict(row)
                        for row in (
                            await conn.execute(text("SELECT id,request_id,checkpoint_id,manifest_id,kind,desired_core_status,desired_task_status,desired_placement_status FROM fleet_workspace_points ORDER BY accepted_at"))
                        ).mappings()
                    ]
                    assert [row["kind"] for row in points] == ["partial", "partial", "paused" if pause else "final"]
                    assert points[-1]["checkpoint_id"] == final["checkpoint_id"] and points[-1]["manifest_id"] == final["manifest_id"]
                    assert await conn.scalar(text("SELECT count(*) FROM fleet_workspace_requests WHERE state='accepted'")) == 3
                    assert await conn.scalar(text("SELECT count(*) FROM fleet_workspace_manifests")) == 3
                    assert await conn.scalar(text("SELECT status FROM runs WHERE run_id=:run"), {"run": record.run_id}) == ("interrupted" if pause else "success")
                    assert await conn.scalar(text("SELECT state FROM fleet_agent_tasks")) == ("input_required" if pause else "succeeded")
                    assert await conn.scalar(text("SELECT state FROM fleet_run_placements")) == ("cancelled" if pause else "succeeded")
                    assert await conn.scalar(text("SELECT state FROM fleet_attempts")) == ("cancelled" if pause else "succeeded")
                    assert await conn.scalar(text("SELECT state FROM fleet_reservations")) == "released"
                    assert await conn.scalar(text("SELECT accepted_workspace_point_id FROM fleet_agent_tasks")) == points[-1]["id"]
                    assert await conn.scalar(text("SELECT final_workspace_point_id FROM fleet_run_placements")) == points[-1]["id"]
                    assert await conn.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 1
                    assert await conn.scalar(text("SELECT core_status FROM fleet_stream_seals")) == ("interrupted" if pause else "success")
                    assert await conn.scalar(text("SELECT count(*) FROM fleet_workspace_processes WHERE state<>'settled'")) == 0
                    first_request = await conn.scalar(text("SELECT min(created_at) FROM fleet_workspace_requests"))
                    original_processes = [
                        dict(row) for row in (await conn.execute(text("SELECT pid,start_ticks,role,tool_execution_id,settled_at FROM fleet_workspace_processes WHERE settled_at<=:first ORDER BY pid"), {"first": first_request})).mappings()
                    ]
                    assert len(original_processes) >= 4
                    for entry in original_processes:
                        entry["settled_at"] = entry["settled_at"].isoformat()
                if fault == "partial-read":
                    from .c08_installed_faults import verify_first_partial_immutable

                    await verify_first_partial_immutable(app=app, record=record, evidence=evidence)
                assert censuses[0]["observed_monotonic"] < seals[0]
                assert len(driver.ready) == 1
                (evidence / "proof.json").write_text(
                    json.dumps(
                        {
                            "mode": mode,
                            "pause": pause,
                            "image_id": image,
                            "container_id": observation["Id"],
                            "started_at": observation["State"]["StartedAt"],
                            "actual_start_count": len(driver.ready),
                            "physical_stop_result": result,
                            "points": points,
                            "censuses": censuses,
                            "seal_count": len(seals),
                            "recover_count": len(recoveries),
                            "original_registered_processes_before_first_request": original_processes,
                            "runner_receipts": rows,
                        },
                        indent=2,
                    )
                )
                if sequence is not None:
                    from .c08_installed_sequence import run_sequence

                    await run_sequence(
                        kind=sequence,
                        app=app,
                        core=core,
                        user=user,
                        db=db,
                        native_private=native_private,
                        backend=backend,
                        driver=driver,
                        daemon=daemon,
                        stager=stager,
                        nas=nas,
                        original=record,
                        points=points,
                        evidence=evidence,
                        tmp_path=tmp_path,
                        mode=mode,
                    )
        finally:
            import sys

            from .c08_installed_cleanup import settle_owned_containers
            from .c08_stock_diagnostics import capture_precleanup

            original_error = sys.exception()

            async def discover_owned():
                return [ref for ref, _ in await driver.list_managed(node_id)]

            before = [("diagnostics", lambda: capture_precleanup(session_factory=db.session_factory, run_id=record.run_id, driver=driver, refs=refs, evidence=evidence))]
            after = []
            if stager is not None:
                after.append(("writers", stager.join_writers))
            if client is not None:
                after.append(("client", client.close))
            if http_client is not None:
                after.append(("http-client", http_client.aclose))
            if "original_seal" in locals():

                def restore_versions():
                    AgentWorkspaceVersions.seal, AgentWorkspaceVersions.recover = original_seal, original_recover

                after.append(("restore-versions", restore_versions))
            tail = [("user-context", lambda: reset_current_user(token)), ("app-config", reset_app_config), ("fleet", fleet.stop)]
            await settle_owned_containers(refs=refs, discover=discover_owned, driver=driver, execution=execution, before=before, after=after, tail=tail, original_error=original_error)


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("full", "delta"))
@pytest.mark.parametrize("pause", (False, True))
async def test_actual_installed_stock_pairs_two_presentations_and_terminal(tmp_path, mode, pause):
    await _run_actual_installed_stock(tmp_path, mode, pause)


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("full", "delta"))
@pytest.mark.parametrize("sequence", ("new-turn", "branch"))
async def test_actual_installed_stock_new_turn_and_branch_preserve_original_state(tmp_path, mode, sequence):
    await _run_actual_installed_stock(tmp_path, mode, False, sequence=sequence)


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("full", "delta"))
@pytest.mark.parametrize("sequence", ("manifest-delete", "digest-mutation"))
async def test_actual_installed_accepted_source_mutation_rejects_new_start(tmp_path, mode, sequence):
    await _run_actual_installed_stock(tmp_path, mode, False, sequence=sequence)


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("full", "delta"))
@pytest.mark.parametrize("fault", ("lost-reply", "preseal-kill", "conflicting-claims", "failed"))
async def test_actual_installed_stock_causal_publication_and_restart_faults(tmp_path, mode, fault):
    await _run_actual_installed_stock(tmp_path, mode, False, fault=fault)
