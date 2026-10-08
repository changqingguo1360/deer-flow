"""Installed full original C04 graph plus real daemon/HTTP/PG/Node census/NAS."""

import asyncio
import json
import sys
import time

import httpx
import pytest
from sqlalchemy import text


async def _settle_staged_linux_workspace(*, refs, node_id, driver, execution, stager, client, http_client, versions, original_seal, original_recover, reset_current_user, token, reset_app_config, fleet, original_error):
    from .c08_installed_cleanup import settle_owned_cleanup, settle_owned_execution

    errors = []

    async def discover():
        try:
            managed = await driver.list_managed(node_id)
            refs.extend(ref for ref, _ in managed if ref not in refs)
        except BaseException as error:
            errors.append(error)

    async def settle(actions):
        try:
            await settle_owned_cleanup(actions)
        except BaseExceptionGroup as group:
            errors.extend(group.exceptions)

    await discover()
    initially_known = set(refs)
    actions = [("stop " + ref, lambda ref=ref: driver.stop(ref)) for ref in refs]
    if execution is not None:
        actions.append(("execution", lambda: settle_owned_execution(execution)))
    await settle(actions)
    # Launch may create an owned container after the first discovery. The
    # original execution is now settled or explicitly failed its bounded join.
    await discover()
    actions = [("stop late " + ref, lambda ref=ref: driver.stop(ref)) for ref in refs if ref not in initially_known]
    if stager is not None:
        actions.append(("writers", stager.join_writers))
    if client is not None:
        actions.append(("client", client.close))
    if http_client is not None:
        actions.append(("http", http_client.aclose))
    if original_seal is not None:
        actions.extend([("restore seal", lambda: setattr(versions, "seal", original_seal)), ("restore recover", lambda: setattr(versions, "recover", original_recover))])
    actions.extend(("remove " + ref, lambda ref=ref: driver.command("rm", "--force", ref, timeout=30)) for ref in refs)
    actions.extend([("user context", lambda: reset_current_user(token)), ("app config", reset_app_config), ("fleet", fleet.stop)])
    await settle(actions)
    if errors:
        if original_error is not None:
            errors.insert(0, original_error)
        raise BaseExceptionGroup("Owned staged Linux cleanup failures", errors)


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("partial", "final"))
async def test_actual_installed_original_daemon_http_stages_one_candidate_without_point(tmp_path, kind, containment_negative=False):
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
    from deerflow.runtime import RunManager
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    from .c04_integration_fixture import control_database, node_server
    from .test_b02_fleet_foundation import service_class, settings
    from .test_c01_remote_agent_admission import c_config
    from .test_c02_remote_agent_admission import request

    image = os.environ.get("FLEET_AGENT_TEST_IMAGE")
    if os.environ.get("FLEET_TEST_CONTAINERS") == "1" and not image:
        pytest.fail("required explicit installed gate needs immutable FLEET_AGENT_TEST_IMAGE")
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
        driver = AgentContainers(state_dir=tmp_path / "agent-state", operator_config=private, provider="c08-staged")
        inspected_image = __import__("json").loads(await driver.checked("image", "inspect", image))[0]
        image = inspected_image["Id"]
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        if bootstrap_failure:
            driver.provider = "not-installed"
        (tmp_path / "execution-nas").mkdir()
        nas = settings(tmp_path / "execution-nas")
        config_data = c_config()
        config_data.update(nas_root=str(nas.nas_root), nas_identity=nas.nas_identity)
        config_data["profiles"]["remote"].update(image=image, runtime_digest=actual.runtime_digest, user=str(os.getuid()) + ":" + str(os.getgid()), network="bridge", pids_limit=256, execution_timeout_seconds=180)
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
            input={"messages": [{"role": "user", "content": "Create deterministic parent and child artifacts c04-control-port=" + str(db.port) + (" c08-final-boundary" if kind == "final" else "")}]},
            config={"recursion_limit": 100, "context": {"model_name": "model-1", "subagent_enabled": True}},
            stream_mode=["values", "messages-tuple", "custom"],
            stream_subgraphs=True,
            interrupt_before=["tools"] if interrupted else None,
        )
        record = await services.start_run(body, "thread-c04", request(core, user), execution_backend=backend)
        refs = []
        execution = None
        client = http_client = None
        stager = None
        lost, renewals, seals, recoveries, censuses = [], [], [], [], []
        evidence = Path(os.environ["C08_LINUX_EVIDENCE_DIR"]) / (
            "escape-negative" if containment_negative == "escape" else "profile-negative" if containment_negative == "profile" else "containment-negative" if containment_negative else kind
        )
        evidence.mkdir(parents=True, exist_ok=False)
        try:
            async with node_server(app) as url:
                transport = httpx.AsyncHTTPTransport()

                class LostPreparedReply(httpx.AsyncBaseTransport):
                    async def handle_async_request(self, request):
                        response = await transport.handle_async_request(request)
                        if request.url.path.endswith("/renew"):
                            renewals.append(time.monotonic())
                        if request.url.path.endswith("/workspace/prepared") and response.status_code == 200 and not lost:
                            await response.aread()
                            await response.aclose()
                            lost.append(time.monotonic())
                            raise httpx.ReadError("Actual committed prepared response dropped", request=request)
                        return response

                    async def aclose(self):
                        await transport.aclose()

                http_client = httpx.AsyncClient(base_url=url + "/", transport=LostPreparedReply(), timeout=10)
                client = NodeClient(gateway_url=url, credential=credential.token, claim_kind="agent", compatibility=actual.model_dump(mode="json"), http_client=http_client)

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

                class ObservedCollectorReaders(dict):
                    def pop(self, ref, default=None):
                        readers = self.get(ref)
                        if readers is not None and all(task.done() for task in readers):
                            values = []
                            for task in readers:
                                try:
                                    value = task.result()
                                    values.append(value.decode(errors="replace") if isinstance(value, bytes) else value)
                                except BaseException as error:
                                    values.append(type(error).__name__)
                            (evidence / "collector-cli.json").write_text(json.dumps(values))
                        return super().pop(ref, default)

                driver._workspace_readers = ObservedCollectorReaders(driver._workspace_readers)
                original_census = driver.quiesce_workspace

                async def observed_census(*args, **kwargs):
                    try:
                        observed = await original_census(*args, **kwargs)
                    except BaseException:
                        diagnostic = (
                            "import os,json,pathlib; rows=[]\n"
                            "for p in pathlib.Path('/proc').iterdir():\n"
                            " if p.name.isdecimal():\n"
                            "  try:\n"
                            "   argv=(p/'cmdline').read_bytes().split(bytes([0]))\n"
                            "   modules=[argv[i+1].decode() for i,v in enumerate(argv[:-1]) if v==b'-m' and argv[i+1].startswith(b'fleet.')]\n"
                            "   rows.append({'pid':int(p.name),'stat':(p/'stat').read_text(),'known_fixture_modules':modules,'uid_lines':[s for s in (p/'status').read_text().splitlines() if s.startswith('Uid:')]})\n"
                            "  except FileNotFoundError: pass\n"
                            "print(json.dumps({'diagnostic_pid':os.getpid(),'processes':rows}))"
                        )
                        grant_for_observation = args[0]
                        snapshot = await driver.checked("exec", "--user", "0:0", grant_for_observation["process_ref"], "python", "-I", "-S", "-c", diagnostic)
                        (evidence / "collector-failure-process-census.json").write_text(snapshot)
                        raise
                    observed["observed_monotonic"] = time.monotonic()
                    censuses.append(observed)
                    (evidence / "collector-censuses.json").write_text(json.dumps(censuses, indent=2))
                    return observed

                driver.quiesce_workspace = observed_census
                daemon = NodeDaemon(client=client, containers=driver, state_dir=tmp_path / "agent-state", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.25, poll_seconds=0.05)
                stager = AgentWorkspacePublication(client=client, containers=driver, nas=NASWorkspace(nas.nas_root, identity=nas.nas_identity), journal=daemon.journal)
                daemon.workspace_publications = stager
                publication_pause = asyncio.Event()
                publication_resume = asyncio.Event()
                original_step = stager.step

                async def observed_step(*args, **kwargs):
                    if publication_pause.is_set():
                        await publication_resume.wait()
                    return await original_step(*args, **kwargs)

                stager.step = observed_step
                await daemon.bootstrap()
                execution = asyncio.create_task(daemon.execute_one())
                deadline = time.monotonic() + 100
                rows = []
                while time.monotonic() < deadline:
                    refs = [ref for ref, _ in await driver.list_managed(node_id)]
                    if execution.done():
                        result = await execution
                        raise AssertionError("Original daemon exited before prepared candidate: " + str(result))
                    if refs:
                        _, stdout, stderr = await driver.command("logs", refs[0])
                        safe_log = (stdout + stderr).replace(db.password, "[private credential redacted]")
                        (evidence / "runner.log").write_text(safe_log)
                        rows = []
                        for line in stdout.splitlines():
                            try:
                                row = json.loads(line)
                            except ValueError:
                                continue
                            if isinstance(row, dict) and "event" in row:
                                rows.append(row)
                        if any(row["event"] == "boundary-error" for row in rows):
                            raise AssertionError("Installed trusted boundary fixture failed; see owned runner.log")
                        if any(row["event"] == "original-prepared-gate-closed" for row in rows) and lost and len(recoveries) >= 2 and len(censuses) >= 4:
                            break
                    await asyncio.sleep(0.05)
                else:
                    raise AssertionError("Actual installed graph/staging deadline elapsed; see owned runner.log")
                assert len(refs) == 1
                observation = await driver.inspect(refs[0])
                assert observation["State"]["Running"]
                assert len(seals) == (2 if kind == "final" else 1) and len(recoveries) >= 2 and len(censuses) >= 4 and renewals
                checkpoint_receipt = next(row for row in rows if row["event"] == "original-request-published")
                joined_receipt = next((row for row in rows if row["event"] == "original-writers-joined-before-publication-sf-begin"), None)
                if os.environ.get("C08_EXPECT_ANCESTRY") == "1":
                    assert joined_receipt is not None
                if joined_receipt is not None:
                    assert len(joined_receipt["ancestry"]) == len(joined_receipt["physically_absent"]) == 6
                    assert joined_receipt["actual_pipe_threads_joined"]
                    assert all(
                        len(entry["original_tool_execution_id"]) == 64 and entry["chain"][-1]["pid"] == entry["supervisor_pid"] and entry["chain"][-1]["start_ticks"] == entry["supervisor_start_ticks"] for entry in joined_receipt["ancestry"]
                    )
                assert checkpoint_receipt["kind"] == kind and checkpoint_receipt["budget_started"] == (kind == "final")
                paused = next(row for row in rows if row["event"] == "original-model-paused")
                assert {"bash", "c04_echo", "c04_submit_job", "task", "present_files", "memory_add"} <= set(paused["tools"])
                reconnected = next(row for row in rows if row["event"] == "original-mcp-close-reconnect")
                assert reconnected["original_owner_done"] and not reconnected["new_owner_is_original"]
                records = daemon.journal.records()
                assert len(records) == 1
                record = records[0]
                pointer = record["workspace_candidate"]
                assert set(pointer) == {"request_id", "request_digest", "barrier_epoch", "nonce", "manifest_id"}
                assert len((daemon.journal.root / (record["claim"]["attempt_id"] + ".json")).read_bytes()) < 1024 * 1024
                output = await prepare(record["claim"], record["grant"])
                for role in ("parent", "child"):
                    artifact = list(Path(output).rglob(role + ".txt"))
                    probe_paths = list(Path(output).rglob(role + "-probe.json"))
                    assert len(artifact) == len(probe_paths) == 1
                    assert artifact[0].read_bytes() == b"c04-artifact\n"
                    probe = json.loads(probe_paths[0].read_text())
                    assert probe["uid"] == os.getuid()
                    assert probe["environ"] == probe["fd"] == probe["mem"] == probe["ptrace"] == "denied"
                    assert probe["db_without_password"] == probe["db_wrong_password"] == "denied"
                    assert probe["raw_config_absent"] and probe["control_env_absent"]
                    assert probe["cgroup"] == probe["runner_cgroup"]
                ticks = sorted(Path(output).rglob("*.ticks"))
                assert len(ticks) == 6
                identities = [json.loads(path.read_text()) for path in sorted(Path(output).rglob("*.identity.json"))]
                assert len(identities) == 6 and len({(item["pid"], item["start_ticks"]) for item in identities}) == 6
                assert all(item["pid"] > 1 and item["ppid"] > 0 and item["start_ticks"] > 0 and item["uid"] == os.getuid() for item in identities)
                assert all(item["pid_namespace"] == "pid:[" + str(censuses[0]["receipt"]["collector"]["pid_namespace"]) + "]" for item in identities)
                assert all(item["pid"] not in census["receipt"]["remaining_pids"] for item in identities for census in censuses)
                assert censuses[0]["observed_monotonic"] < seals[0]
                hashes = {str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest() for path in ticks}
                await asyncio.sleep(0.3)
                assert hashes == {str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest() for path in ticks}
                async with db.engine.connect() as conn:
                    processes = [dict(row) for row in (await conn.execute(text("SELECT pid,start_ticks,role,tool_execution_id,state,supervisor_pid,supervisor_start_ticks,settled_at FROM fleet_workspace_processes ORDER BY pid"))).mappings()]
                    assert processes and all(row["state"] == "settled" and len(row["tool_execution_id"]) == 64 for row in processes)
                    request_state = (
                        await conn.execute(text("SELECT id,run_id,attempt_id,checkpoint_id,kind,request_digest,barrier_epoch,state,candidate_manifest_id,created_at FROM fleet_workspace_requests WHERE id=:id"), {"id": pointer["request_id"]})
                    ).one()
                    assert all(row["settled_at"] <= request_state.created_at for row in processes)
                    assert all(
                        row["role"] != "shell"
                        or any(
                            parent["role"] == "supervisor" and parent["pid"] == row["supervisor_pid"] and parent["start_ticks"] == row["supervisor_start_ticks"] and parent["tool_execution_id"] == row["tool_execution_id"]
                            for parent in processes
                        )
                        for row in processes
                    )
                    for row in processes:
                        row["settled_at"] = row["settled_at"].isoformat()
                    assert request_state.state == "prepared" and request_state.candidate_manifest_id == pointer["manifest_id"]
                    candidate_point_count = await conn.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE request_id=:id"), {"id": pointer["request_id"]})
                    assert candidate_point_count == 0
                    assert request_state.id == pointer["request_id"] and request_state.kind == kind
                    assert request_state.checkpoint_id == checkpoint_receipt["checkpoint_id"]
                    assert request_state.request_digest == pointer["request_digest"] and request_state.barrier_epoch == pointer["barrier_epoch"]
                    prior_points_before_stop = [
                        dict(row) for row in (await conn.execute(text("SELECT id,request_id,run_id,attempt_id,kind,checkpoint_id,manifest_id,request_digest,publication_key FROM fleet_workspace_points ORDER BY id"))).mappings()
                    ]
                    assert await conn.scalar(text("SELECT count(*) FROM fleet_workspace_manifests WHERE id=:id"), {"id": pointer["manifest_id"]}) == 1
                    assert await conn.scalar(text("SELECT count(*) FROM fleet_workspace_points")) == (1 if kind == "final" else 0)
                    assert await conn.scalar(text("SELECT count(*) FROM fleet_workspace_manifests")) == (2 if kind == "final" else 1)
                    if kind == "final":
                        prior = (await conn.execute(text("SELECT p.kind,r.state,p.checkpoint_id,p.manifest_id FROM fleet_workspace_points p JOIN fleet_workspace_requests r ON r.id=p.request_id"))).one()
                        assert prior.kind == "partial" and prior.state == "accepted"
                        assert prior.checkpoint_id != checkpoint_receipt["checkpoint_id"] and prior.manifest_id != pointer["manifest_id"]
                    assert (await conn.execute(text("SELECT content FROM c06_memory ORDER BY content"))).scalars().all()
                    assert (await conn.execute(text("SELECT value FROM c06_extension ORDER BY value"))).scalars().all() == ["lead-start", "start", "subagent-start", "subagent-stop"]
                    assert await conn.scalar(text("SELECT count(*) FROM mcp_tasks")) == 1
                assert all(
                    item["container_id"] == observation["Id"]
                    and item["started_at"] == observation["State"]["StartedAt"]
                    and item["receipt"]["remaining_pids"] == sorted([1, item["receipt"]["collector"]["pid"]])
                    and item["receipt"]["collector"]["uid"] != item["receipt"]["runner"]["uid"]
                    for item in censuses
                )
                if containment_negative:
                    from .c08_linux_negative_observer import observe_containment_negatives, observe_frozen_profile_negatives, observe_namespace_uid_escape

                    publication_pause.set()
                    # Let the current original stage settle; subsequent stages
                    # await only this host observation barrier while renew runs.
                    await asyncio.sleep(0.3)
                    original_request = stager._request(await client.attempt(record["claim"], "workspace/poll"))
                    observer = observe_namespace_uid_escape if containment_negative == "escape" else observe_frozen_profile_negatives if containment_negative == "profile" else observe_containment_negatives
                    negative_proof = await observer(driver, record["grant"], output, original_request)
                    (evidence / "containment-negative-proof.json").write_text(json.dumps(negative_proof, indent=2))
                    if containment_negative == "escape":
                        forged = {key: pointer[key] for key in ("request_id", "request_digest", "barrier_epoch", "nonce")}
                        forged["nonce"] = "f" * 64 if forged["nonce"] != "f" * 64 else "e" * 64
                        with pytest.raises(httpx.HTTPStatusError) as rejected:
                            await client.attempt(record["claim"], "workspace/claim", **forged)
                        assert rejected.value.response.status_code == 409
                        negative_proof["actual_http_forged_nonce_status"] = 409
                        (evidence / "containment-negative-proof.json").write_text(json.dumps(negative_proof, indent=2))
                    publication_resume.set()
                # Stop the same original runner; never start a replacement to
                # recover an already-stopped owner or acknowledge its candidate.
                assert await driver.stop(refs[0])
                result = await asyncio.wait_for(execution, 20)
                assert not (await driver.inspect(refs[0]))["State"]["Running"]
                replay = await http_client.post(
                    "api/fleet/node/attempts/" + record["claim"]["attempt_id"] + "/workspace/claim",
                    headers={"Authorization": "Bearer " + credential.token},
                    json={"node_session_id": client.session_id, "token": record["claim"]["token"], **{key: pointer[key] for key in ("request_id", "request_digest", "barrier_epoch", "nonce")}},
                )
                assert replay.status_code == 409
                candidate = (
                    original_recover(
                        AgentWorkspaceVersions(stager.nas, max_input_bytes=record["grant"]["input_limits"]["max_input_bytes"], max_output_bytes=record["grant"]["execution_profile"]["max_output_bytes"]),
                        stager._identity(original_request, record["claim"], record["grant"]),
                    )
                    if containment_negative
                    else None
                )
                if candidate is not None:
                    late_ack = await http_client.post(
                        "api/fleet/node/attempts/" + record["claim"]["attempt_id"] + "/workspace/prepared",
                        headers={"Authorization": "Bearer " + credential.token},
                        json={
                            "node_session_id": client.session_id,
                            "token": record["claim"]["token"],
                            **{key: pointer[key] for key in ("request_id", "request_digest", "barrier_epoch", "nonce")},
                            "manifest": candidate.model_dump(mode="json"),
                        },
                    )
                    assert late_ack.status_code == 409
                    negative_proof["actual_http_physical_stop_late_ack_status"] = 409
                    (evidence / "containment-negative-proof.json").write_text(json.dumps(negative_proof, indent=2))
                assert len(await driver.list_managed(node_id)) == 1
                await stager.join_writers()
                async with db.engine.connect() as conn:
                    prior_points_after_stop = [
                        dict(row) for row in (await conn.execute(text("SELECT id,request_id,run_id,attempt_id,kind,checkpoint_id,manifest_id,request_digest,publication_key FROM fleet_workspace_points ORDER BY id"))).mappings()
                    ]
                    candidate_point_count_after_stop = await conn.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE request_id=:id"), {"id": pointer["request_id"]})
                assert prior_points_after_stop == prior_points_before_stop
                assert candidate_point_count_after_stop == candidate_point_count
                (evidence / "proof.json").write_text(
                    json.dumps(
                        {
                            "image_id": image,
                            "container_id": observation["Id"],
                            "started_at": observation["State"]["StartedAt"],
                            "root_checkpoint_id": checkpoint_receipt.get("checkpoint_id"),
                            "kind": kind,
                            "request_state": request_state.state,
                            "manifest_id": pointer["manifest_id"],
                            "current_candidate_point_count": candidate_point_count,
                            "current_candidate_point_count_after_stop": candidate_point_count_after_stop,
                            "prior_partial_point_count": len(prior_points_before_stop),
                            "prior_points_before_stop": prior_points_before_stop,
                            "prior_points_after_stop": prior_points_after_stop,
                            "current_request_sql": {key: value for key, value in request_state._mapping.items() if key != "created_at"},
                            "actual_seal_count": len(seals),
                            "actual_recover_count": len(recoveries),
                            "lost_prepared_response_count": len(lost),
                            "original_renew_count": len(renewals),
                            "original_start_count": len(driver.ready),
                            "actual_registered_processes": processes,
                            "censuses": censuses,
                            "actual_six_writer_identities": identities,
                            "request_created_at": request_state.created_at.isoformat(),
                            "before_copy_census_monotonic": censuses[0]["observed_monotonic"],
                            "copy_started_monotonic": seals[0],
                            "postcopy_tick_hashes": hashes,
                            "late_claim_status": replay.status_code,
                            "physical_stop_result": result,
                            "runner_receipts": rows,
                        },
                        indent=2,
                    )
                )
        finally:
            await _settle_staged_linux_workspace(
                refs=refs,
                node_id=node_id,
                driver=driver,
                execution=execution,
                stager=stager,
                client=client,
                http_client=http_client,
                versions=locals().get("AgentWorkspaceVersions"),
                original_seal=locals().get("original_seal"),
                original_recover=locals().get("original_recover"),
                reset_current_user=reset_current_user,
                token=token,
                reset_app_config=reset_app_config,
                fleet=fleet,
                original_error=sys.exc_info()[1],
            )


@pytest.mark.live
@pytest.mark.asyncio
async def test_actual_installed_original_containment_rejects_unknown_and_private_fd_access(tmp_path):
    await test_actual_installed_original_daemon_http_stages_one_candidate_without_point(tmp_path, "partial", containment_negative=True)


@pytest.mark.live
@pytest.mark.asyncio
async def test_actual_installed_original_containment_rejects_frozen_uid_profile_drift(tmp_path):
    await test_actual_installed_original_daemon_http_stages_one_candidate_without_point(tmp_path, "partial", containment_negative="profile")


@pytest.mark.live
@pytest.mark.asyncio
async def test_actual_installed_original_containment_denies_namespace_uid_escape_and_forged_nonce(tmp_path):
    await test_actual_installed_original_daemon_http_stages_one_candidate_without_point(tmp_path, "partial", containment_negative="escape")
