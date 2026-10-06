"""Installed original prepared-success/committed-completion SQL orders."""

import asyncio
import json
import time
from contextlib import AsyncExitStack
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text

from .test_c09_stock_linux_rollback import control_database, owned_setup
from .test_c09_stock_linux_rollback import state as original_state


class CASPhysicalOwnersPending(RuntimeError):
    """Retain actual fixture Tasks/lock/resources; never authorize close."""

    def __init__(self, owners, lock, resources, errors):
        super().__init__("CAS physical owners still retained at their frozen bounds")
        self.owners, self.lock, self.resources, self.errors = owners, lock, resources, errors


async def join_physical(owner, deadline):
    primary = None
    while not owner.done():
        try:
            _, pending = await asyncio.wait({owner}, timeout=max(0, deadline - time.monotonic()))
            if pending:
                raise TimeoutError("Fixture physical owner retained beyond its original bound")
        except asyncio.CancelledError as error:
            if primary is None:
                primary = error
    try:
        result = owner.result()
    except BaseException as error:
        if primary is not None:
            raise BaseExceptionGroup("Original wait and physical owner failures", [primary, error])
        raise
    if primary is not None:
        raise primary
    return result


async def physical_close_barrier(owners, lock, resources):
    errors = []
    for owner, deadline in tuple(owners.items()):
        try:
            await join_physical(owner, deadline)
        except BaseException as error:
            errors.append(error)
    if any(not owner.done() for owner in owners) or lock.locked():
        raise CASPhysicalOwnersPending(owners, lock, resources, errors)
    return errors


async def state(sf, run):
    """Independent committed rows, including original immutable budget inputs."""
    from datetime import timedelta

    value = await original_state(sf, run)
    async with sf() as session:
        row = (
            await session.execute(
                text(
                    "SELECT r.error,t.deadline,a.execution_deadline FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id "
                    "JOIN fleet_attempts a ON a.id=p.active_attempt_id WHERE r.run_id=:run"
                ),
                {"run": run},
            )
        ).one()
    value.update(error=row[0], task_deadline=row[1], attempt_deadline=row[2])
    value["first_cancellation_deadline"] = None if value["cancel_requested_at"] is None else min(value["cancel_requested_at"] + timedelta(seconds=120), row[1], row[2])
    return value


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("order,action", [("cancel-first", "rollback"), ("cancel-first", "interrupt"), ("completion-first", "rollback")])
async def test_c09_installed_original_cas(tmp_path, monkeypatch, order, action):
    mode = "full"
    import os

    case_evidence = Path(os.environ["C09_LINUX_EVIDENCE_DIR"]) / (order + "-" + action)
    case_evidence.mkdir(parents=True, exist_ok=False)
    monkeypatch.setenv("C09_LINUX_EVIDENCE_DIR", str(case_evidence))
    import importlib
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
    resources = AsyncExitStack()
    retain_resources = False
    physical_owners = physical_lock = None
    try:
        db = await resources.enter_async_context(control_database(tmp_path))
        setup = await resources.enter_async_context(owned_setup())
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
                        "content": "Write the original supervised artifact c09-cas=" + order + " c04-control-port=" + str(db.port),
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

        from .c08_installed_cleanup import cancel_owned_task, settle_owned_cleanup, settle_owned_execution
        from .c08_stock_diagnostics import NodeProtocolObservation, capture_precleanup

        evidence = Path(os.environ["C09_LINUX_EVIDENCE_DIR"])
        refs, stops, censuses = [], [], []
        execution = wait_request = None
        client = node_http = public_http = stager = None
        server = AsyncExitStack()
        original_stop, original_census = driver.stop, driver.quiesce_workspace
        collector_completions = []
        release_commands = []
        physical_lock = asyncio.Lock()
        physical_owners = {}
        physical_receipts = []

        async def physical_call(operation, call, deadline):
            async with asyncio.timeout_at(min(deadline, time.monotonic() + 3)):
                await physical_lock.acquire()
            facts = {"operation": operation, "deadline": deadline, "host_monotonic_started": time.monotonic(), "physical_owner_done": False}
            owner = asyncio.create_task(call(), name="c09-owned-physical-" + operation)
            physical_owners[owner] = deadline
            physical_receipts.append(facts)

            def completed(actual):
                facts["physical_owner_done"] = actual.done()
                facts["physical_owner_cancelled"] = actual.cancelled()
                if not actual.cancelled():
                    error = actual.exception()
                    facts["error_type"] = None if error is None else type(error).__name__
                # The original collector call owns CLI/readers until its
                # finally returns. No caller cancellation cancels this Task.
                facts["collector_readers_settled"] = not driver._workspace_readers
                facts["collector_clis_settled"] = all(proc.returncode is not None for proc in driver._workspace_execs.values())
                if facts["collector_readers_settled"] and facts["collector_clis_settled"]:
                    physical_lock.release()
                    facts["lock_released_after_actual_join"] = True
                else:
                    facts["lock_released_after_actual_join"] = False
                facts["host_monotonic_finished"] = time.monotonic()
                (evidence / "original-physical-owners.json").write_text(json.dumps(physical_receipts, indent=2))

            owner.add_done_callback(completed)
            return await join_physical(owner, deadline)

        async def join_all_physical():
            errors = await physical_close_barrier(physical_owners, physical_lock, resources)
            if errors:
                raise BaseExceptionGroup("Completed CAS physical owner failures", errors)

        async def release_original_barrier(attempt):
            assert refs and refs[0] == "fleet-" + attempt
            owned = await state(db.session_factory, record.run_id)
            assert owned["active_attempt_id"] == attempt
            observed = await driver.inspect(refs[0])
            if observed is None:
                return
            labels = observed["Config"].get("Labels") or {}
            assert observed["Image"] == image
            assert labels.get("deerflow.fleet.attempt") == attempt and labels.get("deerflow.fleet.node") == node_id
            assert labels.get("deerflow.fleet.launch")
            if not observed["State"]["Running"]:
                assert observed["State"]["Pid"] == 0
                return
            destination = "/tmp/c09-cas-" + attempt + ".release"
            from datetime import UTC, datetime

            original_bound = min(value for value in (owned["task_deadline"], owned["attempt_deadline"], owned["first_cancellation_deadline"]) if value is not None)
            deadline = min(time.monotonic() + 3, time.monotonic() + (original_bound - datetime.now(UTC)).total_seconds())

            async def touch():
                assert not driver._workspace_readers
                started = time.monotonic()
                code, stdout, stderr = await driver.command("exec", refs[0], "touch", destination, timeout=max(0, min(3, deadline - time.monotonic())))
                release_commands.append(
                    {
                        "run_id": record.run_id,
                        "attempt_id": attempt,
                        "container_id": observed["Id"],
                        "ref": refs[0],
                        "destination": destination,
                        "outside_user_data": True,
                        "shared_collector_lock_held": physical_lock.locked(),
                        "deadline": deadline,
                        "host_monotonic_started": started,
                        "host_monotonic_finished": time.monotonic(),
                        "exit_code": code,
                        "stdout": stdout,
                        "stderr": stderr,
                    }
                )
                (evidence / "original-barrier-release-commands.json").write_text(json.dumps(release_commands, indent=2))
                assert code == 0

            await physical_call("marker-exec", touch, deadline)

        class ObservedCollectorReaders(dict):
            """Observe the same original reader Tasks without replacing joins."""

            def __setitem__(self, ref, readers):
                super().__setitem__(ref, readers)
                observed = {"ref": ref, "host_monotonic_started": time.monotonic()}
                collector_completions.append(observed)

                def completed(task, role):
                    if task.cancelled():
                        observed[role] = {"cancelled": True}
                    elif task.exception() is not None:
                        observed[role] = {"error_type": type(task.exception()).__name__}
                    else:
                        value = task.result()
                        if isinstance(value, bytes):
                            value = value.decode("utf-8", errors="replace")
                            for secret in (db.host_url, db.runner_url, db.password):
                                if secret:
                                    value = value.replace(secret, "[private database credential redacted]")
                        observed[role] = value
                    observed["host_monotonic_last_completion"] = time.monotonic()
                    (evidence / "original-collector-completions.json").write_text(json.dumps(collector_completions, indent=2))

                for role, task in zip(("stdout", "stderr", "exit_code"), readers, strict=True):
                    task.add_done_callback(lambda task, role=role: completed(task, role))

        assert not driver._workspace_readers
        driver._workspace_readers = ObservedCollectorReaders()

        async def observed_stop(ref):
            result = await original_stop(ref)
            observed = await driver.inspect(ref)
            stops.append({"ref": ref, "physical_result": result, "host_monotonic": time.monotonic(), "state": observed["State"] if observed else None})
            return result

        async def observed_census(*args, **kwargs):
            observed = await physical_call("original-collector", lambda: original_census(*args, **kwargs), kwargs["deadline"])
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
                        current = await state(db.session_factory, record.run_id)
                        attempt = current["active_attempt_id"]
                        # Docker logs adds no process inside the workload PID
                        # namespace while the original partial collector seals.
                        rows = await logs()
                        barriers = [row for row in rows if row["event"] == "c09-original-cas-barrier" and row["identity"]["run_id"] == record.run_id and row["identity"]["attempt_id"] == attempt]
                        if barriers:
                            assert len(barriers) == 1
                            barrier = barriers[0]
                            break
                    if execution.done():
                        await execution
                        raise AssertionError("Original runner exited before actual CAS barrier")
                    await asyncio.sleep(0.05)
            assert len(refs) == 1 and (await driver.inspect(refs[0]))["State"]["Running"]
            before = await state(db.session_factory, record.run_id)
            assert barrier["identity"]["run_id"] == record.run_id
            assert barrier["identity"]["attempt_id"] == before["active_attempt_id"]
            assert barrier["order"] == order and barrier["phase"] == ("before-original-cas" if order == "cancel-first" else "after-original-commit")
            assert barrier["identity"]["node_id"] == node_id and barrier["identity"]["node_session_id"] == client.session_id
            assert barrier["identity"]["generation"] == before["generation"]
            assert barrier["identity"]["desired_core_status"] == "success"
            assert before["stopped_at"] is None and before["reservation"] != "released"
            async with db.session_factory() as session:
                prepared_request = dict((await session.execute(text("SELECT * FROM fleet_workspace_requests WHERE id=:request"), {"request": barrier["identity"]["request_id"]})).mappings().one())
                for field, value in barrier["identity"].items():
                    assert prepared_request["id" if field == "request_id" else field] == value
                assert prepared_request["barrier_epoch"] == barrier["barrier_epoch"]
                for field in ("run_id", "agent_task_id", "attempt_id", "generation", "user_id", "thread_id", "launch_spec_digest"):
                    assert barrier["candidate"][field] == barrier["identity"][field]
                assert prepared_request["checkpoint_id"] == barrier["identity"]["checkpoint_id"]
                assert prepared_request["candidate_manifest_id"] == barrier["candidate"]["manifest_id"]
                assert prepared_request["state"] == ("prepared" if order == "cancel-first" else "accepted")
                accepted_before = await session.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE run_id=:run AND kind IN ('final','paused')"), {"run": record.run_id})
                partial_before = [
                    dict(row)
                    for row in (
                        await session.execute(
                            text("SELECT id,request_id,kind,checkpoint_id,manifest_id,desired_core_status,desired_task_status FROM fleet_workspace_points WHERE run_id=:run AND kind='partial' ORDER BY id"), {"run": record.run_id}
                        )
                    ).mappings()
                ]
                assert partial_before and all(row["desired_core_status"] is None and row["desired_task_status"] is None for row in partial_before)
            (evidence / "actual-prepared-CAS.json").write_text(
                json.dumps({"barrier": barrier, "prepared_request": prepared_request, "accepted_terminal_count": accepted_before, "partial_points": partial_before, "before": before}, indent=2, default=str)
            )
            if order == "cancel-first":
                assert before["status"] == "running" and accepted_before == 0 and before["final_workspace_point_id"] is None
                assert before["accepted_workspace_point_id"] in {row["id"] for row in partial_before}
            else:
                assert barrier["finalized"] is True
                assert before["status"] == "success" and before["task_state"] == before["placement_state"] == "finishing"
                assert accepted_before == 1 and before["accepted_workspace_point_id"] == before["final_workspace_point_id"]
            from datetime import UTC, datetime

            original_bound = min(value for value in (before["task_deadline"], before["attempt_deadline"], before["first_cancellation_deadline"]) if value is not None)
            snapshot_deadline = min(time.monotonic() + 3, time.monotonic() + (original_bound - datetime.now(UTC)).total_seconds())
            snapshot_locked = False
            try:
                async with asyncio.timeout_at(snapshot_deadline):
                    await physical_lock.acquire()
                    snapshot_locked = True
                # No await after acquiring the same physical owner lock. The
                # original collector has joined CLI/readers and popped its
                # registry; new collectors cannot enter during this snapshot.
                assert collector_completions and all(row.get("exit_code") == 0 and row.get("stderr") == "" and "stdout" in row for row in collector_completions)
                assert not driver._workspace_readers
                success_collector_prefix = deepcopy(collector_completions)
            finally:
                if snapshot_locked:
                    physical_lock.release()
            (evidence / "prepared-success-collector-prefix.json").write_text(
                json.dumps({"count": len(success_collector_prefix), "original_completions": success_collector_prefix, "original_reader_registry_empty": True, "observed_before_first_public_cancel": True}, indent=2)
            )
            cancel_path = f"/api/threads/{record.thread_id}/runs/{record.run_id}/cancel"
            response = await public_http.post(cancel_path + "?wait=false&action=" + action)
            assert response.status_code == (202 if order == "cancel-first" else 409), response.text
            intent = await state(db.session_factory, record.run_id)
            duplicate = await public_http.post(cancel_path + "?wait=false&action=" + ("interrupt" if action == "rollback" else "rollback"))
            assert duplicate.status_code == response.status_code, duplicate.text
            duplicate_intent = await state(db.session_factory, record.run_id)
            assert duplicate_intent["cancel_action"] == intent["cancel_action"]
            assert duplicate_intent["cancel_requested_at"] == intent["cancel_requested_at"]
            assert duplicate_intent["first_cancellation_deadline"] == intent["first_cancellation_deadline"]
            assert duplicate_intent["task_deadline"] == before["task_deadline"] and duplicate_intent["attempt_deadline"] == before["attempt_deadline"]
            assert duplicate_intent["error"] == before["error"]
            assert intent["generation"] == before["generation"] and intent["active_attempt_id"] == before["active_attempt_id"]
            assert intent["task_cancel"] is None and intent["reservation"] != "released" and intent["stopped_at"] is None
            assert intent["cancel_action"] == (action if order == "cancel-first" else None)
            # The marker and every original census share one physical lock;
            # the exec exits before another collector can enter its namespace.
            await release_original_barrier(before["active_attempt_id"])
            async with asyncio.timeout(130):
                result = await asyncio.shield(execution)
            await stager.join_writers()
            await join_all_physical()
            assert not driver._workspace_readers
            assert collector_completions[: len(success_collector_prefix)] == success_collector_prefix
            assert all(row.get("exit_code") == 0 and row.get("stderr") == "" and "stdout" in row for row in collector_completions)
            observation = await driver.inspect(refs[0])
            assert not observation["State"]["Running"] and observation["State"]["ExitCode"] == 0
            assert observation["Image"] == image and result["stop_reason"] == "exit" and result["state"] == ("cancelled" if order == "cancel-first" else "succeeded") and not result["report_pending"]
            final = await state(db.session_factory, record.run_id)
            expected_status = "success" if order == "completion-first" else "error" if action == "rollback" else "interrupted"
            assert final["status"] == expected_status
            assert final["task_state"] == ("succeeded" if order == "completion-first" else "paused")
            assert final["placement_state"] == ("succeeded" if order == "completion-first" else "cancelled")
            assert final["cancel_requested_at"] == intent["cancel_requested_at"]
            assert final["first_cancellation_deadline"] == intent["first_cancellation_deadline"]
            assert final["error"] == ("Rolled back by user" if order == "cancel-first" and action == "rollback" else None)
            assert final["stopped_at"] is not None and final["reservation"] == "released"
            artifact = await public_http.get(f"/api/threads/{record.thread_id}/artifacts/mnt/user-data/outputs/parent.txt")
            assert artifact.status_code == 200 and artifact.content == b"c04-artifact\n"
            assert final["generation"] == before["generation"] and final["active_attempt_id"] == before["active_attempt_id"]
            assert final["accepted_workspace_point_id"] == final["final_workspace_point_id"] and final["accepted_workspace_point_id"] is not None
            async with db.session_factory() as session:
                all_points = [
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
                stale_state = await session.scalar(text("SELECT state FROM fleet_workspace_requests WHERE id=:request"), {"request": barrier["identity"]["request_id"]})
                assert (stale_state == "accepted") == (order == "completion-first")
                processes = [dict(row) for row in (await session.execute(text("SELECT pid,start_ticks,role,state,settled_at FROM fleet_workspace_processes WHERE run_id=:run"), {"run": record.run_id})).mappings()]
                assert await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE attempt_id=:attempt AND state='released' AND released_at IS NOT NULL"), {"attempt": before["active_attempt_id"]}) == 1
            partial_points = sorted((row for row in all_points if row["kind"] == "partial"), key=lambda row: row["id"])
            assert [{key: row[key] for key in partial_before[0]} for row in partial_points] == partial_before
            assert all(row["state"] == "accepted" for row in partial_points)
            points = [row for row in all_points if row["kind"] in {"final", "paused"}]
            assert len(points) == 1 and points[0]["state"] == "accepted" and points[0]["desired_core_status"] == expected_status
            assert points[0]["kind"] == ("final" if order == "completion-first" else "paused")
            assert (points[0]["request_id"] == barrier["identity"]["request_id"]) == (order == "completion-first")
            if order == "completion-first":
                assert final["accepted_workspace_point_id"] == before["accepted_workspace_point_id"]
            assert processes and all(row["state"] == "settled" and row["settled_at"] is not None for row in processes)
            assert stops and all(row["physical_result"] and row["state"] is not None and not row["state"]["Running"] for row in stops)
            stopped_calls = [row for row in protocol.rows if row["event"] == "original_client_call" and row["operation"].endswith("stopped")]
            assert len(stopped_calls) == 1 and stops[0]["host_monotonic"] <= stopped_calls[0]["host_monotonic_started"]
            async with make_checkpointer(native_private) as restored_saver:
                restored = await CheckpointStateAccessor.bind(seed_graph, restored_saver, mode=mode).aget({"configurable": {"thread_id": record.thread_id}})
                if order == "cancel-first" and action == "rollback":
                    assert [message.id for message in restored.values["messages"]] == ["c09-before-run"]
                    assert restored.values["title"] == "Pre-run title" and not restored.next
                else:
                    assert any(message.id == "c09-cas-final" for message in restored.values["messages"])
                assert restored.config["configurable"]["checkpoint_id"] == points[0]["checkpoint_id"]
            async with db.session_factory() as session:
                assert await session.scalar(text("SELECT error FROM runs WHERE run_id=:run"), {"run": record.run_id}) == ("Rolled back by user" if order == "cancel-first" and action == "rollback" else None)
            rows = await logs()
            assert any(row["event"] == "original-installed-complete-wheel-bytes" for row in rows)
            assert any(row["event"] == "c09-original-installed-origins" for row in rows)
            assert execution.done() and not daemon._active
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
                        "partial_points": partial_points,
                        "partial_points_before": partial_before,
                        "original_collector_completions": collector_completions,
                        "prepared_success_collector_prefix": success_collector_prefix,
                        "original_barrier_release_commands": release_commands,
                        "original_physical_owners": physical_receipts,
                        "prepared_success_request": prepared_request,
                        "prepared_success_request_after": stale_state,
                        "registered_processes": processes,
                        "physical_stop_observations": stops,
                        "collector_censuses": censuses,
                        "node_protocol": protocol.rows,
                        "original_stop_result": result,
                        "cas_order": order,
                        "winning_action": action,
                        "duplicate_intent": duplicate_intent,
                        "pre_run_checkpoint": seed_config,
                        "restored_checkpoint": restored.config,
                        "current_file_contents": artifact.content.decode(),
                        "runner_receipts": rows,
                        "owned_handles_before_cleanup": {"execution_done": execution.done(), "wait_done": wait_request is None or wait_request.done(), "daemon_active": len(daemon._active)},
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

            async def release_cas():
                if refs:
                    owned = await state(db.session_factory, record.run_id)
                    if owned["active_attempt_id"]:
                        await release_original_barrier(owned["active_attempt_id"])

            before_cleanup = [("release-cas", release_cas), ("diagnostics", lambda: capture_precleanup(session_factory=db.session_factory, run_id=record.run_id, driver=driver, refs=refs, evidence=evidence))]
            if refs:
                before_cleanup.append(("runner-log", logs))
            if wait_request is not None:
                before_cleanup.append(("wait-request", lambda: cancel_owned_task(wait_request)))
            completed_primary = execution is not None and execution.done() and not execution.cancelled() and execution.exception() is original_error
            errors = []

            async def settle_phase(actions):
                try:
                    await settle_owned_cleanup(actions)
                except BaseException as error:
                    errors.extend(error.exceptions if isinstance(error, BaseExceptionGroup) else [error])

            # This phase can stop/join actual owners, but closes no client,
            # server, container checkout or database scope.
            await settle_phase(before_cleanup)

            async def record_discovered():
                for ref in await discover_owned():
                    if ref not in refs:
                        refs.append(ref)

            stopped = set()

            async def stop_owned():
                for ref in refs:
                    if ref not in stopped:
                        stopped.add(ref)
                        await settle_phase([("stop-" + ref, lambda ref=ref: driver.stop(ref))])

            await settle_phase([("discover-before", record_discovered)])
            await stop_owned()
            if execution is not None and not completed_primary:
                await settle_phase([("execution", lambda: settle_owned_execution(execution))])
            await settle_phase([("discover-after", record_discovered)])
            await stop_owned()
            if stager is not None:
                await settle_phase([("stager-writers", stager.join_writers)])
            # Non-aggregatable barrier: every owner is visited even when an
            # earlier done owner failed. Pending retains this SAME resource
            # stack and prevents all closes/removal/DB context exit below.
            try:
                errors.extend(await physical_close_barrier(physical_owners, physical_lock, resources))
            except CASPhysicalOwnersPending as pending:
                pending.errors[:0] = ([] if original_error is None else [original_error]) + errors
                raise
            close_actions = []
            for name, owner in (("node-client", client), ("node-http", node_http), ("public-http", public_http)):
                if owner is not None:
                    close_actions.append((name, owner.close if name == "node-client" else owner.aclose))
            close_actions.append(("tcp-server", server.aclose))
            close_actions.extend(("remove-" + ref, lambda ref=ref: driver.command("rm", "--force", ref, timeout=30)) for ref in refs)
            await settle_phase(close_actions)
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
                "physical_owners_done": all(owner.done() for owner in physical_owners),
                "physical_lock_released": not physical_lock.locked(),
                "scoped_cleanup_completed": not errors,
            }
            (evidence / "owned-handles.json").write_text(json.dumps(handles, indent=2))
            if errors:
                raise BaseExceptionGroup("Original CAS and owned cleanup failures", ([] if original_error is None else [original_error]) + errors)
    except CASPhysicalOwnersPending:
        retain_resources = True
        raise
    finally:
        if not retain_resources:
            # Also guard unexpected phase/diagnostic exceptions: none may
            # unwind the original database context past a pending owner.
            if physical_owners is not None:
                try:
                    await physical_close_barrier(physical_owners, physical_lock, resources)
                except CASPhysicalOwnersPending:
                    retain_resources = True
                    raise
            await resources.aclose()


@pytest.mark.asyncio
async def test_cas_physical_pending_blocks_resource_close(tmp_path, monkeypatch):
    """Actual child Tasks + PG/client scope; only fixture cleanup proof."""
    import sys

    from deerflow_ecs_fleet.worker.containers import DockerContainers, DockerError

    evidence = tmp_path / "pending-proof"
    monkeypatch.setenv("C09_LINUX_EVIDENCE_DIR", str(evidence))
    resources = AsyncExitStack()
    owners = {}
    lock = asyncio.Lock()
    marker = tmp_path / "release-actual-child"
    try:
        db = await resources.enter_async_context(control_database(tmp_path))
        client = await resources.enter_async_context(httpx.AsyncClient())
        original = DockerContainers(state_dir=tmp_path, executable=sys.executable)
        early = asyncio.create_task(original.checked("-c", "raise ValueError('original actual child failure')"), name="c09-early-original-command-error")
        owners[early] = time.monotonic() + 2
        await asyncio.wait({early})
        assert isinstance(early.exception(), DockerError)
        await lock.acquire()
        child = asyncio.create_task(
            original.command("-c", "import pathlib,sys,time; p=pathlib.Path(sys.argv[1]); end=time.monotonic()+1;\nwhile not p.exists() and time.monotonic()<end: time.sleep(.01)\nassert p.exists()", str(marker), timeout=2),
            name="c09-retained-original-command",
        )
        owners[child] = time.monotonic() - 0.001  # earlier frozen fixture bound, not production expiry
        child.add_done_callback(lambda actual: lock.release())
        with pytest.raises(CASPhysicalOwnersPending) as failure:
            await physical_close_barrier(owners, lock, resources)
        pending = failure.value
        assert pending.owners is owners and pending.lock is lock and pending.resources is resources
        assert pending.errors[0] is early.exception()
        assert not child.done() and lock.locked() and not client.is_closed
        assert not (evidence / "owned-database.json").exists()
        async with db.session_factory() as session:
            assert await session.scalar(text("SELECT 1")) == 1
        marker.write_bytes(b"release own actual child")
        code, stdout, stderr = await asyncio.shield(child)
        assert code == 0 and stdout == stderr == ""
        errors = await physical_close_barrier(owners, lock, resources)
        assert errors == [early.exception()] and all(owner.done() for owner in owners) and not lock.locked()
        await resources.aclose()
        assert client.is_closed and json.loads((evidence / "owned-database.json").read_text())["dropped"]
        print(
            json.dumps(
                {
                    "event": "c09-physical-resource-close-barrier",
                    "early_original_error": "DockerError",
                    "same_pending_task_retained": True,
                    "actual_child_joined": True,
                    "pending_client_database_held": True,
                    "later_client_closed_database_dropped": True,
                    "scope": "native fixture cleanup only; actual Python child via original command and realPG, not installed Docker/CAS/production deadline proof",
                },
                sort_keys=True,
            )
        )
    finally:
        marker.touch()
        for owner in owners:
            await asyncio.wait({owner})
        await resources.aclose()
