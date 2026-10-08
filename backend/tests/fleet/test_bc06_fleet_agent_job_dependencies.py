"""BC06 authenticated human operations on the original waiting Agent task."""

import json
import os
from pathlib import Path
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text

from .test_bc03_fleet_agent_job_continuations import fleet_database as fleet_database
from .test_bc03_fleet_agent_job_continuations import owner_environment as original_owner_environment
from .test_c02_remote_agent_admission import admission as original_admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner


@pytest_asyncio.fixture
async def admission(fleet_database, tmp_path):
    generator = original_admission.__wrapped__(fleet_database, tmp_path)
    try:
        environment = await anext(generator)
        environment[3].id = "f79f438b-ce11-4e92-9599-536573d65114"
        yield environment
    finally:
        await generator.aclose()


@pytest_asyncio.fixture
async def owner_environment(admission, tmp_path, monkeypatch):
    generator = original_owner_environment.__wrapped__(admission, tmp_path, monkeypatch)
    try:
        env = await anext(generator)
        config = env[3].config.model_copy(update={"scheduling_mode": "serial", "reserved_job_profile": "batch"})
        env[3].config = config
        env[4].state.fleet_ownership.config = config
        yield env
    finally:
        await generator.aclose()


def evidence(name, value):
    if directory := os.environ.get("BC06_EVIDENCE_DIR"):
        Path(directory, name + ".json").write_text(json.dumps(value, default=str, indent=2))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc06_user_message_supersedes_waiting_goal(checkpoint_owner, tmp_path, monkeypatch):
    from langgraph.store.memory import InMemoryStore

    from app.gateway.auth.local_provider import LocalAuthProvider
    from app.gateway.auth.models import User
    from app.gateway.auth.password import hash_password_async
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from app.gateway.routers import auth, fleet_agent_tasks, thread_runs
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    from .c04_integration_fixture import node_server
    from .c12_integration_fixture import login
    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop

    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[2] / "packages/ecs-fleet"), os.environ.get("PYTHONPATH", "")]))
    item = checkpoint_owner
    observed = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path, monkeypatch, continuation_case="bc06")
    evidence("parent-native-stop-workspace", observed)
    assert observed["task_state"] == "waiting_jobs" and observed["stopped_ack"] and observed["reservation_state"] == "released"
    engine, sf, user, runtime, app, record, *_ = item.env
    users = SQLiteUserRepository(sf)
    password = "Bc06-Only-Isolated-Password-7448"
    await users.create_user(User(id=UUID(user.id), email="bc06@example.com", password_hash=await hash_password_async(password), system_role="admin"))
    monkeypatch.setattr("app.gateway.deps._cached_repo", users)
    monkeypatch.setattr("app.gateway.deps._cached_local_provider", LocalAuthProvider(users))
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "")
    app.state.checkpointer = item.writer
    app.state.store = InMemoryStore()
    app.state.thread_store = ThreadMetaRepository(sf)
    app.state.run_event_store = DbRunEventStore(session_factory=sf)
    app.state.run_events_config = None
    app.state.stream_bridge = MemoryStreamBridge()
    app.include_router(auth.router)
    app.include_router(thread_runs.router)
    app.include_router(fleet_agent_tasks.router)
    async with engine.connect() as conn:
        before = dict((await conn.execute(text("SELECT id,generation,current_run_id,state,accepted_workspace_point_id FROM fleet_agent_tasks"))).mappings().one())
    evidence("before-human-operation", before)
    from .test_bc03_fleet_agent_job_continuations import complete_children, execute_continuation

    await runtime.nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
    completed = await complete_children(sf, runtime.config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id, max_jobs=1)
    evidence("accepted-old-child", completed)
    async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
        await login(client, "bc06@example.com", password)
        response = await client.post("/api/threads/" + record.thread_id + "/runs", json={"input": {"messages": [{"role": "user", "content": "Change the target using this new human message"}]}})
        evidence("human-message-http", {"status": response.status_code, "body": response.json()})
        assert response.status_code == 200, response.text
        new = response.json()
        async with engine.connect() as conn:
            after = dict((await conn.execute(text("SELECT id,generation,current_run_id,state FROM fleet_agent_tasks"))).mappings().one())
            placement = dict((await conn.execute(text("SELECT run_id,agent_task_id,generation,state FROM fleet_run_placements WHERE run_id=:run"), {"run": new["run_id"]})).mappings().one())
        evidence("new-generation-sql", {"task": after, "placement": placement})
        assert after["id"] == before["id"] and after["generation"] == before["generation"] + 1
        assert after["current_run_id"] == new["run_id"] and placement["agent_task_id"] == before["id"]

        from app.fleet.continuations import FleetContinuations
        from deerflow.runtime.runs.manager import ConflictError

        with pytest.raises(ConflictError):
            await FleetContinuations(sf, runtime.config, app.state.run_manager).dispatch(observed["group_id"])
        async with engine.connect() as conn:
            children = list((await conn.execute(text("SELECT id,state,accepted_manifest_id FROM fleet_jobs ORDER BY id"))).mappings())
            counts = dict((await conn.execute(text("SELECT (SELECT count(*) FROM fleet_jobs) jobs,(SELECT count(*) FROM fleet_attempts WHERE kind='job') child_attempts,(SELECT count(*) FROM runs) runs"))).mappings().one())
        evidence("old-generation-rejected", {"children": [dict(row) for row in children], "counts": counts})
        assert {row["state"] for row in children} == {"succeeded", "cancelled"} and counts["runs"] == 2
        human = await execute_continuation(item, tmp_path / "human-run", human=True)
        evidence("human-run-current-source", human)
        assert any("Change the target using this new human message" in content for content in human["history"]["human_messages"])
        async with engine.connect() as conn:
            current = dict((await conn.execute(text("SELECT generation,accepted_workspace_point_id FROM fleet_agent_tasks"))).mappings().one())
        resumed = await client.post("/api/threads/" + record.thread_id + "/agent-tasks/" + before["id"] + "/resume", json={"expected_generation": current["generation"], "idempotency_key": "explicit-results"})
        evidence("explicit-resume-http", {"status": resumed.status_code, "body": resumed.json()})
        assert resumed.status_code == 200, resumed.text
        async with engine.connect() as conn:
            final_counts = dict((await conn.execute(text("SELECT (SELECT count(*) FROM fleet_jobs) jobs,(SELECT count(*) FROM fleet_attempts WHERE kind='job') child_attempts"))).mappings().one())
            payload = await conn.scalar(text("SELECT payload FROM fleet_launch_specs WHERE run_id=:run"), {"run": resumed.json()["run_id"]})
        evidence("explicit-results-current-source", {"counts": final_counts, "input": payload["input"], "source": payload["source_workspace_point_id"], "generation": payload["generation"]})
        assert final_counts["jobs"] == counts["jobs"] and final_counts["child_attempts"] == counts["child_attempts"]
        assert payload["source_workspace_point_id"] == current["accepted_workspace_point_id"] and payload["generation"] == current["generation"] + 1
        assert completed[0]["manifest_id"] in json.dumps(payload["input"])

        executed = await execute_continuation(item, tmp_path / "explicit-resume", human=True)
        evidence("explicit-resume-native-current-source", executed)
        assert any("Change the target using this new human message" in content for content in executed["history"]["human_messages"])
        assert any(completed[0]["manifest_id"] in content for content in executed["history"]["human_messages"])
        assert executed["runs"][-1]["status"] == "success" and executed["runs"][-1]["stopped_at"] and executed["runs"][-1]["reservation_state"] == "released"
        async with engine.connect() as conn:
            assert await conn.scalar(text("SELECT count(*) FROM fleet_attempts WHERE kind='job'")) == counts["child_attempts"]


async def _bc06_case(directory, patch, *, queued=False):
    """Compose original fixtures; each terminal/recovery segment owns its schema."""
    from contextlib import AsyncExitStack, asynccontextmanager

    @asynccontextmanager
    async def fixture(generator):
        value = await anext(generator)
        try:
            yield value
        finally:
            await generator.aclose()

    stack = AsyncExitStack()
    directory.mkdir(parents=True)
    for name in ("BC06_EVIDENCE_DIR", "BC03_EVIDENCE_DIR", "BC02_EVIDENCE_DIR"):
        if original := os.environ.get(name):
            destination = Path(original) / directory.name
            destination.mkdir(exist_ok=True)
            patch.setenv(name, str(destination))
    try:
        database = await stack.enter_async_context(fixture(fleet_database.__wrapped__()))
        admitted = await stack.enter_async_context(fixture(admission.__wrapped__(database, directory)))
        owner = await stack.enter_async_context(fixture(owner_environment.__wrapped__(admitted, directory, patch)))
        if queued:
            from types import SimpleNamespace

            from deerflow_ecs_fleet.launch_spec import LaunchSpec

            import deerflow.persistence.models  # noqa: F401
            from deerflow.config.app_config import AppConfig
            from deerflow.persistence.base import Base

            async with owner[0].begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
                schema = await connection.scalar(text("SELECT current_schema()"))
                spec = LaunchSpec.model_validate(await connection.scalar(text("SELECT payload FROM fleet_launch_specs")))
            private = AppConfig.model_validate(
                {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "database": {"backend": "postgres", "postgres_url": owner[0].url.render_as_string(hide_password=False), "postgres_schema": schema}}
            )
            item = SimpleNamespace(engine=owner[0], env=owner, private=private, spec=spec)
        else:
            item = await stack.enter_async_context(fixture(checkpoint_owner.__wrapped__(owner)))
        return stack, item
    except BaseException:
        await stack.aclose()
        raise


async def _bc06_http_host(item, patch, stack):
    from langgraph.store.memory import InMemoryStore

    from app.gateway.auth.local_provider import LocalAuthProvider
    from app.gateway.auth.models import User
    from app.gateway.auth.password import hash_password_async
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from app.gateway.routers import auth, fleet_agent_tasks, thread_runs, threads
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    app, sf, user = item.env[4], item.env[1], item.env[2]
    repository = SQLiteUserRepository(sf)
    password = "Bc06-Boundary-Isolated-Password-7448"
    await repository.create_user(User(id=UUID(user.id), email="bc06-boundary@example.com", password_hash=await hash_password_async(password), system_role="admin"))
    patch.setattr("app.gateway.deps._cached_repo", repository)
    patch.setattr("app.gateway.deps._cached_local_provider", LocalAuthProvider(repository))
    patch.setenv("DEER_FLOW_AUTH_DISABLED", "")
    # The host writer is the original factory, without an executor's private fence.
    app.state.checkpointer = await stack.enter_async_context(make_checkpointer(item.private))
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository

    app.state.pat_repo = PersonalAccessTokenRepository(sf)
    app.state.store = InMemoryStore()
    app.state.thread_store = ThreadMetaRepository(sf)
    app.state.run_event_store = DbRunEventStore(sf)
    app.state.run_events_config = None
    app.state.stream_bridge = MemoryStreamBridge()
    for router in (auth.router, thread_runs.router, fleet_agent_tasks.router, threads.router):
        app.include_router(router)
    return app, password


async def _bc06_sql(item):
    async with item.engine.connect() as connection:
        return {
            table: [dict(row) for row in (await connection.execute(text("SELECT * FROM " + table + " ORDER BY 1"))).mappings()]
            for table in (
                "runs",
                "fleet_scheduler_tickets",
                "fleet_agent_tasks",
                "fleet_run_placements",
                "fleet_attempts",
                "fleet_reservations",
                "fleet_jobs",
                "fleet_job_links",
                "fleet_wait_groups",
                "fleet_task_operation_receipts",
                "thread_execution_bindings",
            )
        }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc06_generation_cancel_and_operation_races(tmp_path, monkeypatch):
    """One ordered boundary: original TX barriers, receipts, STOP and mutations."""
    import asyncio
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from deerflow_ecs_fleet.job_service import FleetJobService

    from app.fleet.continuations import FleetContinuationAdmission, FleetContinuations
    from app.gateway.internal_auth import create_internal_auth_headers
    from deerflow.runtime.runs.manager import ConflictError

    from .c04_integration_fixture import node_server
    from .c12_integration_fixture import login
    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop as waiting
    from .test_bc03_fleet_agent_job_continuations import complete_children, execute_continuation

    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[2] / "packages/ecs-fleet"), os.environ.get("PYTHONPATH", "")]))
    # Initial nonwaiting runs have no continuation lineage. Rejection must
    # precede any cancellation intent, including after original assignment.
    with monkeypatch.context() as patch:
        stack, item = await _bc06_case(tmp_path / "initial-nonwaiting", patch, queued=True)
        async with stack:
            app, password = await _bc06_http_host(item, patch, stack)
            await app.state.thread_store.create(item.spec.thread_id, user_id=item.spec.user_id)
            async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                await login(client, "bc06-boundary@example.com", password)
                before = await _bc06_sql(item)
                assert before["fleet_agent_tasks"][0]["state"] == "queued" and not before["fleet_wait_groups"] and not before["fleet_attempts"]
                rejected = await client.post(
                    f"/api/threads/{item.spec.thread_id}/runs", headers={"Idempotency-Key": "initial-queued-reject"}, json={"input": {"messages": [{"role": "user", "content": "unsupported initial queued supersession"}]}}
                )
                after = await _bc06_sql(item)
                evidence("initial-queued-handoff-rejection", {"before": before, "after": after, "http": rejected.status_code, "response": rejected.json()})
                assert rejected.status_code == 409, rejected.text
                assert after == before, "unsupported initial handoff must leave durable state untouched"
                accepted = await app.state.fleet_ownership.claim_agent("node-c03", node_session_id=item.env[6], worker=item.env[8])
                assert accepted is not None
                await app.state.fleet_ownership.authorize_start(node_id="node-c03", node_session_id=item.env[6], attempt_id=accepted.attempt_id, token=accepted.token)
                assigned_before = await _bc06_sql(item)
                assigned_rejected = await client.post(
                    f"/api/threads/{item.spec.thread_id}/runs", headers={"Idempotency-Key": "initial-assigned-reject"}, json={"input": {"messages": [{"role": "user", "content": "unsupported initial assigned supersession"}]}}
                )
                assigned_after = await _bc06_sql(item)
                evidence(
                    "initial-assigned-handoff-rejection",
                    {"before": assigned_before, "after": assigned_after, "http": assigned_rejected.status_code, "response": assigned_rejected.json(), "scope": "original claim/start grant only; no executor process launched"},
                )
                assert assigned_rejected.status_code == 409, assigned_rejected.text
                assert assigned_after == assigned_before

    # Cancellation wins the original task/job SQL locks; old coordinator waits.
    with monkeypatch.context() as patch:
        stack, item = await _bc06_case(tmp_path / "cancel-first", patch)
        async with stack:
            observed = await waiting(item, tmp_path / "cancel-first", patch, continuation_case="bc06_boundary")
            app, password = await _bc06_http_host(item, patch, stack)
            sf, runtime, task_id, thread_id = item.env[1], item.env[3], item.spec.agent_task_id, item.spec.thread_id
            from app.fleet.job_tracking import register_fleet_driver
            from app.gateway.services import launch_mcp_task_notification_run
            from app.mcp_tasks import McpTaskService
            from deerflow.mcp.tasks import McpTaskDriverRegistry
            from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

            app.state.mcp_task_repo = McpTaskRepository(sf)
            drivers = McpTaskDriverRegistry()
            register_fleet_driver(app, drivers)
            notification = McpTaskService(
                repository=app.state.mcp_task_repo,
                drivers=drivers,
                poll_interval_seconds=1,
                lease_seconds=30,
                max_concurrent_polls=1,
                launch_notification=lambda **kwargs: launch_mcp_task_notification_run(app=app, **kwargs),
                get_run=lambda run_id, **kwargs: app.state.run_manager.get(run_id, **kwargs),
            )
            await notification.run_once(now=datetime.now(UTC))
            # Actual original launcher is suppressed by the persisted awaited
            # binding, rather than promoted to a human operation.
            from app.mcp_tasks.errors import PermanentNotificationError

            async with sf() as session:
                tracking_id = await session.scalar(text("SELECT tracking_task_id FROM fleet_jobs LIMIT 1"))
            with pytest.raises(PermanentNotificationError):
                await launch_mcp_task_notification_run(app=app, thread_id=thread_id, assistant_id="lead_agent", owner_user_id=item.spec.user_id, task_id=tracking_id, dispatch_version=1, dispatch_attempt=1, event={"state": "completed"})

            async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                await login(client, "bc06-boundary@example.com", password)
                forged = await client.post(
                    f"/api/threads/{thread_id}/runs",
                    headers=create_internal_auth_headers(owner_user_id=item.spec.user_id),
                    json={"input": {"messages": [{"role": "user", "content": "notification pretending to be human"}]}, "metadata": {"origin": "human", "human_operation": True}},
                )
                assert forged.status_code == 409
                pat = await client.post("/api/v1/auth/pats", json={"name": "bc06-create-only", "scopes": ["runs:create", "threads:read"]})
                assert pat.status_code == 201, pat.text
                async with httpx.AsyncClient(base_url=url, headers={"Authorization": "Bearer " + pat.json()["token"]}) as restricted:
                    denied = await restricted.post(f"/api/threads/{thread_id}/runs", json={"input": {"messages": [{"role": "user", "content": "create only cannot cancel awaited"}]}})
                    assert denied.status_code == 403, denied.text
                entered, release = asyncio.Event(), asyncio.Event()
                original_cancel = FleetJobService.cancel_locked

                async def held_cancel(service, session, job):
                    if not entered.is_set():
                        entered.set()
                        await release.wait()
                    return await original_cancel(service, session, job)

                patch.setattr(FleetJobService, "cancel_locked", held_cancel)
                cancellation = asyncio.create_task(client.post(f"/api/threads/{thread_id}/agent-tasks/{task_id}/cancel", json={"expected_generation": 1, "idempotency_key": "cancel-first"}))
                coordinator = None
                try:
                    await entered.wait()
                    before = await _bc06_sql(item)
                    coordinator = asyncio.create_task(FleetContinuations(sf, runtime.config, app.state.run_manager).dispatch(observed["group_id"]))
                    await asyncio.sleep(0.05)
                    assert not coordinator.done() and before["fleet_agent_tasks"][0]["generation"] == 1
                    release.set()
                    response = await cancellation
                    assert response.status_code == 200, response.text
                    with pytest.raises(ConflictError):
                        await coordinator
                finally:
                    release.set()
                    await asyncio.gather(cancellation, *([coordinator] if coordinator is not None else []), return_exceptions=True)
                repeated = await client.post(f"/api/threads/{thread_id}/agent-tasks/{task_id}/cancel", json={"expected_generation": 1, "idempotency_key": "cancel-first"})
                stale = await client.post(f"/api/threads/{thread_id}/agent-tasks/{task_id}/cancel", json={"expected_generation": 1, "idempotency_key": "different-old-key"})
                assert repeated.json() == response.json() and stale.status_code == 409
                after = await _bc06_sql(item)
                modes = {link["job_id"]: link["link_mode"] for link in after["fleet_job_links"]}
                assert all(job["state"] == "cancelled" for job in after["fleet_jobs"] if modes[job["id"]] == "awaited")
                detached = [job for job in after["fleet_jobs"] if modes[job["id"]] == "detached"]
                assert len(detached) == 1
                assert all(job["cancel_requested_at"] is None and job["state"] == "queued" for job in detached)
                evidence("cancel-first-boundary", {"before": before, "after": after, "forged_internal_http": forged.status_code, "create_only_http": denied.status_code, "repeat": repeated.json(), "stale": stale.status_code})

    # The coordinator commits first. A real human ingress cancels its unassigned
    # run; a second human message consumes the precise prior receipt chain.
    with monkeypatch.context() as patch:
        stack, item = await _bc06_case(tmp_path / "admit-first", patch)
        async with stack:
            observed = await waiting(item, tmp_path / "admit-first", patch, continuation_case="bc06")
            runtime, sf = item.env[3], item.env[1]
            await runtime.nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
            await complete_children(sf, runtime.config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id)
            app, password = await _bc06_http_host(item, patch, stack)
            entered, release = asyncio.Event(), asyncio.Event()
            original_insert = FleetContinuationAdmission.insert

            async def held_insert(participant, session, run):
                await original_insert(participant, session, run)
                entered.set()
                await release.wait()

            patch.setattr(FleetContinuationAdmission, "insert", held_insert)
            automatic = asyncio.create_task(FleetContinuations(sf, runtime.config, app.state.run_manager).dispatch(observed["group_id"]))
            async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                await login(client, "bc06-boundary@example.com", password)
                first = None
                try:
                    await entered.wait()
                    first = asyncio.create_task(
                        client.post(f"/api/threads/{item.spec.thread_id}/runs", headers={"Idempotency-Key": "queued-human-one"}, json={"input": {"messages": [{"role": "user", "content": "first queued human target"}]}})
                    )
                    await asyncio.sleep(0.05)
                    assert not first.done()
                    release.set()
                    original = await automatic
                    first_response = await first
                    assert first_response.status_code == 200, first_response.text
                finally:
                    release.set()
                    await asyncio.gather(automatic, *([first] if first else []), return_exceptions=True)
                second_body = {"input": {"messages": [{"role": "user", "content": "second queued human target"}]}}
                second = await client.post(f"/api/threads/{item.spec.thread_id}/runs", headers={"Idempotency-Key": "queued-human-two"}, json=second_body)
                assert second.status_code == 200, second.text
                again = await client.post(f"/api/threads/{item.spec.thread_id}/runs", headers={"Idempotency-Key": "queued-human-two"}, json=second_body)
                changed = await client.post(f"/api/threads/{item.spec.thread_id}/runs", headers={"Idempotency-Key": "queued-human-two"}, json={"input": {"messages": [{"role": "user", "content": "changed same key"}]}})
                assert again.json()["run_id"] == second.json()["run_id"] and changed.status_code == 409
                data = await _bc06_sql(item)
                receipts = data["fleet_task_operation_receipts"]
                latest = next(row for row in receipts if row["admitted_run_id"] == second.json()["run_id"])
                assert latest["source_generation"] == 2 and latest["source_point_generation"] == 1 and latest["target_generation"] == 3 and latest["preceding_receipt_id"]
                assert all(row["active_attempt_id"] is None and row["state"] == "cancelled" for row in data["fleet_run_placements"] if row["run_id"] in {original.run_id, first_response.json()["run_id"]})
                executed = await execute_continuation(item, tmp_path / "admit-first" / "chain-execution", human=True)
                assert executed["new_exit"] == 0 and "second queued human target" in executed["history"]["human_messages"]
                evidence("admit-first-chain", {"sql": data, "executed": executed, "retry": again.json()["run_id"], "changed_key_http": changed.status_code})
                # Real ChannelManager launch sites sign server-bound human input.
                # Streaming transport below calls original SDK create; SSE is
                # deliberately outside this native boundary's qualification.
                from app.channels.manager import ChannelManager
                from app.channels.message_bus import InboundMessage, MessageBus

                resume = await client.post(f"/api/threads/{item.spec.thread_id}/agent-tasks/{item.spec.agent_task_id}/resume", json={"expected_generation": 3, "idempotency_key": "prior-explicit-resume"})
                assert resume.status_code == 200, resume.text
                calls = []

                class IngressRuns:
                    async def create(self, thread, assistant, **kwargs):
                        result = await sdk.runs.create(thread, assistant, **kwargs)
                        calls.append({"thread": thread, "input": kwargs["input"], "key": kwargs["headers"]["Idempotency-Key"], "run_id": result["run_id"]})
                        return result

                    async def stream(self, thread, assistant, **kwargs):
                        await self.create(thread, assistant, **kwargs)
                        yield SimpleNamespace(event="values", data={"messages": []})

                channel = ChannelManager(MessageBus(), SimpleNamespace(), langgraph_url=url + "/api")
                sdk = channel._get_client()
                patch.setattr(channel, "_resolve_run_params", lambda msg, thread: ("lead_agent", {}, {"user_id": item.spec.user_id}))

                async def policy(msg, context):
                    return SimpleNamespace(fire_and_forget=True, buffer_followups_on_busy=False)

                patch.setattr(channel, "_apply_channel_policy", policy)
                patch.setattr(channel, "_channel_supports_streaming", lambda name: False)
                ingress = SimpleNamespace(runs=IngressRuns())
                msg = InboundMessage(channel_name="slack", chat_id="owned-fixture-chat", user_id="provider-identity", owner_user_id=item.spec.user_id, text="normal signed human", metadata={"event_id": "bc06-normal-owned-event"})
                await channel._handle_chat_on_thread(ingress, msg, item.spec.thread_id)
                assert len(calls) == 1
                await channel._handle_chat_on_thread(ingress, msg, item.spec.thread_id)
                assert calls[-1]["run_id"] == calls[0]["run_id"]
                buffered = InboundMessage(channel_name="slack", chat_id=msg.chat_id, user_id=msg.user_id, owner_user_id=item.spec.user_id, text="buffered signed human", metadata={"event_id": "bc06-buffer-owned-event"})
                channel._buffer_followup(item.spec.thread_id, buffered)
                await channel._drain_followups_for_thread(ingress, item.spec.thread_id, buffered)
                assert len(calls) == 3
                streamed = InboundMessage(channel_name="slack", chat_id=msg.chat_id, user_id=msg.user_id, owner_user_id=item.spec.user_id, text="stream signed human", metadata={"event_id": "bc06-stream-owned-event"})
                await channel._handle_streaming_chat(ingress, streamed, item.spec.thread_id, "lead_agent", {}, {"user_id": item.spec.user_id}, {"role": "user", "content": streamed.text})
                assert len(calls) == 4
                channel_sql = await _bc06_sql(item)
                latest_receipt = next(row for row in channel_sql["fleet_task_operation_receipts"] if row["admitted_run_id"] == calls[-1]["run_id"])
                assert latest_receipt["source_point_generation"] == 3 and latest_receipt["preceding_receipt_id"]
                evidence("channel-human-actual-ingresses", {"calls": calls, "sql": channel_sql, "stream_scope": "original ingress with original SDK create transport fixture; no SSE qualification"})
                from deerflow_ecs_fleet.persistence.models import TaskOperationReceiptRow, WorkspacePointRow

                from app.fleet.operation_sources import receipt_chain

                async with sf() as session:
                    receipt = await session.get(TaskOperationReceiptRow, latest_receipt["id"])
                    point = await session.get(WorkspacePointRow, latest_receipt["source_workspace_point_id"])
                    assert await receipt_chain(session, receipt=receipt, point=point)
                    receipt.user_id = "different-owner-source-fixture"
                    assert not await receipt_chain(session, receipt=receipt, point=point)
                    await session.rollback()
                latest_execution = await execute_continuation(item, tmp_path / "admit-first" / "channel-chain-execution", human=True)
                assert latest_execution["new_exit"] == 0 and "stream signed human" in latest_execution["history"]["human_messages"]
                evidence("channel-current-final-source-execution", {"executed": latest_execution, "wrong_owner_chain_rejected": True, "source_point_generation": latest_receipt["source_point_generation"]})

    # Assigned cancellation retains publication identity until the original
    # native runner settles, and until the trusted exact STOP releases charge.
    with monkeypatch.context() as patch:
        stack, item = await _bc06_case(tmp_path / "assigned-stop", patch)
        async with stack:
            from .c09_integration_fixture import original_native_execution

            native = await original_native_execution(item, tmp_path / "assigned-stop", patch)
            try:
                app, password = await _bc06_http_host(item, patch, stack)
                auth = dict(node_id=item.identity.node_id, node_session_id=item.identity.node_session_id, attempt_id=item.accepted.attempt_id, token=item.accepted.token)
                async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                    await login(client, "bc06-boundary@example.com", password)
                    route = f"/api/threads/{item.spec.thread_id}/agent-tasks/{item.spec.agent_task_id}/cancel"
                    body = {"expected_generation": 1, "idempotency_key": "assigned-exact-stop"}
                    intent = await client.post(route, json=body)
                    assert intent.status_code == 200 and intent.json()["state"] == "cancellation_requested", intent.text
                    before = await _bc06_sql(item)
                    assert before["fleet_agent_tasks"][0]["generation"] == 1
                    assert before["fleet_agent_tasks"][0]["cancel_requested_at"] is None
                    assert before["fleet_run_placements"][0]["active_attempt_id"] == item.accepted.attempt_id
                    assert before["fleet_attempts"][0]["stopped_at"] is None and before["fleet_reservations"][0]["state"] != "released"
                    native.release.set()
                    completed = await native.execute
                    assert completed.status.value == "interrupted" and not completed.ownership_lost
                    settled = await _bc06_sql(item)
                    assert settled["fleet_agent_tasks"][0]["generation"] == 1 and settled["fleet_agent_tasks"][0]["state"] == "finishing"
                    async with item.env[1]() as session:
                        paused_point = (
                            await session.execute(text("SELECT kind,desired_task_status,desired_core_status FROM fleet_workspace_points WHERE id=:id"), {"id": settled["fleet_agent_tasks"][0]["accepted_workspace_point_id"]})
                        ).one()
                    assert paused_point == ("paused", "paused", "interrupted")
                    pending = await client.post(route, json=body)
                    assert pending.json()["state"] == "cancellation_requested"
                    with pytest.raises(ValueError):
                        await app.state.fleet_ownership.stopped(reason="exit", exit_code=0, process_ref=item.grant["process_ref"], physical_stopped=False, **auth)
                    native.stop_node.set()
                    await native.node
                    stopped = await app.state.fleet_ownership.stopped(reason="exit", exit_code=0, process_ref=item.grant["process_ref"], physical_stopped=True, **auth)
                    terminal = await client.post(route, json=body)
                    assert terminal.status_code == 200 and terminal.json()["state"] == "cancelled" and terminal.json()["generation"] == 2, terminal.text
                    after = await _bc06_sql(item)
                    assert after["fleet_reservations"][0]["state"] == "released" and after["fleet_reservations"][0]["released_at"]
                    evidence("assigned-stop-boundary", {"before": before, "settled_before_stop": settled, "stop": stopped, "after": after, "native_status": completed.status.value})
            finally:
                native.release.set()
                native.stop_node.set()
                if not native.execute.done():
                    native.execute.cancel()
                await asyncio.gather(native.execute, return_exceptions=True)
                native.node.cancel()
                await asyncio.gather(native.node, return_exceptions=True)
                with native.scope():
                    await native.environment.close()
                assert native.teardown.closed and not native.teardown._phases

    # Reservation admission rolls back atomically. A later actual mutation
    # failure is a separate transaction and leaves the committed intent blocked.
    with monkeypatch.context() as patch:
        stack, item = await _bc06_case(tmp_path / "neutral-failure", patch)
        async with stack:
            await waiting(item, tmp_path / "neutral-failure", patch, continuation_case="bc06")
            app, password = await _bc06_http_host(item, patch, stack)
            from app.fleet.task_admission import FleetNeutralAdmission

            original_insert = FleetNeutralAdmission.insert

            async def rejected_insert(participant, session, run):
                await original_insert(participant, session, run)
                raise RuntimeError("bc06 fixture failure after original reservation insert")

            async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                await login(client, "bc06-boundary@example.com", password)
                before = await _bc06_sql(item)
                patch.setattr(FleetNeutralAdmission, "insert", rejected_insert)
                failed_admission = await client.put(f"/api/threads/{item.spec.thread_id}/goal", headers={"Idempotency-Key": "atomic-neutral"}, json={"objective": "new neutral objective"})
                assert failed_admission.status_code == 500, failed_admission.text
                rolled_back = await _bc06_sql(item)
                assert before == rolled_back
                patch.setattr(FleetNeutralAdmission, "insert", original_insert)
                old_root = await app.state.checkpointer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id}})

                async def failed_put(*args, **kwargs):
                    raise RuntimeError("bc06 actual checkpoint write fixture failure")

                patch.setattr(app.state.checkpointer, "aput", failed_put)
                failed_mutation = await client.put(f"/api/threads/{item.spec.thread_id}/goal", headers={"Idempotency-Key": "committed-neutral"}, json={"objective": "new neutral objective"})
                assert failed_mutation.status_code == 500, failed_mutation.text
                after = await _bc06_sql(item)
                root = await app.state.checkpointer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id}})
                assert root.config == old_root.config
                assert after["fleet_agent_tasks"][0]["generation"] == 2
                assert after["fleet_task_operation_receipts"][0]["state"] == "blocked"
                assert after["thread_execution_bindings"][0]["recovery_required"]
                assert all(row["state"] == "cancelled" for row in after["fleet_jobs"])
                resume = await client.post(f"/api/threads/{item.spec.thread_id}/agent-tasks/{item.spec.agent_task_id}/resume", json={"expected_generation": 2, "idempotency_key": "cannot-ignore-recovery"})
                assert resume.status_code == 409
                evidence(
                    "neutral-transaction-boundary",
                    {
                        "before": before,
                        "rolled_back": rolled_back,
                        "after": after,
                        "admission_http": failed_admission.status_code,
                        "mutation_http": failed_mutation.status_code,
                        "resume_http": resume.status_code,
                        "unchanged_root": root.config,
                    },
                )

    # Actual checkpoint success has an unpaired new root and remains recovery
    # blocked; actual delete verifies checkpoint and metadata absence separately.
    with monkeypatch.context() as patch:
        stack, item = await _bc06_case(tmp_path / "neutral-success", patch)
        async with stack:
            await waiting(item, tmp_path / "neutral-success", patch, continuation_case="bc06")
            app, password = await _bc06_http_host(item, patch, stack)
            old_root = await app.state.checkpointer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id}})
            async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                await login(client, "bc06-boundary@example.com", password)
                result = await client.put(f"/api/threads/{item.spec.thread_id}/goal", headers={"Idempotency-Key": "successful-neutral"}, json={"objective": "actual new checkpoint objective"})
                assert result.status_code == 200, result.text
                new_root = await app.state.checkpointer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id}})
                assert new_root.config != old_root.config
                after = await _bc06_sql(item)
                assert after["fleet_task_operation_receipts"][0]["state"] == "blocked" and after["thread_execution_bindings"][0]["recovery_required"]
                evidence("neutral-success-unpaired-root", {"sql": after, "http": result.status_code, "old_root": old_root.config, "new_root": new_root.config})
    with monkeypatch.context() as patch:
        stack, item = await _bc06_case(tmp_path / "neutral-delete", patch)
        async with stack:
            await waiting(item, tmp_path / "neutral-delete", patch, continuation_case="bc06")
            app, password = await _bc06_http_host(item, patch, stack)
            async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                await login(client, "bc06-boundary@example.com", password)
                result = await client.delete(f"/api/threads/{item.spec.thread_id}", headers={"Idempotency-Key": "actual-delete"})
                assert result.status_code == 200, result.text
                assert await app.state.checkpointer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id}}) is None
                assert await app.state.thread_store.get(item.spec.thread_id, user_id=item.spec.user_id) is None
                after = await _bc06_sql(item)
                assert after["fleet_agent_tasks"][0]["state"] == "cancelled" and after["fleet_task_operation_receipts"][0]["state"] == "completed"
                evidence("neutral-actual-delete", {"sql": after, "http": result.status_code, "root_absent": True, "thread_meta_absent": True})

    # A real started child exits, but its original STOP has not arrived. The
    # original lease reconciler preserves uncertain charge; no SQL success seed.
    with monkeypatch.context() as patch:
        stack, item = await _bc06_case(tmp_path / "unknown-charge", patch)
        async with stack:
            await waiting(item, tmp_path / "unknown-charge", patch, continuation_case="bc06")
            runtime = item.env[3]
            await runtime.nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
            app, password = await _bc06_http_host(item, patch, stack)
            from deerflow_ecs_fleet.persistence.attempts import JobAttempts

            class HeldUnknownStop(Exception):
                pass

            async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                await login(client, "bc06-boundary@example.com", password)

                async def expire_before_stop(attempts, **identity):
                    # Explicit clock-failure fixture premise, after actual start
                    # and child exit, before any trusted STOP/release receipt.
                    async with item.env[1].begin() as session:
                        await session.execute(text("UPDATE fleet_attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=:id"), {"id": identity["attempt_id"]})
                    await attempts.expire_pending()
                    before = await _bc06_sql(item)
                    uncertain = next(row for row in before["fleet_attempts"] if row["id"] == identity["attempt_id"])
                    charged = next(row for row in before["fleet_reservations"] if row["attempt_id"] == identity["attempt_id"])
                    assert uncertain["state"] == "unknown" and uncertain["stopped_at"] is None
                    assert charged["state"] == "quarantined" and charged["released_at"] is None
                    message = await client.post(
                        f"/api/threads/{item.spec.thread_id}/runs", headers={"Idempotency-Key": "unknown-cannot-supersede"}, json={"input": {"messages": [{"role": "user", "content": "must not bypass uncertain charge"}]}}
                    )
                    assert message.status_code == 200, message.text
                    after = await _bc06_sql(item)
                    assert after["fleet_agent_tasks"][0]["generation"] == 2
                    assert next(row for row in after["fleet_attempts"] if row["id"] == identity["attempt_id"])["state"] == "unknown"
                    assert next(row for row in after["fleet_reservations"] if row["attempt_id"] == identity["attempt_id"])["released_at"] is None
                    evidence("unknown-charge-boundary", {"before": before, "after": after, "human_http": message.status_code, "premise": "real child exit0; lease expired by isolated clock fixture before original STOP"})
                    raise HeldUnknownStop()

                patch.setattr(JobAttempts, "stopped", expire_before_stop)
                with pytest.raises(HeldUnknownStop):
                    await complete_children(item.env[1], runtime.config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id, max_jobs=1)
