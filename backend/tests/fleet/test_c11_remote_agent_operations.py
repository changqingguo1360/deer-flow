"""C11 mounted authenticated HTTP and native durable lifecycle proof."""

import json
import os
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text

from .test_c10_remote_agent_admission import admission as admission
from .test_c10_remote_agent_admission import c10_environment as c10_environment


@pytest.mark.asyncio
async def test_c11_owned_summary_and_closed_admission_main(c10_environment):
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility

    from app.gateway.auth.jwt import create_access_token

    item = c10_environment
    app, sf, user = item.app, item.sf, item.user
    from deerflow.runtime import RunManager

    # Production constructs the manager after installed store/control hooks.
    app.state.run_manager = RunManager(store=app.state.run_store)
    # Mount exactly the production router once it exists; before implementation
    # the legal owned GET proves missing capability after normal setup succeeds.
    try:
        from app.gateway.routers.fleet_agent_tasks import router
    except ModuleNotFoundError as exc:
        if exc.name != "app.gateway.routers.fleet_agent_tasks":
            raise
    else:
        app.include_router(router)
    observations = {"artifact_inputs": {"profile_image": item.image, "runtime_digest": item.compatibility["runtime_digest"]}}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://native", cookies={"access_token": create_access_token(user.id), "csrf_token": "c10-csrf"}, headers={"X-CSRF-Token": "c10-csrf"}) as client:
        admitted = await client.post("/api/threads/thread-c10-http/runs", json={"input": {"messages": [{"role": "user", "content": "C11 original"}]}, "execution": {"preference": "remote", "profile": "remote"}})
        assert admitted.status_code == 200, admitted.text
        run_id = admitted.json()["run_id"]
        response = await client.get("/api/threads/thread-c10-http/agent-tasks")
        assert response.status_code == 200, response.text
        observations["queued"] = response.json()
        summary = response.json()[0]
        assert summary["current_run_id"] == run_id and summary["state"] == "queued" and summary["stop_state"] == "not_started"
        await item.runtime.nodes.register(node_id="node-c11", name="node-c11", cpu_millis=2000, memory_mib=4096, agent_limit=2, profile_allowlist=["remote"])
        node = await item.runtime.nodes.open_session("node-c11", protocol_version=1)
        await item.runtime.nodes.heartbeat("node-c11", node_session_id=node["node_session_id"], protocol_version=1)
        owner = app.state.fleet_ownership
        worker = WorkerCompatibility.model_validate(item.compatibility)
        accepted = await owner.claim_agent("node-c11", node_session_id=node["node_session_id"], worker=worker)
        assert accepted is not None and accepted.run_id == run_id
        identity = dict(node_id="node-c11", node_session_id=node["node_session_id"], attempt_id=accepted.attempt_id, token=accepted.token)
        grant = await owner.authorize_start(**identity)
        await owner.renew(running=True, **identity)
        response = await client.get("/api/threads/thread-c10-http/agent-tasks/" + summary["task_id"])
        observations["running"] = response.json()
        assert response.status_code == 200 and response.json()["run_status"] == "running" and response.json()["resources_held"] and response.json()["stop_state"] == "unconfirmed"
        cancelled = await client.post(f"/api/threads/thread-c10-http/runs/{run_id}/cancel?wait=false")
        assert cancelled.status_code == 202, cancelled.text
        response = await client.get("/api/threads/thread-c10-http/agent-tasks")
        observations["cancel_intent"] = response.json()
        assert response.json()[0]["cancel_requested"] and response.json()[0]["resources_held"] and response.json()[0]["stop_state"] == "unconfirmed"
        await app.state.thread_store.create("thread-c11-accepted", user_id=user.id)
        waiting = await client.post("/api/threads/thread-c11-accepted/runs", json={"input": {"messages": [{"role": "user", "content": "accepted queued"}]}, "execution": {"preference": "remote", "profile": "remote"}})
        assert waiting.status_code == 200, waiting.text
        closed = item.runtime.config.model_copy(update={"agents_enabled": False, "jobs_enabled": False})
        item.runtime.config = closed
        app.state.fleet_routing_config = owner.config = app.state.fleet_scheduler_tickets.config = app.state.bound_run_execution_backend.config = closed
        await app.state.thread_store.create("thread-c11-closed", user_id=user.id)
        rejected = await client.post("/api/threads/thread-c11-closed/runs", json={"input": {"messages": [{"role": "user", "content": "closed new C"}]}, "execution": {"preference": "remote", "profile": "remote"}})
        assert rejected.status_code == 503, rejected.text
        queued_claim = await owner.claim_agent("node-c11", node_session_id=node["node_session_id"], worker=worker)
        assert queued_claim is not None and queued_claim.run_id == waiting.json()["run_id"]
        assert (await owner.renew(**identity))["control"] == "cancel"
        assert (await owner.stopped(reason="exit", exit_code=0, process_ref=grant["process_ref"], physical_stopped=True, **identity))["stopped"]
        assert (await owner.reconcile_stopped(original_node_session_id=node["node_session_id"], reason="exit", exit_code=0, process_ref=grant["process_ref"], physical_stopped=True, **identity))["stopped"]
        response = await client.get("/api/threads/thread-c10-http/agent-tasks")
        observations["after_stop"] = response.json()
        assert response.json()[0]["stop_state"] == "confirmed" and not response.json()[0]["resources_held"] and response.json()[0]["recovery_required"]
        async with sf() as session:
            rows = (
                await session.execute(
                    text(
                        "SELECT t.state,r.status,r.cancel_requested_at,a.stopped_at,v.state AS reservation_state, a.launch_spec->'execution_profile'->>'image' AS accepted_image, "
                        "a.launch_spec->'launch_spec'->>'runtime_digest' AS accepted_runtime_digest FROM fleet_agent_tasks t JOIN runs r ON r.run_id=t.current_run_id "
                        "JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_attempts a ON a.id=p.active_attempt_id "
                        "JOIN fleet_reservations v ON v.attempt_id=a.id WHERE r.run_id=:id"
                    ),
                    {"id": run_id},
                )
            ).one()
            observations["actual_rows"] = dict(rows._mapping)
    destination = Path(os.environ.get("C11_EVIDENCE_DIR", str(item.tmp_path)))
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "main-observations.json").write_text(json.dumps(observations, indent=2, default=str))


@pytest.mark.asyncio
async def test_never_assigned_terminal_stream_requires_original_empty_proof(c11_stream_environment):
    import asyncio
    from dataclasses import replace

    from app.fleet.events import FleetGatewayBridge, FleetStreamReader, InvalidRemoteCursor
    from deerflow.runtime import RunManager
    from deerflow.runtime.stream_bridge.base import END_SENTINEL, HEARTBEAT_SENTINEL
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = c11_stream_environment
    app = item.app
    app.state.run_manager = RunManager(store=app.state.run_store)
    reader = FleetStreamReader(item.sf)
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://native", headers=item.headers, cookies={"csrf_token": "c10-csrf"}) as client:
        result = await client.post("/api/threads/thread-c10-http/runs", json={"input": {"messages": [{"role": "user", "content": "never assigned"}]}, "execution": {"preference": "remote", "profile": "remote"}})
        assert result.status_code == 200, result.text
        run_id = result.json()["run_id"]
        record = await app.state.run_manager.get(run_id, user_id=item.user.id)
        prepared = await reader.prepare(record, None)
        stream = bridge.subscribe_prepared(run_id, prepared, heartbeat_interval=0.01)
        try:
            assert await asyncio.wait_for(anext(stream), 1) == HEARTBEAT_SENTINEL
            cancel = await client.post(f"/api/threads/thread-c10-http/runs/{run_id}/cancel")
            assert cancel.status_code == 202, cancel.text
            assert await asyncio.wait_for(anext(stream), 1) == END_SENTINEL
            with pytest.raises(StopAsyncIteration):
                await anext(stream)
        finally:
            await stream.aclose()
        reconnect = bridge.subscribe_prepared(run_id, await reader.prepare(record, None))
        try:
            assert await asyncio.wait_for(anext(reconnect), 1) == END_SENTINEL
        finally:
            await reconnect.aclose()
        with pytest.raises(InvalidRemoteCursor):
            await reader.prepare(replace(record, user_id="foreign"), None)
        # Controlled inconsistent core pair must close without accepted END.
        async with item.sf.begin() as session:
            await session.execute(text("UPDATE runs SET status='error' WHERE run_id=:id"), {"id": run_id})
        mismatch = bridge.subscribe_prepared(run_id, prepared)
        try:
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(mismatch), 1)
        finally:
            await mismatch.aclose()


@pytest_asyncio.fixture
async def c11_stream_environment(c10_environment):
    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer

    item = c10_environment
    config = AppConfig.model_validate(
        {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "database": {"backend": "postgres", "postgres_url": item.engine.url.render_as_string(hide_password=False), "postgres_schema": item.schema}}
    )
    async with item.engine.begin() as connection:
        await connection.run_sync(lambda sync: RunEventRow.__table__.create(sync))
    async with make_checkpointer(config):
        yield item


@pytest.mark.asyncio
async def test_owned_allowlist_optional_runtime_and_real_local_execution(c10_environment, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from uuid import uuid4

    from _agent_e2e_helpers import FakeToolCallingModel
    from langchain_core.messages import AIMessage

    from app.gateway.auth.jwt import create_access_token
    from app.gateway.auth.models import User
    from app.gateway.routers.fleet_agent_tasks import router
    from deerflow.config.app_config import AppConfig, set_app_config
    from deerflow.runtime import RunManager

    item = c10_environment
    app, user = item.app, item.user
    app.include_router(router)
    app.state.run_manager = RunManager(store=app.state.run_store)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://native", cookies={"access_token": create_access_token(user.id), "csrf_token": "c10-csrf"}, headers={"X-CSRF-Token": "c10-csrf"}) as client:
        admitted = await client.post("/api/threads/thread-c10-http/runs", json={"input": {"messages": [{"role": "user", "content": "owned summary"}]}, "execution": {"preference": "remote", "profile": "remote"}})
        assert admitted.status_code == 200
        summary = (await client.get("/api/threads/thread-c10-http/agent-tasks")).json()[0]
        assert set(summary) == {"task_id", "state", "current_run_id", "generation", "run_status", "profile", "location", "cancel_requested", "recovery_required", "stop_state", "resources_held"}
        await app.state.thread_store.create("thread-c11-empty", user_id=user.id)
        assert (await client.get("/api/threads/thread-c11-empty/agent-tasks/" + summary["task_id"])).status_code == 404
        outsider_id = uuid4()
        await item.users.create_user(User(id=outsider_id, email="c11-other@example.com", system_role="user"))
        client.cookies.set("access_token", create_access_token(str(outsider_id)))
        assert (await client.get("/api/threads/thread-c10-http/agent-tasks")).status_code == 404
        client.cookies.set("access_token", create_access_token(user.id))
        extensions = app.state.extensions
        app.state.extensions = SimpleNamespace(services=())
        assert (await client.get("/api/threads/thread-c11-empty/agent-tasks")).json() == []
        app.state.extensions = extensions
        item.runtime.ready = False
        assert (await client.get("/api/threads/thread-c10-http/agent-tasks")).status_code == 503
        item.runtime.ready = True
        from deerflow_extension_api import ExtensionData

        from deerflow.extensions.registry import LoadedExtensions
        from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

        app.state.extensions = LoadedExtensions(app_store=ExtensionData("c11-local"), services=extensions.services)
        app.state.stream_bridge = MemoryStreamBridge()
        # Closing only C leaves the actual original Local graph/worker available.
        closed = item.runtime.config.model_copy(update={"agents_enabled": False})
        item.runtime.config = app.state.fleet_routing_config = closed
        set_app_config(
            AppConfig.model_validate(
                {"models": [{"name": "local-proof", "use": "langchain_openai:ChatOpenAI", "model": "unused"}], "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "title": {"enabled": False}, "memory": {"enabled": False}}
            )
        )
        model = FakeToolCallingModel(responses=[AIMessage(content="C11 real local result")])
        monkeypatch.setattr("deerflow.agents.lead_agent.agent.create_chat_model", lambda *args, **kwargs: model)
        await app.state.thread_store.create("thread-c11-local", user_id=user.id)
        result = await client.post("/api/threads/thread-c11-local/runs", json={"input": {"messages": [{"role": "user", "content": "local remains available"}]}, "config": {"configurable": {"model_name": "local-proof"}}})
        assert result.status_code == 200, result.text
        record = await app.state.run_manager.get(result.json()["run_id"], user_id=user.id)
        assert record.task is not None and not record.store_only
        await asyncio.wait_for(record.task, 20)
        durable = await app.state.run_store.get(record.run_id, user_id=user.id)
        assert durable["status"] == "success", durable.get("error")
        checkpoint = await app.state.checkpointer.aget_tuple({"configurable": {"thread_id": "thread-c11-local"}})
        messages = checkpoint.checkpoint["channel_values"]["messages"]
        assert any(getattr(message, "content", None) == "C11 real local result" for message in messages)
        destination = Path(os.environ.get("C11_EVIDENCE_DIR", str(item.tmp_path)))
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "local-observation.json").write_text(
            json.dumps(
                {
                    "run_id": record.run_id,
                    "actual_durable_status": durable["status"],
                    "actual_last_message": messages[-1].content,
                    "agents_enabled": closed.agents_enabled,
                    "jobs_enabled": closed.jobs_enabled,
                    "scope": "original RunManager/run_agent/lead_agent graph; only external model deterministic",
                },
                indent=2,
            )
        )
