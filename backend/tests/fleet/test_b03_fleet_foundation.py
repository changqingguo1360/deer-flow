"""Real node credentials never acquire a user or administrator identity."""

import importlib
import importlib.util
from types import SimpleNamespace

import httpx
import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware

from .test_b02_fleet_foundation import service_class, settings


def require_auth_implementation():
    assert importlib.util.find_spec("app.gateway.fleet_auth") is not None, "Host-scoped Fleet node authentication missing"
    return importlib.import_module("app.gateway.fleet_auth")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_b03_contract(fleet_database, tmp_path, monkeypatch):
    require_auth_implementation()
    engine, sf, _schema = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib) VALUES ('node-a','worker-a',1000,512), ('node-b','worker-b',1000,512)"))
    credentials = importlib.import_module("deerflow_ecs_fleet.node_credentials").NodeCredentials(sf)
    issued = await credentials.issue("node-a", lifetime_seconds=600)
    assert "df_fleet_" in issued.token
    assert issued.token not in repr(issued)
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("deerflow_ecs_fleet:install", service),))
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    router = importlib.import_module("app.gateway.routers.fleet_nodes").router
    app.include_router(router)

    @app.post("/api/fleet/machines")
    async def management():
        raise AssertionError("Node credential reached a management handler")

    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.post("/api/fleet/node/session", json={"protocol_version": 1})).status_code == 401
        assert (await client.post("/api/fleet/node/session", headers={"Authorization": "Bearer bad"}, json={"protocol_version": 1})).status_code == 401
        headers = {"Authorization": "Bearer " + issued.token}
        response = await client.post("/api/fleet/node/session", headers=headers, json={"protocol_version": 1})
        assert response.status_code == 200, response.text
        session_id = response.json()["node_session_id"]
        response = await client.post("/api/fleet/node/heartbeat", headers=headers, json={"node_session_id": session_id, "protocol_version": 1})
        assert response.status_code == 200
        assert (await client.post("/api/fleet/machines", headers=headers, json={})).status_code == 403
        assert (await client.post("/api/fleet/node/session", headers=headers | {"Cookie": "access_token=anything"}, json={"protocol_version": 1})).status_code == 401
        await credentials.revoke(issued.credential_id)
        assert (await client.post("/api/fleet/node/heartbeat", headers=headers, json={"node_session_id": session_id, "protocol_version": 1})).status_code == 401
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT session_id FROM fleet_nodes WHERE id='node-b'"))).scalar_one() is None
    await service.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fleet_node_routes_respect_root_path(fleet_database, tmp_path):
    require_auth_implementation()
    engine, sf, _schema = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib) VALUES ('node-a','worker-a',1000,512)"))
    credentials = importlib.import_module("deerflow_ecs_fleet.node_credentials").NodeCredentials(sf)
    issued = await credentials.issue("node-a", lifetime_seconds=600)
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("deerflow_ecs_fleet:install", service),))
    app.add_middleware(AuthMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, root_path="/prefix"), base_url="http://test/prefix") as client:
        response = await client.post("/api/fleet/node/session", headers={"Authorization": "Bearer " + issued.token}, json={"protocol_version": 1})
        assert response.status_code == 200, response.text
    await service.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_rotation_never_waits_for_execution_lock(fleet_database, tmp_path):
    import asyncio

    from deerflow_ecs_fleet.persistence.models import AttemptRow
    from sqlalchemy import select

    engine, sf, _ = fleet_database
    service = service_class()(settings(tmp_path))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,session_id) VALUES ('n','n',1000,512,'old')"))
        await conn.execute(text("INSERT INTO fleet_jobs (id,user_id,thread_id,tracking_task_id,idempotency_key,spec) VALUES ('j','u','t','tracking','key','{}')"))
        await conn.execute(text("INSERT INTO fleet_attempts (id,kind,job_id,attempt_no,node_id,node_session_id,token_hash,state,output_prefix) VALUES ('a','job','j',1,'n','old','hash','running','a')"))
        await conn.execute(text("INSERT INTO fleet_reservations (id,node_id,attempt_id,cpu_millis,memory_mib,state) VALUES ('r','n','a',1000,512,'active')"))
    async with sf.begin() as session:
        await session.execute(select(AttemptRow).where(AttemptRow.id == "a").with_for_update())
        try:
            rotated = await asyncio.wait_for(service.nodes.open_session("n", protocol_version=1), timeout=1)
        except TimeoutError:
            pytest.fail("Node session rotation inverted execution -> node lock order")
    assert rotated["reconcile_required"]
    heartbeat = await service.nodes.heartbeat("n", node_session_id=rotated["node_session_id"], protocol_version=1)
    assert heartbeat["health"] == "unknown"
    with pytest.raises(ValueError, match="Stale"):
        await service.nodes.heartbeat("n", node_session_id="old", protocol_version=1)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT state FROM fleet_attempts WHERE id='a'"))).scalar_one() == "running"
        assert (await conn.execute(text("SELECT state FROM fleet_reservations WHERE id='r'"))).scalar_one() == "active"
    await service.stop()
