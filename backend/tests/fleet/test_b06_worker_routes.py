"""Host worker routes require both node and attempt scoped identities."""

import importlib
from types import SimpleNamespace

import httpx
import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware

from .test_b02_fleet_foundation import service_class, settings
from .test_b04_fleet_foundation import seed


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_claim_start_renew_stop_routes(fleet_database, tmp_path):
    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    await seed(engine)
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,session_id) VALUES ('other','other',1000,512,'os')"))
    node = await fleet.credentials.issue("n", lifetime_seconds=600)
    other = await fleet.credentials.issue("other", lifetime_seconds=600)
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        headers = {"Authorization": "Bearer " + node.token}
        response = await client.post("/api/fleet/node/claims", headers=headers, json={"node_session_id": "s"})
        assert response.status_code == 200, response.text
        claim = response.json()
        assert claim["kind"] == "job"
        path = "/api/fleet/node/attempts/" + claim["attempt_id"]
        identity = {"node_session_id": "s", "token": claim["token"]}
        assert (await client.post(path + "/start", headers={"Authorization": "Bearer " + other.token}, json=identity | {"node_session_id": "os"})).status_code == 403
        assert (await client.post(path + "/start", headers=headers, json=identity | {"token": "bad"})).status_code == 403
        assert (await client.post(path + "/start", headers=headers, json=identity | {"user_id": "foreign"})).status_code == 422
        grant = await client.post(path + "/start", headers=headers, json=identity)
        assert grant.status_code == 200, grant.text
        assert grant.json()["process_ref"] == "fleet-" + claim["attempt_id"]
        renewal = await client.post(path + "/renew", headers=headers, json=identity | {"running": True})
        assert renewal.status_code == 200 and not renewal.json()["stop"]
        await fleet.nodes.set_admin_state("n", "draining")
        assert (await client.post(path + "/renew", headers=headers, json=identity | {"running": True})).status_code == 200
        assert (await client.post("/api/fleet/node/claims", headers=headers, json={"node_session_id": "s"})).status_code == 204
        stop = await client.post(path + "/stopped", headers=headers, json=identity | {"reason": "exit", "exit_code": 1})
        assert stop.status_code == 200 and stop.json()["state"] == "failed"
    await fleet.stop()
