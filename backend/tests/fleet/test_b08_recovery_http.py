"""Recovery management keeps session admin/CSRF outside worker authentication."""

import importlib
import importlib.util
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.internal_auth import INTERNAL_AUTH_HEADER_NAME

from .test_b06_fleet_durable_jobs import serve
from .test_b08_recovery import unknown_attempt


def recovery_router():
    assert importlib.util.find_spec("app.gateway.routers.fleet_recovery") is not None, "Host admin recovery route missing"
    return importlib.import_module("app.gateway.routers.fleet_recovery").router


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recovery_admin_session_csrf_and_audit_over_http(fleet_database, tmp_path, monkeypatch):
    router = recovery_router()
    fleet, claim, args, manager, command = await unknown_attempt(fleet_database, tmp_path, stopped=True)
    credential = await fleet.credentials.issue("n", lifetime_seconds=600)
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(router)

    async def session_user(request):
        identity = request.cookies.get("access_token")
        if identity not in {"admin", "member"}:
            raise HTTPException(status_code=401)
        return SimpleNamespace(id=identity, email=identity + "@test.local", system_role="admin" if identity == "admin" else "user", needs_setup=False)

    monkeypatch.setattr("app.gateway.deps.get_current_user_from_request", session_user)
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "")
    server, task, sock, url = await serve(app)
    base = "/api/fleet/recovery/jobs"
    endpoint = base + "/" + claim.job_id + "/resolve"
    body = {"expected_attempt_id": claim.attempt_id, "note": "Reviewed process and external effects", "side_effects_reviewed": True}
    admin = {"Cookie": "access_token=admin; csrf_token=recovery-test", "X-CSRF-Token": "recovery-test"}
    try:
        async with httpx.AsyncClient(base_url=url) as client:
            assert (await client.get(base)).status_code == 401
            assert (await client.get(base, headers={"Cookie": "access_token=member"})).status_code == 403
            worker = {"Authorization": "Bearer " + credential.token}
            assert (await client.get(base, headers=worker)).status_code == 403
            assert (await client.post(endpoint, headers=worker, json=body)).status_code == 403
            assert (await client.post(endpoint, headers={"Cookie": "access_token=admin"}, json=body)).status_code == 403
            listing = await client.get(base, headers=admin)
            assert listing.status_code == 200, listing.text
            assert listing.json()["items"][0]["stop_confirmed"] is True
            assert listing.json()["items"][0]["capacity_released"] is True
            assert "token" not in listing.text and "output_prefix" not in listing.text
            assert (await client.post(endpoint, headers=admin, json=body | {"operator_id": "forged"})).status_code == 422
            assert (await client.post(endpoint, headers=admin, json=body | {"side_effects_reviewed": False})).status_code == 422
            assert (await client.post(endpoint, headers=admin, json=body | {"side_effects_reviewed": 1})).status_code == 422
            monkeypatch.setattr("app.gateway.auth_middleware.is_valid_internal_auth_token", lambda token: token == "trusted-internal")
            assert (await client.get(base, headers={INTERNAL_AUTH_HEADER_NAME: "trusted-internal"})).status_code == 403
            assert (await client.post(endpoint, headers={INTERNAL_AUTH_HEADER_NAME: "trusted-internal"}, json=body)).status_code == 403
            assert (await client.post(endpoint, headers=admin, json=body | {"expected_attempt_id": "stale"})).status_code == 409
            assert (await client.post(base + "/missing/resolve", headers=admin, json=body)).status_code == 404
            first = await client.post(endpoint, headers=admin, json=body)
            assert first.status_code == 200, first.text
            assert first.json()["state"] == "failed"
            assert (await client.post(endpoint, headers=admin, json=body)).json() == first.json()
            assert (await client.get(base, headers=admin)).json() == {"items": []}
            events = await client.get(base + "/" + claim.job_id + "/events", headers=admin)
            assert events.status_code == 200, events.text
            assert len(events.json()["items"]) == 1
            assert events.json()["items"][0]["operator_id"] == "admin"
            assert events.json()["items"][0]["note"] == body["note"]
            monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
            assert (await client.get(base, headers=worker)).status_code == 403
            assert (await client.get(base)).status_code == 403
            assert (await client.get(base, headers={"Cookie": "access_token=invalid"})).status_code == 403
        engine, _, _ = fleet_database
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT operator_id FROM fleet_recovery_events"))).scalar_one() == "admin"
    finally:
        server.should_exit = True
        await task
        sock.close()
        await fleet.stop()


def test_gateway_mounts_recovery_management_routes():
    recovery_router()
    from app.gateway.app import create_app

    paths = {route.path for route in create_app().routes}
    assert "/api/fleet/recovery/jobs" in paths
    assert "/api/fleet/recovery/jobs/{job_id}/resolve" in paths
    assert "/api/fleet/recovery/jobs/{job_id}/events" in paths
