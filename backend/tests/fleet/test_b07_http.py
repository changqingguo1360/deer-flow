"""Authenticated host routes complete once and enforce user/thread artifact access."""

import importlib
import importlib.util
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

from .test_b06_fleet_durable_jobs import serve
from .test_b07_fleet_durable_jobs import sealed_attempt


def require_routes():
    assert importlib.util.find_spec("app.gateway.routers.fleet_artifacts") is not None, "Host Fleet artifact routes missing"
    return importlib.import_module("app.gateway.routers.fleet_artifacts").router


@pytest.mark.integration
@pytest.mark.asyncio
async def test_complete_and_download_across_real_http(fleet_database, tmp_path, monkeypatch):
    artifact_router = require_routes()
    assert importlib.util.find_spec("app.gateway.routers.fleet_inputs") is not None, "Host immutable input upload route missing"
    fleet, claim, args, manifest = await sealed_attempt(fleet_database, tmp_path)
    credential = await fleet.credentials.issue("n", lifetime_seconds=600)
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    engine, sf, _ = fleet_database
    async with engine.begin() as conn:
        await conn.run_sync(lambda connection: ThreadMetaRow.__table__.create(connection, checkfirst=True))
    app.state.thread_store = ThreadMetaRepository(sf)
    await app.state.thread_store.create("t", user_id="u")
    await app.state.thread_store.create("other-thread", user_id="u")
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    app.include_router(artifact_router)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_inputs").router)

    async def session_user(request):
        user = request.cookies.get("access_token")
        if user not in {"u", "other"}:
            raise HTTPException(status_code=401)
        return SimpleNamespace(id=user, email=user + "@test.local", system_role="admin", needs_setup=False)

    monkeypatch.setattr("app.gateway.deps.get_current_user_from_request", session_user)
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "")
    server, task, sock, url = await serve(app)
    try:
        async with httpx.AsyncClient(base_url=url) as client:
            endpoint = "/api/fleet/node/attempts/" + claim.attempt_id + "/complete"
            body = {key: args[key] for key in ("node_session_id", "token")} | {"manifest": manifest}
            headers = {"Authorization": "Bearer " + credential.token}
            first = await client.post(endpoint, headers=headers, json=body)
            assert first.status_code == 200, first.text
            second = await client.post(endpoint, headers=headers, json=body)
            assert second.json() == first.json()
            manifest_id = first.json()["manifest_id"]
            base = "/api/threads/t/fleet/manifests/" + manifest_id
            assert (await client.get(base)).status_code == 401
            assert (await client.get(base, headers=headers)).status_code == 403
            owner = {"Cookie": "access_token=u"}
            upload_headers = {"Cookie": "access_token=u; csrf_token=fleet-test", "X-CSRF-Token": "fleet-test"}
            inputs = "/api/threads/t/fleet/inputs"
            uploaded = await client.post(inputs, headers=upload_headers, files={"files": ("data.txt", b"original")})
            assert uploaded.status_code == 201, uploaded.text
            input_id = uploaded.json()["id"]
            later = await client.post(inputs, headers=upload_headers, files={"files": ("data.txt", b"later")})
            assert later.status_code == 201 and later.json()["id"] != input_id
            assert (await client.get(inputs + "/" + input_id, headers=owner)).json()["files"] == uploaded.json()["files"]
            assert (await client.get(inputs + "/" + input_id, headers={"Cookie": "access_token=other"})).status_code == 404
            assert (await client.get(inputs.replace("/threads/t/", "/threads/other-thread/") + "/" + input_id, headers=owner)).status_code == 404
            assert (await client.post(inputs, headers=headers, files={"files": ("data.txt", b"x")})).status_code == 403
            assert (await client.post(inputs, headers=upload_headers, files={"files": ("../escape", b"x")})).status_code == 409
            public = await client.get(base, headers=owner)
            assert public.status_code == 200, public.text
            assert "output_prefix" not in public.json() and "user_id" not in public.json()
            assert public.json()["files"] == manifest["files"]
            download = await client.get(base + "/files/report.txt", headers=owner)
            assert download.status_code == 200 and download.content == b"report ready"
            assert download.headers["content-disposition"].startswith("attachment;")
            assert download.headers["x-content-type-options"] == "nosniff"
            assert (await client.get(base, headers={"Cookie": "access_token=other"})).status_code == 404
            assert (await client.get(base + "/files/report.txt", headers={"Cookie": "access_token=other"})).status_code == 404
            assert (await client.get(base.replace("/threads/t/", "/threads/other-thread/") + "/files/report.txt", headers=owner)).status_code == 404
            changed = manifest | {"total_bytes": 0, "files": []}
            assert (await client.post(endpoint, headers=headers, json=body | {"manifest": changed})).status_code == 409
            path = tmp_path / manifest["output_prefix"] / "report.txt"
            path.parent.chmod(0o700)
            path.unlink()
            outside = tmp_path / "outside"
            outside.write_text("report ready")
            path.symlink_to(outside)
            assert (await client.get(base + "/files/report.txt", headers=owner)).status_code == 404
    finally:
        server.should_exit = True
        await task
        sock.close()
        await fleet.stop()


def test_gateway_mounts_fleet_manifest_routes():
    require_routes()
    from app.gateway.app import create_app

    paths = {route.path for route in create_app().routes}
    assert "/api/threads/{thread_id}/fleet/inputs" in paths
    assert "/api/threads/{thread_id}/fleet/inputs/{input_id}" in paths
    assert "/api/threads/{thread_id}/fleet/manifests/{manifest_id}" in paths
    assert "/api/threads/{thread_id}/fleet/manifests/{manifest_id}/files/{relative_path:path}" in paths
