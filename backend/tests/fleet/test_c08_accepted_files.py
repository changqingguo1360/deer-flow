"""Original accepted SQL points drive real owner-scoped artifact HTTP."""

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from langgraph.store.memory import InMemoryStore

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c08_terminal_pair import prepared_pair as prepared_pair


def archive():
    import io
    import zipfile

    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as output:
        output.writestr("sample/SKILL.md", b"# accepted skill")
        output.writestr("sample/image.svg", b"<svg></svg>")
    return stream.getvalue()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepared_pair",
    [dict(kind="partial", files={"outputs/result.txt": b"accepted content", "outputs/page.html": b"<html>accepted</html>", "outputs/raw.dat": b"\0\xff", "outputs/plain": b"plain", "outputs/sample.skill": archive()})],
    indirect=True,
)
async def test_original_artifact_http_uses_immutable_partial_descriptor_range_and_owner(prepared_pair):
    from app.fleet.ownership import install_fleet_ownership
    from app.fleet.workspace import FleetWorkspaceTerminalParticipant
    from app.gateway.routers import artifacts, fleet_artifacts
    from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore

    p = prepared_pair
    terminal = FleetWorkspaceTerminalParticipant(p.capability, controller=p.controller)
    with p.scope():
        async with p.sf.begin() as session:
            await terminal.accept_partial(session, p.identity, p.candidate, barrier_epoch=1)
    original_app = p.item.env[4]
    app = FastAPI()
    original_app.state.extensions.services[0][1].config = original_app.state.extensions.services[0][1].config.model_copy(update={"nas_root": p.versions.nas.root, "nas_identity": "task4"})
    app.state.extensions = original_app.state.extensions
    app.state.run_store = original_app.state.run_store
    app.state.thread_store = MemoryThreadMetaStore(InMemoryStore())
    await app.state.thread_store.create(p.identity.thread_id, user_id=p.identity.user_id)
    install_fleet_ownership(app, p.sf)

    @app.middleware("http")
    async def fixture_authenticated_principal(request, call_next):
        request.state.user = SimpleNamespace(id=request.headers.get("x-fixture-user", p.identity.user_id), system_role="admin")
        request.state.auth_source = "session"
        return await call_next(request)

    app.include_router(artifacts.router)
    app.include_router(fleet_artifacts.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        url = f"/api/threads/{p.identity.thread_id}/artifacts/mnt/user-data/outputs/result.txt"
        response = await client.get(url, headers={"Range": "bytes=0-7"})
        assert response.status_code == 206, response.text
        assert response.content == b"accepted"
        assert response.headers["x-deerflow-workspace-partial"] == "true"
        assert response.headers["x-deerflow-workspace-manifest"] == p.candidate.manifest_id
        assert response.headers["x-deerflow-workspace-checkpoint"] == p.identity.checkpoint_id
        assert response.headers["content-type"].startswith("text/plain")
        assert (await client.get(url, headers={"x-fixture-user": "other-owner"})).status_code == 404
        metadata = await client.get(f"/api/threads/{p.identity.thread_id}/fleet/manifests/{p.candidate.manifest_id}")
        assert metadata.status_code == 200, metadata.text
        assert metadata.json()["partial"] is True and metadata.json()["checkpoint_id"] == p.identity.checkpoint_id
        assert metadata.json()["manifest_id"] == p.candidate.manifest_id
        assert (await client.get(url, headers={"Range": "bytes=-7"})).content == b"content"
        assert (await client.get(url, headers={"Range": "bytes=999-"})).status_code == 416
        assert (await client.get(url, headers={"Range": "bytes=0-7", "If-Range": response.headers["etag"]})).status_code == 206
        assert (await client.get(url, headers={"Range": "bytes=0-7", "If-Range": '"other"'})).status_code == 200
        prefix = f"/api/threads/{p.identity.thread_id}/artifacts/mnt/user-data/outputs/"
        plain = await client.get(prefix + "plain")
        assert plain.headers["content-type"].startswith("text/plain")
        active = await client.get(prefix + "page.html")
        assert active.headers["content-disposition"].startswith("attachment;")
        binary = await client.get(prefix + "raw.dat")
        assert binary.content == b"\0\xff" and binary.headers["content-type"] == "application/octet-stream"
        skill = await client.get(prefix + "sample.skill/SKILL.md", headers={"Range": "bytes=0-0"})
        assert skill.status_code == 206 and skill.content == b"#"
        svg = await client.get(prefix + "sample.skill/image.svg")
        assert svg.headers["content-disposition"].startswith("attachment;")
        assert (await client.get(prefix + "sample.skill/missing")).status_code == 404
        assert (await client.get(url + "?workspace_point_id=" + p.identity.request_id)).headers["x-deerflow-workspace-partial"] == "true"
        del app.state.fleet_workspace_files
        assert (await client.get(url)).status_code == 503
