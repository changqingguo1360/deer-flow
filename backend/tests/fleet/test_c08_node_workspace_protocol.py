"""Original authenticated Node publication requests on actual PostgreSQL."""

import time
from contextlib import AsyncExitStack, contextmanager

import httpx
import pytest
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission  # noqa: F401
from .test_c03_remote_agent_admission import owner_environment as owner_environment  # noqa: F401
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner  # noqa: F401


async def publish_original(item, *, presented_paths=()):
    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import _AgentResourceTeardown
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    capability = FleetMutationCapability(item.identity, item.spec)
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 20

    @contextmanager
    def original_scope():
        with remote_mutation_scope(capability.context), workspace_writer_scope(controller):
            yield

    teardown = _AgentResourceTeardown(AsyncExitStack(), original_scope, capability.context)
    teardown.workspace_writers = controller
    publisher = FleetWorkspacePublisher(item.env[1], capability, controller=controller, teardown=teardown, session_pool=MCPSessionPool())
    identity = WorkspaceBoundaryIdentity.from_context(
        capability.context, request_id="original-node-request", checkpoint_id=item.config["configurable"]["checkpoint_id"], kind="partial", publication_key="presentation", presented_paths=presented_paths, source_workspace_version="original"
    )
    from langgraph.checkpoint.base import empty_checkpoint

    with original_scope():
        checkpoint = empty_checkpoint() | {"channel_values": item.checkpoint["channel_values"], "channel_versions": item.checkpoint["channel_versions"]}
        item.config = await item.writer.aput(item.config, checkpoint, {"source": "loop", "step": 1, "parents": {}}, {})
    identity = __import__("dataclasses").replace(identity, checkpoint_id=item.config["configurable"]["checkpoint_id"])
    with original_scope():
        epoch = await publisher.publish(identity)
    return identity, epoch, teardown


@pytest.mark.asyncio
async def test_actual_node_auth_poll_claim_nonce_epoch_and_no_point_acceptance(checkpoint_owner):  # noqa: F811
    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    # The original one-shot claim's token remains in this trusted native fixture.
    async with item.engine.connect() as connection:
        before = (await connection.execute(text("SELECT row_to_json(t)::text FROM runs t"))).all()
    # Do not claim again: the accepted claim is available on the original
    # ownership grant only through the test's original fixture carrier.
    token = item.accepted.token
    base = "/api/fleet/node/attempts/" + identity.attempt_id + "/workspace/"
    body = {"node_session_id": identity.node_session_id, "token": token}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            poll = await client.post(base + "poll", json=body)
            assert poll.status_code == 200
            data = poll.json()
            assert data["request"]["identity"]["request_id"] == identity.request_id
            assert data["request"]["request_digest"] == identity.request_digest
            fields = {"request_id": identity.request_id, "request_digest": identity.request_digest, "barrier_epoch": epoch, "nonce": "e" * 64}
            first = await client.post(base + "claim", json={**body, **fields})
            assert first.status_code == 200
            repeat = await client.post(base + "claim", json={**body, **fields})
            assert repeat.status_code == 200 and repeat.json() == first.json()
            for delta in ({"nonce": "f" * 64}, {"barrier_epoch": epoch + 1}, {"request_digest": "f" * 64}, {"token": "wrong-original-token"}):
                rejected = await client.post(base + "claim", json={**body, **fields, **delta})
                assert rejected.status_code in {403, 409}
        async with item.engine.connect() as connection:
            after = (await connection.execute(text("SELECT row_to_json(t)::text FROM runs t"))).all()
            row = (await connection.execute(text("SELECT state, claim_nonce, barrier_epoch FROM fleet_workspace_requests"))).one()
            assert tuple(row) == ("sealing", "e" * 64, epoch)
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_points"))).scalar_one() == 0
        assert after == before
    finally:
        await teardown.close()


async def actual_candidate(item, identity, tmp_path):
    import os

    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions
    from deerflow_ecs_fleet.workspace import NASWorkspace

    source = tmp_path / "actual-source"
    source.mkdir()
    for category in ("workspace", "uploads", "outputs"):
        (source / category).mkdir()
    (source / "outputs/result.txt").write_text("actual native candidate")
    cfg = item.env[3].config
    versions = AgentWorkspaceVersions(NASWorkspace(cfg.nas_root, identity=cfg.nas_identity), max_input_bytes=item.grant["input_limits"]["max_input_bytes"], max_output_bytes=item.grant["execution_profile"]["max_output_bytes"])
    fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        manifest = versions.seal(identity, fd)
    finally:
        os.close(fd)
    return versions, manifest


def original_request(item, identity, epoch):
    body = {"node_session_id": identity.node_session_id, "token": item.accepted.token, "request_id": identity.request_id, "request_digest": identity.request_digest, "barrier_epoch": epoch, "nonce": "e" * 64}
    base = "/api/fleet/node/attempts/" + identity.attempt_id + "/workspace/"
    return base, body


@pytest.mark.asyncio
async def test_actual_http_prepared_lost_reply_service_rebuild_verifies_same_candidate(checkpoint_owner, tmp_path, monkeypatch):  # noqa: F811
    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions

    from app.fleet.workspace import FleetWorkspaceNodeService

    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            assert (await client.post(base + "claim", json=body)).status_code == 200
            versions, candidate = await actual_candidate(item, identity, tmp_path)
            first = await client.post(base + "prepared", json={**body, "manifest": candidate.model_dump(mode="json")})
            assert first.status_code == 200
            # Discard the successful acknowledgement and reconstruct the actual
            # host service over the same durable database/NAS, with no new claim
            # or Agent start. Retry must read/verify the descriptor, never copy.
            original_verify = AgentWorkspaceVersions.verify
            verified = []

            def descriptor_verify(self, manifest):
                result = original_verify(self, manifest)
                verified.append(result.manifest_id)
                return result

            monkeypatch.setattr(AgentWorkspaceVersions, "verify", descriptor_verify)
            monkeypatch.setattr(AgentWorkspaceVersions, "seal", lambda *args: pytest.fail("Existing prepared candidate must not be copied"))
            app.state.fleet_workspaces = FleetWorkspaceNodeService(item.env[1], app.state.fleet_ownership)
            repeated = await client.post(base + "prepared", json={**body, "manifest": candidate.model_dump(mode="json")})
            assert repeated.status_code == 200
            assert repeated.json()["request"]["candidate"]["manifest_id"] == candidate.manifest_id
            assert verified == [candidate.manifest_id]
            assert versions.verify(candidate).manifest_id == candidate.manifest_id
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 1
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_manifests"))).scalar_one() == 1
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_points"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "prepared"
    finally:
        await teardown.close()


@pytest.mark.asyncio
async def test_actual_http_prepare_sql_rollback_keeps_candidate_unaccepted(checkpoint_owner, tmp_path, monkeypatch):  # noqa: F811
    from sqlalchemy.exc import DBAPIError

    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            assert (await client.post(base + "claim", json=body)).status_code == 200
            versions, candidate = await actual_candidate(item, identity, tmp_path)
            original_prepared = app.state.fleet_workspaces.requests.prepared

            async def fail_actual_sql(session, *args, **kwargs):
                await original_prepared(session, *args, **kwargs)
                await session.execute(text("SELECT 1/0"))

            monkeypatch.setattr(app.state.fleet_workspaces.requests, "prepared", fail_actual_sql)
            with pytest.raises(DBAPIError):
                await client.post(base + "prepared", json={**body, "manifest": candidate.model_dump(mode="json")})
            assert versions.verify(candidate).manifest_id == candidate.manifest_id
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_manifests"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_points"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "sealing"
    finally:
        await teardown.close()


@pytest.mark.asyncio
async def test_actual_original_lease_expires_during_unlocked_nas_verification(checkpoint_owner, tmp_path, monkeypatch):  # noqa: F811
    import asyncio
    import threading

    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions

    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    entered, release = threading.Event(), threading.Event()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            assert (await client.post(base + "claim", json=body)).status_code == 200
            _, candidate = await actual_candidate(item, identity, tmp_path)
            original_verify = AgentWorkspaceVersions.verify

            def actual_slow_verify(self, manifest):
                verified = original_verify(self, manifest)
                entered.set()
                release.wait(3)
                return verified

            monkeypatch.setattr(AgentWorkspaceVersions, "verify", actual_slow_verify)
            preparing = asyncio.create_task(client.post(base + "prepared", json={**body, "manifest": candidate.model_dump(mode="json")}))
            try:
                assert await asyncio.to_thread(entered.wait, 1)
                # These real UPDATEs finish while NAS I/O is parked, proving
                # original task/run/attempt SQL locks were released before I/O.
                async with item.engine.begin() as connection:
                    await connection.execute(text("SET LOCAL lock_timeout='500ms'"))
                    await connection.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
                    await connection.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs LIMIT 1)"))
            finally:
                release.set()
            response = await preparing
            assert response.status_code == 409
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_manifests"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_points"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "sealing"
    finally:
        release.set()
        await teardown.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("credential_change", ("revoked_at", "expires_at"))
async def test_actual_node_credential_revoked_during_nas_verification(checkpoint_owner, tmp_path, monkeypatch, credential_change):  # noqa: F811
    import asyncio
    import threading

    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions

    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    entered, release = threading.Event(), threading.Event()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            assert (await client.post(base + "claim", json=body)).status_code == 200
            _, candidate = await actual_candidate(item, identity, tmp_path)
            original_verify = AgentWorkspaceVersions.verify

            def physical_verify(self, manifest):
                result = original_verify(self, manifest)
                entered.set()
                release.wait(3)
                return result

            monkeypatch.setattr(AgentWorkspaceVersions, "verify", physical_verify)
            preparing = asyncio.create_task(client.post(base + "prepared", json={**body, "manifest": candidate.model_dump(mode="json")}))
            try:
                assert await asyncio.to_thread(entered.wait, 1)
                async with item.engine.begin() as connection:
                    await connection.execute(text("SET LOCAL lock_timeout='500ms'"))
                    await connection.execute(text("UPDATE fleet_credentials SET " + credential_change + "=clock_timestamp()-interval '1 second'"))
            finally:
                release.set()
            assert (await preparing).status_code == 403
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_manifests"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_points"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "sealing"
    finally:
        release.set()
        await teardown.close()


@pytest.mark.asyncio
async def test_actual_credential_lock_wait_rechecks_original_lease(checkpoint_owner, monkeypatch):  # noqa: F811
    import asyncio

    from deerflow_ecs_fleet.persistence.models import CredentialRow
    from sqlalchemy.ext.asyncio import AsyncSession

    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    entered = asyncio.Event()
    original_get = AsyncSession.get

    async def actual_get(self, entity, *args, **kwargs):
        if entity is CredentialRow and kwargs.get("with_for_update"):
            entered.set()
        return await original_get(self, entity, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "get", actual_get)
    try:
        async with item.engine.begin() as connection:
            await connection.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
            await connection.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs LIMIT 1)"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            async with item.engine.begin() as blocker:
                await blocker.execute(text("SELECT id FROM fleet_credentials FOR UPDATE"))
                claiming = asyncio.create_task(client.post(base + "claim", json=body))
                try:
                    await asyncio.wait_for(entered.wait(), 1)
                    await asyncio.sleep(1.1)
                except BaseException:
                    claiming.cancel()
                    raise
            response = await claiming
            assert response.status_code == 409
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "requested"
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_manifests"))).scalar_one() == 0
    finally:
        await teardown.close()


@pytest.mark.asyncio
async def test_actual_credential_expires_during_projection_rolls_back_claim(checkpoint_owner, monkeypatch):  # noqa: F811
    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    service = app.state.fleet_workspaces
    original_projection = service._projection

    async def physical_sql_projection(session, *args):
        projection = await original_projection(session, *args)
        await session.execute(text("SELECT pg_sleep(1.1)"))
        return projection

    monkeypatch.setattr(service, "_projection", physical_sql_projection)
    try:
        async with item.engine.begin() as connection:
            await connection.execute(text("UPDATE fleet_credentials SET expires_at=clock_timestamp()+interval '1 second'"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            assert (await client.post(base + "claim", json=body)).status_code == 403
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "requested"
            assert (await connection.execute(text("SELECT claim_nonce FROM fleet_workspace_requests"))).scalar_one() is None
    finally:
        await teardown.close()


@pytest.mark.asyncio
async def test_actual_second_node_and_wrong_original_session_cannot_poll_or_claim(checkpoint_owner):  # noqa: F811
    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    try:
        async with item.engine.begin() as connection:
            await connection.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,session_id) VALUES ('second-node','second-node',1000,512,'second-session')"))
        other = await item.env[3].credentials.issue("second-node", lifetime_seconds=600)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            for token, session_id in ((credential.token, "wrong-original-session"), (other.token, "second-session")):
                headers = {"Authorization": "Bearer " + token}
                assert (await client.post(base + "poll", headers=headers, json={"token": body["token"], "node_session_id": session_id})).status_code in {403, 409}
                assert (await client.post(base + "claim", headers=headers, json={**body, "node_session_id": session_id})).status_code in {403, 409}
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "requested"
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_points"))).scalar_one() == 0
    finally:
        await teardown.close()


@pytest.mark.asyncio
async def test_actual_draining_original_node_stops_workspace_poll_and_claim(checkpoint_owner):  # noqa: F811
    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    try:
        async with item.engine.begin() as connection:
            await connection.execute(text("UPDATE fleet_nodes SET admin_state='draining'"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            for operation, fields in (("poll", {"node_session_id": body["node_session_id"], "token": body["token"]}), ("claim", body)):
                response = await client.post(base + operation, json=fields)
                assert response.status_code == 200 and response.json() == {"stop": True, "request": None}
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "requested"
    finally:
        await teardown.close()


@pytest.mark.asyncio
async def test_actual_node_sql_lock_wait_is_bounded_by_original_lease(checkpoint_owner):  # noqa: F811
    import asyncio

    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    app, credential = item.env[4], item.env[7]
    base, body = original_request(item, identity, epoch)
    waiting = None
    try:
        async with item.engine.begin() as connection:
            await connection.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
            await connection.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs LIMIT 1)"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + credential.token}) as client:
            async with item.engine.begin() as blocker:
                await blocker.execute(text("SELECT id FROM fleet_agent_tasks FOR UPDATE"))
                waiting = asyncio.create_task(client.post(base + "claim", json=body))
                await asyncio.sleep(1.3)
                finished_with_original_lease = waiting.done()
            response = await waiting
            assert finished_with_original_lease, "Node SQL waiter must finish before the original lease expires, without releasing its blocker"
            assert response.status_code == 409
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT state FROM fleet_workspace_requests"))).scalar_one() == "requested"
    finally:
        if waiting is not None and not waiting.done():
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
        await teardown.close()
