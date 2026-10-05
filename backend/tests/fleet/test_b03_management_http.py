"""Session-admin management through real TCP/JWT/SQL, with persisted admission."""

import importlib
import importlib.util
import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from app.gateway.auth import create_access_token
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.models import User
from app.gateway.auth.pat import PAT_ALLOWED_SCOPES, generate_pat_token, pat_token_digest
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.internal_auth import INTERNAL_AUTH_HEADER_NAME
from deerflow.persistence.personal_access_tokens.model import PersonalAccessTokenRow
from deerflow.persistence.personal_access_tokens.sql import PersonalAccessTokenRepository
from deerflow.persistence.user.model import UserRow

from .test_b02_fleet_foundation import service_class, settings
from .test_b06_fleet_durable_jobs import serve


def management_router():
    assert importlib.util.find_spec("app.gateway.routers.fleet_management") is not None, "B03 machine management HTTP adapter missing"
    return importlib.import_module("app.gateway.routers.fleet_management").router


@pytest_asyncio.fixture
async def management_environment(fleet_database, tmp_path, monkeypatch):
    engine, sf, _ = fleet_database
    cfg = settings(tmp_path)
    cfg = cfg.model_copy(update={"profiles": cfg.profiles | {"other": cfg.profiles["batch"]}})
    fleet = service_class()(cfg)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: UserRow.__table__.create(sync))
        await connection.run_sync(lambda sync: PersonalAccessTokenRow.__table__.create(sync))
    users = SQLiteUserRepository(sf)
    admin = await users.create_user(User(email="admin@example.com", system_role="admin"))
    member = await users.create_user(User(email="member@example.com"))
    # Only inject the fixture database-backed provider; JWT parsing and all
    # middleware/principal resolution remain the production implementations.
    monkeypatch.setattr("app.gateway.deps._cached_repo", users)
    monkeypatch.setattr("app.gateway.deps._cached_local_provider", LocalAuthProvider(users))
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "")
    pat_repo = PersonalAccessTokenRepository(sf)
    pat = generate_pat_token()
    await pat_repo.create(user_id=str(admin.id), name="admin automation", scopes=list(PAT_ALLOWED_SCOPES), token_digest=pat_token_digest(pat))
    app = FastAPI()
    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.state.pat_repo = pat_repo
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    tokens = {"admin": create_access_token(str(admin.id)), "member": create_access_token(str(member.id))}
    headers = {role: {"Cookie": "access_token=" + token + "; csrf_token=b03", "X-CSRF-Token": "b03"} for role, token in tokens.items()}
    try:
        yield fleet, app, headers, pat, str(admin.id), engine
    finally:
        await fleet.stop()


BODY = {"node_id": "machine-a", "name": "worker-a", "cpu_millis": 1000, "memory_mib": 512, "profile_allowlist": ["batch"]}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admin_management_preserves_capacity_scoped_credentials_and_drain(management_environment):
    router = management_router()
    fleet, app, headers, _, actor, engine = management_environment
    app.include_router(router)
    server, task, sock, url = await serve(app)
    try:
        async with httpx.AsyncClient(base_url=url) as client:
            admin = headers["admin"]
            created = await client.post("/api/fleet/machines", headers=admin, json=BODY)
            assert created.status_code == 201, created.text
            assert created.json()["node"]["profile_allowlist"] == ["batch"]
            assert created.json()["node"]["registered_by"] == actor
            assert (await client.post("/api/fleet/machines", headers=admin, json=BODY | {"cpu_millis": 9999})).status_code == 409
            assert (await client.get("/api/fleet/machines/machine-a", headers=admin)).json()["node"]["cpu_millis"] == 1000
            assert (await client.delete("/api/fleet/machines/machine-a", headers=admin)).status_code == 409
            assert (await client.post("/api/fleet/machines", headers=admin, json=BODY | {"node_id": "machine-b", "name": "worker-b"})).status_code == 201
            issued = await client.post("/api/fleet/machines/machine-a/credentials", headers=admin, json={"lifetime_seconds": 600})
            assert issued.status_code == 201, issued.text
            assert issued.headers["cache-control"] == "no-store"
            secret = issued.json()
            credential_b = (await client.post("/api/fleet/machines/machine-b/credentials", headers=admin, json={"lifetime_seconds": 600})).json()
            assert (await client.delete("/api/fleet/machines/machine-a/credentials/" + credential_b["credential_id"], headers=admin)).status_code == 404
            assert (await fleet.credentials.authenticate("Bearer " + credential_b["token"]))["node_id"] == "machine-b"
            worker = {"Authorization": "Bearer " + secret["token"]}
            opened = await client.post("/api/fleet/node/session", headers=worker, json={"protocol_version": 1})
            assert opened.status_code == 200, opened.text
            state = await client.patch("/api/fleet/machines/machine-a", headers=admin, json={"admin_state": "draining"})
            assert state.status_code == 200, state.text
            opened = (await client.post("/api/fleet/node/session", headers=worker, json={"protocol_version": 1})).json()
            heartbeat = await client.post("/api/fleet/node/heartbeat", headers=worker, json={"protocol_version": 1, "node_session_id": opened["node_session_id"]})
            assert heartbeat.status_code == 200 and heartbeat.json()["admin_state"] == "draining"
            status = await client.get("/api/fleet/machines/machine-a", headers=admin)
            assert secret["token"] not in status.text and "token_hash" not in status.text
            assert (await client.delete("/api/fleet/machines/machine-a/credentials/" + secret["credential_id"], headers=admin)).status_code == 204
            assert (await client.post("/api/fleet/node/session", headers=worker, json={"protocol_version": 1})).status_code == 401
            assert (await client.patch("/api/fleet/machines/machine-b", headers=admin, json={"admin_state": "disabled"})).status_code == 200
            assert (await client.delete("/api/fleet/machines/machine-b", headers=admin)).status_code == 204
            assert (await client.get("/api/fleet/machines/machine-b", headers=admin)).status_code == 404
        await fleet.stop()
        await fleet.start(ExtensionRuntimeDeps(session_factory=app.state.pat_repo._sf))
        persisted = await fleet.nodes.status("machine-a")
        assert persisted["node"]["admin_state"] == "draining"
        assert persisted["node"]["profile_allowlist"] == ["batch"]
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT registered_by FROM fleet_nodes WHERE id='machine-a'"))).scalar_one() == actor
    finally:
        server.should_exit = True
        await task
        sock.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_management_rejects_non_session_admin_csrf_and_mixed_identity(management_environment, monkeypatch):
    router = management_router()
    fleet, app, headers, pat, actor, engine = management_environment
    app.include_router(router)
    await fleet.nodes.register(node_id="bearer-node", name="bearer-node", cpu_millis=1000, memory_mib=512)
    credential = await fleet.credentials.issue("bearer-node", lifetime_seconds=600)
    server, task, sock, url = await serve(app)
    monkeypatch.setattr("app.gateway.auth_middleware.is_valid_internal_auth_token", lambda token: token == "test-internal")
    denied = [
        ({"Cookie": "csrf_token=b03", "X-CSRF-Token": "b03"}, 401),
        (headers["member"], 403),
        ({"Cookie": headers["admin"]["Cookie"]}, 403),
        ({"Authorization": "Bearer " + credential.token}, 403),
        ({"Authorization": "Bearer " + pat}, 403),
        ({INTERNAL_AUTH_HEADER_NAME: "test-internal"}, 403),
        (headers["admin"] | {"Authorization": "Bearer " + credential.token}, 403),
        (headers["admin"] | {"Authorization": "Bearer invalid"}, 401),
        (headers["admin"] | {INTERNAL_AUTH_HEADER_NAME: "test-internal"}, 403),
    ]
    try:
        async with httpx.AsyncClient(base_url=url) as client:
            for auth in ({}, headers["member"], {"Authorization": "Bearer " + credential.token}, {"Authorization": "Bearer " + pat}, {INTERNAL_AUTH_HEADER_NAME: "test-internal"}):
                response = await client.get("/api/fleet/machines/bearer-node", headers=auth)
                assert response.status_code in {401, 403}, response.text
            for auth, expected in denied:
                response = await client.post("/api/fleet/machines", headers=auth, json=BODY)
                assert response.status_code == expected, response.text
            assert (await client.post("/api/fleet/machines", headers=headers["admin"], json=BODY | {"registered_by": "forged"})).status_code == 422
            for method, path, body in [
                ("PATCH", "/bearer-node", {"admin_state": "disabled"}),
                ("DELETE", "/bearer-node", None),
                ("POST", "/bearer-node/credentials", {"lifetime_seconds": 600}),
                ("DELETE", "/bearer-node/credentials/" + credential.credential_id, None),
            ]:
                for auth in (headers["member"], {"Authorization": "Bearer " + credential.token}, headers["admin"] | {INTERNAL_AUTH_HEADER_NAME: "test-internal"}):
                    response = await client.request(method, "/api/fleet/machines" + path, headers=auth, json=body)
                    assert response.status_code == 403, response.text
            assert (await fleet.credentials.authenticate("Bearer " + credential.token))["node_id"] == "bearer-node"
            expired = create_access_token(actor, expires_delta=timedelta(seconds=-1))
            response = await client.post("/api/fleet/machines", headers={"Cookie": "access_token=" + expired + "; csrf_token=b03", "X-CSRF-Token": "b03"}, json=BODY)
            assert response.status_code == 401, response.text
            async with engine.begin() as conn:
                await conn.execute(text("UPDATE fleet_credentials SET expires_at=clock_timestamp()-interval '1 second' WHERE id=:id"), {"id": credential.credential_id})
            response = await client.post("/api/fleet/node/session", headers={"Authorization": "Bearer " + credential.token}, json={"protocol_version": 1})
            assert response.status_code == 401, response.text
            monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
            for auth in (
                {"Cookie": "csrf_token=b03", "X-CSRF-Token": "b03"},
                {"Cookie": "access_token=invalid; csrf_token=b03", "X-CSRF-Token": "b03"},
                {"Authorization": "Bearer " + credential.token},
                headers["admin"] | {"Authorization": "Bearer ignored"},
            ):
                assert (await client.post("/api/fleet/machines", headers=auth, json=BODY)).status_code == 403
            async with engine.connect() as conn:
                assert (await conn.execute(text("SELECT count(*) FROM fleet_nodes"))).scalar_one() == 1
    finally:
        server.should_exit = True
        await task
        sock.close()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"cpu_millis": 0},
        {"cpu_millis": True},
        {"memory_mib": "512"},
        {"node_id": "../unsafe"},
        {"name": "../unsafe"},
        {"profile_allowlist": []},
        {"profile_allowlist": ["unknown"]},
        {"profile_allowlist": ["*"]},
        {"profile_allowlist": ["batch", "batch"]},
        {"profile_allowlist": None},
    ],
)
async def test_registration_validation_has_no_database_mutation(management_environment, change):
    router = management_router()
    _, app, headers, _, _, engine = management_environment
    app.include_router(router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/fleet/machines", headers=headers["admin"], json=BODY | change)
        assert response.status_code == 422, response.text
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_nodes"))).scalar_one() == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_profile_denial_is_persisted_and_allocates_nothing(management_environment):
    router = management_router()
    fleet, app, headers, _, _, engine = management_environment
    app.include_router(router)
    server, task, sock, url = await serve(app)
    try:
        async with httpx.AsyncClient(base_url=url) as client:
            assert (await client.post("/api/fleet/machines", headers=headers["admin"], json=BODY)).status_code == 201
            issued = (await client.post("/api/fleet/machines/machine-a/credentials", headers=headers["admin"], json={"lifetime_seconds": 600})).json()
            worker = {"Authorization": "Bearer " + issued["token"]}
            opened = (await client.post("/api/fleet/node/session", headers=worker, json={"protocol_version": 1})).json()
            session_id = opened["node_session_id"]
            await client.post("/api/fleet/node/heartbeat", headers=worker, json={"protocol_version": 1, "node_session_id": session_id})
            async with engine.begin() as conn:
                for name in ("other", "batch"):
                    await conn.execute(
                        text(
                            "INSERT INTO fleet_jobs (id,user_id,thread_id,tracking_task_id,idempotency_key,spec,state,queue_deadline,queued_at) VALUES (:name,'u','t',:name,:name,:"
                            "spec,'queued',clock_timestamp()+interval '1 hour',clock_timestamp())"
                        ),
                        {"name": name, "spec": json.dumps({"task_name": name, "profile": name, "argv": ["true"]})},
                    )
                await conn.execute(text("UPDATE fleet_jobs SET state='staged' WHERE id='batch'"))
            assert (await client.post("/api/fleet/node/claims", headers=worker, json={"node_session_id": session_id})).status_code == 204
            async with engine.connect() as conn:
                assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations"))).scalar_one() == 0
            await fleet.stop()
            await fleet.start(ExtensionRuntimeDeps(session_factory=app.state.pat_repo._sf))
            async with engine.begin() as conn:
                await conn.execute(text("UPDATE fleet_jobs SET state='queued' WHERE id='batch'"))
            claim = await client.post("/api/fleet/node/claims", headers=worker, json={"node_session_id": session_id})
            assert claim.status_code == 200 and claim.json()["job_id"] == "batch", claim.text
            assert (await client.patch("/api/fleet/machines/machine-a", headers=headers["admin"], json={"admin_state": "disabled"})).status_code == 409
            assert (await client.delete("/api/fleet/machines/machine-a", headers=headers["admin"])).status_code == 409
            async with engine.connect() as conn:
                assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations"))).scalar_one() == 1
                assert (await conn.execute(text("SELECT state FROM fleet_jobs WHERE id='other'"))).scalar_one() == "queued"
    finally:
        server.should_exit = True
        await task
        sock.close()


def test_gateway_mounts_machine_management_routes():
    management_router()
    from app.gateway.app import create_app

    paths = {route.path for route in create_app().routes}
    assert "/api/fleet/machines" in paths
    assert "/api/fleet/machines/{node_id}" in paths
    assert "/api/fleet/machines/{node_id}/credentials" in paths


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("allowlist", [[], ["other"]])
async def test_claim_checks_node_allowlist_before_reservation(fleet_database, tmp_path, allowlist):
    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        async with engine.begin() as conn:
            # Before the implementation this schema field exists only in the
            # fixture, so this tests the actual missing scheduler decision.
            await conn.execute(text("ALTER TABLE fleet_nodes ADD COLUMN IF NOT EXISTS profile_allowlist JSONB"))
            await conn.execute(
                text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,session_id,health,last_seen_at,profile_allowlist) VALUES ('restricted','restricted',1000,512,'s','online',clock_timestamp(),:allowlist)"),
                {"allowlist": json.dumps(allowlist)},
            )
            await conn.execute(
                text(
                    "INSERT INTO fleet_jobs (id,user_id,thread_id,tracking_task_id,idempotency_key,spec,state,queue_deadline,queued_at) VALUES ('j','u','t','tracking','key"
                    "',:spec,'queued',clock_timestamp()+interval '1 hour',clock_timestamp())"
                ),
                {"spec": json.dumps({"task_name": "batch", "profile": "batch", "argv": ["true"]})},
            )
        assert await fleet.scheduler.claim_job("restricted", node_session_id="s") is None
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_reservations"))).scalar_one() == 0
    finally:
        await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_f0005_upgrade_retains_nodes_claims_credentials_and_legacy_profiles(fleet_database, tmp_path):
    from alembic import command
    from alembic.config import Config
    from deerflow_ecs_fleet import service

    engine, sf, _ = fleet_database

    def previous(sync):
        config = Config()
        config.set_main_option("script_location", str(Path(service.__file__).parent / "migrations"))
        config.attributes["connection"] = sync
        command.upgrade(config, "f0005_job_invocations")

    async with engine.begin() as conn:
        await conn.run_sync(previous)
        await conn.execute(
            text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,session_id,health,last_seen_at) VALUES ('legacy','legacy',1000,512,'s','online',clock_timestamp()),('busy','busy',1000,512,'busy-s','online',clock_timestamp())")
        )
        await conn.execute(
            text(
                "INSERT INTO fleet_jobs (id,user_id,thread_id,tracking_task_id,idempotency_key,spec,state,queue_deadline,queued_at) VALUES ('j','u','t','tracking','key"
                "',:spec,'queued',clock_timestamp()+interval '1 hour',clock_timestamp()),('busy-j','u','t','busy-tracking','busy-key',:spec,'running',clock_timestamp()"
                "+interval '1 hour',clock_timestamp())"
            ),
            {"spec": json.dumps({"task_name": "batch", "profile": "batch", "argv": ["true"]})},
        )
        await conn.execute(
            text(
                "INSERT INTO fleet_attempts (id,kind,job_id,attempt_no,node_id,node_session_id,token_hash,state,output_prefix,start_authorized_at) VALUES ('busy-a','jo"
                "b','busy-j',1,'busy','busy-s','hash','running','output',clock_timestamp())"
            )
        )
        await conn.execute(text("INSERT INTO fleet_reservations (id,node_id,attempt_id,cpu_millis,memory_mib,state) VALUES ('busy-r','busy','busy-a',1000,512,'active')"))
        await conn.execute(text("INSERT INTO fleet_credentials (id,node_id,token_hash,expires_at) VALUES ('cred','busy','digest',clock_timestamp()+interval '1 hour')"))
        before = {table: [dict(row) for row in (await conn.execute(text("SELECT * FROM " + table))).mappings()] for table in ("fleet_attempts", "fleet_reservations", "fleet_credentials", "fleet_jobs")}
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT version_num FROM fleet_alembic_version"))).scalar_one() == "f0009_workspace_points"
            for table, rows in before.items():
                assert [dict(row) for row in (await conn.execute(text("SELECT * FROM " + table))).mappings()] == rows
            assert (await conn.execute(text("SELECT profile_allowlist FROM fleet_nodes WHERE id='legacy'"))).scalar_one() is None
        assert await fleet.scheduler.claim_job("legacy", node_session_id="s") is not None
        await fleet.nodes.register(node_id="new-cli", name="new-cli", cpu_millis=1000, memory_mib=512)
        assert (await fleet.nodes.status("new-cli"))["node"]["profile_allowlist"] == ["batch"]
    finally:
        await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("body", [{"lifetime_seconds": 0}, {"lifetime_seconds": True}, {"lifetime_seconds": "600"}, {"lifetime_seconds": 31_536_001}, {"lifetime_seconds": 600, "operator_id": "forged"}])
async def test_credential_request_validation_never_issues_a_secret(management_environment, body):
    router = management_router()
    _, app, headers, _, _, engine = management_environment
    app.include_router(router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.post("/api/fleet/machines", headers=headers["admin"], json=BODY)).status_code == 201
        assert (await client.post("/api/fleet/machines/machine-a/credentials", headers=headers["admin"], json=body)).status_code == 422
        assert (await client.post("/api/fleet/machines/machine-a/credentials", headers={"Cookie": headers["admin"]["Cookie"]}, json={"lifetime_seconds": 600})).status_code == 403
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM fleet_credentials"))).scalar_one() == 0
