"""Actual fixture callers over original PG ownership; no physical Docker proof."""

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import owner_environment as owner_environment
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c08_terminal_pair import prepared_pair as prepared_pair


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_owner", ["current", "other", "missing"])
async def test_sequence_actual_caller_requires_existing_original_owner(admission, tmp_path, monkeypatch, existing_owner):
    from app.gateway.routers import threads
    from deerflow.persistence.thread_meta.model import ThreadMetaRow
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    from .c08_installed_sequence import run_sequence

    engine, sf, _, user = admission
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync: ThreadMetaRow.__table__.create(sync, checkfirst=True))
    store = ThreadMetaRepository(sf)
    if existing_owner != "missing":
        await store.create("original-parent", user_id=user.id if existing_owner == "current" else "another-owner")
    before = await store.get("original-parent", user_id=None)
    app = FastAPI()
    app.state.thread_store = store
    original_paths = threads.get_paths
    reached = []

    class AfterOwnedRead(RuntimeError):
        pass

    def after_owned_read(*args, **kwargs):
        reached.append(True)
        raise AfterOwnedRead("controlled boundary after actual caller owner read")

    monkeypatch.setattr("deerflow.config.paths.Paths", after_owned_read)
    error = AfterOwnedRead if existing_owner == "current" else AssertionError
    with pytest.raises(error):
        await run_sequence(
            kind="new-turn",
            app=app,
            core=SimpleNamespace(),
            user=user,
            db=None,
            native_private=None,
            backend=None,
            driver=None,
            daemon=None,
            stager=None,
            nas=None,
            original=SimpleNamespace(thread_id="original-parent"),
            points=[],
            evidence=tmp_path,
            tmp_path=tmp_path,
            mode="full",
        )
    assert reached == ([True] if existing_owner == "current" else [])
    assert await store.get("original-parent", user_id=None) == before
    assert threads.get_paths is original_paths


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["partial"], indirect=True)
async def test_restart_actual_caller_keeps_original_recovery_guard(prepared_pair, tmp_path):
    from dataclasses import asdict

    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon

    from app.fleet.workspace import FleetWorkspaceTerminalParticipant

    from .c08_installed_faults import observe_restarts_after_kill

    p = prepared_pair
    env = p.item.env
    app = env[4]
    from deerflow.persistence.thread_meta.model import ThreadMetaRow
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    async with p.item.engine.begin() as conn:
        await conn.run_sync(lambda sync: ThreadMetaRow.__table__.create(sync, checkfirst=True))
    app.state.thread_store = ThreadMetaRepository(p.sf)
    await app.state.thread_store.create(p.identity.thread_id, user_id=p.identity.user_id)
    runtime = env[3]
    runtime.config = runtime.config.model_copy(update={"nas_root": p.versions.nas.root, "nas_identity": "task4"})
    with p.scope():
        async with p.sf.begin() as session:
            await FleetWorkspaceTerminalParticipant(p.capability, controller=p.controller).accept_partial(session, p.identity, p.candidate, barrier_epoch=1)
    stopped = await app.state.fleet_ownership.stopped(
        reason="exit", exit_code=137, process_ref=p.identity.process_ref, physical_stopped=True, node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token
    )
    assert stopped["state"] == "recovery_required"
    async with p.item.engine.connect() as conn:
        assert await conn.scalar(text("SELECT accepted_workspace_point_id FROM fleet_agent_tasks")) == p.identity.request_id
        assert await conn.scalar(text("SELECT final_workspace_point_id FROM fleet_run_placements")) is None
    ref = p.identity.process_ref

    class NativeStoppedObservation:
        # Native protocol fixture only: original PG STOP, not a physical proof.
        def __init__(self):
            self.ready = []
            self.stopped = []

        async def inspect(self, target):
            assert target == ref
            return {"State": {"Running": False, "ExitCode": 137}}

        async def list_managed(self, node_id):
            assert node_id == p.identity.node_id
            return [(ref, p.identity.attempt_id)]

        async def stop(self, target):
            assert target == ref
            self.stopped.append(target)
            return True

        async def command(self, operation, target):
            assert operation == "logs" and target == ref
            return 0, "", ""

    driver = NativeStoppedObservation()
    credential = env[7]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        client = NodeClient(gateway_url="http://test", credential=credential.token, claim_kind="agent", compatibility=env[8].model_dump(mode="json"), http_client=http)
        client.node_id, client.session_id = p.identity.node_id, p.identity.node_session_id
        daemon = NodeDaemon(client=client, containers=driver, state_dir=tmp_path / "private-journal")
        claim = asdict(p.item.accepted) | {"kind": "agent"}
        daemon.journal.save({"node_id": client.node_id, "claim": claim, "grant": p.item.grant, "reported": True, "server_state": stopped["state"], "stop_reason": "exit", "exit_code": 137})
        await observe_restarts_after_kill(
            app=app,
            db=SimpleNamespace(engine=p.item.engine, session_factory=p.sf),
            driver=driver,
            daemon=daemon,
            client=client,
            http_client=http,
            credential=credential,
            actual=env[8],
            record=env[5],
            result=stopped | {"report_pending": False},
            rows=[],
            evidence=tmp_path,
        )
    assert driver.stopped == [ref]
    assert driver.ready == []
    proof = __import__("json").loads((tmp_path / "actual-restart-recovery-proof.json").read_text())
    assert proof["old_attempt_new_session_status"] == 409
    assert proof["automatic_replay_count"] == 0
    assert proof["owner_http_status"] == 200 and proof["published_partial_marked"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepared_pair",
    [
        {
            "messages": [
                HumanMessage(content="original", id="original-human"),
                ToolMessage(content="first presentation", name="present_files", tool_call_id="presentation-1"),
                ToolMessage(content="second presentation", name="present_files", tool_call_id="presentation-2"),
                AIMessage(content="accepted final answer", id="original-answer"),
            ],
            "files": {"outputs/parent.txt": b"accepted original parent artifact"},
        }
    ],
    indirect=True,
)
async def test_sequence_actual_branch_caller_preserves_csrf_and_original_clone(prepared_pair, tmp_path, monkeypatch):
    from app.gateway import services
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.persistence.thread_meta.model import ThreadMetaRow
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    from .c08_installed_sequence import run_sequence
    from .test_c08_terminal_pair import participant

    p = prepared_pair
    env = p.item.env
    app, user, runtime = env[4], env[2], env[3]
    runtime.config = runtime.config.model_copy(update={"nas_root": p.versions.nas.root, "nas_identity": "task4"})
    from app.fleet.ownership import install_fleet_ownership

    install_fleet_ownership(app, p.sf)
    async with p.item.engine.begin() as conn:
        await conn.run_sync(lambda sync: ThreadMetaRow.__table__.create(sync, checkfirst=True))
    app.state.thread_store = ThreadMetaRepository(p.sf)
    await app.state.thread_store.create(p.identity.thread_id, user_id=user.id)
    with p.scope():
        await RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p)).update_status(p.identity.run_id, "success")
    stopped = await app.state.fleet_ownership.stopped(
        reason="exit", exit_code=0, process_ref=p.identity.process_ref, physical_stopped=True, node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token
    )
    assert stopped["state"] == "succeeded"
    async with p.item.engine.connect() as conn:
        points = [dict(row) for row in (await conn.execute(text("SELECT id,manifest_id,checkpoint_id FROM fleet_workspace_points"))).mappings()]
    from deerflow.runtime import RunManager

    core = RunManager(store=app.state.run_store)
    app.state.run_manager = core
    reached = []

    class AfterOriginalBranch(RuntimeError):
        pass

    async def after_original_branch(body, thread_id, request, **kwargs):
        assert thread_id != p.identity.thread_id
        assert body.input["messages"][0]["content"] == "c08-continuation=branch"
        reached.append(thread_id)
        raise AfterOriginalBranch("after original HTTP branch, owned clone, trusted origin and preview assertions")

    monkeypatch.setattr(services, "start_run", after_original_branch)

    class SettledPublisher:
        async def join_writers(self):
            pass

    with pytest.raises(AfterOriginalBranch):
        await run_sequence(
            kind="branch",
            app=app,
            core=SimpleNamespace(),
            user=user,
            db=SimpleNamespace(engine=p.item.engine),
            native_private=p.item.private,
            backend=None,
            driver=None,
            daemon=None,
            stager=SettledPublisher(),
            nas=SimpleNamespace(nas_root=p.versions.nas.root),
            original=SimpleNamespace(thread_id=p.identity.thread_id, run_id=p.identity.run_id),
            points=points,
            evidence=tmp_path,
            tmp_path=tmp_path,
            mode="full",
        )
    assert len(reached) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("pair", ["missing", "mismatch", "valid"])
async def test_original_csrf_middleware_requires_matching_owner_pair(owner_environment, pair):
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME, generate_csrf_token
    from app.gateway.internal_auth import create_internal_auth_headers

    app, user = owner_environment[4], owner_environment[2]
    calls = []

    @app.post("/api/threads/csrf-native/branches")
    async def original_middleware_boundary():
        calls.append(True)
        return {"accepted": True}

    token = generate_csrf_token()
    headers = create_internal_auth_headers(owner_user_id=user.id)
    if pair != "missing":
        headers[CSRF_HEADER_NAME] = token if pair == "valid" else generate_csrf_token()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers, cookies={CSRF_COOKIE_NAME: token}) as client:
        response = await client.post("/api/threads/csrf-native/branches", json={"message_id": "native"})
    assert response.status_code == (200 if pair == "valid" else 403)
    assert calls == ([True] if pair == "valid" else [])
