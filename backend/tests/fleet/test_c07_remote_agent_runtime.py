"""C07 behavior must fail on the original real producer, not missing interfaces."""

import asyncio
import hashlib
import importlib
import json
import os
from contextlib import AsyncExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import FastAPI
from sqlalchemy import text

from .c04_integration_fixture import node_server
from .c07_integration_fixture import C07Scenario, RedisFaultProxy, native_runtime_paths, owned_redis
from .test_b02_fleet_foundation import service_class, settings
from .test_c01_remote_agent_admission import c_config
from .test_c02_remote_agent_admission import request as admission_request


@pytest_asyncio.fixture
async def c07_scenario(fleet_database, tmp_path, request):
    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_ecs_fleet.worker.client import NodeClient

    from app.fleet.execution import FleetExecutionBackend
    from app.fleet.ownership import install_fleet_ownership
    from app.gateway import services
    from app.gateway.auth_middleware import AuthMiddleware
    from app.gateway.csrf_middleware import CSRFMiddleware
    from app.gateway.routers.thread_runs import RunCreateRequest
    from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
    from deerflow.persistence.base import Base
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime import RunManager
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.store.async_provider import make_store
    from deerflow.runtime.stream_bridge.redis import RedisStreamBridge
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    engine, sf, schema = fleet_database
    directory = Path(os.environ.get("C07_EVIDENCE_DIR", str(tmp_path / "c07-evidence")))
    directory = directory / (request.node.originalname + "-" + hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:12]) / request.node.callspec.params["reader"]
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o700)
    runtime_files = directory / "runtime"
    (runtime_files / "skills").mkdir(parents=True)
    (runtime_files / "runtime-bundle.json").write_text(json.dumps({"skills": [], "plugins": [], "mcp_servers": {}, "secret_bindings": {"operator-model-binding": {"name": "MODEL_API_KEY", "kind": "model", "target": "model-1"}}}))
    (runtime_files / "model-bindings.json").write_text(json.dumps({"model-1": {"provider_use": "fleet.c07_integration_fixture:BarrierModel", "target_model": "c07", "version": "v1"}}))
    # Actual compatibility reads all installed distributions and provider bytes.
    async with native_runtime_paths(runtime_files) as host:
        actual = host.installed_compatibility()
    assert RunEventRow.__table__.metadata is Base.metadata
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    cfg_data = c_config()
    nas = directory / "nas"
    nas.mkdir()
    nas_settings = settings(nas)
    cfg_data.update(nas_root=str(nas_settings.nas_root), nas_identity=nas_settings.nas_identity)
    cfg_data["profiles"]["remote"]["runtime_digest"] = actual.runtime_digest
    cfg = FleetConfig.model_validate(cfg_data)
    fleet = service_class()(cfg)
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    user = SimpleNamespace(id="user-c07", system_role="admin")
    public = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "memory": {"enabled": False}})
    set_app_config(public)
    token = set_current_user(user)
    app = FastAPI()
    if request.node.callspec.params.get("termination") == "network-cancel-adapter":
        # Test-only transport adapter: observe a real network disconnect at the
        # outer ASGI boundary, then cancel the actual route helper task. This
        # proves cancellation-finally policy, not default middleware detection.
        app.state.wait_entered = asyncio.Event()
        app.state.network_disconnect_receipts = []

        class NetworkCancellationAdapter:
            def __init__(self, app):
                self.app = app

            async def __call__(self, scope, receive, send):
                if scope["type"] != "http" or not scope["path"].endswith(("/cancel", "/stream")):
                    return await self.app(scope, receive, send)

                async def disconnect_then_cancel():
                    await app.state.wait_entered.wait()
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            app.state.network_disconnect_receipts.append({"type": message["type"], "path": scope["path"], "helper_task_cancelled": True})
                            app.state.original_wait_task.cancel()
                            return

                monitor = asyncio.create_task(disconnect_then_cancel())
                try:
                    await self.app(scope, receive, send)
                finally:
                    monitor.cancel()
                    await asyncio.gather(monitor, return_exceptions=True)

    app.state.extensions = SimpleNamespace(services=(("fleet", fleet),))
    app.state.run_store = RunRepository(sf)
    app.state.run_manager = RunManager(store=app.state.run_store)
    app.state.thread_store = ThreadMetaRepository(sf)
    install_fleet_ownership(app, sf)
    app.add_middleware(AuthMiddleware)
    app.add_middleware(CSRFMiddleware)
    if request.node.callspec.params.get("termination") == "network-cancel-adapter":
        app.add_middleware(NetworkCancellationAdapter)
    app.include_router(importlib.import_module("app.gateway.routers.fleet_nodes").router)
    app.include_router(importlib.import_module("app.gateway.routers.thread_runs").router)
    await fleet.nodes.register(node_id="node-c07", name="node-c07", cpu_millis=1500, memory_mib=4096, agent_limit=1, profile_allowlist=["remote"])
    credential = await fleet.credentials.issue("node-c07", lifetime_seconds=600)
    scenario = None
    resources = AsyncExitStack()
    try:
        async with owned_redis(directory / "redis") as (redis, redis_port, redis_pid):
            proxy = RedisFaultProxy(redis_port)
            await proxy.start()
            resources.push_async_callback(proxy.close)
            prefix = "c07:" + uuid4().hex
            (directory / "redis-prefix.txt").write_text(prefix)
            bridge = RedisStreamBridge(redis_url=f"redis://127.0.0.1:{proxy.port}/0", key_prefix=prefix, heartbeat_interval=0.1)
            resources.push_async_callback(bridge.close)
            app.state.stream_bridge = bridge
            from app.fleet.events import install_fleet_events

            fleet_events = install_fleet_events(app, sf)
            resources.push_async_callback(fleet_events.close)
            await fleet_events.start()
            import deerflow_ecs_fleet.event_bridge as actual_publisher
            import deerflow_ecs_fleet.persistence.models as actual_models

            import app.fleet.events as actual_events
            import deerflow.runtime.events.store.db as actual_event_store
            import deerflow.runtime.events.transactions as actual_transactions

            modules = {}
            for module in (actual_publisher, actual_models, actual_events, actual_event_store, actual_transactions):
                path = Path(module.__file__).resolve()
                modules[module.__name__] = {"file": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            (directory / "gateway-source-origins.json").write_text(json.dumps(modules, indent=2))
            private = {
                "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider", "allow_host_bash": True},
                "database": {"backend": "postgres", "postgres_url": engine.url.render_as_string(hide_password=False), "postgres_schema": schema},
                "run_events": {"backend": "db"},
                "agent_storage": {"backend": "db"},
                "stream_bridge": {"type": "redis", "redis_url": f"redis://127.0.0.1:{proxy.port}/0", "key_prefix": prefix, "heartbeat_interval_seconds": 0.1},
                "skills": {"path": str(runtime_files / "skills")},
                "memory": {"enabled": False, "manager_class": "noop"},
                "title": {"enabled": False},
                "summarization": {"enabled": False},
                "models": [{"name": "model-1", "use": "fleet.c07_integration_fixture:BarrierModel", "model": "c07"}],
                "tools": [{"name": "c07_barrier", "group": "c07", "use": "fleet.c07_integration_fixture:c07_barrier"}],
                "tool_groups": [{"name": "c07", "description": "Owned acceptance barrier"}],
            }
            private_config = AppConfig.model_validate(private)
            app.state.checkpointer = await resources.enter_async_context(make_checkpointer(private_config))
            app.state.store = await resources.enter_async_context(make_store(private_config))
            from deerflow.runtime.events.store.db import DbRunEventStore

            app.state.run_event_store = DbRunEventStore(sf)
            app.state.run_events_config = private_config.run_events
            body = RunCreateRequest(
                input={"messages": [{"role": "user", "content": "Run the bounded acceptance barrier then answer."}]},
                config={"recursion_limit": 50, "context": {"model_name": "model-1", "subagent_enabled": False}},
                stream_mode=["values", "messages-tuple", "updates"],
                stream_subgraphs=True,
            )
            backend = FleetExecutionBackend(
                config=cfg,
                profile_name="remote",
                model_name="model-1",
                model_version="v1",
                skill_snapshot=actual.skill_snapshot,
                plugin_snapshot=actual.plugin_snapshot,
                workspace_manifest_ref="c07-workspace",
                secret_refs=[{"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"}],
            )
            record = await services.start_run(body, "thread-c07", admission_request(app.state.run_manager, user), execution_backend=backend)
            # Host-side HTTP permissions must see the real admitted thread;
            # the trusted admission helper uses its own neutral memory store.
            await app.state.thread_store.create(record.thread_id, user_id=record.user_id)
            async with node_server(app) as url:
                node = NodeClient(gateway_url=url, credential=credential.token, claim_kind="agent", compatibility=actual.model_dump(mode="json"))
                try:
                    await node.open_session()
                    await node.heartbeat()
                    scenario = C07Scenario(directory=directory, engine=engine, record=record, claim=None, grant=None, bootstrap=None, app=app, url=url, node=node, proxy=proxy, redis=redis, prefix=prefix)

                    async def claim_original():
                        assert scenario.claim is None
                        claim = await node.claim()
                        assert claim and claim["run_id"] == record.run_id
                        grant = await node.attempt(claim, "start")
                        scenario.claim, scenario.grant = claim, grant
                        scenario.bootstrap = {
                            "schema_version": 1,
                            "identity": {
                                "node_id": node.node_id,
                                "node_session_id": node.session_id,
                                "agent_task_id": claim["agent_task_id"],
                                "generation": grant["generation"],
                                "attempt_id": claim["attempt_id"],
                                "owner_worker_id": claim["owner_worker_id"],
                                "token_stamp": hashlib.sha256(claim["token"].encode()).hexdigest(),
                            },
                            "operator_config": private,
                        }

                    scenario.claim_original = claim_original
                    if not request.node.callspec.params.get("queued", False):
                        await claim_original()
                    (directory / "resources.json").write_text(json.dumps({"redis_pid": redis_pid, "redis_port": redis_port, "proxy_port": proxy.port, "schema": schema, "key_prefix": prefix}))

                    async def renew_owned_attempt():
                        while True:
                            await asyncio.sleep(2)
                            if scenario.claim is None:
                                continue
                            result = await node.attempt(scenario.claim, "renew", running=False)
                            if result.get("stop"):
                                raise RuntimeError("Actual node renewal requested stop")

                    renewal = asyncio.create_task(renew_owned_attempt())
                    try:
                        yield scenario
                    finally:
                        renewal.cancel()
                        await asyncio.gather(renewal, return_exceptions=True)
                finally:
                    if scenario:
                        await scenario.close()
                    await node.close()
    finally:
        await resources.aclose()
        reset_current_user(token)
        reset_app_config()
        await fleet.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["cached", "hydrated"])
async def test_committed_remote_tail_reconnects_without_runner_restart(c07_scenario, reader):
    s = c07_scenario
    try:
        await s.start_once()
        await s.wait_for_committed_model_event()
        cursor = await s.initial_cursor()
        await s.disconnect_redis()
        await s.release_model_barrier()
        await s.wait_for_runner_exit()
        stopped = await s.acknowledge_actual_stop()
        assert stopped["state"] in {"succeeded", "failed", "cancelled", "timed_out"}, "Real exit must be accepted, not uncertain"
        if reader == "hydrated":
            from deerflow.runtime import RunManager

            # Actual separate Gateway reader hydration, no state writes or Agent restart.
            s.app.state.run_manager = RunManager(store=s.app.state.run_store)
        await s.app.state.stream_bridge.publisher.stop()
        await s.restore_redis()
        await s.delete_owned_redis_keys()
        frames = await s.join(cursor=cursor)
        evidence = await s.durable_receipt()
        assert len(evidence["starts"]) == 1
        assert any(s.expected_tail in row[1] for row in evidence["events"]), "Actual tool tail must be committed in independent SQL observation"
        assert any(s.expected_tail in json.dumps(frame.get("data")) for frame in frames), "Committed remote tool tail must replay after Redis loss before terminal END"
        assert frames[-1]["event"] == "end"
        ids = [frame["id"] for frame in frames if frame.get("id")]
        seqs = [int(cursor.rsplit(".", 1)[1]) for cursor in ids]
        assert seqs == sorted(set(seqs))
        assert all(cursor.startswith("fleet.v1.") for cursor in ids)
        assert int(cursor.rsplit(".", 1)[1]) < seqs[0]
        evidence["observed"] = {"ordered_unique_events": seqs == sorted(set(seqs)), "runner_starts": len(evidence["starts"]), "terminal_end_recovered": frames[-1]["event"] == "end"}
        (s.directory / "observed.json").write_text(json.dumps(evidence, indent=2, default=str))
    finally:
        if s.process is not None and s.process.returncode is not None:
            final_receipt = await s.durable_receipt()
            if "evidence" in locals() and "observed" in evidence:
                final_receipt["observed"] = evidence["observed"]
            (s.directory / "observed.json").write_text(json.dumps(final_receipt, indent=2, default=str))


@pytest.mark.asyncio
@pytest.mark.parametrize("write", ["put", "put_batch", "put_if_absent"])
async def test_same_session_participant_failure_rolls_back_event(fleet_database, write):
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.events.store.db import DbRunEventStore

    engine, sf, _ = fleet_database
    async with engine.begin() as conn:
        await conn.run_sync(RunEventRow.__table__.create)
        await conn.execute(text("CREATE TABLE c07_participant_probe(event_id bigint, txid bigint)"))
    observations = []

    class FailingParticipant:
        async def insert(self, session, *, event_id, record):
            event = (await session.execute(text("SELECT id, txid_current() FROM run_events WHERE id=:id"), {"id": event_id})).one()
            marker = (await session.execute(text("INSERT INTO c07_participant_probe VALUES (:id, txid_current()) RETURNING event_id, txid"), {"id": event_id})).one()
            observations.append((event, marker, record["seq"]))
            raise RuntimeError("same-session participant rejected")

    store = DbRunEventStore(sf, transaction_participant=FailingParticipant())
    event = dict(thread_id="thread-c07-participant", run_id="run-c07-participant", event_type="run.start", category="trace", content="probe")
    with pytest.raises(RuntimeError, match="same-session participant rejected"):
        if write == "put_batch":
            await store.put_batch([event])
        else:
            await getattr(store, write)(**event)
    assert len(observations) == 1
    row, marker, seq = observations[0]
    assert row == marker  # Both actual INSERTs were visible in the same original transaction.
    assert seq == 1
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM run_events"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT count(*) FROM c07_participant_probe"))).scalar_one() == 0


@pytest.mark.asyncio
async def test_existing_singleton_does_not_repeat_participant(fleet_database):
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.events.store.db import DbRunEventStore

    engine, sf, _ = fleet_database
    async with engine.begin() as conn:
        await conn.run_sync(RunEventRow.__table__.create)
    calls = []

    class Participant:
        async def insert(self, session, *, event_id, record):
            calls.append((event_id, record["seq"]))

    store = DbRunEventStore(sf, transaction_participant=Participant())
    event = dict(thread_id="thread-singleton", run_id="run-singleton", event_type="run.end", category="lifecycle")
    first, created = await store.put_if_absent(**event)
    repeated, created_again = await store.put_if_absent(**event)
    assert created and not created_again
    assert first == repeated
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["cached"])
async def test_remote_cursor_errors_are_http400_before_stream_headers(c07_scenario, reader):
    from dataclasses import replace

    import httpx

    from app.fleet.events import remote_cursor
    from app.gateway.internal_auth import create_internal_auth_headers

    s = c07_scenario
    await s.start_once()
    await s.wait_for_committed_model_event()
    cursor = await s.initial_cursor()
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME

    csrf = uuid4().hex
    active_headers = create_internal_auth_headers(owner_user_id=s.record.user_id)
    active_url = f"{s.url}/api/threads/{s.record.thread_id}/runs/{s.record.run_id}"
    async with httpx.AsyncClient(timeout=5, cookies={CSRF_COOKIE_NAME: csrf}) as active_client:
        active_response = await active_client.post(active_url + "/stream?action=interrupt", headers=active_headers | {"Last-Event-ID": "malformed", CSRF_HEADER_NAME: csrf})
    active_receipt = {"kind": "active-cancel-preflight", "status": active_response.status_code, "body": active_response.json(), "content_type": active_response.headers.get("content-type")}
    (s.directory / "active-http400.json").write_text(json.dumps(active_receipt, indent=2))
    assert active_response.status_code == 400
    async with s.engine.connect() as connection:
        assert (await connection.execute(text("SELECT cancel_action FROM runs WHERE run_id=:run"), {"run": s.record.run_id})).scalar_one() is None
    assert s.process.returncode is None
    await s.release_model_barrier()
    await s.wait_for_runner_exit()
    await s.acknowledge_actual_stop()
    bridge = s.app.state.stream_bridge
    identity = await bridge.reader.identity(s.record.run_id)
    frames = await s.join(cursor=cursor)
    assert frames[-1]["event"] == "end"
    last_seq = max(int(frame["id"].rsplit(".", 1)[1]) for frame in frames if frame.get("id"))
    bad = ["nonsense", "fleet.v1.X.Y.01", remote_cursor(identity, last_seq + 1000), remote_cursor(replace(identity, run_id="foreign-run"), 1), remote_cursor(replace(identity, attempt_id="old-attempt"), 1)]
    receipts = [active_receipt]
    headers = create_internal_auth_headers(owner_user_id=s.record.user_id)
    url = f"{s.url}/api/threads/{s.record.thread_id}/runs/{s.record.run_id}"
    async with httpx.AsyncClient(timeout=5) as client:
        for invalid in bad:
            response = await client.get(url + "/join", headers=headers | {"Last-Event-ID": invalid})
            assert response.status_code == 400
            assert response.headers["content-type"].startswith("application/json")
            receipts.append({"cursor": invalid, "status": response.status_code, "body": response.json()})
        async with s.engine.begin() as connection:
            deleted = (await connection.execute(text("DELETE FROM run_events WHERE run_id=:run AND seq=:seq RETURNING id"), {"run": identity.run_id, "seq": int(cursor.rsplit(".", 1)[1])})).scalar_one()
            assert (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox WHERE event_id=:id"), {"id": deleted})).scalar_one() == 1
        response = await client.get(url + "/join", headers=headers | {"Last-Event-ID": cursor})
        assert response.status_code == 400
        receipts.append({"kind": "deleted-core-event-retained-pointer", "status": response.status_code, "body": response.json()})
        from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME

        csrf = uuid4().hex
        client.cookies.set(CSRF_COOKIE_NAME, csrf)
        response = await client.post(url + "/stream?action=interrupt", headers=headers | {"Last-Event-ID": "malformed", CSRF_HEADER_NAME: csrf})
        assert response.status_code == 400
        receipts.append({"kind": "cancel-preflight", "status": response.status_code})
    async with s.engine.connect() as connection:
        assert (await connection.execute(text("SELECT cancel_action FROM runs WHERE run_id=:run"), {"run": identity.run_id})).scalar_one() is None
    (s.directory / "http400.json").write_text(json.dumps(receipts, indent=2))
    (s.directory / "observed.json").write_text(json.dumps(await s.durable_receipt(), indent=2, default=str))


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["cached", "hydrated"])
async def test_actual_stopped_terminal_process_recovers_failed_writer_seal(c07_scenario, reader):
    s = c07_scenario
    async with s.engine.begin() as connection:
        await connection.execute(text("CREATE FUNCTION c07_seal_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'actual C07 writer seal fault'; END $$"))
        await connection.execute(text("CREATE TRIGGER c07_seal_fault BEFORE INSERT ON fleet_stream_seals FOR EACH ROW EXECUTE FUNCTION c07_seal_fault()"))
    await s.start_once()
    await s.wait_for_committed_model_event()
    cursor = await s.initial_cursor()
    await s.release_model_barrier()
    await s.wait_for_runner_exit()
    async with s.engine.begin() as connection:
        status = (await connection.execute(text("SELECT status FROM runs WHERE run_id=:run"), {"run": s.record.run_id})).scalar_one()
        assert status == "success"
        assert (await connection.execute(text("SELECT count(*) FROM fleet_stream_seals"))).scalar_one() == 0
        assert (await connection.execute(text("SELECT stopped_at FROM fleet_attempts WHERE id=:attempt"), {"attempt": s.claim["attempt_id"]})).scalar_one() is None
        await connection.execute(text("DROP TRIGGER c07_seal_fault ON fleet_stream_seals"))
    assert s.process.returncode == 0
    process_receipts = [json.loads(line) for line in (s.directory / "process.jsonl").read_text().splitlines()]
    assert any(row["event"] == "runner-exception" and row.get("writer_seal_fault") is True for row in process_receipts)
    assert (await s.acknowledge_actual_stop())["state"] == "succeeded"
    async with s.engine.connect() as connection:
        seal = (await connection.execute(text("SELECT source,last_seq,core_status FROM fleet_stream_seals WHERE run_id=:run"), {"run": s.record.run_id})).one()
    assert seal[0] == "physical_stop" and seal[2] == "success"
    if reader == "hydrated":
        from deerflow.runtime import RunManager

        s.app.state.run_manager = RunManager(store=s.app.state.run_store)
    frames = await s.join(cursor=cursor)
    assert frames[-1]["event"] == "end"
    assert any(s.expected_tail in json.dumps(frame.get("data")) for frame in frames)
    assert len(await s.start_receipts()) == 1
    evidence = await s.durable_receipt()
    evidence["seal"] = list(seal)
    (s.directory / "physical-stop-recovery.json").write_text(json.dumps(evidence, indent=2, default=str))


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["cached"])
async def test_owned_publisher_startup_scans_accepted_stopped_history(c07_scenario, reader):
    from app.fleet.events import install_fleet_events

    from .c07_integration_fixture import bounded_wait

    s = c07_scenario
    original_bridge = s.app.state.stream_bridge
    await original_bridge.publisher.stop()
    await s.start_once()
    await s.wait_for_committed_model_event()
    cursor = await s.initial_cursor()
    await s.release_model_barrier()
    await s.wait_for_runner_exit()
    await s.acknowledge_actual_stop()
    async with s.engine.begin() as connection:
        # A migration-era accepted physical-stop receipt with no transport seal.
        await connection.execute(text("DELETE FROM fleet_stream_seals WHERE run_id=:run"), {"run": s.record.run_id})
    s.app.state.stream_bridge = original_bridge.local_bridge
    replacement = install_fleet_events(s.app, original_bridge.reader.sf)
    await replacement.start()
    owned_task = replacement.publisher._task
    try:

        async def recovered():
            async with s.engine.connect() as connection:
                return (await connection.execute(text("SELECT source FROM fleet_stream_seals WHERE run_id=:run"), {"run": s.record.run_id})).scalar_one_or_none() == "physical_stop"

        await bounded_wait(recovered, seconds=5)
        frames = await s.join(cursor=cursor)
        assert frames[-1]["event"] == "end"
        assert any(s.expected_tail in json.dumps(frame.get("data")) for frame in frames)
        assert len(await s.start_receipts()) == 1
    finally:
        await replacement.close()
    assert owned_task.done() and replacement.publisher._task is None
    evidence = await s.durable_receipt()
    evidence["publisher_task_settled"] = owned_task.done()
    (s.directory / "startup-scanner.json").write_text(json.dumps(evidence, indent=2, default=str))


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["cached"])
async def test_actual_http_wait_for_trusted_remote_record_waits_for_durable_seal(c07_scenario, reader, monkeypatch):
    import httpx

    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME
    from app.gateway.internal_auth import create_internal_auth_headers

    s = c07_scenario
    await s.start_once()
    await s.wait_for_committed_model_event()
    assert s.record.task is None
    module = importlib.import_module("app.gateway.routers.thread_runs")

    async def admitted_original(body, thread_id, request):
        # Explicit trusted-admission boundary injection. Public Fleet selection
        # remains closed; downstream HTTP/wait/DB/seal are actual production.
        assert thread_id == s.record.thread_id
        return s.record

    monkeypatch.setattr(module, "start_run", admitted_original)
    csrf = uuid4().hex
    headers = create_internal_auth_headers(owner_user_id=s.record.user_id) | {CSRF_HEADER_NAME: csrf}
    async with httpx.AsyncClient(timeout=10, cookies={CSRF_COOKIE_NAME: csrf}) as client:
        task = asyncio.create_task(client.post(f"{s.url}/api/threads/{s.record.thread_id}/runs/wait", headers=headers, json={"input": {"messages": [{"role": "user", "content": "observe original admitted run"}]}}))
        try:
            await asyncio.sleep(0.2)
            async with s.engine.connect() as connection:
                assert (await connection.execute(text("SELECT count(*) FROM fleet_stream_seals WHERE run_id=:run"), {"run": s.record.run_id})).scalar_one() == 0
            assert not task.done(), "Remote taskNone creator /wait must remain pending before durable seal"
            await s.release_model_barrier()
            response = await asyncio.wait_for(task, 10)
            assert response.status_code == 200
            assert "c07-final-tail" in json.dumps(response.json())
            await s.wait_for_runner_exit()
            await s.acknowledge_actual_stop()
            async with s.engine.connect() as connection:
                seal = (await connection.execute(text("SELECT source,last_seq FROM fleet_stream_seals WHERE run_id=:run"), {"run": s.record.run_id})).one()
            assert seal[1] > 0
            (s.directory / "wait-http.json").write_text(
                json.dumps({"trusted_admission_boundary_injected": True, "status": response.status_code, "response": response.json(), "seal": list(seal), "runner_starts": len(await s.start_receipts())}, indent=2)
            )
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["cached"])
async def test_accepted_original_run_replays_after_current_generation_advances(c07_scenario, reader):
    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.persistence.models import AgentTaskRow, LaunchSpecRow, RunPlacementRow

    from deerflow.persistence.run.model import RunRow

    s = c07_scenario
    await s.start_once()
    await s.wait_for_committed_model_event()
    cursor = await s.initial_cursor()
    await s.release_model_barrier()
    await s.wait_for_runner_exit()
    assert (await s.acknowledge_actual_stop())["state"] == "succeeded"
    identity = await s.app.state.stream_bridge.reader.identity(s.record.run_id)
    next_run = str(uuid4())
    next_spec = LaunchSpec.model_validate(s.claim["launch_spec"]).model_copy(update={"run_id": next_run, "generation": identity.generation + 1})
    sf = s.app.state.stream_bridge.reader.sf
    async with sf.begin() as session:
        placement = await session.get(RunPlacementRow, s.record.run_id)
        task = await session.get(AgentTaskRow, placement.agent_task_id, with_for_update=True)
        session.add(RunRow(run_id=next_run, thread_id=identity.thread_id, user_id=identity.user_id, status="pending", kwargs_json={"execution_backend": "fleet"}))
        spec_id = str(uuid4())
        session.add(
            LaunchSpecRow(
                id=spec_id,
                run_id=next_run,
                agent_task_id=placement.agent_task_id,
                generation=next_spec.generation,
                user_id=identity.user_id,
                thread_id=identity.thread_id,
                payload=next_spec.canonical_payload(),
                payload_digest=next_spec.payload_digest(),
            )
        )
        await session.flush()
        session.add(
            RunPlacementRow(
                run_id=next_run,
                agent_task_id=placement.agent_task_id,
                generation=next_spec.generation,
                user_id=identity.user_id,
                thread_id=identity.thread_id,
                requested_backend=placement.requested_backend,
                profile=placement.profile,
                launch_spec_ref=spec_id,
                queue_deadline=placement.queue_deadline,
            )
        )
        await session.flush()
        # Controlled later-placement fixture; this does not execute continuations.
        task.current_run_id = next_run
        task.generation = next_spec.generation
        task.state = "queued"
    assert await s.app.state.stream_bridge.reader.identity(s.record.run_id) == identity
    frames = await s.join(cursor=cursor)
    assert frames[-1]["event"] == "end"
    assert any(s.expected_tail in json.dumps(frame.get("data")) for frame in frames)
    assert len(await s.start_receipts()) == 1
    evidence = await s.durable_receipt()
    evidence["later_placement_fixture"] = {"current_run_id": next_run, "generation": next_spec.generation, "executed": False}
    (s.directory / "historical-replay.json").write_text(json.dumps(evidence, indent=2, default=str))


@pytest.mark.asyncio
@pytest.mark.parametrize("reader,queued", [("cached", True)])
@pytest.mark.parametrize("owner", ["matching", "wrong-user", "wrong-thread"])
async def test_actual_queued_creator_wait_prepares_original_owner_before_claim(c07_scenario, reader, queued, owner, monkeypatch):
    from dataclasses import replace

    import httpx

    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME
    from app.gateway.internal_auth import create_internal_auth_headers

    s = c07_scenario
    assert s.claim is None and s.record.task is None
    async with s.engine.connect() as connection:
        assert await connection.scalar(text("SELECT state FROM fleet_run_placements WHERE run_id=:run"), {"run": s.record.run_id}) == "queued"
    admitted = s.record if owner == "matching" else replace(s.record, **({"user_id": "wrong-user"} if owner == "wrong-user" else {"thread_id": "wrong-thread"}))
    module = importlib.import_module("app.gateway.routers.thread_runs")

    async def trusted_admission(body, thread_id, request):
        # Controlled trusted-admission record injection only. Public activation
        # remains closed; actual HTTP wait, node claim, producer and SQL are used.
        return admitted

    monkeypatch.setattr(module, "start_run", trusted_admission)
    prepared_receipts = []
    actual_stop = None
    original_prepare = s.app.state.stream_bridge.prepare

    async def observe_prepare(record, cursor):
        try:
            prepared = await original_prepare(record, cursor)
            prepared_receipts.append({"user_id": prepared.expected_user_id, "thread_id": prepared.expected_thread_id, "identity": prepared.identity, "cursor": cursor})
            return prepared
        except Exception:
            prepared_receipts.append({"rejected": True, "cursor": cursor})
            raise

    monkeypatch.setattr(s.app.state.stream_bridge, "prepare", observe_prepare)
    csrf = uuid4().hex
    headers = create_internal_auth_headers(owner_user_id=s.record.user_id) | {CSRF_HEADER_NAME: csrf}
    async with httpx.AsyncClient(timeout=15, cookies={CSRF_COOKIE_NAME: csrf}) as client:
        task = asyncio.create_task(client.post(f"{s.url}/api/threads/{s.record.thread_id}/runs/wait", headers=headers, json={"input": {"messages": [{"role": "user", "content": "queued trusted creator"}]}}))
        try:
            if owner != "matching":
                await asyncio.sleep(0.25)
                assert task.done(), "Wrong admitted owner must reject before waiting for queued claim"
                response = await task
                assert response.status_code == 400
                assert await s.start_receipts() == [] and s.claim is None
            else:
                await asyncio.sleep(0.25)
                assert len(prepared_receipts) == 1, "Queued creator must prepare against its actual original record"
                assert prepared_receipts[0] == {"user_id": s.record.user_id, "thread_id": s.record.thread_id, "identity": None, "cursor": None}
                assert not task.done()
                await s.claim_original()
                await s.start_once()
                await s.wait_for_committed_model_event()
                assert not task.done()
                await s.release_model_barrier()
                response = await asyncio.wait_for(task, 10)
                assert response.status_code == 200 and "c07-final-tail" in json.dumps(response.json())
                await s.wait_for_runner_exit()
                actual_stop = await s.acknowledge_actual_stop()
                assert actual_stop["state"] == "succeeded"
                assert len(await s.start_receipts()) == 1
            async with s.engine.connect() as connection:
                seal = (await connection.execute(text("SELECT source,last_seq,core_status FROM fleet_stream_seals WHERE run_id=:run"), {"run": s.record.run_id})).mappings().one_or_none()
            (s.directory / "queued-creator-wait.json").write_text(
                json.dumps(
                    {
                        "trusted_admission_boundary_injected": True,
                        "owner_case": owner,
                        "status": response.status_code,
                        "response": response.json(),
                        "sql_seal": dict(seal) if seal else None,
                        "actual_stop": actual_stop,
                        "prepared": prepared_receipts,
                        "runner_starts": len(await s.start_receipts()),
                    },
                    indent=2,
                    default=str,
                )
            )
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["hydrated"])
@pytest.mark.parametrize("surface", ["cancel", "stream"])
@pytest.mark.parametrize("termination", ["mapping-eof", "network-cancel-adapter"])
async def test_actual_store_only_cancel_wait_does_not_add_observer_cancellation(c07_scenario, reader, surface, termination, monkeypatch):
    import httpx

    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME
    from app.gateway.internal_auth import create_internal_auth_headers
    from deerflow.runtime import RunManager

    from .c07_integration_fixture import bounded_wait

    s = c07_scenario
    # Real admitted/claimed remote run; no Agent is started for this observer
    # protocol negative matrix, and no physical-stop receipt is fabricated.
    from deerflow.config.run_ownership_config import RunOwnershipConfig

    manager = RunManager(store=s.app.state.run_store, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True))
    s.app.state.run_manager = manager
    observed = await manager.get(s.record.run_id)
    assert observed.store_only and observed.task is None and observed.on_disconnect.value == "cancel"
    cancellations = []
    original_cancel = manager.cancel

    async def observe_cancel(run_id, **kwargs):
        result = await original_cancel(run_id, **kwargs)
        cancellations.append({"run_id": run_id, "action": kwargs.get("action"), "outcome": result.value})
        return result

    monkeypatch.setattr(manager, "cancel", observe_cancel)
    module = importlib.import_module("app.gateway.routers.thread_runs")
    original_wait = module.wait_for_run_completion
    settled = asyncio.Event()

    async def observe_wait(*args, **kwargs):
        if termination == "network-cancel-adapter":
            s.app.state.original_wait_task = asyncio.current_task()
            s.app.state.wait_entered.set()
        try:
            return await original_wait(*args, **kwargs)
        finally:
            settled.set()

    monkeypatch.setattr(module, "wait_for_run_completion", observe_wait)
    csrf = uuid4().hex
    headers = create_internal_auth_headers(owner_user_id=s.record.user_id) | {CSRF_HEADER_NAME: csrf}
    path = f"{s.url}/api/threads/{s.record.thread_id}/runs/{s.record.run_id}/" + ("cancel?wait=true&action=interrupt" if surface == "cancel" else "stream?action=interrupt&wait=1")
    async with httpx.AsyncClient(timeout=10, cookies={CSRF_COOKIE_NAME: csrf}) as client:

        async def actual_network_request():
            from urllib.parse import urlsplit

            endpoint = urlsplit(path)
            network_reader, writer = await asyncio.open_connection(endpoint.hostname, endpoint.port)
            try:
                request_headers = headers | {"Host": endpoint.netloc, "Cookie": f"{CSRF_COOKIE_NAME}={csrf}", "Content-Length": "0", "Connection": "close"}
                wire = f"POST {endpoint.path}?{endpoint.query} HTTP/1.1\r\n" + "".join(f"{key}: {value}\r\n" for key, value in request_headers.items()) + "\r\n"
                writer.write(wire.encode())
                await writer.drain()
                return await network_reader.read()
            finally:
                writer.close()
                await writer.wait_closed()

        task = asyncio.create_task(actual_network_request() if termination == "network-cancel-adapter" else client.post(path, headers=headers))
        try:

            async def explicit_cancel_recorded():
                return bool(cancellations)

            await bounded_wait(explicit_cancel_recorded, seconds=2)
            assert len(cancellations) == 1 and cancellations[0]["action"] == "interrupt"
            if termination == "mapping-eof":
                async with s.engine.begin() as connection:
                    await connection.execute(text("UPDATE fleet_run_placements SET active_attempt_id=NULL WHERE run_id=:run"), {"run": s.record.run_id})
                reply = await asyncio.wait_for(task, 3)
                assert reply.status_code == 202
            else:
                task.cancel()  # Closes actual client TCP; adapter requires actual ASGI disconnect.
                await asyncio.gather(task, return_exceptions=True)
            await asyncio.wait_for(settled.wait(), 3)
            network_receipts = getattr(s.app.state, "network_disconnect_receipts", [])
            if termination == "network-cancel-adapter":
                assert len(network_receipts) == 1 and network_receipts[0]["type"] == "http.disconnect"
            assert len(cancellations) == 1, "Remote store-only wait observation must not add disconnect/EOF cancellation"
            async with s.engine.connect() as connection:
                action = await connection.scalar(text("SELECT cancel_action FROM runs WHERE run_id=:run"), {"run": s.record.run_id})
            assert action == "interrupt" and await s.start_receipts() == []
            (s.directory / "observer-cancel-wait.json").write_text(
                json.dumps(
                    {
                        "surface": surface,
                        "termination": termination,
                        "cancellations": cancellations,
                        "sql_cancel_action": action,
                        "observer_wait_settled": settled.is_set(),
                        "controlled_network_cancel_adapter": termination == "network-cancel-adapter",
                        "actual_asgi_disconnect": network_receipts,
                        "runner_starts": len(await s.start_receipts()),
                    },
                    indent=2,
                )
            )
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            (s.directory / "observer-transport-final.json").write_text(
                json.dumps({"termination": termination, "settled": settled.is_set(), "network_receipts": getattr(s.app.state, "network_disconnect_receipts", []), "cancellations": cancellations}, indent=2)
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["cached", "hydrated"])
async def test_actual_host_whole_retention_after_original_seal_returns_410_without_restart(c07_scenario, reader):
    import httpx

    from app.gateway.internal_auth import create_internal_auth_headers
    from deerflow.runtime import RunManager
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import current_remote_mutation_context

    s = c07_scenario
    await s.start_once()
    await s.wait_for_committed_model_event()
    await s.release_model_barrier()
    await s.wait_for_runner_exit()
    stopped = await s.acknowledge_actual_stop()
    assert stopped["state"] == "succeeded"
    if reader == "hydrated":
        s.app.state.run_manager = RunManager(store=s.app.state.run_store)
    frames = await s.join()
    assert frames[-1]["event"] == "end"
    actual_cursor = next(frame["id"] for frame in frames if frame.get("id"))
    await s.app.state.stream_bridge.publisher.stop()

    async def private_state():
        async with s.engine.connect() as connection:
            pointers = (
                await connection.execute(text("SELECT run_id,attempt_id,seq,generation,user_id,thread_id,launch_spec_digest,event_id,published_at FROM fleet_event_outbox WHERE run_id=:run ORDER BY seq"), {"run": s.record.run_id})
            ).all()
            seal = (await connection.execute(text("SELECT source,last_seq,core_status FROM fleet_stream_seals WHERE run_id=:run"), {"run": s.record.run_id})).one()
            return {"pointers": [list(row) for row in pointers], "seal": list(seal)}

    before = await private_state()
    assert before["seal"][0] == "writer" and before["seal"][1] > 0
    assert current_remote_mutation_context() is None
    # Trusted host retention through the actual supported store method, not an
    # original terminal remote capability performing a forbidden deletion.
    removed = await DbRunEventStore(s.app.state.run_store._sf).delete_by_run(s.record.thread_id, s.record.run_id, user_id=s.record.user_id)
    assert removed > 0 and await private_state() == before
    headers = create_internal_auth_headers(owner_user_id=s.record.user_id)
    replies = []
    async with httpx.AsyncClient(timeout=3) as client:
        for cursor, status in ((None, 410), (actual_cursor, 400)):
            request_headers = headers | ({"Last-Event-ID": cursor} if cursor else {})
            async with client.stream("GET", f"{s.url}/api/threads/{s.record.thread_id}/runs/{s.record.run_id}/join", headers=request_headers) as response:
                replies.append({"cursor": cursor, "status": response.status_code, "content_type": response.headers.get("content-type")})
                assert response.status_code == status
                replies[-1]["body"] = (await response.aread()).decode()
    async with s.engine.connect() as connection:
        remaining = await connection.scalar(text("SELECT count(*) FROM run_events WHERE run_id=:run"), {"run": s.record.run_id})
    assert remaining == 0 and len(await s.start_receipts()) == 1
    (s.directory / "host-retention-http.json").write_text(
        json.dumps(
            {
                "fixture": "supported trusted host DbRunEventStore.delete_by_run outside remote context",
                "reader": reader,
                "removed_host_events": removed,
                "remaining_host_events": remaining,
                "private_before": before,
                "private_after": await private_state(),
                "actual_stop": stopped,
                "http": replies,
                "runner_starts": await s.start_receipts(),
            },
            indent=2,
            default=str,
        )
    )
