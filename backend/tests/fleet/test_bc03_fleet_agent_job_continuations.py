"""Native parent STOP, accepted child outputs and original SQL admission."""

import asyncio
import json
import os
from dataclasses import asdict
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text

from .conftest import fleet_database as base_fleet_database
from .test_bc02_fleet_agent_job_dependencies import owner_environment as original_owner_environment
from .test_c02_remote_agent_admission import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner


@pytest_asyncio.fixture
async def fleet_database():
    original = base_fleet_database.__wrapped__()
    engine, sf, schema = await anext(original)
    if directory := os.environ.get("BC03_EVIDENCE_DIR"):
        Path(directory, "owned-schema.json").write_text(json.dumps({"schema": schema, "created": True, "dropped": False}))
    try:
        yield engine, sf, schema
    finally:
        await original.aclose()
        if directory := os.environ.get("BC03_EVIDENCE_DIR"):
            Path(directory, "owned-schema.json").write_text(json.dumps({"schema": schema, "created": True, "dropped": True}))


@pytest_asyncio.fixture
async def owner_environment(admission, tmp_path, monkeypatch):
    from deerflow.runtime import RunManager

    from . import test_c03_remote_agent_admission as original

    original_backend = original.backend

    def selected():
        backend = original_backend()
        backend.continuation_budget = 2
        backend.config = backend.config.model_copy(update={"continuations_enabled": True})
        return backend

    monkeypatch.setattr(original, "backend", selected)
    generator = original_owner_environment.__wrapped__(admission, tmp_path, monkeypatch)
    try:
        env = await anext(generator)
        env[4].state.run_manager = RunManager(store=env[4].state.run_store)
        async with env[0].begin() as conn:
            await conn.execute(text("UPDATE fleet_nodes SET cpu_millis=4000,memory_mib=8192"))
        yield env
    finally:
        await generator.aclose()


async def complete_children(sf, wire, node_id, node_session_id):
    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_ecs_fleet.job_service import FleetJobService
    from deerflow_ecs_fleet.persistence.attempts import JobAttempts
    from deerflow_ecs_fleet.persistence.manifests import FleetManifests
    from deerflow_ecs_fleet.scheduler import FleetScheduler
    from deerflow_ecs_fleet.workspace import NASWorkspace

    from app.fleet.job_tracking import read_tracking

    config = FleetConfig.model_validate(wire)
    jobs = FleetJobService(sf, config, tracking_reader=read_tracking)
    async with sf() as session:
        ids = list((await session.execute(text("SELECT id FROM fleet_jobs ORDER BY id"))).scalars())
    for job_id in ids:
        await jobs.reconcile(job_id)
    scheduler, attempts = FleetScheduler(sf, config), JobAttempts(sf, config)
    workspace = NASWorkspace(config.nas_root, identity=config.nas_identity)
    manifests = FleetManifests(sf, attempts=attempts, workspace=workspace)
    completed = []
    while claim := await scheduler.claim_job(node_id, node_session_id=node_session_id):
        identity = dict(node_id=node_id, node_session_id=node_session_id, attempt_id=claim.attempt_id, token=claim.token)
        grant = await attempts.authorize_start(**identity)
        await attempts.renew(**identity, running=True)
        output = workspace.prepare(asdict(claim), grant)
        child = await asyncio.create_subprocess_exec(*claim.spec["argv"], cwd=output)
        await child.wait()
        assert child.returncode == 0
        manifest = workspace.seal(asdict(claim), grant, stopped=True, max_bytes=1024)
        await attempts.stopped(**identity, reason="exit", exit_code=child.returncode)
        result = await manifests.complete(**identity, manifest=manifest)
        completed.append({"job_id": claim.job_id, "pid": child.pid, "exit": child.returncode, "manifest_id": result["manifest_id"], "files": manifest["files"]})
    if directory := os.environ.get("BC03_EVIDENCE_DIR"):
        Path(directory, "children-" + node_id + ".json").write_text(json.dumps(completed, indent=2))
    assert len(completed) == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc03_before_seal_two_coordinators_admit_exactly_one(checkpoint_owner, tmp_path, monkeypatch):
    from types import SimpleNamespace

    from fastapi import FastAPI

    from app.fleet.ownership import install_fleet_ownership
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime import RunManager

    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop

    item = checkpoint_owner
    from app.fleet.continuations import FleetContinuations

    original_dispatch = FleetContinuations.dispatch
    arrived = []
    release = asyncio.Event()

    async def concurrent_dispatch(coordinator, group_id):
        if not release.is_set():
            arrived.append(id(coordinator))
            if len(set(arrived)) >= 2:
                release.set()
            await asyncio.wait_for(release.wait(), timeout=15)
        return await original_dispatch(coordinator, group_id)

    # Rendezvous controls timing only; both calls execute the real SQL UOW.
    monkeypatch.setattr(FleetContinuations, "dispatch", concurrent_dispatch)
    # Two real host installations share DB truth and use independent managers.
    second = FastAPI()
    second.state.extensions = SimpleNamespace(services=(("fleet", item.env[3]),))
    second.state.run_store = RunRepository(item.env[1])
    second.state.run_manager = RunManager(store=second.state.run_store)
    install_fleet_ownership(second, item.env[1])
    observed = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path, monkeypatch, continuation_case="before_seal")
    if directory := os.environ.get("BC03_EVIDENCE_DIR"):
        Path(directory, "parent-observed.json").write_text(json.dumps(observed, default=str, indent=2))
    deadline = asyncio.get_running_loop().time() + 3
    while True:
        async with item.engine.connect() as conn:
            run_count = await conn.scalar(text("SELECT count(*) FROM runs"))
        if run_count == 2 or asyncio.get_running_loop().time() >= deadline:
            break
        await asyncio.sleep(0.02)
    assert run_count == 2, "Settled accepted children plus sealed/stopped parent must admit one NEW continuation run"
    assert len(set(arrived)) == 2
    if directory := os.environ.get("BC03_EVIDENCE_DIR"):
        Path(directory, "concurrent-coordinators.json").write_text(json.dumps({"distinct_coordinators": len(set(arrived)), "released_together": release.is_set()}))
    await verify_admission(item, observed, second)


async def verify_admission(item, observed, second):
    from langchain_core.messages import HumanMessage

    from app.fleet.continuations import FleetContinuations
    from app.fleet.execution import decode_graph_input
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime import RunManager

    group_id = observed["group_id"]
    coordinators = [item.env[4].state.fleet_continuations, second.state.fleet_continuations]
    records = await asyncio.gather(*(coordinators[i % 2].dispatch(group_id) for i in range(4)))
    restarted = FleetContinuations(item.env[1], item.env[3].config, RunManager(store=RunRepository(item.env[1])))
    recovered = await restarted.dispatch(group_id)
    async with item.engine.connect() as conn:
        runs = list((await conn.execute(text("SELECT run_id,status FROM runs ORDER BY created_at"))).mappings())
        placements = list((await conn.execute(text("SELECT run_id,agent_task_id,generation,state FROM fleet_run_placements ORDER BY created_at"))).mappings())
        task = (await conn.execute(text("SELECT id,current_run_id,generation,state,continuation_budget,wait_group_id FROM fleet_agent_tasks"))).mappings().one()
        group = (await conn.execute(text("SELECT * FROM fleet_wait_groups"))).mappings().one()
        payloads = list((await conn.execute(text("SELECT payload,payload_digest FROM fleet_launch_specs ORDER BY created_at"))).mappings())
        jobs = list(
            (
                await conn.execute(
                    text(
                        "SELECT j.id,j.state,j.accepted_manifest_id,a.stopped_at,a.finished_at,res.state reservation_state "
                        "FROM fleet_jobs j JOIN fleet_attempts a ON a.id=j.active_attempt_id "
                        "JOIN fleet_reservations res ON res.attempt_id=a.id"
                    )
                )
            ).mappings()
        )
        assert await conn.scalar(text("SELECT count(*) FROM fleet_artifact_manifests")) == 2
        assert await conn.scalar(text("SELECT count(*) FROM fleet_attempts WHERE kind='agent'")) == 1
    assert len(runs) == len(placements) == len(payloads) == 2
    assert runs[0]["run_id"] == item.spec.run_id and runs[0]["status"] == "success"
    new_id = runs[1]["run_id"]
    assert new_id != item.spec.run_id and runs[1]["status"] == "pending"
    assert all(record.run_id == new_id and record.store_only and record.task is None for record in [*records, recovered])
    assert task["id"] == item.spec.agent_task_id and task["generation"] == item.spec.generation
    assert task["state"] == "queued" and task["current_run_id"] == new_id and task["continuation_budget"] == 1
    assert task["wait_group_id"] is None and group["state"] == "dispatched" and group["continuation_run_id"] == new_id
    assert payloads[0]["payload_digest"] == item.spec.payload_digest()
    assert all(p["agent_task_id"] == task["id"] and p["generation"] == task["generation"] for p in placements)
    new = payloads[1]["payload"]
    assert new["source_workspace_point_id"] == observed["workspace_point_id"]
    assert new["source_workspace_checkpoint_id"] == observed["checkpoint_id"]
    data = decode_graph_input(new["input"])
    assert len(data["messages"]) == 1 and isinstance(data["messages"][0], HumanMessage)
    message = data["messages"][0]
    assert message.additional_kwargs["deerflow_untrusted_results"] is True
    from deerflow.agents.middlewares.input_sanitization_middleware import _USER_INPUT_BEGIN, _USER_INPUT_END

    framed = message.content.split("\n\n", 1)[1]
    assert framed.startswith(_USER_INPUT_BEGIN) and framed.endswith(_USER_INPUT_END)
    payload = json.loads(framed[len(_USER_INPUT_BEGIN) : -len(_USER_INPUT_END)])
    assert {row["job_id"] for row in payload["results"]} == set(observed["group_members"])
    assert len(jobs) == 2 and all(j["state"] == "succeeded" and j["accepted_manifest_id"] and j["stopped_at"] and j["finished_at"] and j["reservation_state"] == "released" for j in jobs)
    snapshot = await item.writer.aget({"configurable": {"thread_id": item.spec.thread_id, "checkpoint_id": observed["checkpoint_id"]}})
    history = list(snapshot["channel_values"]["messages"]) + data["messages"]
    calls = [c["id"] for message in history for c in getattr(message, "tool_calls", [])]
    paired = [message.tool_call_id for message in history if getattr(message, "type", None) == "tool"]
    assert len(calls) == len(set(calls)) and sorted(calls) == sorted(paired)
    if directory := os.environ.get("BC03_EVIDENCE_DIR"):
        Path(directory, "observed.json").write_text(
            json.dumps(
                {"parent": observed, "runs": [dict(r) for r in runs], "placements": [dict(p) for p in placements], "task": dict(task), "receipt": new_id, "child_count": len(jobs), "payload": new, "valid_tool_history": True},
                default=str,
                indent=2,
            )
        )


async def execute_continuation(item, tmp_path):
    """Actual new AgentRunner process restores NAS source and publishes its next pair."""
    import hashlib
    import sys
    from dataclasses import asdict

    import httpx
    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions, WorkspaceBoundaryIdentity
    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_ecs_fleet.workspace import NASWorkspace

    from deerflow.config import paths

    from .c04_integration_fixture import node_server

    runtime, app = item.env[3], item.env[4]
    await runtime.nodes.register(node_id="node-bc03-new", name="node-bc03-new", cpu_millis=4000, memory_mib=8192, agent_limit=1, profile_allowlist=["remote", "batch"])
    opened = await runtime.nodes.open_session("node-bc03-new", protocol_version=1)
    session_id = opened["node_session_id"]
    await runtime.nodes.heartbeat("node-bc03-new", node_session_id=session_id, protocol_version=1)
    credential = await runtime.credentials.issue("node-bc03-new", lifetime_seconds=600)
    accepted = await app.state.fleet_ownership.claim_agent("node-bc03-new", node_session_id=session_id, worker=item.env[8])
    assert accepted is not None
    spec = LaunchSpec.model_validate(accepted.launch_spec)
    identity = ExecutionIdentity(
        node_id="node-bc03-new",
        node_session_id=session_id,
        agent_task_id=spec.agent_task_id,
        generation=spec.generation,
        attempt_id=accepted.attempt_id,
        owner_worker_id=accepted.owner_worker_id,
        token_stamp=hashlib.sha256(accepted.token.encode()).hexdigest(),
    )
    versions = AgentWorkspaceVersions(NASWorkspace(runtime.config.nas_root, identity=runtime.config.nas_identity), max_input_bytes=runtime.config.max_input_bytes, max_output_bytes=runtime.config.profiles[spec.profile].max_output_bytes)
    home = tmp_path / "continuation-home"
    local = paths.Paths(home)
    local.ensure_thread_dirs(spec.thread_id, user_id=spec.user_id)
    source = local.sandbox_user_data_dir(spec.thread_id, user_id=spec.user_id)
    for category in ("workspace", "uploads", "outputs"):
        (source / category).rmdir()
    observation = tmp_path / "continuation-process.json"
    child = None
    stop = asyncio.Event()
    prepared = []
    restored = []
    claim = {"kind": "agent", "attempt_id": identity.attempt_id, "token": accepted.token}

    class Containers:
        def bind_claim(self, value):
            assert value is claim

        async def launch(self, grant, **kwargs):
            nonlocal child
            assert grant["accepted_workspace"]["point_id"] == spec.source_workspace_point_id
            fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                versions.restore(grant["accepted_workspace"]["manifest"], fd)
            finally:
                os.close(fd)
            restored.append((source / "outputs/result").read_text())
            payload = {
                "identity": asdict(identity),
                "spec": spec.model_dump(mode="json"),
                "private": item.private.model_dump(mode="json"),
                "fleet": runtime.config.model_dump(mode="json"),
                "grant": grant,
                "home": str(home),
                "observation": str(observation),
                "continue_existing": True,
            }
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                "import asyncio,json,sys; from fleet.test_bc02_fleet_agent_job_dependencies import original_child; asyncio.run(original_child(json.load(sys.stdin)))",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=os.environ.copy(),
            )
            child.stdin.write(json.dumps(payload).encode())
            await child.stdin.drain()
            child.stdin.close()
            return {"State": {"Status": "running", "Running": True}}

        async def inspect(self, ref):
            assert ref == "fleet-" + identity.attempt_id
            return {"State": {"Running": child.returncode is None, "ExitCode": child.returncode}}

        async def stop(self, ref):
            if child is not None and child.returncode is None:
                child.terminate()
                await child.wait()
            return True

    async def prepare(value, grant):
        return source

    async with node_server(app) as url, httpx.AsyncClient(base_url=url) as http:
        client = NodeClient(gateway_url=url, credential=credential.token, http_client=http)
        client.node_id, client.session_id = identity.node_id, identity.node_session_id
        auth = {"node_session_id": identity.node_session_id, "token": accepted.token}
        headers = {"Authorization": "Bearer " + credential.token}
        base = "/api/fleet/node/attempts/" + identity.attempt_id + "/workspace/"

        async def publish():
            while not stop.is_set():
                reply = await http.post(base + "poll", json=auth, headers=headers)
                if reply.status_code == 409:
                    await asyncio.sleep(0.01)
                    continue
                reply.raise_for_status()
                request = reply.json()["request"]
                if request is None or request["identity"]["request_id"] in prepared:
                    await asyncio.sleep(0.01)
                    continue
                boundary = WorkspaceBoundaryIdentity(**{**request["identity"], "presented_paths": tuple(request["identity"]["presented_paths"])})
                fields = {"request_id": boundary.request_id, "request_digest": boundary.request_digest, "barrier_epoch": request["barrier_epoch"], "nonce": "e" * 64}
                (await http.post(base + "claim", json=auth | fields, headers=headers)).raise_for_status()
                fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    manifest = await asyncio.to_thread(versions.seal, boundary, fd)
                finally:
                    os.close(fd)
                (await http.post(base + "prepared", json=auth | fields | {"manifest": manifest.model_dump(mode="json")}, headers=headers)).raise_for_status()
                prepared.append(boundary.request_id)

        publisher = asyncio.create_task(publish())
        daemon = NodeDaemon(client=client, containers=Containers(), state_dir=tmp_path / "continuation-daemon", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.01, poll_seconds=0.01)
        daemon._ready = True
        try:
            await daemon.execute(claim)
            stdout, stderr = await child.communicate()
            assert child.returncode == 0, stderr.decode()[-4000:]
        finally:
            stop.set()
            await publisher
            if child is not None and child.returncode is None:
                await child.wait()
    async with item.engine.connect() as conn:
        rows = list(
            (
                await conn.execute(
                    text(
                        "SELECT r.run_id,r.status,r.error,p.state,p.node_id,a.stopped_at,res.state reservation_state "
                        "FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id "
                        "JOIN fleet_attempts a ON a.id=p.active_attempt_id "
                        "JOIN fleet_reservations res ON res.attempt_id=a.id ORDER BY r.created_at"
                    )
                )
            ).mappings()
        )
        child_count = await conn.scalar(text("SELECT count(*) FROM fleet_jobs"))
        final_point = await conn.scalar(text("SELECT accepted_workspace_point_id FROM fleet_agent_tasks"))
    observed = {
        "runs": [dict(row) for row in rows],
        "restored_bytes": restored,
        "prepared": prepared,
        "new_pid": child.pid,
        "new_exit": child.returncode,
        "child_count": child_count,
        "new_final_point": final_point,
        "history": json.loads(observation.read_text()),
    }
    if directory := os.environ.get("BC03_EVIDENCE_DIR"):
        Path(directory, "continuation-observed.json").write_text(json.dumps(observed, default=str, indent=2))
        Path(directory, "continuation-process.txt").write_text(stdout.decode() + stderr.decode())
    assert restored == ["sealed original bytes"] and child_count == 2
    assert rows[0]["status"] == rows[1]["status"] == "success", observed
    assert rows[1]["node_id"] == "node-bc03-new" and rows[1]["stopped_at"] and rows[1]["reservation_state"] == "released"
    assert final_point != spec.source_workspace_point_id and observed["history"]["unpaired_tool_calls"] == 0
    assert observed["history"]["model_calls_total"] == 1 and observed["history"]["continuation_message_seen"]


def verify_bounded_observations():
    from types import SimpleNamespace

    from deerflow_ecs_fleet.artifacts import ArtifactFile
    from deerflow_ecs_fleet.continuation_payload import MAX_PAYLOAD_BYTES, child_summary, continuation_payload

    # Accepted artifact paths include Unicode and JSON escapes; no fake shortened refs.
    paths = ["/".join([('雪"' * 35) + str(index)] * 7) for index in range(16)]
    files = [ArtifactFile(path=path, size=1, sha256="a" * 64).model_dump() for path in sorted(paths)]
    jobs = [SimpleNamespace(id=f"job-{index:03}", state="succeeded", error='雪"\\' * 1024) for index in range(128)]
    results = [child_summary(job, SimpleNamespace(id=f"manifest-{job.id}", total_bytes=16, files=files)) for job in jobs]
    group = SimpleNamespace(continuation_key="bounded", job_ids=[job.id for job in jobs])
    encoded = continuation_payload(group, results)
    assert len(encoded.encode()) <= MAX_PAYLOAD_BYTES
    assert encoded == continuation_payload(group, list(reversed(results)))
    rows = json.loads(encoded)["results"]
    assert [row["job_id"] for row in rows] == sorted(group.job_ids)
    assert all(row["state"] == "succeeded" and row["manifest_id"] == f"manifest-{row['job_id']}" and row["total_bytes"] == 16 for row in rows)
    assert any(row["files_truncated"] for row in rows)
    assert any(row["error_truncated"] for row in rows)
    assert all(entry in files for row in rows for entry in row["files"])
    assert results == [child_summary(job, SimpleNamespace(id=f"manifest-{job.id}", total_bytes=16, files=files)) for job in jobs]
    with pytest.raises(ValueError, match="membership"):
        continuation_payload(group, results[:-1])


async def verify_child_admission_bound(jobs):
    from deerflow_ecs_fleet.persistence import job_links
    from deerflow_ecs_fleet.persistence.models import JobRow
    from deerflow_ecs_fleet.protocol import JobSpec
    from sqlalchemy import select

    async with jobs.sf() as session:
        children = list((await session.scalars(select(JobRow).order_by(JobRow.id))).all())
    assert len(children) == 2
    original = children[0]
    values = dict(
        user_id=original.user_id, thread_id=original.thread_id, source_run_id=original.source_run_id, tracking_task_id=original.tracking_task_id, idempotency_key=original.idempotency_key, spec=JobSpec.model_validate(original.spec)
    )
    previous = job_links.MAX_CHILDREN
    job_links.MAX_CHILDREN = 2
    try:
        assert (await jobs.submit(**values))["id"] == original.id
        with pytest.raises(ValueError, match="Awaited child limit"):
            await jobs.submit(**{**values, "tracking_task_id": "over-limit", "idempotency_key": "over-limit"})
        async with jobs.sf() as session:
            assert len(list((await session.scalars(select(JobRow))).all())) == 2
    finally:
        job_links.MAX_CHILDREN = previous


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc03_after_seal_restart_receipt_and_new_runner_source(checkpoint_owner, tmp_path, monkeypatch):
    from app.fleet.continuations import FleetContinuations
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime import RunManager

    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop

    verify_bounded_observations()
    item = checkpoint_owner
    runtime = item.env[3]
    enabled = runtime.config
    runtime.config = enabled.model_copy(update={"continuations_enabled": False})
    observed = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path, monkeypatch, continuation_case="after_seal")
    async with item.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM runs")) == 1
    runtime.config = enabled
    first = FleetContinuations(item.env[1], enabled, RunManager(store=RunRepository(item.env[1])))
    record = await first.dispatch(observed["group_id"])
    # Lose the acknowledgement/manager: committed receipt remains sole truth.
    restarted = FleetContinuations(item.env[1], enabled.model_copy(update={"enabled": False, "agents_enabled": False, "continuations_enabled": False}), RunManager(store=RunRepository(item.env[1])))
    recovered = await restarted.dispatch(observed["group_id"])
    assert recovered.run_id == record.run_id and recovered.idempotency_reused
    await verify_admission(item, observed, item.env[4])
    await execute_continuation(item, tmp_path)
