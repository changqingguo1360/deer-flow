"""BC07 native original executor/Node/PG with actual chat-completions HTTP."""

import asyncio
import json
import os
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, Request
from langchain_openai import ChatOpenAI
from sqlalchemy import text


def evidence(name, value):
    if directory := os.environ.get("BC07_EVIDENCE_DIR"):
        Path(directory, name + ".json").write_text(json.dumps(value, default=str, indent=2))


class ReceiptChatOpenAI(ChatOpenAI):
    """A real SDK model; the server owns synthetic decisions and measured usage."""

    def __init__(self, *, responses=None, **kwargs):
        super().__init__(model="bc07-controlled-cl100k", base_url=os.environ["BC07_PROVIDER_URL"] + "/v1", api_key="synthetic-local-provider", max_completion_tokens=4096, max_retries=0, disable_streaming=True, **kwargs)
        # The original factory hook is used once available; baseline keeps its SDK.
        from importlib.util import find_spec

        if find_spec("deerflow.models.budgeted_provider") is not None:
            from deerflow.models.budgeted_provider import guard_model

            guard_model(self, model_name="model-1", provider_use="langchain_openai:ChatOpenAI")


async def original_http_child(payload):
    """Run the accepted native fixture, replacing its provider only."""
    from contextlib import AsyncExitStack
    from importlib.util import find_spec

    import _agent_e2e_helpers

    from fleet.test_bc02_fleet_agent_job_dependencies import original_child

    _agent_e2e_helpers.FakeToolCallingModel = ReceiptChatOpenAI
    async with AsyncExitStack() as stack:
        if find_spec("app.fleet.model_budgets") is not None:
            from deerflow_ecs_fleet.launch_spec import LaunchSpec
            from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity
            from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

            from app.fleet.model_budgets import FleetModelBudgetCapability
            from deerflow.models.call_budget import call_budget_scope

            engine = create_async_engine(payload["private"]["database"]["postgres_url"], connect_args={"server_settings": {"search_path": payload["private"]["database"]["postgres_schema"]}})
            stack.push_async_callback(engine.dispose)
            capability = FleetModelBudgetCapability(async_sessionmaker(engine, expire_on_commit=False), ExecutionIdentity(**payload["identity"]), LaunchSpec.model_validate(payload["spec"]), contracts={"model-1": controlled_contract()})
            stack.enter_context(call_budget_scope(capability))
        await original_child(payload)


def controlled_contract():
    return {
        "adapter": "openai-chat-v1",
        "tokenizer": "cl100k_base",
        "serialization_revision": "utf8-json-framing-v1",
        "provider_use": "langchain_openai:ChatOpenAI",
        "target_model": "bc07-controlled-cl100k",
        "version": "v1",
        "max_completion_tokens": 4096,
        "framing_tokens_per_message": 32,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc07_cumulative_budget_original_execution(tmp_path, monkeypatch):
    import tiktoken

    from .c04_integration_fixture import node_server
    from .c12_integration_fixture import login
    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop
    from .test_bc03_fleet_agent_job_continuations import complete_children
    from .test_bc06_fleet_agent_job_dependencies import _bc06_case, _bc06_http_host

    # isort: split
    from . import test_c03_remote_agent_admission as source

    encoding = tiktoken.get_encoding("cl100k_base")
    receipts = []
    phase = 1
    provider = FastAPI()

    @provider.post("/v1/chat/completions")
    async def complete(request: Request):
        data = await request.json()
        assert data["model"] == "bc07-controlled-cl100k" and not data.get("stream")
        calls = [message for message in data["messages"] if message.get("tool_calls")]
        current_calls = [message for message in calls if any(call["id"].startswith("phase-" + str(phase) + "-") for call in message["tool_calls"])]
        if phase < 3 and not current_calls:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "phase-" + str(phase) + "-submit-" + str(i),
                        "type": "function",
                        "function": {
                            "name": "submit_fleet_job",
                            "arguments": json.dumps({"task_name": "phase-" + str(phase) + "-child-" + str(i), "profile": "batch", "argv": ["/bin/sh", "-c", "printf 'measured child result' > report.txt"], "link_mode": "awaited"}),
                        },
                    }
                    for i in range(2)
                ],
            }
        elif phase < 3 and len(current_calls) == 1:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {"id": "phase-" + str(phase) + "-await", "type": "function", "function": {"name": "await_fleet_jobs", "arguments": "{}"}},
                    {"id": "phase-" + str(phase) + "-sibling", "type": "function", "function": {"name": "settled_sibling", "arguments": "{}"}},
                ],
            }
        else:
            message = {"role": "assistant", "content": "Consumed the original accepted child results."}
        # Actual tokenizer measurements of all serialized fixture input/output.
        input_tokens = len(encoding.encode(json.dumps(data, ensure_ascii=False, separators=(",", ":"))))
        output_tokens = len(encoding.encode(json.dumps(message, ensure_ascii=False, separators=(",", ":"))))
        usage = {"prompt_tokens": input_tokens, "completion_tokens": output_tokens, "total_tokens": input_tokens + output_tokens}
        assert output_tokens <= data.get("max_completion_tokens", data.get("max_tokens", 0))
        receipts.append({"request": data, "response": message, "usage": usage})
        evidence("provider-http-receipts", receipts)
        return {
            "id": "bc07-" + str(len(receipts)),
            "object": "chat.completion",
            "created": 1,
            "model": data["model"],
            "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if message.get("tool_calls") else "stop"}],
            "usage": usage,
        }

    original_backend = source.backend

    def bounded_backend():
        backend = original_backend()
        backend.config = backend.config.model_copy(update={"task_run_limit": 3, "task_job_limit": 4, "task_token_limit": 1_000_000})
        return backend

    monkeypatch.setattr(source, "backend", bounded_backend)
    # Existing original helper launches a genuine process. Only its fixture entry
    # and synthetic provider endpoint change; no executor loop is substituted.
    subprocess_exec = asyncio.create_subprocess_exec

    async def launch(*argv, **kwargs):
        argv = tuple(
            value.replace("from fleet.test_bc02_fleet_agent_job_dependencies import original_child", "from fleet.test_bc07_fleet_agent_job_dependencies import original_http_child as original_child") if isinstance(value, str) else value
            for value in argv
        )
        return await subprocess_exec(*argv, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(root / "tests"), str(root), str(root / "packages/ecs-fleet"), os.environ.get("PYTHONPATH", "")]))
    async with node_server(provider) as provider_url:
        monkeypatch.setenv("BC07_PROVIDER_URL", provider_url)
        stack, item = await _bc06_case(tmp_path / "main", monkeypatch)
        try:
            observed = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path / "main", monkeypatch, continuation_case="bc07")
            evidence("original-parent-stop", observed)
            assert observed["stopped_ack"] and observed["reservation_state"] == "released"
            await item.env[3].nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
            completed = await complete_children(item.env[1], item.env[3].config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id)
            evidence("original-child-actual-processes", completed)
            # Hold only the second group's background dispatch until the
            # genuine human admission has committed. It never runs/cancels an
            # extra queued run: after release the ORIGINAL dispatcher rejects
            # the superseded group through its normal generation fence.
            from app.fleet.continuations import FleetContinuations

            dispatch = FleetContinuations.dispatch
            human_committed = asyncio.Event()

            async def rendezvous(coordinator, group_id):
                if group_id != observed["group_id"]:
                    await human_committed.wait()
                return await dispatch(coordinator, group_id)

            monkeypatch.setattr(FleetContinuations, "dispatch", rendezvous)
            app, password = await _bc06_http_host(item, monkeypatch, stack)
            async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
                await login(client, "bc06-boundary@example.com", password)
                path = "/api/threads/" + item.spec.thread_id + "/agent-tasks/" + item.spec.agent_task_id + "/resume"
                queued = await FleetContinuations(item.env[1], item.env[3].config, app.state.run_manager).dispatch(observed["group_id"])
                evidence("original-automatic-admission", {"run_id": queued.run_id, "store_only": queued.store_only})
                phase = 2
                second = await execute_budget_continuation(item, tmp_path / "second", expected_jobs=4, expected_calls=2)
                assert second["task"]["state"] == "waiting_jobs"
                assert second["task"]["wait_group_id"] != observed["group_id"]
                assert second["spec"]["source_workspace_point_id"] == observed["workspace_point_id"]
                assert all(row["manifest_id"] in json.dumps(second["spec"]["input"]) for row in completed)
                evidence("original-second-stop", second)
                completed_second = await complete_children(item.env[1], item.env[3].config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id)
                assert not ({row["job_id"] for row in completed} & {row["job_id"] for row in completed_second})
                evidence("original-second-child-processes", completed_second)
                third = await client.post(path, json={"expected_generation": second["task"]["generation"], "idempotency_key": "bc07-third-executed-generation"})
                assert third.status_code == 200, third.text
                human_committed.set()
                evidence("keyed-human-generation", third.json())
                phase = 3
                human = await execute_budget_continuation(item, tmp_path / "human", human=True, expected_jobs=4, expected_calls=1)
                assert human["spec"]["source_workspace_point_id"] == second["new_final_point"]
                assert human["history"]["continuation_message_seen"]
                assert all(row["manifest_id"] in json.dumps(human["spec"]["input"]) for row in completed_second)
                evidence("original-human-stop", human)
                async with item.engine.connect() as connection:
                    before = dict((await connection.execute(text("SELECT id,generation,current_run_id,state FROM fleet_agent_tasks"))).mappings().one())
                    run_count = await connection.scalar(text("SELECT count(*) FROM runs"))
                assert run_count == 3 and before["generation"] == item.spec.generation + 1
                fourth = await client.post(path, json={"expected_generation": before["generation"], "idempotency_key": "bc07-over-run-limit"})
                evidence("run-limit-http", {"status": fourth.status_code, "body": fourth.json(), "before": before, "measured_provider_calls": len(receipts)})
                assert fourth.status_code == 409, "Original admission admitted a fourth run despite the proposed frozen lifetime limit of three"
                async with item.engine.connect() as connection:
                    task = dict((await connection.execute(text("SELECT * FROM fleet_agent_tasks"))).mappings().one())
                    budget = dict((await connection.execute(text("SELECT * FROM fleet_task_budgets"))).mappings().one())
                    tickets = [dict(row) for row in (await connection.execute(text("SELECT * FROM fleet_model_reservations ORDER BY created_at"))).mappings()]
                    assert await connection.scalar(text("SELECT count(*) FROM runs")) == 3
                    assert await connection.scalar(text("SELECT count(*) FROM fleet_jobs")) == 4
                evidence("cumulative-ledger", {"task": task, "budget": budget, "tickets": tickets})
                assert budget["admitted_runs"] == 3 and budget["submitted_jobs"] == 4
                assert budget["run_limit"] == 3 and budget["job_limit"] == 4 and budget["token_limit"] == 1_000_000
                assert budget["spent_tokens"] == sum(receipt["usage"]["total_tokens"] for receipt in receipts)
                assert budget["reserved_tokens"] == 0 and len(tickets) == len(receipts)
                assert all(ticket["state"] == "settled" for ticket in tickets)
        finally:
            if "human_committed" in locals():
                human_committed.set()
            await stack.aclose()


async def execute_budget_continuation(item, tmp_path, *, human=False, expected_jobs, expected_calls):
    """Original BC03 Node/restore/publisher composition with cumulative assertions."""
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
    from deerflow_ecs_fleet.persistence.models import NodeRow

    async with item.env[1]() as session:
        existing = await session.get(NodeRow, "node-bc03-new")
    if existing is None:
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
            try:
                if child is not None and child.returncode is None:
                    child.kill()
                    await child.communicate()
            finally:
                await publisher
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
        task = dict((await conn.execute(text("SELECT * FROM fleet_agent_tasks"))).mappings().one())
        groups = [dict(row) for row in (await conn.execute(text("SELECT * FROM fleet_wait_groups ORDER BY created_at"))).mappings()]
        points = [dict(row) for row in (await conn.execute(text("SELECT * FROM fleet_workspace_points WHERE run_id=:run ORDER BY accepted_at,id"), {"run": spec.run_id})).mappings()]
        final_point = await conn.scalar(text("SELECT accepted_workspace_point_id FROM fleet_agent_tasks"))
    observed = {
        "spec": spec.model_dump(mode="json"),
        "new_run": next(dict(row) for row in rows if row["run_id"] == spec.run_id),
        "accepted_points": points,
        "task": task,
        "groups": groups,
        "runs": [dict(row) for row in rows],
        "restored_bytes": restored,
        "prepared": prepared,
        "new_pid": child.pid,
        "new_exit": child.returncode,
        "child_count": child_count,
        "new_final_point": final_point,
        "history": json.loads(observation.read_text()),
    }
    if directory := os.environ.get("BC07_EVIDENCE_DIR"):
        Path(directory, "continuation-" + spec.run_id + ".json").write_text(json.dumps(observed, default=str, indent=2))
        Path(directory, "continuation-" + spec.run_id + ".txt").write_text(stdout.decode() + stderr.decode())
    assert restored == ["sealed original bytes"] and child_count == expected_jobs
    assert all(row["status"] == "success" for row in rows), observed
    assert observed["new_run"]["node_id"] == "node-bc03-new" and observed["new_run"]["stopped_at"] and observed["new_run"]["reservation_state"] == "released"
    assert any(point["id"] == final_point and point["accepted_at"] is not None for point in points)
    assert final_point != spec.source_workspace_point_id and observed["history"]["unpaired_tool_calls"] == 0
    assert observed["history"]["model_calls_total"] == expected_calls
    if not human:
        assert observed["history"]["continuation_message_seen"]
    return observed


async def claim_budget_continuation(item):
    """Original admission/claim/start owner; no synthetic run or STOP rows."""
    import hashlib
    from types import SimpleNamespace

    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity

    from app.fleet.continuations import FleetContinuations

    async with item.engine.connect() as connection:
        group_id = await connection.scalar(text("SELECT wait_group_id FROM fleet_agent_tasks"))
    app, runtime = item.env[4], item.env[3]
    queued = await FleetContinuations(item.env[1], runtime.config, app.state.run_manager).dispatch(group_id)
    await runtime.nodes.register(node_id="node-bc07-budget", name="node-bc07-budget", cpu_millis=4000, memory_mib=8192, agent_limit=1, profile_allowlist=["remote", "batch"])
    opened = await runtime.nodes.open_session("node-bc07-budget", protocol_version=1)
    session_id = opened["node_session_id"]
    await runtime.nodes.heartbeat("node-bc07-budget", node_session_id=session_id, protocol_version=1)
    accepted = await app.state.fleet_ownership.claim_agent("node-bc07-budget", node_session_id=session_id, worker=item.env[8])
    assert accepted is not None
    spec = LaunchSpec.model_validate(accepted.launch_spec)
    assert spec.run_id == queued.run_id and spec.agent_task_id == item.spec.agent_task_id
    identity = ExecutionIdentity(
        node_id="node-bc07-budget",
        node_session_id=session_id,
        agent_task_id=spec.agent_task_id,
        generation=spec.generation,
        attempt_id=accepted.attempt_id,
        owner_worker_id=accepted.owner_worker_id,
        token_stamp=hashlib.sha256(accepted.token.encode()).hexdigest(),
    )
    grant = await app.state.fleet_ownership.authorize_start(node_id=identity.node_id, node_session_id=session_id, attempt_id=accepted.attempt_id, token=accepted.token)
    assert grant["accepted_workspace"]["point_id"] == spec.source_workspace_point_id
    return SimpleNamespace(identity=identity, spec=spec, grant=grant)


async def native_crash_parent(item, tmp_path, monkeypatch, *, crash=True):
    import sys
    from dataclasses import asdict

    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions, WorkspaceBoundaryIdentity
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_ecs_fleet.workspace import NASWorkspace

    import deerflow.persistence.models  # noqa: F401
    from deerflow.config import paths
    from deerflow.persistence.base import Base

    from .c04_integration_fixture import node_server

    normal_end, continuation_case = False, "bc07-crash"
    async with item.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    home = tmp_path / "home"
    monkeypatch.setattr(paths, "_paths", paths.Paths(home))
    paths._paths.ensure_thread_dirs(item.spec.thread_id, user_id=item.spec.user_id)
    source = paths._paths.sandbox_user_data_dir(item.spec.thread_id, user_id=item.spec.user_id)
    (source / "outputs/result").write_text("sealed original bytes")
    observation = tmp_path / "child-observed.json"
    fleet = item.env[3].config.model_copy(update={"continuations_enabled": True})
    versions = AgentWorkspaceVersions(NASWorkspace(fleet.nas_root, identity=fleet.nas_identity), max_input_bytes=item.grant["input_limits"]["max_input_bytes"], max_output_bytes=item.grant["execution_profile"]["max_output_bytes"])
    child = None
    stop = asyncio.Event()
    prepared = []
    claim = {"kind": "agent", "attempt_id": item.identity.attempt_id, "token": item.accepted.token}

    async def observe_sql():
        async with item.engine.connect() as connection:
            return dict(
                (
                    await connection.execute(
                        text(
                            "SELECT r.status old_run_status,t.state task_state,p.state placement_state,a.stopped_at,a.outcome,res.state reservation_state "
                            "FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id "
                            "JOIN fleet_attempts a ON a.id=p.active_attempt_id JOIN fleet_reservations res ON res.attempt_id=a.id"
                        )
                    )
                )
                .mappings()
                .one()
            )

    class Containers:
        def bind_claim(self, value):
            assert value is claim

        async def launch(self, grant, **kwargs):
            nonlocal child
            payload = {
                "identity": asdict(item.identity),
                "spec": item.spec.model_dump(mode="json"),
                "private": item.private.model_dump(mode="json"),
                "fleet": fleet.model_dump(mode="json"),
                "grant": grant,
                "home": str(home),
                "observation": str(observation),
                "normal_end": normal_end,
                "continuation_case": continuation_case,
            }
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                "import asyncio,json,sys; from fleet.test_bc07_fleet_agent_job_dependencies import original_http_child as original_child; asyncio.run(original_child(json.load(sys.stdin)))",
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
            assert ref == "fleet-" + item.identity.attempt_id
            # POSIX subprocess returncodes encode a signal as negative. The
            # ORIGINAL container protocol reports 128+signal (SIGKILL=137).
            protocol_exit = 128 - child.returncode if child.returncode is not None and child.returncode < 0 else child.returncode
            return {"State": {"Running": child.returncode is None, "ExitCode": protocol_exit}}

        async def stop(self, ref):
            if child.returncode is None:
                child.terminate()
                await child.wait()
            return True

    async def prepare(value, grant):
        return source

    async with node_server(item.env[4]) as url, httpx.AsyncClient(base_url=url) as http:
        client = NodeClient(gateway_url=url, credential=item.env[7].token, http_client=http)
        client.node_id, client.session_id = item.identity.node_id, item.identity.node_session_id
        auth = {"node_session_id": item.identity.node_session_id, "token": item.accepted.token}
        headers = {"Authorization": "Bearer " + item.env[7].token}
        base = "/api/fleet/node/attempts/" + item.identity.attempt_id + "/workspace/"

        async def publish_original():
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
                identity = WorkspaceBoundaryIdentity(**{**request["identity"], "presented_paths": tuple(request["identity"]["presented_paths"])})
                fields = {"request_id": identity.request_id, "request_digest": identity.request_digest, "barrier_epoch": request["barrier_epoch"], "nonce": "e" * 64}
                (await http.post(base + "claim", json=auth | fields, headers=headers)).raise_for_status()
                fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    manifest = await asyncio.to_thread(versions.seal, identity, fd)
                finally:
                    os.close(fd)
                (await http.post(base + "prepared", json=auth | fields | {"manifest": manifest.model_dump(mode="json")}, headers=headers)).raise_for_status()
                prepared.append(identity.request_id)

        publisher = asyncio.create_task(publish_original())
        daemon = NodeDaemon(client=client, containers=Containers(), state_dir=tmp_path / "daemon", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.01, poll_seconds=0.01)
        daemon._ready = True
        try:
            node_result = await daemon.execute(claim)
            evidence("crash-node-result" if crash else "unknown-usage-node-result", node_result)
            stdout, stderr = await child.communicate()
            if continuation_case and (directory := os.environ.get("BC03_EVIDENCE_DIR")):
                Path(directory, "parent-process.txt").write_text(stdout.decode() + stderr.decode())
            assert child.returncode == (-9 if crash else 0), stderr.decode()[-6000:]
        finally:
            stop.set()
            try:
                if child is not None and child.returncode is None:
                    child.kill()
                    await child.communicate()
            finally:
                await publisher
    final = await observe_sql()
    evidence("crash-native-stop" if crash else "unknown-usage-native-stop", {"pid": child.pid, "exit": child.returncode, "prepared": prepared, "sql": final})
    assert final["stopped_at"] is not None and final["reservation_state"] == "released"
    if crash:
        assert not observation.exists(), "Killed executor cannot write a completed checkpoint observation"
    else:
        assert observation.exists()
        evidence("unknown-usage-original-executor", json.loads(observation.read_text()))
    return final


def budget_denial(reason):
    """Inspect the ORIGINAL policy exception through SDK transport wrapping."""
    from contextlib import contextmanager

    from deerflow_ecs_fleet.task_budgets import TaskBudgetExceeded

    @contextmanager
    def expected():
        with pytest.raises(Exception) as observed:
            yield
        error, seen = observed.value, set()
        while error is not None and id(error) not in seen:
            if isinstance(error, TaskBudgetExceeded):
                assert isinstance(observed.value, TaskBudgetExceeded), "Private pretransport denial must retain its original middleware classification"
                assert str(error) == reason
                return
            seen.add(id(error))
            error = error.__cause__
        raise AssertionError("Original budget reason missing from actual exception chain") from observed.value

    return expected()


async def budget_sql(item):
    async with item.engine.connect() as connection:
        return {
            "task": dict((await connection.execute(text("SELECT * FROM fleet_agent_tasks"))).mappings().one()),
            "budget": dict((await connection.execute(text("SELECT * FROM fleet_task_budgets"))).mappings().one()),
            "tickets": [dict(row) for row in (await connection.execute(text("SELECT * FROM fleet_model_reservations ORDER BY created_at"))).mappings()],
            "jobs": [dict(row) for row in (await connection.execute(text("SELECT * FROM fleet_jobs ORDER BY id"))).mappings()],
            "run_count": await connection.scalar(text("SELECT count(*) FROM runs")),
        }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc07_unknown_deadline_and_parent_crash(tmp_path, monkeypatch):
    """One concentrated boundary: original budget denials and physical recovery."""
    import signal
    from dataclasses import asdict

    import tiktoken
    from deerflow_ecs_fleet.job_service import FleetJobService
    from deerflow_ecs_fleet.persistence.attempts import JobAttempts
    from deerflow_ecs_fleet.protocol import JobSpec
    from deerflow_ecs_fleet.scheduler import FleetScheduler
    from deerflow_ecs_fleet.task_budgets import TaskBudgetExceeded
    from deerflow_ecs_fleet.workspace import NASWorkspace
    from fastapi.responses import StreamingResponse

    from app.fleet.continuations import FleetContinuations
    from app.fleet.job_tracking import BoundJobParent, read_tracking
    from app.fleet.model_budgets import FleetModelBudgetCapability
    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.task_recovery import FleetTaskRecovery
    from deerflow.models.call_budget import call_budget_scope
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    from . import test_c03_remote_agent_admission as source
    from .c04_integration_fixture import node_server
    from .c12_integration_fixture import login
    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop
    from .test_bc03_fleet_agent_job_continuations import complete_children
    from .test_bc06_fleet_agent_job_dependencies import _bc06_case, _bc06_http_host

    root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(root / "tests"), str(root), str(root / "packages/ecs-fleet"), os.environ.get("PYTHONPATH", "")]))
    original_backend = source.backend
    mode, receipts, processes = "plain", [], []
    provider = FastAPI()
    encoding = tiktoken.get_encoding("cl100k_base")

    @provider.post("/v1/chat/completions")
    async def complete(request: Request):
        data = await request.json()
        receipt = {"mode": mode, "request": data}
        receipts.append(receipt)
        calls = [message for message in data["messages"] if message.get("tool_calls")]
        if mode in {"deadline", "crash", "token-cross-run", "job-cross-run"} and not calls:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "submit-" + str(i),
                        "type": "function",
                        "function": {
                            "name": "submit_fleet_job",
                            "arguments": json.dumps(
                                {
                                    "task_name": "child-" + str(i),
                                    "profile": "batch",
                                    "argv": ["/bin/sh", "-c", ("printf 'physical child result' > report.txt; exec sleep 45" if mode == "deadline" else "printf 'physical child result' > report.txt")],
                                    "link_mode": "awaited",
                                }
                            ),
                        },
                    }
                    for i in range(2)
                ],
            }
        elif mode in {"deadline", "token-cross-run", "job-cross-run"}:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [{"id": "await-one", "type": "function", "function": {"name": "await_fleet_jobs", "arguments": "{}"}}, {"id": "sibling-one", "type": "function", "function": {"name": "settled_sibling", "arguments": "{}"}}],
            }
        else:
            message = {"role": "assistant", "content": "Measured provider response."}
        usage = {"prompt_tokens": len(encoding.encode(json.dumps(data, ensure_ascii=False, separators=(",", ":")))), "completion_tokens": len(encoding.encode(json.dumps(message, ensure_ascii=False, separators=(",", ":"))))}
        usage["total_tokens"] = sum(usage.values())
        receipt["usage"] = usage
        evidence("boundary-provider-http", receipts)
        if mode == "crash" and calls:
            # This SECOND serialized request proves original tool submissions
            # returned. Kill the actual executor before returning any await call.
            assert len(processes) == 1 and processes[0].returncode is None
            processes[0].send_signal(signal.SIGKILL)
            await processes[0].wait()
            receipt["killed_pid"] = processes[0].pid
            evidence("boundary-provider-http", receipts)
        if mode == "stream-unknown":
            assert data["stream"]
            chunk = {"id": "bc07-stream", "object": "chat.completion.chunk", "created": 1, "model": data["model"], "choices": [{"index": 0, "delta": {"role": "assistant", "content": message["content"]}, "finish_reason": None}]}
            last = chunk | {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}
            # No authoritative usage despite an actual complete/closed stream.
            return StreamingResponse(iter(["data: " + json.dumps(chunk) + "\n\n", "data: " + json.dumps(last) + "\n\n", "data: [DONE]\n\n"]), media_type="text/event-stream")
        response = {
            "id": "bc07-boundary-" + str(len(receipts)),
            "object": "chat.completion",
            "created": 1,
            "model": data["model"],
            "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if message.get("tool_calls") else "stop"}],
            "usage": usage,
        }
        if mode == "invalid-usage":
            response["usage"] = usage | {"total_tokens": usage["total_tokens"] + 1}
        return response

    def limits(*, tokens=1_000_000, jobs=128, deadline=None):
        def backend():
            result = original_backend()
            result.config = result.config.model_copy(update={"task_token_limit": tokens, "task_job_limit": jobs})
            if deadline is not None:
                result.profile = result.profile.model_copy(update={"execution_timeout_seconds": deadline})
                result.config = result.config.model_copy(update={"profiles": result.config.profiles | {result.profile_name: result.profile}, "lease_seconds": 30, "renew_seconds": 1})
            return result

        monkeypatch.setattr(source, "backend", backend)

    def scope(item, owner=None):
        from contextlib import ExitStack

        stack = ExitStack()
        owner = owner or item
        mutation = FleetMutationCapability(owner.identity, owner.spec)
        capability = FleetModelBudgetCapability(item.env[1], owner.identity, owner.spec, contracts={"model-1": controlled_contract()})
        stack.enter_context(remote_mutation_scope(mutation.context))
        stack.enter_context(call_budget_scope(capability))
        stack.capability = capability
        return stack

    subprocess_exec = asyncio.create_subprocess_exec

    async def launch(*argv, **kwargs):
        argv = tuple(
            value.replace("from fleet.test_bc02_fleet_agent_job_dependencies import original_child", "from fleet.test_bc07_fleet_agent_job_dependencies import original_http_child as original_child") if isinstance(value, str) else value
            for value in argv
        )
        process = await subprocess_exec(*argv, **kwargs)
        if "-c" in argv and any("original_http_child" in str(value) for value in argv):
            processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)

    async with node_server(provider) as url:
        monkeypatch.setenv("BC07_PROVIDER_URL", url)
        # Cached ORIGINAL SDK model is constructed before private scope binding.
        cached = ReceiptChatOpenAI()
        limits(tokens=12_000)
        mode = "token-cross-run"
        stack, item = await _bc06_case(tmp_path / "token", monkeypatch)
        try:
            first = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path / "token", monkeypatch, continuation_case="bc07-token-cross-run")
            await item.env[3].nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
            children = await complete_children(item.env[1], item.env[3].config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id)
            prior = await budget_sql(item)
            owner = await claim_budget_continuation(item)
            bounds = []
            with scope(item, owner) as private:
                original_reserve = private.capability.reserve_async

                async def capture(bound):
                    bounds.append(dict(bound))
                    return await original_reserve(bound)

                monkeypatch.setattr(private.capability, "reserve_async", capture)
                before = len(receipts)
                from types import SimpleNamespace

                from deerflow.agents.middlewares.llm_error_handling_middleware import LLMErrorHandlingMiddleware

                middleware = LLMErrorHandlingMiddleware(app_config=item.private)

                async def denied_sdk(request):
                    return await cached.ainvoke("fresh fitting serialized request " + "x" * 7400)

                with budget_denial("task_token_budget_exhausted"):
                    await middleware.awrap_model_call(SimpleNamespace(), denied_sdk)
                assert len(receipts) == before
            assert len(bounds) == 1
            request_bound = bounds[0]["input_bound"] + bounds[0]["output_bound"]
            assert request_bound <= prior["budget"]["token_limit"]
            assert prior["budget"]["spent_tokens"] + request_bound > prior["budget"]["token_limit"]
            observed = await budget_sql(item)
            assert observed["budget"]["spent_tokens"] == prior["budget"]["spent_tokens"] == sum(row["usage"]["total_tokens"] for row in receipts if row["mode"] == mode)
            assert observed["budget"]["reserved_tokens"] == 0
            assert observed["budget"]["blocked_reason"] == "task_token_budget_exhausted"
            assert observed["run_count"] == observed["budget"]["admitted_runs"] == 2
            evidence(
                "token-pretransport-denial",
                observed
                | {
                    "prior": prior,
                    "original_first_stop": first,
                    "original_children": children,
                    "new_owner": {"spec": owner.spec.model_dump(mode="json"), "identity": asdict(owner.identity)},
                    "actual_guard_serialized_bounds": bounds,
                    "fresh_fits": True,
                    "prior_spent_causes_denial": True,
                    "provider_calls": len(receipts),
                    "execution_scope": "original C1/B1 then original admitted/claimed/start-authorized C2 SDK owner; no C2 graph/STOP qualification",
                },
            )
        finally:
            await stack.aclose()

        limits(jobs=2)
        mode = "job-cross-run"
        stack, item = await _bc06_case(tmp_path / "job", monkeypatch)
        try:
            first = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path / "job", monkeypatch, continuation_case="bc07-job-cross-run")
            await item.env[3].nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
            children = await complete_children(item.env[1], item.env[3].config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id)
            prior = await budget_sql(item)
            owner = await claim_budget_continuation(item)
            mutation = FleetMutationCapability(owner.identity, owner.spec)
            jobs = FleetJobService(item.env[1], item.env[3].config, tracking_reader=read_tracking, parent_capability=BoundJobParent(mutation))
            with remote_mutation_scope(mutation.context):
                with pytest.raises(TaskBudgetExceeded, match="task_job_budget_exhausted"):
                    await jobs.submit(
                        user_id=owner.spec.user_id,
                        thread_id=owner.spec.thread_id,
                        source_run_id=owner.spec.run_id,
                        tracking_task_id="bc07-new-run-tracking",
                        idempotency_key="bc07-new-run-job",
                        spec=JobSpec(task_name="new-run-job", profile="batch", argv=["/bin/true"], link_mode="awaited"),
                    )
            observed = await budget_sql(item)
            assert {row["id"] for row in observed["jobs"]} == {row["id"] for row in prior["jobs"]}
            assert len(observed["jobs"]) == observed["budget"]["submitted_jobs"] == 2
            assert all(row["state"] == "succeeded" and row["accepted_manifest_id"] for row in observed["jobs"])
            assert observed["run_count"] == observed["budget"]["admitted_runs"] == 2
            assert observed["budget"]["blocked_reason"] == "task_job_budget_exhausted"
            evidence(
                "job-original-denial",
                observed
                | {
                    "prior": prior,
                    "original_first_stop": first,
                    "original_children": children,
                    "new_owner": {"spec": owner.spec.model_dump(mode="json"), "identity": asdict(owner.identity)},
                    "execution_scope": "original C1/B1 then original admitted/claimed/start-authorized C2 JobService owner; no C2 graph/STOP qualification",
                },
            )
        finally:
            await stack.aclose()

        if os.environ.get("BC07_EVIDENCE_SCOPE") == "token-job-cross-run":
            evidence("explicit-partial-scope", {"evidence_scope": "token/job cross-run only", "carried_unmodified_scope": "boundary05 stream/invalid/deadline/crash", "full_boundary_latest_execution": False})
            return

        limits()
        mode = "stream-unknown"
        stack, item = await _bc06_case(tmp_path / "stream", monkeypatch)
        try:
            stream_model = cached.model_copy(update={"disable_streaming": False})
            with scope(item):
                chunks = [chunk async for chunk in stream_model.astream("measured streaming request")]
                assert any(chunk.content for chunk in chunks)
                before = len(receipts)
                with budget_denial("model_usage_unknown"):
                    await cached.ainvoke("later cached call must be blocked")
                assert len(receipts) == before
                mutation = FleetMutationCapability(item.identity, item.spec)
                jobs = FleetJobService(item.env[1], item.env[3].config, tracking_reader=read_tracking, parent_capability=BoundJobParent(mutation))
                with budget_denial("model_usage_unknown"):
                    await jobs.submit(
                        user_id=item.spec.user_id,
                        thread_id=item.spec.thread_id,
                        source_run_id=item.spec.run_id,
                        tracking_task_id="bc07-unknown-job",
                        idempotency_key="bc07-unknown-admission",
                        spec=JobSpec(task_name="blocked", profile="batch", argv=["/bin/true"], link_mode="awaited"),
                    )
            observed = await budget_sql(item)
            assert observed["jobs"] == [] and observed["budget"]["submitted_jobs"] == 0
            ticket = observed["tickets"][0]
            assert ticket["state"] == "unknown" and observed["budget"]["blocked_reason"] == "model_usage_unknown"
            assert observed["budget"]["spent_tokens"] == 0
            assert observed["budget"]["reserved_tokens"] == ticket["input_bound"] + ticket["output_bound"]
            evidence("stream-unknown-full-charge", observed | {"provider_calls": before})
        finally:
            await stack.aclose()

        mode = "invalid-usage"
        stack, item = await _bc06_case(tmp_path / "invalid", monkeypatch)
        try:
            # Original graph receives a valid answer with INVALID accounting and
            # reaches END. Its genuine final publisher/STOP must pause the task.
            stopped = await native_crash_parent(item, tmp_path / "invalid", monkeypatch, crash=False)
            observed = await budget_sql(item)
            assert observed["tickets"][0]["state"] == "unknown"
            assert observed["budget"]["reserved_tokens"] > 0 and observed["budget"]["spent_tokens"] == 0
            assert observed["budget"]["blocked_reason"] == "model_usage_unknown"
            assert observed["task"]["state"] == "paused" and observed["task"]["wait_group_id"] is None
            async with item.engine.connect() as connection:
                points = [dict(row) for row in (await connection.execute(text("SELECT kind,desired_core_status,desired_task_status,accepted_at FROM fleet_workspace_points"))).mappings()]
            assert len(points) == 1 and points[0]["kind"] == "paused" and points[0]["desired_task_status"] == "paused"
            assert points[0]["accepted_at"] is not None and stopped["stopped_at"] is not None
            app, _ = await _bc06_http_host(item, monkeypatch, stack)
            scan_deadline = asyncio.get_running_loop().time() + 3
            while getattr(app.state, "fleet_continuations", None) is None:
                assert asyncio.get_running_loop().time() < scan_deadline
                await asyncio.sleep(0.05)
            await asyncio.sleep(1.1)
            assert (await budget_sql(item))["run_count"] == 1
            evidence("invalid-usage-original-terminal-full-charge", observed | {"physical_stop": stopped, "accepted_points": points})
        finally:
            await stack.aclose()

        # Deadline is frozen at ORIGINAL admission. No SQL clock/deadline edits.
        mode = "deadline"
        limits(deadline=15)
        stack, item = await _bc06_case(tmp_path / "deadline", monkeypatch)
        child = None
        identity = None
        try:
            observed = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path / "deadline", monkeypatch, continuation_case="bc07-deadline")
            await item.env[3].nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
            config, sf = item.env[3].config, item.env[1]
            jobs = FleetJobService(sf, config, tracking_reader=read_tracking)
            async with item.engine.connect() as connection:
                ids = list((await connection.execute(text("SELECT id FROM fleet_jobs ORDER BY id"))).scalars())
            for job_id in ids:
                await jobs.reconcile(job_id)
            claim = await FleetScheduler(sf, config).claim_job(item.identity.node_id, node_session_id=item.identity.node_session_id)
            assert claim is not None
            identity = dict(node_id=item.identity.node_id, node_session_id=item.identity.node_session_id, attempt_id=claim.attempt_id, token=claim.token)
            attempts = JobAttempts(sf, config)
            grant = await attempts.authorize_start(**identity)
            await attempts.renew(**identity, running=True)
            output = NASWorkspace(config.nas_root, identity=config.nas_identity).prepare(asdict(claim), grant)
            child = await subprocess_exec(*claim.spec["argv"], cwd=output)
            # Original expirer uses the real PostgreSQL clock and immutable lease.
            deadline = asyncio.get_running_loop().time() + config.lease_seconds + 5
            while True:
                await attempts.expire_pending()
                async with item.engine.connect() as connection:
                    row = dict(
                        (
                            await connection.execute(
                                text(
                                    "SELECT j.state job_state,a.state attempt_state,a.lease_expires_at,a.stopped_at,res.state reservation_state,"
                                    "res.cpu_millis,res.memory_mib,clock_timestamp() now FROM fleet_jobs j "
                                    "JOIN fleet_attempts a ON a.id=j.active_attempt_id JOIN fleet_reservations res ON res.attempt_id=a.id WHERE j.id=:job"
                                ),
                                {"job": claim.job_id},
                            )
                        )
                        .mappings()
                        .one()
                    )
                if row["job_state"] == "unknown":
                    break
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.2)
            assert child.returncode is None and row["stopped_at"] is None
            assert row["reservation_state"] == "quarantined" and row["cpu_millis"] > 0 and row["memory_mib"] > 0
            await FleetTaskRecovery(sf).reconcile(item.spec.agent_task_id)
            state = await budget_sql(item)
            assert state["task"]["deadline"] <= row["now"]
            assert state["budget"]["blocked_reason"] == "task_deadline_unknown_child"
            assert state["task"]["state"] == "recovery_required"
            app, _ = await _bc06_http_host(item, monkeypatch, stack)
            from deerflow.runtime.runs.manager import ConflictError

            with pytest.raises(ConflictError):
                await FleetContinuations(sf, config, app.state.run_manager).dispatch(observed["group_id"])
            assert (await budget_sql(item))["run_count"] == 1
            evidence("unknown-child-real-deadline", state | {"child_pid": child.pid, "child_alive": child.returncode is None, "physical": row, "parent": observed})
        finally:
            if child is not None and child.returncode is None:
                child.terminate()
                await child.wait()
            if identity is not None:
                await JobAttempts(item.env[1], item.env[3].config).stopped(**identity, reason="lease_lost", exit_code=child.returncode if child is not None else -1)
            await stack.aclose()

        # Real SIGKILL after original child submit, before await/paired yield.
        mode, processes = "crash", []
        limits()
        stack, item = await _bc06_case(tmp_path / "crash", monkeypatch)
        try:
            stopped = await native_crash_parent(item, tmp_path / "crash", monkeypatch)
            assert processes[0].returncode == -signal.SIGKILL
            before = await budget_sql(item)
            assert len(before["jobs"]) == 2 and before["task"]["wait_group_id"] is None
            async with item.engine.connect() as connection:
                assert await connection.scalar(text("SELECT count(*) FROM fleet_wait_groups")) == 0
                assert await connection.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE checkpoint_id IS NOT NULL")) == 0
            await item.env[3].nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
            completed = await complete_children(item.env[1], item.env[3].config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id)
            app, password = await _bc06_http_host(item, monkeypatch, stack)
            await FleetTaskRecovery(item.env[1]).reconcile(item.spec.agent_task_id)
            # Run the original bounded coordinator scan after children succeed.
            loop_deadline = asyncio.get_running_loop().time() + 3
            while getattr(app.state, "fleet_continuations", None) is None:
                assert asyncio.get_running_loop().time() < loop_deadline
                await asyncio.sleep(0.05)
            await asyncio.sleep(1.1)
            state = await budget_sql(item)
            assert all(job["state"] == "succeeded" and job["accepted_manifest_id"] for job in state["jobs"])
            assert state["run_count"] == state["budget"]["admitted_runs"] == 1
            assert state["task"]["generation"] == item.spec.generation
            assert state["task"]["state"] == "recovery_required"
            assert state["budget"]["reserved_tokens"] == before["budget"]["reserved_tokens"] > 0
            assert state["budget"]["blocked_reason"] in {"parent_execution_unknown", "parent_stopped_without_accepted_pair"}
            async with node_server(app) as host_url, httpx.AsyncClient(base_url=host_url) as client:
                await login(client, "bc06-boundary@example.com", password)
                response = await client.get("/api/threads/" + item.spec.thread_id + "/agent-tasks")
                assert response.status_code == 200, response.text
                summary = response.json()[0]
                assert summary["execution_uncertain"] and summary["recovery_required"]
                assert summary["budget"]["reserved_tokens"] == state["budget"]["reserved_tokens"]
                assert summary["blocked_reason"] == state["budget"]["blocked_reason"]
                evidence("crash-owned-public-recovery", response.json())
            evidence("crash-child-success-no-autoresume", state | {"children": completed, "physical_stop": stopped, "actual_exit": processes[0].returncode})
        finally:
            for process in processes:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            await stack.aclose()
