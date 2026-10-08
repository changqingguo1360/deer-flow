"""One actual installed single-slot C→B→C spine and concentrated policy boundary."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from .c04_integration_fixture import node_server


@asynccontextmanager
async def child_provider(evidence):
    app = FastAPI()
    calls = []
    control = {"submitted": asyncio.Event(), "release": asyncio.Event()}

    @app.post("/v1/chat/completions")
    async def complete(request: Request):
        body = await request.json()
        messages = body["messages"]
        continued = any("The awaited background jobs have settled." in str(message.get("content")) for message in messages)
        tools = [message for message in messages if message["role"] == "tool"]
        call = None
        if not continued and tools:
            control["submitted"].set()
            await control["release"].wait()
        if not continued:
            name = "submit_fleet_job" if not tools else "await_fleet_jobs"
            args = (
                {
                    "task_name": "single-child",
                    "profile": "batch-standard",
                    "argv": ["/bin/sh", "-c", "printf 'single-slot child result\\n' > child.txt"],
                    "link_mode": "awaited",
                    "execution_timeout_seconds": 120,
                    "queue_timeout_seconds": 120,
                }
                if not tools
                else {}
            )
            call = {"id": "bc05-" + name, "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}
        else:
            assert "child.txt" in str(messages)
        calls.append({"continued": continued, "tool_count": len(tools), "last_tool": tools[-1]["content"] if tools else None, "chosen_tool": call["function"]["name"] if call else None})
        (evidence / "provider.json").write_text(json.dumps(calls, indent=2))
        message = {"role": "assistant", "content": "" if call else "The original child result was consumed."}
        if call:
            message["tool_calls"] = [call]
        common = {"id": "bc05-" + str(len(calls)), "created": 1791216000, "model": body["model"]}
        if not body.get("stream"):
            return common | {"object": "chat.completion", "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if call else "stop"}], "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}}

        async def chunks():
            delta = dict(message)
            if call:
                delta["tool_calls"] = [{"index": 0, **call}]
            for payload in (
                common | {"object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
                common | {"object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if call else "stop"}], "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
            ):
                yield "data: " + json.dumps(payload) + "\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(chunks(), media_type="text/event-stream")

    async with node_server(app) as url:
        yield url, calls, control


@pytest.mark.live
@pytest.mark.asyncio
async def test_bc05_single_slot_cbc_main(tmp_path, monkeypatch):
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication
    from deerflow_ecs_fleet.workspace import NASWorkspace

    from deerflow.config import paths

    from .c12_integration_fixture import login, normal_gateway, owned_database, private_configuration, response_json
    from .test_c01_remote_agent_admission import c_config

    evidence = Path(os.environ["BC05_EVIDENCE_DIR"])
    image, docker = os.environ["FLEET_AGENT_TEST_IMAGE"], os.environ["C12_DOCKER"]
    monkeypatch.setenv("AUTH_JWT_SECRET", "bc05-owned-key-" + tmp_path.name)
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(paths, "_paths", paths.Paths(tmp_path / "home"))
    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("fleet-test\n")
    async with owned_database(evidence) as db, child_provider(evidence) as (provider_url, calls, control):
        private = private_configuration(db, provider_url)
        driver = AgentContainers(state_dir=tmp_path / "node", operator_config=private, provider="gateway", executable=docker)
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        (evidence / "installed-compatibility.json").write_text(actual.model_dump_json(indent=2))
        host = {**private, "database": {**private["database"], "postgres_url": db.host_url}, "models": [{**private["models"][0], "base_url": provider_url + "/v1"}]}
        async with normal_gateway(host) as (_, client, _):
            user = await response_json(client, "POST", "/api/v1/auth/initialize", json={"email": "bc05@example.com", "password": "BC05-Private-4827"})
        fleet = c_config()
        fleet.update(nas_root=str(nas), nas_identity="fleet-test", continuations_enabled=True, scheduling_mode="serial")
        for name in ("remote", "batch-standard"):
            fleet["profiles"][name].update(image=image, user=str(os.getuid()) + ":" + str(os.getgid()), network="bridge", execution_timeout_seconds=180)
        fleet["profiles"]["remote"].update(runtime_digest=actual.runtime_digest, pids_limit=256)
        fleet["agent_bindings"] = {
            "remote": {
                "allowed_user_ids": [user["id"]],
                "model_name": "model-1",
                "model_version": "c12-v1",
                "compatibility": actual.model_dump(mode="json"),
                "secret_refs": [{"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"}],
                "continuation_budget": 2,
            }
        }
        plugin = {"name": "ecs-fleet", "package": "deerflow-ecs-fleet", "use": "deerflow_ecs_fleet:install", "required": True, "table_prefix": "fleet_", "config": fleet}
        private["plugins"] = [plugin]
        driver.operator_config = private
        host["plugins"] = [plugin]
        async with normal_gateway(host) as (app, client, url):
            await login(client, "bc05@example.com", "BC05-Private-4827")
            node_id = "bc05-" + __import__("uuid").uuid4().hex
            await response_json(client, "POST", "/api/fleet/machines", json={"node_id": node_id, "name": node_id, "cpu_millis": 1000, "memory_mib": 2048, "agent_limit": 1, "profile_allowlist": ["remote", "batch-standard"]})
            credential = await response_json(client, "POST", f"/api/fleet/machines/{node_id}/credentials", json={"lifetime_seconds": 600})
            await response_json(client, "POST", "/api/threads", json={"thread_id": "bc05-original"})
            run = await response_json(
                client,
                "POST",
                "/api/threads/bc05-original/runs",
                json={
                    "input": {"messages": [{"role": "user", "content": "Submit the single child, await and consume its result."}]},
                    "config": {"recursion_limit": 1000, "context": {"model_name": "model-1", "subagent_enabled": False}},
                    "stream_mode": ["values", "messages-tuple", "custom"],
                    "execution": {"preference": "remote", "profile": "remote"},
                },
            )
            async with httpx.AsyncClient(base_url=url + "/", timeout=15) as node_http:
                node = NodeClient(gateway_url=url, credential=credential["token"], claim_kind="mixed", compatibility=actual.model_dump(mode="json"), http_client=node_http)

                async def prepare(claim, grant):
                    return await driver.prepare_workspace(nas, claim, grant)

                daemon = NodeDaemon(
                    client=node, containers=driver, state_dir=tmp_path / "node", workspace=NASWorkspace(nas, identity="fleet-test"), agent_prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.25, poll_seconds=0.05
                )
                stager = AgentWorkspacePublication(client=node, containers=driver, nas=NASWorkspace(nas, identity="fleet-test"), journal=daemon.journal)
                daemon.workspace_publications = stager
                await daemon.bootstrap()
                mixed = await node_http.post("api/fleet/node/claims", headers={"Authorization": "Bearer " + credential["token"]}, json={"kind": "mixed", "node_session_id": node.session_id, "compatibility": actual.model_dump(mode="json")})
                assert mixed.status_code == 200, "A single Node session must claim both B and C through the stock mixed protocol: " + mixed.text
                owned = []
                execution = None
                try:
                    claim = mixed.json()
                    for index in range(3):
                        if index:
                            async with asyncio.timeout(30):
                                while (claim := await node.claim()) is None:
                                    await node.heartbeat()
                                    await asyncio.sleep(0.05)
                        owned.append("fleet-" + claim["attempt_id"])
                        execution = asyncio.create_task(daemon.execute(claim))
                        if index == 0:
                            submitted = asyncio.create_task(control["submitted"].wait())
                            try:
                                async with asyncio.timeout(60):
                                    done, _ = await asyncio.wait({submitted, execution}, return_when=asyncio.FIRST_COMPLETED)
                                assert submitted in done, "Original C stopped before the child submission: " + str(execution.result())
                            finally:
                                submitted.cancel()
                                await asyncio.gather(submitted, return_exceptions=True)
                            from app.fleet.runtime import fleet_runtime

                            async with db.engine.connect() as connection:
                                child_id = await connection.scalar(text("SELECT id FROM fleet_jobs"))
                            assert child_id is not None, calls
                            await fleet_runtime(app).jobs.reconcile(child_id)
                            assert await node.claim() is None
                            active = await driver.inspect(owned[-1])
                            assert active["State"]["Running"]
                            async with db.engine.connect() as connection:
                                charges = (await connection.execute(text("SELECT count(*),sum(cpu_millis),sum(memory_mib),sum(agent_units) FROM fleet_reservations WHERE state!='released'"))).one()
                                assert tuple(charges) == (1, 1000, 2048, 1)
                                assert await connection.scalar(text("SELECT state FROM fleet_jobs WHERE id=:job"), {"job": child_id}) == "queued"
                            (evidence / "before-parent-stop.json").write_text(json.dumps({"active_container": active["State"], "charges": list(charges), "child_id": child_id, "child_claim": None}, indent=2))
                            control["release"].set()
                        async with asyncio.timeout(120):
                            result = await asyncio.shield(execution)
                        assert not stager.pending_copies and not stager.pending_saves
                        observation = await driver.inspect(owned[-1])
                        assert observation and not observation["State"]["Running"] and observation["State"]["ExitCode"] == 0
                        assert not result["report_pending"]
                        async with db.engine.connect() as connection:
                            rows = (
                                (await connection.execute(text("SELECT a.kind,a.id,a.started_at,a.stopped_at,r.state reservation FROM fleet_attempts a JOIN fleet_reservations r ON r.attempt_id=a.id ORDER BY a.created_at,a.id")))
                                .mappings()
                                .all()
                            )
                            assert all(row["stopped_at"] is not None and row["reservation"] == "released" for row in rows)
                            assert await connection.scalar(text("SELECT count(*) FROM fleet_reservations WHERE state!='released'")) == 0
                        (evidence / ("step-" + str(index) + ".json")).write_text(json.dumps({"rows": [dict(row) for row in rows], "container": observation["State"], "node": result}, default=str, indent=2))
                    assert [row["kind"] for row in rows] == ["agent", "job", "agent"]
                    async with db.engine.connect() as connection:
                        assert await connection.scalar(text("SELECT count(*) FROM fleet_jobs")) == 1
                        assert await connection.scalar(text("SELECT count(*) FROM fleet_wait_groups WHERE state='dispatched' AND continuation_run_id IS NOT NULL")) == 1
                        assert await connection.scalar(text("SELECT count(*) FROM runs WHERE status='success'")) == 2
                        assert await connection.scalar(text("SELECT count(*) FROM fleet_agent_tasks WHERE state='succeeded'")) == 1
                    assert [call["chosen_tool"] for call in calls] == ["submit_fleet_job", "await_fleet_jobs", None]
                    (evidence / "main-observations.json").write_text(json.dumps({"sequence": [row["kind"] for row in rows], "initial_run_id": run["run_id"], "calls": calls}, indent=2))
                finally:
                    control["release"].set()
                    async with db.engine.connect() as connection:
                        snapshots = {}
                        for table, columns in {
                            "fleet_agent_tasks": "state,generation,current_run_id,deadline,cancel_requested_at",
                            "runs": "status,owner_worker_id,lease_expires_at,cancel_action,cancel_requested_at",
                            "fleet_run_placements": "state,generation,active_attempt_id,node_id",
                            "fleet_attempts": "kind,state,node_session_id,process_ref,start_authorized_at,stopped_at,lease_expires_at,execution_deadline",
                            "fleet_reservations": "state,node_id,agent_units,cpu_millis,memory_mib,released_at",
                            "fleet_nodes": "session_id,health,last_seen_at,claim_kinds",
                        }.items():
                            snapshots[table] = [dict(row) for row in (await connection.execute(text("SELECT " + columns + " FROM " + table))).mappings()]
                        snapshots["database_clock"] = await connection.scalar(text("SELECT clock_timestamp()"))
                        snapshots["execution_done"] = execution is not None and execution.done()
                        if execution is not None and execution.done() and not execution.cancelled():
                            error = execution.exception()
                            snapshots["daemon_result"] = {"error_type": type(error).__name__} if error else execution.result()
                    (evidence / "fence-snapshot.json").write_text(json.dumps(snapshots, default=str, indent=2))
                    if execution is not None and not execution.done():
                        execution.cancel()
                        await asyncio.gather(execution, return_exceptions=True)
                    await stager.join_writers()
                    residuals = [ref for ref, _ in await driver.list_managed(node_id)]

                    async def settle(ref):
                        try:
                            _, stdout, stderr = await driver.command("logs", ref, timeout=10)
                            output = stdout + stderr
                            for private_value in (db.host_url, db.runner_url, db.password, credential["token"], private["models"][0]["api_key"]):
                                if private_value:
                                    output = output.replace(private_value, "[private redacted]")
                            (evidence / (ref + ".log")).write_text(output)
                        finally:
                            await driver.stop(ref)
                            await driver.checked("rm", "-f", ref)

                    refs = sorted(set(owned + residuals))
                    cleanup = await asyncio.gather(*(settle(ref) for ref in refs), return_exceptions=True)
                    remaining = await driver.list_managed(node_id)
                    (evidence / "container-cleanup.json").write_text(json.dumps({"owned_refs": refs, "errors": [type(error).__name__ for error in cleanup if isinstance(error, BaseException)], "remaining": remaining}, default=str, indent=2))
                    assert not any(isinstance(error, BaseException) for error in cleanup)
                    assert not remaining


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc05_shared_turn_and_reserved_capacity(tmp_path, monkeypatch):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from deerflow_ecs_fleet.admission_policy import SharedAdmissionPolicy
    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from deerflow_ecs_fleet.persistence.models import JobRow, NodeRow, SchedulingRow
    from deerflow_extension_api import ExtensionRuntimeDeps

    from app.fleet.admission import agent_candidates
    from app.fleet.ownership import install_fleet_ownership
    from app.fleet.scheduler_tickets import FleetSchedulerTickets
    from app.gateway.services import start_run
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
    from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow
    from deerflow.runtime import RunManager
    from deerflow.runtime.execution.mutation_context import OwnershipRejected

    from .c12_integration_fixture import owned_database
    from .test_b02_fleet_foundation import service_class
    from .test_c02_remote_agent_admission import admission, backend, body, request

    evidence = Path(os.environ["BC05_EVIDENCE_DIR"])
    async with owned_database(evidence) as db:
        fixture = admission.__wrapped__((db.engine, db.session_factory, db.schema), tmp_path)
        _, sf, _, user = await anext(fixture)
        runtime = None
        try:
            injected = backend()
            wire = injected.config.model_dump(mode="json")
            wire.update(nas_root=str(tmp_path), nas_identity="fleet-test", continuations_enabled=True, scheduling_mode="reserved")
            worker = WorkerCompatibility(runtime_digest=wire["profiles"]["remote"]["runtime_digest"], skill_snapshot={"entries": []}, plugin_snapshot={"entries": []}, workspace_contract_version=1)
            wire["agent_bindings"] = {
                "remote": {
                    "allowed_user_ids": [user.id],
                    "model_name": "model-1",
                    "model_version": "v1",
                    "compatibility": worker.model_dump(mode="json"),
                    "secret_refs": [{"name": "MODEL_API_KEY", "reference_id": "opaque-model-key"}],
                    "continuation_budget": 2,
                }
            }
            cfg = FleetConfig.model_validate(wire)
            injected.config, injected.continuation_budget = cfg, 2
            runtime = service_class()(cfg)
            await runtime.start(ExtensionRuntimeDeps(session_factory=sf))
            app = FastAPI()
            app.state.extensions = SimpleNamespace(services=(("fleet", runtime),))
            app.state.run_store = RunRepository(sf)
            app.state.run_manager = RunManager(store=app.state.run_store)
            install_fleet_ownership(app, sf)
            async with db.engine.begin() as conn:
                await conn.run_sync(lambda sync: ScheduledTaskRow.__table__.create(sync))
                await conn.run_sync(lambda sync: ScheduledTaskRunRow.__table__.create(sync))

            sessions = {}

            async def node(name, cpu, memory, profiles, kinds="mixed"):
                await runtime.nodes.register(node_id=name, name=name, cpu_millis=cpu, memory_mib=memory, agent_limit=10, profile_allowlist=profiles)
                opened = await runtime.nodes.open_session(name, protocol_version=1)
                sessions[name] = opened["node_session_id"]
                await runtime.nodes.heartbeat(name, node_session_id=sessions[name], protocol_version=1)
                await runtime.nodes.advertise(name, node_session_id=sessions[name], kind=kinds, compatibility=worker.model_dump(mode="json") if kinds != "job" else None)

            jobs, agents, sequence = [], [], []
            first_agent = None

            async def arrivals(label):
                job = "job-" + label
                async with sf.begin() as session:
                    session.add(
                        JobRow(
                            id=job,
                            user_id=user.id,
                            thread_id="batch-" + label,
                            tracking_task_id="tracking-" + label,
                            idempotency_key=label,
                            spec={"task_name": label, "profile": "batch-standard", "argv": ["true"]},
                            state="queued",
                            queued_at=datetime.now(UTC),
                            queue_deadline=datetime.now(UTC) + timedelta(minutes=10),
                        )
                    )
                record = await start_run(body(), "agent-" + label, request(app.state.run_manager, user), execution_backend=injected)
                jobs.append(job)
                agents.append(record.run_id)
                return job, record.run_id

            # Every wave leaves both categories queued while new real eligible
            # work arrives. Actual host/scheduler claims commit the durable turn.
            await node("shared", 10000, 20480, ["remote", "batch-standard"])
            await arrivals("oldest")
            await arrivals("second")
            for wave in range(4):
                await arrivals("arrival-" + str(wave))
                restarted = __import__("deerflow_ecs_fleet.scheduler", fromlist=["FleetScheduler"]).FleetScheduler(sf, cfg)
                restarted.agent_candidates = agent_candidates
                b = await asyncio.gather(*(restarted.claim_job("shared", node_session_id=sessions["shared"]) for _ in range(2)))
                winner = [claim for claim in b if claim is not None]
                assert len(winner) == 1 and winner[0].job_id == jobs[wave]
                sequence.append("job")
                assert await restarted.claim_job("shared", node_session_id=sessions["shared"]) is None
                c = await asyncio.gather(*(app.state.fleet_ownership.claim_agent("shared", node_session_id=sessions["shared"], worker=worker) for _ in range(2)))
                winner = [claim for claim in c if claim is not None]
                assert len(winner) == 1 and winner[0].run_id == agents[wave]
                sequence.append("agent")
                first_agent = first_agent or winner[0]
                async with sf() as session:
                    turn = await session.get(SchedulingRow, "shared")
                    assert turn.next_kind == "job"
                    charges = (await session.execute(text("SELECT sum(cpu_millis),sum(memory_mib),sum(agent_units) FROM fleet_reservations WHERE state!='released'"))).one()
                    assert tuple(charges) == ((wave + 1) * 2000, (wave + 1) * 4096, wave + 1)
            assert sequence == ["job", "agent"] * 4

            # The original charged pool remains intact. Isolate subsequent
            # projections by disabling its node, never fabricating a STOP.
            async with sf.begin() as session:
                shared = await session.get(NodeRow, "shared", with_for_update=True)
                shared.admin_state = "disabled"
                await session.execute(text("UPDATE fleet_jobs SET cancel_requested_at=clock_timestamp() WHERE state='queued'"))
                await session.execute(text("UPDATE fleet_agent_tasks SET cancel_requested_at=clock_timestamp() WHERE state='queued'"))
            await node("quota", 2000, 4096, ["remote", "batch-standard"])
            quota_job, quota_run = await arrivals("quota")
            assert (await runtime.scheduler.claim_job("quota", node_session_id=sessions["quota"])).job_id == quota_job
            # B already uses the protected quota; C may use the other slot.
            assert (await app.state.fleet_ownership.claim_agent("quota", node_session_id=sessions["quota"], worker=worker)).run_id == quota_run
            await arrivals("quota-next")
            assert await app.state.fleet_ownership.claim_agent("quota", node_session_id=sessions["quota"], worker=worker) is None
            async with sf.begin() as session:
                (await session.get(NodeRow, "quota", with_for_update=True)).admin_state = "disabled"
                await session.execute(text("UPDATE fleet_jobs SET cancel_requested_at=clock_timestamp() WHERE state='queued'"))
                await session.execute(text("UPDATE fleet_agent_tasks SET cancel_requested_at=clock_timestamp() WHERE state='queued'"))
            await node("c-only", 1000, 2048, ["remote"], "agent")
            await node("b-cpu", 1500, 1024, ["batch-standard"], "job")
            await node("b-memory", 500, 4096, ["batch-standard"], "job")
            policy = SharedAdmissionPolicy(cfg, agent_candidates=agent_candidates)
            async with sf.begin() as session:
                window = await policy.lock(session)
                target = next(row for row in window.nodes if row.id == "c-only")
                assert not window.capacity(target, cfg.profiles["remote"])
                # A standard B profile must fit one real node, not totals.
                fitting = next(row for row in window.nodes if row.id == "b-cpu")
                fitting.memory_mib = 2048
                assert window.capacity(target, cfg.profiles["remote"])
                await session.flush()
            # Reconstructing serial policy cannot ignore charged disabled or
            # unknown executions elsewhere in the pool.
            async with sf.begin() as session:
                await session.execute(text("UPDATE fleet_attempts SET state='unknown' WHERE kind='job'"))
                serial = await SharedAdmissionPolicy(cfg.model_copy(update={"scheduling_mode": "serial"}), agent_candidates=agent_candidates).lock(session)
                assert not serial.capacity(next(row for row in serial.nodes if row.id == "c-only"), cfg.profiles["remote"])

            # Real scheduled parent/occurrence ownership precedes policy/nodes.
            now = datetime.now(UTC)
            task = ScheduledTaskRow(
                id="schedule",
                user_id=user.id,
                thread_id="scheduled-thread",
                context_mode="reuse_thread",
                title="boundary",
                prompt="hello",
                schedule_type="once",
                schedule_spec={},
                timezone="UTC",
                execution={"preference": "remote", "profile": "remote"},
                status="enabled",
            )
            occurrence = ScheduledTaskRunRow(id="occurrence", task_id=task.id, thread_id=task.thread_id, scheduled_for=now, trigger="manual", status="launching", lease_owner="original-scheduler", lease_expires_at=now + timedelta(minutes=1))
            async with sf.begin() as session:
                session.add_all([task, occurrence])
            tickets = FleetSchedulerTickets(sf, cfg)
            async with sf.begin() as session:
                locked_task = await session.get(ScheduledTaskRow, task.id, with_for_update=True)
                locked_occurrence = await session.get(ScheduledTaskRunRow, occurrence.id, with_for_update=True)
                (await session.get(SchedulingRow, "shared", with_for_update=True)).next_kind = "agent"
                ticket = await tickets.reserve(session, task=locked_task, occurrence=locked_occurrence, lease_owner=occurrence.lease_owner, lease_expires_at=occurrence.lease_expires_at)
                assert isinstance(ticket, str)
            async with sf.begin() as session:
                locked_task = await session.get(ScheduledTaskRow, task.id, with_for_update=True)
                locked_occurrence = await session.get(ScheduledTaskRunRow, occurrence.id, with_for_update=True)
                assert await tickets.reserve(session, task=locked_task, occurrence=locked_occurrence, lease_owner=occurrence.lease_owner, lease_expires_at=occurrence.lease_expires_at) is False
                assert (await session.get(SchedulingRow, "shared")).next_kind == "job"
                assert await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE ticket_id=:id"), {"id": ticket}) == 1

            admitted = await start_run(body(), occurrence.thread_id, request(app.state.run_manager, user), execution_backend=injected, idempotency_key="scheduled-task:" + occurrence.id)
            stored = await app.state.run_store.get(admitted.run_id, user_id=user.id)
            async with sf.begin() as session:
                await session.get(ScheduledTaskRow, task.id, with_for_update=True)
                locked_occurrence = await session.get(ScheduledTaskRunRow, occurrence.id, with_for_update=True)
                await tickets.consume(session, ticket, locked_occurrence, stored, "remote")
                original_reservation = await session.scalar(text("SELECT id FROM fleet_reservations WHERE ticket_id=:id"), {"id": ticket})
            ticket_claim = await app.state.fleet_ownership.claim_agent("c-only", node_session_id=sessions["c-only"], worker=worker)
            assert ticket_claim is not None and ticket_claim.run_id == admitted.run_id
            async with sf() as session:
                assert (await session.get(SchedulingRow, "shared")).next_kind == "job"
                transferred = (await session.execute(text("SELECT id,attempt_id FROM fleet_reservations WHERE ticket_id=:id"), {"id": ticket})).one()
                assert tuple(transferred) == (original_reservation, ticket_claim.attempt_id)
                assert await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE ticket_id=:id"), {"id": ticket}) == 1

            # Main7 observed real read timeout retry. A never-authorized native
            # claim has no write authority: the original SQL guard must reject.
            import hashlib

            from deerflow_ecs_fleet.launch_spec import LaunchSpec
            from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity

            from app.fleet.agent_control import OriginalAgentCancellation
            from app.fleet.mutation import FleetMutationCapability

            identity = ExecutionIdentity(
                node_id="shared",
                node_session_id=sessions["shared"],
                agent_task_id=first_agent.agent_task_id,
                generation=1,
                attempt_id=first_agent.attempt_id,
                owner_worker_id=first_agent.owner_worker_id,
                token_stamp=hashlib.sha256(first_agent.token.encode()).hexdigest(),
            )
            capability = FleetMutationCapability(identity, LaunchSpec.model_validate(first_agent.launch_spec))
            observer = OriginalAgentCancellation(sf, capability, None, None, None)
            with pytest.raises(OwnershipRejected, match="Checkpoint ownership fence"):
                await observer.observe()
            (evidence / "boundary-observations.json").write_text(
                json.dumps(
                    {
                        "real_claim_sequence": sequence,
                        "fifo_jobs": jobs[:4],
                        "fifo_agents": agents[:4],
                        "active_b_quota": True,
                        "fragmented_b_denied": True,
                        "single_real_b_fit": True,
                        "serial_charged_unknown_denied": True,
                        "held_ticket": ticket,
                        "duplicate_ticket_denied": True,
                        "inactive_sql_authority_rejected": True,
                        "ticket_reservation_transferred_once": True,
                        "ticket_claim_turn_unchanged": True,
                    },
                    indent=2,
                )
            )
        finally:
            if runtime is not None:
                await runtime.stop()
            await fixture.aclose()
