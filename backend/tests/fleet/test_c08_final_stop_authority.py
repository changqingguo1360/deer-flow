"""Native PG immutable outcome authority; physical proof here is not Linux census."""

import pytest
from sqlalchemy import text

from .test_c08_terminal_pair import admission as admission
from .test_c08_terminal_pair import checkpoint_owner as checkpoint_owner
from .test_c08_terminal_pair import owner_environment as owner_environment
from .test_c08_terminal_pair import participant
from .test_c08_terminal_pair import prepared_pair as prepared_pair

OUTCOMES = [
    {"core": "success", "task": "succeeded", "placement": "succeeded"},
    {"kind": "paused", "core": "interrupted", "task": "input_required", "placement": "cancelled"},
    {"core": "error", "task": "failed", "placement": "failed"},
    {"core": "timeout", "task": "timed_out", "placement": "timed_out"},
]


async def accept(pair):
    from deerflow.persistence.run.sql import RunRepository

    with pair.scope():
        await RunRepository(pair.sf, mutation_capability=pair.capability, terminal_participant=participant(pair)).update_status(pair.identity.run_id, pair.identity.desired_core_status)


async def point_bytes(pair):
    async with pair.sf() as session:
        return await session.scalar(text("SELECT row_to_json(w)::text FROM fleet_workspace_points w"))


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", OUTCOMES, indirect=True)
@pytest.mark.parametrize("reason", ["exit", "cancelled", "lease_lost", "execution_deadline"])
async def test_exact_committed_outcome_survives_transport_stop_reason(prepared_pair, reason):
    p = prepared_pair
    await accept(p)
    immutable = await point_bytes(p)
    auth = dict(node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token)
    ownership = p.item.env[4].state.fleet_ownership
    result = await ownership.stopped(reason=reason, exit_code=137, process_ref=p.identity.process_ref, physical_stopped=True, **auth)
    assert result == {"state": p.identity.desired_placement_status, "stopped": True}
    async with p.sf() as session:
        assert await session.scalar(text("SELECT status FROM runs")) == p.identity.desired_core_status
        assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == p.identity.desired_task_status
        assert await session.scalar(text("SELECT state FROM fleet_run_placements")) == p.identity.desired_placement_status
        assert await session.scalar(text("SELECT state FROM fleet_attempts")) == ("expired" if p.identity.desired_placement_status == "timed_out" else p.identity.desired_placement_status)
        pointers = (await session.execute(text("SELECT t.accepted_workspace_point_id,p.final_workspace_point_id FROM fleet_agent_tasks t JOIN fleet_run_placements p ON p.agent_task_id=t.id"))).one()
        assert tuple(pointers) == (p.identity.request_id, p.identity.request_id)
        assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "released"
        assert await session.scalar(text("SELECT outcome->>'stop_reason' FROM fleet_attempts")) == reason
        assert await session.scalar(text("SELECT core_status FROM fleet_stream_seals")) == p.identity.desired_core_status
        assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 1
    assert await point_bytes(p) == immutable
    assert await ownership.stopped(reason="cancelled", exit_code=0, process_ref=p.identity.process_ref, physical_stopped=True, **auth) == result
    async with p.sf() as session:
        assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 1
        assert await session.scalar(text("SELECT outcome->>'stop_reason' FROM fleet_attempts")) == reason
    assert await point_bytes(p) == immutable


@pytest.mark.asyncio
@pytest.mark.parametrize("lose_lease", [False, True])
async def test_original_daemon_postpair_idle_or_real_lease_loss_keeps_immutable_outcome(prepared_pair, tmp_path, lose_lease):
    """Original daemon/TCP/PG with a held native child, not installed Runner cleanup."""
    import asyncio
    import subprocess
    import sys

    import httpx
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication

    from .c04_integration_fixture import node_server

    p = prepared_pair
    child = None
    immutable = None
    calls = []
    native_release = tmp_path / "postpair-native-release"
    claim = {"kind": "agent", "attempt_id": p.identity.attempt_id, "token": p.item.accepted.token}

    class Client(NodeClient):
        async def attempt(self, value, operation, **fields):
            try:
                result = await super().attempt(value, operation, **fields)
            except httpx.HTTPStatusError as error:
                calls.append((operation, error.response.status_code))
                raise
            if operation == "workspace/poll":
                calls.append((operation, 200))
                native_release.touch()
            if operation == "stopped":
                assert child.poll() is not None
                calls.append((operation, fields["reason"]))
            return result

    class Containers:
        def bind_claim(self, value):
            assert value is claim

        async def launch(self, grant, **kwargs):
            nonlocal child, immutable
            script = (
                "import time; time.sleep(60)"
                if lose_lease
                else "import sys,time; from pathlib import Path; deadline=time.monotonic()+3; marker=Path(sys.argv[1]);\nwhile not marker.exists():\n if time.monotonic()>deadline: raise SystemExit(2)\n time.sleep(0.005)"
            )
            child = subprocess.Popen([sys.executable, "-c", script, str(native_release)])
            await accept(p)
            immutable = await point_bytes(p)
            if lose_lease:
                async with p.sf.begin() as session:
                    await session.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
                    await session.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
            return {"State": {"Status": "running", "Running": True}}

        async def inspect(self, ref):
            assert ref == p.identity.process_ref
            return {"State": {"Running": child is not None and child.poll() is None, "ExitCode": (128 - child.returncode if child.returncode is not None and child.returncode < 0 else child.returncode) if child is not None else 137}}

        async def stop(self, ref):
            if child is not None and child.poll() is None:
                child.terminate()
                await asyncio.to_thread(child.wait, 3)
            return True

    async def prepare(value, grant):
        output = tmp_path / "native-output"
        output.mkdir()
        return output

    async with node_server(p.item.env[4]) as url, httpx.AsyncClient(base_url=url) as http:
        client = Client(gateway_url=url, credential=p.item.env[7].token, http_client=http)
        client.node_id, client.session_id = p.identity.node_id, p.identity.node_session_id
        containers = Containers()
        daemon = NodeDaemon(client=client, containers=containers, state_dir=tmp_path / "private-daemon", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.01, poll_seconds=0.005)
        daemon.workspace_publications = AgentWorkspacePublication(client=client, containers=containers, nas=None, journal=daemon.journal)
        daemon._ready = True
        try:
            result = await asyncio.wait_for(daemon.execute(claim), 3)
            expected_reason = "lease_lost" if lose_lease else "exit"
            assert ("workspace/poll", 409 if lose_lease else 200) in calls
            assert ("stopped", expected_reason) in calls
            assert result["stop_reason"] == expected_reason
            assert child.returncode == (-15 if lose_lease else 0)
            assert result["state"] == "succeeded"
            assert not result["report_pending"]
            assert await point_bytes(p) == immutable
            async with p.sf() as session:
                assert await session.scalar(text("SELECT state FROM fleet_reservations")) == "released"
                assert await session.scalar(text("SELECT core_status FROM fleet_stream_seals")) == "success"
            import hashlib
            import json
            from pathlib import Path
            from uuid import uuid4

            evidence = Path(__file__).resolve().parents[3] / ".local/fleet-evidence/c08-task4"
            (evidence / ("final-stop-daemon-observation-" + uuid4().hex + ".json")).write_text(
                json.dumps(
                    {
                        "scope": "original NodeDaemon/NodeClient/AgentWorkspacePublication with authenticated loopback TCP and original PG; controlled held native child, not installed AgentRunner/Docker",
                        "calls": calls,
                        "native_pid": child.pid,
                        "native_returncode": child.returncode,
                        "process_joined_before_stopped_http": True,
                        "result": result,
                        "point_sha256": hashlib.sha256(immutable.encode()).hexdigest(),
                        "immutable_point_unchanged": True,
                        "reservation": "released",
                        "core_seal": "success",
                    },
                    indent=2,
                )
            )
        finally:
            await containers.stop(p.identity.process_ref)


async def durable_rows(pair):
    async with pair.sf() as session:
        return {
            table: tuple((await session.execute(text("SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"))).scalars())
            for table in ("runs", "fleet_agent_tasks", "fleet_run_placements", "fleet_attempts", "fleet_reservations", "fleet_workspace_points", "fleet_stream_seals")
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["proof", "process", "token", "session"])
async def test_transport_reason_never_bypasses_original_stop_identity(prepared_pair, bad):
    p = prepared_pair
    await accept(p)
    auth = dict(node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token)
    proof, process = True, p.identity.process_ref
    if bad == "proof":
        proof = False
    elif bad == "process":
        process = "fleet-wrong"
    elif bad == "token":
        auth["token"] = "wrong-token"
    else:
        auth["node_session_id"] = "wrong-session"
    before = await durable_rows(p)
    with pytest.raises(PermissionError if bad == "token" else ValueError):
        await p.item.env[4].state.fleet_ownership.stopped(reason="lease_lost", exit_code=137, process_ref=process, physical_stopped=proof, **auth)
    assert await durable_rows(p) == before


@pytest.mark.asyncio
async def test_actual_new_node_session_still_cannot_report_old_attempt(prepared_pair):
    """Strict ordinary STOP; dedicated restart proof cannot change original authority."""
    import httpx
    from deerflow_ecs_fleet.worker.client import NodeClient

    p = prepared_pair
    await accept(p)
    immutable = await point_bytes(p)
    app = p.item.env[4]
    runtime = p.item.env[3]
    await runtime.nodes.register(node_id="foreign-stop", name="foreign-stop", cpu_millis=1000, memory_mib=2048)
    foreign = await runtime.credentials.issue("foreign-stop", lifetime_seconds=600)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test/") as http:
        client = NodeClient(gateway_url="http://test", credential=p.item.env[7].token, http_client=http)
        await client.open_session()
        assert client.session_id != p.identity.node_session_id
        before = await durable_rows(p)
        claim = {"kind": "agent", "attempt_id": p.identity.attempt_id, "token": p.item.accepted.token}
        fields = dict(reason="lease_lost", exit_code=137, process_ref=p.identity.process_ref, physical_stopped=True)
        with pytest.raises(httpx.HTTPStatusError) as rejection:
            await client.attempt(claim, "stopped", **fields)
        assert rejection.value.response.status_code == 409
        assert await durable_rows(p) == before
        path = "/api/fleet/node/attempts/" + p.identity.attempt_id + "/reconcile-stopped"
        body = fields | {"node_session_id": client.session_id, "original_node_session_id": p.identity.node_session_id, "token": p.item.accepted.token}
        for changed, credential, expected in (
            ({"token": "wrong"}, p.item.env[7].token, 403),
            ({"original_node_session_id": "wrong"}, p.item.env[7].token, 409),
            ({"physical_stopped": False}, p.item.env[7].token, 409),
            ({"process_ref": "fleet-wrong"}, p.item.env[7].token, 409),
            ({}, foreign.token, 403),
        ):
            denied = await http.post(path, headers={"Authorization": "Bearer " + credential}, json=body | changed)
            assert denied.status_code == expected, denied.text
            assert await durable_rows(p) == before
        result = await client.reconcile_stopped(claim, original_node_session_id=p.identity.node_session_id, **fields)
        assert result == {"state": p.identity.desired_placement_status, "stopped": True}
        assert await point_bytes(p) == immutable
        async with p.sf() as session:
            assert await session.scalar(text("SELECT outcome->>'stop_reason' FROM fleet_attempts")) == "lease_lost"
            assert await session.scalar(text("SELECT core_status FROM fleet_stream_seals")) == p.identity.desired_core_status
            assert await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE state='released' AND released_at IS NOT NULL")) == 1
        stopped = await durable_rows(p)
        assert await client.reconcile_stopped(claim, original_node_session_id=p.identity.node_session_id, **(fields | {"reason": "cancelled", "exit_code": 0})) == result
        assert await durable_rows(p) == stopped


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["final", "partial"], indirect=True)
async def test_prepared_only_never_synthesizes_terminal_pair_on_transport_stop(prepared_pair):
    p = prepared_pair
    await p.item.env[4].state.fleet_ownership.stopped(
        reason="lease_lost", exit_code=137, process_ref=p.identity.process_ref, physical_stopped=True, node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token
    )
    async with p.sf() as session:
        assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == "recovery_required"
        assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points")) == 0
        assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 0


@pytest.mark.asyncio
async def test_accepted_pair_with_changed_current_root_requires_recovery(prepared_pair):
    p = prepared_pair
    await accept(p)
    immutable = await point_bytes(p)
    async with p.sf.begin() as session:
        await session.execute(text("UPDATE checkpoints SET metadata=metadata || '{\"deerflow_execution_run_id\":\"other-run\"}'::jsonb WHERE checkpoint_ns=''"))
    await p.item.env[4].state.fleet_ownership.stopped(
        reason="lease_lost", exit_code=137, process_ref=p.identity.process_ref, physical_stopped=True, node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token
    )
    async with p.sf() as session:
        assert await session.scalar(text("SELECT state FROM fleet_run_placements")) == "recovery_required"
        assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 0
    assert await point_bytes(p) == immutable


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["partial"], indirect=True)
async def test_accepted_stage_never_authorizes_terminal_transport_stop(prepared_pair):
    from types import SimpleNamespace

    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool

    p = prepared_pair
    publisher = FleetWorkspacePublisher(p.sf, p.capability, controller=p.controller, teardown=SimpleNamespace(context=p.capability.context, workspace_writers=p.controller, workspace_sessions=None), session_pool=MCPSessionPool())
    publisher.pool.freeze_scope(publisher.scope_key, barrier_epoch=1)
    with p.scope():
        await publisher.accept_partial(p.identity, p.candidate, barrier_epoch=1)
    immutable = await point_bytes(p)
    await p.item.env[4].state.fleet_ownership.stopped(
        reason="lease_lost", exit_code=137, process_ref=p.identity.process_ref, physical_stopped=True, node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token
    )
    async with p.sf() as session:
        assert await session.scalar(text("SELECT state FROM fleet_run_placements")) == "recovery_required"
        assert await session.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 0
    assert await point_bytes(p) == immutable
