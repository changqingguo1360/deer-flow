"""Causal host observations around original installed publication and recovery."""

import hashlib
import json
import time

import httpx
from sqlalchemy import text


class LostPreparedReply(httpx.AsyncBaseTransport):
    def __init__(self, evidence):
        self.transport = httpx.AsyncHTTPTransport()
        self.observations = []
        self.evidence = evidence

    async def handle_async_request(self, request):
        response = await self.transport.handle_async_request(request)
        if request.url.path.endswith("/workspace/prepared") and response.status_code == 200 and not self.observations:
            raw = await response.aread()
            await response.aclose()
            self.observations.append({"actual_status": response.status_code, "response_sha256": hashlib.sha256(raw).hexdigest(), "observed_monotonic": time.monotonic()})
            (self.evidence / "actual-committed-prepared-reply-loss.json").write_text(json.dumps(self.observations, indent=2))
            raise httpx.ReadError("Actual committed prepared response dropped", request=request)
        return response

    async def aclose(self):
        await self.transport.aclose()


async def kill_before_second_seal(*, driver, grant, request, db, evidence, seals):
    from deerflow_ecs_fleet.worker.workspace_publication import WorkspacePublicationStopped

    async with db.engine.connect() as conn:
        accepted = await conn.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE run_id=:run"), {"run": grant["run_id"]})
    if accepted != 1 or request["identity"]["kind"] != "partial" or len(seals) != 1:
        return
    ref = grant["process_ref"]
    before = await driver.inspect(ref)
    assert before["State"]["Running"]
    await driver.checked("kill", "--signal", "KILL", ref)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        after = await driver.inspect(ref)
        if not after["State"]["Running"]:
            break
    else:
        raise AssertionError("Owned original Docker KILL did not physically stop")
    assert after["Id"] == before["Id"] and after["State"]["ExitCode"] == 137
    (evidence / "actual-preseal-kill.json").write_text(
        json.dumps(
            {
                "container_id": after["Id"],
                "image": after["Image"],
                "State": after["State"],
                "request_id": request["identity"]["request_id"],
                "checkpoint_id": request["identity"]["checkpoint_id"],
                "seals_before_kill": len(seals),
                "prior_accepted_point_count": accepted,
            },
            indent=2,
        )
    )
    raise WorkspacePublicationStopped("Actual original container KILL before second seal")


async def observe_restarts_after_kill(*, app, db, driver, daemon, client, http_client, credential, actual, record, result, rows, evidence):
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon, RecoveryRequired

    from app.fleet.ownership import install_fleet_ownership

    journal = daemon.journal.records()[0]
    claim = journal["claim"]
    ref = "fleet-" + claim["attempt_id"]
    physical = await driver.inspect(ref)
    assert not physical["State"]["Running"] and physical["State"]["ExitCode"] == 137
    assert result["state"] == "recovery_required" and not result["report_pending"]

    async def state():
        async with db.engine.connect() as conn:
            return dict(
                (
                    await conn.execute(
                        text(
                            "SELECT r.status AS core_status,t.state AS task_state,p.state AS placement_state,t.accepted_workspace_point_id,p.final_workspace_point_id "
                            "FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id WHERE r.run_id=:run"
                        ),
                        {"run": record.run_id},
                    )
                )
                .mappings()
                .one()
            )

    before = await state()
    assert before["task_state"] == before["placement_state"] == "recovery_required"
    assert before["accepted_workspace_point_id"] and before["final_workspace_point_id"] is None
    starts_before = len(driver.ready)
    _, logs_before, _ = await driver.command("logs", ref)
    # Logical Gateway ownership/workspace reconstruction over original PG/NAS.
    # This is explicitly not an OS Gateway process restart.
    original_service = app.state.fleet_workspaces
    install_fleet_ownership(app, db.session_factory)
    assert app.state.fleet_workspaces is not original_service
    assert await state() == before
    new_client = NodeClient(gateway_url=str(http_client.base_url).rstrip("/"), credential=credential.token, claim_kind="agent", compatibility=actual.model_dump(mode="json"), http_client=http_client)
    restarted = NodeDaemon(client=new_client, containers=driver, state_dir=daemon.journal.root)
    try:
        with __import__("pytest").raises(RecoveryRequired, match="^Node retains unresolved execution; operator reconciliation required$"):
            await restarted.bootstrap()  # residual stop only; no claim or execute loop
        assert restarted._ready is False
        assert new_client.session_id != client.session_id
        with __import__("pytest").raises(httpx.HTTPStatusError) as rejected:
            await new_client.attempt(claim, "renew")
        assert rejected.value.response.status_code == 409
        after = await state()
        _, logs_after, _ = await driver.command("logs", ref)
        tools_before = sum(len(row["calls"]) for row in rows if row["event"] == "original-stock-provider-returned-calls")
        rows_after = []
        for line in logs_after.splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict) and "event" in value:
                rows_after.append(value)
        tools_after = sum(len(row["calls"]) for row in rows_after if row["event"] == "original-stock-provider-returned-calls")
        assert logs_after == logs_before and tools_after == tools_before and len(driver.ready) == starts_before
        assert after == before
        async with db.engine.connect() as conn:
            point = dict((await conn.execute(text("SELECT id,checkpoint_id,manifest_id,kind FROM fleet_workspace_points WHERE id=:id"), {"id": before["accepted_workspace_point_id"]})).mappings().one())
        from app.gateway.internal_auth import create_internal_auth_headers
        from app.gateway.routers import fleet_artifacts

        app.include_router(fleet_artifacts.router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=create_internal_auth_headers(owner_user_id=journal["grant"]["launch_spec"]["user_id"])) as owner_http:
            metadata_reply = await owner_http.get(f"/api/threads/{record.thread_id}/fleet/manifests/{point['manifest_id']}")
        assert metadata_reply.status_code == 200
        metadata = metadata_reply.json()
        assert metadata["point_id"] == point["id"] and metadata["checkpoint_id"] == point["checkpoint_id"] and metadata["partial"] is True
        (evidence / "actual-restart-recovery-proof.json").write_text(
            json.dumps(
                {
                    "gateway_restart_scope": "original ownership/workspace service reconstruction over same PG/NAS; no OS process restart",
                    "before": before,
                    "after": after,
                    "old_attempt_new_session_status": rejected.value.response.status_code,
                    "point": point,
                    "owner_http_status": metadata_reply.status_code,
                    "owner_metadata": metadata,
                    "published_partial_marked": metadata["partial"] and metadata["checkpoint_id"] == point["checkpoint_id"] and point["kind"] == "partial",
                    "actual_starts_before": starts_before,
                    "actual_starts_after": len(driver.ready),
                    "tool_receipts_before": tools_before,
                    "tool_receipts_after": tools_after,
                    "automatic_replay_count": (len(driver.ready) - starts_before) + (tools_after - tools_before),
                    "physical_stop_result": result,
                },
                indent=2,
            )
        )
    finally:
        await new_client.close()


async def observe_conflicting_claims(*, client, http_client, credential, daemon, request, evidence):
    import asyncio

    journal = daemon.journal.records()[0]
    claim = journal["claim"]
    body = {"node_session_id": client.session_id, "token": claim["token"], "request_id": request["identity"]["request_id"], "request_digest": request["request_digest"], "barrier_epoch": request["barrier_epoch"], "nonce": request["nonce"]}
    conflicting = dict(body, nonce="f" * 64 if body["nonce"] != "f" * 64 else "e" * 64)
    path = "api/fleet/node/attempts/" + claim["attempt_id"] + "/workspace/claim"
    responses = await asyncio.gather(*(http_client.post(path, headers={"Authorization": "Bearer " + credential.token}, json=value) for value in (body, conflicting)))
    statuses = [response.status_code for response in responses]
    assert statuses == [200, 409]
    (evidence / "actual-concurrent-claim-conflict.json").write_text(
        json.dumps({"statuses": statuses, "request_id": request["identity"]["request_id"], "checkpoint_id": request["identity"]["checkpoint_id"], "request_digest": request["request_digest"]}, indent=2)
    )


async def observe_failed_stock(*, app, db, record, result, rows, observation, evidence):
    assert not observation["State"]["Running"] and observation["State"]["ExitCode"] == 0
    assert result["state"] == "failed" and not result["report_pending"]
    failed = next(row for row in rows if row["event"] == "original-stock-observed-actual-tool-failure")
    assert failed["failed_tool_ids"]
    final = next(row for row in rows if row["event"] == "original-stock-final-prepared-gate-closed")
    committed = next(row for row in rows if row["event"] == "original-stock-terminal-committed-finishing")
    assert final["kind"] == "final" and final["desired_core_status"] == committed["core_status"] == "error"
    assert final["budget_started"] and 0 < final["remaining_seconds"] <= 120
    cleanup = next(row for row in rows if row["event"] == "original-stock-final-cleanup-deadline-preserved")
    assert cleanup["deadline_before"] == cleanup["deadline_after"]
    async with db.engine.connect() as conn:
        state = dict(
            (
                await conn.execute(
                    text(
                        "SELECT r.status AS core_status,t.state AS task_state,p.state AS placement_state,t.accepted_workspace_point_id,p.final_workspace_point_id,s.core_status AS seal_core_status "
                        "FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id "
                        "JOIN fleet_stream_seals s ON s.run_id=r.run_id WHERE r.run_id=:run"
                    ),
                    {"run": record.run_id},
                )
            )
            .mappings()
            .one()
        )
    assert state["core_status"] == state["seal_core_status"] == "error" and state["task_state"] == state["placement_state"] == "failed"
    assert state["accepted_workspace_point_id"] == state["final_workspace_point_id"] == committed["point_id"]
    (evidence / "actual-failed-stock-proof.json").write_text(
        json.dumps({"actual_failed_tool": failed, "prepared_final": final, "committed_terminal": committed, "sql_state": state, "physical_stop_result": result, "DockerState": observation["State"], "runner_receipts": rows}, indent=2)
    )


async def observe_first_partial(*, app, db, driver, ref, record, rows, evidence, kill):
    """Read accepted bytes while the same original runner awaits our release."""
    from app.gateway.internal_auth import create_internal_auth_headers

    barrier = next(row for row in rows if row["event"] == "original-stock-first-partial-barrier")
    assert not any(call["id"] == "c08-second-write" for row in rows for call in row.get("calls", []))
    async with db.engine.connect() as conn:
        points = [dict(row) for row in (await conn.execute(text("SELECT id,checkpoint_id,manifest_id,kind FROM fleet_workspace_points WHERE run_id=:run ORDER BY accepted_at"), {"run": record.run_id})).mappings()]
    assert len(points) == 1 and points[0]["kind"] == "partial"
    point = points[0]
    assert point["checkpoint_id"] == barrier["checkpoint_id"] and point["manifest_id"] == barrier["manifest_id"]
    url = f"/api/threads/{record.thread_id}/artifacts/mnt/user-data/outputs/parent.txt"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=create_internal_auth_headers(owner_user_id=record.user_id)) as http:
        reply = await http.get(url, params={"workspace_point_id": point["id"]})
    assert reply.status_code == 200 and reply.content and b"accepted-second-turn" not in reply.content
    before = await driver.inspect(ref)
    assert before["State"]["Running"]
    proof = {"point": point, "url": url, "http_status": reply.status_code, "body_hex": reply.content.hex(), "rows_before_release": rows, "physical_before": before["State"], "container_id": before["Id"]}
    (evidence / "actual-first-partial-read.json").write_text(json.dumps(proof, indent=2))
    if kill:
        await driver.checked("kill", "--signal", "KILL", ref)
        import asyncio

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            after = await driver.inspect(ref)
            if not after["State"]["Running"]:
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("Owned first-partial KILL did not physically stop")
        assert after["Id"] == before["Id"] and not after["State"]["Running"] and after["State"]["ExitCode"] == 137
        (evidence / "actual-first-partial-kill.json").write_text(json.dumps({"container_id": after["Id"], "State": after["State"], "point": point}, indent=2))
    else:
        await driver.checked("exec", ref, "/usr/local/bin/python", "-c", "from pathlib import Path; Path('/tmp/c08-first-partial-release').touch()")


async def verify_first_partial_immutable(*, app, record, evidence):
    from app.gateway.internal_auth import create_internal_auth_headers

    proof = json.loads((evidence / "actual-first-partial-read.json").read_text())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=create_internal_auth_headers(owner_user_id=record.user_id)) as http:
        repeated = await http.get(proof["url"], params={"workspace_point_id": proof["point"]["id"]})
        current = await http.get(proof["url"])
    assert repeated.status_code == current.status_code == 200
    assert repeated.content == bytes.fromhex(proof["body_hex"])
    assert current.content == repeated.content + b"accepted-second-turn\n"
    (evidence / "actual-first-partial-immutable.json").write_text(
        json.dumps({"old_http_status": repeated.status_code, "old_body_hex": repeated.content.hex(), "current_http_status": current.status_code, "current_body_hex": current.content.hex(), "point": proof["point"]}, indent=2)
    )
