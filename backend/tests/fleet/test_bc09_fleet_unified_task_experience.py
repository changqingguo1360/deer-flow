"""BC09: one native owned lifecycle, reused by DOM and controlled IM."""

import json
import os
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text


def evidence(name, value):
    directory = Path(os.environ["BC09_EVIDENCE_DIR"])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / (name + ".json")).write_text(json.dumps(value, default=str, indent=2))


async def controlled_im(directory, patch, fixture, session_factory):
    """Original manager/bus; only model replies and owned HTTP are controlled."""
    from app.channels.manager import ChannelManager
    from app.channels.message_bus import InboundMessage, InboundMessageType, MessageBus
    from app.channels.store import ChannelStore
    from app.gateway.internal_auth import INTERNAL_AUTH_HEADER_NAME, INTERNAL_OWNER_USER_ID_HEADER_NAME, is_valid_internal_auth_token
    from deerflow.persistence.channel_connections import ChannelConnectionRepository

    bus = MessageBus()
    connections = ChannelConnectionRepository(session_factory)
    manager = ChannelManager(bus, ChannelStore(directory / "channel-store.json"), gateway_url="http://bc09-summary.invalid", connection_repo=connections, require_bound_identity=True)
    outbound, requests = [], []

    async def capture(message):
        outbound.append(message)

    bus.subscribe_outbound(capture)
    owner, thread = fixture["proof"]["owner_id"], fixture["proof"]["thread_id"]
    bound = {provider: await connections.upsert_connection(owner_user_id=owner, provider=provider, external_account_id="platform-only", status="connected") for provider in ("slack", "telegram")}
    fixture["proof"]["connection_ids"] = {provider: row["id"] for provider, row in bound.items()}
    message = InboundMessage(
        channel_name="slack",
        chat_id="bc09-controlled",
        user_id="platform-only",
        text="Collect results",
        connection_id=bound["slack"]["id"],
        owner_user_id=owner,
        thread_ts="bc09-topic",
        metadata={"message_id": "bc09-inbound", "raw_message": "UNACCEPTED-WORKER-LOG"},
    )
    streaming_message = replace(message, channel_name="telegram", connection_id=bound["telegram"]["id"])
    assert await manager._get_bound_identity_rejection(message) is None
    assert await manager._get_bound_identity_rejection(streaming_message) is None
    result = {"messages": [{"type": "human", "content": "Collect results"}, {"type": "ai", "content": "This run has yielded."}]}
    client = SimpleNamespace(runs=SimpleNamespace(wait=AsyncMock(return_value=result)))
    original_client = httpx.AsyncClient

    def summary(request):
        assert request.method == "GET"
        assert request.url.path == "/api/threads/" + thread + "/agent-tasks"
        assert request.url.params["limit"] == "20"
        assert request.headers.get(INTERNAL_OWNER_USER_ID_HEADER_NAME) == owner
        assert is_valid_internal_auth_token(request.headers.get(INTERNAL_AUTH_HEADER_NAME))
        requests.append({"method": request.method, "path": request.url.path, "limit": request.url.params["limit"], "owner_id": owner, "internal_authenticated": True})
        return httpx.Response(200, json=fixture["accepted_waiting"]["body"])

    def http_client(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(summary)
        return original_client(*args, **kwargs)

    with patch.context() as controlled:
        controlled.setattr(httpx, "AsyncClient", http_client)
        controlled.setattr(manager, "_lookup_thread_id", AsyncMock(return_value=thread))
        await manager._handle_chat_on_thread(client, message, thread, storage_user_id=owner)

        async def stream(*args, **kwargs):
            yield SimpleNamespace(event="messages-tuple", data=[{"type": "AIMessageChunk", "id": "bc09-ai", "content": "This run has yielded."}, {}])
            yield SimpleNamespace(event="values", data=result)

        client.runs.stream = stream
        await manager._handle_streaming_chat(client, streaming_message, thread, "lead_agent", {}, {"user_id": owner}, {"role": "user", "content": message.text}, storage_user_id=owner)
        await manager._handle_command(replace(message, text="/status", msg_type=InboundMessageType.COMMAND))
    safe = [
        {"channel": msg.channel_name, "text": msg.text, "is_final": msg.is_final, "thread_id": msg.thread_id, "owner_user_id": msg.owner_user_id, "connection_id": msg.connection_id, "thread_ts": msg.thread_ts, "metadata": msg.metadata}
        for msg in outbound
    ]
    evidence("controlled-im", {"qualification": "Controlled in-memory bus and captured original HTTP, no external messages", "requests": requests, "outbound": safe})
    return requests, outbound


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc09_owned_waiting_goal_summary_and_resume(tmp_path, monkeypatch):
    from .c04_integration_fixture import node_server
    from .c12_integration_fixture import login
    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop
    from .test_bc03_fleet_agent_job_continuations import complete_children
    from .test_bc06_fleet_agent_job_dependencies import _bc06_case, _bc06_http_host

    root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(root / "tests"), str(root), str(root / "packages/ecs-fleet"), os.environ.get("PYTHONPATH", "")]))
    stack, item = await _bc06_case(tmp_path / "main", monkeypatch)
    try:
        parent = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path / "main", monkeypatch, continuation_case="bc09")
        evidence("native-parent", parent)
        assert parent["old_run_status"] == "success" and parent["task_state"] == "waiting_jobs"
        assert parent["stopped_ack"] and parent["reservation_state"] == "released"
        assert parent["prepared_count"] and parent["checkpoint_id"] and parent["workspace_point_id"]
        assert parent["unpaired_tool_calls"] == 0 and parent["model_calls_after_await"] == 0
        assert len(parent["group_members"]) == 2 and not parent["resume_before_stop_ack"]
        app, password = await _bc06_http_host(item, monkeypatch, stack)
        path = "/api/threads/" + item.spec.thread_id + "/agent-tasks"
        fixture = {
            "proof": {
                "owner_id": item.spec.user_id,
                "thread_id": item.spec.thread_id,
                "task_id": item.spec.agent_task_id,
                "original_run_id": item.spec.run_id,
                "job_ids": sorted(parent["group_members"]),
                "original_generation": item.spec.generation,
            }
        }

        async def capture(client, name):
            response = await client.get(path + "?limit=20")
            assert response.status_code == 200, response.text
            fixture[name] = {"status": response.status_code, "body": response.json()}
            evidence("http-flow", fixture)
            return response.json()[0] if response.json() else None

        async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
            await login(client, "bc06-boundary@example.com", password)
            waiting = await capture(client, "waiting")
            assert waiting["state"] == "waiting_jobs" and waiting["run_status"] == "success"
            runtime = item.env[3]
            await runtime.nodes.advertise(item.identity.node_id, node_session_id=item.identity.node_session_id, kind="mixed", compatibility=item.env[8].model_dump(mode="json"))
            completed = await complete_children(item.env[1], runtime.config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id)
            evidence("native-children", completed)
            fixture["proof"]["manifest_ids"] = {row["job_id"]: row["manifest_id"] for row in completed}
            accepted = await capture(client, "accepted_waiting")
            body = {"expected_generation": accepted["generation"], "idempotency_key": str(uuid4())}
            response = await client.post(path + "/" + item.spec.agent_task_id + "/resume", json=body)
            fixture["resume"] = {"status": response.status_code, "body": response.json(), "request": body}
            evidence("http-flow", fixture)
            assert response.status_code == 200, response.text
            resumed = await capture(client, "resumed")
            async with item.engine.connect() as connection:
                task = dict((await connection.execute(text("SELECT id,generation,current_run_id,state,accepted_workspace_point_id FROM fleet_agent_tasks"))).mappings().one())
                payload = await connection.scalar(text("SELECT payload FROM fleet_launch_specs WHERE run_id=:run"), {"run": response.json()["run_id"]})
                jobs = [
                    dict(row)
                    for row in (
                        await connection.execute(
                            text(
                                "SELECT j.id,j.state,j.accepted_manifest_id,a.stopped_at,a.finished_at,r.state reservation_state "
                                "FROM fleet_jobs j JOIN fleet_attempts a ON a.id=j.active_attempt_id "
                                "JOIN fleet_reservations r ON r.attempt_id=a.id ORDER BY j.id"
                            )
                        )
                    ).mappings()
                ]
                counts = dict((await connection.execute(text("SELECT (SELECT count(*) FROM runs) runs,(SELECT count(*) FROM fleet_jobs) jobs,(SELECT count(*) FROM fleet_attempts WHERE kind='agent') agent_attempts"))).mappings().one())
            evidence(
                "resume-sql",
                {
                    "task": task,
                    "source_workspace_point_id": payload["source_workspace_point_id"],
                    "source_workspace_checkpoint_id": payload["source_workspace_checkpoint_id"],
                    "generation": payload["generation"],
                    "result_refs": fixture["proof"]["manifest_ids"],
                    "jobs": jobs,
                    "counts": counts,
                },
            )
            assert task["generation"] == accepted["generation"] + 1 and task["current_run_id"] != item.spec.run_id
            assert task["state"] == "queued" and counts == {"runs": 2, "jobs": 2, "agent_attempts": 1}
            assert payload["source_workspace_point_id"] == parent["workspace_point_id"] and payload["source_workspace_checkpoint_id"] == parent["checkpoint_id"]
            assert payload["generation"] == task["generation"]
            assert all(row["manifest_id"] in json.dumps(payload["input"]) for row in completed)
            assert all(row["state"] == "succeeded" and row["accepted_manifest_id"] and row["stopped_at"] and row["finished_at"] and row["reservation_state"] == "released" for row in jobs)
            body = {"expected_generation": resumed["generation"], "idempotency_key": str(uuid4())}
            response = await client.post(path + "/" + item.spec.agent_task_id + "/cancel", json=body)
            fixture["cancel"] = {"status": response.status_code, "body": response.json(), "request": body}
            assert response.status_code == 200, response.text
            cancelled = await capture(client, "cancelled")
            response = await client.get(path + "/" + item.spec.agent_task_id)
            fixture["cancelled_single"] = {"status": response.status_code, "body": response.json()}
            async with item.engine.connect() as connection:
                cancelled_task = dict((await connection.execute(text("SELECT id,generation,current_run_id,state FROM fleet_agent_tasks"))).mappings().one())
                receipt = dict(
                    (await connection.execute(text("SELECT agent_task_id,user_id,thread_id,operation,source_generation,target_generation,source_run_id,state FROM fleet_task_operation_receipts WHERE operation='cancel'"))).mappings().one()
                )
            evidence("cancel-sql", {"task": cancelled_task, "receipt": receipt})
            assert cancelled_task["state"] == "cancelled" and receipt["state"] == "completed"
            assert cancelled_task["generation"] == receipt["target_generation"] == resumed["generation"] + 1
            assert receipt["source_generation"] == resumed["generation"] and receipt["source_run_id"] == resumed["current_run_id"]
            assert (receipt["user_id"], receipt["thread_id"], receipt["agent_task_id"]) == (item.spec.user_id, item.spec.thread_id, item.spec.agent_task_id)
            evidence("http-flow", fixture)

        requests, outbound = await controlled_im(tmp_path / "main", monkeypatch, fixture, item.env[1])
        evidence("http-flow", fixture)
        # Everything above is the same original lifecycle, captured before RED.
        expected_jobs = set(fixture["proof"]["job_ids"])
        assert {row["job_id"] for row in waiting.get("jobs", [])} == expected_jobs, "Missing original bounded related jobs"
        assert waiting["jobs_truncated"] is False and waiting["runs_truncated"] is False
        assert waiting["runs"] == [{"run_id": item.spec.run_id, "generation": item.spec.generation, "run_status": "success"}]
        assert all(row["accepted_manifest_id"] is None for row in waiting["jobs"])
        assert {row["job_id"]: row["accepted_manifest_id"] for row in accepted["jobs"]} == fixture["proof"]["manifest_ids"]
        assert all(row["parent_run_id"] == item.spec.run_id and row["generation"] == item.spec.generation and row["link_mode"] == "awaited" for row in accepted["jobs"])
        assert {row["run_id"] for row in resumed["runs"]} == {item.spec.run_id, task["current_run_id"]}
        assert resumed["current_run_id"] == task["current_run_id"] and resumed["run_status"] == "pending"
        assert cancelled is not None and cancelled["state"] == "cancelled", "Completed owned cancellation must remain visible"
        assert cancelled["generation"] == cancelled_task["generation"] and cancelled["current_run_id"] == resumed["current_run_id"]
        assert fixture["cancelled_single"]["status"] == 200 and fixture["cancelled_single"]["body"] == cancelled
        assert "untrusted child result" not in json.dumps(fixture)
        assert len(requests) == 3
        finals = [msg for msg in outbound if msg.is_final]
        assert len(finals) == 3
        for msg in finals:
            assert "Waiting for computation" in msg.text
            assert item.spec.agent_task_id in msg.text and item.spec.run_id in msg.text
            assert all(job_id in msg.text for job_id in expected_jobs)
            assert all(row["manifest_id"] in msg.text for row in completed)
            assert msg.owner_user_id == item.spec.user_id and msg.connection_id == fixture["proof"]["connection_ids"][msg.channel_name]
            assert "UNACCEPTED-WORKER-LOG" not in json.dumps(asdict(msg))
            assert "untrusted child result" not in msg.text and "http://bc09-summary.invalid" not in msg.text
        assert any(not msg.is_final for msg in outbound)
        assert all("Waiting for computation" not in msg.text for msg in outbound if not msg.is_final)
    finally:
        await stack.aclose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc09_owner_conflict_and_bounded_summary(tmp_path, monkeypatch):
    """One queued original admission; no extra executor or continuation graph."""
    from sqlalchemy import event

    from app.channels.fleet_summary import UNAVAILABLE, format_fleet_summary, read_fleet_summary
    from app.gateway.auth.models import User
    from app.gateway.auth.password import hash_password_async
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from app.gateway.internal_auth import create_internal_auth_headers

    from .c04_integration_fixture import node_server
    from .c12_integration_fixture import login
    from .test_bc06_fleet_agent_job_dependencies import _bc06_case, _bc06_http_host

    stack, item = await _bc06_case(tmp_path / "boundary", monkeypatch, queued=True)
    try:
        app, password = await _bc06_http_host(item, monkeypatch, stack)
        foreign_id = uuid4()
        await SQLiteUserRepository(item.env[1]).create_user(User(id=foreign_id, email="bc09-foreign@example.com", password_hash=await hash_password_async(password), system_role="user"))
        if await app.state.thread_store.get(item.spec.thread_id, user_id=item.spec.user_id) is None:
            await app.state.thread_store.create(item.spec.thread_id, user_id=item.spec.user_id)
        path = f"/api/threads/{item.spec.thread_id}/agent-tasks"
        task_path = path + "/" + item.spec.agent_task_id
        projection_limits = []
        projection_sql = []

        def observe(_connection, _cursor, statement, parameters, _context, _many):
            sql = statement.lstrip()
            kind = "jobs" if sql.startswith("SELECT fleet_job_links.job_id") else "runs" if sql.startswith("SELECT runs.run_id, fleet_run_placements.generation") else None
            if kind:
                related = kind == "runs" or "accepted_manifest_id" in statement and "fleet_jobs.state" in statement
                projection_sql.append({"projection": kind, "related_summary": related, "statement": statement, "parameters": parameters})
                evidence("boundary-projection-sql", projection_sql)
                if related:
                    projection_limits.append({"projection": kind, "bounded": "LIMIT" in statement, "fetch21": 21 in parameters})

        event.listen(item.engine.sync_engine, "before_cursor_execute", observe)
        async with node_server(app) as url, httpx.AsyncClient(base_url=url) as client:
            await login(client, "bc06-boundary@example.com", password)
            owned = await client.get(path + "?limit=20")
            assert owned.status_code == 200
            initial = owned.json()[0]
            assert initial["task_id"] == item.spec.agent_task_id and initial["state"] == "queued"
            assert initial["jobs"] == [] and len(initial["runs"]) == 1
            assert initial["runs"][0]["run_id"] == item.spec.run_id
            assert not initial["jobs_truncated"] and not initial["runs_truncated"]
            denied = []
            async with httpx.AsyncClient(base_url=url) as foreign:
                await login(foreign, "bc09-foreign@example.com", password)
                for method, endpoint in (("GET", path), ("GET", task_path), ("POST", task_path + "/cancel"), ("POST", task_path + "/resume")):
                    response = await foreign.request(method, endpoint, **({"json": {"expected_generation": initial["generation"], "idempotency_key": str(uuid4())}} if method == "POST" else {}))
                    denied.append(response.status_code)
                    assert response.status_code == 404 and item.spec.agent_task_id not in response.text
            async with item.engine.connect() as connection:
                before = dict((await connection.execute(text("SELECT generation,current_run_id,state FROM fleet_agent_tasks WHERE id=:id"), {"id": item.spec.agent_task_id})).mappings().one())
            stale = await client.post(task_path + "/cancel", json={"expected_generation": initial["generation"] + 1, "idempotency_key": str(uuid4())})
            assert stale.status_code == 409
            async with item.engine.connect() as connection:
                after = dict((await connection.execute(text("SELECT generation,current_run_id,state FROM fleet_agent_tasks WHERE id=:id"), {"id": item.spec.agent_task_id})).mappings().one())
                assert after == before
                assert await connection.scalar(text("SELECT count(*) FROM fleet_task_operation_receipts")) == 0
            body = {"expected_generation": initial["generation"], "idempotency_key": str(uuid4())}
            cancelled = await client.post(task_path + "/cancel", json=body)
            repeated = await client.post(task_path + "/cancel", json=body)
            assert cancelled.status_code == repeated.status_code == 200 and cancelled.json() == repeated.json()
            stale_again = await client.post(task_path + "/cancel", json={**body, "idempotency_key": str(uuid4())})
            assert stale_again.status_code == 409
            final = await client.get(task_path)
            assert final.status_code == 200 and final.json()["state"] == "cancelled"
            async with item.engine.connect() as connection:
                assert await connection.scalar(text("SELECT count(*) FROM fleet_task_operation_receipts")) == 1
            assert {row["projection"] for row in projection_limits} == {"jobs", "runs"}
            assert all(row["bounded"] and row["fetch21"] for row in projection_limits)
            evidence(
                "boundary-http",
                {
                    "qualification": "Original queued admission/auth/routes; no worker spawned; actual related SQL LIMIT21 observed without fabricating 21 executions",
                    "initial": owned.json(),
                    "foreign_statuses": denied,
                    "stale_status": stale.status_code,
                    "unchanged_goal": after,
                    "cancel_body": body,
                    "cancel": cancelled.json(),
                    "duplicate": repeated.json(),
                    "terminal": final.json(),
                    "projection_limits": projection_limits,
                },
            )

        fixture = {"proof": {"owner_id": item.spec.user_id, "thread_id": item.spec.thread_id}, "accepted_waiting": {"body": [initial]}}
        requests, outbound = await controlled_im(tmp_path / "im-boundary", monkeypatch, fixture, item.env[1])
        assert len(requests) == 3
        assert all("Queued" in msg.text and "STOP: not_started" in msg.text for msg in outbound if msg.is_final)
        assert all("UNACCEPTED-WORKER-LOG" not in msg.text for msg in outbound)
        pending = {**initial, "state": "running", "run_status": "running", "cancel_requested": True, "stop_state": "unconfirmed", "resources_held": True}
        pending_text = format_fleet_summary([pending])
        assert "Stopping" in pending_text and "STOP: unconfirmed" in pending_text and "resources held: yes" in pending_text
        many = {
            **initial,
            "jobs": [
                {"job_id": f"safe-job-{i:02}", "generation": initial["generation"], "parent_run_id": item.spec.run_id, "link_mode": "awaited", "state": "queued", "accepted_manifest_id": None, "private_log": "UNACCEPTED-WORKER-LOG"}
                for i in range(20)
            ],
            "runs": [{"run_id": f"safe-run-{i:02}", "generation": initial["generation"], "run_status": "pending"} for i in range(20)],
        }
        bounded = format_fleet_summary([many])
        assert len(("\n\n" + bounded).encode()) <= 4096
        assert sum(line.startswith(("Job ", "Run ")) for line in bounded.splitlines()) <= 20
        assert "Further observations omitted." in bounded and "UNACCEPTED-WORKER-LOG" not in bounded
        assert bounded == format_fleet_summary([many]) and item.spec.run_id in bounded
        original_client = httpx.AsyncClient
        network_reads = []
        modes = iter(((200, []), (404, {"detail": "Not Found"}), (404, {"detail": "Thread not found"}), (403, {"detail": "private denial"}), (200, None)))

        def response(request):
            status, payload = next(modes)
            network_reads.append(request.headers.get("X-DeerFlow-Owner-User-Id"))
            if payload is None:
                raise httpx.ReadTimeout("private tracking failure", request=request)
            return httpx.Response(status, json=payload)

        with monkeypatch.context() as patch:
            patch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: original_client(*args, **{**kwargs, "transport": httpx.MockTransport(response)}))
            headers = create_internal_auth_headers(owner_user_id=item.spec.user_id)
            results = [await read_fleet_summary("http://controlled.invalid", item.spec.thread_id, headers=headers) for _ in range(5)]
            assert results == ["", "", UNAVAILABLE, UNAVAILABLE, UNAVAILABLE]
            assert await read_fleet_summary("http://controlled.invalid", item.spec.thread_id, headers={}) == UNAVAILABLE
        assert network_reads == [item.spec.user_id] * 5
        evidence("boundary-im", {"qualification": "Controlled bus plus neutral HTTP reader; no external messages", "pending": pending_text, "bounded": bounded, "reader_results": results, "owner_ids": network_reads})
    finally:
        await stack.aclose()
