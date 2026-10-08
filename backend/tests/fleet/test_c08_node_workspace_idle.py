"""Original HTTP/PG races; these native contracts do not prove Linux cleanup."""

from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import text

from .test_c08_final_stop_authority import accept, durable_rows
from .test_c08_terminal_pair import admission as admission  # noqa: F401
from .test_c08_terminal_pair import checkpoint_owner as checkpoint_owner  # noqa: F401
from .test_c08_terminal_pair import owner_environment as owner_environment  # noqa: F401
from .test_c08_terminal_pair import prepared_pair as prepared_pair  # noqa: F401


async def client_for(pair, http):
    from deerflow_ecs_fleet.worker.client import NodeClient

    client = NodeClient(gateway_url="http://test", credential=pair.item.env[7].token, http_client=http)
    client.node_id, client.session_id = pair.identity.node_id, pair.identity.node_session_id
    return client, {"kind": "agent", "attempt_id": pair.identity.attempt_id, "token": pair.item.accepted.token}


def observe_failure(monkeypatch, service, operation, errors):
    original = getattr(service, operation)

    async def observed(**kwargs):
        try:
            return await original(**kwargs)
        except Exception as error:
            errors.append((type(error).__name__, str(error)))
            raise

    monkeypatch.setattr(service, operation, observed)


@pytest.mark.asyncio
async def test_exact_final_finishing_workspace_poll_is_read_only_idle(prepared_pair, monkeypatch):
    p = prepared_pair
    await accept(p)
    before = await durable_rows(p)
    errors = []
    observe_failure(monkeypatch, p.item.env[4].state.fleet_workspaces, "poll", errors)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=p.item.env[4]), base_url="http://test") as http:
        client, claim = await client_for(p, http)
        try:
            result = await client.attempt(claim, "workspace/poll")
        except httpx.HTTPStatusError as error:
            pytest.fail(f"original finishing poll rejected HTTP {error.response.status_code}: {errors}")
        assert result == {"stop": False, "request": None}
    assert await durable_rows(p) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["partial"], indirect=True)
async def test_original_prepared_poll_then_partial_accept_stale_claim_is_read_only_idle(prepared_pair, monkeypatch):
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool

    p = prepared_pair
    errors = []
    observe_failure(monkeypatch, p.item.env[4].state.fleet_workspaces, "claim", errors)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=p.item.env[4]), base_url="http://test") as http:
        client, claim = await client_for(p, http)
        projection = (await client.attempt(claim, "workspace/poll"))["request"]
        assert projection["state"] == "prepared"
        pool = MCPSessionPool()
        teardown = SimpleNamespace(context=p.capability.context, workspace_writers=p.controller, workspace_sessions=None)
        publisher = FleetWorkspacePublisher(p.sf, p.capability, controller=p.controller, teardown=teardown, session_pool=pool)
        pool.freeze_scope(publisher.scope_key, barrier_epoch=1)
        with p.scope():
            await publisher.accept_partial(p.identity, p.candidate, barrier_epoch=1)
        async with p.sf() as session:
            assert await session.scalar(text("SELECT state FROM fleet_workspace_requests")) == "accepted"
            assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE kind='partial'")) == 1
            assert await session.scalar(text("SELECT clock_timestamp()<lease_expires_at FROM fleet_attempts"))
        before = await durable_rows(p)
        try:
            result = await client.attempt(claim, "workspace/claim", request_id=p.identity.request_id, request_digest=projection["request_digest"], barrier_epoch=projection["barrier_epoch"], nonce=projection["nonce"])
        except httpx.HTTPStatusError as error:
            pytest.fail(f"original accepted-request claim rejected HTTP {error.response.status_code}: {errors}")
        assert result == {"stop": False, "request": None}
        assert await durable_rows(p) == before


async def accept_partial(pair):
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool

    pool = MCPSessionPool()
    teardown = SimpleNamespace(context=pair.capability.context, workspace_writers=pair.controller, workspace_sessions=None)
    publisher = FleetWorkspacePublisher(pair.sf, pair.capability, controller=pair.controller, teardown=teardown, session_pool=pool)
    pool.freeze_scope(publisher.scope_key, barrier_epoch=1)
    with pair.scope():
        await publisher.accept_partial(pair.identity, pair.candidate, barrier_epoch=1)


async def sql_digests(pair):
    import hashlib

    async with pair.sf() as session:
        return {
            table: hashlib.sha256("\n".join((await session.execute(text("SELECT row_to_json(t)::text FROM " + table + " t ORDER BY row_to_json(t)::text"))).scalars()).encode()).hexdigest()
            for table in (
                "runs",
                "checkpoints",
                "checkpoint_writes",
                "fleet_nodes",
                "fleet_credentials",
                "fleet_agent_tasks",
                "fleet_run_placements",
                "fleet_attempts",
                "fleet_reservations",
                "fleet_workspace_requests",
                "fleet_workspace_points",
                "fleet_workspace_manifests",
            )
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["partial"], indirect=True)
async def test_accepted_partial_receipt_survives_original_fenced_same_run_advancement(prepared_pair):
    from langgraph.checkpoint.base import empty_checkpoint

    p = prepared_pair
    await accept_partial(p)
    with p.scope():
        new_root = await p.item.writer.aput(p.item.config, empty_checkpoint(), {"source": "loop", "step": 2, "parents": {}}, {})
    assert new_root["configurable"]["checkpoint_id"] != p.identity.checkpoint_id
    before = await sql_digests(p)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=p.item.env[4]), base_url="http://test") as http:
        client, claim = await client_for(p, http)
        assert await client.attempt(claim, "workspace/claim", request_id=p.identity.request_id, request_digest=p.identity.request_digest, barrier_epoch=1, nonce="e" * 64) == {"stop": False, "request": None}
    assert await sql_digests(p) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepared_pair",
    [
        {"core": "success", "task": "succeeded", "placement": "succeeded"},
        {"kind": "paused", "core": "interrupted", "task": "input_required", "placement": "cancelled"},
        {"core": "error", "task": "failed", "placement": "failed"},
        {"core": "timeout", "task": "timed_out", "placement": "timed_out"},
    ],
    indirect=True,
)
async def test_exact_finishing_poll_and_accepted_final_claim_return_no_grant_or_work(prepared_pair):
    p = prepared_pair
    await accept(p)
    before = await sql_digests(p)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=p.item.env[4]), base_url="http://test") as http:
        client, claim = await client_for(p, http)
        for operation, fields in (("workspace/poll", {}), ("workspace/claim", {"request_id": p.identity.request_id, "request_digest": p.identity.request_digest, "barrier_epoch": 1, "nonce": "e" * 64})):
            assert await client.attempt(claim, operation, **fields) == {"stop": False, "request": None}
    assert await sql_digests(p) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["partial", "final"], indirect=True)
@pytest.mark.parametrize("fault", ["nonce", "digest", "epoch", "token", "session", "stamp", "lease", "credential"])
async def test_idle_receipt_invalid_authority_rejects_without_sql_changes(prepared_pair, fault):
    p = prepared_pair
    await (accept_partial(p) if p.identity.kind == "partial" else accept(p))
    fields = {"request_id": p.identity.request_id, "request_digest": p.identity.request_digest, "barrier_epoch": 1, "nonce": "e" * 64}
    if fault in {"stamp", "lease", "credential"}:
        async with p.sf.begin() as session:
            if fault == "stamp":
                await session.execute(text("UPDATE checkpoints SET metadata=jsonb_set(metadata,'{deerflow_execution_run_id}','\"another-run\"'::jsonb)"))
            elif fault == "lease":
                await session.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second'"))
                await session.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
            elif fault == "credential":
                await session.execute(text("UPDATE fleet_credentials SET revoked_at=clock_timestamp()"))
    before = await sql_digests(p)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=p.item.env[4]), base_url="http://test") as http:
        client, claim = await client_for(p, http)
        if fault == "nonce":
            fields["nonce"] = "f" * 64
        elif fault == "digest":
            fields["request_digest"] = "f" * 64
        elif fault == "epoch":
            fields["barrier_epoch"] = 2
        elif fault == "token":
            claim["token"] = "invalid-original-token"
        elif fault == "session":
            client.session_id = "another-session"
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await client.attempt(claim, "workspace/claim", **fields)
        assert caught.value.response.status_code in {401, 403, 409}
        assert caught.value.response.json().get("detail") in {"Workspace attempt unavailable", "Workspace request unavailable", "Invalid node credential", "Workspace original deadline elapsed"}
    assert await sql_digests(p) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["partial", "final"], indirect=True)
@pytest.mark.parametrize("table,assignment", [("fleet_workspace_manifests", "total_bytes=total_bytes+1"), ("fleet_workspace_points", "publication_key='conflicting'")])
async def test_original_immutable_evidence_rejects_tampering_and_preserves_valid_receipt(prepared_pair, table, assignment):
    from sqlalchemy.exc import DBAPIError

    p = prepared_pair
    await (accept_partial(p) if p.identity.kind == "partial" else accept(p))
    before = await sql_digests(p)
    with pytest.raises(DBAPIError, match="Fleet workspace evidence is immutable"):
        async with p.sf.begin() as session:
            await session.execute(text("UPDATE " + table + " SET " + assignment))
    assert await sql_digests(p) == before
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=p.item.env[4]), base_url="http://test") as http:
        client, claim = await client_for(p, http)
        assert await client.attempt(claim, "workspace/claim", request_id=p.identity.request_id, request_digest=p.identity.request_digest, barrier_epoch=1, nonce="e" * 64) == {"stop": False, "request": None}
    assert await sql_digests(p) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_pair", ["partial"], indirect=True)
@pytest.mark.parametrize("accept_before_claim", [1, 2])
async def test_original_node_claim_race_idle_returns_before_extra_census_copy_or_prepare(prepared_pair, tmp_path, accept_before_claim):
    """Original HTTP/PG/NAS/stager; census values are explicit native adapters."""
    import time

    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.journal import AttemptJournal
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication
    from deerflow_ecs_fleet.workspace import NASWorkspace

    p = prepared_pair
    calls, censuses, copies = [], [], []

    class Client(NodeClient):
        async def attempt(self, claim, operation, **fields):
            calls.append(operation)
            if operation == "workspace/claim" and calls.count(operation) == accept_before_claim:
                await accept_partial(p)
            return await super().attempt(claim, operation, **fields)

    async def census(grant, **kwargs):
        censuses.append(kwargs["request"]["state"])
        return {
            "container_id": "a" * 64,
            "started_at": "original",
            "image": "original",
            "launch_fingerprint": "b" * 64,
            "receipt": {"collector": {"pid_namespace": 77}, "runner": {"pid": 1, "ppid": 0, "start_ticks": 100, "state": "S", "uid": 65534}, "cgroup_digest": "c" * 64},
        }

    output = tmp_path / "node-output"
    source = output / ".deer-flow/users" / p.identity.user_id / "threads" / p.identity.thread_id / "user-data"
    for category in ("workspace", "uploads", "outputs"):
        (source / category).mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=p.item.env[4]), base_url="http://test") as http:
        client = Client(gateway_url="http://test", credential=p.item.env[7].token, http_client=http)
        client.node_id, client.session_id = p.identity.node_id, p.identity.node_session_id
        journal = AttemptJournal(tmp_path / "node-journal")
        stager = AgentWorkspacePublication(client=client, containers=SimpleNamespace(quiesce_workspace=census), nas=NASWorkspace(tmp_path / "nas", identity="task4"), journal=journal)
        original_candidate = stager._candidate

        def counted_candidate(*args, **kwargs):
            copies.append(True)
            return original_candidate(*args, **kwargs)

        stager._candidate = counted_candidate
        claim = {"kind": "agent", "attempt_id": p.identity.attempt_id, "token": p.item.accepted.token}
        record = {"claim": claim, "node_id": p.identity.node_id, "reported": False, "grant": p.item.grant}
        assert not await stager.step(claim, p.item.grant, output_dir=output, record=record, deadline=time.monotonic() + 10)
        assert calls == ["workspace/poll", *(["workspace/claim"] * accept_before_claim)]
        assert len(censuses) == len(copies) == accept_before_claim - 1
        assert not stager.pending_copies and not stager.pending_saves
        ticket = p.controller.reserve()
        ticket.finish()
        await stager.join_writers()
