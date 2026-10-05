"""Original paired finishing retains read authority while resources remain owned."""

import asyncio

import pytest
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c07_stream_reader import original_stream
from .test_c08_terminal_pair import prepared_pair as prepared_pair


async def finishing_stream(pair):
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.runs.manager import RunStatus

    from .test_c08_terminal_pair import participant

    capability, identity, producer, record, reader = await original_stream(pair.item)
    repo = RunRepository(pair.sf, mutation_capability=pair.capability)
    repo._terminal_participant = participant(pair)
    with remote_mutation_scope(capability.context):
        await producer.publish(identity.run_id, "values", {"tail": "actual-before-finishing"})
        await repo.update_status(identity.run_id, status="success")
    record.status = RunStatus.success
    async with pair.sf() as session:
        row = (
            await session.execute(
                text(
                    "SELECT t.state,p.state,t.accepted_workspace_point_id,p.final_workspace_point_id,a.stopped_at,res.state "
                    "FROM fleet_agent_tasks t JOIN fleet_run_placements p ON p.agent_task_id=t.id "
                    "JOIN fleet_attempts a ON a.id=p.active_attempt_id JOIN fleet_reservations res ON res.attempt_id=a.id"
                )
            )
        ).one()
    assert tuple(row[:5]) == ("finishing", "finishing", pair.identity.request_id, pair.identity.request_id, None)
    assert row[5] != "released"
    return capability, identity, producer, record, reader


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ("original-record", "hydrated-record"))
async def test_exact_finishing_pair_keeps_committed_tail_and_seal_without_releasing_capacity(prepared_pair, view):
    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime import RunManager
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.stream_bridge.base import END_SENTINEL, HEARTBEAT_SENTINEL
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    pair = prepared_pair
    capability, identity, producer, original_record, reader = await finishing_stream(pair)
    record = original_record
    if view == "hydrated-record":
        manager = RunManager(store=pair.item.env[4].state.run_store)
        record = await manager.get(identity.run_id, user_id=identity.user_id)
    assert await reader.identity(identity.run_id) == identity
    prepared = await reader.prepare(record, None)
    assert prepared.identity == identity
    page = await reader.page(prepared)
    assert len(page) == 1 and page[0]["content"]["data"] == {"tail": "actual-before-finishing"}
    assert await reader.seal(identity) is None
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(heartbeat_interval=0.01), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, prepared)
    try:
        frame = await asyncio.wait_for(anext(stream), 1)
        assert frame.data == {"tail": "actual-before-finishing"}
        assert await asyncio.wait_for(anext(stream), 1) is HEARTBEAT_SENTINEL
        with remote_mutation_scope(capability.context):
            await producer.publish_end(identity.run_id)
        assert await reader.seal(identity) is not None
        assert await asyncio.wait_for(anext(stream), 1) is END_SENTINEL
    finally:
        await stream.aclose()
    async with pair.sf() as session:
        retained = (await session.execute(text("SELECT p.state,a.stopped_at,res.state FROM fleet_run_placements p JOIN fleet_attempts a ON a.id=p.active_attempt_id JOIN fleet_reservations res ON res.attempt_id=a.id"))).one()
    assert retained[0] == "finishing" and retained[1] is None and retained[2] != "released"


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("task-pointer", "placement-pointer", "frozen-spec", "attempt-state", "core-outcome"))
async def test_finishing_read_rejects_changed_pair_or_original_mapping_before_cached_yield(prepared_pair, fault):
    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    pair = prepared_pair
    _, identity, _, record, reader = await finishing_stream(pair)
    prepared = await reader.prepare(record, None)
    assert prepared.identity == identity
    prior_page = await reader.page(prepared)
    assert len(prior_page) == 1
    statements = {
        "task-pointer": "UPDATE fleet_agent_tasks SET accepted_workspace_point_id=NULL",
        "placement-pointer": "UPDATE fleet_run_placements SET final_workspace_point_id=NULL",
        "frozen-spec": "UPDATE fleet_attempts SET launch_spec=jsonb_set(launch_spec,'{launch_spec,model_name}',to_jsonb('different-model'::text))",
        "attempt-state": "UPDATE fleet_attempts SET state='unknown'",
        "core-outcome": "UPDATE runs SET status='error'",
    }
    async with pair.item.engine.begin() as conn:
        await conn.execute(text(statements[fault]))
    assert await reader.identity(identity.run_id) is None
    assert await reader.ensure_history_available(prepared, expected_seq=prior_page[0]["seq"]) is False
    assert await reader.page(prepared) is None
    assert await reader.seal(identity) is None
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, prepared)
    try:
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 1)
    finally:
        await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("task-pointer", "frozen-spec", "root-stamp"))
async def test_original_seal_select_rechecks_finishing_pair_after_actual_identity_snapshot(prepared_pair, fault, tmp_path):
    """SQL-only interleaving; no physical-stop or unapplied-proposal proof."""
    import json
    import os
    from dataclasses import asdict
    from pathlib import Path

    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    pair = prepared_pair
    capability, identity, producer, _, reader = await finishing_stream(pair)
    with remote_mutation_scope(capability.context):
        await producer.publish_end(identity.run_id)
    # Controlled legacy-active placement admits the current production preflight.
    # The real terminal point and writer seal were produced by original APIs.
    async with pair.item.engine.begin() as conn:
        await conn.execute(text("UPDATE fleet_run_placements SET state='running'"))
    original_identity = reader.identity
    assert await original_identity(identity.run_id) == identity
    observed = []

    async def interleaved_identity(run_id):
        actual_identity = await original_identity(run_id)
        if not observed:
            assert actual_identity == identity
            async with pair.item.engine.begin() as conn:
                await conn.execute(text("UPDATE fleet_run_placements SET state='finishing'"))
                faults = {
                    "task-pointer": "UPDATE fleet_agent_tasks SET accepted_workspace_point_id=NULL",
                    "frozen-spec": "UPDATE fleet_attempts SET launch_spec=jsonb_set(launch_spec,'{launch_spec,model_name}',to_jsonb('different-model'::text))",
                    "root-stamp": "UPDATE checkpoints SET metadata=jsonb_set(metadata,'{deerflow_execution_run_id}',to_jsonb('different-run'::text)) WHERE checkpoint_ns='' AND checkpoint_id=:checkpoint",
                }
                changed = await conn.execute(text(faults[fault]), {"checkpoint": pair.identity.checkpoint_id})
                assert changed.rowcount == 1
                state = dict(
                    (
                        await conn.execute(
                            text(
                                "SELECT p.state,t.accepted_workspace_point_id,p.final_workspace_point_id,q.state AS request_state,"
                                "c.metadata->>'deerflow_execution_run_id' AS root_stamp FROM fleet_run_placements p "
                                "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id JOIN fleet_workspace_requests q ON q.run_id=p.run_id "
                                "JOIN checkpoints c ON c.thread_id=p.thread_id AND c.checkpoint_ns='' ORDER BY c.checkpoint_id DESC LIMIT 1"
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
            observed.append({"actual_preflight_identity": asdict(actual_identity), "mutation_rowcount": changed.rowcount, "after_interleaving": state})
        return actual_identity  # actual original preflight snapshot, not fabricated authority

    reader.identity = interleaved_identity
    try:
        actual_seal = await reader.seal(identity)
    finally:
        reader.identity = original_identity
    assert len(observed) == 1
    evidence = Path(os.environ.get("C08_FINISHING_RACE_EVIDENCE_DIR", str(tmp_path)))
    evidence.mkdir(parents=True, exist_ok=True)
    evidence.chmod(0o700)
    (evidence / (fault + ".json")).write_text(json.dumps({"scope": "SQL-only original preflight/select interleaving", "fault": fault, "observed": observed, "actual_seal": actual_seal}, indent=2))
    assert actual_seal is None, "Original awaited seal SELECT must reject the now-invalid finishing pair before END"


@pytest.mark.asyncio
async def test_original_accepted_request_state_rewrite_is_rejected_before_reader_query(prepared_pair):
    """DB immutability negative, explicitly not a reader-race proof."""
    from sqlalchemy.exc import DBAPIError

    pair = prepared_pair
    await finishing_stream(pair)
    with pytest.raises(DBAPIError, match="Fleet workspace request outcome is immutable"):
        async with pair.item.engine.begin() as conn:
            await conn.execute(text("UPDATE fleet_workspace_requests SET state='rejected'"))
    async with pair.item.engine.connect() as conn:
        row = (await conn.execute(text("SELECT state,candidate_manifest_id FROM fleet_workspace_requests"))).one()
    assert tuple(row) == ("accepted", pair.candidate.manifest_id)
