"""Real PG read authority is independent of a writer's renewable lease."""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import claim
from .test_c03_remote_agent_admission import owner_environment as owner_environment
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c07_event_transactions import attached_manager


async def original_stream(item):
    from deerflow_ecs_fleet.persistence.outbox import EventOutbox

    from app.fleet.events import FleetEventParticipant, FleetProducerBridge, FleetStreamReader, FleetStreamSeals, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.events.store.db import DbRunEventStore

    async with item.engine.begin() as connection:
        await connection.run_sync(RunEventRow.__table__.create, checkfirst=True)
    capability = FleetMutationCapability(item.identity, item.spec)
    identity = RemoteStreamIdentity.from_context(capability.context)
    store = DbRunEventStore(item.env[1], mutation_capability=capability, transaction_participant=FleetEventParticipant(identity=identity, spec=item.spec, capability=capability, outbox=EventOutbox()))
    manager, record = await attached_manager(item)
    producer = FleetProducerBridge(event_store=store, identity=identity, spec=item.spec, capability=capability, seals=FleetStreamSeals(item.env[1]), manager=manager)
    return capability, identity, producer, record, FleetStreamReader(item.env[1])


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["unknown", "quarantined"])
async def test_unknown_original_attempt_replays_frames_without_old_seal_end(checkpoint_owner, state):
    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.runs.manager import RunStatus
    from deerflow.runtime.stream_bridge.base import HEARTBEAT_SENTINEL
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    capability, identity, producer, record, reader = await original_stream(item)
    with remote_mutation_scope(capability.context):
        await producer.publish(identity.run_id, "values|child:one", {"tail": "committed"})
        record.status = RunStatus.success
        async with item.engine.begin() as connection:
            await connection.execute(text("UPDATE runs SET status='success'"))
        await producer.publish_end(identity.run_id)
    async with item.engine.begin() as connection:
        await connection.execute(text("UPDATE fleet_run_placements SET state='unknown'"))
        await connection.execute(text("UPDATE fleet_attempts SET state=:state"), {"state": state})
        await connection.execute(text("UPDATE fleet_reservations SET state='quarantined'"))
        await connection.execute(text("UPDATE fleet_agent_tasks SET generation=generation+1,state='unknown'"))
        before = (await connection.execute(text("SELECT lease_expires_at,stopped_at FROM fleet_attempts"))).one()
    prepared = await reader.prepare(record, None)
    assert prepared.identity == identity
    assert await reader.seal(identity) is None
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(heartbeat_interval=0.05), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, prepared)
    try:
        frame = await asyncio.wait_for(anext(stream), 1)
        assert frame.event == "values|child:one" and frame.data == {"tail": "committed"}
        assert await asyncio.wait_for(anext(stream), 1) is HEARTBEAT_SENTINEL
    finally:
        await stream.aclose()
    async with item.engine.connect() as connection:
        assert (await connection.execute(text("SELECT lease_expires_at,stopped_at FROM fleet_attempts"))).one() == before


@pytest.mark.asyncio
async def test_changed_original_mapping_stops_stream_without_end_and_rejects_old_cursor(checkpoint_owner):
    from app.fleet.events import FleetGatewayBridge, InvalidRemoteCursor
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    capability, identity, producer, record, reader = await original_stream(item)
    with remote_mutation_scope(capability.context):
        await producer.publish(identity.run_id, "metadata", {"run_id": identity.run_id})
    prepared = await reader.prepare(record, None)
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, prepared)
    try:
        frame = await anext(stream)
        async with item.engine.begin() as connection:
            await connection.execute(text("UPDATE fleet_run_placements SET active_attempt_id=NULL"))
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 1)
        with pytest.raises(InvalidRemoteCursor):
            await reader.prepare(record, frame.id)
    finally:
        await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["user_id", "thread_id"])
async def test_queued_reader_validates_actual_core_placement_owner(owner_environment, field):
    from app.fleet.events import FleetStreamReader, InvalidRemoteCursor

    _, sf, _, _, _, record, *_ = owner_environment
    reader = FleetStreamReader(sf)
    prepared = await reader.prepare(record, None)
    assert prepared.identity is None
    assert (prepared.expected_user_id, prepared.expected_thread_id) == (record.user_id, record.thread_id)
    with pytest.raises(InvalidRemoteCursor, match="owner"):
        await reader.prepare(replace(record, **{field: "wrong-owner"}), None)


@pytest.mark.asyncio
async def test_queued_subscription_binds_once_after_real_claim(owner_environment):
    import hashlib
    from types import SimpleNamespace

    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity

    from app.fleet.events import FleetGatewayBridge, FleetStreamReader
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    engine, sf, _, _, app, record, session_id, *_ = owner_environment
    reader = FleetStreamReader(sf)
    prepared = await reader.prepare(record, None)
    assert prepared.identity is None
    accepted = await claim(owner_environment)
    await app.state.fleet_ownership.authorize_start(node_id="node-c03", node_session_id=session_id, attempt_id=accepted.attempt_id, token=accepted.token)
    identity = ExecutionIdentity(
        node_id="node-c03",
        node_session_id=session_id,
        agent_task_id=accepted.agent_task_id,
        generation=accepted.launch_spec["generation"],
        attempt_id=accepted.attempt_id,
        owner_worker_id=accepted.owner_worker_id,
        token_stamp=hashlib.sha256(accepted.token.encode()).hexdigest(),
    )
    item = SimpleNamespace(engine=engine, env=owner_environment, identity=identity, spec=LaunchSpec.model_validate(accepted.launch_spec))
    capability, stream_identity, producer, _, _ = await original_stream(item)
    with remote_mutation_scope(capability.context):
        await producer.publish(record.run_id, "metadata", {"run_id": record.run_id})
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    stream = bridge.subscribe_prepared(record.run_id, prepared)
    try:
        frame = await asyncio.wait_for(anext(stream), 1)
        assert frame.event == "metadata" and frame.data == {"run_id": record.run_id}
        assert frame.id.split(".")[:2] == ["fleet", "v1"]
        assert await reader.identity(record.run_id) == stream_identity
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_real_attempt_mapping_switch_never_merges_original_frames(checkpoint_owner):
    from uuid import uuid4

    from deerflow_ecs_fleet.persistence.models import AttemptRow

    from app.fleet.events import InvalidRemoteCursor, PreparedRemoteSubscription, remote_cursor
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = checkpoint_owner
    capability, identity, producer, record, reader = await original_stream(item)
    with remote_mutation_scope(capability.context):
        await producer.publish(identity.run_id, "values", {"tail": "old attempt only"})
    prepared = await reader.prepare(record, None)
    old_rows = await reader.page(prepared)
    assert len(old_rows) == 1
    replacement = str(uuid4())
    async with item.engine.begin() as connection:
        old = dict((await connection.execute(AttemptRow.__table__.select().where(AttemptRow.id == identity.attempt_id))).mappings().one())
        await connection.execute(text("UPDATE fleet_attempts SET state='failed' WHERE id=:id"), {"id": identity.attempt_id})
        # Controlled identity fixture, not takeover implementation or a new Agent.
        old.update(id=replacement, attempt_no=old["attempt_no"] + 1, state="claimed", token_hash="e" * 64, stopped_at=None, finished_at=None, outcome=None, process_ref=None, start_authorized_at=None, started_at=None)
        await connection.execute(AttemptRow.__table__.insert().values(**old))
        await connection.execute(text("UPDATE fleet_run_placements SET active_attempt_id=:id,state='claimed'"), {"id": replacement})
    assert await reader.page(prepared) is None
    with pytest.raises(InvalidRemoteCursor):
        await reader.prepare(record, remote_cursor(identity, old_rows[0]["seq"]))
    new_identity = await reader.identity(identity.run_id)
    assert new_identity.attempt_id == replacement
    assert await reader.page(PreparedRemoteSubscription(new_identity, 0)) == []
    assert await reader.seal(new_identity) is None


async def retention_stream(item, *, sealed=True, count=3, semantic_gap=False):
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.runs.manager import RunStatus

    capability, identity, producer, record, reader = await original_stream(item)
    with remote_mutation_scope(capability.context):
        for number in range(count):
            await producer.publish(identity.run_id, "values", {"retention_frame": number})
            if semantic_gap and number == 0:
                await producer.event_store.put(thread_id=identity.thread_id, run_id=identity.run_id, event_type="run.start", category="trace", content={"chain": "semantic-only"})
        if sealed:
            record.status = RunStatus.success
            async with item.engine.begin() as connection:
                await connection.execute(text("UPDATE runs SET status='success' WHERE run_id=:run"), {"run": identity.run_id})
            await producer.publish_end(identity.run_id)
    prepared = await reader.prepare(record, None)
    rows = []
    while batch := await reader.page(prepared):
        rows.extend(batch)
        prepared = replace(prepared, after_seq=batch[-1]["seq"])
    return identity, producer, record, reader, rows


async def trusted_host_retention(item, identity, rows, removal):
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import current_remote_mutation_context

    assert current_remote_mutation_context() is None
    if removal == "whole":
        # Supported trusted host retention, without the original terminal remote
        # writer's mutation capability/context. Private pointers/seal remain.
        return await DbRunEventStore(item.env[1]).delete_by_run(identity.thread_id, identity.run_id, user_id=identity.user_id)
    # Tail/middle/prefix are controlled SQL host-retention fixtures. They do not
    # represent an original terminal writer performing an authorized deletion.
    target = rows[-1] if removal == "tail" else rows[1] if removal == "middle" else rows[0]
    async with item.engine.begin() as connection:
        await connection.execute(text("DELETE FROM run_events WHERE run_id=:run AND seq=:seq"), {"run": identity.run_id, "seq": target["seq"]})
    return 1


@pytest.mark.asyncio
@pytest.mark.parametrize("sealed", [True, False])
@pytest.mark.parametrize("removal", ["whole", "tail", "middle"])
async def test_host_retention_missing_unconsumed_pointer_preflights_history_unavailable(checkpoint_owner, sealed, removal):
    from fastapi import HTTPException

    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item, sealed=sealed)
    await trusted_host_retention(item, identity, rows, removal)
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    with pytest.raises(HTTPException) as rejected:
        await bridge.prepare(record, None)
    assert rejected.value.status_code == 410
    async with item.engine.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM fleet_event_outbox")) == 3
        assert await connection.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == int(sealed)


@pytest.mark.asyncio
@pytest.mark.parametrize("removal", ["tail", "middle"])
async def test_retained_valid_cursor_rejects_missing_future_frame_with_410(checkpoint_owner, removal):
    from fastapi import HTTPException

    from app.fleet.events import FleetGatewayBridge, remote_cursor
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item)
    cursor = remote_cursor(identity, rows[0]["seq"])
    await trusted_host_retention(item, identity, rows, removal)
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    with pytest.raises(HTTPException) as rejected:
        await bridge.prepare(record, cursor)
    assert rejected.value.status_code == 410


@pytest.mark.asyncio
async def test_deleted_or_malformed_cursor_retains_400_priority_over_unavailable_history(checkpoint_owner):
    from fastapi import HTTPException

    from app.fleet.events import FleetGatewayBridge, remote_cursor
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item)
    deleted_cursor = remote_cursor(identity, rows[0]["seq"])
    await trusted_host_retention(item, identity, rows, "whole")
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    for cursor in (deleted_cursor, "malformed"):
        with pytest.raises(HTTPException) as rejected:
            await bridge.prepare(record, cursor)
        assert rejected.value.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize("removal", ["whole", "tail", "middle"])
async def test_retention_during_prefetched_subscription_ends_without_end_then_rejects_reconnect(checkpoint_owner, removal):
    from fastapi import HTTPException

    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item)
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, await bridge.prepare(record, None))
    try:
        first = await anext(stream)  # Original page has already prefetched all3.
        await trusted_host_retention(item, identity, rows, removal)
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 1)
        with pytest.raises(HTTPException) as rejected:
            await bridge.prepare(record, first.id)
        assert rejected.value.status_code == (400 if removal == "whole" else 410)
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_retention_before_consumed_cursor_allows_remaining_tail_and_end(checkpoint_owner):
    from app.fleet.events import FleetGatewayBridge, remote_cursor
    from deerflow.runtime.stream_bridge.base import END_SENTINEL
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item)
    # Cursor2 is an actual committed frame. Prefix1 was already consumed.
    cursor = remote_cursor(identity, rows[1]["seq"])
    await trusted_host_retention(item, identity, rows, "prefix")
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, await bridge.prepare(record, cursor))
    try:
        frame = await anext(stream)
        assert frame.id == remote_cursor(identity, rows[2]["seq"])
        assert await anext(stream) is END_SENTINEL
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_semantic_sequence_gap_without_missing_stream_pointer_replays_and_seals(checkpoint_owner):
    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.stream_bridge.base import END_SENTINEL
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item, semantic_gap=True)
    assert rows[1]["seq"] > rows[0]["seq"] + 1
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, await bridge.prepare(record, None))
    try:
        delivered = [await anext(stream) for _ in rows]
        assert [frame.data for frame in delivered] == [row["content"]["data"] for row in rows]
        assert await anext(stream) is END_SENTINEL
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_zero_frame_original_seal_still_emits_end(checkpoint_owner):
    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.stream_bridge.base import END_SENTINEL
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item, count=0)
    assert rows == []
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, await bridge.prepare(record, None))
    try:
        assert await anext(stream) is END_SENTINEL
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_publisher_target_lookup_keeps_missing_frame_none_without_history_error_or_ack(checkpoint_owner):
    from deerflow_ecs_fleet.persistence.models import EventOutboxRow

    item = checkpoint_owner
    identity, _, _, reader, rows = await retention_stream(item)
    await trusted_host_retention(item, identity, rows, "tail")
    async with item.env[1]() as session:
        # Use mapped ORM pointers exactly as the original publisher does.
        from sqlalchemy import select

        pointers = list((await session.scalars(select(EventOutboxRow).order_by(EventOutboxRow.seq))).all())
        assert (await reader.load_committed(pointers[0]))["seq"] == rows[0]["seq"]
        assert await reader.load_committed(pointers[-1]) is None
        assert all(pointer.published_at is None for pointer in pointers)


@pytest.mark.asyncio
async def test_healthy_long_history_runtime_retention_probes_have_bounded_pointer_candidates(checkpoint_owner, tmp_path):
    import json
    import os
    from pathlib import Path

    from sqlalchemy import event

    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item, count=384)
    assert len(rows) == 384
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    prepared = await bridge.prepare(record, None)  # Full preheader check is a separate phase.
    captured = []

    def record_runtime_probe(connection, cursor, statement, parameters, context, executemany):
        if "fleet_event_outbox" in statement:
            kind = "missing_history_window" if "run_events.id IS NULL" in statement else "authority_point" if "run_events" not in statement else "pointer_page"
            captured.append((kind, statement, parameters))

    event.listen(item.engine.sync_engine, "before_cursor_execute", record_runtime_probe)
    stream = bridge.subscribe_prepared(identity.run_id, prepared)
    try:
        delivered = [await anext(stream) for _ in range(3)]
        assert [frame.data for frame in delivered] == [row["content"]["data"] for row in rows[:3]]
    finally:
        await stream.aclose()
        event.remove(item.engine.sync_engine, "before_cursor_execute", record_runtime_probe)
    assert sum(kind == "missing_history_window" for kind, _, _ in captured) >= 3
    observations = []

    def plan_nodes(node):
        yield node
        for child in node.get("Plans", []):
            yield from plan_nodes(child)

    async with item.engine.connect() as connection:
        for kind, statement, parameters in captured:
            raw = await connection.exec_driver_sql("EXPLAIN (ANALYZE, FORMAT JSON) " + statement, parameters)
            actual = raw.scalar_one()
            actual = json.loads(actual) if isinstance(actual, str) else actual
            pointer_nodes = [node for node in plan_nodes(actual[0]["Plan"]) if node.get("Relation Name") == "fleet_event_outbox"]
            assert pointer_nodes
            if kind == "pointer_page":
                # Measure the limited candidate/output stage, not physical heap
                # rows that a planner may filter before choosing that window.
                limited_windows = [node for node in plan_nodes(actual[0]["Plan"]) if node["Node Type"] == "Limit" and any(child.get("Relation Name") == "fleet_event_outbox" for child in plan_nodes(node))]
                matching_pointer_rows = max((node["Actual Rows"] * node["Actual Loops"] for node in limited_windows), default=actual[0]["Plan"]["Actual Rows"])
            else:
                matching_pointer_rows = sum(node["Actual Rows"] * node["Actual Loops"] for node in pointer_nodes)
            observations.append({"probe_kind": kind, "sql": statement, "parameters": list(parameters), "plan": actual, "matching_pointer_candidates": matching_pointer_rows})
    evidence = Path(os.environ.get("C07_EVIDENCE_DIR", str(tmp_path))) / "long-history-runtime-query-observation.json"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(
        json.dumps(
            {
                "actual_committed_frames": len(rows),
                "phase": "runtime after completed preheader preparation",
                "observations": observations,
                "scope": "Actual filtered pointer candidates only; EXPLAIN physical heap/filter rows retained separately; not a latency benchmark",
            },
            indent=2,
            default=str,
        )
    )
    assert all(0 <= observed["matching_pointer_candidates"] <= 128 for observed in observations), "Each runtime retention probe must check a bounded prefetched pointer window, not the entire healthy suffix"


@pytest.mark.asyncio
@pytest.mark.parametrize("deleted_index", [128, 299])
async def test_retention_after_prepare_across_pointer_page_boundary_never_skips_missing_frame(checkpoint_owner, deleted_index, tmp_path):
    import json
    import os
    from pathlib import Path

    from fastapi import HTTPException

    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item, count=300)
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    prepared = await bridge.prepare(record, None)
    # Controlled SQL host-retention fixture after successful preparation;
    # original pointers remain, including page boundary129 and last tail300.
    async with item.engine.begin() as connection:
        await connection.execute(text("DELETE FROM run_events WHERE run_id=:run AND seq=:seq"), {"run": identity.run_id, "seq": rows[deleted_index]["seq"]})
    stream = bridge.subscribe_prepared(identity.run_id, prepared)
    observed = []
    try:
        while True:
            try:
                frame = await asyncio.wait_for(anext(stream), 1)
            except StopAsyncIteration:
                break
            assert hasattr(frame, "data"), "Missing private pointer must produce EOF, never heartbeat/END"
            assert frame.data["retention_frame"] < deleted_index
            observed.append(frame)
        assert len(observed) <= deleted_index
        evidence = Path(os.environ.get("C07_EVIDENCE_DIR", str(tmp_path))) / f"pointer-page-boundary-delete-{deleted_index + 1}.json"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(
            json.dumps(
                {
                    "fixture": "controlled SQL host retention after real preparation",
                    "committed_frames": len(rows),
                    "deleted_pointer_seq": rows[deleted_index]["seq"],
                    "healthy_prefix_frames_delivered": len(observed),
                    "last_actual_cursor": observed[-1].id if observed else None,
                    "closure": "EOF without END",
                },
                indent=2,
            )
        )
        with pytest.raises(HTTPException) as rejected:
            await bridge.prepare(record, None)
        assert rejected.value.status_code == 410
    finally:
        await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mapping", ["abandoned", "replacement"])
async def test_prefetched_original_frames_stop_before_next_yield_when_accepted_mapping_changes(checkpoint_owner, mapping):
    from uuid import uuid4

    from deerflow_ecs_fleet.persistence.models import AttemptRow
    from fastapi import HTTPException

    from app.fleet.events import FleetGatewayBridge
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    item = checkpoint_owner
    identity, _, record, reader, rows = await retention_stream(item)
    bridge = FleetGatewayBridge(local_bridge=MemoryStreamBridge(), reader=reader)
    stream = bridge.subscribe_prepared(identity.run_id, await bridge.prepare(record, None))
    try:
        first = await anext(stream)  # Three original frames are already prefetched.
        async with item.engine.begin() as connection:
            if mapping == "abandoned":
                await connection.execute(text("UPDATE fleet_run_placements SET active_attempt_id=NULL WHERE run_id=:run"), {"run": identity.run_id})
            else:
                # Controlled accepted mapping fixture, not takeover or a new
                # Agent process. Original attempt data supplies the identity.
                original = dict((await connection.execute(AttemptRow.__table__.select().where(AttemptRow.id == identity.attempt_id))).mappings().one())
                replacement = str(uuid4())
                original.update(id=replacement, attempt_no=original["attempt_no"] + 1, state="claimed", token_hash="d" * 64, stopped_at=None, finished_at=None, outcome=None, process_ref=None, start_authorized_at=None, started_at=None)
                await connection.execute(text("UPDATE fleet_attempts SET state='failed' WHERE id=:id"), {"id": identity.attempt_id})
                await connection.execute(AttemptRow.__table__.insert().values(**original))
                await connection.execute(text("UPDATE fleet_run_placements SET active_attempt_id=:id,state='claimed' WHERE run_id=:run"), {"id": replacement, "run": identity.run_id})
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 1)
        with pytest.raises(HTTPException) as rejected:
            await bridge.prepare(record, first.id)
        assert rejected.value.status_code == 400
        assert len(rows) == 3
    finally:
        await stream.aclose()
