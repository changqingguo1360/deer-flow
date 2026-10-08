"""Actual original-authority SQL transactions for remote transport frames."""

import pytest
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import owner_environment as owner_environment
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c08_terminal_pair import prepared_pair as prepared_pair


async def attached_manager(item):
    from deerflow.runtime import RunManager

    manager = RunManager(store=item.env[4].state.run_store, worker_id=item.identity.owner_worker_id)
    record = await manager.attach_existing_executor(item.spec.run_id, user_id=item.spec.user_id, thread_id=item.spec.thread_id, owner_worker_id=item.identity.owner_worker_id, execution_backend="fleet")
    return manager, record


@pytest.mark.asyncio
@pytest.mark.parametrize("write", ["put", "put_batch", "put_if_absent"])
async def test_original_writer_commits_event_and_private_pointer_together(checkpoint_owner, write):
    from deerflow_ecs_fleet.persistence.outbox import EventOutbox

    from app.fleet.events import FleetEventParticipant, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = checkpoint_owner
    _, sf, *_ = item.env
    async with item.engine.begin() as connection:
        await connection.run_sync(RunEventRow.__table__.create, checkfirst=True)
    capability = FleetMutationCapability(item.identity, item.spec)
    identity = RemoteStreamIdentity.from_context(capability.context)
    participant = FleetEventParticipant(identity=identity, spec=item.spec, capability=capability, outbox=EventOutbox())
    store = DbRunEventStore(sf, mutation_capability=capability, transaction_participant=participant)
    event = dict(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="stream.frame", category="stream", content={"event": "values|child:one", "data": {"content": "真实"}}, metadata={"stream_version": 1})
    with remote_mutation_scope(capability.context):
        if write == "put_batch":
            written = await store.put_batch([event, event | {"content": {"event": "updates", "data": {"done": True}}}])
        elif write == "put_if_absent":
            first, created = await store.put_if_absent(**event)
            again, repeated = await store.put_if_absent(**event)
            assert created and not repeated and first == again
            written = [first]
        else:
            written = [await store.put(**event)]
    async with item.engine.connect() as connection:
        rows = (await connection.execute(text("SELECT e.id,e.seq,o.event_id,o.seq AS pointer_seq,o.attempt_id,o.user_id,o.launch_spec_digest FROM run_events e JOIN fleet_event_outbox o ON e.id=o.event_id ORDER BY e.seq"))).mappings().all()
    assert len(rows) == len(written)
    assert [row["seq"] for row in rows] == [event["seq"] for event in written]
    for row in rows:
        assert row["id"] == row["event_id"] and row["seq"] == row["pointer_seq"]
        assert (row["attempt_id"], row["user_id"], row["launch_spec_digest"]) == (identity.attempt_id, identity.user_id, identity.launch_spec_digest)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["second_pointer", "post_sql_expiry"])
async def test_actual_private_pointer_and_whole_batch_rollback(checkpoint_owner, failure):
    from deerflow_ecs_fleet.persistence.outbox import EventOutbox

    from app.fleet.events import FleetEventParticipant, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = checkpoint_owner
    _, sf, *_ = item.env
    async with item.engine.begin() as connection:
        await connection.run_sync(RunEventRow.__table__.create, checkfirst=True)
        if failure == "post_sql_expiry":
            await connection.execute(text("UPDATE runs SET lease_expires_at=clock_timestamp()+interval '1 second'"))
            await connection.execute(text("UPDATE fleet_attempts SET lease_expires_at=(SELECT lease_expires_at FROM runs)"))
    capability = FleetMutationCapability(item.identity, item.spec)
    identity = RemoteStreamIdentity.from_context(capability.context)
    observations = []

    class Outbox(EventOutbox):
        async def insert(self, session, **kwargs):
            await super().insert(session, **kwargs)
            rows = (await session.execute(text("SELECT e.id,e.seq,o.event_id,o.seq FROM run_events e JOIN fleet_event_outbox o ON e.id=o.event_id ORDER BY e.seq"))).all()
            observations.append(rows)
            if failure == "post_sql_expiry":
                await session.execute(text("SELECT pg_sleep(1.2)"))
            elif len(observations) == 2:
                raise RuntimeError("second actual pointer rejects")

    participant = FleetEventParticipant(identity=identity, spec=item.spec, capability=capability, outbox=Outbox())
    store = DbRunEventStore(sf, mutation_capability=capability, transaction_participant=participant)
    event = dict(thread_id=item.spec.thread_id, run_id=item.spec.run_id, event_type="stream.frame", category="stream", content={"event": "messages", "data": "tail"})
    with remote_mutation_scope(capability.context), pytest.raises(RuntimeError, match="ownership|second actual pointer"):
        if failure == "second_pointer":
            await store.put_batch([event, event])
        else:
            await store.put(**event)
    assert len(observations) == (2 if failure == "second_pointer" else 1)
    assert len(observations[-1]) == len(observations)
    async with item.engine.connect() as connection:
        assert (await connection.execute(text("SELECT count(*) FROM run_events"))).scalar_one() == 0
        assert (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox"))).scalar_one() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("closure", ["closed", "finalizing", "ownership_lost", "sql_running", "wrong_status"])
async def test_narrow_writer_seal_requires_original_closed_terminal_record(prepared_pair, closure):
    from app.fleet.events import FleetStreamSeals, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.runs.manager import RunStatus

    item = prepared_pair.item
    _, record = await attached_manager(item)
    from deerflow.persistence.run.sql import RunRepository

    from .test_c08_terminal_pair import participant

    with prepared_pair.scope():
        await RunRepository(prepared_pair.sf, mutation_capability=prepared_pair.capability, terminal_participant=participant(prepared_pair)).update_status(item.spec.run_id, "success")
    record.status = RunStatus.success
    record.finalizing = closure == "finalizing"
    record.ownership_lost = closure == "ownership_lost"
    async with item.engine.begin() as connection:
        await connection.execute(text("UPDATE runs SET status=:status"), {"status": "running" if closure == "sql_running" else "error" if closure == "wrong_status" else "success"})
    capability = FleetMutationCapability(item.identity, item.spec)
    identity = RemoteStreamIdentity.from_context(capability.context)
    seals = FleetStreamSeals(item.env[1])
    with remote_mutation_scope(capability.context):
        if closure == "closed":
            first = await seals.writer_seal(identity=identity, spec=item.spec, capability=capability, original_record=record)
            repeated = await seals.writer_seal(identity=identity, spec=item.spec, capability=capability, original_record=record)
            async with item.env[1].begin() as session:
                recovered = await seals._insert(session, identity=identity, status="success", source="physical_stop")
            for receipt in (first, repeated, recovered):
                assert {name: getattr(receipt, name) for name in identity.__dataclass_fields__} == {name: getattr(identity, name) for name in identity.__dataclass_fields__}
                assert (receipt.last_seq, receipt.core_status, receipt.source, receipt.created_at) == (0, "success", "writer", first.created_at)
        else:
            with pytest.raises(RuntimeError, match="closure|terminal|ownership"):
                await seals.writer_seal(identity=identity, spec=item.spec, capability=capability, original_record=record)
    async with item.engine.connect() as connection:
        rows = (await connection.execute(text("SELECT source,last_seq,core_status FROM fleet_stream_seals"))).all()
    assert rows == ([("writer", 0, "success")] if closure == "closed" else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("event_name", ["debug", "events", "debug|subgraph:one", "values|subgraph:one", "messages", "updates"])
async def test_original_producer_preserves_namespace_payload_and_trace_byte_limit(checkpoint_owner, event_name):
    from deerflow_ecs_fleet.persistence.outbox import EventOutbox

    from app.fleet.events import FleetEventParticipant, FleetProducerBridge, FleetStreamSeals, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = checkpoint_owner
    async with item.engine.begin() as connection:
        await connection.run_sync(RunEventRow.__table__.create, checkfirst=True)
    capability = FleetMutationCapability(item.identity, item.spec)
    identity = RemoteStreamIdentity.from_context(capability.context)
    participant = FleetEventParticipant(identity=identity, spec=item.spec, capability=capability, outbox=EventOutbox())
    store = DbRunEventStore(item.env[1], max_trace_content=128, mutation_capability=capability, transaction_participant=participant)
    manager, _ = await attached_manager(item)
    bridge = FleetProducerBridge(event_store=store, identity=identity, spec=item.spec, capability=capability, seals=FleetStreamSeals(item.env[1]), manager=manager)
    payload = {"text": "真实"}
    while store.serialized_content_size({"event": event_name, "data": payload}) < 128:
        payload["text"] += "x"
    assert store.serialized_content_size({"event": event_name, "data": payload}) == 128
    with remote_mutation_scope(capability.context):
        await bridge.publish(identity.run_id, event_name, payload)
        larger = {"text": payload["text"] + "真"}
        if event_name.split("|", 1)[0] in {"debug", "events"}:
            with pytest.raises(ValueError, match="max_trace_content"):
                await bridge.publish(identity.run_id, event_name, larger)
        else:
            larger = {"text": "真" * 5000}
            assert store.serialized_content_size({"event": event_name, "data": larger}) > 10240
            await bridge.publish(identity.run_id, event_name, larger)
    rows = await store.list_events(identity.thread_id, identity.run_id, user_id=identity.user_id)
    assert rows[0]["content"] == {"event": event_name, "data": payload}
    assert len(rows) == (1 if event_name.split("|", 1)[0] in {"debug", "events"} else 2)
    async with item.engine.connect() as connection:
        assert (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox"))).scalar_one() == len(rows)
    for row in rows:
        assert set(row["metadata"]) == {"stream_version", "content_is_json", "content_is_dict"}
        assert row["category"] == "stream"
    assert await store.list_messages(identity.thread_id, user_id=identity.user_id) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["user_id", "thread_id", "owner_worker_id", "replacement"])
async def test_producer_rejects_changed_original_record_before_sql(checkpoint_owner, change):
    from dataclasses import replace

    from deerflow_ecs_fleet.persistence.outbox import EventOutbox

    from app.fleet.events import FleetEventParticipant, FleetProducerBridge, FleetStreamSeals, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    item = checkpoint_owner
    async with item.engine.begin() as connection:
        await connection.run_sync(RunEventRow.__table__.create, checkfirst=True)
    capability = FleetMutationCapability(item.identity, item.spec)
    identity = RemoteStreamIdentity.from_context(capability.context)
    store = DbRunEventStore(item.env[1], mutation_capability=capability, transaction_participant=FleetEventParticipant(identity=identity, spec=item.spec, capability=capability, outbox=EventOutbox()))
    manager, record = await attached_manager(item)
    bridge = FleetProducerBridge(event_store=store, identity=identity, spec=item.spec, capability=capability, seals=FleetStreamSeals(item.env[1]), manager=manager)
    with remote_mutation_scope(capability.context):
        await bridge.publish(identity.run_id, "metadata", {"run_id": identity.run_id})
        if change == "replacement":
            manager._runs[identity.run_id] = replace(record)
        else:
            setattr(record, change, "wrong-original-identity")
        with pytest.raises(RuntimeError, match="executor|ownership"):
            await bridge.publish(identity.run_id, "values", {"must_not_commit": True})
    async with item.engine.connect() as connection:
        assert (await connection.execute(text("SELECT count(*) FROM run_events"))).scalar_one() == 1
        assert (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox"))).scalar_one() == 1


@pytest.mark.asyncio
async def test_retained_private_sequence_survives_actual_host_event_deletion(checkpoint_owner):
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    from .test_c07_stream_reader import original_stream

    item = checkpoint_owner
    capability, identity, producer, _, _ = await original_stream(item)
    with remote_mutation_scope(capability.context):
        await producer.publish(identity.run_id, "values", {"tail": "before retention"})
        before = await producer.event_store.list_events(identity.thread_id, identity.run_id, user_id=identity.user_id)
        await producer.event_store.delete_by_run(identity.thread_id, identity.run_id, user_id=identity.user_id)
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT count(*) FROM run_events"))).scalar_one() == 0
            assert (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox"))).scalar_one() == 1
        await producer.publish(identity.run_id, "values", {"tail": "after retention"})
        after = await producer.event_store.list_events(identity.thread_id, identity.run_id, user_id=identity.user_id)
    assert after[0]["seq"] > before[-1]["seq"], "Private committed highwater must prevent sequence/cursor reuse after host retention"


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("last_seq", 1), ("core_status", "error"), ("thread_id", "foreign-thread"), ("user_id", "foreign-user"), ("generation", 999), ("launch_spec_digest", "0" * 64)])
async def test_repeat_original_writer_rejects_conflicting_durable_seal(prepared_pair, field, value):
    from app.fleet.events import FleetStreamSeals, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, remote_mutation_scope
    from deerflow.runtime.runs.manager import RunStatus

    item = prepared_pair.item
    _, record = await attached_manager(item)
    from deerflow.persistence.run.sql import RunRepository

    from .test_c08_terminal_pair import participant

    with prepared_pair.scope():
        await RunRepository(prepared_pair.sf, mutation_capability=prepared_pair.capability, terminal_participant=participant(prepared_pair)).update_status(item.spec.run_id, "success")
    record.status = RunStatus.success
    record.finalizing = False
    async with item.engine.begin() as connection:
        await connection.execute(text("UPDATE runs SET status='success'"))
    capability = FleetMutationCapability(item.identity, item.spec)
    identity = RemoteStreamIdentity.from_context(capability.context)
    seals = FleetStreamSeals(item.env[1])
    from deerflow_ecs_fleet.persistence.models import StreamSealRow
    from sqlalchemy.dialects.postgresql import insert

    # Seed a conflicting first receipt through the normal schema. Immutable
    # UPDATE triggers stay enabled; no corrupted production mutation is claimed.
    values = {name: getattr(identity, name) for name in identity.__dataclass_fields__}
    values.update(last_seq=0, core_status="success", source="writer")
    values[field] = value
    async with item.env[1].begin() as session:
        await session.execute(insert(StreamSealRow).values(**values))
    with remote_mutation_scope(capability.context), pytest.raises(OwnershipRejected, match="seal.*conflict"):
        await seals.writer_seal(identity=identity, spec=item.spec, capability=capability, original_record=record)
    async with item.engine.connect() as connection:
        assert await connection.scalar(text("SELECT " + field + " FROM fleet_stream_seals")) == value


@pytest.mark.asyncio
async def test_core_terminal_without_accepted_workspace_point_cannot_seal(checkpoint_owner):
    from app.fleet.events import FleetStreamSeals, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, remote_mutation_scope
    from deerflow.runtime.runs.manager import RunStatus

    item = checkpoint_owner
    _, record = await attached_manager(item)
    record.status = RunStatus.success
    record.finalizing = False
    async with item.engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET status='success'"))
    capability = FleetMutationCapability(item.identity, item.spec)
    with remote_mutation_scope(capability.context), pytest.raises(OwnershipRejected, match="ownership"):
        await FleetStreamSeals(item.env[1]).writer_seal(identity=RemoteStreamIdentity.from_context(capability.context), spec=item.spec, capability=capability, original_record=record)
    async with item.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM fleet_stream_seals")) == 0
