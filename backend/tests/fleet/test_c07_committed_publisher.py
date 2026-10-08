"""Real Redis RESP delivery and actual private SQL acknowledgement faults."""

import json

import pytest
from sqlalchemy import text

from .c07_integration_fixture import RedisFaultProxy, owned_redis
from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import owner_environment as owner_environment
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["lost_response", "lost_ack"])
async def test_actual_redis_committed_delivery_retries_once_without_duplicate(checkpoint_owner, tmp_path, fault):
    from deerflow_ecs_fleet.event_bridge import CommittedEventPublisher
    from deerflow_ecs_fleet.persistence.outbox import EventOutbox
    from redis.asyncio import Redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    from app.fleet.events import FleetEventParticipant, FleetStreamReader, RemoteStreamIdentity
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
    event = dict(thread_id=identity.thread_id, run_id=identity.run_id, event_type="stream.frame", category="stream", content={"event": "values", "data": {"tail": "真实"}})
    with remote_mutation_scope(capability.context):
        committed = await store.put_batch([event, event])
    reader = FleetStreamReader(item.env[1])

    async def recover():
        return 0

    prefix = "c07:" + tmp_path.name
    async with owned_redis(tmp_path / "redis") as (redis, port, pid):
        proxy = RedisFaultProxy(port)
        await proxy.start()
        client = Redis(host="127.0.0.1", port=proxy.port, decode_responses=True, socket_connect_timeout=1, socket_timeout=1, retry=Retry(NoBackoff(), 0))

        def make_publisher():
            return CommittedEventPublisher(session_factory=item.env[1], candidate_pointers=reader.candidate_pointers, load_committed=reader.load_committed, redis_client=client, key_prefix=prefix, recover_seals=recover)

        publisher = make_publisher()
        try:
            if fault == "lost_response":
                proxy.lost_response = True
            else:
                async with item.engine.begin() as connection:
                    await connection.execute(text("CREATE FUNCTION c07_reject_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'actual C07 ACK failure'; END $$"))
                    await connection.execute(text("CREATE TRIGGER c07_ack_fault BEFORE UPDATE ON fleet_event_outbox FOR EACH ROW EXECUTE FUNCTION c07_reject_ack()"))
            with pytest.raises(Exception):
                await publisher.publish_once()
            key = f"{prefix}:{identity.run_id}:{identity.attempt_id}"
            first = await redis.xrange(key)
            assert len(first) == 1 and first[0][0].decode() == f"{committed[0]['seq']}-0"
            assert proxy.command_receipts[-1]["accepted"] and proxy.command_receipts[-1]["command"] == "EVAL"
            if fault == "lost_response":
                assert proxy.dropped_replies == 1 and proxy.command_receipts[-1]["reply_dropped"]
            else:
                async with item.engine.begin() as connection:
                    await connection.execute(text("DROP TRIGGER c07_ack_fault ON fleet_event_outbox"))
            async with item.engine.connect() as connection:
                assert (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox WHERE published_at IS NOT NULL"))).scalar_one() == 0
            await publisher.stop()
            publisher = make_publisher()  # Restarted delivery owner retains only DB/Redis receipts.
            assert await publisher.publish_once()
            assert len(await redis.xrange(key)) == 1
            assert await publisher.publish_once()
            delivered = await redis.xrange(key)
            assert [entry[0].decode() for entry in delivered] == [f"{record['seq']}-0" for record in committed]
            async with item.engine.connect() as connection:
                assert (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox WHERE published_at IS NOT NULL"))).scalar_one() == 2
            (tmp_path / "delivery.json").write_text(json.dumps({"fault": fault, "redis_pid": pid, "resp": proxy.command_receipts, "delivered_ids": [entry[0].decode() for entry in delivered]}, indent=2))
        finally:
            await publisher.stop()
            await client.aclose()
            await proxy.close()


@pytest.mark.asyncio
async def test_real_redis_lua_keeps_bigint_sequence_precision(tmp_path):
    from deerflow_ecs_fleet.event_bridge import _COMMITTED_HINT

    async with owned_redis(tmp_path / "redis") as (redis, _, _):
        key = "c07:bigint:" + tmp_path.name
        seqs = [9007199254740992, 9007199254740993, 9223372036854775807]
        for seq in seqs:
            assert await redis.eval(_COMMITTED_HINT, 2, key, key + ":highwater", str(seq), "run", "attempt", 256, 86400) == 1
            assert await redis.eval(_COMMITTED_HINT, 2, key, key + ":highwater", str(seq), "run", "attempt", 256, 86400) == 0
        assert [entry[0].decode() for entry in await redis.xrange(key)] == [f"{seq}-0" for seq in seqs]


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown", ["cancel_owned_task", "socket_timeout"])
async def test_actual_blocked_delivery_settles_task_and_releases_sql_lock(checkpoint_owner, tmp_path, shutdown):
    import asyncio
    import time

    from deerflow_ecs_fleet.event_bridge import CommittedEventPublisher
    from deerflow_ecs_fleet.persistence.outbox import EventOutbox
    from redis.asyncio import Redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    from app.fleet.events import FleetEventParticipant, FleetStreamReader, RemoteStreamIdentity
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    from .c07_integration_fixture import bounded_wait

    item = checkpoint_owner
    async with item.engine.begin() as connection:
        await connection.run_sync(RunEventRow.__table__.create, checkfirst=True)
    capability = FleetMutationCapability(item.identity, item.spec)
    identity = RemoteStreamIdentity.from_context(capability.context)
    store = DbRunEventStore(item.env[1], mutation_capability=capability, transaction_participant=FleetEventParticipant(identity=identity, spec=item.spec, capability=capability, outbox=EventOutbox()))
    event = dict(thread_id=identity.thread_id, run_id=identity.run_id, event_type="stream.frame", category="stream", content={"event": "values", "data": "tail"})
    with remote_mutation_scope(capability.context):
        await store.put_batch([event, event])
    reader = FleetStreamReader(item.env[1])

    async def recover():
        return 0

    prefix = "c07:" + tmp_path.name
    async with owned_redis(tmp_path / "redis") as (redis, port, pid):
        proxy = RedisFaultProxy(port)
        await proxy.start()
        proxy.hold_delivery_reply = True
        client = Redis(host="127.0.0.1", port=proxy.port, decode_responses=True, socket_connect_timeout=1, socket_timeout=1, retry=Retry(NoBackoff(), 0))
        publisher = CommittedEventPublisher(session_factory=item.env[1], candidate_pointers=reader.candidate_pointers, load_committed=reader.load_committed, redis_client=client, key_prefix=prefix, recover_seals=recover)
        task = None
        try:
            started = time.monotonic()
            if shutdown == "cancel_owned_task":
                await publisher.start()
                task = publisher._task
                await asyncio.wait_for(proxy.delivery_reply_held.wait(), 2)
                await asyncio.wait_for(publisher.stop(), 2)
                assert task.done() and publisher._task is None
            else:
                with pytest.raises(Exception):
                    await publisher.publish_once()
                assert proxy.delivery_reply_held.is_set()
            elapsed = time.monotonic() - started
            assert elapsed < 2.5
            async with item.engine.begin() as connection:
                assert (await connection.execute(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))"), {"key": "fleet:delivery:" + identity.run_id})).scalar_one() is True
                assert (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox WHERE published_at IS NOT NULL"))).scalar_one() == 0
            key = f"{prefix}:{identity.run_id}:{identity.attempt_id}"
            assert len(await redis.xrange(key)) == 1
        finally:
            await publisher.stop()
            await client.aclose()
            await proxy.close()
        # A real new owned task resumes from the durable receipts.
        direct = Redis(host="127.0.0.1", port=port, decode_responses=True, socket_connect_timeout=1, socket_timeout=1)
        restarted = CommittedEventPublisher(session_factory=item.env[1], candidate_pointers=reader.candidate_pointers, load_committed=reader.load_committed, redis_client=direct, key_prefix=prefix, recover_seals=recover)
        await restarted.start()
        replacement_task = restarted._task
        try:

            async def acknowledged():
                async with item.engine.connect() as connection:
                    return (await connection.execute(text("SELECT count(*) FROM fleet_event_outbox WHERE published_at IS NOT NULL"))).scalar_one() == 2

            await bounded_wait(acknowledged, seconds=5)
            assert len(await redis.xrange(key)) == 2
        finally:
            await restarted.stop()
            await direct.aclose()
        assert replacement_task.done()
        (tmp_path / "lifecycle.json").write_text(
            json.dumps({"shutdown": shutdown, "redis_pid": pid, "blocked_duration": elapsed, "original_task_done": task.done() if task else None, "replacement_task_done": replacement_task.done(), "resp": proxy.command_receipts}, indent=2)
        )


@pytest.mark.asyncio
async def test_trusted_per_run_heads_exclude_more_than_limit_corrupt_launch_identities(checkpoint_owner):
    """Corruption fixture drops only its owned schema FK; no execution or ACK is forged."""
    from deerflow_ecs_fleet.launch_spec import LaunchSpec

    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    from .test_c07_stream_reader import original_stream

    item = checkpoint_owner
    capability, identity, producer, _, reader = await original_stream(item)
    with remote_mutation_scope(capability.context):
        for index in range(70):
            await producer.publish(identity.run_id, "values", {"index": index})
    async with item.engine.begin() as connection:
        await connection.execute(text("ALTER TABLE fleet_run_placements DROP CONSTRAINT fk_fleet_placement_launch_identity"))
        originals = {
            table: (await connection.execute(text("SELECT row_to_json(t) FROM " + table + " t LIMIT 1"))).scalar_one() for table in ("runs", "fleet_launch_specs", "fleet_attempts", "fleet_run_placements", "run_events", "fleet_event_outbox")
        }

        async def clone(table, changes):
            value = originals[table] | changes
            await connection.execute(text("INSERT INTO " + table + " SELECT * FROM json_populate_record(NULL::" + table + ",CAST(:value AS json))"), {"value": json.dumps(value)})

        for index in range(66):
            run = ("000-corrupt-" if index < 65 else "zzz-valid-") + str(index)
            attempt = "c07-clone-attempt-" + str(index)
            spec_id = "c07-clone-spec-" + str(index)
            spec = LaunchSpec.model_validate(originals["fleet_launch_specs"]["payload"]).model_copy(update={"run_id": run})
            payload = spec.model_dump(mode="json")
            digest = spec.payload_digest()
            await clone("runs", {"run_id": run, "status": "success"})
            await clone("fleet_launch_specs", {"id": spec_id, "run_id": run, "payload": payload, "payload_digest": digest})
            await clone("fleet_attempts", {"id": attempt, "run_id": run, "launch_spec": originals["fleet_attempts"]["launch_spec"] | {"launch_spec": payload}})
            generation = identity.generation + (index < 65)
            await clone("fleet_run_placements", {"run_id": run, "active_attempt_id": attempt, "launch_spec_ref": spec_id, "generation": generation})
            event_id = originals["run_events"]["id"] + 1000 + index
            seq = 1000 + index
            await clone("run_events", {"id": event_id, "run_id": run, "seq": seq})
            await clone("fleet_event_outbox", {"run_id": run, "attempt_id": attempt, "generation": generation, "launch_spec_digest": digest, "event_id": event_id, "seq": seq})
        from sqlalchemy.ext.asyncio import AsyncSession

        async with AsyncSession(bind=connection) as session:
            pointers = await reader.candidate_pointers(session)
        assert len(pointers) == 2, "invalid launch identity prefixes must be excluded before limit; select one head per valid run"
        assert {pointer.run_id for pointer in pointers} == {identity.run_id, "zzz-valid-65"}
        assert next(pointer.seq for pointer in pointers if pointer.run_id == identity.run_id) == 1
        assert await connection.scalar(text("SELECT count(*) FROM fleet_event_outbox WHERE published_at IS NOT NULL")) == 0


@pytest.mark.asyncio
async def test_schema_legal_retained_pointers_do_not_block_current_head(checkpoint_owner):
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    from .test_c07_stream_reader import original_stream

    item = checkpoint_owner
    capability, identity, producer, _, reader = await original_stream(item)
    with remote_mutation_scope(capability.context):
        for index in range(70):
            await producer.publish(identity.run_id, "updates", {"index": index})
    async with item.engine.begin() as connection:
        await connection.execute(text("DELETE FROM run_events WHERE run_id=:run AND seq<=65"), {"run": identity.run_id})
        assert await connection.scalar(text("SELECT count(*) FROM fleet_event_outbox")) == 70
    async with item.env[1]() as session:
        pointers = await reader.candidate_pointers(session)
        assert len(pointers) == 1 and pointers[0].run_id == identity.run_id and pointers[0].seq == 66
        assert await reader.load_committed(pointers[0]) is not None
        assert await session.scalar(text("SELECT count(*) FROM fleet_event_outbox WHERE published_at IS NOT NULL")) == 0


@pytest.mark.asyncio
async def test_two_real_publishers_serialize_original_run_head(checkpoint_owner, tmp_path):
    import asyncio
    from uuid import uuid4

    from deerflow_ecs_fleet.event_bridge import CommittedEventPublisher
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    from .c07_integration_fixture import bounded_wait
    from .test_c07_stream_reader import original_stream

    item = checkpoint_owner
    capability, identity, producer, _, reader = await original_stream(item)
    with remote_mutation_scope(capability.context):
        await producer.publish(identity.run_id, "values", {"index": 1})
        await producer.publish(identity.run_id, "values", {"index": 2})

    application = "c07-competing-" + uuid4().hex
    async with item.engine.connect() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))
    competing_engine = create_async_engine(item.engine.url, connect_args={"server_settings": {"search_path": schema, "application_name": application}})
    competing_sf = async_sessionmaker(competing_engine, expire_on_commit=False)
    waiters = []

    async def recover():
        return 0

    async with owned_redis(tmp_path / "redis") as (redis, port, pid):
        proxy = RedisFaultProxy(port)
        await proxy.start()
        proxy.hold_delivery_reply = True
        client = Redis(host="127.0.0.1", port=proxy.port, socket_connect_timeout=1, socket_timeout=2)
        publishers = [
            CommittedEventPublisher(session_factory=competing_sf, candidate_pointers=reader.candidate_pointers, load_committed=reader.load_committed, redis_client=client, key_prefix="c07:competition", recover_seals=recover)
            for _ in range(2)
        ]
        tasks = []
        try:
            tasks.append(asyncio.create_task(publishers[0].publish_once()))
            await asyncio.wait_for(proxy.delivery_reply_held.wait(), 1)
            tasks.append(asyncio.create_task(publishers[1].publish_once()))

            async def blocked_on_real_lock():
                async with item.engine.connect() as connection:
                    rows = (
                        (
                            await connection.execute(
                                text("SELECT pid,query,wait_event FROM pg_stat_activity WHERE datname=current_database() AND application_name=:application AND wait_event='advisory' AND query LIKE '%pg_advisory_xact_lock%'"),
                                {"application": application},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    waiters[:] = [dict(row) for row in rows]
                    return bool(waiters)

            await bounded_wait(blocked_on_real_lock, seconds=1)
            assert not tasks[1].done()
            key = f"c07:competition:{identity.run_id}:{identity.attempt_id}"
            assert [entry[0].decode() for entry in await redis.xrange(key)] == ["1-0"]
            proxy.release_delivery_reply.set()
            assert await asyncio.wait_for(asyncio.gather(*tasks), 2) == [True, True]
            assert [entry[0].decode() for entry in await redis.xrange(key)] == ["1-0", "2-0"]
            async with item.engine.connect() as connection:
                assert await connection.scalar(text("SELECT count(*) FROM fleet_event_outbox WHERE published_at IS NOT NULL")) == 2
            (tmp_path / "competition.json").write_text(
                json.dumps({"redis_pid": pid, "observed_advisory_wait": bool(waiters), "waiters": waiters, "delivered_ids": [entry[0].decode() for entry in await redis.xrange(key)], "resp": proxy.command_receipts}, indent=2)
            )
        finally:
            proxy.release_delivery_reply.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await client.aclose()
            await proxy.close()
            await competing_engine.dispose()
