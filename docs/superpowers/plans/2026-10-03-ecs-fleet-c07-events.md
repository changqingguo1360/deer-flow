# ECS Fleet C07 Committed Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replay committed remote Agent SSE frames in durable order after Redis failure, deduplicate delivery, exclude stale attempts, and recover terminal END after proven physical stop without starting another Agent.

**Architecture:** The existing run_agent remains the only execution loop. Its remote StreamBridge adapter persists each actual serialized frame through DbRunEventStore and a neutral participant inserts a private Fleet outbox pointer in that same fenced transaction; Gateway replay reads PostgreSQL authoritatively and Redis carries committed delivery/wake notifications. A narrow live-writer seal or independently trusted physical-stop recovery seal closes the durable stream after its tail; Local streaming and closed remote activation retain their existing behavior.

**Tech Stack:** Python asyncio, SQLAlchemy AsyncSession/asyncpg, independent Fleet Alembic migrations, Redis asyncio, FastAPI StreamingResponse, existing LangGraph executor and scripted Linux acceptance runner, pytest.

---

## Execution scope and prerequisites

Work only in `/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs`, branch `feature/personal-agent-ecs`. Read-only audited HEAD was `a78c7c962ad0cc0a9cefb6823c1e8f21d907457b`. The shared default cwd is not this checkout. This document is planning only; production/test/doc changes, PG, Docker, tests and commits require root implementation authorization. All later mutations/executions in this checkout require `exec_command(sandbox_permissions="require_escalated", justification="按已批准 C07 计划在专用 worktree 实施并验证持久事件，不操作其他项目或未知资源")`.

Root owns exclusive PG/Docker coordination. Wait for its actual test URI/image handoff before any runtime command. Never inspect/kill historical PID70755, schemas1b0d/f829/ad1 or unrelated assetforge3 containers. C07 creates no admission routing, continuation, checkpoint/artifact restore, unknown recovery, new Agent execution loop or activation override. Do not mark OpenSpec 7.1–7.4 during planning. One explicit C07 slice commit follows root acceptance; this overrides the skill's frequent-commit default.

Existing real fixtures: `backend/tests/fleet/conftest.py::fleet_database` reads TEST_POSTGRES_URI and creates its own random schema; `c04_integration_fixture.py::control_database` creates an owned Docker PG container; `node_server` creates actual TCP HTTP. Existing image builder is `backend/tests/fleet/build_c04_runner_image.py --tag ...`, not c04_runner_image.py. Container gates use FLEET_TEST_CONTAINERS=1 and FLEET_AGENT_TEST_IMAGE. A dedicated native `/opt/homebrew/bin/redis-server` bound to127.0.0.1 on an owned random port/directory can provide real outage testing without a Redis Docker image; retain its actual owned PID for shutdown and restart only that instance. Redis tests use DEER_FLOW_TEST_REDIS_URL; C07 fixture additionally accepts TEST_REDIS_URL, with both set to the same dedicated URL. Every test uses a random key prefix, deletes only its keys, and never FLUSHDB. Missing runtime prerequisites are failed acceptance preflight, never counted as behavior RED or GREEN.

## Source facts that determine the implementation

- `runtime/runs/worker.py` publishes metadata at approximately line824, graph frames through `_publish_stream_item` around2602, and END around1471. Namespace is part of SSE event name: `values|<namespace>`; do not flatten it or encode namespace into VARCHAR(32) event_type.
- RunJournal callback records are separate from actual SSE. `run.end` is opaque graph output, may precede rollback/error/tail, and can be absent on preflight/error/interrupted paths. It is never sufficient alone to close a stream.
- DbRunEventStore pre/post fences hold the actual session through commit; seq is thread-global under advisory lock, not contiguous per run or per transport. Generic events.put/batch/singleton are active-only under C06.
- Worker stages terminal when event_store exists, flushes journal/receipts/checkpoints, persists terminal, completes bookkeeping/observers, clears finalizing, then calls publish_end even after ownership loss. A remote END adapter must deny stale authority. Local behavior stays unchanged.
- `FleetRunOwnership.stopped` already holds task → core run → placement → node → reservation → attempt locks. Valid exit/live lease + terminal core produces accepted placement state and releases stopped reservation. Unknown/quarantined, expired or non-exit stop becomes unknown, even with stopped_at present. active_attempt_id is retained in both cases; there is no accepted_attempt_id column.
- `RunRecord.finalizing` is process-local (`RunManager.set_finalizing` around1104); RunRow has no finalizing column. Live seal binds the original registered record; trusted physical-stop recovery uses proven stop instead of a nonexistent durable flag.
- Gateway terminal-missing shortcut and orphan-heartbeat shortcut currently can synthesize END without durable tail. Both must be remote-aware, including wait_for_run_completion.
- Cursor validation inside an async generator happens after HTTP headers. Both routers must prepare remote subscriptions before constructing StreamingResponse.

## Exact file map

Create:

1. `backend/packages/harness/deerflow/runtime/events/transactions.py`: neutral RunEventTransactionParticipant protocol, only insert on caller's session; no app/Fleet imports.
2. `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/outbox.py`: private pointer/seal ORM repository, ordered candidate selection and idempotent ACK; no core ORM model imports.
3. `backend/packages/ecs-fleet/deerflow_ecs_fleet/event_bridge.py`: publisher lifecycle and committed Redis notification transport, owned tasks, no Agent loop.
4. `backend/packages/ecs-fleet/deerflow_ecs_fleet/migrations/versions/f0008_event_outbox.py`: append after f0007_agents; no historical migration rewrites.
5. `backend/app/fleet/events.py`: actual host participant, producer StreamBridge adapter, trusted placement/core joins, immutable historical reader, prepared remote subscription and narrow live/recovery seals.
6. `backend/tests/fleet/test_c07_remote_agent_runtime.py`: public behavioral contract, real PG/HTTP/Redis failures and installed runner gates.
7. `backend/tests/fleet/c07_integration_fixture.py`: actual resource ownership, Redis TCP fault proxy, fixture assembly using existing C04 admission/provider/runtime, event collection and process receipts.
8. `docs/ecs-fleet-c07-acceptance.md`: actual evidence after GREEN, written only at acceptance stage.

Modify:

- `backend/packages/harness/deerflow/runtime/events/store/db.py`: optional same-session participant invoked for actual inserted/flushed rows before postwrite fence/commit.
- `backend/packages/harness/deerflow/runtime/events/catalog.py`: fixed `stream.frame` definition, category `stream`; include in fixed catalog. No category/value rename of existing events.
- `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/models.py`: private OutboxRow and StreamSealRow on FleetBase only.
- `backend/app/fleet/mutation.py`: explicit `stream.seal` capability, terminal-only checks; never add events.* to terminal set.
- `backend/app/fleet/runner_context.py`: one event store shared by journal and durable producer adapter, bind original identity/participant, return adapter as AgentEnvironment.bridge. Raw Redis resource may be available privately but producer cannot publish tokens to it.
- `backend/app/fleet/ownership.py`: recover seal in the same stopped transaction after accepted stopped/released state; before returning response.
- `backend/app/gateway/deps.py`: install host multiplexing reader/publisher only after Fleet migration/ready; own teardown before start; preserve underlying Local bridge resource lifetime.
- `backend/app/gateway/services.py`: `prepare_sse_subscription` and remote-aware terminal/orphan shortcuts, reused by wait.
- `backend/app/gateway/routers/thread_runs.py`: prevalidate stream/join/cancel-stream response surfaces before StreamingResponse.
- `backend/app/gateway/routers/runs.py`: same prevalidation for stateless stream.
- `backend/tests/fleet/build_c04_runner_image.py`: export existing optional redis extra for actual Redis acceptance image; B image not rebuilt.
- `backend/tests/fleet/fixtures/c04-runner/Dockerfile`: install newly added C07 fixture only if container test needs it; retain actual source/wheel bootstrap verification.
- `contracts/run_event_stream_contract.json`, `backend/docs/RUN_EVENT_STREAM.md`, `backend/tests/test_run_event_stream_contract.py`: additive transport definition and payload/cursor/limits contract.
- `backend/tests/test_stream_bridge.py`: Local protocol parity and actual Redis neighbors.
- `README.md`, `backend/AGENTS.md`, `backend/packages/harness/deerflow/runtime/AGENTS.md`, `backend/app/gateway/AGENTS.md`, `docs/ecs-fleet-development.md`: factual user/developer capability update at completion.
- `openspec/changes/add-ecs-remote-agent/tasks.md`: only after full runtime evidence/root acceptance, exactly7.1–7.4.

No change to core RunEventRow/Base tables or to run_agent graph execution loop is planned. Its existing publish API is the injection point. All Fleet models stay private. If a writer operation is discovered after durable terminal other than END, pause that implementation path and report its concrete call site to root rather than granting generic terminal transport writes.

## Locked interfaces and durable schema

Neutral participant:

```python
from typing import Protocol
from sqlalchemy.ext.asyncio import AsyncSession

class RunEventTransactionParticipant(Protocol):
    async def insert(self, session: AsyncSession, *, event_id: int, record: dict) -> None: ...
```

DbRunEventStore constructor adds `transaction_participant: RunEventTransactionParticipant | None = None`. `put`, `put_batch` and newly inserted `put_if_absent` use this order under their existing transaction:

```python
session.add(row)
await session.flush()
if self._transaction_participant is not None:
    await self._transaction_participant.insert(
        session, event_id=row.id, record=self._row_to_dict(row)
    )
# Existing _post_write_fence flush/revalidation follows; begin commits last.
```

Batch inserts all rows, flushes once, then invokes participant for each row in assigned seq order. Existing singleton return does not reinsert a pointer. Participant ignores non-stream.frame rows and validates original private context for transport rows. Participant exceptions roll back both tables. Participant never performs Redis I/O or commits. Its record content is the already persisted JSON-normalized representation, so publisher cannot access different raw tokens.

Private tables, exact SQL types/constraints:

```sql
CREATE TABLE fleet_event_outbox (
  run_id VARCHAR(64) NOT NULL REFERENCES fleet_run_placements(run_id),
  attempt_id VARCHAR(64) NOT NULL REFERENCES fleet_attempts(id),
  generation INTEGER NOT NULL CHECK (generation > 0),
  user_id VARCHAR(64) NOT NULL, thread_id VARCHAR(64) NOT NULL,
  launch_spec_digest VARCHAR(71) NOT NULL,
  event_id BIGINT NOT NULL, seq BIGINT NOT NULL CHECK (seq > 0),
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, attempt_id, seq),
  UNIQUE (event_id), UNIQUE (thread_id, seq)
);
CREATE INDEX ix_fleet_event_outbox_pending
  ON fleet_event_outbox (run_id, attempt_id, seq) WHERE published_at IS NULL;
CREATE TABLE fleet_stream_seals (
  run_id VARCHAR(64) NOT NULL REFERENCES fleet_run_placements(run_id),
  attempt_id VARCHAR(64) NOT NULL REFERENCES fleet_attempts(id),
  generation INTEGER NOT NULL CHECK (generation > 0),
  user_id VARCHAR(64) NOT NULL, thread_id VARCHAR(64) NOT NULL,
  launch_spec_digest VARCHAR(71) NOT NULL,
  last_seq BIGINT NOT NULL CHECK (last_seq >= 0),
  core_status VARCHAR(16) NOT NULL
    CHECK (core_status IN ('success','error','interrupted','timeout')),
  source VARCHAR(16) NOT NULL CHECK (source IN ('writer','physical_stop')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, attempt_id)
);
```

event_id deliberately has no foreign key into host metadata: host tables/models remain independent; read joins verify event_id/thread/run/user/seq/type each time and missing row never recreates history. Add immutable UPDATE trigger for identity/content fields of pointer/seal; outbox permits only published_at update. Unique pointer/event_id constraints prevent duplicate identity. Host-owned deletion must not let a private pointer resurrect deleted run_events. No raw token, bearer, model input or SSE payload in these private rows. Public run_events /events reads expose transport content/metadata, so those fields must never include token_stamp, node_session_id, host handles or bound capabilities; only the opaque attempt identifier in the cursor is public.

Host types/signatures to define in events.py:

```python
@dataclass(frozen=True)
class RemoteStreamIdentity:
    run_id: str
    thread_id: str
    user_id: str
    attempt_id: str
    generation: int
    launch_spec_digest: str

@dataclass(frozen=True)
class PreparedRemoteSubscription:
    identity: RemoteStreamIdentity | None
    after_seq: int

class FleetEventParticipant:
    def __init__(self, *, identity, spec, capability, outbox): ...
    async def insert(self, session, *, event_id, record): ...

class FleetProducerBridge(StreamBridge):
    def __init__(self, *, event_store, identity, spec, capability, seals, manager): ...
    async def publish(self, run_id, event, data): ...
    async def publish_end(self, run_id): ...
    async def cleanup(self, run_id, *, delay=0): ...
    def subscribe(self, run_id, *, last_event_id=None, heartbeat_interval=None): ...

class FleetGatewayBridge(StreamBridge):
    supports_cross_process = True
    def __init__(self, *, local_bridge, reader, publisher): ...
    async def prepare(self, record, last_event_id): ...
    async def is_remote(self, run_id): ...
    def subscribe_prepared(self, run_id, prepared, *, heartbeat_interval=None): ...
    async def stream_exists(self, run_id): ...
    async def publish(self, run_id, event, data): ...
    async def publish_end(self, run_id): ...
    def subscribe(self, run_id, *, last_event_id=None, heartbeat_interval=None): ...
    async def cleanup(self, run_id, *, delay=0): ...

class FleetStreamReader:
    def __init__(self, session_factory): ...
    async def prepare(self, record, cursor): ...
    async def page(self, prepared, *, limit=128): ...
    async def seal(self, identity): ...

class FleetStreamSeals:
    def __init__(self, session_factory): ...
    async def writer_seal(self, *, identity, spec, capability, original_record): ...
    async def recover_locked(self, session, *, run, placement, attempt, reservation): ...
    async def recover_accepted_batch(self, *, limit=64): ...
```

Producer subscribe is explicitly unsupported (raise RuntimeError) because it is an execution-only adapter; cleanup is a no-op for durable DB rows. Gateway publish/end delegate Local only and reject remote run publication without the original producer adapter. Gateway cleanup delegates Local, and clears only its optional Redis wake key for remote; never deletes DB events/outbox/seals after60s.

Publisher interface in optional event_bridge.py:

```python
class CommittedEventPublisher:
    def __init__(self, *, outbox, load_committed, redis_client, key_prefix,
                 recover_seals, poll_interval=0.25): ...
    async def start(self): ...
    async def publish_once(self): ...
    async def stop(self): ...
```

`load_committed(pointer)` is a host callback returning exact validated event data from SQL, or None for deleted/stale identity. The optional publisher has no core RunRow import. Redis Lua transport stores a monotonic high-watermark and XADDs committed seq/attempt notification atomically only for seq greater than that watermark; repeating after lost response returns already-delivered. Hold a PostgreSQL per-run advisory delivery lock during ordered send+ACK, independent of execution authority; no execution lease locks are held across Redis. Retry a failed head before later seq. Redis messages are wake hints/committed row identities, not the SSE source of truth; duplicated/lost/trimmed hints cannot duplicate or lose client frames. ACK updates only private published_at, never runs/placements/attempt leases. Publisher Redis client sets socket_connect_timeout=1.0 and socket_timeout=1.0 seconds and publish_once has an explicit asyncio timeout of3 seconds; advisory delivery TX always rolls back/releases after timeout/cancellation. DBpool cannot remain occupied on an unbounded network wait. Publisher stop cancels and joins its original owned task, then SQL/Redis resources unwind; do not discard an unsettled task. Redis absent uses DB-only operation and a bounded poll interval at least0.25 seconds, without attempting publish or logging repeated warnings; outage logging occurs on availability transitions with capped retry backoff up to5 seconds, while DB reader heartbeats remain independent.

## Task1: Baseline real behavior RED

**Files:** create the C07 test/fixture files above; existing production untouched.

- [x] Write resource preflight fixture with `assert os.environ.get('TEST_POSTGRES_URI')`, `assert os.environ.get('TEST_REDIS_URL')`, actual SELECT1 and Redis PING. On explicit container gate assert image resolves to a real digest and installed source matches. Do not skip C07 tests.
- [x] Implement actual Redis TCP fault proxy: asyncio.start_server forwards RESP bytes to the dedicated Redis; toggled stop drops upstream connections; toggled lost_response forwards one accepted write, discards reply, and closes downstream. Count intercepted actual Redis command/replies, not monkeypatched Redis methods. Random owned key prefixes and sockets have finally cleanup.
- [x] Assemble `C07Scenario` fixture by adapting C04 actual claim/start/AgentRunner input/resources and `node_server`; use real existing scripted model/lead graph and tool execution. Expose methods `start_once()`, `wait_for_committed_model_event()`, `disconnect_redis()`, `restore_redis()`, `join(cursor=None)`, `wait_for_runner_exit()`, `acknowledge_actual_stop()`, `delete_owned_redis_keys()`, `read_committed_frames()`, `start_receipts()`, `last_received_id()`, `release_model_barrier()` and `expected_tail_names`. `release_model_barrier()` releases the actual scripted provider gate only after independent SQL evidence and Redis disconnect are established; it cannot call another runner. `expected_tail_names` is the set of event names emitted by the deterministic fixture after that gate, derived from its scripted graph/provider configuration and checked against baseline actual successful Local output (includes root messages/values and a namespaced subgraph values frame). These methods operate actual resources; no copied run_agent loop. Runner count receipts include actual container/process ID and entry start; count execution starts, not model token callbacks.
- [x] Write the first baseline-safe test, using only existing public production surfaces and SQL rows. The fixture chooses actual current runner bridge; it never imports a missing C07 API on baseline. It runs one Agent to a model-event barrier, confirms independent committed journal evidence, cuts real Redis, releases the model barrier, waits actual exit/stop, restores Redis, deletes only retained keys and reconnects through actual HTTP. Assert received frames contain the committed remote graph tail and end. Baseline fails on missing replay/tail or Redis-induced execution failure, never nonexistent table/API/import.

```python
@pytest.mark.asyncio
async def test_committed_remote_tail_reconnects_without_runner_restart(c07_scenario):
    s = c07_scenario
    await s.start_once()
    await s.wait_for_committed_model_event()
    cursor = s.last_received_id()
    await s.disconnect_redis()
    await s.release_model_barrier()
    await s.wait_for_runner_exit()
    await s.acknowledge_actual_stop()
    await s.restore_redis()
    await s.delete_owned_redis_keys()
    frames = await s.join(cursor=cursor)
    assert s.expected_tail_names <= {f.event for f in frames}
    assert frames[-1].event == 'end'
    assert len(await s.start_receipts()) == 1
```

- [x] Run only this test against baseline after exclusive runtime handoff: `PYTHONPATH=. uv run pytest tests/fleet/test_c07_remote_agent_runtime.py::test_committed_remote_tail_reconnects_without_runner_restart -vv --tb=short`. Save actual assertion failure and independent DB/HTTP/process receipt. If graph finishes before outage barrier, fix fixture synchronization; if setup fails, repair preflight and rerun rather than claim RED.
- [x] Add feature tests after production interfaces exist for duplicate ACK/outbox TX rollback; those constitute subsequent RED/GREEN cycles rather than claiming nonexistent outbox is baseline behavior.

## Task2: Neutral same-TX participant and private schema

**Files:** transactions.py, db.py, outbox.py, models.py, f0008_event_outbox.py, C07 tests.

- [x] Write a participant that inserts a marker then raises a deliberate test exception on the actual AsyncSession. Assert a separate session sees neither RunEventRow nor marker and the next valid thread seq has no committed gap. Also block after participant SQL until DB lease expires; assert existing postfence rolls back both tables. Use pytest.raises on the explicit error/OwnershipRejected and snapshot both actual tables.
- [x] Run targeted tests to observe real rollback/atomicity failure, then implement protocol/constructor/insertion order described above. Never alter pre/post C06 fence sequencing.
- [x] Implement private migration with the locked SQL and matching FleetBase models. revision='f0008_event_outbox', down_revision='f0007_agents'; downgrade raises drained-backup requirement consistently with f0007. Check actual Fleet migration head and independent host tables after upgrading a clean random schema.
- [x] Implement outbox.insert(session, identity, event_id, seq) using caller session, flush only. Check stream seal absence under the writer's existing placement/run lock; a sealed stream cannot append. Identity is derived from trusted bound context/spec, never event metadata.
- [x] Implement ACK as conditional update of exact `(run_id, attempt_id, seq, event_id)` with `published_at=coalesce(published_at,clock_timestamp())`. Same pointer ACK twice returns same row/time; wrong pointer ACK updates zero rows. Candidate query uses earliest unpublished row per run, ordered run/seq, then publisher advisory lock rechecks head.
- [x] Run actual PG tests for row/pointer atomicity, independent session visibility, duplicate ACK and seq interleaving with semantic journal rows. Require zero skips.

## Task3: Persist the actual serialized SSE producer and payload contract

**Files:** host events.py, runner_context.py, catalog.py, contract/docs/tests.

- [x] Write namespace/message/values/debug tests on actual producer surface. Check oversized debug rejects before any row/pointer/Redis command, UTF-8 byte counts not characters, and >10240-byte values/messages round-trip unchanged. Trace payload must not appear in metadata.
- [x] Add `STREAM_FRAME_EVENT = RunEventDefinition('stream.frame','stream')` to FIXED_RUN_EVENT_DEFINITIONS and contract entry. Envelope is exactly `{'event': str, 'data': JSON value}`; metadata carries only version/mode/namespace identity-neutral protocol fields. Private attempt identity lives in outbox.
- [x] Implement producer normalization and bound write:

```python
async def publish(self, run_id, event, data):
    if run_id != self.spec.run_id:
        raise OwnershipRejected('Remote stream target rejected')
    encoded = json.dumps(data, default=str, ensure_ascii=False, separators=(',', ':'))
    envelope = {'event': event, 'data': json.loads(encoded)}
    envelope_bytes = json.dumps(envelope, default=str, ensure_ascii=False).encode('utf-8')
    mode = event.partition('|')[0]
    if mode in {'events', 'debug'} and len(envelope_bytes) > self.max_trace_content:
        raise ValueError('Remote trace frame exceeds run_events.max_trace_content')
    await self.event_store.put(
        thread_id=self.spec.thread_id, run_id=run_id,
        event_type='stream.frame', category='stream',
        content=envelope,
        metadata={'stream_version': 1},
    )
```

Define max_trace_content from original private run_events config in constructor; trace limit measures the complete persisted transport content envelope in UTF-8, including event name/data/envelope overhead; document this explicitly. The check uses the actual DbRunEventStore._content_to_db serialization (`json.dumps(default=str,ensure_ascii=False)` default separators), so its persisted envelope overhead is counted without changing Local serialization. Other payloads have their existing producer limits and no new universal10240 cap. Actual `debug` and any legacy `events` trace-derived frame obey this rejection; `tasks/checkpoints/custom` retain their existing payload contracts and this classification is explicit in docs. No raw string truncation, oversized JSON chunking or metadata side channel. Publish does no Redis calls.
- [x] Build the event store once in runner_context with both mutation_capability and participant, give that same object to RunContext and producer. AgentEnvironment.bridge becomes producer; existing worker receives unchanged publish API. No shadow runner.
- [x] Run targeted PG producer tests, contract catalog tests and harness-boundary tests. Confirm ordinary Local event stores construct without participant and retain signatures/behavior.

## Task4: Narrow END writer seal and trusted physical-stop recovery

**Files:** events.py, mutation.py, ownership.py, outbox.py, C07 tests.

- [x] Write tests where generic events.put/batch remain rejected after terminal; live stream.seal alone may succeed after finalizing=false. Ownership_lost, expired lease, stale token, wrong target, pending/running core, finalizing=true and changed terminal status must not create seal. Test the same clock-after-wait and precommit-expiry windows as C06.
- [x] Add `stream.seal` to explicit active-operation catalog and terminal-operation allowance, then require the actual guard return SQL row is terminal. Separately require the bound original RunRecord finalizing is False and ownership_lost is False; never query a finalizing SQL column. Its only MutationTarget is original user/thread/run with status matching core. It cannot be a payload/event write operation. The producer receives the existing manager from runner_context. On its first publish/end, obtain the actual registered record with `await manager.get(spec.run_id, user_id=spec.user_id, raise_on_store_error=True)`; capture that exact object and reject store_only/mismatched run/thread/user/owner or object replacement. Recheck the same object finalizing/ownership_lost/status before SQL and immediately before commit. writer_seal validates SQL authority pre and post on the same session, locks original placement, reads actual max accepted transport seq (zero allowed), inserts only private StreamSealRow and commits. Repeat exact seal returns existing record; status/digest/identity/high-water disagreement rejects.
- [x] Implement producer.publish_end by invoking writer_seal under the original ambient scope. If original ownership already rejected, no seal/Redis END is produced; preserve OwnershipRejected identity, do not generate successful completion. Cleanup remains no-op for DB.
- [x] Add recovery inside stopped() after release_stopped and flush, before return:

```python
if placement.state in {'succeeded','failed','cancelled','timed_out'}:
    await self.stream_seals.recover_locked(
        session, run=run, placement=placement,
        attempt=attempt, reservation=reservation,
    )
```

Initialize stream_seals once on host ownership, including standalone ownership fixtures. recover_locked is an independently trusted host operation; it does not call writer capability, accept attempt bearer as write authority, renew leases or alter terminal status. It requires exact run placement/launch immutable identity, active_attempt_id original, core terminal matching placement outcome, attempt.stopped_at present, outcome.stop_reason='exit', attempt state matching accepted terminal, and reservation released_at plus released state. No current task generation/current_run_id/live lease/node session requirement for historical accepted runs. The stopped path's prior authenticated physical proof and accepted transition is source authority. Unknown/quarantined or expired/non-exit never seal.
- [x] Physical stop proves there can be no subsequent tail writer. Recovery therefore does not read or require the dead process finalizing flag. For core-terminal accepted physical-stop evidence, seal the committed high-watermark even if the original process died during its process-local finalization. Never clear a core/Fleet flag or invent a durable finalizing column. Test controlled safe zero-exit after durable terminal before publish_end persistence, then actual daemon/container-observed stopped proof and accepted mapping. Additional phase coverage may keep the actual original record finalizing True while safely terminating after C06 settlement; recovery never consults that dead boolean. Never assume a SIGKILL/lease-expired/non-exit stop is accepted.
- [x] Implement startup/periodic `recover_accepted_batch(limit=64)` selecting accepted terminal placements lacking seal; each transaction locks task → run → placement → node → reservation → attempt in existing order and then rechecks all recovery predicates. Locking task is serialization only, not current generation authorization. Read the tail only after these locks so no earlier writer can commit after recovered seal. Unknown rows remain untouched. Idempotent unique seal handles concurrent stopped/scan transactions.
- [x] Test actual subprocess/container termination after core terminal but before seal, at a fixture-owned preseal gate, authentic node stopped HTTP, then new publisher/reader process replay tail and END. Also remove seal in a test-owned historical accepted fixture and prove startup scan recovery. The crash fixture closes/settles original graph/checkpoint/memory phases, waits at the real producer preseal call, and exits through a fixture-controlled zero-exit process path without writing seal. Actual daemon/container wait inspection supplies reason=exit, exit_code=0, process_ref and physical_stopped evidence while the original lease is still valid; assert the real HTTP stopped response maps to an accepted terminal state. If the actual observer reports non-exit/expired/unknown, the test must fail and the fixture must be corrected, never change SQL or relabel its receipt. No SQL-created physical stop counts as runtime proof; negative unit matrices may seed states explicitly and are labeled as such.

## Task5: Committed Redis publisher and restart delivery

**Files:** optional event_bridge.py, host events.py, deps.py, builder and C07 tests.

- [x] Write fault-proxy tests for Redis unavailable before send, XADD committed reply lost, private DB ACK lost, duplicate ACK, publisher process restart and Redis retention lost. Assert outbox row remains durable, emitted HTTP IDs unique/ordered, model/runner starts unchanged.
- [x] Implement CommittedEventPublisher owned lifecycle. load_committed joins outbox→run_events by actual event_id plus user/thread/run/seq/type and current per-run accepted attempt identity. It returns nothing for foreign/stale/deleted rows; stale pointer is not republished. Redis receives committed identity wake entry only after DB load. Outage catches Redis transport errors, leaves ACK absent, retries next poll; never lets a Redis error cancel Agent graph.
- [x] Use per-run DB advisory lock through delivery+ACK to serialize publishers; obtain it in a private delivery transaction, reread earliest pending row. This is not _FleetExecutionGuard and cannot authorize event creation. Transport Lua atomically verifies watermark, emits wake XADD with stable seq ID in an attempt-specific key and stores watermark; duplicate/lower already-delivered notification is idempotent. After Redis retention deletion, DB replay still works even if ACK already present; polling does not depend on republishing retained hints.
- [x] Wake payload uses run/attempt/seq only. Apply random prefix tests and configured existing Redis retention/TTL to wake transport. With Redis configured absent/unreachable, remote DB poll still provides stream correctness; Local bridge remains independently configured. Do not add Redis as a remote execution prerequisite.
- [x] Install publisher after Fleet migrations in deps.py, bind private reader/seals, register stop callback before start. Periodic/startup recovery uses trusted seal scanner above. Start it for ready Fleet even new admission flags closed. Stop task before closing raw Redis/engine. Producer lifetime doesn't own Gateway publisher.
- [x] For container Redis tests add `--extra redis` to existing builder export, build new C07 tag only after authorization; inspect actual image ID/source receipt and ensure B image unchanged.

## Task6: DB replay, stable strict cursor, pre-header HTTP validation

**Files:** events.py, services.py, deps.py, routers/thread_runs.py, routers/runs.py, C07 tests.

- [x] Write actual HTTP 400 tests with headers for malformed, foreign run, foreign attempt, future seq and syntactically valid nonexistent seq; assert JSON error and no text/event-stream response. Add valid reconnect/no-cursor tests and Local malformed/gap/disconnect neighbors.
- [x] Cursor format is `fleet.v1.<base64url(run_id) without padding>.<base64url(attempt_id) without padding>.<positive durable seq>`. Encode/decode canonically; bound header length1024, reject whitespace, signs, leading-zero seq, noncanonical base64, extra segments. Cursor seq must identify an actual accepted pointer for that immutable run/attempt. No cursor means after_seq=0. A cursor after last_seq/future or for an old attempt returns HTTP400 before iterator starts. Cursor zero is not emitted/accepted. Thread-global semantic gaps are not transport gaps.
- [x] `FleetStreamReader.prepare(record,cursor)` validates record.user_id/thread against actual core + placement + launch identities. Readability uses original per-run placement generation/digest and attempt's frozen launch spec. Live claimed/running and accepted terminal cases are readable; unknown/quarantined can replay already committed matching frames but cannot yield synthesized END. It never relies on current task.current_run_id/generation for historical accepted run, and never uses current lease/node session for reads. No-cursor queued run may prepare identity=None; poll until trusted original mapping appears, bind before first delivered frame. Subsequent mapping change cannot merge attempts: revalidate before every page; if bound mapping becomes stale, stop the async iterator without END so HTTP closes as an incomplete stream. Client reconnect must run the same pre-header cursor checks and old/foreign attempt cursor returns400; never merge attempts or describe this EOF as completion.
- [x] Implement bounded ordered SQL pages seq>after_seq, identity filter plus event_id join. Yield `StreamEvent(id=cursor(identity,row.seq), event=envelope['event'], data=envelope['data'])`. Advance only delivered seq, no Redis-derived ID/cursor. Repeat wake notifications do not yield duplicates. After an empty page read matching seal and verify cursor>=seal.last_seq; only then yield END_SENTINEL. For zero-frame seal, after_seq0 satisfies closure. Poll DB even during Redis outage; heartbeat at existing configured interval.
- [x] Add services preflight and route call before each StreamingResponse:

```python
async def prepare_sse_subscription(bridge, record, request):
    prepare = getattr(bridge, 'prepare', None)
    return await prepare(record, request.headers.get('Last-Event-ID')) if prepare else None

prepared = await prepare_sse_subscription(bridge, record, request)
return StreamingResponse(
    sse_consumer(bridge, record, request, run_mgr,
                 apply_on_disconnect=False, prepared_subscription=prepared),
    media_type='text/event-stream', headers=existing_headers,
)
```

`sse_consumer` gains optional prepared_subscription parameter and uses subscribe_prepared for remote; same Local generator/cursor remains. Validate cancel-stream cursor before executing a cancel action so bad cursor cannot mutate a run then400. stateless/new streaming routes prevalidate after admitted record exists; their admission stays existing behavior.
- [x] `_terminal_record_stream_missing` returns False for remote after actual trusted placement detection; no missing Redis shortcut. `_orphan_recovery_observed_after_heartbeat` gains bridge parameter (all callers updated) and returns False for remote: only durable seal closes it. wait_for_run_completion uses prepared remote subscription and same seal, preserving existing Local terminal/gap/disconnect semantics. Remote store_only observation never creates cancel-on-disconnect.
- [x] Test later task generation/current_run_id changes after accepted older run: old run reconnect still succeeds with immutable old placement cursor, while old writer cannot append or seal. Test old attempt pointer, foreign owner rows and Redis injected notification never leak frames. Root-only and namespaced values remain exact.

## Task7: Real GREEN evidence, neighbors, docs and accepted slice

**Files:** all implementation files, acceptance/report docs and OpenSpec7 only at end.

- [x] Before GREEN, freeze fresh C07 source/input/dependency/wheel manifest with SHA256 plus newly built image digest. Compare actual installed `/opt/deerflow/libexec_bootstrap.py` bytes and installed wheel modules against the frozen C07 source/wheel manifest; retain actual executed entry receipt. Baseline RED may reuse accepted C06v12 runtime, but GREEN may not reuse it as evidence of new C07 code. Tag alone is never an identity.
- [x] Run targeted native real PG/Redis C07 suite with prerequisite fixture failures enabled, then installed Linux C07 runner suite on new frozen image; record source HEAD/tree diff, dependency versions, image digest and container process ID. First acceptance container test must derive all three facts from observed DB/HTTP/process:

```python
ids = [f.id for f in frames if f.id]
seqs = [decode_cursor(f.id).seq for f in frames if f.id]
receipts = await scenario.start_receipts()
observed = {
    'ordered_unique_events': seqs == sorted(seqs) and len(ids) == len(set(ids)),
    'runner_starts': len(receipts),
    'terminal_end_recovered': frames[-1].event == 'end'
        and await scenario.original_process_exited()
        and await scenario.redis_keys_absent_before_join(),
}
assert observed['ordered_unique_events']
assert observed['runner_starts'] == 1
assert observed['terminal_end_recovered']
```

Define decode_cursor using the real production decoder. Persist observed receipts/result JSON in ignored test output directory with redacted URIs/tokens; acceptance document references actual commands/results, not hardcoded dictionaries. Report duplicate wake/ACK counts separately from client event count.
- [x] Run native neighbors C05/C06 primary mutations, relevant C06c cleanup/tracking and Local worker/stream contract tests. Run required installed-container neighbors using existing tests without rebuilding B. Missing/skipped required runtime case blocks acceptance; do not count optional platform rejection tests as container coverage.
- [x] Run backend `make test`, `make test-blocking-io`, `make format`, `make lint`; inspect format changes and avoid unrelated edits. Rerun affected tests only for actual new changes/failures. Check harness/app boundary, full contract sync and diff whitespace.
- [x] Update README and relevant AGENTS/Fleet development guide with committed transport, reader authority, trace rejection, strict remote cursor, accepted-only physical-stop END recovery and remaining fail-closed activation. Add actual acceptance evidence. Coordinate doc edits with root before touching shared docs.
- [x] Complete sequential SPEC → QUALITY reviews against frozen source/runtime evidence, address both review stages, then root runs independent whole-slice gates against the same actual inputs/wheels/image and source tree. No acceptance on implementer report alone. Submit source/runtime report to root for acceptance; no commit before root approves. Then stage exact C07 files, commit `feat(fleet): persist and replay committed remote agent events`, verify clean status and commit ID. Check OpenSpec7.1–7.4 only after actual task evidence and accepted slice; leave C08–C12 and B/C continuation unchecked.

## Actual commands for authorized execution

Run commands with workdir=`/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend` and require_escalated. Root supplies actual environment values privately, never print URIs/passwords or use shell interpolation to expose them. Confirm prerequisite values are present with boolean checks in Python, not echo. Export TEST_POSTGRES_URI, TEST_REDIS_URL, DEER_FLOW_TEST_REDIS_URL from root's handoff; the last two identify the same dedicated Redis instance.

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c07_remote_agent_runtime.py::test_committed_remote_tail_reconnects_without_runner_restart -vv --tb=short
PYTHONPATH=. uv run pytest tests/fleet/test_c07_remote_agent_runtime.py -k 'not installed_runner' -q --tb=short
PYTHONPATH=. uv run python tests/fleet/build_c04_runner_image.py --tag deerflow-c07-runner:local
docker image inspect --format '{{.Id}}' deerflow-c07-runner:local
FLEET_TEST_CONTAINERS=1 FLEET_AGENT_TEST_IMAGE=deerflow-c07-runner:local PYTHONPATH=. uv run pytest tests/fleet/test_c07_remote_agent_runtime.py -k installed_runner -vv --tb=short
PYTHONPATH=. uv run pytest tests/fleet/test_c05_remote_agent_runtime.py tests/fleet/test_c06_remote_agent_runtime.py tests/fleet/test_c06c_remote_tracking.py tests/fleet/test_c06c_remote_profiles.py tests/test_stream_bridge.py tests/test_run_event_stream_contract.py tests/test_harness_boundary.py tests/test_run_worker_delivery.py tests/test_run_worker_rollback.py -q --tb=short
FLEET_TEST_CONTAINERS=1 FLEET_AGENT_TEST_IMAGE=deerflow-c07-runner:local PYTHONPATH=. uv run pytest tests/fleet/test_c04_remote_agent_runner.py tests/fleet/test_c05_remote_agent_runtime.py tests/fleet/test_c06_remote_agent_runtime.py -q --tb=short
make test
make test-blocking-io
make format
make lint
git diff --check
```

Expected initial single behavior test: actual assertion FAIL on missing durable remote tail/reconnect, with setup complete. Expected implementation targets: PASS with zero skips. Existing full-suite platform/live skips remain reported individually and are not evidence for required real C07 gates. Expected formatting/lint/diff: exit0; capture actual test summaries and distinguish unrelated preexisting failures instead of dropping them.

Git operations run at repository root after acceptance; stage explicit touched paths from this map (no `git add .`), commit once, report `git rev-parse HEAD` and `git status --short`. Never commit config.yaml/extensions_config.json, test credentials or raw process bootstrap.

## Self-review and acceptance gates

- [x] Actual SSE publication participates in original fenced transaction; rollback creates no pointer/Redis side effect.
- [x] DB committed rows replay in durable ascending seq; duplicate/lost Redis replies and ACK do not duplicate client events.
- [x] Namespace/subgraph and exact accepted payload JSON preserved; trace/debug oversized rejected before writes; no metadata bypass/global values cap.
- [x] Old attempts/user mismatches filtered, immutable old accepted run remains readable after task continuation changes.
- [x] Live seal is terminal/finalizingfalse only; generic events.* terminal authority remains closed; postwait clock and original deadline preserved.
- [x] Physically stopped accepted run can recover missing seal via stopped same TX/startup scan; unknown/quarantined/expired/non-exit/no-stop never synthesize END; no recovery dependency on dead process-local finalizing.
- [x] END follows sealed tail, survives Redis deletion and original process disappearance, without restart.
- [x] Cursor foreign/malformed/future/nonexistent produce actual pre-header400; Local malformed/gap/disconnect remains unchanged.
- [x] Publisher/recovery tasks owned and settled before resources unwind, accepted work drains with admission flags closed.
- [x] Actual DB/HTTP/process receipts demonstrate ordered_unique_events=true, runner_starts=1, terminal_end_recovered=true; required runtime gates have zero skips.
- [x] README/AGENTS/contract synchronized; format/lint/diff and acceptance reports complete; OpenSpec7 only and commit only after root acceptance.

Plan self-review confirms finalizing is only in RunRecord, not RunRow. Live seal checks the actual original object; recovery uses accepted physical-stop proof and immutable per-run identity, so it covers crash-before-seal even during in-process finalization. Unknown/unproven-stop streams remain open without END and never restart an Agent.


### Retention and creator wait refinements verified during native TDD

The neutral optional `RunEventSequenceFloor.sequence_floor(session, thread_id)` participant runs under the original thread advisory lock and guarded caller transaction. Allocation is `max(retained host event seq, private outbox highwater) + 1`; Local stores without the participant retain their existing behavior. Host deletion never reuses a remote cursor sequence, and deletion of the public event leaves its private pointer unreadable and its cursor invalid. Real host delete followed by original-capability publication first failed with private pointer seq1 collision, then passed at seq2 after this hook.

Both thread and stateless creator `/wait` routes call neutral `should_wait_for_run_stream`: a Local record with task uses its original wait behavior; an optional bridge `is_remote(run_id)` also waits for remote taskNone records using DB frame/seal semantics. Unknown identity or stream EOF without seal cannot report completed. The HTTP fixture injects a previously trusted admitted original record only at the admission boundary; public Fleet selection remains closed.


### Restart self-review refinement: repeated seal receipts

Both live writer sealing and accepted physical-stop recovery compare the persisted seal's complete immutable identity, core status and high-watermark after the same-transaction conflict insert. A disagreement raises OwnershipRejected and rolls back; an exact repeat preserves the original source, including a writer receipt rechecked by physical-stop recovery. Fresh real-PG tests seed first conflicting receipts with normal schema triggers active; they do not claim an immutable UPDATE is reachable.


### SOURCE SPEC wait refinement

`wait_for_run_completion` prepares the actual original record with cursor None
for remote waits, including queued runs before a real claim. Preparation occurs
before cancellation cleanup. Explicit-cancel waits on remote store-only records
observe the durable request without adding a new cancel on disconnect or
unsealed EOF. Local creator cancellation and replay-gap behavior remain in the
existing branch. Controlled trusted-admission HTTP tests label injected records;
normal matching queued runs still execute the actual original Agent once.

The TCP cancellation-finally regression uses a test-only controlled transport
adapter: an actual authenticated raw TCP POST enters the original wait helper,
the client closes its socket, and an outer ASGI receive observes the real
`http.disconnect` before cancelling that helper task. This covers remote
store-only cancellation cleanup, not automatic disconnect detection through
the default BaseHTTPMiddleware stack. Mapping-change EOF is a separate
controlled protocol-state case, not a claim of a normal placement mutation.

## SOURCE QUALITY retained-history refinement

A trusted host `DbRunEventStore.delete_by_run`, outside remote mutation context,
may remove public frames while private outbox pointers and the terminal seal
remain. A matching original pointer with no matching host stream frame after
the consumed cursor is explicitly unavailable history. Cursorless preflight or
a valid retained cursor returns HTTP 410; malformed/deleted cursor remains 400
with validation priority. Active and sealed streams use the same rule.

The preheader existence probe checks the complete remaining matching history
once, selecting only a missing pointer sequence. It matches original
run/attempt/generation/user/thread/spec identity plus public
id/run/user/thread/seq/type/category. It never treats semantic sequence gaps as
missing transport history. Pointer-first bounded pages preserve missing host slots; pre-yield checks of
that fixed window detect retention after its page was prefetched. Missing
later-page history is detected when that page is fetched, allowing healthy
earlier pages to emit. Such an established stream ends with
EOF and no END; reconnect reports the documented error. Consumed history before
a valid cursor does not block the remaining tail, and a zero-frame seal still
permits END. Publisher exact-target lookup remains None/no ACK for absent host
rows; no rows are resurrected and no Agent is restarted.

Regression evidence distinguishes supported trusted host whole-run deletion
from controlled SQL tail/middle/prefix retention fixtures. Native cached/hydrated
HTTP proof runs the original Agent once to its real writer seal and accepted
physical stop before the supported deletion, then checks 410/400 and unchanged
private pointers/seal without restarting the Agent.

## SOURCE QUALITY bounded runtime refinement

A healthy missing-row anti-join over every remaining pointer is still a full
suffix scan even with LIMIT 1, so it must not repeat for every output frame.
Preparation retains one complete remaining-history check with cursor400 priority
and history410. Runtime selects at most128 private pointers first, with original
query-time placement/attempt/launch/core identity guards and before/after identity
checks, then LEFT JOIN matching host frames. Missing host rows occupy pointer
slots and cause EOF rather than being skipped to later valid frames.

Every pre-yield missing check is constrained to the current fetched pointer
window. One exact pending pointer rechecks accepted mapping/core authority using
its indexed sequence, without hydrating a launch payload or hashing it. Mapping
change after prefetch ends with EOF before the next cached frame. After draining
a healthy page, the reader fetches the next bounded page; only an empty original
pointer page and consumed matching seal highwater permits END. A deletion in a
later page may therefore allow healthy earlier pages before EOF; deletion within
an already prefetched page is detected before its next output frame.

Actual PostgreSQL evidence uses384 original committed frames, captures runtime
SQL/parameters and EXPLAIN ANALYZE JSON after preparation, and distinguishes
filtered pointer candidates from planner physical/filter rows. Boundary129 and
tail300 are removed after successful preparation. This proves bounded query
scope and retention behavior, not a latency or throughput benchmark. Publisher
exact-target lookup and preheader cursor validation keep their separate paths.

### Installed capsule prerequisite correction after SOURCE v4

The first fresh six-wheel build passed source and complete image byte checks,
but four original installed starts exited before readiness. The real sanitized
stderr contained only the trusted-bootstrap failure message. A separately
labelled diagnostic stdlib trace, using the actual capsule and original /opt
bootstrap with an instrumented invocation, located the first ValueError at
`validate_model_bindings`: the builder added c07-model while the C07 private
configuration omitted the original model-1/child bindings. The original C04
plugin and MCP bundle also require their declared private configuration. These
are installed fixture prerequisites, not C07 protocol RED/GREEN or successful
Agent execution proof. Original failed reports/images remain historical.

The test-only correction preserves the shared original model-1/child bindings
and secret bundle. ScriptedModel adds an explicit c07_gate option, default false,
and delegates its C07 response to BarrierModel.answer only after the original
credential, trusted-scope, privacy and skill checks. The installed C07 capsule
uses model-1/v1 with that option, both declared model credentials, actual c04
stdio MCP authentication and all three original secret references. Production
validation is unchanged. Source prerequisites are tested against the actual
prepare-only builder output and production model/runtime/secret validators;
credential/scope rejection controls exercise both provider modes. A new SOURCE
freeze and both reviews are required before fresh wheels/Runner construction
and actual installed execution can count as GREEN.


## C07 local acceptance — 2026-10-03

The implementation and final runtime evidence passed sequential SPEC and QUALITY
reviews (C0/I0/M0), followed by Root acceptance. All detailed C07 task and
acceptance checkboxes refer to the actual implementation, not literal execution
of illustrative snippets. Frozen v9 1597/18 and fresh six-wheel Runner/Provider
bytes match; Root installed4, fullbackend13310/913defaultskip/1deselect,
blocking75/boundary74/static gates pass. Historical native89/C01-C06742/Local422
and B261 preserve their original scope; fresh v9 image15/currentmanager1 cover
changed packaged resources. See [C07 acceptance](../../../docs/ecs-fleet-c07-acceptance.md)
and implementation progress for exact source/image/manifest identities, original
RED/failure history and two existing guidance soft warnings. This supersedes
planning-stage status above; C08-C12/BC and remote activation remain pending.
Acceptance bookkeeping changes only nonpackaged documentation and task state.
