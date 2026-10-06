### Stream Bridge Heartbeats

Memory and Redis bridges take their default idle heartbeat cadence from the startup-only `stream_bridge.heartbeat_interval_seconds` setting. Keep the default on the bridge instance so SSE, `/wait`, and internal subscribers stay aligned; an explicit `subscribe(..., heartbeat_interval=...)` remains a per-subscription override.

### Checkpoint Channel Modes (`full` / `delta`)

Checkpointer storage runs in one of two channel modes, selected by `checkpoint_channel_mode` in `config.yaml` (default `full`). `delta` mode adopts LangGraph 1.2's `DeltaChannel` for `messages`: checkpoints store a sentinel + per-step writes instead of the full message list, so storage/serde grows O(N) instead of O(N²) in turns. All checkpointer backends (memory/sqlite/postgres) serve both modes unchanged — the semantics live in the compiled graph's channel table, not in the saver.

**Mode is process-frozen and restart-required.** `make_lead_agent` and the embedded `DeerFlowClient` freeze the resolved mode (`runtime/checkpoint_mode.py::freeze_checkpoint_channel_mode`) before compiling the graph with the mode-matched schema (`agents/thread_state.py::get_thread_state_schema`, plus `adapt_state_schema_for_mode` / `normalize_middleware_state_schemas` for middleware state). Adapted middleware schemas are cached by schema, mode, and resolved snapshot frequency so a pre-freeze ephemeral graph cannot leave a stale default-frequency schema behind. A second, different mode or frequency in the same process raises `CheckpointModeReconfigurationError`. To switch: edit config, restart.

**Delta snapshot cadence is configurable but frozen with the mode.** `database.checkpoint_delta.snapshot_frequency` (default `10`) sets the `DeltaChannel` snapshot cadence. It is frozen alongside the mode (`freeze_checkpoint_snapshot_frequency`; non-positive direct inputs raise `ValueError`, while a frozen-value mismatch raises `CheckpointModeReconfigurationError`), restart-required, and must match across every process sharing one checkpoint database — the cadence lives in each compiled graph's channel table and is deliberately NOT stamped into checkpoint metadata, so the mode-compatibility marker and full -> delta migration semantics are unchanged. Schema helpers resolve it explicit-arg -> frozen -> default, and every schema/graph cache (`_delta_thread_state_schema`, `_adapt_state_schema_for_delta`, the client agent-config key, the gateway accessor-graph cache) keys on the resolved value.

**Compiled-graph cache cap is configurable and hot-reloadable.** `database.checkpoint_graph_cache.accessor_graph_max` (default `64`) bounds the gateway accessor-graph cache, which clears wholesale at the cap. The cap is re-read on every eviction check (`resolve_checkpoint_graph_cache_max`), so a config.yaml reload takes effect without a restart — a size change never affects graph semantics, only eviction timing.

**Compatibility is asymmetric and fail-closed.** Every checkpoint written in delta mode carries metadata marker `deerflow_checkpoint_channel_mode: "delta"` (injected via `inject_checkpoint_mode`; absence of marker = full, so pre-feature checkpoints need no migration). Before any state read/write, `ensure_checkpoint_mode_compatible` rejects a full-mode process opening a delta thread with `CheckpointModeMismatchError` (surfaced as HTTP 409 with the cause and thread id by the threads router; `CheckpointModeReconfigurationError` maps to 503) — a full-mode raw read of a delta blob would silently return empty/partial `messages`. The reverse direction is allowed: delta-mode processes read full checkpoints transparently (old full checkpoints seed the delta channel), so full → delta is the smooth migration path; delta → full requires materializing/converting the data first. Detection also honors upstream's `counters_since_delta_snapshot.messages` metadata, and an explicit config marker takes precedence over any ambient context value.

**Never bypass `CheckpointStateAccessor` (`runtime/checkpoint_state.py`) for thread-state access.** It is the single choke point binding graph + checkpointer + mode: it injects the mode marker into configs, runs the compatibility check before every `get`/`update`/`history`, and returns materialized state (delta checkpoints lack `channel_values.messages` — raw `get_tuple` reads see a sentinel). Gateway `services.py` builds and passes the accessor; thread-owned reads (state/history/regeneration) must use `build_thread_checkpoint_state_accessor` so the recorded assistant's middleware schema materializes every channel. `history(limit)` semantics: `0` means zero items (explicit empty), `None` means unlimited — do not pass `limit=0` through to `graph.get_state_history`. Assistant metadata lookup is fail-closed for mutation accessors so a store outage cannot silently select the default schema and discard extension channels. In `full` mode the read path degrades to a raw checkpointer read (`_RawCheckpointReadAccessor`) when the agent factory cannot build the graph (bad model config, MCP outage) — full checkpoints carry complete `channel_values`, so reads don't need the graph; degraded snapshots take `created_at` from the standard checkpoint `ts` field, falling back to metadata only for compatibility. The delta gate still applies on the degraded path; `next`/`tasks` degrade to empty and thread status falls back to the stored status because task presence is not derivable, while delta mode has no fallback (materialization needs the channel table).

**Replay checkpoint lookup prefers lineage and degrades only for an explicitly missing legacy parent link.** Branch and regenerate paths first walk `parent_config`, which prevents a global chronological scan from selecting a sibling created by regeneration. `CheckpointParentMissingError` alone enables the bounded newest-first history fallback in `app/gateway/checkpoint_lineage.py`; cycles, dangling/non-addressable parents, target mismatches, and depth exhaustion raise `CheckpointLineageIntegrityError` and fail closed instead of selecting a sibling. The compatibility scans request 400 raw checkpoints so up to 200 duration-only entries do not consume the effective branch-history budget; the fallback scans oldest-to-newest internally, skips duration-only checkpoints, and accepts only checkpoints with an addressable id as the replay base. A source history with no discoverable pre-user checkpoint preserves the historical single-checkpoint branch behavior instead of rejecting the branch; regeneration remains unavailable for that inherited response. Existing single-checkpoint branches are not mutated by regenerate preparation, and no raw checkpoint tuple is copied across threads because delta state depends on ancestry and pending writes. Regenerate source-run lookup uses the current thread's exact event, then the server-stamped `run_id` on the copied human message, then verified RunManager content matching; it does not read parent-thread events. When an interrupted response was streamed but never checkpointed, regeneration accepts only the latest visible human message's server-stamped `run_id` after verifying that it belongs to the same thread and still has `interrupted` status. Storage or checkpoint-mode failures are not treated as a missing base and still fail closed.

**A delta-mode run cannot fork; `runtime/runs/worker.py` linearizes the resume instead.** Resuming from an older checkpoint (regenerate, or any client-supplied `checkpoint`) forks the lineage, and delta state for a fork is not materializable: `BaseCheckpointSaver.get_delta_channel_history` — and the bespoke overrides in `InMemorySaver`/`PostgresSaver` — collect **every** `pending_writes` entry stored on each on-path ancestor, but a shared parent also carries the writes of the sibling child that was abandoned. Those writes replay into the fork, so the run starts from a message list still containing the answer it was supposed to replace (#4458: regenerating in a branched thread showed the superseded assistant message beside the new one after a reload; reproduced on postgres, sqlite, and the in-memory saver). Write-to-child ownership belongs to the upstream delta contract, so DeerFlow does not reimplement the walk: `_linearize_delta_checkpoint_resume` materializes the requested checkpoint's complete state and writes every channel onto the **current head** (which has no siblings) through the state mutation graph, using `Overwrite` for reducer channels and resetting newer head-only channels to their schema default (or `None` when no constructible default exists); it then drops the `checkpoint_id` selector and lets the run proceed linearly, while the abandoned turn stays in history as the rewritten head's ancestry. The worker holds `_checkpoint_thread_lock` across `_capture_rollback_point` and the optional linear rewrite, making the rollback snapshot and rewrite atomic with graph streaming and the preceding run's duration-metadata checkpoint write. Capture preserves the complete real pre-run state; cancel-with-rollback then linearly replaces the current delta head with that captured state rather than forking the now-shared pre-run checkpoint, so the abandoned turn is restored without replaying the resume sibling's writes. The worker also recomputes the current-run message boundary from the rewritten state and fails closed (an unreadable resume checkpoint raises rather than falling back to the corrupt fork). `full` mode keeps forking — its checkpoints carry complete `channel_values` and need no replay — so LangGraph branching semantics are unchanged there. Root namespace only; subgraph namespaces are left alone.

**Wholesale state replacement uses a state-only mutation graph + `Overwrite`.** `update_state` values pass through channel reducers (`add_messages` merge in full, append in delta), so replacing reducer values requires `Overwrite` rather than an ordinary update. Full-mode rollback and context compaction replace `messages`; delta resume and delta rollback replace every materialized channel and reset current-head-only channels to their schema default (or `None`). These writes go through `build_state_mutation_graph(as_node, mode, state_schema)`, and `state_schema` MUST be the thread's effective schema (`graph_state_schema(assistant_graph)`), because the base-ThreadState fallback silently discards written channels contributed by custom `AgentMiddleware.state_schema`. Channels absent from a full-mode fork write inherit the parent's channel blobs, so middleware channels survive rollback/compaction (locked by `test_rollback_preserves_middleware_contributed_channels` and `test_compact_thread_context_preserves_middleware_contributed_channels`). The compiled mutation graph has one no-op node (entry = finish) whose checkpoint machinery (channels/versions/metadata) is identical to the agent graph's but schedules no pending tasks, so the restored/compacted head stays idle instead of re-triggering the agent. Never hand-write checkpoints via `checkpointer.aput` for this; raw writers elsewhere must preserve checkpoint parentage — severed ancestry breaks delta replay (see `runtime/runs/worker.py` writer parenting and `checkpoint_patches.py`).

**Run rollback flow** (`runtime/runs/worker.py`): `_capture_rollback_point` materializes the complete pre-run state via the accessor and captures raw `pending_writes` via `aget_tuple` into an immutable `RollbackPoint` before the run starts — capture failure disables rollback (fail-closed), never restores partial state. In `full` mode, cancel-with-rollback forks from the pre-run checkpoint via the mutation graph and inherits non-message channels from that parent. In `delta` mode, forking is unsafe once the cancelled path has attached sibling writes to the pre-run checkpoint, so rollback replaces every captured channel on the current head, using `Overwrite` for reducers and schema defaults for current-head-only channels. Both modes reattach only the captured pre-run pending writes to the restored checkpoint. Edit replay runs (`metadata.replay_kind="edit"`) also restore the pre-run checkpoint on failed, timed-out, or interrupted completion and publish the restored `values` snapshot to the stream before `end`, so clients do not remain on a transient edited branch when the replay did not produce a successful replacement.

**Targeted run-event attribution** (`runtime/events/store/`):
`RunEventStore.find_latest_ai_message_run_ids()` has a complete-or-error
contract. Its default implementation walks `list_messages()` backward in
1000-row pages, preserves the first page's high-watermark through the exclusive
`before_seq` cursor, and raises when a full page has no safe progressing `seq`.
Memory and database stores use that bounded path; the JSONL store overrides it
with one complete thread-log read because each JSONL page would otherwise
rescan every run file. The default and JSONL paths share the public
`normalize_message_ids()` and `match_ai_message_run_id()` helpers from
`events/store/base.py`. Database owner filtering is inherited on every page.
Callers may use a missing key as proof that no valid AI event exists only after
an ordinary return, never after an exception. A caller that crosses a run or
checkpoint-write admission boundary must repeat the complete audit after
admission; a pre-admission exact hit can be superseded by a later event just as
a pre-admission miss can become an exact hit.

Gateway `POST /api/threads/{id}/history` uses that lookup to migrate legacy AI
messages. An exhaustive miss preserves the human-boundary fallback; an
incomplete lookup removes unproven synthesized IDs. Its metadata-only
write-on-read cache stores `run_message_ids` for every audited AI ID (including
exhaustive misses) plus required `run_durations`; duration presence alone does
not prove attribution. Historical `body.before` reads write the audit to the
head, and the merge may retain IDs no longer in materialized history, which
readers ignore. Migration must acquire the durable `checkpoint_write`
reservation, then repeat the whole message audit and batch-reload required run
rows before persisting. Post-admission exact hits replace foreground exact or
boundary mappings, and recomputed final durations replace foreground snapshots.
Successful workers keep their durable run row active through the final duration
checkpoint write, so a peer migration cannot enter during terminalization.
The first `RunManager.list_by_thread()` hydration page uses a 100-row floor or
the number of required IDs, whichever is larger; missing exact runs use targeted
`get()` calls.

**Where things live**:
- `runtime/checkpoint_mode.py` — mode + snapshot-frequency freeze, marker injection, delta detection, compatibility gate, both error types
- `runtime/checkpoint_state.py` — `CheckpointStateAccessor`, `build_state_mutation_graph`, `RollbackPoint`
- `checkpoint_patches.py` (package root) — checkpoint-machinery patches: delta-history folding for `InMemorySaver` (delegating to the base walk), stable message IDs across materialization, upstream first-write drop fix, and `BinaryOperatorAggregate` unwrapping an `Overwrite` first write into an empty (MISSING) channel — Union-typed reducer channels (`sandbox`/`goal`/`todos`/`promoted`) have no constructible default, so a replace-style write into a fresh branch thread or a never-written channel stored the wrapper literally and crashed the next consumer (#4380; probe-guarded, stands down if upstream fixes it)
- `agents/thread_state.py` — `ThreadState`/`DeltaThreadState`, `delta_messages_field` / `DELTA_MESSAGES_FIELD` (`DeltaChannel` at the configured `snapshot_frequency`, default 10), schema adaptation helpers
- `runtime/context_compaction.py` — compaction via accessor + mutation graph (reference consumer)
- `runtime/checkpoint_cache/` + `runtime/checkpointer/cached_saver.py` — delta-mode checkpoint history cache; checkpoint state reads MUST go through `CheckpointStateAccessor`, and the checkpointer may be a `CachedHistorySaver` wrapper — never rely on concrete saver types
- Tests: `tests/test_checkpoint_mode.py` (freeze/detect/gate), `tests/test_checkpoint_state.py` (accessor/mutation graph), `tests/test_delta_channel_checkpointers.py` (saver parity), `tests/test_threads_checkpoint_mode.py`, `tests/test_gateway_checkpoint_mode.py` (dual-mode e2e parity), `tests/test_context_compaction.py` (mutation-graph write, no scheduling), `tests/test_run_worker_rollback.py`, `tests/test_cached_history_saver.py` + `tests/test_cached_history_saver_integration.py` (history cache)

**Checkpoint channel benchmark**: `scripts/benchmark/checkpoint/bench_channels.py`
runs paired `full`/`delta` message-only StateGraphs in a fresh child process per
case, using sync `InMemorySaver` or `SqliteSaver` so reducer, serialization, and
saver costs stay separate from Gateway/async scheduling. It reports deterministic
correctness digests, write windows/percentiles, warm and graph-rebuilt cold reads,
logical checkpoint/write bytes, SQLite DB/WAL/SHM footprint, reducer replay time,
and peak RSS as versioned JSONL. The controller alternates mode order and rejects
performance data when paired modes materialize different state. Its default 1 GiB
estimated cumulative full-payload cap skips both modes of an oversized pair when
`full` is selected, including every delta cadence in a `--snapshot-frequencies`
sweep; intentional `--modes delta` diagnostics bypass this full-payload cap, so
size those runs explicitly. Use `--allow-large-cases` only
on a provisioned machine. Duplicate CSV matrix values are ignored with a warning;
use `--repetitions` for repeated samples. Summarize paired successful repetitions
with `scripts/benchmark/checkpoint/summarize_channels.py` (all ratios are
`delta/full`). `--profile-dir /tmp/checkpoint-profiles` writes one cProfile
artifact per case for attribution. Profiled rows carry `profiled: true`, and the
summarizer automatically excludes them from baseline summaries with a warning.
Storage-size collection relies on saver-specific diagnostic layouts; if those
layouts change, the timing/correctness row remains successful while storage
fields become `null` and `storage_stats_error` records the diagnostic failure.
Example:

```bash
cd backend
PYTHONPATH=. uv run python scripts/benchmark/checkpoint/bench_channels.py \
  --backends sqlite --updates 100,500,999,1000,1001 --payload-bytes 128 \
  --repetitions 7 --output /tmp/checkpoint-bench.jsonl
PYTHONPATH=. uv run python scripts/benchmark/checkpoint/summarize_channels.py \
  /tmp/checkpoint-bench.jsonl
```

The production-shaped layer lives in
`scripts/benchmark/checkpoint/bench_production.py`: per-case child processes
run graph-level `ainvoke` turns through the real lead-agent graph (scripted
deterministic model, real `AsyncSqliteSaver`), then measure
`GET /threads/{id}/state` and `POST /threads/{id}/history` through the real
Gateway route stack in the same event loop (httpx ASGITransport), split into
cold/warm accessor-graph-cache samples. It sweeps `snapshot_frequency`
(config: `checkpoint_delta.snapshot_frequency`, process-frozen like the mode),
pairs every delta frequency against the same full row, and fails both
rows of a pair when materialized or wire digests diverge. Each case must have
more than the two discarded warm-up turns, and SQLite DB/WAL/SHM sizes are
captured while the saver is still open so they represent the online storage
footprint. Summarize with
`scripts/benchmark/checkpoint/summarize_production.py` (ratios are
`delta/full`; it also emits `snapshot_write_spike` and `cache_effect_ms`,
the decision inputs for the production snapshot-frequency and accessor-cache
defaults). Harness tests live in `tests/test_bench_checkpoint_production.py`
and `tests/test_summarize_checkpoint_production.py`; timing thresholds are
not CI gates. The matrix test pins that every `(repetition, turns)` group
contains both modes and that their execution order flips between consecutive
groups, including across repetition boundaries.

Operational limits learned from the first runs (the default matrix is too
large to run blindly):

- The default `--timeout-seconds 900` is insufficient for delta mode at
  `snapshot_frequency=1000` once turns reach 500 (measured: delta-500 takes
  ~1100-1200s; delta-2000 takes ~45min). Pass an explicit
  `--timeout-seconds` for any large matrix, and treat the turns=2000 corner
  as practical only at small snapshot frequencies.
- Full-mode 2000-turn runs produce a ~33GB sqlite DB. Point `TMPDIR` at real
  disk, not tmpfs (the benchmark uses `tempfile.TemporaryDirectory`, which
  honors `TMPDIR`), or the run dies mid-case.
- The history route clamps `limit` to 100 (`le=100` on
  `ThreadHistoryRequest.limit`), so `--history-limits` values above 100 are
  measured and reported by their effective (clamped) limit.

Example:

```bash
cd backend
PYTHONPATH=. uv run python scripts/benchmark/checkpoint/bench_production.py \
  --turns 10,100,500,1000,2000 --payload-bytes 128 \
  --snapshot-frequencies 10,50,100,500,1000 \
  --repetitions 7 --output /tmp/production-bench.jsonl
PYTHONPATH=. uv run python scripts/benchmark/checkpoint/summarize_production.py \
  /tmp/production-bench.jsonl
```


### Trusted execution backend admission

execution/contracts.py owns backend selection and SQL admission participation;
harness must not import app or the optional Fleet package. App injects the backend
through an internal keyword; client metadata/config does not select an executor.
LocalExecutionBackend remains default and passes no new participant keyword to
ordinary stores. Memory stores explicitly reject SQL participants.

RunRepository's RunAdmissionUnitOfWork holds one session/transaction: participant
prepare locks its goal before core thread/run work, then insert persists extension
records after the run flush. Any exception or CancelledError rolls back both;
RunManager registers only committed admissions. Retry validation uses immutable
original identity/inputs and cannot leave freshly prepared goal rows. Remote pending
records have no Gateway owner or lease and create no local asyncio task.
C03 ownership adds SQL local eligibility for absent/local server-owned
backend labels, plus a trusted host predicate applied to scans and mutations. Remote
hydration preserves its label; store_only requires valid nonlocal admission output.
See the C05/C06a write boundaries below. Gateway remote activation remains closed; the Fleet development guide owns app adapters and input codecs.


### Trusted existing-executor attachment (C04 locally verified)

`RunManager.attach_existing_executor` verifies a previously admitted SQL run's
user/thread/backend/owner and live pending lease, then registers it through the
normal manager index. It neither admits another run nor starts a Local heartbeat.
The existing `run_agent` still calls the store's real `try_start`; nonlocal
SQL start rechecks pending state, owner and lease against database time.
The actual keyword is `run_manager`. Preserve the complete graph input,
normalized config, stream modes, subgraph flag, interrupts and recursion budget.

The host supplies private identity, resources and agent factory through the
installed runner bridge. Harness remains independent of Fleet/app. This attachment
alone grants no checkpoint or finalizer write authority. Remote activation
stays closed pending the remaining slices.


### Trusted PostgreSQL checkpoint write boundary (C05)

`make_checkpointer(..., write_fence=...)` is a trusted infrastructure keyword;
client checkpoint configs cannot choose an execution identity. The neutral
`execution/fence.py` protocol receives the actual psycopg writer cursor, target
thread and operation. Harness must not import app/Fleet to validate ownership.
Host binds the original accepted token stamp and immutable execution identity.

The audited remote adapter supports AsyncPostgresSaver 3.1.1's `aput`,
`aput_writes`, `adelete_thread` and its synchronous aliases. Hold one physical
connection and explicit transaction across the host ownership locks, validation,
stock SQL and commit/rollback. Pipeline mode alone is not a transaction fence;
a guard using an independent SQLAlchemy session is insufficient. Reads retain
stock behavior. Fence the inner PostgreSQL saver before CachedHistorySaver;
full/delta state access still uses CheckpointStateAccessor. The wrapper explicitly forwards
`after_root_commit` reads/replacements to the committing saver; setting a
wrapper-only callback would silently omit delta-mode publication. Unsupported inherited
copy/prune/delete-for-runs operations remain unsupported. The separate sync
PostgresSaver is a Local path; do not present it as remote-fenced.

Remote fenced setup only verifies already initialized schema and the complete
known migration set read-only. It must not create schemas, execute migrations or
insert migration versions, and must reject fallback to another schema. Trusted
Gateway/Local initialization keeps stock setup, whose migrations include CREATE
INDEX CONCURRENTLY. Tests use the real trusted initializer before remote startup.
When no fence is supplied, retain existing Local backend selection and behavior.

Keep legitimate final checkpoint writers before durable terminal persistence,
including duration and interrupted-title fallback. Preserve ownership, replay,
prior-finalizing and later-run guards. Daemon terminal cleanup renewal grants no
checkpoint write permission. This boundary does not protect thread metadata,
memory, event stores or extension finalizers; their C06 contracts remain separate.
Remote Gateway activation remains closed until all required C slices are verified.


### Trusted remote primary mutations (C06a)

`execution/mutation_context.py` supplies immutable private attempt context and
nonretryable `OwnershipRejected`. Host-bound optional `mutation_capability` on
RunRepository, ThreadMetaRepository and DbRunEventStore validates the original
identity on the writer transaction before target/advisory locks, retaining execution
locks through commit/rollback. Recheck the DB clock after target SQL/ORM flush,
before commit on that same transaction: target waits can outlive leases. Harness
imports no app/Fleet. Missing/wrong callback context rejects; unbound Local keeps
its behavior. Scopes span attachment/lifecycle; no identity/token stamp in model config.

Trusted runner initialization atomically creates missing original-owned thread
metadata and preserves existing rows; never adopt another owner.
Remote targets and actual ThreadMeta ownership must match the bound user/thread/run.
Absent ambient auth does not authorize ownerless writes or another user's events.
Remote admission/admin/ownership mutations and thread-wide history deletion reject.
Active writes require pending/running. Same terminal outcome allows only completion
bookkeeping, matching final thread status and the worker's checkpoint-title sync;
conflicting outcome/error/stop reason, ordinary metadata/progress and event writes
reject. Original bounded leases/reservation/deadlines still apply. Checkpoints
remain active-only under C05.

RunManager/worker mark rejected execution ownership lost without durable writes.
Journal background/explicit flush retains the rejection even after a task's done
callback; it must not rebuffer it for retry. Delivery receipt persistence stops on
this rejection. Preserve ordinary transient retries and cleanup notifications.
C06b adds direct caller-context Store operations, same-cursor explicit TX,
embeddings before locks, pure SELECT for TTL-disabled GET and read-only setup.
Bound sync definitions reject file cleanup; after unique-conflict rollback/retry,
recheck original authority before ordinary error handling.
See [Store/tool boundaries](../../../../../docs/ecs-fleet-development.md).
C06 acceptance evidence belongs to its external plan. Later C/BC remain pending;
remote activation stays closed.


C06c `BoundMutationTransactions` fences actual memory/extension writer transactions
through commit/rollback and grants no terminal SQL authority.

Remote `RunContext.before_terminal_mutations` drains original memory, then owned
nested extension callbacks, before durable terminal status within a shared budget.
`OwnershipRejected` marks execution lost and stops completion writes. Postterminal
cleanup grants no active write authority. Local contexts without the hook retain
their policy.

Remote teardown retains its entire original resource stack until actual owned
Task rollback/finally settles, including native memory workers and failed
construction. Positive memory drain and owned observers must jointly settle
before service stop and again before resource unwind; actual enqueue/completion
revisions detect work queued across both directions. False/exception retains
resources, first error and deadline; memory close alone proves no settlement.
Create the original
cleanup controller before the first resource; early construction failure must
use the same retained phases and budget, never a raw stack.aclose bypass. First cleanup fixes
one monotonic 120s deadline across phases and retries. The isolated Agent entry
keeps the original loop for bounded settlement. If rollback remains unsettled,
deadline self-exit precedes resource unwind and implicit loop shutdown. Host/Gateway/embedded callers only
receive private pending cleanup, never process exit. Physical exit is not success
or reservation release; the daemon retains its original stop-proof responsibility.
Verify real subprocess termination and PG rollback/lock release, plus normal
settlement cleanup. Actual /opt/deerflow/libexec_bootstrap.py must match frozen
source as well as its installed wheel copy.

Remote host `RunContext.settle_stream` is a private optional lifecycle hook. The
worker explicitly closes the original astream in single/multiple modes and
awaits actual graph/checkpoint cleanup. A naturally exhausted generator starts
no cleanup timer, preserving normal goal continuation. Closing a live original
stream starts the same first monotonic 120s budget; retain its real graph-stream
phase Task, context and resources across caller cancellation and retries.
`ExecutionCleanupPending` is a neutral control exception carrying only the
original error, never authority/host handles. It bypasses ordinary recovery and
durable finalization while preserving typed/cancellation identity. Bootstrap
joins the retained graph phase before memory/observers, services and resources;
only the isolated worker retains physical deadline exit. Local hosts close the
stream explicitly without this hook or process exit. Verify actual checkpoint
SQL rollback barriers, no premature service/resource unwind and independent PG
rollback/connection/lock checks. Journal drain alone does not settle astream.


### Remote event transaction participation

`DbRunEventStore` accepts a neutral optional transaction participant from
`runtime/events/transactions.py`. Insert callbacks receive the same original
Session, persisted event ID and record inside its guarded transaction. Batch
callbacks retain whole-batch rollback; existing singleton rows never call insert
again. The optional sequence floor runs after the original thread advisory lock
in that transaction and prevents sequence reuse after public event retention.
Local None preserves existing allocation/flush behavior. Never import app or
Fleet from the harness. `stream.frame`/`stream` is a separate transport projection,
not a message-history row. App adapters preserve actual frame event/namespace and
value; trace-derived envelopes enforce the exact persisted JSON byte limit before
writing. The narrow terminal seal is app-owned and grants no events.* authority.


Remote workspace terminal pairing uses the neutral `RunTerminalParticipant`
on each original RunRepository terminal transaction. Its before/after hooks
share the writer's original AsyncSession; after runs only for an updated row,
before original SQL flush/fresh ownership validation and commit. Hooks do no
file copying, writer settlement or network work. Local has no participant.

A bound remote worker uses sync durability on both stock astream shapes. Root
saver callbacks execute after the original saver cursor/transaction/connection
and lock have exited. Presentation intents derive from trusted ExecutionInfo
and successful materialized root ToolMessages; task namespaces include a
node/task suffix even at the root. Subgraph parents never stage root points.
Full/delta reads use the original CheckpointStateAccessor, not raw channel data.

Remote duration/history/title root copies preserve corresponding pending task
writes by exact name/path, including cached completed tool results. Interrupt
IDs are rebuilt with the new task namespace so explicit keyed Command resume
continues the actual copied task. Missing/ambiguous mappings fail closed.
Actual graph pending tasks yield interrupted/paused boundaries; human interrupts
request input. Preparation follows the last original checkpoint mutation.
Checkpoint/preparation failure cannot produce END or clear retained stream state.
Final workspace gates never reopen; only a committed exact partial pair permits
original same-owner writer/MCP reopening. Physical-stop outcome application and
immutable accepted final/paused authority belong to the Fleet host. Node workspace
poll/claim may return a read-only idle receipt under exact original ownership and
lease fences: finishing requires the accepted final point/current root, and an
accepted request requires its immutable point/descriptor/nonce/epoch and same-run
root ancestry. Idle grants no launch, copy, writer reopening or new claim; the
Node returns before further census/copy. Ordinary terminal writes stay fenced.

Trusted terminal participants require the original cancellation CAS even when
Local heartbeat is disabled (the host may own remote lease renewal). The three
RunManager terminal entry paths propagate participant/SQL faults; ordinary Local
best-effort bookkeeping defaults remain unchanged. A fault after accepted final
keeps that immutable point but blocks live END. Exact physical-stop recovery may
seal the accepted outcome after the original process stops.

The original clarification middleware ends via Command(goto=END), which may
leave snapshot.next/tasks empty. Remote completion also checks its private
current-run root human-input observation against the materialized successful
ToolMessage and last assistant tool call. This produces interrupted/input_required;
ordinary graph pauses without a human request produce interrupted/paused.
Historical cards alone do not confer this authority. Card/request IDs retain
their original protocol identity.

Remote workers refresh materialized pause immediately after every original
stream turn, including hidden goal continuations, before the next goal evaluator
or checkpoint copy can run. A paused turn retains its pending interrupt IDs and
cached parallel results and cannot enqueue another hidden turn or become success.
Local goal continuation defaults remain unchanged.


Thread routing uses neutral host `ThreadExecutionBindingRow` storage and an
application-injected transactional admission guard. Harness must not import app
or Fleet for this check. Server-owned bindings survive run/cache state changes;
client metadata cannot grant routing or branch identity. `store_only` records
defer to trusted SQL admission, while actual Local executors retain their
same-worker guard. See [C08 runtime contracts](../../../../../docs/ecs-fleet-c08-runtime.md)
for accepted-source preparation, immutable file reads and branch recovery.


Cooperative yield uses a neutral host-injected RunContext controller. Explicit
await ends through the first before_model hook after complete ToolNode settlement.
Natural graph completion also collects unconsumed awaited children before hidden
goal evaluation. Neither unfinished interrupts nor ordinary failures become
success. The original terminal participant pairs checkpoint/files and desired
waiting outcome; physical STOP remains the host's release barrier. Local None
preserves its lifecycle. See [BC02 boundary](../../../../../docs/ecs-fleet-bc02-runtime.md).
