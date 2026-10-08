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
See [Store/tool boundaries](../../docs/ecs-fleet-development.md).
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
same-worker guard. See [C08 runtime contracts](../../docs/ecs-fleet-c08-runtime.md)
for accepted-source preparation, immutable file reads and branch recovery.


Cooperative yield uses a neutral host-injected RunContext controller. Explicit
await ends through the first before_model hook after complete ToolNode settlement.
Natural graph completion also collects unconsumed awaited children before hidden
goal evaluation. Neither unfinished interrupts nor ordinary failures become
success. The original terminal participant pairs checkpoint/files and desired
waiting outcome; physical STOP remains the host's release barrier. Local None
preserves its lifecycle. See [BC02 boundary](../../docs/ecs-fleet-bc02-runtime.md).

### BC03 continuation admission lock entry

RunRepository accepts a trusted host before-thread admission guard alongside the
existing participant hook. Ordinary run, rejected admission and checkpoint
operations enter that guard before the core thread/binding locks. Scheduled
participants retain their occurrence-first hook; host goal/task locks follow it.
Continuation admission still uses the original UoW, immutable reuse validation
and store_only result. Harness must not import app or the optional Fleet package.
The host owns wait-group receipts, original stopped-source proof and untrusted
result framing. See [BC03 boundary](../../docs/ecs-fleet-bc03-runtime.md).
