# ECS Fleet development contracts

`packages/ecs-fleet` owns the optional `deerflow_ecs_fleet` package. Fleet tables use
private metadata and a locked independent Alembic chain; never register them on host
Base. The host `gateway/fleet_auth.py` authenticates only explicit worker POST routes;
node credentials cannot acquire session/PAT/internal privileges, even with auth disabled.
Contributed management routers remain session authenticated.

`app/fleet/job_tracking.py` injects the host tracking reader and registers the Fleet
long-task driver for an already-loaded ready Fleet service, including when new jobs
are disabled, so accepted work remains tracked and reconciled.
Staged jobs cannot be claimed until matching host tracking commits. Reconciliation
runs until service shutdown. Execution → node → reservation is the lock order;
node session rotation changes only node identity and retains old resource charges.

Use `tests/fleet` with TEST_POSTGRES_URI pointing to an isolated test database.
Tests create/drop random schemas, never real business schemas. PostgreSQL saver
fixtures keep their owned schema across reopen and drop it only after saver closure,
including creation/connection-close failure paths. Use the existing
normalize_libpq_dsn helper to remove the SQLAlchemy driver while preserving original
URI query bytes and keyword DSNs, then apply the original libpq search_path encoding;
do not serialize the scoped URI again. Options-regression fixtures preserve the
parent DSN form and existing options for URI and keyword inputs before adding
their timeout; the new test parent constructor uses percent-only URI decoding to
preserve literal+ options. This does not certify arbitrary raw+ options passed
directly to the unchanged production search_path helper. Whole-schema integration gates use a fresh owned test database when an
older test database already contains public tables; retain the original no-public
assertion and never remove another fixture's tables to satisfy it. Worker client/daemon
and private restart journals exist; tests exercise real TCP Gateway loss and a lost
start-grant response with local Docker. Bootstrap must finish stop reconciliation
before claiming work. Native initial and accepted Agent preparation must retain
its physical copy/fsync/control-marker writer through repeated cancellation.
The Node waits for that writer before stopped reporting and daemon flock/client
release; publication-only joins do not own preparation. Stop every owned residual before reporting recovery failure;
missing journals or a stop RPC failure must not leave other owned containers running.
Remote Agent and continuations are pending; consult the root delivery roadmap before
enabling or advertising Fleet execution. Dependency installation remains operator
controlled through the extension manager; do not add an unconditional host dependency.

`app/fleet/runtime.py` binds the core `deerflow.mcp.tasks.fleet_runtime` submitter
bridge only after the ready service, persistent MCP task repository and task service
are available. Harness code must not import app or the optional Fleet package.
`submit_fleet_job` derives user/thread/run/invocation identity from trusted graph
runtime; its model schema contains no owner, node, image, credential or link-mode
controls. Only approved job profile names are exposed. Tool visibility is gated in
both subagent configurations; shutdown clears the bridge before stopping task tracking.
Unknown/quarantined snapshots expose reconciliation instructions through the original
tracking ID; do not include the private Fleet job handle in public input_required or
notification payloads. B09 tests exercise the actual lead-agent/run_agent lifecycle,
TCP worker/Docker publication, busy thread admission and two task-service restarts,
including a lost committed notification launch response. Notification runs and
run.delivery receipts are persisted in isolated SQL; Gateway process restart and
production deployment remain later acceptance work.

Fleet task retries use TaskSubmission.tracking_task_id to opt into the host
McpTaskRepository.create_idempotent boundary. Ordinary MCP drivers retain duplicate
remote-handle rejection. Never infer invocation identity from provider call IDs alone:
use the graph-injected ExecutionInfo helper in deerflow.mcp.tasks.invocation.
B10 scheduled job identity is injected only by the internal scheduler launch through
`start_run(trusted_schedule_id=...)`. Strip client-supplied scheduled_task_id from both
context and configurable before injection; request metadata never grants this identity.
The scheduler supplies its server-owned context mode. Scheduled Fleet jobs require
`reuse_thread`; reject `fresh_thread_per_run` before creating work, and do not expose
canonical tracking across threads. Operator `scheduled_job_slots` maps bounded slot
names to existing job profiles; the
model may select an approved slot, not a dedupe key. Per-owner schedule/slot admission
uses a PostgreSQL transaction advisory lock before job locks, and checks original
invocation identity before unfinished group reuse. Unknown/quarantined and cancellation
pending jobs keep the slot; terminal jobs permit a new occurrence.
`TaskSubmission.reuse_existing` explicitly selects a read-only canonical tracking lookup
outside compensation: never cancel older accepted work on a missing tracking row.
Original-invocation retry retains create_idempotent recovery. Cross-occurrence reuse
preserves the original run, name and arguments. The private f0005_job_invocations
migration owns immutable invocation receipts and backfills existing canonical jobs.
Every admission records requested owner/key/thread/run/spec/group -> canonical job,
including reuse decisions. All submission paths serialize owner+invocation before
schedule-group/job locks. Receipt replay precedes active-group lookup and stays bound
to the original job after terminal state or a newer cycle; never rebind a lost response. API execution_uncertain is additive and
true only for Fleet input_required with execution_unknown; public status remains unchanged.
Worker start/renew/stopped requests require node + session + attempt token; closing
new-work flags must not cut off these accepted-work endpoints. The persisted start
grant freezes the claim's profile. Stop acknowledgement can release capacity but
cannot establish successful completion without an accepted manifest.


Fleet `workspace.NASWorkspace` owns the explicit deployment sentinel and descriptor
traversal; `artifacts` owns bounded manifest metadata and verified copies/reads. These
blocking filesystem methods must run in asyncio.to_thread from async services.
Do not create the NAS root or sentinel automatically on a missing mount. A task only
mounts its own outputs read-write and only declared input/code versions read-only
under /inputs/<version-id>; its sealed output copy is a separate tree. A successful file seal alone
is not job completion: persistence.manifests requires durable physical stop proof,
zero exit, current attempt/token/session and unexpired lease/deadline, then verifies
sealed files and rechecks the DB clock before the atomic completion commit.
The host fleet_artifacts router uses threads:read + thread ownership + manifest
user/thread filters. Stream the verified descriptor, never reopen it through FileResponse.
Worker publication journals the sealed manifest before complete; a lost accepted
completion is replayable after restart. Unknown/quarantined attempts stay unknown
on late stopped acknowledgements. nas_identity is explicit and the sentinel is checked
before Fleet migrations. B11 public POSIX worker entry is worker/__main__.py:
`python -m deerflow_ecs_fleet.worker --settings <absolute-json>`. Node credentials are
read only from owned private regular no-follow files; bounded settings preflight NAS,
separate private state, and hold an incarnation lock before session bootstrap. Signals
stop the daemon and retained journals must have acknowledged stop/completion; a stopped
zero-exit/server-running record without a manifest still requires recovery and nonzero
exit. Do not print HTTP/Docker/config exceptions that may carry secrets.
The trusted operator module registers nodes without SQL, issues exclusive/fsynced
private credential files, revokes credentials, drains/disables/enables and queries safe
execution/capacity history. It uses existing Fleet locked migrations on an already
bootstrapped host database, never host ORM tables. Existing node budgets are not silently
replaced. Disable rejects charged capacity; enable never requeues unknown work.
The worker-only image copies exact Fleet source plus hashed offline pydantic/httpx
runtime dependencies and a real static Linux Docker CLI. The daemon owns Docker control;
job containers never inherit its credentials/state/socket. Compose publishes no worker
ports and refuses missing host binds; NAS paths stay identical to host paths. Gateway
and workers need compatible NAS UIDs because workspaces are private 0700.
B11 tests cover two actual host CLI workers plus image entry/control and Compose render;
B12 now also verifies actual containerized Compose daemon lifecycle and TCP partition behavior.
See [B local acceptance](ecs-fleet-b-acceptance.md) for exact evidence and remaining
production ECS/NAS and Linux-host limitations.

InputManifests uses the independent f0003_inputs migration and private
fleet_input_manifests table. Thread-owned multipart uploads use threads:write,
existing thread ownership and normal session/CSRF middleware. Each upload creates a
fresh immutable version; same filenames never replace previous versions. Submission
and claim both resolve only same-user/same-thread registered inputs or accepted output
manifests. Frozen launch_spec includes the declared versions' metadata, bounded by
max_input_bytes and the metadata limit. Worker NAS verification precedes copying only
manifest-listed files into attempt-scoped inputs; Docker mounts exactly those IDs
read-only. No entire NAS root or undeclared version may be mounted.


Fleet cancellation is serialized on the job lock with completion. A terminal accepted
result wins later cancellation. The cancellation helper locks node then active attempt,
and uses persisted stopped_at to finish an already stopped job; cancellation intent
alone cannot release resources or claim stopped. Background reconciliation selects
staged rows plus only expired queued rows, ordered by their relevant deadline, so a
large waiting queue cannot hide staged expiry. Node credential revocation rejects
subsequent worker HTTP operations; local daemon stop is independent, and capacity
remains charged until authenticated stop acknowledgement. Unknown never requeues
started execution automatically. B08 operator recovery management is implemented in gateway/routers/fleet_recovery.py
and the private FleetRecovery service. Management requires auth_source=session plus
require_admin_user, derives operator_id from the server principal, and uses normal
CSRF validation. PAT, internal, auth-disabled fallback and node identities cannot use
these routes. A valid administrator session remains required even with auth disabled.

The independent f0004_recovery migration owns fleet_recovery_events. Resolution checks
expected current attempt, unknown/quarantined state, no accepted manifest, durable
stopped_at and a released reservation under job → node → attempt → reservation locks.
Explicit side-effect review and a note are required. The job and attempt become failed
in the same transaction as the unique immutable per-attempt audit record. Repeated
same operator/note requests return the original record; altered requests conflict.
Audit insert failure rolls back state. Never expose an operator flag that invents
physical stop, accepts uncertain output or automatically retries a started attempt.
Closing jobs_enabled preserves reconciliation for accepted work. See
[reconciliation guide](../docs/ecs-fleet-recovery.md).


## Node management and profile admission

The host fleet_management router uses actual session-admin identity and normal CSRF;
PAT, internal, node, fallback and mixed credentials do not authorize it. Fleet package
management.py owns repository operations and never imports app. Register strict
node/capacity/profile inputs, derive registered_by from the administrator, return an
issued credential once with no-store, and check node scope before revocation.

f0006_nodes is a private migration after f0005. Migrated NULL allowlists preserve
legacy eligibility for currently configured operator job profiles. New trusted CLI
registration defaults to concrete configured job profiles; HTTP requires a non-empty
unique configured allowlist with no wildcard. Explicit Agent profiles require a
positive agent_limit; its strict range is 0..1000000 and default is zero. Migrated
NULL allowlists never authorize Agent claims. Scheduler checks membership under the original node lock
before reservation. Drain survives heartbeat/restart; disabled requires no charged
capacity; deletion requires disabled/no execution history and atomically removes only
that node's credentials. Never delete retained execution history to make deletion pass.


## C01 remote Agent foundation

f0007_agents follows f0006_nodes and owns private fleet_agent_tasks,
fleet_run_placements and fleet_launch_specs. The canonical launch payload and digest
are immutable under a PostgreSQL trigger. Ownership/generation keys join only Fleet
tables; host RunStore/Base remain separate. AgentTasks and RunPlacements receive
the caller AsyncSession and flush without committing, for C02 core admission.

LaunchSpec preserves run parameters, streaming, interrupts, recursion and deadlines;
operator profile pins resources and runtime digest. WorkerCompatibility compares the
stored description to current advertised runtime/skill/plugin snapshots. Public
repository summaries exclude input/configuration, snapshot entries and secret refs;
HTTP run/task integration is later work. Model identifiers accept provider/model and
local colon forms; snapshot/model versions support local build/epoch identifiers.
Raw execution credentials belong in out-of-band secret references.

Gateway checks the actual resolved application/checkpoint PostgreSQL identity/schema,
database run events and ownership heartbeat. Legacy checkpointer=None correctly
selects unified persistence. Even complete prerequisites cannot enable agents_enabled
until the real runner and write fences are connected. C01 creates no claims or remote
execution. B v1 job_wire explicitly excludes agent-only runtime_digest so retained
worker images keep their existing strict profile schema.


## C02 core atomic admission

Harness execution/contracts defines RunExecutionParameters, ExecutionPlan and the
RunAdmissionParticipant protocol; LocalExecutionBackend is the default. The host
app/fleet/execution.py adapter receives only trusted operator/backend injection.
services.start_run does not select remote execution from client metadata/config.

RunRepository uses one RunAdmissionUnitOfWork session/transaction, preparing the
Fleet task lock before core run insertion and inserting private launch/placement
records before commit. Errors and cancellation roll back all four tables. Same-key
retry compares the stored immutable inputs and original IDs/deadline; altered owner,
thread, profile, input/configuration, streams, runtime/model version or references
conflict. Prepared goal rows also roll back during reuse. Independent processes
serialize on the database, not a process-local lock.

Remote pending records keep owner/lease unset and store_only true, create no local
task and receive no Gateway heartbeat renewal. Public kwargs contain only the safe
backend/profile/version summary. Memory stores reject participation; existing Local
store calls retain their signature. Input uses the deerflow-normalized-input-v1
state/command envelope, preserving normalized message fields, plain strings and
Command graph/update/resume/goto without lossy string conversion. C04 consumes
the paired decoder. C10 public routing remains pending, and the Gateway activation
guard is unchanged. C03 ownership implementation is locally verified below.


## C03 locally verified ownership foundation

The host FleetRunOwnership bridge claims and renews core run and private attempt
ownership in one SQL transaction. Both rows use fleet-agent:<attempt UUID> and the
same lease expiry. Renewal verifies node/session/token, current task generation,
active reservation, and both unexpired rows. Claim charges CPU, memory and one
agent unit through the shared B ledger. Explicit Agent allowlist, positive capacity,
stored launch compatibility and current operator admission rules gate new claims.
Accepted renewals do not depend on new-work flags or changed operator profiles.

RunRepository intrinsically restricts local recovery/takeover and interrupt/rollback
to absent/local server-owned execution_backend labels. After private migrations,
the host adds a SQL NOT EXISTS placement guard. Scans and mutations enforce these
predicates; hydrated remote records preserve their backend label. Harness imports
neither Fleet nor app. Trusted store_only plans require a valid nonlocal label.

The node-bearer claim endpoint accepts kind=agent plus WorkerCompatibility;
default job requests and grants preserve B wire compatibility. Attempt renewal
dispatches by persisted kind and checks node ownership before revealing availability.
Agent start/stopped remain unavailable pending runner/stop integration; no independent
Agent read/reconcile endpoint or physical-stop capacity release is claimed here.
Raw github_token is now rejected in LaunchSpec, matching the existing runtime-only
credential field; Local execution is unchanged. Future runner credential resolution
must use out-of-band references. Gateway agents_enabled remains fail closed.


## C04 locally verified runner interfaces

The host wheel exposes
`deerflow.fleet.agent_environment:gateway` through installed entry-point metadata.
Operator worker settings select the provider; LaunchSpec and client inputs cannot.
The optional Fleet package discovers the bridge without importing app directly.
Nonsecret preflight calls the same selected factory's worker_compatibility(); missing,
ambiguous and unfit providers fail closed, including in an image without Gateway app.
The bridge builds real SQL/checkpointer/event/definition resources and the existing
RunContext. AgentRunner attaches the admitted run and invokes the existing
`run_agent(run_manager=...)` with the complete frozen execution parameters.

Private operator bootstrap is delivered through bounded stdin to an isolated
Linux `python -I -S` stdlib entrypoint. Nondumpable/core-limit protection precedes
site/package/provider loading; stdio is closed/reset before tools. Model secrets,
MCP configuration and definition stores use trusted scopes. Public AppConfig
contains neither control connection credentials nor private MCP authentication.
Installed provider code, model bindings, bundled skills and activated plugin
distributions participate in actual runtime compatibility. Ordinary Local defaults
retain their existing behavior outside those scopes.

Agent start authorization freezes the execution profile while Core remains pending;
the actual owned SQL start is the transition to running. Attachment/start locks the
actual row first, then checks clock_timestamp() in the same transaction; a lock wait
cannot extend an expired lease. PG/Redis driver parsing excludes decoded, query and
legacy control passwords from private MCP argv/env without requiring Redis when absent. Terminal Core cleanup
may renew only the same live authorized physical owner within the original bounded
leases/deadlines, without changing terminal status. Physical stopped proof precedes
reservation release. A private daemon recovery journal may retain its original
claim bearer in owner-only control state, never on an execution/NAS mount.

These C04 interfaces have local acceptance; the full C release is incomplete. Gateway activation
remains closed; C06 remaining durable mutation fences, C07 event replay, C08 publication and C09
complete cancellation/recovery still follow. The retained B image and job wire must
continue to pass unchanged. See the OpenSpec tasks and implementation progress for
verified evidence; container tests do not establish production ECS deployment.


## C05 locally verified checkpoint interfaces

The trusted factory `make_checkpointer(..., write_fence=...)` installs the neutral
ExecutionWriteFence protocol on the inner AsyncPostgresSaver before cache wrapping.
The actual host FleetCheckpointFence binds the original ExecutionIdentity token
stamp and immutable LaunchSpec. Checkpoint config supplies only the write target;
it cannot choose ownership. Validate using the actual writer cursor under task →
run → placement → node → reservation → attempt locks, then use clock_timestamp()
to check the joined identity, active states, charged resources and equal live leases.
Hold one explicit psycopg transaction through stock SQL and commit/rollback.

The audited installed saver version is 3.1.1. aput, aput_writes and adelete_thread,
plus the same Async saver's external-thread sync aliases, all share that boundary.
Reads retain stock behavior; full/delta materialization retains CheckpointStateAccessor
and CachedHistorySaver. Independent synchronous PostgresSaver remains Local;
unsupported inherited copy/prune/run-delete methods remain unsupported. Unreviewed
saver versions reject remote construction without making Local depend on the extra.

Gateway/Local perform trusted stock schema initialization before Fleet services.
Remote setup executes only read-only schema/migration readiness checks and rejects
missing/stale/unknown versions, wrong columns/keys/indexes or schema fallback. Stock
CREATE INDEX CONCURRENTLY migrations are never placed in a runtime write transaction.
The real run_agent interrupted-title fallback, including late cancellation during
finalization, writes before durable terminal persistence and preserves existing guards.
C04 terminal cleanup renewal still grants no checkpoint write permission.

This is local PostgreSQL/Linux-container acceptance, not production ECS deployment.
C06 memory/events/finalizers and later recovery/routing/continuation slices remain
outstanding; Gateway remote activation stays closed. The implementation progress
records the real race/rollback/identity matrices and independent acceptance evidence.


### Remote Store and definition writes (C06b locally verified)

The trusted host binds optional `mutation_capability` to the actual PostgreSQL
Store and synchronous Agent/ManagedSubagent repositories. The harness contract
remains independent of app/Fleet. Local resources without a bound capability
retain their existing setup, batching and cleanup behavior.

The audited remote Store adapter supports langgraph-checkpoint-postgres 3.1.1.
Its async conveniences call `abatch` in the caller context instead of queuing
context-free operations to the constructor background task; external-thread sync
aliases retain that same context. Put/delete/batch and TTL-refreshing Get/Search
validate the original authority on the stock writer cursor inside one explicit
transaction and hold execution locks through commit/rollback. External embeddings
finish before those locks; SQL entry revalidates freshly, so revocation completed
during embedding prevents the late result from mutating Store/vector rows.

TTL-disabled GET uses pure SELECT; TTL-disabled search and namespace listing are
read-only transactions and remain available after terminal state. Arbitrary Store
namespace semantics are preserved; a namespace position is not an ownership rule.
Global TTL sweeping is unavailable to the execution. Remote setup verifies the
trusted initialized schema, exact migrations, column types, primary keys and
required Store/vector indexes and constraints without DDL or schema fallback.
Only trusted Gateway/Local performs stock schema initialization.

Synchronous definitions validate on the actual SQLAlchemy writer session. Agent
update revalidates in the new transaction after IntegrityError rollback before
its retry read/write. Remote Agent deletion rejects an existing file-memory
directory before SQL and never runs postcommit `rmtree`. Managed subagents retain
the current global-name schema; no user column is inferred. Unsafe file definition
profiles reject before resource construction.

Actual setup/update tools require private bound definition stores and the original
context; missing stores cannot create an unbound Local fallback. Default setup
would write global SOUL.md and therefore rejects before filesystem effects. Typed
OwnershipRejected propagates through the tools, ToolErrorHandlingMiddleware and
installed ToolNode instead of becoming recoverable model output. Ordinary Local
tool-error handling remains compatible.

C06a primary mutations and C06b Store/definition writes have independent local
PostgreSQL/Linux-container acceptance, including frozen installed images and both
formal review stages. This accepts these substeps only; C06c memory, extension,
scheduler and MCP boundaries and later C/BC slices remain outstanding. Full C06
release gates and remote Gateway activation/production deployment remain closed.
Exact source/image identities and commands are in the implementation progress.


## C07 committed remote events (locally accepted)

The original AgentRunner/run_agent path publishes actual SSE frames through
FleetProducerBridge. A neutral DbRunEventStore participant inserts each private
outbox pointer on that same fenced Session; post-write fresh-clock validation
continues through commit/rollback. A private highwater floor under the same
thread lock prevents retained pointers from colliding after host deletion.
The appended f0008 migration uses FleetBase only. Neither the harness nor Local
execution depends on optional Fleet imports.

The Gateway adapter reads joined public events and private pointers directly from
PostgreSQL, independently of Redis availability. Retention of an unconsumed
original public stream frame makes history unavailable: cursorless or valid
cursor preflight returns 410, and an established stream closes without END.
Deleted/malformed cursor validation keeps 400 priority; consumed retention and
semantic journal sequence gaps do not block replay. Preparation checks remaining
history once. Runtime pages select at most128 original pointers before joining
host frames and keep pre-yield checks within that fixed window; later-page loss
is detected when that page is fetched. An exact pointer authority check closes
the original stream after a mapping change, including already prefetched frames.
Only an empty pointer page plus its consumed seal permits END. No private pointer can
recreate a removed public row. Historical reads
use the immutable per-run placement; current task generation and writer lease
remain mutation guards. Cursor validation runs before HTTP headers. END is a
separate narrow closure: the live seal checks the same original registered record
twice around SQL terminal validation, while recovery accepts only a trusted
accepted exit/physical-stop mapping. The stopped transaction and owned startup
scanner share lock order and complete identity checks. No stream closure renews
a writer or broadens events.* terminal permissions.

Gateway installs the C publisher and its seal-recovery callback only when the
Fleet runtime has `agents_enabled=true`. A B-only runtime may use a legacy
memory/SQLite checkpointer with PostgreSQL application storage and Redis without
creating PostgreSQL checkpoint tables. The Fleet bridge remains installed, and
historical C reads retain the same authoritative reader checks when C publication
is disabled. Missing checkpoint storage cannot authorize a historical C read.

A ready Fleet runtime with both new-work flags disabled may have an empty profiles
mapping. Its accepted workspace reader uses the canonical ExecutionProfile output
limit (64MiB) in that case, retaining bounded reads and all accepted-point guards.
Nonempty profiles retain their configured maximum; enabling C still requires valid
profiles. This fallback does not guarantee larger historical outputs remain readable
after their larger profile limits are removed.

The owned publisher selects one trusted earliest pending pointer per run before
its limit, reselects under its private delivery advisory lock, and sends stable
Redis seq IDs with exact decimal comparisons. It never holds execution locks
across Redis. Lost replies/ACK failures retain DB pending state; retries are
idempotent. Connect/socket timeouts and a bounded delivery transaction prevent
unbounded pool occupation; shutdown cancels and joins the original owned Task.
Readers poll authoritative DB state, so delivery hints are an optional transport
and cannot determine completion. Payload/cursor details live in
[RUN_EVENT_STREAM](../backend/docs/RUN_EVENT_STREAM.md).

Installed C07 proof uses a fresh frozen six-wheel image and the actual
`python -I -S /opt/deerflow/libexec_bootstrap.py` entry through NodeDaemon and
AgentContainers. A fixture-only provider can omit the seal after checking real
original closure, then perform original environment.close and naturally exit0;
actual daemon inspect/stopped must trigger production physical-stop recovery.
This is a safe seal-omission injection, not a SIGKILL or arbitrary-crash claim.
Native receipts do not establish Linux installed-entry acceptance. SOURCE SPEC
and SOURCE QUALITY reviews must precede formal C07 image builds; public remote
admission and continuation flags remain closed. C07 is locally accepted; C09–C12
and B/C continuations remain pending. See [C07 acceptance](ecs-fleet-c07-acceptance.md).

Installed C07 and C04 neighbors share the original strict two-model runtime
bindings (`model-1` and `child`) and declared model/MCP secret references. The
deterministic fixture provider's explicit `c07_gate` option selects the C07
barrier response after its original credential, private-scope and skill checks;
it defaults to the C04 path. C07 supplies the actual bundled c04 stdio MCP and
plugin configuration. This fixture option does not change production model
binding validation, Agent bootstrap, execution loops or admission.

C08 has isolated local acceptance, including owner-file access and original
installed full/delta initial, new-turn and branch execution. See
[C08 acceptance](ecs-fleet-c08-acceptance.md) for current versus historical gates and
[C08 runtime contracts](ecs-fleet-c08-runtime.md) for durable thread bindings,
accepted-file reads, new-attempt source selection and branch recovery. The stock single/multi astream paths request sync
durability. A remote root saver callback runs outside the original cursor, SQL
transaction, connection and lock, and materializes full/delta state through the
original accessor. Successful private root presentation turns receive distinct
versions even when the provider reuses a tool-call ID and path; retries of the
same original task remain idempotent. Prepared candidates keep native/MCP gates
closed. Only the actual accepted partial pair commit permits the same original
writer controller and MCP scope to reopen, using the original execution deadline.

A neutral trusted terminal participant joins the three original core repository
transitions on their existing AsyncSession and transaction. Preparation follows
the last duration/history/title/rollback checkpoint; metadata copies preserve
pending writes and rebuild interrupt IDs for the new task namespace. The pair
records the exact immutable root and verified candidate, core terminal outcome,
task accepted point, placement final point and both finishing states atomically.
Fresh clock checks follow locks and SQL flush. Finishing continues charging
thread/capacity until the original authenticated process is physically stopped;
then its immutable desired statuses apply regardless of transport stop reason.
The reason remains an observation in the attempt outcome; it cannot downgrade
an exact accepted final/paused point or prevent physical-stop stream recovery.
A fresh Node session still cannot report an old-session Agent attempt; that
separate recovery boundary remains pending. A partial point or core-only terminal
row cannot authorize final sealing or recovery END. Post-pair bookkeeping uses
that exact accepted final/paused authority and never reopens filesystem gates.

Both original graph interruption and clarification middleware hard END can
represent a pause. A current trusted root human-input observation must match the
materialized successful ToolMessage, original request/card ID and last assistant
tool call for clarification END. Historical/client artifacts, nested graphs and
suppressed clarification cannot grant that authority. Human input waits use
core interrupted/task input_required; other graph pauses use task paused. Their
physical placement/attempt outcome remains cancelled. No automatic replay or
new public resume endpoint is introduced. Remote checkpoint/preparation/terminal
SQL faults retain the first typed failure and block live END; an already accepted
immutable final survives later bookkeeping failure for exact physical-stop
recovery. The original cumulative 120-second final cleanup clock covers all
participants; partial publication must not start it.

Task4 native stock fixtures use authenticated ASGI HTTP and prewritten output
with actual graph/tools/saver/SQL pairing. They do not establish installed Linux
proof. The fresh installed `c08-stock` fixture instead wraps the original host
environment before its first actual presentation callback, observes the original
six supervised writer descendants settled before first publication SQL, and
executes actual post-accept bash/MCP and a second same-path presentation. Its
required full/delta × normal/clarification-pause matrix uses the original
AgentRunner, NodeDaemon, TCP HTTP, container collector, PostgreSQL and NAS.
Source SPEC then QUALITY must accept its exact inputs before image construction.
Installed continuation fixtures read the already created parent through the original
owner-filtered thread repository. A Node restart retaining unresolved work expects
the original RecoveryRequired and remains unable to claim work; fixtures then verify
the old attempt is rejected and accepted partial data stays readable, without replay.
Host-only fixture repairs require a new complete SOURCE review. Existing fresh image
IDs can be retained only when the full prepared image input path sets and bytes
remain identical; earlier failed runtime windows remain failed evidence.

Host continuation tests must install the same immutable fixture memory/provider
wheels as the Runner in their dedicated test environment, keeping UV_NO_SYNC and
the dependency lock unchanged. Real branch POST requests retain internal owner
authentication and original CSRF protection by sending the same original generated
token in csrf_token and X-CSRF-Token. Native branch regressions use the installed
ownership services, RunManager and NAS consistently; HTTP/clone/SQL/preview success
before a controlled launch sentinel is native evidence only. Full installed Runner
execution remains a separate required gate.

First-launch snapshots must establish all approved user-data category directories
even for empty or uploads-only inputs and runs that never acquire a sandbox. The
Node does this through trusted no-follow descriptors after original input validation;
prepared retries validate existing directories and reject removal/symlink changes.
Staged Linux fixtures observe the original presentation callback before its MCP
owners close. A final candidate may follow a legitimate accepted partial; tests
must scope rejection/acceptance assertions to the exact current candidate and retain
prior immutable partial provenance. Diagnostic main-path builds and passing existing
cases precede the deferred full regression/review when the user requests that order;
they do not by themselves establish whole C08 acceptance.
