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
Tests create/drop random schemas, never real business schemas. Worker client/daemon
and private restart journals exist; tests exercise real TCP Gateway loss and a lost
start-grant response with local Docker. Bootstrap must finish stop reconciliation
before claiming work. Stop every owned residual before reporting recovery failure;
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
