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
full containerized daemon deployment and B12 remain pending. See ../docs/deployment/ecs-fleet.md.

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
registration saves concrete configured job profiles; HTTP requires a non-empty unique
allowlist with no wildcard. Scheduler checks membership under the original node lock
before reservation. Drain survives heartbeat/restart; disabled requires no charged
capacity; deletion requires disabled/no execution history and atomically removes only
that node's credentials. Never delete retained execution history to make deletion pass.
