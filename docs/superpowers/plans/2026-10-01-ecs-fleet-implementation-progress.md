# ECS Fleet implementation evidence

Date: 2026-10-01. Goal: deliver B, then C, then C → B → C continuations.

Implementation has started in the personal-agent-ecs worktree. None of the three
OpenSpec changes has met its release gate; do not archive them or mark IMPLEMENTED.

## Current code

- B01: standalone optional package, strict profiles and identity-free JobSpec;
  disabled installation does not import the host runtime. Host dependency manager
  installation and deployable configuration remain to be exercised.
- B02: eight private fleet_ tables plus independent fleet_alembic_version migration;
  service startup serializes migrations using a Postgres advisory transaction lock.
- B03: host-only bearer authentication for worker routes, persisted hashed node
  credentials, node session fencing and heartbeat. Attempt endpoints and cross-node
  attempt ownership tests will accompany B06. Management routers remain pending.
- B04: FIFO queued-job claims create attempts and reservations in one transaction;
  all unreleased capacity counts, including quarantine. Drain survives heartbeat.
  Stop proof is required before releasing capacity. Node session rotation takes no
  execution locks, avoiding inversion of execution → node lock order.
- B05: staged submission with user-scoped unique key, immutable payload verification,
  owner/thread/handle/driver-scoped tracking handshake, staged timeout, Fleet task
  driver registration and stoppable background reconciliation. Real McpTaskService
  is used; there is no second user task tracking store.

B05 now derives invocation identity from LangGraph's injected ExecutionInfo
(owner/run/checkpoint/task/tool identity). Real graph crash/replay preserves the
identity; the next tool turn changes it even with a reused provider call ID.
Fleet's driver returns a canonical tracking_task_id. The task service opts into
create_idempotent only for drivers that provide this identity, returns the original
matching row after a unique race, and never overwrites its cancellation intent.
Real Postgres tests cover sequential/concurrent retries and tracking commit failure
with both successful and failed compensation. The model-visible Fleet submission tool is implemented in a partial B09 slice;
notification acceptance and scheduled dedupe integration remain pending (B09/B10).

B06 is in progress: claim/start/renew/stopped host routes enforce node, session,
attempt and token identity. The exact operator profile is frozen in launch_spec
when reserving capacity (migration f0002_launch_spec). Start authorization is durable
and idempotent. Expiry requeues only attempts never granted permission to start;
granted attempts become unknown and their reservation is quarantined. Cancellation
stops renewal but is not a physical stop proof.

Host-only Docker control requires a start grant, uses a deterministic container name
and a private fsynced one-shot start journal. It uses non-root execution, immutable
image IDs, read-only root filesystem, no-new-privileges, dropped capabilities, resource
and log bounds, and no Docker socket mount. Monotonic watchdog expiry actually kills
the test container. Repeating launch, including after reconstructing the control
object, never restarts a finished attempt. Stop refuses unrelated containers with
similar names. The daemon/client and private fsynced attempt journal now exist. Bootstrap rotates
node session, verifies journal ownership, discovers only containers labeled for that
node, proves residual executions stopped, replays pending stop acknowledgements, and
requires online health before claim. A failed/uncertain execution closes local claim
admission. Start authorization also bounds its lease by the execution deadline.
Never-authorized attempts can be safely requeued and their stop acknowledgement is
idempotent even after clearing active_attempt_id. Late renewal replies cannot extend
a local lease past their send-time bound.

Real TCP HTTP → authenticated host routes → Postgres → worker → Docker tests cover:
(1) Gateway shutdown while an actual counter container runs, local stop despite lost
renewals, quarantine before stop acknowledgement, restart replay and blocked claim;
(2) a committed start grant whose HTTP response is dropped, no container launch,
unknown state, released capacity after stop proof, and blocked subsequent claim.
Private journal tests reject public permissions, symlinks and mismatched identities;
foreign-node journals block engine access and claim. Duplicate launch remains covered
by the real Docker component test. Startup now attempts every owned residual stop
before reporting a missing journal or engine failure. Real Docker tests prove three
owned orphan containers are stopped while a foreign node container stays running;
a failed stop RPC does not prevent stopping other owned residuals and keeps claim
blocked. The live HTTP/Postgres daemon loop starts two containers concurrently,
stops both on shutdown, persists both stop acknowledgements, releases both resource
reservations, leaves both uncertain jobs unknown and the third job queued. Public
worker/credential/NAS deployment integration remains pending; B06 is not accepted
as a deployable whole.

B07 filesystem foundations now exist: NASWorkspace requires an explicit deployment
identity in a non-symlink `.deerflow-fleet-root` sentinel; it never creates a missing
NAS root or falls back to a local empty directory. Every path component uses dirfd
and no-follow traversal. Outputs are scoped to owner/thread/job/attempt and bound to
server start-grant job_id/output_prefix. Stopped outputs are copied to a separate
sealed tree, with non-writable files/directories and bounded entries, depth and bytes.
Symlinks, hardlinks and non-regular files are rejected. Reads verify manifest size
and digest on the same open descriptor subsequently returned to the reader; replacing
the pathname cannot redirect that reader. Missing sentinel after prepare also blocks
seal. These synchronous filesystem operations must be called through asyncio.to_thread.

B07 accepted-manifest service integration now exists. Enabled jobs require explicit
nas_identity; service startup validates the NAS sentinel before migrations/ready.
Completion locks job → node → attempt, authenticates node/session/token, requires the
active stopped zero-exit attempt and a live lease/deadline, verifies the sealed NAS
manifest/files in a thread, then rechecks the Postgres clock before atomically inserting
the manifest and finishing the job. Concurrent/repeated completion returns one accepted
manifest, including a replay after lease expiry; changed completion data is rejected.
Unknown/quarantined execution cannot be revived by a late stopped acknowledgement.

The host complete route uses node bearer authentication; GET manifest/download routes
use the existing threads:read permission and real thread-store ownership checks plus
manifest user/thread filters. Downloads stream the verified open descriptor, force
attachment/octet-stream/nosniff and expose no internal NAS prefix. NAS reads do not
block the event loop. The worker seals only after physical stop, journals the exact
manifest before complete, and can replay a lost accepted-completion response after
restart without another execution. Existing McpTaskService polling receives completed
and the accepted manifest ID through the original tracking row.

B07 immutable input registration/read-only mounts are implemented and exercised.
B08 cancellation/recovery is locally accepted. B09 controlled submission and host
binding are verified; full notification acceptance remains pending. B10–B12, all C
and continuation tasks remain pending. There is no public runnable worker deployment.
No remote Agent run has executed and no business ECS has been deployed.

## Verification evidence

Tests use a temporary local Postgres 16 instance on loopback and random schemas;
they do not touch business databases or NAS. TEST_POSTGRES_URI must target a test DB.
Run from backend using the existing venv, with optional package path injected by
fleet/conftest.py:

```bash
TEST_POSTGRES_URI=<local-test-uri> PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/fleet tests/test_mcp_task_service.py -q -p no:cacheprovider
```

Earlier foundation/driver checkpoint: 79 passed, zero skipped. Current combined verification is recorded below.
Current combined verification (2026-10-02): 448 passed, zero skipped, four existing
Starlette/httpx and uvicorn/websockets deprecation warnings. FLEET_TEST_CONTAINERS=1 enables real Docker
alongside TEST_POSTGRES_URI for isolated Postgres schemas. Exact regression scope:

```bash
FLEET_TEST_CONTAINERS=1 TEST_POSTGRES_URI=<local-test-uri> PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/fleet tests/test_mcp_task_service.py tests/test_mcp_task_repository.py tests/test_mcp_task_models.py tests/test_mcp_task_ordinary_driver.py tests/test_mcp_task_tool_wrapping.py tests/test_mcp_tasks_router.py tests/test_auth_middleware.py tests/test_csrf_middleware.py tests/test_pat_auth.py tests/test_extension_config.py tests/test_extension_api_contracts.py tests/test_gateway_startup.py tests/test_gateway_lifespan_shutdown.py tests/test_authorization_tool_filter.py tests/test_extension_app_loading.py -q -p no:cacheprovider --tb=short
```

This is component and adjacent regression coverage, not the B release gate or the
entire backend suite. Docker tests use existing local Alpine content IDs, unique
fleet-UUID test container names, private temp state/output paths, and finally remove
only their own test containers.

RED → GREEN was observed for the node-session lock inversion, B04 atomic capacity,
B05 staged submission, generic task driver and host startup binding. Tests call real
SQL repositories and HTTP middleware directly; a generic FleetProbe is unnecessary.

Scoped Ruff check and format pass. Standalone hatchling wheel build succeeds; wheel
output is kept in /private/tmp, outside the repository. Build is not a release gate.

## Next steps

1. Integrate B06 private credential loading and operator worker startup with
   B07 workspace preparation and B11 reproducible deployment. Core startup orphan,
   engine stop failure and concurrent shutdown scenarios now have real evidence.
2. Complete B09 full scripted-run and durable result notification acceptance.
   B08 cancellation, deadline reconciliation and audited operator closure are verified. B07 NAS sentinel, immutable inputs,
   read-only mounts, accepted manifests and user download/result publication are exercised.
3. B09 now wires durable invocation identity into controlled model submission; B10
   still needs scheduled dedupe. Deliver B11/B12 before enabling C. Keep C/continuation flags off.

## B06 daemon slice evidence

RED was observed before daemon implementation (explicit missing-daemon assertion),
for ungranted restart release and node-bound start grants, for repeated stop confirmation,
for delayed renewal, and for claim admission after the committed start response was lost.
The final command above passes 319 tests, zero skipped. These are isolated real local
services and containers; no production ECS or NAS deployment is implied. The public
worker command, NAS sentinel/authorized inputs and sealed result delivery remain pending.

## B06 residual/shutdown slice evidence

Previous daemon slice: commit 36210a8a. Startup-orphan RED: the first owned residual
was stopped but the other two remained running. GREEN now stops all three and never
touches the foreign-node test container. Injected engine stop-RPC failure verifies
other real owned containers are still stopped and claim remains blocked. Live loop
shutdown evidence uses actual TCP host routes, isolated Postgres schemas and two
concurrent Docker counter containers. The combined command above passes 322 tests,
zero skipped, four deprecation warnings. No business containers or data are touched.

## B07 filesystem slice evidence

B06 residual/shutdown slice: commit bc784e08. B07 test_b07_workspace.py had 14 RED
assertions before the filesystem implementation existed; a further foreign-job RED
proved claim tampering could redirect prepare without a job-bound start grant.
Sixteen filesystem tests now pass, covering missing/wrong/symlink sentinel, prefix
escape, owner/attempt/job mismatch, parent symlink, post-prepare sentinel disappearance,
separate attempts, stopped-only immutable copies, descriptor-stable reads, unsafe
file kinds and byte limits. Combined real Fleet/adjacent regression is 338 passed,
zero skipped. This is filesystem evidence, not B07 complete/DB/HTTP acceptance.

## B07 accepted-manifest/publication slice evidence

Filesystem baseline: commit 07febc50. Fourteen manifest RED tests were observed before
FleetManifests existed; two HTTP RED tests preceded the host artifact router; real
worker publication RED proved the missing workspace/publication integration. A further
unknown-before-stop RED exposed and fixed resurrection into running. Final suite:
371 passed, zero skipped, four deprecation warnings. Scope is the combined command
above, now including test_mcp_tasks_router.py. Entire backend Ruff lint passes and
format --check verifies 1350 files; no production deployment or full-backend test gate
is implied.

Real Postgres tests cover five concurrent completes, exactly one accepted row,
idempotent expiry replay, user/thread mismatch, late/unknown/noncurrent/cancelled and
nonzero-exit attempts, token/node/session mismatch, missing physical stop, output
budget, bad prefix/size/digest/symlink, NAS disappearance and expiry during NAS I/O.
Wrong/missing/symlink NAS identity fails before schema migration. Closing jobs_enabled
on service restart preserves accepted-work completion and owner reads.

Real TCP host tests exercise node completion, repeat response, owner manifest/download,
401 anonymous, 403 node credential download, 404 cross-user/thread/symlink and 409
changed completion. They use actual Postgres thread metadata and the existing host
permission/middleware flow; the session-user resolver is a test stub rather than a
live login provider. Real Docker/TCP/Postgres worker tests execute the report once,
seal and accept the output, poll the real McpTaskService to completed, and replay a
committed completion with a deliberately lost response after restarting the worker.
The attempt/manifest counts remain one; no business ECS/NAS data is accessed.


## B07 immutable-input slice evidence

Publication baseline: commit 0c9dd640. Input-registry RED preceded implementation;
HTTP upload/metadata RED preceded the host router. Real Docker input execution RED
preceded readonly mount integration. Combined regression: 384 passed, zero skipped,
four existing deprecation warnings. Entire backend Ruff lint passes; format --check
verifies 1354 files. All three OpenSpec changes pass strict validation.

The private f0003_inputs migration owns fleet_input_manifests. Fresh upload IDs pin
immutable filenames/size/digests; registration, submission and claim enforce owner,
thread and byte/metadata limits. Code versions share the same authorization boundary.
Missing, oversized, duplicate or escaping files cannot create a submission. Worker
rejects changed source bytes, symlinks, foreign prefixes, missing or extra snapshots.
Real HTTP tests cover session + CSRF + thread ownership, distinct version IDs,
metadata reads and node-bearer upload rejection. Session resolver remains a stub;
thread metadata, permissions and PostgreSQL are real.

Real Docker executes the declared code version, reads a specified older input,
rejects writes to readonly inputs and cannot see an unrequested newer version.
Mount inspection proves only outputs are read-write. Lost accepted-completion replay
still executes once. A second real job reuses the first accepted output manifest as
its authorized readonly input and produces another accepted output. The first job's
tracking remains canonical. No production ECS/NAS is accessed. Public worker startup,
B08–B12, C and continuations remain pending; no proposal is release-complete.


Independent input review found and reproduced upload-version relabelling and unlisted
materialization content. Three new RED cases (wrong version, extra file, extra empty
directory) preceded the fix. Workers now bind uploaded prefix identity to version ID
and reject the complete destination inventory unless it matches manifest files and
their parent directories. Focused PostgreSQL/Docker validation: 15 passed. Independent
re-review confirmed both findings resolved and found no new blocker. Full combined
regression after the fix: 384 passed, zero skipped, four existing warnings.


## B08 cancellation/deadline slice evidence

B07 baseline: commit 9b376c05. Two state RED assertions preceded implementation:
confirmed stopped-but-unaccepted job remained running after cancel; expired queued
job remained queued with no available node. A third RED reproduced 101 not-due queued
jobs starving expired staged reconciliation. GREEN now respects durable stop evidence
and filters background candidates by the relevant deadlines. Six PostgreSQL tests
cover these cases, accepted-complete-before-cancel, running/unknown cancellation and
reservation retention/release. Both concurrent APIs serialize on the durable job lock.

Two additional real TCP/Postgres/Docker scenarios extend test_b06_fleet_durable_jobs:
(1) cancel the original McpTaskService row, physically stop the actual counter container,
hold the stopped HTTP request before its DB commit and observe non-cancelled state,
no stop proof and unreleased capacity; release it and observe cancelled plus the same
single original task row and stable counter file; (2) revoke the actual node credential,
observe local stop, rejected stale worker requests, wait for the real PostgreSQL lease
expiry, then observe unknown/quarantine; a fresh credential's restart replays physical
stop, releases capacity but still blocks claim and does not start another attempt.

Independent code review approves this slice: lock order, accepted-result authority,
unknown non-retry and deadline candidate selection. Combined Fleet/adjacent verification:
392 passed, zero skipped, four existing warnings. Backend Ruff lint and format check
pass (1356 files). OpenSpec strict validation passes all three changes. Whole B08 remains
partial until operator recovery management is delivered; B09–B12/C/continuations are
pending and no production deployment or B release acceptance is claimed.


## B08 audited operator recovery acceptance

Cancellation baseline: commit 2e8a92a3. Eight recovery RED assertions preceded the
private recovery service; two HTTP RED assertions preceded the host router. Independent
f0004_recovery adds fleet_recovery_events, separate from host metadata. Resolution locks
job → node → attempt → reservation, checks expected current uncertain attempt, durable
stopped_at and released capacity, requires explicit side-effect review and a bounded
note, and atomically closes job/attempt as failed with one immutable audit event.
Concurrent identical requests and service reconstruction return the same record;
changed request/attempt, missing stop, unreleased capacity, absent review or successful
accepted output reject. Injected audit insert failure rolls back both states. Late
stopped/complete replies cannot rewrite the original outcome or accept success.

Host GET /api/fleet/recovery/jobs, POST /{job_id}/resolve and GET /{job_id}/events
require actual administrator session and normal CSRF. Actor identity is server-derived;
forged body actor, boolean coercion and unknown fields reject. Anonymous/member/node,
trusted internal and auth-disabled fallback identities cannot use the management
boundary. Session resolution is a test stub; real middleware/permissions, TCP and SQL
execute. Closing jobs_enabled preserves existing-work recovery after actual Fleet
service restart.

The real credential-revocation Docker scenario now completes the recovery loop:
unknown/quarantine → current authenticated stop replay → capacity released but claim
blocked → original McpTaskService row input_required → audited operator resolution →
the same tracking row failed → worker bootstrap/online admission restored. Actual
counter remains one start and attempt count remains one. Two additional concurrent
cancel/complete tests hold the real job lock through explicit filesystem/cancellation
barriers and verify both commit orders, with one accepted manifest or none as appropriate.

Independent read-only review found no correctness/security blocker. Final combined
regression scope recorded above: 406 passed, zero skipped, four existing warnings.
Entire backend Ruff lint/format pass (1361 files); all three OpenSpec changes pass strict
validation. README, backend AGENTS, shared contract and docs/ecs-fleet-recovery.md match
implemented behavior. B08 is locally accepted; B09–B12 and C/continuations remain
pending. This is not the B release gate, full backend test gate or a production ECS/NAS
deployment. Previous pending statements are historical checkpoints.


## B09 controlled submission slice — 2026-10-02

The core submitter bridge and `submit_fleet_job` builtin now bind through host
startup after ready Fleet service and persistent MCP task tracking validation.
No optional-package or app import enters the harness. Approved job profile names
are discoverable without exposing deployment settings. Server graph context owns
user/thread/run/invocation identity; B remains detached. Closing new-job admission
hides the tool while the Fleet driver continues polling accepted jobs.

Observed RED/GREEN covers missing tool/runtime binding, missing profile discovery
and unconditional tool visibility with subagent support enabled. Real Postgres plus
a checkpointed ToolNode graph proves a replay after post-submit response loss creates
one job/tracking row; a later turn reusing the provider call ID creates a second job.
Independent review found the unconditional subagent tool registration; it was removed
and re-reviewed after tests exercised both subagent configurations and single registration.

Focused tool/authorization checks: 17 passed. Final expanded Fleet/adjacent scope:
448 passed, zero skipped, four existing warnings, 70.76 seconds. Backend Ruff lint
and format check (1365 files) and git diff whitespace check pass. This slice does
not yet prove full run_agent submission followed by worker completion, busy-thread
notification retry, service restart and exactly one accepted notification receipt.
B09 remains partial; its full acceptance checkboxes and B release gate remain open.


### B09 uncertain-status disclosure regression — 2026-10-02

Unknown/quarantined Fleet snapshots previously included the internal job handle in
input_required, which the user task detail and notification event forwarded. Two
observed RED tests reproduce that disclosure using the actual public projections.
The adapter now returns reconciliation instructions without that handle; public
tracking ID and honest uncertain state remain available. Focused Fleet driver,
cancellation, public status and MCP task route regression: 19 passed. Scoped Ruff
lint and format checks pass after import sorting. Full B09 notification acceptance
remains pending; this fix does not alter internal ownership or operator recovery.
