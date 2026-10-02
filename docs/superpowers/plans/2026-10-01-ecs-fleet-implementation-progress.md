# ECS Fleet implementation evidence

Date: 2026-10-01. Goal: deliver B, then C, then C → B → C continuations.

B has met its isolated local release gate; C and BC remain incomplete. Current status:
[B acceptance](../../../docs/ecs-fleet-b-acceptance.md). Dated historical sections below
do not override the final matrix.

## Initial foundation snapshot (historical)

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
notification acceptance is locally verified; scheduled dedupe remains pending (B10).

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
B08 cancellation/recovery and B09 controlled submission/result notification are
locally accepted. B10–B12, all C
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
Current combined verification (2026-10-02): 459 passed, zero skipped, four existing
Starlette/httpx and uvicorn/websockets deprecation warnings. FLEET_TEST_CONTAINERS=1 enables real Docker
alongside TEST_POSTGRES_URI for isolated Postgres schemas. Exact regression scope:

```bash
FLEET_TEST_CONTAINERS=1 TEST_POSTGRES_URI=<local-test-uri> PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/fleet tests/test_mcp_task_service.py tests/test_mcp_task_repository.py tests/test_mcp_task_models.py tests/test_mcp_task_ordinary_driver.py tests/test_mcp_task_tool_wrapping.py tests/test_mcp_tasks_router.py tests/test_auth_middleware.py tests/test_csrf_middleware.py tests/test_pat_auth.py tests/test_extension_config.py tests/test_extension_api_contracts.py tests/test_gateway_startup.py tests/test_gateway_lifespan_shutdown.py tests/test_authorization_tool_filter.py tests/test_extension_app_loading.py tests/test_runtime_lifecycle_e2e.py -q -p no:cacheprovider --tb=short
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
2. Deliver B10 trusted scheduled-job dedupe and truthful task UI. B09 scripted-run
   submission and durable result notification are locally accepted.
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


## B09 real submission and notification acceptance — 2026-10-02

`test_b09_fleet_job_integration.py` replaces only the external model call. A real
lead-agent HTTP run executes `submit_fleet_job` through start_run/run_agent and
finishes before the worker runs. A real TCP worker, local Docker and isolated
Postgres produce one sealed accepted manifest; its actual count file contains one
start and its report contains the expected worker output. The original tracking row
retains the authenticated source user/thread/run identity.

A real checkpoint-write admission reservation keeps completion notification pending
without counting busy admission as a failure. The task service is reconstructed;
a notification launch is committed through the real start_run path and its response
is deliberately lost. A second task-service reconstruction retries the stable key
and receives the same persisted run. Direct host SQL inspection finds one successful
notification run and one owner-scoped run.delivery receipt; Fleet SQL records one
job, attempt, manifest and delivered event version. Replaying the same launcher
again preserves those identities. Public task detail, thread snapshot and notification
event contain neither the node credential, NAS prefix nor private job handle.

This is new integration GREEN evidence on the existing protocol. Genuine RED/GREEN
was observed earlier for the missing controlled tool/bridge/host binding, profile
visibility and uncertain-status disclosure; integration test fixture corrections
are not counted as feature RED. Focused chain: 1 passed, two upstream websocket
warnings, 6.05 seconds after cleanup review. Fleet/adjacent plus real runtime lifecycle regression:
459 passed, zero skipped, four existing warnings, 47.71 seconds. Full backend Ruff
lint and format check: 1367 files clean; OpenSpec strict validation: 3/3; whitespace
check clean. Independent reviews assess the actual implementation and evidence.

Scope: Fleet and tracking use isolated Postgres; host runs/events use isolated
SQLite with the database event-store backend. These are task-service restarts while
Gateway stays running, not full-process restart or business ECS/NAS deployment.
B09 is locally accepted. B10–B12/B release gate and all C/continuation tasks remain
pending; no change is archived or marked IMPLEMENTED.

B09 final review: admission is held before the actual Docker job completes. Cleanup
now guarantees Fleet shutdown despite earlier cleanup errors and bounds TCP server
shutdown; the focused real chain passed again after that change. Spec and quality
re-reviews approve the local slice. Acceptance commit: `test(fleet): verify real job notification lifecycle`.


## B10 scheduled named slots and truthful task visibility — 2026-10-02

FleetConfig.scheduled_job_slots defaults empty and maps bounded operator names to
existing job profiles. The model can select an approved slot but cannot supply a
schedule identity or dedupe group. The actual scheduler forwards its context mode;
only internal launch passes trusted_schedule_id/trusted_schedule_mode to start_run.
Raw context and configurable copies lose both reserved keys, including auth-disabled
external callers. Fleet scheduled jobs require reuse_thread; fresh_thread_per_run is
rejected before the first submission, preserving canonical thread tracking.

A per-owner schedule+slot advisory transaction lock precedes job locks, including
empty groups. Original invocation retry is checked first, even after a terminal cycle
or newer occurrence. New occurrences reuse staged/queued/claimed/running/unknown/
quarantined work; pending cancellation still occupies the slot. Reuse keeps original
arguments, tracking ID/name/source run. TaskSubmission.reuse_existing selects a
read-only host lookup outside compensation; missing original tracking causes a retry
without cancelling older accepted work. The original invocation can still repair its
own uncommitted tracking row. Terminal work permits a new occurrence.

Quality review caught a lost-response replay gap in cross-occurrence reuse. Two genuine
Pg REDs showed replay binding to a new job after the original became terminal or a
newer cycle was active. f0005_job_invocations now persists immutable owner/key/thread/
source run/requested spec/group -> canonical job receipts for every admission decision,
including reuse, and backfills existing canonical jobs. All submissions serialize
owner+invocation before group/job locks; receipt lookup precedes active-group lookup.
Replay never changes its job binding; conflicting request identity is rejected. Host
service coverage preserves the original terminal tracking/name/run without compensation.

Public task statuses remain unchanged. execution_uncertain is additive and specific
to Fleet input_required + execution_unknown. The UI maps it to Needs confirmation /
需要确认, retains active polling and pending cancellation, preserves degraded tracking,
and gives terminal status precedence over stale flags. Ordinary MCP input requests
and old payloads without the flag keep their prior presentation. No machine UI added.

RED evidence: seven initial backend failures covered absent slot config/public flag
and real Pg active-group uniqueness failures; host canonical group reuse, trusted
context helper and tool schema also had behavior failures before implementation.
The mode review fix had three further REDs: missing actual scheduler mode forwarding,
fresh mode reaching submission, and raw forged mode surviving scrub. UI pure mapping
had ten RED assertions before implementation; actual card DOM tests had two behavior
failures for unknown badge/details before wiring, alongside two existing behavior passes.
The full runtime acceptance was added GREEN; fixture/collection failures are not RED.

Actual runtime acceptance extends test_b09_fleet_job_integration.py with
test_real_scheduled_slots_http_boundary_and_one_docker_execution. It exercises the
production internal launch and original run_agent; only external LLM replies are
scripted. Four forged external HTTP payloads cannot establish schedule identity/mode.
Two internal scheduled occurrences complete their Agent runs while the original job
remains unfinished. Both return original tracking/name/source run. The real TCP node
client and Docker job write the counter once; Pg records one job, attempt, accepted
manifest and tracked notification. This is local isolated PG/SQLite/NAS fixture/Docker,
not production ECS/NAS or a full Gateway process restart.

Existing B07 input test now waits at most two seconds for a claim rather than asserting
on its first SKIP LOCKED result; background reconciliation may briefly hold that row.
It still requires actual admission and verifies the frozen input snapshot. No scheduler
business behavior was changed for this test stabilization.

Frontend: 26 passed, zero skipped/todo; pnpm check (lint + tsc) passed. Dependencies
installed offline with frozen lockfile from local cache; no dependency/lock changes.
UI specification and quality reviews approved. Backend specification review approved
supported reuse_thread scope after early-mode rejection. Final backend quality review
and expanded configured regression are recorded below after completion.

Final review: backend specification and quality re-review approve the immutable receipt
fix. Focused PG B10/foundation/driver: 26 passed, including a populated f0004→f0005
migration. Real Agent/TCP/Docker B09+B10: 2 passed, two known deprecation warnings.
The final expanded configured regression: 679 passed, zero skipped, four existing
warnings. Backend Ruff check and format check pass (1370 Python files).
OpenSpec strict validation: 3/3 changes pass. git diff --check passes.

Final backend command (from backend, local isolated fixture URI supplied):

```bash
PYTHONDONTWRITEBYTECODE=1 TEST_POSTGRES_URI=<local-test-uri> FLEET_TEST_CONTAINERS=1 .venv/bin/python -m pytest tests/fleet tests/test_mcp_task_service.py tests/test_mcp_task_repository.py tests/test_mcp_task_models.py tests/test_mcp_task_ordinary_driver.py tests/test_mcp_task_tool_wrapping.py tests/test_mcp_tasks_router.py tests/test_auth_middleware.py tests/test_csrf_middleware.py tests/test_pat_auth.py tests/test_extension_config.py tests/test_extension_api_contracts.py tests/test_gateway_startup.py tests/test_gateway_lifespan_shutdown.py tests/test_authorization_tool_filter.py tests/test_extension_app_loading.py tests/test_runtime_lifecycle_e2e.py tests/test_gateway_services.py tests/test_scheduler_config.py tests/test_scheduled_task_service.py tests/test_scheduled_task_lifecycle.py tests/test_scheduled_task_queue.py tests/test_scheduled_task_claims.py tests/test_scheduled_task_dispatch_race.py tests/test_scheduled_task_postgres.py -q -p no:cacheprovider --tb=short --show-capture=no
```

Frontend commands (repo root, both PASS; 26 scoped tests):

```bash
python3 scripts/pnpm.py rstest run tests/unit/core/background-tasks tests/unit/components/workspace/thread-background-tasks.dom.test.tsx
python3 scripts/pnpm.py check
```

B10 acceptance covers supported reuse_thread scheduled Fleet slots. No production
worker/ECS/NAS deployment or remote Agent execution is claimed. All feature flags
remain disabled by default; B11 deployment and B12 release gate remain pending before C.
Implementation commit: `6236636c` — `feat(fleet): deduplicate scheduled slots with durable invocation receipts`.


## B11 public worker/operator and offline image — 2026-10-02

Public POSIX worker entry reads bounded JSON settings and an owned/private regular
credential file, validates NAS identity, separates private persistent state, and locks
one daemon incarnation before opening a session. Signals request graceful stop; shutdown
fails nonzero if a stop acknowledgement or completion is pending. Spec review caught
and fixed the stopped-zero-exit/server-running/no-manifest gap; it now uses the same
completion recovery condition as bootstrap. Existing journals remain available for replay.

The trusted operator CLI uses private database settings and existing locked Fleet
migrations on the existing host schema. Public register/issue/revoke/drain/disable/
enable/status commands replace handwritten SQL. Issue reserves an exclusive mode600
file, fsyncs bytes/directory, prints only credential ID, and revokes on failed delivery.
Register does not overwrite an existing budget; disable rejects charged capacity.
History, stopped proof and unreleased resources remain queryable after disabling.

Two actual host CLI workers run against real TCP/Gateway node routes, isolated Postgres,
NAS fixture and Docker. Old protocol and stale claims are rejected; draining node has
zero starts; the active node executes one script/output. Closing admission while it is
charged preserves accepted work, and disabling charged capacity fails. Accepted results
remain recorded after completion/disable. Real SIGTERM during a running Docker job
proves stop, durable acknowledgement and release_stopped capacity while unknown history
remains retained. Correct physical-stop release assertions are not labeled feature RED.

Genuine RED: two missing runnable-entry assertions, absent public registration/CLI,
missing enable choice, unreported shutdown and the stopped-zero-exit completion gap.
Fixture/dataclass corrections and initial incorrect quarantine-after-proved-stop test
expectations are not behavioral RED evidence. Final focused B11: 20 passed, zero skipped,
two existing websockets deprecation warnings, 10.59 seconds.

The worker-only image copies the exact Fleet source, installs hashed offline pydantic/
httpx wheels, and contains the verified real static Linux Docker CLI. No Gateway or C
entry is provided. A Dockerfile-specific whitelist excludes unrelated config/runtime data.
Image entry --help and real DockerContainers.inspect through its Linux CLI/socket are
verified; actual PortBindings are empty. Rendered Compose preserves identical absolute
host/NAS path and has zero worker ports, private state/credential binds and no auto-created
host paths. Frozen CP314/Linux aarch64 artifact reference lock and complete build commands
are in docker/fleet/README.md; other platforms need their own reviewed matching artifacts.

Actual offline build base:
python@sha256:51dafde81dbdb6ebde285137a295cf18a47ca95234fe388a343719cb97305b3d
Static CLI source: https://download.docker.com/linux/static/stable/aarch64/docker-29.2.0.tgz
CLI archive SHA256: e1590e656abaf2dfe8a1d724d99b82644446fe24844f438ea000e08006774717
Final local worker image ID: sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8
Built worker/__main__.py SHA256 equals final source:
b3a24d92198f16f436169be841b4067424b958eaf73cb3e8e1b7ebd9b38a9f06

Final expanded B01–B11/Gateway/notification/scheduler regression: 699 passed, zero
skipped, four existing warnings, 66.07 seconds. Command is the B10 expanded command
above with these additional explicit prerequisites:

```bash
FLEET_TEST_WORKER_IMAGE=sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8
FLEET_TEST_DOCKER_SOCKET=<local-host-docker-socket>
```

FLEET_TEST_CONTAINERS=1 and the isolated TEST_POSTGRES_URI remain required. B11 entry,
real worker integration and image files are included by tests/fleet. Full Ruff check and
format check pass (1375 Python files), diff whitespace passes, OpenSpec strict validates
all three changes. Spec and quality reviews approve the scoped local deployment slice.
B10 replay message follow-up 9cdd7924 also has 26 passing graph/PG tool tests; wording
preserves terminal truth instead of calling a replayed completed submission active.

Scope: two supported host CLI workers, actual image entry/Docker control, and rendered
Compose. Full containerized Compose daemon execution and production ECS/NAS are not
claimed. B12 must finish the release fault/gate acceptance before C. Feature flags remain
disabled by default; no production credentials/images are supplied or deployments made.
Implementation commit: `054d7007` — `feat(fleet): add public worker entry and reproducible deployment helpers`.


## B12 foundation audit and release-gate follow-up — 2026-10-02

Read-only audit confirms later B07–B11 cover earlier B03–B06 component/deployment
obligations; unchecked historical milestones still require an explicit evidence map.
Two startup gaps remain real: optional install errors can silently omit enabled
Fleet, and jobs_enabled=false bypasses the ready-runtime/durable-tracking check.
Supported Gateway deployment must declare required:true and table_prefix:fleet_,
and enabled Fleet must retain a ready service and durable tracking after admission
closes. Generic optional extension semantics need not change.

Before B acceptance, exercise the real extension-manager local install/entrypoint
and packaged migration assets in an isolated checkout, plus host autogenerate table
protection. Existing source-path fixtures are not that packaging evidence. Complete
containerized Compose daemon execution, control-partition external side-effect
observations and an explicit zero-skip release runner are also pending.

Required blocking-I/O target executed with PYTHONDONTWRITEBYTECODE=1 UV_NO_SYNC=1
make test-blocking-io: 75 passed, zero skipped, two existing warnings, 5.08 seconds.
Full offline make test result and scoped follow-up are recorded below. B12 and overall B/C/BC
remain incomplete; the audit itself is not implementation acceptance.


## B12 TCP partition and explicit test runner slice — 2026-10-02

The loopback fault proxy closes existing control connections and rejects new ones
while the independent real Docker job remains running. A validated 30-second lease
and 90-second execution budget distinguish lease watchdog stop from job timeout.
An isolated HTTP mock runs in a separate non-root container with no published port.
Actual output and external-request files each contain one effect. Docker inspect
proves physical stop while the database lease is still live; permitting a second
effect afterwards does not create it. After expiry, PG records unknown/quarantine;
restart bootstrap replays stopped acknowledgement and releases proved-stopped
capacity, but preserves unknown and blocks admission. Submission replay preserves
the canonical tracking/job; one attempt and zero untracked authorized starts persist.
This fault test adds real integration coverage to the existing production protocol;
it did not expose a new protocol RED. The lease and side effects are actual observations.

The explicit runner validates PG/Docker/image/socket prerequisites, fixes selection
to B01–B12 plus its own behavior tests, clears inherited PYTEST_ADDOPTS and configured
addopts, runs serial pytest and validates a freshly generated actual JUnit report.
Empty/skipped/failed/error/missing-module reports fail; actual testcase count wins
over XML summary claims. A new retained-report path is exclusive. Future C/BC tests
do not enter B. Genuine gate RED/GREEN covers nine absent checks, missing module,
then B00/B13/C/BC selection. Final pure gate tests: 11 passed, zero skipped.

Root ran the final files with deliberately hostile PYTEST_ADDOPTS='-k nonexistent':
213 passed, zero skipped, two existing warnings, 80.11 seconds. Actual report retained
at /private/tmp/fleet-b12-root-gate.xml (local artifact); exact invocation:

```bash
PYTHONDONTWRITEBYTECODE=1 TEST_POSTGRES_URI=<isolated-postgresql+asyncpg-uri> FLEET_TEST_CONTAINERS=1 FLEET_TEST_WORKER_IMAGE=sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8 FLEET_TEST_DOCKER_SOCKET=<actual-absolute-local-socket> PYTEST_ADDOPTS='-k nonexistent' backend/.venv/bin/python scripts/fleet_b_gate.py --report /private/tmp/fleet-b12-root-gate.xml
```

Full default offline target (initial attempt, UV_NO_SYNC=1 UV_OFFLINE=1): 13146 passed,
168 skipped, one deselected, six failed, 307.98 seconds. Failures include missing
Fleet modules in the existing thread-ID route sweep and an oversized backend guide.
The sweep now includes real Fleet input/artifact 422 cases. Detailed Fleet contracts
moved intact to docs/ecs-fleet-development.md; shared durable MCP detail moved to its
existing module guide, preserving all obligations within the backend soft budget.
These two whole regression files pass: 73 tests, one existing warning, 2.06 seconds.
Two local-HTTP Git extension fixtures were blocked by the explicit UV_OFFLINE setting;
without that setting both pass. The other extension bootstrap and sandbox Docker
test also pass on focused rerun; no unrelated production behavior was changed.
The final full default target still needs to be rerun after startup/packaging fixes.

Spec and quality reviews approve this scoped fault/runner slice. Required deployment
docs now show --required, required:true, table_prefix:fleet_ and persistent tracking
after admission closes. Actual foundational startup enforcement/installed-artifact
tests and full containerized Compose daemon acceptance remain outstanding follow-ups.
B12 whole-task and all B/C/BC release markers stay unchecked; passing this current
test matrix is not completion of those remaining requirements. No ECS deployment.


## B01/B02 installed Gateway startup follow-up — 2026-10-02

Enabled Fleet is now preflighted before generic optional extension loading: required:true,
table_prefix:fleet_, and NAS root/identity are mandatory for the supported Gateway path.
Closing jobs admission keeps readiness and durable MCP tracking requirements in force.
Disabled Fleet remains inert; standalone operator configuration is unchanged.

Actual ExtensionManager installation runs in an isolated checkout/virtual environment.
Tests inspect installed entrypoint and five migration assets, enter the installed Gateway
lifespan twice, observe ready service and SQL driver binding, persist node state across
restart, and prove required missing-package failure. Host autogenerate uses actual host
metadata with a negative control: Fleet tables are protected by the declared prefix,
while an unrelated probe table remains visible. Each PG fixture owns its schema.

Spec and quality reviewers approved this slice. Root independently ran startup, actual
installation and Fleet tools: 18 passed, zero skipped, 28.03 seconds. Four changed Python
files pass Ruff check/format; OpenSpec strict validates all three changes; diff check
passes. Earlier implementer broader scope: 26 passed, zero skipped. Full containerized
Compose and final full B regression remain pending; this does not complete B/C/BC.


## B12 full local Compose daemon acceptance — 2026-10-02

Two distinct UUID public worker daemons run the committed Compose configuration with
the retained built content ID, private 0600 credentials and independent journals. Actual
CA-verified HTTPS reaches a loopback Gateway on macOS Docker Desktop. PG sessions differ;
Docker inspection proves image identity, read-only root, dropped capabilities and no
published/exposed worker ports. The drained node has zero attempts. Actual completion
starts one job, survives admission closure, publishes one sealed manifest, leaves original
tracking completed and releases capacity; modifying the writable attempt cannot change
the sealed result. Actual SIGTERM stops the still-held job, reports a durable journal,
releases proved-stopped capacity and preserves unknown/input_required rather than success.
Execution waits on an explicit release file; complete releases only after charged-capacity
and disable rejection checks, while TERM never releases. A 60-second deadline bounds it.

Compose acceptance: implementer final 2 passed, zero skipped, 8.85 seconds; root earlier
enhanced file 2 passed, zero skipped, 11.53 seconds. Root explicit matrix including final
release-barrier Compose, installation and gate guards: 237 passed, zero skipped, two known
warnings, 113.27 seconds, with hostile PYTEST_ADDOPTS='-k nonexistent' ignored. Retained
actual report /private/tmp/fleet-b12-compose-root-final-gate.xml. The gate now preflights
frozen base digest, CLI SHA256 and absolute existing build artifacts; guard tests had
genuine 9 RED/11 passed before validation, then 20 passed.

The first matrix exposed a test observation race (236 passed, one failed): HTTP count
creation preceded content flush. The job now writes an acknowledgement after successful
HTTP completion; bounded polling requires its actual contents and both effect counters
before cutting control. All exactly-one assertions remain strict. The final marker-content
refinement was made after the 237-test matrix collected its files; root separately reran
that final partition file successfully (1 passed, zero skipped). These are fixture fixes,
not a new production protocol RED. Spec and quality reviews approve the final four files.

No Linux Compose runtime or production ECS/NAS deployment is claimed. B03 session-admin
node registration/state/credential management remains a real spec gap; trusted CLI is
additional and cannot replace the promised HTTP path/profile restrictions. Final full B
regressions and that follow-up remain required before C. Historical initial RED evidence
for B01/B02 and B03 authentication is not explicitly recorded and will not be invented.

Full default target before this slice: 13176 passed, 172 skipped, one deselected, one
failed, 264.48 seconds. The failure was UV_NO_SYNC=1 inherited by a fixture that must
install a temporary wheel; without it the exact test passes. Final default target must
run without that environment setting. Blocking-I/O after startup follow-up: 75 passed,
zero skipped, two known warnings, 4.05 seconds.


## B03 administrator management completion follow-up — 2026-10-02

Real RED: 14 missing HTTP management behavior tests and three actual profile/migration
tests (two forbidden-profile claims still allocated, one old migration head). Final
management plus B02–B04 scope: 36 passed, zero skipped, 5.76 seconds; installed-artifact
acceptance: two passed, zero skipped, 28.33 seconds. f0006 follows f0005 and preserves
existing jobs, attempts, reservations and credentials while adding persisted node
allowlists and server-derived registered_by. Actual JWT users/PG/PAT repository and
TCP middleware prove admin-session/CSRF restrictions, private one-time credentials,
scoped revocation, safe deletion and drain persistence; no auth principal replacement.

Root final B matrix: 259 passed, zero skipped, two known warnings, 118.03 seconds, with
all final files including full Compose and partition content barriers. Report retained
/private/tmp/fleet-b03-management-root-gate.xml. Root full backend Ruff check/format: 1385
files clean; blocking I/O: 75 passed, zero skipped, two known warnings, 4.16 seconds.
Spec and quality reviewers approve all 12 code/test files. Whole-B read-only audit finds
all 12 SHALL behaviors covered, with no remaining runtime feature blocker. Shared docs
now explicitly map B controlled-tool submission and existing thread MCP task read/cancel
APIs instead of advertising an unimplemented /api/fleet/jobs facade. C attempt submission
remains pending. Final full default regression and acceptance checkbox recording pending.

Prior committed Compose full default target (f2cd4cef): 13186 passed, 174 optional skips,
one deselected, 19 known warnings, 265.04 seconds, without UV_NO_SYNC/UV_OFFLINE. Current
management code has a fresh full default target running; its result supersedes this
checkpoint when recorded. No production ECS/NAS or Linux Compose runtime claim.


## B final default regression — 2026-10-02

Final management tree: make test 13187 passed, 195 optional skipped, one deselected,
19 known warnings, 264.59 seconds. Explicit gate 259/0; blocking I/O75/0; Ruff1385clean.
Final whole-B/spec/security/quality reviews approve. Tasks record actual implementation
and historical RED limitations; C and BC remain active goal obligations. No production deployment.

## C01 started after B acceptance — 2026-10-02

B final implementation/acceptance commit: 518a59cf. C01 now builds immutable versioned
launch descriptions, private durable placement/agent-task identity and actual host
dependency guards. C/BC remain unaccepted; no runnable remote Agent is advertised.
Next private revision is f0007_agents after f0006_nodes. B admission/image wire
compatibility remains a required adjacent regression.


## C01 foundation locally verified — 2026-10-02

Spec and quality/security reviewers independently approve the frozen thirteen backend
files. Genuine RED5 is recorded in the implementation thread (no separate retained
log); additional valid-model RED2 is /private/tmp/c01-model-red.log. Fixture failures
are excluded. Agent GREEN23/0, installed7/f7 plus B neighbors97/0. Root independently
ran C01 23/0 (1.82s), retained-image B gate259/0 (123.34s), default make test13208 pass
197 optional skip/1 deselected/19 knownwarnings (271.45s), blocking-I/O75/0 (5.68s),
guidance/thread92/0 (1.82s), full backend Ruff1390 clean. Reports/logs use
/private/tmp/fleet-c01-root*, including b-gate.xml, full-test.log and blocking-io.log.

f0007 preserves f0006 B data; canonical launch records prohibit SQL update/delete;
caller rollback leaves no task/spec/placement rows. Host validates actual unified
checkpoint/application identity and accepts legacy checkpointer=None. Gateway still
rejects agents_enabled until actual runner/fences exist. B old strict profile grants
exclude runtime_digest and passed the retained unchanged image. No remote execution,
C03 claim, public Agent HTTP summaries or production deployment is claimed.
C02 and all remaining C/BC tasks remain active goal work.

C01 implementation and local verification commit: d0ebd0f8.

## C02 started — 2026-10-02

Proceeding to core Local/Fleet backend registration and same-session atomic admission.
C02 is not complete; no runnable remote Agent or claims are advertised. The worker
image remains retained for B regressions.


## C02 atomic admission locally verified — 2026-10-02

Frozen nine backend files passed independent spec and quality/security review.
Local baseline211/0; C02 eighteen scenarios + SQL/ownership/C01/B neighbors438/0.
Initial RED1 is missing trusted production entry; independent-transaction negative
control detects retained run count1 and correct SQL is restored before GREEN.
Logs /private/tmp/c02-entry-red.log, c02-atomic-negative-control.log and
c02-neighbor-green.log. Fixture failures are excluded from RED claims.

Root actual PG C01+C02 41/0 (4.97s), report/log /private/tmp/fleet-c02-root.xml/.log;
old-image B259/0 (122.79s), fleet-c02-root-b-gate.xml/.log; default backend13211pass,
212 optionalskip/1deselected/19knownwarnings (270.34s), fleet-c02-root-full-test.log;
blocking-I/O75/0 (4.39s), guidance92/0 (1.94s), full backendRuff1395clean.
OpenSpec strict3/3 and diffcheck clean. No source changes occurred during final gates.

Core run and Fleet goal/spec/placement use one caller session/transaction and
rollback together on fault or cancellation. Same manager/restart/three independent
processes reuse one immutable admission; changed inputs/identity conflict. Trusted
remote pending rows have no Gateway ownership/lease or local task. Local defaults
and memory/store interfaces retain compatibility. Versioned input preserves actual
normalized messages and Command semantics. No migration was added in C02.
Gateway remote activation remains closed; C03 ownership/recovery/claims, C04runner
and remaining C/BC tasks are outstanding goal work.

C02 implementation and verification commit: 9a60c310.

## C03 started — 2026-10-02

Proceeding to shared run/attempt claim ownership and lease renewal, including real
Gateway recovery/hydration boundaries. C03 is not complete; Gateway remote execution
activation remains closed. No full runner or production deployment is claimed.


## C03 ownership foundation locally verified — 2026-10-02

Spec and quality/security review approved thirteen frozen source/test files.
Four unique genuine RED behaviors (overlapping logs are not additive): queued remote
run wrongly terminalized by Gateway recovery (c03-recovery-red.log); actual admin
Agent capacity registration returned422 (c03-red.log); raw github_token accepted
(c03-secret-red.log); cross-node Agent start leaked kind through503 rather than403
(c03-node-scope-red.log). All logs reside in /private/tmp. Fixture/configuration
failures are excluded. Focused GREEN33/0 (4.33s), c03-final-green.log; neighboring
GREEN486/0 (23.19s), c03-neighbor-green.log (before the final cross-node test).

Root C01-C03 PostgreSQL74/0 (7.65s), fleet-c03-root.xml/.log; unchanged retained-image
B gate259/0 (121.67s), fleet-c03-root-b-gate.xml/.log. Default backend13217 passed,
239 optional skipped,1 deselected,19 known warnings (270.58s), fleet-c03-root-full-test.log.
Required PostgreSQL/container tests were independently executed without skips.
Blocking-I/O75/0 (4.75s), fleet-c03-root-blocking-io.log; guidance/boundary tests26/0
(1.37s), fleet-c03-root-guidance.log; thread route contracts61/0 (1.70s),
fleet-c03-root-thread-contract.log. Full backend Ruff1397 clean; strict OpenSpec3/3.
Actual guidance checker has0errors/4soft chain-size warnings; the same four paths
warn at parent HEAD, with no new warning category/path. Strict-warnings therefore
is not reported as passing. Diff check clean.

Claim/renew atomically bind core run and attempt to identical leases and shared
capacity. Local SQL recovery/ownership mutations exclude remote placements and
server-owned nonlocal backend labels; hydration and scheduler recovery preserve them.
Node bearer kind dispatch verifies scope, explicit Agent profiles require positive
capacity, legacy defaults remain jobs, and accepted renewal survives closed flags.
No migration change. Raw runtime github_token now requires out-of-band references.
Gateway activation stays closed. C04 runner, complete cancellation/physical stop,
Agent read/reconcile and all remaining C/BC work are outstanding.


C03 implementation and verification commit: 65600e04.

## C04 preparation — 2026-10-02

C03 is committed. Preparing actual harness run_agent reuse with trusted attachment
to the already admitted SQL run, complete RunContext bootstrap and an independent
worker process. C04 is not implemented or checked off. Tool execution must not see
control database/node/Redis credentials through context, environment, files or process
inspection. The existing host LocalSandbox is not claimed as a filesystem boundary.
Linux non-dumpable hardening is under evaluation alongside clean bootstrap/FD and
configuration isolation; actual container probes are required before claiming it.
Relevant primary documentation:
[Linux Yama](https://www.kernel.org/doc/html/latest/admin-guide/LSM/Yama.html),
[PR_SET_DUMPABLE](https://www.man7.org/linux/man-pages/man2/PR_SET_DUMPABLE.2const.html),
[proc environ](https://www.man7.org/linux/man-pages/man5/proc_pid_environ.5.html).
Gateway activation remains closed through this preparation.


C04 preparation probe (not runner acceptance): /private/tmp/fleet-c04-linux-isolation-probe.py
and .log ran in the unchanged retained B image with --user65534:65534,
--cap-dropALL, --security-optno-new-privileges, --networknone and read-only root.
A same-UID child could open runner /proc environ/fd/mem in the negative control;
after PR_SET_DUMPABLE=0 and RLIMIT_CORE=(0,0), all three reads returned PermissionError.
The test used a fake memfd control value, no real credentials; --rm removed its
container. This establishes only the proposed Linux process barrier. Actual clean
bootstrap, configuration/file/environment boundaries, trusted attachment and full
lead graph/tool execution still require C04 tests.


## C04 started — 2026-10-02

Locked the actual runner/bootstrap/attachment/start-stop interfaces in
openspec/ecs-fleet-contracts.md. Proceeding with TDD using the real harness execution
path and Linux container credential probes. The C04 checkbox remains unchecked;
the earlier probe proves only process protection. Complete C05/C06 durable write
fences, C09 operations and later acceptance still precede Gateway activation.


### C04 real runner integration — in progress

Actual NodeDaemon TCP claim/start reached a separately installed Linux runner and
reported container PID1 readiness. The first adapter called real run_agent with the
wrong manager keyword; corrected to run_manager. Its retained adapter-contract
failure is /private/tmp/c04-runner-adapter-red.log, not full graph acceptance.
Subsequent real graph execution reached artifact-delivery enforcement: a scripted
final response omitted the produced artifact links. The fixture is being corrected
against that existing contract; the delivery policy is not bypassed.

/private/tmp/c04-basic-e2e.log and c04-runner-last-diagnostic.log are mutable live
diagnostics, not final evidence. SCRAM PostgreSQL integration, Local/Fleet parity,
subagent/resource/credential probes, full runtime resources, actual bundle snapshots
and review/regression gates remain outstanding. C04 checkboxes remain unchecked.


C04 basic chain subsequently passed: /private/tmp/c04-basic-e2e-green.log,
1 passed / 0 skipped / 2 dependency warnings (12.47s). Observed actual NodeDaemon
TCP claim/start, independent Linux container readiness, real lead graph and task
subagent parent/child artifact bytes, successful SQL run, persisted events and
checkpoints, stopped container and released reservation. This single basic scenario
is not complete C04 acceptance: Local parity, resource/credential negative probes,
full bootstrap resources, snapshot derivation and reviews/regressions remain pending.


C04 enhanced resource integration exposed sync definition stores querying the wrong
PostgreSQL schema. Root preserved /private/tmp/fleet-c04-schema-red.log and private
fleet-c04-schema-diagnostic.log from the completed failing run (8.07s). These are
resource diagnostics, not complete requirements RED/GREEN. Actual optional trusted
session_factory constructors and a host-owned schema-aware sync engine are present;
container re-verification remains pending. Operator file/db selection must remain
honest. The active implementation also bridges scoped skill state/projection and
actual MCP toolset discovery instead of depending on a raw configuration mount.


C04 actual MCP cleanup revealed a lifecycle failure: Core success was already
persisted, but active-only renewal rejected the same still-cleaning physical owner.
Retained SQL behavior RED: /private/tmp/c04-terminal-cleanup-red.log,
1 failed (1.75s). After adding the narrowly bounded terminal-cleanup renewal,
the same test passed /private/tmp/c04-terminal-cleanup-green.log,
1 passed / 0 skipped (1.61s). Terminal Core status is preserved; original identity,
start authorization, generation and identical live leases remain required.
Physical stop still precedes reservation release. C05/C06 durable write fencing
and complete C09 recovery are not claimed by this renewal.

The implementer reported a subsequent enhanced real-container resource pass
(14.47s), but /private/tmp/c04-resources-e2e.log is a mutable diagnostic reused
during Local parity work; it is not final retained acceptance evidence. Local parity
must avoid backend/tests/conftest.py's deliberate mock executor, and independently
execute the real graph/subagents. C04 remains unchecked pending source freeze,
full retained behavioral evidence, two-stage review and independent regression gates.


C04 enhanced resources/Local parity diagnostic subsequently passed:
1 passed / 0 skipped / 2 dependency warnings (23.57s). Root retained a private
non-overwriting log copy at
/private/tmp/fleet-c04-parity-resources-observed-green-2357.log.
The acceptance scenario now includes a fresh Native subprocess running the real
lead/subagent stack, actual message/tool/usage comparison and PostgreSQL checkpoint
state/references plus artifact bytes. Container probes check numeric nonroot UID,
PID namespace, cgroup, denied process inspection and specific SCRAM authentication
failure; background-child output stops and a repeated launch keeps StartedAt.
These assertions still require final frozen-source attribution and independent
re-execution. Plugin lifecycle, failed-bootstrap/one-shot matrix, formal two-stage
review and regression gates are outstanding; C04 remains unchecked.

Models/config/MCP/skills module guides now describe trusted private execution
scopes and their Local fallback behavior. They explicitly distinguish C04 plumbing
from the later durable mutation fences and retain closed remote admission.


C04 plugin/failed-bootstrap scenarios passed before final input binding:
2 passed / 0 skipped / 2 known dependency warnings (29.27s); root retained
/private/tmp/fleet-c04-plugin-once-observed-green-2927.log. The source is still
mutable and complete acceptance/reviews/gates remain pending.

An implementation audit identified an unimplemented initial workspace input:
prepare_workspace created an isolated directory but did not resolve the immutable
workspace_manifest_ref. This is a C04 full-runtime input requirement, not completed
by creating an empty directory. A trusted approved Agent snapshot resolver and real
workspace/uploads consumption tests are now required by the shared contract.
C08 still owns checkpoint/workspace joint recovery and publication; it is not
implicitly completed by this initial input bridge. No C04 completion checkbox changed.


C04 source-freeze checkpoint (not acceptance): 49 implementation/fixture files
matched /private/tmp/c04-source-freeze.json during root inspection. Child retained
full matrix64 passed/0 skipped/2 warnings (51.82s),
/private/tmp/c04-final-full-matrix.log, and neighbors781 passed/0 skipped/4 warnings
(23.74s), /private/tmp/c04-final-neighbor-regression.log. Image:
sha256:f77a6e7c3d2be847ad7ae10dd8a24b21fbb3f8b7a16aef45d44cda919a7cf1b6.
These are implementer observations, not root independent acceptance.

Formal read-only spec review is in progress and identified a concrete preflight
issue: AgentContainers.compatibility directly imports app.fleet.runner_context
instead of honoring the installed operator-selected environment provider. Frozen
source is awaiting the reviewer's full findings before repairs. Quality review,
root independent gates and slice commit remain outstanding; C04 tasks stay unchecked.


Formal C04 spec review returned one P2 blocker and no approval: compatibility
preflight hardcodes the Gateway host import. Repair is in progress using the same
selected installed factory's worker_compatibility() interface, without changing its
async bootstrap call shape. An installed non-Gateway provider/image without app
must execute this query; missing/ambiguous/unfit metadata must reject. Source freeze
will be renewed after behavior tests, image rebuild and full matrix. Quality review
has not started, root independent acceptance is outstanding, and C04 remains unchecked.


C04 provider repair source freeze: root verified all52 source hashes against
/private/tmp/c04-provider-fix-source-freeze.json; original49file evidence was retained.
Actual installed non-Gateway provider preflight RED1failed/5passed/64deselected
(7.45s), /private/tmp/c04-provider-behavior-red.log. The valid alternate wheel in
an actual image without app/harness was rejected by the old hardcoded host import.
The repair queries the selected installed factory's worker_compatibility().
Missing/ambiguous/unfit/invalid/noncallable providers reject; private bootstrap is
not read by the hardened compatibility-only entry.

Child full revised matrix70 passed/0 skipped/2 warnings (75.45s),
/private/tmp/c04-provider-fix-full-matrix.log; related C01-C03/boundary75 passed/
0 skipped/2 warnings (18.86s), /private/tmp/c04-provider-fix-neighbor.log. The previous
781-neighbor run belongs to the original freeze, not this changed provider source.
Runner image sha256:2bd67fcdb64314160ac272085e2614de2596a140cedf2d813e9ab3f25699123d;
standalone provider image sha256:12599383dcb5516edb51fb0c13f5cd69a0c0c020e9c7a139d730beba67954431.
Docker window released. Formal spec re-review is running; quality review and root
independent gates have not run. C04 remains unchecked and uncommitted.

Root preparatory documentation checks: strict OpenSpec3/3 and changed-guide checker
24 AGENTS.md/0 errors/0 warnings. These do not constitute runtime acceptance.


C04 provider spec re-review approved the renewed52file freeze. Quality/security
review then returned NOT APPROVED with two P1 defects: owned-start/attachment uses
transaction-start current_timestamp(), permitting a lock wait to outlive the lease;
private MCP credential exclusion misses URL-decoded and query/legacy connection
passwords. Repairs require actual PostgreSQL lock-wait RED/GREEN for both entry
points and encoded/query/control-source credential regressions. No C04 checkbox
or completion/commit was made.

Independent root observations on the provider-fix52file source: actual runner70
passed/0 skipped/2 warnings (62.02s), /private/tmp/fleet-c04-root-runner.log/.xml;
C01-C03 PostgreSQL74 passed/0 skipped/2 warnings (11.43s),
/private/tmp/fleet-c04-root-c01-c03.log/.xml. These do not prove the two missing
regressions. Further root acceptance was stopped after quality findings. B gate
was deliberately SIGINT-interrupted at20passed (139.20s),
/private/tmp/fleet-c04-root-b-gate.log; this is NOT B acceptance or a regression
failure. Orchestrator exited and Docker window released. Full backend/lint/root
remaining gates did not run. Repairs/re-freeze/reviews/independent acceptance remain
necessary; source and dual-image attribution will use fresh evidence paths.


## C04 independent local acceptance — 2026-10-02

Formal spec and quality/security re-review approved the final52file freeze.
The two P1 repairs have actual PG unchanged-row lock-wait and decoded/query/legacy
credential RED/GREEN evidence. Original genuine REDs and the intermediate optional
Redis loading regression remain separate, retained logs; no fixture failure or
API absence was counted as a behavioral RED. Redis parsing stays lazy, with no
new dependency version. Gateway activation is still closed; C05-C12/BC are pending.

Child final92/0/2warnings76.10s and related313/0/3warnings17.14s:
/private/tmp/c04-clock-secret-fix-final-full-matrix.log and final-neighbor.log.
Root final runner92/0 (61.67s), C01-C03
74/0 (11.30s), unchanged retained-image B gate
259/0 (120.37s), with actual required PostgreSQL and
container scenarios independently executed without skips. All root logs/XML use
/private/tmp/fleet-c04-root-final-*. Full backend: 13287 passed, 261 skipped, 1 deselected, 19 warnings in 274.70s (0:04:34)
Blocking-I/O75/0 (4.01s), boundary/thread
74/0 (2.41s). Full backend Ruff clean,
final guidance0errors/4 existing soft chain-size warnings, strict OpenSpec3/3 and
diffcheck clean. The same four warning paths/codes exist in parent HEAD; no new
warning category/path. Strict-warnings is not reported as passing. Final make
format left all1414 Python files unchanged.
Optional default-suite skips do not replace the independently run required cases.

Immutable runner sha256:d553a22390438717b7c19a8c417eeb53f393ddc981693cb454afeaba3c792d00; alternate provider sha256:c95135de1a1383c9239c859026fea0d64b2ad37e0e8b1b30d472b58752c77ed8.
Exact source/image binding: /private/tmp/c04-clock-secret-fix-source-freeze.json.
Original B image sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8
was not rebuilt. This is local Docker/PostgreSQL acceptance, not production ECS
or user-facing activation. The slice contains this evidence; exact commit is
recorded after committing. The full C change is not IMPLEMENTED or archived.

C04 verified implementation commit: `991a97fd0f3c5b3a6216124c5327d88b5ff48f95`. Post-document boundary/thread checks
74 passed/0 skipped (2.66s); OpenSpec3/3 and diffcheck clean. All52 frozen source
hashes match the independently verified snapshot. C05-C12 and BC remain pending.


### C05 in progress: locked protocol and first actual behavior RED (2026-10-02)

Starting from clean HEAD 1fd2d544, audited installed
langgraph-checkpoint-postgres 3.1.1 rather than assuming upstream APIs. The
C05 plan and shared OpenSpec contracts now lock the actual three async mutation
families and their sync aliases, same psycopg connection/explicit transaction
ownership guard, original token stamp binding, inner-before-cache injection,
and full/delta CheckpointStateAccessor preservation. Unsupported CRUD methods
remain unsupported and the separate synchronous PostgresSaver remains Local.

Stock setup contains CREATE INDEX CONCURRENTLY; trusted Gateway/Local
initialization retains stock setup while remote startup will verify existing
schema/migrations read-only. Actual Gateway initializes before extension
services. The sole direct late checkpoint writer found is interrupted-title
fallback; its legitimate write will move before durable terminal persistence,
retaining the existing ownership/replay/prior-finalizing/later-run guards.
Other finalizers and durable resources remain C06 scope.

First actual PG behavior RED: **3 failed / 0 skipped, 2.07s**, retained at
/private/tmp/c05-checkpoint-actual-red.log. Replacing the original attempt token
still allowed stock aput, aput_writes and adelete_thread; each failed because the
expected rejection did not occur. Earlier fixture setup errors in separate
c05-checkpoint-red/behavior-red logs are not behavior RED or acceptance evidence.
Implementation and the full race/rollback/identity/runner matrix are still pending;
no C05 checklist is marked complete and remote activation remains closed.
Strict OpenSpec validation after the planning update passed 3/3; this validates
spec structure only, not runtime implementation.


C05 first source GREEN: original-token replacement now rejects all three actual
async mutation families and leaves checkpoint table snapshots unchanged,
**3 passed / 0 skipped, 2.06s** at /private/tmp/c05-checkpoint-first-green.log.
This is the first focused implementation check, not full C05 acceptance.
The full identity/sync/race/rollback/readiness/materialization/runner matrix,
formal two-stage review and independent regression gates remain outstanding.

Root additionally inspected the actual built wheels: runtime/AGENTS.md is packaged
and included in installed compatibility hashing. Its C05 write-boundary guidance
was therefore synchronized before the final runner build/freeze; later acceptance
records stay in external docs. Current planning validation remains strict 3/3;
worktree guidance check reports 24 AGENTS, 0 errors/0 warnings. The existing
isolated PG16 service, installed saver 3.1.1/pool3.3.0 and original immutable B
image c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8
were independently rechecked available. These environment/planning checks are not
runtime acceptance. No C05 task is marked complete and no slice commit is made.


### C05 final independent local acceptance (2026-10-02)

Formal specification and quality/security reviews both Approved the frozen eight
source files; no unresolved must-fix. Real writer-cursor transactions cover all
three actual AsyncPostgresSaver 3.1.1 mutations and their external-thread sync
aliases. The host binds the original token stamp and immutable execution, locks
the joined ownership rows and checks database wall clock after locking. Real
stock SQL shares the guard's physical PID/transaction; takeover blocks until
commit/rollback. Independent-guard negative control demonstrates race sensitivity.
Expired/replaced identities leave all three checkpoint tables unchanged; first-SQL
Exception/CancelledError rolls back every change and releases locks. Full/delta
with cache retains actual CheckpointStateAccessor materialization. Special pending
UPSERT, remote read-only schema/migration readiness, actual PG interrupted title,
late-cancel title ordering and the complete Linux lead/subagent/tool graph pass.

Effective RED remains 3 actual rejected-operation expectation failures at
/private/tmp/c05-checkpoint-actual-red.log (2.07s); fixture setup failures are
retained separately and excluded. Child final C05-only130/0 (18.40s), final
combined222/0/2warnings (79.78s =130C05+92C04), neighboring317/0/3warnings (26.59s),
default-no-PG-extra82pass/1expected optional skip (5.97s). Logs respectively:
c05-final-checkpoint-matrix-130.log, c05-final-complete-matrix-222.log,
c05-neighbors-corrected.log and c05-local-default-regression.log under /private/tmp.
Earlier 78/82/83/221 results remain phase evidence, not the final source acceptance.

Root independently verified the same eight-file freeze with no source drift:
checkpoint130/0 (18.57s), runner92/0/2warnings (62.08s), C01-C03 admission74/0/
2warnings (7.87s), original unchanged-image B gate259/0/2warnings (125.87s).
Every required PG/container case ran, with nonempty JUnit reports and zero skips.
Root logs/XML: /private/tmp/fleet-c05-root-final-*. Full backend make test passed:
13288 passed,390 optional skips,1 deselected,19 known warnings in270.85s;
blocking-I/O75/0/2warnings (3.85s), boundary/thread74/0/1warning (2.46s).
Ruff check/format-check1417 files clean, strict OpenSpec3/3, diffcheck clean.
Optional default-suite skips do not replace the explicitly executed required cases.

Final document guidance has0errors/4existing AG002 chain-size warnings. The same
four warning paths/codes exist in parent HEAD (gateway, memory, middlewares,
sandbox). The temporary new backend length warning was removed by condensing its
new C05 paragraph; no new category/path remains. Strict-warnings is not claimed
passing. Package runtime/AGENTS.md was finalized before the actual image build;
root checked its actual installed bytes from the immutable image against the frozen source hash. External
README/backend guide/plan/task records were finalized after the acceptance gates.

Immutable runner sha256:9951f1974ef5a9c81cb48bb5bd53c82813c1a562e1a39bc8b105ec69e4f6b1d0;
provider sha256:b19d18d2a8199ac1ec88baa666e8cd45b17695e6c67b0528cea230d5d291f625.
Exact source/image binding: /private/tmp/c05-source-freeze.json and
/private/tmp/c05-source-files.txt. B image remains
sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8.
This is local PG/Linux-container acceptance, not production ECS deployment or
user-facing activation. C06-C12 and BC remain pending, Gateway agents_enabled
stays closed, and the full C change is neither IMPLEMENTED nor archived.
Verified source commit is recorded after committing this slice.


C05 verified implementation commit: `cf7b107969ec511423774a572e17feb7704c8997`.
All eight frozen source hashes match the actual committed blobs. Final make format
left1417files unchanged. Post-document boundary/thread checks74passed/0skip
(2.81s), strict OpenSpec3/3 and diffcheck clean; guidance warning paths/codes equal
parent HEAD. C06-C12 and BC remain pending and remote activation remains closed.


## C06 planning checkpoint — 2026-10-02

Resumed from clean `cd251cbf`; C05 source `cf7b1079` remains verified. Actual
write-surface audit refined C06 into sequential C06a (execution capability and
Run/ThreadMeta/events), C06b (Store including TTL and sync definitions), C06c
(configured adapted memory, extensions, scheduler and runner MCP tracking).
The [detailed plan](2026-10-02-ecs-fleet-c06-durable-mutations.md) replaces the
phantom fleet_probe/raw-token sketch. Read-only plan review found no blocker to
C06a and its four coverage clarifications were incorporated. A single implementer
was dispatched for C06a; no C06 behavior RED/GREEN or completion is claimed here.
Strict OpenSpec validation passed all three changes; git diff --check passed
before the implementation dispatch. Main C06 and OpenSpec 6.1–6.4 remain unchecked;
C07–C12/BC and remote activation remain pending.
