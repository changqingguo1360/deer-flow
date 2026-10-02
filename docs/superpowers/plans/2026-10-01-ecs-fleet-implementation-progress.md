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


C06a genuine behavior RED: 36 failed in6.96s, retained at
/private/tmp/c06a-primary-behavior-red.log. Root inspected the failure summary:
all36 failures are expected rejection DID NOT RAISE, covering token/owner/expiry
with actual Run completion/status/progress/model, ThreadMeta display/status/metadata,
and event put/batch/singleton/delete_run/delete_thread. Fixture/import/connection
failures were not counted. C06a implementation is in progress; no GREEN, final
review, full C06 acceptance or remote activation is claimed. Planning source commit:
`3195f375` (four planning documents only).


Additional C06b installed-code finding: inspected langgraph/store/postgres/aio.py
and installed langgraph/store/base/batch.py. Public async conveniences enqueue
(Future,Op) to a constructor-created worker without per-call mutation context.
The detailed plan now requires capture at that boundary or direct BaseStore async
conveniences preserving caller context. Stock GET with refresh_ttl=False still
contains an UPDATE CTE; a truly read-only terminal GET needs pure SELECT.
This is verified source inspection, not a C06b implementation/test pass. C06a
implementer remains active; no parallel source edits or Docker tests were started.


C06a focused first GREEN (implementer run, root inspected retained output):
166passed/0skipped in24.02s, /private/tmp/c06a-primary-first-green.log, comprising
36 primary stale-write cases plus130 C05 checkpoint cases. This proves the
focused RED cases now reject; it is not full C06a/C06 acceptance. Actual target
owner/stamping, positive terminal, unsupported operation, lock/race/rollback and
manager rejection propagation tests are still being added before source freeze,
independent reviews and root gates. Original B image remains unchanged.


C06a expanded intermediate GREEN:89passed/0skip in14.23s at
/private/tmp/c06a-primary-matrix-green.log (root inspected). Prior intermediate
logs preserve four fixture/snapshot failures and then87pass/2fail (one explicit
batch-user behavior regression, one admission TypeError fixture); the regression
was repaired before this GREEN. Later expanded96pass/2NameError fixture failures
are not counted as RED or final evidence. C06a now also includes runtime/journal.py:
actual background flush rebuffering and worker delivery-receipt generic retries
must distinguish nonretryable OwnershipRejected. This is necessary original
C06 ownership propagation scope, not a replacement runtime. Matrix/full runner,
source freeze, both reviews and independent acceptance remain pending.


C06a additional retry-path genuine RED2 at
/private/tmp/c06a-journal-retry-behavior-red.log (2fail/98deselected/2.68s):
actual stale PG receipt rejection was swallowed/retried and journal rejection
rebuffered; fixture errors excluded. Focused worker/journal GREEN5/96deselected
2.54s and expanded PG GREEN231/0 (101C06a+130C05,38.63s); Local neighbors
481/0/1warning15.47s, retained distinct logs. Root inspected these outputs.

First rebuilt Linux matrix395pass/2actual C04 failures90.70s at
/private/tmp/c06a-linux-complete-matrix.log: parity/interrupt fail because direct
remote store_only admission skips Local metadata initialization and has no
ThreadsMeta row. Earlier silent no-op hid this gap. No full runner acceptance
is claimed. Root approved actual trusted runner/repository original-owner
initialization (atomic, existing-row preserving, never adopt another user),
with genuine positive/stale/wrong-owner/concurrent tests, source rebuild and
full matrix rerun. This preserves ownership rules and Local parity. Initial
images are retained as failed evidence, not final images: runner e9579ee533822a2c86d5a9d77516c1f466076dc2bd22dcc624919c104e447efe;
provider261bcbfcdcb5f2882328e7377f7c7a16eb5780c7fe595d6fcc290bf512aa858c.
Packaged runtime guide was updated before rebuild; C06a/C06 review and root
acceptance remain pending and remote activation remains closed.


C06a atomic-initialization frozen candidate: implementer complete matrix404passed/0skip
107.05s at /private/tmp/c06a-atomic-final-complete-matrix.log; Local481passed
18.05s at /private/tmp/c06a-atomic-final-local-neighbors.log (root inspected).
Candidate runner16021c02fb37f8306ef8245b47179eb6f707c69e99e10e0d6ed1d070c3d82a12
and provider6fe7809b9035159e3a70dc5618085ded9bc8e39bc5f11cde6cf304a84ba785a1.
These are candidate evidence, not independent root acceptance. Formal SPEC review
confirmed all13 hashes at review start/end but returned must-fix at RunManager.try_start:
start_owned_run OwnershipRejected is wrapped in RunStartupError without immediately
marking local ownership_lost. Candidate is not approved. Single implementer now
adds genuine manager/worker startup RED/GREEN and typed propagation fix; images and
freeze must be rebuilt before review and independent gates. C06b/c, whole C06 and
C07-C12/BC remain pending; OpenSpec6.1-6.4 remain unchecked, remote activation closed.


C06a startup review fix: actual strongRED6 (manager/worker x token/owner/expiry)
2.85s at /private/tmp/c06a-start-rejection-strong-behavior-red.log, GREEN6
2.41s. New frozen candidate13files at /private/tmp/c06a-start-fix-source-freeze.json:
runner65c9d3a3c9b8716a71bdb9bbf8ae592a2f1df0e225aceb744c03c940cf28459b,
provider3a891c13c679508e9c17c76df3f51e09e9a3a6bc218e1400d991b57d3b72c446.
Implementer matrix410/0skip117.26s; Local481/0skip16.18s. Formal SPEC re-review
Approved; quality review then found one must-fix: actual worker
_SubagentEventBuffer.flush swallows OwnershipRejected and re-buffers it;
final pending flush must also mark loss and allow cleanup/end/finalizing release.
Single implementer repairs this path with actual PG streaming/final-buffer RED/GREEN.

Root independent gates on that candidate completed before the attempted stop,
exit0 session81152, prefix /private/tmp/fleet-c06a-root-reviewed-:
mutation114/0skip26.73s, checkpoint130/0skip22.03s, realrunner92/0skip71.15s,
C01-C0374/0skip8.54s, Local481/0skip15.81s; fresh required XML verified nonempty
and zero skipped/failed/error. Blocking75/4.36s, boundaries74/2.79s, Ruffcheck
plus1420formatted, guidance24/0errors/0warnings, strictOpenSpec3/3, diffclean.
These are prior-candidate evidence only, not approval despite passing tests.
Installedbytes proof at /private/tmp/fleet-c06a-root-reviewed-installed-bytes.log
matched runner12productionfiles includingguide, provider2Fleetfiles and absent
app/harness. Initial generic provider proof wrongly expected host files; corrected
per actual Dockerfile.provider (fixture-only image), no source/image modification.
No commit/wholeC06 checkmarks or remote activation; final rebuilt candidate and
repeat reviews/affected independent gates required before C06a acceptance.


C06a accepted partial substep after both formal SPEC and QUALITY/SECURITY reviews
Approved against all13 start/end hashes. The sole quality subagent-buffer defect
has genuine strongRED6/7.09s at /private/tmp/c06a-subagent-rejection-strong-behavior-red.log
and GREEN6/6.63s. Final implementer installedmatrix416/0skip279.32s andLocal492
29.81s retained under /private/tmp/c06a-subagent-fix-*; candidate freeze
/private/tmp/c06a-subagent-fix-source-freeze.json.
runner34d1eadb03d23aa2f8a837df134b1fea23d1d175b973b15179e4ff9ed927759a,
provider544eb103492ed1f84b5a639bff9e68a4e8ed4e63304c98d627f635bfb8b798d3.

Root independent final gates exit0 session66766, prefix
/private/tmp/fleet-c06a-root-subagent-final-: mutation120/0skip61.90s,
checkpoint130/0skip22.12s, realrunner92/0skip70.74s, C01-C0374/0skip8.33s,
Local492/0skip17.09s; every required fresh XML verified nonempty/no
skipped/failure/error. Blocking75/4.32s, boundaries74/2.65s, Ruffcheck and
1420formatted, guidance24/0errors/0warnings, strictOpenSpec3/3, diffclean.
Installedbytes log matched all12runner productionfiles includingguide and
2providerFleetfiles, noapp/harness in the independent fixture-provider image.
No fullB/fullbackend gate is claimed for this partial substep; those remain
mandatory at wholeC06 acceptance after b/c. OriginalB image remains unchanged.

C06a detailed substep checkboxes only now checked. MainC06 andOpenSpec6.1-6.4
remain unchecked; C06b realStore/synchronousdefinitions is next, C06c memory/
extensions/scheduler/MCP andC07-C12/BC still pending. Remoteactivation closed.
Source commit is a partial capability/primary-repository increment, not fullC06.


C06a verified source commit:48a2b501bc0a9c933c5584bf1c1c1a2124c02669
(feat(fleet): fence primary remote Agent mutations), explicit15files; clean
worktree immediately after commit. Single implementer dispatched C06b with actual
Store batching-context/pureSELECT TTL-read findings and synchronousdefinition
guards; no parallel implementation. C06a remains accepted; wholeC06, C06b/c and
C07-C12/BC still pending. No remote activation/deployment.


C06b current-state audit (no implementation acceptance): installedAsyncStore
3.1.1 _execute_batch uses _cursor(pipeline=True), whose supported pipeline path
lacks an explicit transaction; remote adapter must retain an explicit writerTX.
ActualSqlAgentStore.update retry after IntegrityError rollback needs a fresh
guard in the second TX. delete commits SQL then removes file-backed memory;
remote cannot claim that unfenced rmtree is PostgreSQL-atomic. Actualsetup_agent
default branch directly writes globalSOUL.md evenwithdbdefinitions; remote must
reject before filesystem effects. setup/update tools' broad error handling may
swallow typedOwnershipRejected, so actualtool propagation belongs inC06b tests.
Detailed plan records these discovered reachable boundaries; managedsubagent
global-name schema stays unchanged. Single implementer preparing actualstaleRED.

C06b actualoutertool audit: ToolErrorHandlingMiddleware sync/async wrappers
catch ordinary Exception and convert to recoverable tool output; both need
typedOwnershipRejected propagation. InstalledToolNode default onlyhandles
ToolInvocationError and rethrows otherexceptions; actualLangChainfactory leaves
that default unchanged. Recorded inplan and singleimplementer brief; no
upstream dependency changes or speculative alternative runner are needed.


C06b first genuine actualStore/definitions RED:14failed/120deselected4.39s at
/private/tmp/c06b-secondary-first-behavior-red.log (root inspected actualtest
source and output). Stockmake_store initializes real migrations; futureoptional
cap kwargs passed onlywhen signatures support them, preserving original writes.
Originaltoken invalidated after binding; actualaput/adelete/TTLGet/Search/abatch,
externalthreadput/delete/batch, Agent/Managedcreate/update/delete all returned
instead of rejecting (DIDNOTRAISE), nofixture/import/connectionerrors.
This is initialbehavior RED, not wholeC06b acceptance. Singleimplementer now
adds actualsamecursorTXStoreadapter and sameSessiondefinitionguards; positives,
all aliases/vector/TTL, race/rollback/context, tool propagation, profile/setup
and actualinstalledrunner/reviews/rootacceptance remain pending.


C06b first focused GREEN:14passed/120deselected4.31s at
/private/tmp/c06b-secondary-first-green.log (root inspected actualoutput and
productiondiff). Covers the initial realStore/definition token-revocation RED,
not complete C06b. NewStore adapter uses caller-contextdirectasyncconveniences,
explicitwriterTX, pureSELECTreadonlyGet and prepares externalembeddings before
executionlocks; SQLentry revalidates originalauthority freshly. SyncAgent
upsert retry revalidates afterrollback, remoteAgentdelete neverrmtree.
Native isolatedPG availablevector extension0.8.6 confirmed via readonly catalog
query; no newglobalinstall needed for actualvector tests. Full readiness types/
PK/index/vectorconditions, positive/race/rollback/allaliases/context/actualtools,
hostwiring/installedrunner/reviews/rootgates andC06c remain outstanding.


Read-only next-C06c audit while singleimplementer completesC06b: actual
MemoryManager factory returns globalcachedmanager before readingconfig and
constructs via from_config(**host_hooks); remote must bind privateconfigured
resources/preflight before unsafeconstructor orwarm effects. Memory CRUDtools
catchordinaryexceptions into errorstrings; actualemergencysummarizationflush
routes memory_flush_hook→add_nowait through SummarizationMiddleware._fire_hooks,
which also swallows ordinaryexceptions. Typedownership rejection must survive
these actualcallers. CurrentDeerMem queue coalesces by thread/user/agent and
bypassflag, notattempt, rawTimer/shutdownThread do notcopycontext. Unsafefile
profiles remainunsupported; adapted configuredrealPGbackend must demonstrate
originalcapture, coalescing separation and shutdown/background stale-write fence.
Added actualcaller map toC06c plan; noC06c implementation started or completion
claimed, andno parallel sourceimplementation.


C06b expanded actual-nativePG evidence (not acceptance):27passed/120deselected
7.62s at /private/tmp/c06b-secondary-expanded2.log; legal sync/async writes,
terminal TTL-disabled reads, missing/wrong resource scopes, readonly readiness
and actual tool middleware rejection propagation. Earlier fixture/type failures
were corrected and retained separately, never counted as behavior RED.
Real vector/identity matrix24passed/147deselected6.69s at
/private/tmp/c06b-vector-identity-matrix.log: native pgvector Store/vector rows,
legal upsert/search/delete, late embedding revocation before SQL, and21 actual
Store/Agent/Managed identity-change cases. Embedding fixture supplies vectors
and a revocation callback only; persistence uses actual PostgreSQL tables.
Store transaction matrix4passed/171deselected2.60s at
/private/tmp/c06b-store-transaction-matrix.log: post-lock-wait expiry, actual
stock SQL and guard share PID/TXID and block takeover until commit, and
Exception/CancelledError after first real SQL roll back Store/vector rows.
Root independently read test source and these logs. Remaining definition retry,
file cleanup, actual ToolNode paths, readiness drift, Local regressions, frozen
installed images, formal reviews and independent gates are still required.
C06b/wholeC06/OpenSpec6.* unchecked; no remote activation or deployment.


Read-only C06c extension caller audit: actual runner binds private_scope around
plugin load/start/stop, but start_services passes the unrestricted SQLAlchemy
session_factory through ExtensionRuntimeDeps. _notify_each and awaited
_notify_each_on_extension_loop both contain ordinary exceptions; detached
dispatch futures are removed by discard-only done callbacks. The C06c tests
must exercise these real callers, preserve original scope/capability across
cross-loop dispatch and cleanup, and surface/retain typed ownership rejection.
Local contributor fail-open behavior remains separate. Added to the detailed
plan; no C06c implementation or acceptance, C06b remains the sole source task.


C06b complete native C01-C03/C05/C06a+b matrix400passed/0skips72.62s,
/private/tmp/c06b-native-complete.log (root independently read output). Actual
Agent loop4passed/189deselected2.83s at c06b-agent-loop-matrix.log: installed
create_agent/model→ToolNode→ToolErrorHandlingMiddleware→real definition SQL;
stale rejection stops before a second model call, legal calls complete both.
Independent-connection guard negative control2passed/187deselected7.22s at
c06b-store-negative-control.log; the same PID/TXID/takeover-blocking criterion
fails for separately committed validation.

Initial20-fileLocal aggregation:736passed/2skipped/2failed19.52s. One actual
Local call compatibility regression introduced lock=False into the existing
AgentStore._row patch; corrected by passing the lock keyword only for remote.
The other middleware fixture aliased5 roles to one FakeMiddleware type and
therefore violated real ordering. The unchanged oldHEAD reproduces the failure
(c06b-existing-middleware-head-baseline.log:1failed1.37s); production source was
restored byte-for-byte. Fixture now uses distinct subclasses per role, preserving
production ordering/isinstance assertions. Two skips were live schema opt-in;
Local rerun supplies DEERFLOW_TEST_POSTGRES_URL explicitly. Final Local, actual
rebuilt installed images, formal reviews and root independent gates still pending.
No C06b or wholeC06 acceptance/activation is claimed.


Read-only C06c scheduler/MCP audit: handle_run_completion derives two IDs from
RunRecord metadata and writes occurrence status and parent task in separate
repository transactions. Each must validate actual original-run/owner/parent
association, not trust metadata; revocation between calls belongs in the matrix.
McpTaskRepository.create/create_idempotent already persist user/thread/run;
runner tracking uses these original targets while host poll/cancel/notification
leases remain separate. Added actual entrypoints to C06c plan; no source change
or C06c implementation while the sole C06b implementer completes verification.


C06b source-only review checkpoint: formal SPEC then QUALITY/SECURITY Approved,
no must-fix, both independently inspected actual source/installed stock Store
and start/end19source hashes. v3 source equals reviewedv2 byte-for-byte.
Freeze /private/tmp/c06b-source-freeze-v3.json SHA256
154178fa71c31582dc5ebe38c3e1da9248a38b6bc68eca8a197b1c8dc805155b.
runner11c02fcf2ab8de66fb0effbc548a9375ca0e0708eace8b916fbb6bc723cb0590,
providera7da031ac7932f0be37ffa7b626063e59003bbf87d76b5e3afa0786e3d639c75.
Root independently verified all19 current hashes and reviewed installed-byte
log:runner16production files including3packaged guides; provider2Fleet files
and absent app/harness (/private/tmp/c06b-installed-bytes-proof.log).

Final20-fileLocal740passed/0skips18.40s, independently read byroot at
/private/tmp/c06b-local-neighbors-green.log. Initial build did not capture a
verifiable prebuild hash file and is not claimed to have done so. Subsequent
actual freeze→cache build checked all19hashes before/after; BuildKit attestation
changed manifest-list identities despite cached layers. Consequentlyv3 binds
new images and requires fresh matrix, not old image evidence.

First installed matrix493pass/2fail136.21s remains failure evidence. Two C04
checkpoint-negative fixtures defaulted to unsupported file definitions, so
were minimally set to database definitions to isolate their original boundary.
No production checks changed; v2/v3 include that exact test correction.
Freshv3 installed matrix is pending (/private/tmp/c06b-installed-final-matrix
.log/.xml); it showed an E during execution, final trace not yet available.
Source approval is not runtime acceptance: any necessary source/test fix
requires new freeze/delta reviews and affected gates. Root independent gates
still await Docker release. C06b/mainC06/OpenSpec6.* remain unchecked and
C06c/C07-C12/BC/remoteactivation/deployment remain pending.


C06b v3→v5 review checkpoint: finalv3 matrix495passed but1teardownERROR
154.05s remains failed evidence. Actual [end] test DROP SCHEMA deadlocked
with this run's still-finishing journal transaction; root read the PG trace and
existing sleep(0), which did not drain owned callbacks. The sole test delta
captures its actualRunJournal and bounded-waits existing flush/progress tasks
under asyncio.timeout(5), including replacement progress tasks. It never calls
flush/retry, touches global tasks or retriesDDL; original before/row/fencing/
finalization/end/cleanup assertions remain. Focused3passed/0skip2.54s at
/private/tmp/c06b-owned-callback-cleanup.log.

V5 /private/tmp/c06b-source-freeze-v5.json SHA256
5a5ad256c4d219857d335fa0baede168721b887892aa052010be9e23e1c34546.
Root verified19current hashes; only test_c06_remote_agent_runtime.py differs
fromv3. Both formalSPEC thenQUALITY/SECURITY delta reviewsApproved with
19source/JSON start/end hashes matching. The16installed production/guide files
andbothv3images remain identical; fresh actualbytes matched again at
/private/tmp/c06b-installed-bytes-v5.log. No acceptance of the failed matrix.
Fresh complete495/XML is running session82499 at
/private/tmp/c06b-installed-final-v5.log and.xml; root independent gates wait
for its terminal result/Docker release. C06b/wholeC06 remain unchecked;
C06c andlaterC/BC/activation/deployment remain pending.


C06b accepted partial substep after formalSPEC thenQUALITY/SECURITY Approved
(including v3→v5 actual-journal-drain delta), all19 start/end hashes matching.
Final implementer matrix495passed/0skip/failure/error143.18s at
/private/tmp/c06b-installed-final-v5.log/.xml, verified independently byroot.
Local740/0skip18.40s retained at c06b-local-neighbors-green.log.
Freeze /private/tmp/c06b-source-freeze-v5.json SHA256
5a5ad256c4d219857d335fa0baede168721b887892aa052010be9e23e1c34546.
runner11c02fcf2ab8de66fb0effbc548a9375ca0e0708eace8b916fbb6bc723cb0590,
providera7da031ac7932f0be37ffa7b626063e59003bbf87d76b5e3afa0786e3d639c75.

Root independent final driver exit0 session74637, freshprefix
/private/tmp/fleet-c06b-root-final-: mutation199/0skip49.01s,
checkpoint130/0skip20.75s, actualLinuxrunner92/0skip70.26s, C01-C0374/0skip
8.10s; Local753/0skip17.93s plus realLocalPGschema2/0skip1.27s. Every
required XML verified nonempty/no skipped/failure/error, all19 hashes verified
before/after each gate. Blocking75/4.26s; boundaries74/2.53s; Ruffcheck and
1421formatted; guidance24/0errors/1existingmiddleware softwarning96536bytes
under98304hard; strictOpenSpec3/3 anddiffclean. Root fresh installedbyte proof
matched16runner production/guide files and2providerFleetfiles, noapp/harness
in independent fixture-provider image (fleet-c06b-root-final-installed-bytes.log).
OriginalBimage untouched; fullB/fullbackend reserved for wholeC06 acceptance
afterC06c, not claimed for this partial substep.

Detailed C06b5checkboxes only now checked; mainC06/OpenSpec6.1-6.4 remain
unchecked. C06c actualMemoryManager/extensions/scheduler/MCP is next; C07-C12
andBC remain pending. No CIMPLEMENTED/archive/remoteactivation/deployment.
Source commit records verified frozen Store/tool increment, not fullC06.


C06b verified source commit:f159703d126aa5ed71e063653b7ba88f1be86ac9
(feat(fleet): fence remote Store and Agent definitions), explicit20files;
worktree clean immediately after commit and all19 frozen hashes still matched
that exact source commit. V5 image/installed-byte proofs are anchored to that
verified commit. This receipt updates only the packaged guidance's verification
status caption from pendingC06b to pendingC06; no technical contract/production
code changes or image revalidation claim. Next C06c build will freeze its updated
guidance normally. C06a/b partial substeps accepted; full C06/OpenSpec6.* stay
unchecked, C06c is next, C07-C12/BC andactivation/deployment still pending.


## C06c first actual configured-memory and extension RED — 2026-10-02

Retained /private/tmp/c06c-first-red.log and .xml: 4 failed, 0 error, 0 skipped, 2.35s. Root independently inspected the actual log and all four XML failure cases. This is behavior RED against unchanged production source; only the configured fixture backend and test file were added. No Docker build or final acceptance has occurred.

The existing dotted-class MemoryManager/from_config factory instantiated a configured PostgreSQL fixture backend. Its actual background extraction entered a barrier; replacing the original attempt token, replacing the core-run owner, or expiring the run lease before releasing extraction each persisted a late durable fact in the real c06_memory table. The test asserts the actual table must remain empty. The actual start_services path also started an undeclared unsafe contributor and supplied the unrestricted session_factory instead of rejecting before start. Fixture/import/API absence is not counted as RED.

C06c implementation now proceeds with private manager/capability binding, guarded real write transactions and all-contributor preflight/typed failure propagation. Root prepared /private/tmp/fleet-c06-root-verify.py for eventual frozen-source independent whole-C06 gates, including unchanged B image and full backend; preparing/syntax-checking the driver is not running or passing those gates. Detailed C06c and whole C06/OpenSpec6.* remain unchecked; C07-C12/BC and activation/deployment remain pending.


C06c initial focused GREEN4: /private/tmp/c06c-initial-green.log and .xml record 4 passed/0 skipped/error in 2.27s; root independently checked all four XML cases. Original token/owner/lease revocation now leaves the real configured PG memory table empty and shutdown_flush propagates OwnershipRejected. Actual extension startup rejects unsupported contributors before start. Production private-manager scope and active-only memory/extension transaction adapter are in progress. This focused pass is not full C06c acceptance: positive CRUD/extraction, singleton isolation, actual tools/hooks, thread/cross-loop/lifecycle/terminal cases, runner wiring, scheduler/MCP, immutable images and formal/root whole-C06 gates remain outstanding. No checklist item or activation is changed.


C06c expanded configured-profile GREEN58: /private/tmp/c06c-expanded.log and .xml contain 58 passed/0 skipped/error in 10.52s, independently inspected by root. This unfrozen matrix covers actual PG memory CRUD/extraction, original-context rejection, Local singleton isolation, unsafe backend pre-constructor rejection, actual memory tool/emergency hook propagation, actual SQL extension start/stop/task callbacks and cross-loop delivery under legal/stale/missing/wrong/terminal identity. Whole C06c remains pending; no immutable-image/formal/whole-root acceptance is claimed.

Intermediate expanded execution reported 57 pass/1 fixture failure: emergency SummarizationEvent had thread_id=None because the fixture put it only on runtime.config, whereas the actual resolver reads runtime.context or LangGraph ambient config. The fixture now supplies the real runtime.context thread_id, without modifying the production early-return rule. That intermediate expanded log was overwritten by the implementer; its output is retained only in root tool history, not claimed as a retained failure file or genuine behavior RED. Future evidence runs require fresh log/XML names. The separate callers-red file is retained, but its source rollback/restore provenance is still being confirmed before classifying its four failures.


C06c actual tracking RED3 and caller sensitivity negative control — 2026-10-02

/private/tmp/c06c-tracking-red.log/.xml record genuine initial scheduler/MCP behavior RED3: 3 failed/0 error/skip, 2.26s. Root independently inspected unchanged-row assertion failures and the three XML cases. After original attempt token revocation, the actual ScheduledTaskService completion changed scheduler parent/occurrence rows, McpTaskRepository.create inserted a task, and request_cancel changed the tracked task. Real PostgreSQL table snapshots prove the failures; no constructor/import absence is counted. Same-transaction associations and guards are now being implemented.

/private/tmp/c06c-callers-red.log/.xml contain 4 failed/0 error/skip, 54 deselected, 3.29s. This is a caller sensitivity negative control, not another original stock-memory RED: the private manager scope was already installed, and the implementer temporarily removed OwnershipRejected rethrows from memory tools and summarization middleware, then restored them in finally. Actual tools returned ordinary errors and the hook logged/swallowed the typed failure. Expanded GREEN58 afterwards demonstrates restored propagation; independent root verified both XML sets.

Root updated packaged memory/extensions/runtime guides before final image freeze. Guidance checker: 24 guides/0 errors/1 visible changed-chain warning. Independent all-path comparison against actual HEAD finds the same four AG002 warning paths/categories in HEAD and worktree; memory effective chain grew from87463 to89121bytes, still below98304hard. No strict-warnings pass is claimed. Complete terminal APIs, runner wiring, scheduler/MCP positives/races, images/reviews and whole-C06 gates remain outstanding; all C06c/mainC06/OpenSpec6.* checkboxes stay unchecked.


C06c profiles/tracking provisional GREEN85: /private/tmp/c06c-profiles-tracking-v2.log/.xml contain85passed/0skip/error in14.18s; root independently checked all85XMLcases. This overlaps earlierGREEN58 and is not an additive total. Actual scheduler completion succeeds across both guarded transactions; revocation after occurrence commit rejects the parent transaction while leaving the parent running. Persisted target/terminal mismatch cases preserve actual rows. Actual MCP tracking/cancel legal and identity-mismatch scenarios also pass. Runner private factory and bound scheduler/MCP wiring are implemented but not yet proven by a rebuilt installed runner.

Root source audit identified a new review target before freezing: remote MCP create_idempotent conflict lookup must retain persisted user ownership, not only equal thread/run/server/driver/handle fields. Missing/wrong factory context must reject before backend construction. Detached callback failures must remain associated with their bound resource attempt rather than a lost/wrong ambient context. These cases, narrow terminal extension APIs, full installed/Local/race matrices and formal/root gates remain outstanding. WholeC06c/OpenSpec6.* remains unchecked.


C06c actual durable MCP submit factory audit: root traced `mcp/tools.py::_make_background_submit_tool` to `get_mcp_task_submitter` in `mcp/tasks/runtime.py`. That bridge uses a process-global submitter and does not use the bound `RunExecutionContext.mcp_task_repo`. Current runner wiring alone therefore does not prove durable tool tracking is fenced. The implementation task now explicitly includes a private actual submission service/configuration scope, Local/Gateway singleton isolation, and actual tool->service->PG positive/stale tests; no Gateway poll/notification workers or second Agent loop in the runner. This is a source-audit finding, not an executed behavior RED or completed requirement. C06c and wholeC06 remain pending.


C06c memory finalization timing audit: root traced private close_memory to the runner exit stack after run_agent completion. Normal queued middleware extraction drained only there would occur after core-run terminal status, whereas memory.write intentionally remains active-only. The supported configured profile must prove actual queued extraction is committed before terminal finalization, with fresh original-ownership validation; postterminal shutdown only closes resources. The full installed lead/subagent middleware path must demonstrate that positive case and the late-revocation unchanged-row case. This source finding is added to the implementation plan; no preterminal-drain implementation or behavior test is claimed yet. Local asynchronous policy remains compatible. C06c/wholeC06 and all laterC/BC are pending.


C06c provisional profiles/tracking GREEN104: /private/tmp/c06c-profiles-tracking-v3.log/.xml contain104passed/0skip/error in16.85s, root independently checked allXMLcases. This overlaps prior85/58 rather than adding to them. Private actual MCP service/scope and remote preterminal memory drain are now wired in production, but expanded tests/real middleware timing and immutable installed-runner verification remain underway. Packaged MCP/memory/extensions/runtime guides were synchronized before final freeze; C06c and mainC06/OpenSpec6.* remain pending. Local213pass/19.28s and a missing-test-path MCP command are observed interim outputs, not whole independent acceptance; exact Local command provenance and later final matrices are pending.


C06c native runtime-neighbors provisional461: /private/tmp/c06c-runtime-neighbors-v2.log/.xml contain461passed/0skip/error in69.01s. Root independently parsed allXMLcases and module counts: C06 existing runtime199, new profiles77/tracking33, affectedLocal152. The110newC06c cases overlap earlier104/85/58, not additive. This run does not include C05/C04/C01-C03 or installed container proof. Actual worker now has remote-only before_terminal_mutations and the fake model's manual queue/flush shortcut is removed; normal MemoryMiddleware installed mode still awaits build/verification. Queue/TX race/whole matrices/reviews remain pending. Root prepared an additional Local memory_middleware case in the eventual whole-C06 verifier (now20 affectedLocalfiles); no root acceptance gate has run against this unfrozen slice. C06c/mainC06/OpenSpec6.* remain unchecked.


C06c transaction sensitivity proof and draft build receipt — 2026-10-02

Profiles/tracking v4 log/XML contain122passed/0skip/error in21.27s, root independently parsed cases. Focused actual memory/extension writer TX proof v3 contains8passed/0skip/error,77deselected,2.96s: same guard/writer PID+TXID, execution locks retained until commit, first actual SQL completed then RuntimeError/CancelledError leaves durable rows unchanged. These subsets overlap the122. Initial TX proof v1/v2 had two memory ready-observation fixture failures because extension_profile overwrote the shared item's capability; v3 observes the actual manager-held capability. They are retained but not genuine behavior RED.

A separate same-positive-invariant sensitivity run is retained at c06c-independent-guard-red-v1.log/.xml:2failures+2passing unrelated lockwait cases,83deselected,12.58s. Only the guard connection was deliberately made independent; the original takeover-blocking expectation stayed unchanged and failed. Original guard restored byte-for-byte: c06c-independent-guard-restoration-v1.json records equal before/restored SHA256 3ecdf862313b942494d27bfa04e4d9b9286570a315fdc776ba43d6a713526755 and negative_exit1. Subsequent original-guard GREEN6/0skip/error,83deselected,3.64s is retained. Root checked all corresponding XML sets. Restoration hashes prove that stage, not the later formatted/further-developed test file.

First draft six-wheel image build is live in child session43503; root independently observed Docker build PID35256 after read-only sandbox process listing required escalation. No root Docker operation was started. Draft /private/tmp/c06c-source-draft-v1.json has50source/18buildinputs. Root currently observes49/50source matching: only host test_c06c_remote_profiles.py changed during build to add real create_agent MemoryMiddleware -> run_agent queue/drain barrier coverage. All18buildinputs and production/fixture installed inputs still match. This draft is explicitly not final source/byte acceptance; final freeze/build/matrix will be refreshed. Worker-memory-v1 mistakenly supplied a nonexistent run_agent context keyword; that fixture/API error is not behavior RED and v2 uses the actual worker-created runtime context. WholeC06c/mainC06/OpenSpec6.* and laterC/BC remain pending.


C06c real worker/middleware queue and provisional126 receipt — 2026-10-02

Root independently inspected c06c-worker-memory-v2.log/.xml:2passed/0error/skip,89deselected,2.71s. Actual create_agent uses normal MemoryMiddleware, actual run_agent constructs runtime context, and the configured memory extraction blocks on a barrier. Preterminal callback observes the real PG core-run status running and a nonempty pending queue. Legal drain commits the actual durable fact before status success. Token revocation inside the barrier leaves memory empty, marks record.ownership_lost, and preserves core-run running instead of falsely committing terminal success. Model only emits a response; it does not manually enqueue/flush memory. This replaces the earlier fixture-only v1 TypeError; that error is not counted as behavior RED.

Root independently parsed c06c-profiles-tracking-v5.log/.xml:126passed/0error/skip in22.55s, overlapping prior122/461 rather than adding them. Actual raw-thread -> owner-loop detached SQL callbacks retain original failure ownership, and another attempt/Local cannot consume or inherit that failure. Registry's new mutation_context is keyword-only, preserving existing positional constructor compatibility.

Implementation is now converting remote scheduler/MCP writer paths to explicit session.begin, so production draft inputs will change. The running draft six-wheel build is development evidence only and cannot establish final source correspondence; a refreshed final freeze/build/full matrix remains mandatory. Root additionally traced actual detached system-model observers: runner currently drains them at exit-stack stop_plugins after terminal, while active-only extension SQL requires preterminal authority. This is a new source-audit target sent to implementer for a real barrier test and correct preterminal owned-dispatch drain; no behavior RED or completed fix is yet claimed. C06c/mainC06/OpenSpec6.* remain unchecked, with laterC/BC and activation/deployment pending.


C06c additional original-writer transaction matrix: root independently parsed c06c-profiles-tracking-v6.log/.xml,151passed/0error/skip,28.37s, and c06c-tracking-explicit-tx-v1,72passed/0error/skip,9.69s. These overlap prior126 and are not additive. Actual scheduler occurrence/parent and MCP create/cancel guards now use explicit session.begin; actual writer tests verify same PID/TXID and original locks through commit, rollback after first SQL and fresh-clock lock waits. Subsequent detached extension drain/cleanup correction is still underway, so these unfrozen matrices are not final acceptance. Root updated the detailed plan to retain original-attempt failures through inspected cleanup, clear Local lifecycle history, and drain owned nested active SQL observations before terminal (including later-model cancellation branches when applicable). Final source/image correspondence, formal reviews and whole-root gates remain pending.


C06c actual detached lifecycle behavior RED2: c06c-notify-lifecycle-red-v1.log/.xml retain2fail/0error/skip,102deselected,2.25s. Root independently inspected both assertions. A bound real SQL child observer enqueued by its parent remains blocked on a barrier, but the original one-snapshot drain returns before the child completes. The second actual Local callback raises RuntimeError and its recorded failure remains visible after reset_extension_notify_loop. These are genuine behavior failures of the current in-progress C06c notification implementation, not stock original implementation RED and not missing fixture/API errors. Corrections and fresh GREEN are still pending. First draft runner build finished successfully with manifest sha256:389347266043813fbbf5d6ab6473be8220d6e10a13670d9168992f57db4b6455, per retained draft build log. This is only development-image evidence: explicit-TX/notification source updates require a refreshed final source freeze and rebuilt installed image before acceptance. No wholeC06/OpenSpec checklist change.


C06c actual ToolNode/runtime propagation fix receipt — 2026-10-02

Root independently parsed notify-lifecycle-green-v1:8passed/0error/skip,96deselected,3.04s, including the two prior nested-drain/Local-reset behavior failures. Later budget/release and final matrices remain pending. Draft installed-daemon-v1 retains6cases,4passed/2parityfailures/0error/skip,89deselected,54.20s. Actual installed run lost ownership, skipped durable finalization and left core-run running, with placement unknown. It is not setup/import failure or an installed GREEN. Separate parity diagnostic is retained rather than only an overwritten last-case file.

Root actual dependency-source inspection and pure no-DB/Docker probe /private/tmp/c06c-root-tool-runtime-probe.py/.log demonstrate StructuredTool's injected-argument restoration drops optional and postponed Runtime annotations after validating the original explicit MCP args schema; concrete Runtime retains injection. Implementer confirmed the actual ToolNode -> durable MCP wrapper -> bound McpTaskService path receives run_id=None and triggers the correct original-run association guard. Actual c06c-mcp-toolnode-red-v1 retains1failed valid-path case/1passed stale case,0error/skip,49deselected,2.30s. Dynamic persistent-session and durable-submit wrappers now retain concrete Runtime signature metadata, with no model-visible runtime field and no weakened fence. Actual ToolNode restored GREEN2/0error/skip,56deselected,1.98s is independently parsed by root. Final rebuilt installed daemon proof is still pending.

Packaged extensions/runtime/MCP guides now describe owned nested preterminal drain, shared budget, original-scope diagnostic release/Local reset and actual Runtime propagation. An initial docs script updated two guides then hit an old MCP-text assertion; the separate MCP update completed, and fresh full guidance checker reports24guides/0errors/only existing memory-chain softwarning89668bytes. Compression avoids new extension/runtime soft warnings; git diff --check passes. All four affected packaged guides are now stable for final freeze. WholeC06c/OpenSpec6.* remains unchecked; C07-C12/BC and activation/deployment remain pending.


C06c timeout cleanup audit — 2026-10-02

Previous goal turn made concrete progress: root saved independently parsed real ToolNode RED/GREEN and installed draft failure evidence, updated three packaged guides and verified full guidance/diff, rather than only restating status. Current actual implementer remains running; no replacement or concurrent Docker/PG gate was started.

Root reviewed the new bounded drain/release implementation. A pure actual asyncio cancellation probe retained at /private/tmp/c06c-root-dispatch-cancel-probe.py/.log reports concurrentFuture done=True/cancelled=True while the actual writer finally remains blocked and rollback_done=False; real cleanup only completes after its barrier is released. This is dependency lifecycle evidence, not actual project SQL-writer acceptance. Source audit indicates notification ownership currently follows the concurrent Future done callback, which may remove the original owner before SQL rollback/finally completes. Detailed plan now requires actual owner-task completion and real SQL cleanup-barrier RED/GREEN before release/reset/engine teardown. Implementer is adding the actual scenario. C06c/final immutable image/formal reviews and whole-C06 acceptance remain outstanding.


C06c actual dispatch rollback/finally RED/GREEN — 2026-10-02

Root independently parsed c06c-dispatch-rollback-red-v1.log/.xml:1failed/0error/skip,107deselected,2.11s. The actual bound extension SQL writer inserts a real row, then blocks its cancellation-finally inside the live transaction before __aexit rollback. Original notification reset wrongly succeeds while actual rollback is incomplete. This genuine behavior RED confirms the earlier pure asyncio lifecycle audit.

The project now schedules an actual owner-loop asyncio Task and bridges it to a concurrent Future; only actual Task completion after transaction cleanup releases original dispatch ownership. Shielded cleanup markers outlive bridge cancellation. Focused c06c-dispatch-task-green-v1.log/.xml:9passed/0error/skip,99deselected,3.03s, independently inspected by root. It includes the SQL rollback barrier, nested callbacks, raw-thread/foreign-attempt isolation and Local reset; these overlap earlier focused results rather than adding them. Working and cancellation cleanup budgets are separate, and cleanup expiry retains ownership so release/reset rejects. Packaged extension guidance now states actual Task completion/rollback authority. Cross-thread bridge completion/cancel publication, additional release/budget/cancellation-winner/Local schema cases and the final rebuilt images/full gates remain under verification. No C06/OpenSpec completion mark.


C06c frozen native evidence and formal whole-C06 SPEC findings — 2026-10-02

Root independently parsed c06c-frozen-native-v1.xml:767 tests/0 failures/errors/skips, and c06c-frozen-boundaries-v1.xml:74/0/0/0. Native767 includes the existing C06 runtime199, profiles120, tracking51 and affected Local397; these overlap preceding matrices rather than adding to them. The historical v1 source freeze contained52source/18build inputs and matched before/after that native run. It is not final installed or whole-C06 acceptance. Subsequent host tests and the formal-review production fixes require a refreshed final freeze. In-flight v2 six-wheel build is retained to its terminal result and will be superseded; do not mistake it for verified final images.

Actual memory update existing-row wait proof is independently parsed at c06c-domain-target-row-green-v1.xml:5 passes/0 failures/errors/skips. The separate c06c-row-commit-revalidation-red-v1.xml contains1 DIDNOTRAISE failure when the same post-yield validation is deliberately removed. That is a sensitivity negative control of the in-progress fix, not a stock-original behavior RED. Actual production restoration is recorded separately by the implementer.

Fresh whole-C06 SPEC report /private/tmp/c06-full-spec-review-v1.md is NOT APPROVED. It identifies the same target-lock expiry family in Store/definitions/ThreadMeta/events/scheduler/MCP: execution-lock validation before target SQL is insufficient when the later target wait exceeds the actual database-clock lease. Required correction is post-SQL/flush, same-transaction fresh-clock revalidation before commit, preserving original identity and allowed terminal fields. Root independently parsed actual c06c-existing-target-red-v1.xml:5 tests/5 DIDNOTRAISE OwnershipRejected failures/0 errors/skips, for existing Store, Agent, managed subagent, ThreadMeta row waits and event advisory wait. This is a genuine behavior RED on those production paths. A second SPEC finding requires rejecting contradictory already-terminal once-parent scheduled status/error, rather than overwriting it with core-derived completion. Root additionally independently parsed c06c-tracking-target-parent-red-v1.xml:6 failures/0 errors/skips, all DIDNOTRAISE OwnershipRejected. Four actual target-wait cases cover occurrence, parent, MCP create and MCP cancellation; two actual ScheduledTaskService.handle_run_completion cases cover contradictory already-terminal failed/cancelled once parents. The test source uses real target row/table locks, observes the actual PostgreSQL writer blocking and waits for clock_timestamp to exceed equal run/attempt leases. For parent contradiction, actual durable parent/core/occurrence rows are prepared before the service call; this is not a mocked association guard. Corrections and fresh GREEN remain pending. Historical C06a/b substep acceptance does not waive these whole-C06 findings.

C06c/mainC06/OpenSpec6.1–6.4 remain unchecked. Final source/image correspondence, installed lead/subagent parity, SPEC re-review then QUALITY review and independent whole-C06/root gates remain pending. C07-C12/BC and activation/deployment remain pending.


C06 whole-review target-wait fixes, focused GREEN and guide sync — 2026-10-02

The reviewed family is being corrected with precise per-writer changes, after actual RED5+RED6. Neutral validate_mutation_after_sql(_sync) only flushes/revalidates bound resources; unbound Local retains its previous flush/commit behavior. Store revalidates on its actual psycopg cursor after SQL; synchronous definitions do so after actual ORM flush, including rollback/retry; ThreadMeta covers target-row locks and post-SQL/flush, scheduler validates association/once-parent terminal consistency and revalidates each actual completion transaction; MCP tracking revalidates before creation/cancellation commit. Event post-write context runs inside session.begin, flushes then validates on the same session before transaction exit, including early singleton/terminal receipt returns. These production changes are still awaiting full SPEC re-review.

Root independently parsed focused XML (overlapping, not additive): spec-store-green-v1 1/0 failures/errors/skips; spec-agent-green-v1 9/0/0/0; spec-managed-green-v1 51/0/0/0; spec-thread-lock-green-v1 1/0/0/0; spec-thread-commit-green-v1 56/0/0/0; spec-scheduler-lock-green-v1 4/0/0/0; spec-scheduler-commit-green-v1 5/0/0/0; spec-parent-commit-green-v1 4/0/0/0; spec-mcp-commit-green-v1 25/0/0/0; spec-events-commit-green-v2 82/0/0/0. All paths are under /private/tmp/c06c-. The events-commit-green-v1 filename is misleading: actual XML is82 tests/1 failure/0 errors/skips. Its patch had stopped on a unique-context assertion before any event source writes, but the shell continued to test. The same original events DIDNOTRAISE is repeated RED, not accepted GREEN or a new regression. V2 is the corrected fresh result.

One multi-file regex mutation was rejected by automatic approval review for broad core persistence/event/scheduler/MCP data-consistency risk; it performed no file writes. Implementer switched to bounded exact writer edits and actual per-family RED/GREEN. No rejected script was executed indirectly. The user was informed of the action and reason. Final-v2 six-wheel build completed naturally with historical runner sha256:f55f583b74d492c91982723cd1b7904e950c15b204ea53232ca0faf3074ce342; it predates these fixes and is not final installed acceptance.

Root updated packaged runtime/MCP guides with the actual post-SQL/ORM-flush same-transaction fresh-clock requirement. An initial added runtime soft length warning was removed by precise wording compression without weakening the contract. Final guidance check24guides/0errors/only existing memory-chain softwarning89970bytes; git diff --check passes. All four affected root-owned packaged guides are stable for refreshed freeze. The representative actual unique-conflict wait case, final native/frozen image correspondence, installed runner, formal SPEC then QUALITY and independent whole/root gates remain pending. C06c/mainC06/OpenSpec6.* remains unchecked; no remote activation/deployment.


C06 final-v3 freeze, unique-conflict classification and whole native GREEN — 2026-10-02

Root independently parsed full-native-v4.xml:786tests/0 failures/errors/skips, log101.40s; module counts199existingC06,130profiles,60tracking,397affectedLocal. This overlaps preceding focused/native matrices and is not an additive total. Historical full-native-v3 remains786tests/785passes/1 failure/0 errors/skips101.30s: original independent-connection negative control still expected a bad writer to commit events after its takeover, but the new post-flush guard correctly rejected/rolled back. The identical takeover-block criterion was retained; only final writer expectation now requires OwnershipRejected and durable rows unchanged. Fresh v4 is the all-green result, not a relabeling of v3.

Actual combined spec-all-target-green-v1 is16/0/0/0. Parent consistency includes completed status with contradictory last_error as well as failed/cancelled outcomes. Actual uncommitted-peer unique wait typed-classification proof: unique-direct-red-v1 has2cases/1failure/1pass; ordinary MCP create returned IntegrityError for mcp_tasks_pkey instead of OwnershipRejected, whereas the idempotent path already rejected. Post-rollback original-capability fresh validation corrected it; direct-green-v1 has2/0/0/0. Later unique-remotehandle-green-v1 deliberately uses a different contender task ID to exercise the user/server/remotehandle unique constraint, and its matrix is12/0/0/0 including Local neighbors. Agent .create actual unique wait retains agent-unique-red-v1 1failure/0error/skip (AgentExistsError instead of typed loss), then agent-unique-green-v1 3/0/0/0 including Local. Managed create's matching conflict catch preserves the same original-authority classification; no-cap Local retains ordinary Exists behavior. These are precise known-family corrections, not expanded exception authorization or a new product.

Root additionally synchronized runtime/MCP guides: after unique-conflict rollback, recheck original authority before ordinary error handling/compensation. Final guidance24guides/0errors/only existing memory-chain softwarning89970bytes and diff check pass. Packaged guides remain stable.

Final-v3 freeze /private/tmp/c06c-source-final-v3.json pre-image SHA256 e228ab9b1b859a9445f26c295da035ebddece4d42036b3831fce7353d5ded936 contains54source/18buildinputs. Root independently matched every hash and all45 changed/untracked backend files are covered. Images were still{} at this read; JSON will acquire actual immutable image metadata after the complete six-wheel build, changing its JSON hash but not frozen source/input hashes. Final build childsession34873/log c06c-image-build-final-v3.log is underway. Fresh formal whole-C06 SPEC reviewer has been dispatched against this v3 source and real evidence; no final approval or QUALITY acceptance is claimed. Final installed byte/full-runner proof and independent C01-C06/B/fullbackend/static gates remain required. C06/mainOpenSpec6.* stays unchecked; C07-C12/BC and remote activation/deployment remain pending.


Formal v3 SPEC follow-up — acceptance withheld

Read-only reviewer found three remaining source paths in the same approved post-SQL lease-validation requirement. BoundMutationTransactions async/sync validates after callback yield but does not flush ORM writes first; session.add may defer its actual SQL until session.begin exit after that validation. Existing configured fixture uses direct text SQL, so its GREEN does not prove deferred ORM writes. RunRepository primary status/progress/model/completion/finalize writers still lack after-SQL revalidation. SELECT FOR UPDATE already owning the run row does not rule out later table-lock upgrade waits: an external SHARE table lock can permit initial ROW SHARE and block the UPDATE's ROW EXCLUSIVE. ThreadMeta.create also needs original-authority typed classification after a delayed unique-conflict rollback; ensure ON CONFLICT already has post-SQL validation. These are source findings pending actual PG reproductions by the sole implementer, not claimed executed behavior RED.

The precise findings were dispatched for real ORM/table/unique-lock barriers, then flush/fresh-clock corrections preserving terminal allowlists and Local behavior. Source compliance has not been approved; no QUALITY reviewer or C07 work starts. The running v3 complete six-wheel build is retained to natural completion as historical evidence; updated source/tests require a new v4 freeze and exact rebuilt installed proof. The stable packaged guide already specifies post-ORM-flush validation for all writers. Native786 passes remain valid historical coverage of v3, not proof of these newly identified paths or full C06 acceptance. Goal work continues; C06/OpenSpec6.* and laterC/BC stay unchecked.


Reviewer also asked to clarify memory.backend_config privacy. The current tested adapted memory uses host-supplied session factories, so no current fixture DSN disclosure is claimed. The schema nevertheless permits opaque libpq DSN strings that the generic public-config URL/sensitive-key detector does not recognize. Root verified the manager is already constructed from private.memory, and asked the implementer to check consumers and remove backend_config from public execution projection while retaining private constructor config/host hooks, if that projection is unnecessary. This closes the existing private-credential boundary; it introduces no new memory product or credential-binding channel. Exact behavior proof and source correction remain pending.


C06 v4 source fixes, native evidence and actual installed failure — 2026-10-02

Root independently parsed actual v4-mustfix-red-v1 XML:10 failures/0 errors/skips. Nine are behavior REDs: two actual deferred ORM callback writes, five actual primary RunRepository target-table upgrade waits, ThreadMeta.create delayed unique-conflict classification, and opaque private backend configuration retained in the public projection. The start case in that first XML is a fixture timeout from missing launch arguments, not a behavior RED. Separately corrected v4-start-red-v2 is1 actual DIDNOTRAISE behavior failure. Focused GREEN XML (overlapping, not additive): bound2, primary6, thread1, private-config6, all zero failures/errors/skips. Bound transactions now flush mapped ORM additions before their final original-authority check; all six primary run writers check after target SQL; ThreadMeta.create rechecks original authority after unique rollback. Public memory backend_config projects only the recognized failure_policy.read enum, preserving the actual lead prompt's fail_closed behavior; manager construction retains full private configuration.

Root independently parsed full-native-v6:799 tests/0 failures/errors/skips (199 existing C06,143 profiles,60 tracking,397 affected Local). Historical full-native-v5 is799 tests/1 failure: an observer fixture published its PID Future twice after the new second guard. The fixture now publishes once while retaining its original task-before-run lock criterion. V6 is fresh GREEN, not a relabeling of v5. The four packaged guides describe actual ORM flush/post-SQL validation and the public policy projection. Guidance24/0 errors/only existing memory-chain softwarning90144bytes; git diff --check passes.

Final-v4 freeze contains55source/18build inputs and covers all46 changed/untracked backend files; root matched every hash before the build. Pre-image JSON SHA2561daa534747fc277f06317f13b5ecd0434dcddadf89df3bfe4abee987caaa7097. Complete six-wheel build15811 naturally exited0 with unchanged pre/post source/input hashes. Actual immutable runner sha256:8ad12e8c3da4e1c739bf0d6b2b369657893388ffa5805cc5022c2e079be5a4a5 and provider sha256:36e33a9a9448e72be95f7597e02d76461ce81370de191ff1e57decb9fb67e5a3 are recorded in the freeze; resulting JSON SHA256013d424f17de267df5c2350415d98b46697f301b93bf8c1b52e07d6835d1ce07. Actual installed-byte verifier exited0, runner44/provider5 matches and provider excludes app/harness. This byte evidence does not prove runtime acceptance.

Actual installed-daemon-final-v4 naturally exited1:6 tests/4passes/2 failures/0 errors/skips. Root independently parsed the XML. Both normal tool/middleware parity cases returned unknown, skipped durable finalization after ownership rejection and left the core run running. The actual cause is not yet identified; original guards must not be weakened to make the cases pass. Sole implementer owns PG/Docker and is collecting a minimal fixture-only exception traceback before choosing a correction. A diagnostic build is distinct from final-v4 and requires a new final freeze/proof after any fix.

Formal report /private/tmp/c06-full-spec-review-v4.md closes the known v1/v3 source findings and private-config projection, with no additional supported-writer omission found, but whole-C06 SPEC remains NOT APPROVED because of the real installed failure. QUALITY has not started. C06c/mainC06/OpenSpec6.1–6.4 stay unchecked; required whole/root gates, C07-C12/BC and activation/deployment remain pending.


C06 installed first-rejection diagnosis — 2026-10-02

Diagnostic-v5 single normal parity case fails and its actual cap wrapper sees only extension.task_stop rejection after the core run was left running. Root read the container traceback and confirmed this is secondary, not evidence of the first cause. No guard was relaxed or target identity fabricated. Diagnostic-v6 separately wraps only trusted fixture observation of the actual RunManager rejection and private MCP submitter, emitting traceback and matching booleans without credential/config/identity values.

Root independently read /private/tmp/c06c-installed-diagnostic-v6-container.log: original ambient mutation context matches=True, request user/thread/run matches=(True,True,False). The first actual stack is worker._stream_once -> real LangGraph/create_agent tool middleware/ToolNode -> durable MCP wrapper -> McpTaskService.submit, whose original run association check rejects. This is not actual lease expiry, extension terminal receipt failure, or the earlier optional-Runtime signature filtering layer. The current concrete Runtime injection alone does not prove full-graph context retention. Root read both actual worker astream branches: they manually inject __pregel_runtime into config but do not pass official context=. Sole implementer is reproducing this with the real compiled Agent graph and original bound service before a minimal worker fix. Root does not claim the proposed fix has run or passed. Diagnostic-v6 build session12646 and single-case run61031 reached natural terminal states; historical final-v4 remains6cases/4pass2fail and unaccepted.

The detailed acceptance plan now explicitly requires real whole-graph runtime identity propagation across both astream branches; isolated ToolNode context injection is insufficient. Final source/guides must be frozen and rebuilt again after correction, then real installed lead/subagent/skills/MCP/hooks proof, formal SPEC, QUALITY and independent whole/root gates. C06/OpenSpec6.* remains unchecked; C07-C12/BC and activation/deployment remain pending.


C06 full-graph diagnosis qualification — stock component GREEN

Root independently parsed c06c-worker-runtime-red-v1 XML:2 failures/0 errors/skips. Both actual stacks fail in the test Model._generate positional signature before the durable tool path; these are fixture errors, not behavior REDs despite the filename. After correcting only the model fixture, c06c-worker-runtime-red-v2 XML is2passes/0 errors/skips,2.43s, with the actual unmodified worker and real plain create_agent plus bound PostgreSQL MCP service. Single and multiple stream branches both retain original runtime identity in that simpler graph. Native/image dependency versions match (langgraph1.2.9/langchain1.3.14/core1.4.9), according to the implementer's installed comparison; root has not independently run a version probe.

The real final-v4 installed failures and diagnostic-v6 first run-identity mismatch remain valid. The plain-graph GREEN narrows the missing reproduction to the actual complete lead assembly/middleware/runtime path; it does not justify weakening original-run validation or pretending a proposed official-context correction is proved. Sole implementer has therefore withheld that production edit and is running a single-variable fixture-only full installed observation: where official context is absent, forward the same existing parent runtime context to CompiledStateGraph.astream. Only the actual result can establish whether this changes the failing complete graph. No acceptance mark, diagnostic image promoted to final image, or QUALITY approval.


C06 loaded MCP synchronous bridge metadata diagnosis

Actual diagnostic-v7 single installed normal tool case still fails (unknown,1failure/13.74s in the current log) despite forwarding the same existing parent runtime context as official graph context in the fixture-only experiment. This does not support promoting the proposed worker change to production. Root has not yet parsed a final v7 XML/container observation; retain the natural-terminal evidence separately.

Root source/dependency inspection found an omitted real pipeline step: get_mcp_tools assigns tool.func=make_sync_tool_wrapper(tool.coroutine,tool.name), but the resulting sync function has no runtime annotation. Installed ToolNode._get_all_injected_args prefers tool.func over tool.coroutine, so the prior concrete coroutine Runtime annotation is hidden in the fully loaded tool. Both existing direct ToolNode and plain create_agent component fixtures omit that actual sync-bridge assignment and therefore do not reproduce the full installed tool.

Root executed the actual dependency no-PG/no-Docker probe /private/tmp/c06c-root-sync-metadata-probe.py with the real make_sync_tool_wrapper and ToolNode classifier. Exit0 outputs: coroutine_only_runtime_injection=runtime; after_real_sync_bridge_runtime_injection=None; coroutine_has_runtime_annotation=True; sync_func_has_runtime_annotation=False. This is real dependency metadata evidence, not a substitute for actual guarded SQL/installed acceptance. Sole implementer has been asked to include the real bridge in the bound-service ToolNode/full-worker reproduction and then preserve the required metadata with existing config injection and Local behavior. Do not weaken identity guards or add a speculative worker context fix. New source/final frozen image/installed proof and formal approvals are still required; C06/OpenSpec6.* remains open.


C06 actual loaded sync-bridge RED/GREEN and guide boundary

Root independently parsed diagnostic-v7 XML:1failure/0errors/skips and its container observation: original ambient context=True; user/thread/run matches=(True,True,False); run missing=True. The official-context-only fixture experiment did not correct the real installed path, so no production worker context change was made.

Root independently parsed real-sync-bridge-red-v3 XML:7cases/6failures/1pass/0errors/skips, log3.21s. Adding the actual get_mcp_tools final sync bridge reproduces3 genuine PostgreSQL guarded paths (normal ToolNode and real worker single/multiple stream), one Local pooled tool Runtime=None failure and two known Runtime/config injection metadata failures. The stale case still rejects. Historical red-v2 includes a Local fixture missing import, and red-v1 component Model._generate errors were setup errors; v3 is the setup-free genuine bridge behavior proof.

The precise production fix in tools/sync.py adds two static concrete ToolRuntime branches, with/without the existing RunnableConfig forwarding, while retaining ordinary kwargs/config-only behavior and contextvars/thread bridging. No arbitrary user annotations or dynamic signature are copied; worker runtime identity/fences are unchanged. Root independently parsed real-sync-bridge-green-v1 XML:148passes/0failures/errors/skips,19.74s. This includes real guarded PG ToolNode/full worker plus Local session-pool/sync-wrapper/OAuth neighbors. These are overlapping focused results, not additive native/installed totals.

All fixture-only diagnostic wrappers have been removed. Root compared the plugin bytes with the v4 freeze: restored SHA2562eaa18ec579cf49689ecd009d2004acf6172e858476fc103e20484314bbff0dc. Root updated MCP and tools guides with the final func-before-coroutine injection boundary and explicit Runtime/config compatibility. Guidance24/0errors/only preexisting memory-chain90144byte softwarning; diffcheck passed. Five root-owned packaged guides are stable for the new freeze. Independent root verifier now includes test_mcp_sync_wrapper.py and test_mcp_oauth.py with existing session-pool checks; this prepared gate has not been executed.

A new complete native run, source/input freeze, six-wheel immutable build and actual installed proof are underway or pending. Historical final-v4 remains4passes/2failures; focused148GREEN does not substitute for real final installed parity. Formal whole-C06 SPEC remains withheld until fresh proof, followed by QUALITY and independent whole/root gates. C06/OpenSpec6.* stays unchecked/uncommitted; later C/BC and activation remain pending.


C06 final-v5 full native, source freeze and source SPEC approval

Root independently parsed full-native-v7 XML:825 tests/0 failures/errors/skips; log119.04s. Partitions199 existing C06,143 profiles,62 tracking,421 Local across24 affected modules. These overlap prior native/focused suites rather than adding to them. Source57/build-input18 freeze /private/tmp/c06c-source-final-v5.json pre-image JSON SHA256e8291dcdb09c76924f0ae861a5005be4abc3b6a200937c5cb715e52406f35d14. Root independently matched every hash, zero mismatches, and all49 changed/untracked backend files are covered. At this check images={} and the six-wheel final-v5 build83221 remained owned by the implementer. Exported-image log output alone is not a claimed natural-terminal or installed pass.

Formal read-only /private/tmp/c06-full-spec-review-v5.md independently closes all whole-C06 source requirements including the final loaded sync bridge: SOURCE SPEC PASSES. Previous v4 guard/flush/unique/privacy/lifecycle code is byte-identical; no production worker-context workaround was made. It independently parses native825 and actual bridge RED/GREEN, with correct fixture-error/insufficient-pipeline provenance. Final runtime SPEC decision remains pending actual immutable new-image bytes and installed normal tool/middleware success. Source approval is not completed C06 acceptance or permission to check/commit/activate. The sequence remains actual installed compliance -> final SPEC decision -> QUALITY -> independent whole/root gates; overall delivery is withheld until every gate succeeds.

Implementer identified two actual source files copied directly into site-packages/fleet by the runner Dockerfile: c04_worker_fixture.py and c04_mcp_fixture.py. Root independently inspected the builder and prepared verifier, then corrected the temporary raw-byte verifier's test-source filtering to compare both copied installed files against their frozen hashes on runner only. Provider selection still excludes fleet/app/harness; source+build inputs are now hash-checked before/after each byte-probe container, rejecting conflicting duplicate inputs. This prepared correction requires no product/guidance/image change and has not yet been run against final images. Original verifier's44/5 v4 match cannot prove these omitted copies or final-v5 runtime. Final complete byte/runtime proof and independent full required gates remain pending, with C06/OpenSpec6.* still unchecked and later C/BC pending.


C06 final-v5 tracking protocol failure and final-v6 fixture proof

Complete final-v5 six-wheel build83221 naturally exited0 with unchanged pre/post57source/18inputs; recorded immutable runner84a4e854c8ae948b8c9993f2c3d040a34fa1978a4c12a1795668096eef5b7d3c/provider8dc065da4e27465e1a3df6aa20102522aa8819f4ce9059610ff1d4753b08dc26. The implementer ran the corrected temporary byte verifier with runner47/provider5 match; root did not independently run Docker in that exclusive window. Actual daemon-final-v5 session51147 naturally failed:6cases/4pass2fail/0errors/skips,73.20s. Root independently parsed the XML/log: core success and extension/terminal-receipt checks now pass, but both normal cases fail at the required mcp_tasks.one() assertion with NoResultFound. The corrected Runtime identity is not the new cause. Actual container stack identifies ordinary._structured_content rejecting submit_job because structuredContent=None; no tracking row was inserted. Core success alone is insufficient acceptance.

Root read the actual fixture and installed SDK function metadata code. Bare dict return annotation does not provide the structured schema expected by the strict durable driver. Root independently parsed mcp-structured-red-v1:1failure/0errors/skips, actual CallToolResult has isError=False and valid task JSON TextContent but structuredContent=None. This is genuine stdio protocol behavior, not failed authentication/setup. Only fixture submit/status/cancel return annotations changed to dict[str,str]; production ordinary driver remains strict and tracking assertions remain. New C04 actual stdio test exercises all three structured outputs. Root independently parsed mcp-structured-green-v1:4passes/0failures/errors/skips (protocol plus authentication),3.71s.

Final-v6 freeze57source/18inputs pre-image SHA256285f5bcad2878aa15375de3a5ebdf35b39d03b7c77dce7846ff63adcabadc512. Root independently verifies zero hash mismatches; v5→v6 differs only c04_mcp_fixture.py and test_c04_remote_agent_runner.py, with no build-input/production/guidance changes. Native-v8 omitted TEST_POSTGRES_URI and has825tests/397skips (428passes); it is explicitly not accepted. Correct full-environment native-v9 session75805 naturally exited0, root independently parsed825tests/0failures/errors/skips,122.96s, with frozen pre/post match reported by the implementer. Native825 and protocol4 overlap existing evidence; they are not accumulated.

Final-v6 six-wheel build93846 naturally exited0 and recorded runner sha256:4e053a2b984a76bbc3bd444ca79e609c8b72bcf87954a75c735bdffe8cc98be9/provider sha256:3b5d24fd879c427417f43562aff60b3f31e8b930428a4497661e55de12bae6f4; populated JSON SHA256639b5d87e2db7b0674e10854374f75a209789c8210fbf006f0da4dfb0ff6e26e. Root independently matches current57/18 hashes and these manifest identities. The implementer-run installed byte proof is retained at c06c-installed-bytes-final-v6.log: runner47/provider5; root read its raw outputs, including both actual fleet COPY scripts and new sync.py, but independent root Docker probe remains pending until exclusive-window release. Fresh daemon6 session18544 is running only after native PG finished; full installed matrix/final SPEC, QUALITY and independent required whole/root gates still pending. C06/OpenSpec6.* remains unchecked/uncommitted; later C/BC and activation remain pending.


C06 final-v6/v7 remaining fixture failures and final-v8 daemon proof

Root independently retained v6 daemon XML:6 cases/4 passes/2 failures/0 errors/skips. Required actual child memory was missing in tool mode; Local parity correctly rejected the fixed external MCP handle on a genuinely new submission in middleware mode. Fixture fixes preserve production duplicate ownership checks and all actual durable-row/parity assertions. Role-specific memory-call detection avoids inherited parent messages suppressing a child call; new submissions now use distinct UUID external handles. The structured-protocol green receipt (4 passes,3.71s) and separate role-handle receipt (4 passes,3.58s) are distinct files and were not overwritten.

Final-v7 daemon yielded6 cases/5 passes/1 failure/0 errors/skips. Middleware complete Local/Remote parity passed; tool child memory remained absent because existing subagent get_available_tools does not automatically append the lead memory tool. Final-v8 changes only the C04 acceptance fixture configuration/test: operator-configured tools explicitly include the existing memory_add_tool in tool mode, through the normal shared loader. No production source, packaged guide or default subagent memory policy changed. Formal v7 source/fixture SPEC passed while final runtime approval remained withheld.

Final-v8 source57/build-input18 freeze pre-image SHA2565c4845cdd8ef70c2f69546511b756944c0a0e870fad382ae720ccfa05eca53a3; populated JSON SHA2561502349a3bd1e5ef329449ab5d4c86326edd49a133f1021130a4d53c7e7aa308. Root independently verified every source/input hash and coverage of every changed/untracked backend file with zero mismatches/omissions. Implementer reports build45639 natural exit0 with frozen pre/post match. Immutable runner sha256:47b6c0c05ce6fcccdb8be7c3146f8787b87576f94beb7fd0d926b60e5b47df62; provider sha256:d42d341108ba67b632581521ca78b16efb423b44e7b50be9b3952417ff321796.

Implementer-run installed byte proof matches runner47/provider5. Root independently parsed fresh c06c-installed-daemon-final-v8.xml:6 passes/0 failures/errors/skips, XML87.154s (pytest87.32s). Original actual child memory, MCP original identity and complete Local/Remote parity assertions pass in both modes. Root has not yet independently run Docker/PG. Full required installed matrix session54311 is running under the sole implementer's exclusive PG/Docker window. Final whole-C06 SPEC, fresh QUALITY and independent root delivery gates remain pending; C06/OpenSpec6.* stays unchecked/uncommitted. Later C/BC and activation remain pending.


C06 final-v8 complete required installed matrix

Implementer confirms session54311 naturally exited0, with all build/daemon/full sessions terminal and PG/Docker exclusivity released to root. Root independently parsed /private/tmp/c06c-installed-full-final-v8.xml:704 cases/0 failures/errors/skips, XML259.633s; actual log259.87s and frozen pre/post match. Partitions:existing C06 199,profiles143,tracking62,C05 130,C04 96,C01-C03 74. This includes the daemon subset rather than adding its6 cases. Source57/input18 and immutable image identities remain final-v8. Implementer reports Ruff clean,1426 already formatted,diffcheck clean; these are not the root's final independent gates. Final formal SPEC report and fresh QUALITY are pending, followed by independent root installed-byte/required-C/Local/B/full-backend/static gates. C06/OpenSpec6.* remains unchecked/uncommitted and activation closed.


C06 final-v8 SPEC approval and fresh QUALITY teardown finding

Formal /private/tmp/c06-full-spec-review-v8.md approves whole-C06 frozen source and actual required installed runtime704/daemon6. This permits fresh QUALITY, not delivery acceptance. Fresh whole review starts from accepted C05 basecd251cbf through HEAD plus all dirty/untracked C06 sources. Root has not started final independent PG/Docker gates.

Fresh QUALITY identifies a concrete Important teardown path: runner_context.stop_plugins catches drain_extension_dispatches cleanup-budget OwnershipRejected and continues stop_services while the actual owner-loop writer Task may still be rolling back. If pending ownership remains, release/reset fail closed, but AsyncExitStack.aclose continues other callbacks, including memory/MCP/Store/checkpointer and engine cleanup. notify.cancel_and_settle explicitly retains ownership after its separate cleanup timeout; no production process-abort-before-unwind branch was found. Existing barrier tests prove within-budget actual-task settlement, not the actual host-close resource sequence when cancellation cleanup exceeds budget. Root independently inspected this same source path and agrees it requires correction.

The sole implementer is assigned an actual host-environment close plus PostgreSQL rollback barrier beyond a bounded injectable cleanup budget: retain genuine behavior RED, then fix resource sequencing/retention without losing original scope, diagnostics or Local compatibility. PG/Docker window returned exclusively to that implementer; root final gates remain pending. A production fix will supersede final-v8 freeze/runtime evidence for final acceptance, require relevant packaged-guide review, refreshed source/image/runtime proof and formal review. C06/OpenSpec6.* remains unchecked/uncommitted; activation and later C/BC remain pending.


C06 actual host teardown genuine behavior RED

Formal fresh /private/tmp/c06-whole-quality-review-v8.md is NOT APPROVED: Critical0/Important1/Minor0. Only blocker is actual host resource unwind before an owned extension writer's rollback/finally settles. Root independently parsed c06c-host-teardown-red-v3.xml:4 failures/0 errors/skips, XML2.998s/log3.06s. Cases are detached-close,detached-build-failure,awaited-close,awaited-build-failure, through actual build_agent_environment and real PostgreSQL BoundMutationTransactions INSERT. Each captures effects while writer done=False and finally awaits the barrier: service-stop, failure-release, notify-reset, Store/checkpointer exit and sync/async engine dispose already occurred. Root inspected the test's early capture before barrier release; release afterward permits safe fixture cleanup, actual row rollback and lock release checks. Earlier v1 pool-init timeout setup and v2 finally/awaiter masking are retained non-accepted attempts, not substituted for genuine v3 behavior RED.

Minimal fix under review gates the entire host resource stack before any unwind, distinguishing settled rejection from pending actual writer cleanup. Both ordinary close and failed construction need retained original-scope resources/diagnostics and safe trusted retry after settlement; no model-visible cleanup capability or new C09 supervisor protocol. Actual bootstrap caller consumption is being assessed explicitly, rather than assuming asyncio.run shutdown provides the intended policy. Only sole implementer edits technical sources; formal source/runtime reapproval, QUALITY and independent root gates still pending.


C06 host guard GREEN and actual bootstrap RED; auto-review authorization pending

Root independently parsed c06c-host-teardown-green-v1.xml:4 passes/0 failures/errors/skips, XML2.703s/log2.77s. The pre-aclose host gate prevents early service/resource unwind across both close/build-failure and detached/awaited cases; actual writer rollback and lock release pass after barrier release. This is an interim source fix, not whole-C06 acceptance.

Root independently parsed c06c-bootstrap-teardown-red-v1.xml:4 genuine assertion failures/0 errors/skips, XML2.937s/log3.00s. Each actual bootstrap case returns before original owner-loop SQL cleanup settles. The formal QUALITY reviewer evaluates that retaining references then rethrowing into asyncio.run is insufficient: implicit cancel-all/loop shutdown does not supply an explicit safe settlement/retry caller. An actual caller must keep the original loop/authority, settle the true writer Task, then retry cleanup and propagate the original error.

The proposed multi-file neutral pending-cleanup plus host/notify/bootstrap settlement patch was rejected by automatic approval review before any writes. Stated reason: cross-file persistent resource lifecycle and cancellation changes include waiting without a clear overall time limit, which could hang shutdown or retain resources; this specific side effect lacks explicit user authorization. The rejected scheme will not be split or reexecuted. Root is preparing the exact proposal and test evidence for a direct user authorization decision; unaffected source review and documentation continue. No new final freeze/build, root acceptance, checkbox, commit, activation or deployment is claimed. The goal remains active while awaiting this specific authorization; later C/BC remains pending.


C06 authorization continuation: verified physical boundary and bounded alternative

The goal continuation is not an explicit answer to the pending human authorization question. The rejected unbounded-settlement patch remains unexecuted; its SHA2560fca8a205b94cc63deca36dbd3e0b90568150bd78fbc0660a517a42f26b4ed03 was rechecked. Root readonly inspection confirms AgentDockerContainers runs the bootstrap as the dedicated container Python entrypoint, NodeDaemon bounds watchdog by min(lease/execution remaining), and DockerContainers.stop validates original attempt labels then kills/inspects that container; CLI commands have15s timeout. No code, service, PG/Docker test or rejected candidate was executed during this audit.

Formal QUALITY reviewer technically assesses an explicit bounded physical termination boundary as a viable C06 alternative, with strict isolated-entrypoint/cumulative-deadline/original-authority/no-success-proof constraints. This assessment is not user permission or implementation approval. Reviewable policy /private/tmp/c06c-bounded-cleanup-approval-proposal.md proposes90s cumulative monotonic cleanup budget (normal drain/cancel settlement/service cleanup share it, no retry reset), successful actual writer settlement before resource unwind, and dedicated worker-only nonzero physical self-termination if the budget expires. Normal host/Gateway/embedded close never terminates its process. Existing daemon stop proof/failed-attempt bookkeeping remains authoritative; no C09 protocol or fresh Agent runtime.

This genuinely bounded alternative avoids the rejected infinite-wait policy, but has the explicit different cost of skipping remaining worker application cleanup. User authorization is being requested for that concrete side effect; neither policy is executed. Mandatory future evidence is actual isolated process/PG barrier deadline termination plus independently observed rollback/lock release, successful-settlement resource order and full refreshed SPEC/QUALITY/root acceptance. C06/OpenSpec remains unchecked/uncommitted; activation and later C/BC remain pending.


C06 human steering: bounded cleanup budget120seconds

The user directly replied to the bounded worker-only termination policy approval question: “90秒是不是有点短，120秒？”. This counterproposal accepts the bounded approach with a120second total budget. Proceed with that user-adjusted policy rather than asking the same permission again. First teardown establishes one monotonic deadline; drain, cancellation settlement, dedicated original-loop pending cleanup and service/resource cleanup share it, including retries. Dedicated worker entrypoint only may physically exit nonzero at the deadline; host/Gateway/embedded factories remain fail-closed and do not exit their callers. No original infinite-settlement patch is executed or split.

Updated reviewable policy /private/tmp/c06c-bounded-cleanup-approval-proposal.md. The sole implementer will write genuinely bounded source/test corrections, prove actual SQL barrier settlement and real isolated-process deadline exit, preserve original error/authority and Local behavior, then coordinate guides/new freeze/images/SPEC/QUALITY/root gates. Earlier hostGREEN4,bootstrapRED4,v8matrix704 remain historical supporting evidence only. No C06 checkbox, accepted commit, deployment or activation is claimed. Full B→C→BC objective remains intact. The goal tool still reports blocked from the prior authorization audit; that tool cannot resume goals, but the human's new steering authorizes continued concrete work.


C06 user-authorized120 policy: actual isolated-entrypoint deadline RED

Root independently parsed /private/tmp/c06c-bounded-bootstrap-red-v4.xml:4 genuine failures/0 errors/skips, XML22.912s/log22.99s. Cases detached-close,detached-build-failure,awaited-close,awaited-build-failure launch the actual isolated bootstrap entry in real subprocesses and PostgreSQL writer INSERT/finally barriers. Trace proves writer-cleaning and process/DB PIDs; old entry remains alive beyond the trusted test-only0.3s total budget until the parent5s containment limit. Each fails the actual bounded deadline assertion. Parent containment stops owned test processes. Independent PG rollback/connection/lock checks are located after the failed deadline/exit assertions and were not reached in this RED; the required GREEN must actually execute them. Earlier v1JSON/v2import setup and mixed v3 fixture issues are retained non-accepted attempts, not the clean behavior RED.

The sole implementer now implements the genuinely bounded120-second policy using one first-teardown monotonic budget shared across retries/phases, actual-task settlement and explicit dedicated-worker-only nonzero physical exit at deadline. The original unbounded181-line rejected patch remains unexecuted. Root technical source writes remain limited to packaged guides after GREEN; current source/image evidence is still historical pending new freeze/runtime review and independent gates. No C06 acceptance, completed commit, deployment or activation is claimed.


C06 bounded120 first actual GREEN and guidance coordination

Root independently parsed /private/tmp/c06c-bounded-bootstrap-green-v1.xml:12 passes/0 failures/errors/skips, XML12.631s/log12.71s. Eight actual host/bootstrap within-budget cases cover close/build-failure and detached/awaited paths. Four actual isolated subprocess deadline cases exit70 and reach retained no-early-resource-unwind, independent PG uncommitted-row rollback, original connection gone and FOR UPDATE NOWAIT assertions. Production total is120seconds; tests use a trusted short injected budget. This replaces neither final installed full-matrix evidence nor formal review.

Root updated extension/runtime packaged guides with whole-stack pending retention, original-scope settlement/retry, single first-cleanup monotonic120 deadline, isolated-entrypoint-only nonzero physical exit and unchanged daemon physical proof responsibilities. Remaining Local/error/foreignretry/noreset self-audit and full native are implementer-owned. Temporary byte verifier now checks changed libexec_bootstrap.py twice for runner: wheel module and actual /opt/deerflow/libexec_bootstrap.py entry COPY; provider checks the wheel only. This prepared verifier has not yet run on refreshed images. Source freeze/build remain pending root guide/static coordination and implementer test terminal. C06/OpenSpec remains unchecked/uncommitted, with SPEC/QUALITY/root gates and later C/BC still required.


C06 bounded120 original-error regression and focused15GREEN

Root independently parsed c06c-cleanup-original-error-red-v1.xml:3cases/2 genuine failures/1pass/0errors/skips, XML2.122s. Actual bootstrap build/run original ValueError was replaced by secondary settled OwnershipRejected. Implementer adds private original-error retention and bootstrap precedence without altering the120deadline/worker-only physical exit policy. Intermediate bounded-green-v2/v3 fixture expectation/empty-context issues are not new product RED.

Root independently parsed c06c-bounded-bootstrap-green-v4.xml and log:15passes/0failures/errors/skips, XML12.760s/log12.91s,143deselected. Focused15 includes host/bootstrap8, real isolatedPGprocess4 and error/budget3; it overlaps the earlier12 and is not accumulated. Implementer natural terminal/full native/self-audit remain to be reported before freeze/build/review. Actual extended installed image and independent final gates are pending. Root guide check after120policy documentation:24guides/0errors/3softwarnings (memory90144 original; extensions82311/runtime82672 new; all below98304hard). Diffcheck passes. No completion/activation is claimed.


C06 bounded120 focused17 and full native842 verified

Root independently parsed bounded-bootstrap-green-v5 XML:17passes/0failures/errors/skips (13.329s XML/13.40s log). Adds actual caller cancellation preservation and private watchdog first-teardown arm/no deadline extension/normal finish disarm. Full native-v10 naturally exits0:842passes/0failures/errors/skips (129.460s XML/129.55s log), comprising199existing C06+160profiles+62tracking+421Local across24modules. Counts overlap and are not accumulated. Real isolated process cases exit70 and independently prove PG rollback, connection disappearance and lock release.

Root finalized extension/runtime guides, qualifying the no-resource-unwind promise while writer rollback remains unsettled; diffcheck passes. Sole implementer authorized to produce final-v9 source/input freeze only. SOURCE SPEC then QUALITY and refreshed six-wheel immutable-image/actual-entry byte proof/full installed gates remain pending. Independent root acceptance remains required. C06/OpenSpec unchecked, no commit/deployment/activation; later C/BC remains pending.


C06 final-v9 source freeze and formal SPEC hold

Root independently validated final-v9 d70e395f8bc7514870145223e91036bf9e04b2a6d1fdca67d6ab8a782c1494cf:60source+18inputs, all hashes match, no uncovered dirty/untracked backend sources. Ten changed sources from historicalv8 include actual hardened entry/neutral cleanup/process fixture and guides. No images built.

Formal SOURCE SPEC identified an additional original-error precedence boundary: immediately settled environment.close or failed-build teardown.close can directly raise secondary OwnershipRejected rather than PendingAgentCleanup, overriding the original run/build error. The existing17GREEN covers pending-to-settled error identity but not this immediate-settled path. Root keeps the full error-identity contract and holds freeze/build; sole implementer is assigned actual-entry RED then minimal correction/GREEN, followed by replacement freeze and renewed SPEC→QUALITY. No acceptance, completed checkbox or commit is claimed.


C06 original-error direct exits22GREEN; early cleanup budget closure required

Root independently parsed immediate-original-error-red-v1 andv3:each2 genuine failures/0errors/skips. Four real exits cover run ValueError, original CancelledError, post-service actual build failure, and early checkpointer constructor failure followed by engine disposal secondary error. v2 fixture teardown error is not clean RED. Minimal production catch preserves original BaseException by identity and secondary cause through both direct and pending paths. Root independently parsed bounded-bootstrap-green-v7:22passes/0failure/error/skip; includes close-only positive. v6 double-layer engine-dispose expected-event fixture issue is not product RED. Full native-v11 still running at this receipt.

Sole implementer self-audit and formal SOURCE addendum /private/tmp/c06-source-spec-early-cleanup-addendum.md confirm another concrete early-budget omission: AsyncExitStack resources precede teardown controller creation, so early failure calls raw stack.aclose without starting120budget, retaining actual phase or arming isolated watchdog. Locked first-cleanup120 contract includes this path. Root assigned actual host/isolated PG barrier RED, then controller created before first resource under original bootstrap cleanup scope, same context/stack/budget retained when fullprivate scope is installed, unified pending/error paths. No raw early aclose exception bypass or deadline reset allowed. No freeze/build until correction and renewed source review; C06 remains unaccepted and later C/BC pending.


C06 early budget genuine RED and24focused GREEN

Root parsed full-native-v11:847passes/0failures/errors/skips, XML143.288s/log143.41s, natural exit0; this proves prior immediately-settled original-error correction, not later early-budget bytes. early-budget-red-v1 fixture used wrong BoundMutationTransactions argument and did not enter SQL; it is not accepted behavior RED. Root independently parsed red-v2:2genuine failures/0errors/skips, XML7.440s/log7.51s. Actual host bound SQL cleanup enters resource-close barrier but no first-teardown observer fires; actual isolated process enters SQL cleaning then remains alive beyond trusted0.3 deadline until parent5s containment kill. PG rollback/connection/lock postchecks were not reached in RED.

Implementer now constructs original FleetMutationCapability/controller/bootstrap private cleanup scope before first resource, later replaces full private scope while retaining original context/stack/budget, and removes raw early aclose bypass. Root independently parsed bounded-bootstrap-green-v8:24passes/0failures/errors/skips, log16.34s. Host early resource phase caller cancellation retains real task, missing/foreign retry rejects, same deadline and eventual registry release pass. Isolated early actual SQL cleaning exits70 and executes independent PG rollback/connection/lock assertions. Native-v12 still running; no replacement final freeze/build/review/root acceptance yet. Runtime guide documents controller-before-resource rule; root guidance check24guides/0errors/3softwarnings, diffclean. C06 remains unchecked/uncommitted; all later C/BC pending.


C06 replacement final-v10 SOURCE SPEC approved; QUALITY pending

Sole implementer reported full-native-v12 naturalexit0; root independently parsed849tests/0failure/error/skip (XML132.683s/log132.79s). Root independently validated replacement final-v10 /private/tmp/c06c-source-final-v10.json SHA828fdfd87d39050b1edc40e2bb15f91f16c1b53c21cac6852f0174165c6531f3:60source18inputs allmatch, no dirty/untracked backend omission. v9→v10 five source changes, no build-input change; no new images yet.

Formal /private/tmp/c06-full-source-spec-review-v10.md SOURCE SPEC APPROVED closes immediate/direct and pending original build/run/cancellation identity, close-only failure, and early first-resource cleanup budget. Whole-C06 baseline preserved, original context/stack/budget retained, only later scope callback replaced, raw early stack bypass removed.24focused/849native independently zero. Root dispatched fresh SOURCE QUALITY to the whole reviewer. Hold immutable source until QUALITY result; then build six wheels/new immutable images, prove actual /opt and wheel rawbytes plus installed fullmatrix and independent root complete gates. Historicalv8 image/runtime evidence is not current final proof. No C06 checkbox/commit/activation/deployment or later C/BC completion is claimed.


C06 fresh SOURCE QUALITY v10: extension closed, actual native-memory owner remains Important

Formal /private/tmp/c06-whole-source-quality-review-v10.md:NOT APPROVED, Critical0/Important1/Minor0. Original extension/earlyresource cleanup issue is source CLOSED and60/18frozenhashes plus24/849XML independently match. New soleImportant is concrete existing configured transactional PostgresMemory native queue:shutdown_flush=False explicitly means original non-daemon worker stillalive; inherited MemoryManager.close is no-op. Current close_memory ignoresFalse and permits stack resource unwind/controllerclosed/registryclear andbootstrapwatchdogdisarm, so native worker can outlive dependencies or hang interpreter beyond120deadline. Existing fences are not alleged weakened; this is trueworker/resource lifetime.

Root assigned sole implementer actual configuredPG native-thread/SQLbarrier RED and minimal remote-only pre-unwind positive-quiescence gate under same fixed120deadline. Retain complete original owner/context/resources through real worker completion; only actual shutdown/worker positive boundary counts, not asyncio.to_thread task returningFalse. Cover service stop and possible subsequent enqueue before resource unwind; preserve Localbest-effort policy and existing unsupported-provider rejection, no newmemoryproduct/defaultbackend/genericbackend support. Sourcefreezev10 is historical held; nofinalimagebuild or acceptance until corrective GREEN/freshSOURCE SPEC→QUALITY, then distinct installedbyte/runtimeproof and rootgates. C06 unchecked/uncommitted, laterC/BCpending.


C06 actual native-memory RED and joint-quiescence correction

Root independently parsed native-memory-teardown-red-v2:5genuine failures/0errors/skips, XML6.283s/log6.43s. Three host cases configure real PostgresMemory.add_nowait/native Thread/bound synchronous transaction SQL INSERT/finally barrier, and actual close/buildfailure/stop-enqueue incorrectly complete after shutdown_flush(False). Two isolated actualnativeSQL cases entered/cleaning then physically exit70 but fail the no-early-service-stop assertion; these are ordering defects, not deadline-overrun failures. Independent PGpostchecks follow those failed assertions and were not reached in RED. v1 mixed fixture imports/private-manager setup errors are not clean whole RED.

QUALITY technical feedback requires joint memory/extension quiescence: memory extraction can dispatch observer, observer can enqueue memory. Under one originalscope/first120deadline, positive memory drain plus actual owned observer drain must repeat when private enqueue/actualTaskcompletion revision changes, before service stop and again after stop before resources. Current pendingFalse alone cannot prove no prior observer queued native work. Sole implementer assigned minimal remoteonly private revision/fixedpoint integration; False/exception never grants close/unwind, preserve firstfailure/resources/actualphase/watchdog, avoid busyloop. No generic memory backend introspection/expansion or Local shutdown behavior change. Await actualGREEN, guides/freshfreeze/fullnative, formalSOURCE SPEC→QUALITY, immutableimages/rawbyte/installruntime/rootgates. No C06 acceptance or later C/BCcompletion.


C06 native-memory joint gate30GREEN/native855; service-internal sequencing follow-up

Root independently parsed bounded-bootstrap-green-v9:30passes/0failures/errors/skips, log20.53s. Adds real nativehost3/isolated2 plus actual memoryextract→ownedobserver→secondmemory nativeSQLbarrier roundtrip to historical24. Actual isolatednative cases now reach code70/noearlyservice/reset/resourceunwind and independentPG rowrollback/connection disappearance/lock-release assertions. Controller gets memorypositive callback immediately after factory; private dispatch revision tracks enqueue and actualTaskdone, not pendingFalse. False/exception retainsfirstfailure and .05s clippedpacing withinfirst120; memoryclose runs only after positivejointgate.

Root synchronized memory/extensions/runtime guides:positive nativeworker drain+ownedobservers共同静止 before/afterservices; Localbest-effort unchanged. Guidance24/0errors/3softwarnings, allbelowhard98304; diffclean. Root independently parsed full-native-v13:855passes/0bad, naturalexit0/log137.33s. Before finalfreeze, soleimplementer self-audit identified stop_plugins internal extension-only drains could wait/cancel an observer awaiting memoryflush before outerpostjointgate. Root assigned actual postserviceroundtrip RED then samefixedcontrollerjointdrain integration; this is the alreadylocked before/afterservice sequencing contract.855 is interim evidence, not finalnewbyteacceptance. No finalreplacementfreeze/build/acceptancecheckbox/commit; formalSPECQUALITY andinstalled/rootgatesremain.


C06 replacement final-v11 SOURCE SPEC approved with31/856GREEN

Root independently parsed postservice-roundtrip-red-v2:1actualfailure/0errors/skips, XML2.578s/log2.65s. Actual service.stop entered owned observer awaiting memoryflush after firstnativeSQLcommit; old internal extensiononlydrain failed secondnativeSQL/finallymarker. v1 actuallyPASS (race allowedouterpostjointgate) is positive/not RED. Sole implementer replaced internal drain with samecontroller jointquiescence. Root independently parsed bounded-bootstrap-green-v10:31passes/0bad; full-native-v14:856passes/0bad, naturalexit0/XML145.181s/log145.28s. Counts are fresh matrices, not cumulative.

Root independently verified final-v11 /private/tmp/c06c-source-final-v11.json SHA7c46ce7f751c21383f3c08ee975ba0675769cbf14dc24ccebaf0a504c2e63937:60source18inputs allmatch anddirty/untrackedbackendcovered. v10→v11 seven source differences andzeroinputdelta. Formal /private/tmp/c06-full-source-spec-review-v11.md SOURCE SPEC APPROVED:positive memoryisTrue,firstfailure/pendingphase/resource retention, enqueue+actualTaskdone revisions/fixedpoint allservicepre/post/internaldrains, pureclosepositivegate; v10originalerrors/earlycontroller and120deadline/onlyhardentryclosure retained. No images yet. Fresh formal SOURCE QUALITY dispatched; source remains frozen until result. Newwheel/images/actualoptbytes/installedmatrix andindependentrootallgates stillneeded. C06unchecked/uncommitted, activationclosed/laterC/BCpending.


C06 final-v11 SOURCE QUALITY approved; immutablebuild authorized

Formal /private/tmp/c06-whole-source-quality-review-v11.md whole-C06 SOURCE QUALITY APPROVED:C0/I0/M0. Independent60source18inputs hash/JSON7c46… and31/856XML plusactualnativeRED5/postserviceRED1 verified. Originalv8extension andv10native-memory Important issues are SOURCE CLOSED; fixedpoint servicepre/post/internaldrains retain actualnativeworker/resources/first120deadline/originalerrors. Wholebaseline unchanged mutation/privacy/Local contracts retained. This is source approval only, not installedruntime acceptance.

Root authorized soleimplementer sixwheel/newimmutable runner+provider build from exactlyfinal-v11sources/inputs, sourcepre/posthash and naturalterminal proof. Imageids populatefreeze metadatawithoutmapchange; newpopulatedJSONhash recorded. Preparedinstalledbyteverifier must compare bothFleetwheellibexec andactual /opt/deerflow/libexec_bootstrap.py, neutralhelper,hostcontroller/notify/agent_runner andotherchangedproduction. Then freshnormaldaemon6 andrequiredwholeinstalledmatrix/zero skips; historicalv8images/704/6 do not substitute. NativephysicalSQLdeadline fixture remains a distinctlayer fromnormalinstalledhardentryproof. PG/Docker stayssoleimplementer-exclusive until explicitrelease; root then independentbytes+complete C/B/Local/backend/static gates. NoC06checkbox/commit/activation/deployment or laterC/BCcompletionyet.


C06 final-v11 fresh immutable images, byte proof and daemon6 verified

Soleimplementer build2023 naturallyexit0, log/private/tmp/c06c-image-build-final-v11.log with60/18pre/postmatch. Runner sha256:ee7993587a281c5f46df6a0833b52b6b76de629e35504dbdf533974c8bc4e80d; provider sha256:5481f9297b6cb4228cdd2535621a52721d5b841150d61a0d273db847c58e1ce4. Six originalwheel files/rawSHAs preserved in/private/tmp/c06c-six-wheels-final-v11.json; root independently hashedall6/zero mismatch. Populatedfinal-v11JSON SHA5aeca573acc8a8ebdc803d46bc021092c208f2740d72caad902b6b2ee4f3fdc7. Root removedimages metadata only and exactly reconstructed original7c46…JSON SHA, proving no source/inputmetadata drift; all60/18currentfiles independently stillmatch.

Preparedinstalledbytes verifier implementer-owned naturalexit0, /private/tmp/c06c-installed-bytes-final-v11.log:runner50/provider7rawfilesmatch, including actual /opt/deerflow/libexec_bootstrap.py andwheelmodule separately, newneutralhelper/controller/notify/guidebytes. Root read this log but has not independently runDocker bytechecks while exclusivewindow remainschild-owned. Root independently parsed newinstalleddaemon-final-v11XML:6passes/0failure/error/skip, naturalexit0/log79.58s/preposthashmatch; existingwarningsvisible. Freshinstalledfull C01-C06+profiles174/tracking62/Local24421 expected1156 isrunning underchild-exclusivePG/Docker; finalruntimeSPECQUALITY androotcompletegates remainpending. Historicalv8installed704/6 do not replace these newimageproofs. C06notaccepted/ticked/committed oractivated; laterC/BCpending.


C06 finalruntimeSPEC approved; independentroot435 teardownerrors prohibit acceptance

Formal /private/tmp/c06-full-spec-review-v11.md wholeSOURCE+RUNTIME SPEC APPROVED independently retains60/18freeze,6wheelSHA,actualinstalled50/7bytes anddaemon6/full1156zero/naturalterminal. Root independently ran actualDockerbyteverifier50/7naturalexit0; then wholefinaldriver /private/tmp/fleet-c06-root-final-v11-driver.log session7274naturallyexit1 on firstmutationpartition. Actual435behaviorcasesPASS but two fleet_database fixtureteardownERRORS occur for actual_worker_subagent_rejection_fences_and_completes_cleanup threshold/end. Temporaryschema DROP CASCADE deadlocks AccessExclusive with anothertransaction RowShare; original logs/XML retained /private/tmp/fleet-c06-root-final-v11-mutation.*. RemainingC/B/Local/fullbackend/staticgates were not run;1156formerpass cannot replace independentrootfail.

Root appliedsuperpowers/systematic-debugging skill and transferredexclusivePG/Docker back tosoleimplementer for minimal reproducible diagnosis: distinguish productactualownedTask/transactionlife gap fromfixturetooearlyDROP; do not blindretryDROP, omitcases, killunknownhistoricalPG or broadlydrop. Root hasstoppedalltests. Production/sourceguidechanges requirefreshfreeze/SPECreview/QUALITY/images/runtime; test-onlyfixturechanges still requireupdatedfreeze/review andconfirmedproductionbyteinvariance beforeimageproofreuse. Noactualrootacceptance/C06checkbox/commit/activation/laterC/BCcompletion claimed. FinalQUALITY receivesactualrooterrors as deliveryboundary.


C06 root deadlock diagnosed as actual Agent stream lifetime; deterministic RED

Soleimplementer originalSQL/ownedpendingTask diagnostics locate DROP deadlock opponent in sameoriginalexecution guard SELECT fleet_nodes FORUPDATE, notunknownhistoricalPG. Diagnosticv3 3passes stillrecords actual AsyncPregelLoop checkpoint callbacks/FencedAsyncPostgresSaver.aput_writes/asyncgeneratorathrow live after run_agent andjournal taskssettle. Actualastream exitwasGCscheduled, notawaited byworker asyncfor premature exit. v1/v2diagnosticsetup/scope disturbances are not acceptedRED. Productlifetimegap identified, notfixtureDROP policy; noblindretryor taskkilling used.

Root independently parsed stream-settlement-red-v2:3tests/2genuinefailure(threshold,end)+1pass(pending),0error/skip/naturalexit1. Retains originalactualPregel.astream reference soGCcannotmask ag_frame stilllive whenrun_agentreturns; finallysettlesonlyownedrecordedstream toavoidfixturedeadlock. v1assertwritefailureproduced3pass isnotRED. Soleimplementer assigned minimal explicitstreamclosurebothsingle/multimode preservingoriginaltyped/cancelidentity. Rootapprovedneutral privateoptionalRunContext settlementhook tooriginalcontroller phase, neutralcontrolpendingmarker(nonordinaryrecovery/nocapabilityhandle), first actualunexhaustedstreamcleanup activates same120deadline beforeenvironment.close; bootstrapjoinsretainedgraphphase beforememory/observers/services/resources. OrdinaryLocalexplicitlyclosesstreamwithnoselfexit; normalnaturallyexhaustedstream doesnotturn120 into runexecutiontimeout. ActualcheckpointSQLrollbackbarrier/isolateddeadlineproof needed. Newproductiondelta requiresnewfreeze/fullreviews/wheels/images/runtime/rootgates; v11proofhistorical. WholefinalQUALITYwithheld, noC06acceptance/tick/commit.


C06 stream settlement focused GREEN; bounded-proof provenance

Root independently parsed stream-settlement-green-v10: 10 passes, zero failures/errors/skips. The cases cover original single/multimode astream settlement and checkpoint completion, original error identity/control handoff, retained host graph SQL rollback phase across cancellation/retry, and isolated post-node checkpoint cleanup. The control-handoff cases are not themselves actual pending-SQL proofs. Runtime guide now records the private RunContext.settle_stream hook, naturally exhausted stream exemption, first live-stream cleanup starting the same cumulative 120-second budget, retained graph phase before all host resources, and Local explicit stream closure without process self-exit. Guidance checks: 24 checks, zero errors, three soft warnings; diff whitespace check passed.

Earlier stream-budget-red-v2 did not establish actual cleanup entry and is not accepted cleanup-deadline RED; the initial inference was withdrawn. Calibrated stream-budget-green-v7 reaches actual post-node checkpoint INSERT/finally, isolated exit 70, and independent rollback/backend disappearance/lock checks. Sensitivity-red-v8 restores only the original v11 _stream_once body while retaining the additive hook field; its single real failure proves premature service/resource ordering, not physical deadline overrun. Restoration SHA equals the pre-experiment SHA. These are fresh individual matrices, not accumulated counts.

The original root mutation partition is being rerun under sole implementer PG/Docker ownership. A self-audit found wait_for_cleanup could propagate a settled graph phase exception before retrying the remaining original stack; deterministic real-host RED and minimal classification fix are pending before source freeze. No C06 acceptance, task checkbox, commit, activation, or replacement image claim. Fresh source SPEC then QUALITY, immutable wheels/images, installed runtime proofs, and independent root full gates remain required.


C06 replacement final-v12 source freeze; stream lifecycle regression verified

Sole implementer confirmed full-native-v15 natural exit 0: 1616 passes, zero failures/errors/skips (442 mutation = 204 C06 + 176 profiles + 62 tracking; 421 Local C06c neighbors + 753 worker/checkpointer/ownership neighbors). Root independently parsed all 1616 XML cases. The prior 442-only intermediate run predates the final wait error classification and is not final source proof by itself. Actual wait-error-red-v11 has one real SQL rollback CancelledError failure, zero errors/skips; shared settle_graph_stream now records settled Task first failure for both wait and close before remaining original stack cleanup. Final focused stream-green-v13: 10 passes/zero bad. Same original 120-second deadline, no extra Task cancellation or reset.

Root independently verified final-v12 JSON SHA 7d46ff1c00465ecc91a7e08703249be5247006b0634bb5bd153bd05b246c0db6: 60 source/18 inputs raw hashes match, all dirty/untracked backend files covered, images empty. Seven source deltas from v11, zero build input deltas. Original v11 root deadlocks and calibrated negative-control provenance remain recorded. Fresh whole-C06 SOURCE SPEC dispatched first; SOURCE QUALITY, new six wheels/immutable images, installed byte/runtime evidence and independent root full gates remain pending. C06 tasks unchecked/uncommitted, no activation or later C/BC completion.


C06 final-v12 SOURCE SPEC and QUALITY approved; new immutable build authorized

Fresh whole-C06 SOURCE SPEC /private/tmp/c06-full-source-spec-review-v12.md and SOURCE QUALITY /private/tmp/c06-whole-source-quality-review-v12.md both approve exact freeze 7d46ff1c00465ecc91a7e08703249be5247006b0634bb5bd153bd05b246c0db6. QUALITY C0/I0/M0. Both independently verify 60/18 hashes, actual stream lifetime, same first cumulative 120-second cleanup budget, true graph Task retention, settled-error wait/close classification, original identity and Local/private boundaries. Neutral marker has no host fields/imports; host-private cause chaining is allowed but not public serialization. Existing whole-C06 mutation, native-memory and extension fixed-point guarantees remain intact.

Root authorized sole implementer to build new six original wheels/immutable runner and provider from these exact bytes with pre/post hash checks, preserving raw wheel hashes/context and changing only images metadata in freeze. Fresh installed byte proof must separately cover hardened /opt entry and wheel module; normal daemon plus full installed matrix must include both 24 C06c Local and 21 newly affected worker/checkpointer/ownership neighbors, zero required skips. Native physical-deadline fixture is not claimed as an installed-container deadline proof. PG/Docker remains implementer-exclusive until handoff. Fresh runtime reviews and independent root full C/B/Local/backend/static gates still required. No C06 acceptance/check/commit, activation, or later C/BC completion.


C06 final-v12 immutable images and daemon evidence — 2026-10-03

Sole implementer build session74424 naturally exited0; image-build-final-v12 log records all stage1–13 frozen source/input pre/post checks. Runner sha256:a2eda1bbdd7d9897068c342bb3e12a8c8ad87ee1d572969ba0572cb234098c06, provider sha256:ff113e45973509dcfaeccf3b4fd4ccf84188d5d097cd30ccebcac45944b3c7e9. Root independently rehashed all six original wheels in /private/tmp/c06c-six-wheels-final-v12.json with zero mismatches. Populated final-v12 freeze SHA564cf373fdaadcbcc80312f6e17252fc4878d555f2573fe80afbb962d3eb8fd5; root emptied only images metadata and reconstructed exact SOURCE SHA7d46ff1c00465ecc91a7e08703249be5247006b0634bb5bd153bd05b246c0db6. All 60/18 current source/input hashes still match.

Implementer prepared installed byte verifier naturally exited0, /private/tmp/c06c-installed-bytes-final-v12.log runner50/provider7 matched, including actual /opt entry and wheel module separately, neutral marker/worker/private hostcontroller. Root read the evidence but has not yet run its own Docker byte check during the exclusive implementer window. Fresh daemon session88088 naturally exited0: 6passes/zero failure/error/skip, log82.13s; root independently parsed XML. Fresh installed verification matrix1916 is running, including actual installed container pipeline plus host-native PG fixtures and Local24/Local21 regressions; do not describe all1916 as executing inside images. Native shortened-budget physical fixture remains distinct from installed normal daemon evidence.

PG/Docker remains sole implementer-exclusive until natural full-matrix completion and explicit resource release. Fresh whole runtime SPEC/QUALITY and independent root full gates are still pending. C06 unchecked/uncommitted; no activation, later C/BC completion or historical-image acceptance substitution.


C06 final-v12 independent acceptance — 2026-10-03

Root session92763 naturallyexit0/allgatespassed; requiredC/Local/PG1920+B259 zero skipped. Fullbackend13302pass826default-suite skip1deselected; blocking75/boundaries74; allstaticgatespassed. Actualrootv11 deadlocks closed by new originalstream/trueTask settlement underfirst120deadline, not overwritten by historical1156. Whole source/runtime dualreviews approve exactv12. C06 checkboxes nowaccepted; finalnonpackagedREADME/backendorientation/acceptancereceipt reflect this completed slice without changing frozenproduction/tests/packagedguides/wheels. Commit remains pending finaldocumentationchecks and explicit staging. C07-C12/BC/activation stillpending.

Final nonpackaged acceptance documentation: guidance24/0errors/6soft chain warnings (all below hard limits), OpenSpec3/0 and diff0; frozen technical60/18 and wheels unchanged.
