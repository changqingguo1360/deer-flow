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
