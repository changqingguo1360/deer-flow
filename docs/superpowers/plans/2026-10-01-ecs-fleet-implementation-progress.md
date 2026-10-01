# ECS Fleet implementation evidence

Date: 2026-10-01. Goal: deliver B, then C, then C → B → C continuations.

Implementation has started in the personal-agent-ecs worktree. None of the three
OpenSpec changes has met its release gate; do not archive them or mark IMPLEMENTED.

## Current code

- B01: standalone optional package, strict profiles and identity-free JobSpec;
  disabled installation does not import the host runtime. Host dependency manager
  installation and deployable configuration remain to be exercised.
- B02: six private fleet_ tables plus independent fleet_alembic_version migration;
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
with both successful and failed compensation. The model-visible Fleet submission
tool and scheduled dedupe integration remain pending (B09/B10).

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

B07 service integration is still pending: immutable input registration/read-only
mounts, accepted-manifest transaction, host download authorization and worker result
publication. B08–B12, all C and all continuation tasks remain pending. There is no
manifest completion endpoint or public runnable worker deployment yet.
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
Current combined verification: 338 passed, zero skipped, four existing
Starlette/httpx and uvicorn/websockets deprecation warnings. FLEET_TEST_CONTAINERS=1 enables real Docker
alongside TEST_POSTGRES_URI for isolated Postgres schemas. Exact regression scope:

```bash
FLEET_TEST_CONTAINERS=1 TEST_POSTGRES_URI=<local-test-uri> PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/fleet tests/test_mcp_task_service.py tests/test_mcp_task_repository.py tests/test_mcp_task_models.py tests/test_mcp_task_ordinary_driver.py tests/test_mcp_task_tool_wrapping.py tests/test_auth_middleware.py tests/test_csrf_middleware.py tests/test_pat_auth.py tests/test_extension_config.py tests/test_extension_api_contracts.py -q -p no:cacheprovider
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
2. Deliver B07 NAS sentinel, authorized mounts, sealing and manifest completion;
   then B08 cancellation/recovery fault cases. Keep success gated on accepted artifacts.
3. B09 wires the tested invocation helper into model-visible submission; B10 handles
   scheduled dedupe. Deliver B11/B12 before enabling C. Keep C/continuation flags off.

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
