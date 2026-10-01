# B local acceptance — 2026-10-02

B runtime requirements pass isolated local PostgreSQL/Docker/NAS/Compose acceptance.
C and BC remain unimplemented. All feature flags default off. Production ECS/NAS
deployment and Linux-host Compose runtime are not claimed.

| Check | Actual result |
|---|---|
| Explicit B gate, all final files | 259 passed, 0 skipped, 118.03 seconds, 2 known warnings |
| backend make test, no UV_NO_SYNC/UV_OFFLINE | 13187 passed, 195 optional skips, 1 deselected, 264.59 seconds, 19 known warnings |
| backend make test-blocking-io | 75 passed, 0 skipped, 4.16 seconds, 2 known warnings |
| Full backend Ruff check/format | 1385 files clean |
| OpenSpec strict all | 3 changes pass |
| Spec/security/quality and final whole-B review | Approved; no remaining B runtime blocker |

Default optional skips are not integration evidence. Every required B case executes in
the explicit zero-skip gate. Actual report: /private/tmp/fleet-b03-management-root-gate.xml.
Prerequisites and invocation: [deployment guide](deployment/ecs-fleet.md).

| Requirement | Actual evidence |
|---|---|
| B01 optional/strict startup | test_b01 foundation/startup/installation; disabled/missing package, required/prefix/NAS, durable tracking after admission closes |
| B02 independent migration | test_b02, B01 installation, B03 f0005 upgrade; concurrent migration, actual installed Gateway lifespan twice, host autogenerate |
| B03 node identity/management | test_b03, B06 routes; real JWT/TCP admin session/CSRF, persisted profile list, scoped revoke |
| B04 capacity/draining | test_b04; independent SQL competition, quarantine charged until proved stop |
| B05 staged/dedupe/tracking | test_b05 and B10 receipts; real graph invocation/replay, tracking failure compensation |
| B06 launch/watchdog | test_b06; actual Docker launch once, private journals, control-loss stop and owned-orphan barriers |
| B07 inputs/results | test_b07; actual read-only mounts, safe paths, owner/thread downloads and immutable sealed results |
| B08 cancel/unknown | test_b08; transaction order, actual stopped proof, retained uncertainty and audited operator resolution |
| B09 original tracking/notification | test_b09; real Agent/worker, busy-thread delay, restart/lost reply with one receipt/run |
| B10 schedules/UI | test_b10; trusted slots and replay receipts; recorded frontend unit/check acceptance |
| B11 deployment/disable | test_b11; public CLI, private secrets, verified offline Linux image and real Docker control |
| B12 release faults | test_b12 and test_b_acceptance; actual TCP file/HTTP effects, two Compose daemons, strict report/skip guards |

Current private head f0006_nodes ships six migration assets. Upgrade preserves jobs,
attempts, reservations and credentials. B submits via controlled submit_fleet_job;
status/cancel use existing thread MCP task APIs. No /api/fleet/jobs facade is deployed.

Foundation eb81ca2f, attempts/daemon dff47173/36210a8a, restart bc784e08 and the later
artifact/cancel/notification/schedule/deployment slices are recorded in implementation
progress. Startup/install a72c6d9e, partition gate e95e0ce0, full Compose f2cd4cef and
the present reviewed B03 management follow-up close the earlier foundation obligations.

Initial B01/B02 RED and auth-specific B03 initial RED were not separately recorded.
Current GREEN cannot reconstruct them. Later startup, management, profile/migration
and gate changes have actual RED/GREEN evidence. Historical steps now explicitly
record this limitation instead of inventing retroactive RED. Direct SQL/HTTP/process
observations replace the FleetProbe sketch; the host management adapter/package facade
preserves the specified identity boundary. Added integration GREEN and fixture
synchronization failures are not represented as a new production protocol RED.

C01 must add f0007_agents after f0006_nodes, preserving existing f0002_launch_spec.
C full runner/checkpoint/events/memory fencing and BC continuation gates remain required.

B final implementation and acceptance commit: 518a59cf.
