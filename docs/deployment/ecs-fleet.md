# ECS Fleet durable-job deployment

B durable jobs share the Fleet control plane with the later C remote Agent runner.
C and continuations are not available yet. All feature flags default to false. This
B11 host entry/deployment helpers are locally verified; local build evidence does not
pass the B12 release gate.

## Components and identities

The Gateway owns admission, PostgreSQL tracking, capacity reservations and result
acceptance. Each worker makes outbound requests with its own `df_fleet_` credential,
controls only its node's Docker execution, and keeps a private durable attempt journal.
Workers expose no HTTP listener and publish no ports. A node credential does not grant
user, administrator, session or PAT identity. Use HTTPS outside loopback.

The worker daemon controls the host Docker socket; job containers never receive that
socket, daemon credentials or the private journal. Jobs use a pinned operator profile,
explicit non-root UID, read-only root filesystem, CPU/memory/PID limits, and only their
declared read-only inputs plus writable attempt output. B has no Agent/model loop in
the worker image.

NAS must have the same absolute path in the Gateway, worker daemon and Docker host.
The daemon passes those absolute bind paths to the host Docker API; mounting NAS at a
different path inside the daemon does not remap the job's host path. Provision an
explicit deployment root and `.deerflow-fleet-root` identity sentinel. The runtime
refuses a missing/wrong sentinel or symlink path rather than initializing another root.
Keep worker state outside NAS and outside every job mount, owned by the daemon with
private permissions. Retain state across worker replacement.

## Trusted operator entry

Install the trusted Fleet package in the host environment through the extension
manager. Bootstrap host PostgreSQL first. Operator commands use an existing host
schema and the same independent, locked Fleet migration chain; they do not create or
migrate host tables. Read database credentials from a private owned JSON file:

```json
{
  "database_url": "postgresql+asyncpg://fleet-operator@127.0.0.1:5432/deerflow",
  "schema": "deerflow",
  "fleet": {"enabled": true, "jobs_enabled": false}
}
```

Match the actual host database/schema; omit schema only when the host uses its default
search path. Store this file with mode 0600 outside NAS, and use a trusted database
operator identity. Do not put database secrets on command lines. The separate operator
config does not reopen Gateway job admission.

```bash
python -m deerflow_ecs_fleet.operator --settings /private/operator.json register --node-id worker-a --name worker-a --cpu-millis 2000 --memory-mib 4096
python -m deerflow_ecs_fleet.operator --settings /private/operator.json issue --node-id worker-a --credential-file /private/worker-a-credential --lifetime-seconds 86400
python -m deerflow_ecs_fleet.operator --settings /private/operator.json status --node-id worker-a
python -m deerflow_ecs_fleet.operator --settings /private/operator.json drain --node-id worker-a
python -m deerflow_ecs_fleet.operator --settings /private/operator.json disable --node-id worker-a
python -m deerflow_ecs_fleet.operator --settings /private/operator.json revoke --credential-id CREDENTIAL_ID
python -m deerflow_ecs_fleet.operator --settings /private/operator.json enable --node-id worker-a
```

Registration rejects duplicate identities instead of changing an active node's budget.
Issue writes an exclusive private file, fsyncs it, and prints only the credential ID;
it does not overwrite an existing credential or print its value. Preserve the returned
ID for later revocation. Each node needs its own credential and persistent state. Re-enable a drained/disabled
node only after accounting for accepted work; enable does not requeue unknown execution.
Status locates execution history and unreleased reservations without printing tokens,
launch arguments or NAS prefixes. Disabled records remain queryable.

## Admission and scheduling

Configure profiles explicitly with immutable image digests and bounded resources.
Keep agents_enabled and continuations_enabled false. New job admission requires both
Fleet readiness and persistent host long-task tracking; disabling new admission does
not authorize deleting accepted work.

Scheduled Fleet jobs require `reuse_thread`. Configure `scheduled_job_slots` as named
slot -> approved job profile; the model chooses a declared slot. The server derives
schedule and invocation identity. Unfinished, uncertain and cancellation-pending work
keeps the slot. A genuinely new occurrence after terminal completion may start a new
cycle, while replay of a previous invocation remains bound to its original job by a
persistent receipt. Agent occurrence success means submission-run success, not job
completion.

## Drain and disable

1. Close new job admission (`jobs_enabled: false`) and retain the enabled/ready Fleet
   service, accepted-work node routes, task polling and reconciliation.
2. Mark the relevant node draining. Drain prevents new claims and does not stop or
   release an existing attempt.
3. Wait for actual stop acknowledgements and accepted completion where applicable.
   Inspect unresolved attempts and unreleased reservations. A cancellation request,
   elapsed lease, missing worker or successful Agent submission run is not stop proof.
4. For stopped unknown work, use the session-administrator reconciliation procedure in
   [the recovery guide](../ecs-fleet-recovery.md). It records a reviewed failure and
   retains history; it never fabricates success or requeues possible side effects.
5. Disable a node only after charged capacity is zero. Preserve its journal and database
   history. Globally unload/disable Fleet only after accepted work is accounted for.

A forced Gateway/worker outage may leave unknown execution and quarantined capacity.
Re-enable the ready service for reconciliation; do not replace its history with a fresh
node or delete rows to make capacity appear free.

## Upgrade and rollback

Drain first, prove physical stop, then back up host/Fleet PostgreSQL records, immutable
NAS artifacts and private worker journals. Install trusted extension artifacts through
the extension manager and restart the Gateway with Fleet enabled but new admission
closed. Host persistence bootstraps first; Fleet applies its independent migration chain
under a database advisory lock. Fleet tables do not belong to the host ORM metadata.

The current private migration chain includes f0001_jobs, f0002_launch_spec,
f0003_inputs, f0004_recovery and f0005_job_invocations. The last migration backfills
canonical invocation receipts, preserving old job identities through replay.
Downgrades are deliberately refused: rolling back a binary is not permission to drop
accepted jobs, receipts, manifests or quarantine records. Restore a consistent tested
backup only after draining and accounting for accepted work; retain newer histories for
operator review. Keep admission closed on schema/protocol/NAS compatibility failure.

Start upgraded workers with retained private state. Bootstrap stops managed residual
containers, replays durable acknowledgements and proves readiness before any new claim.
An orphan execution or unresolved completion fails closed. Do not bypass the restart
barrier. Verify protocol compatibility and healthy nodes before reopening admission.

## Verification scope

The implementation evidence records exact commands, passed/skipped counts, migration
checks and local real-process tests. Required PostgreSQL/container tests must execute;
a skip cannot pass B12. Production deployment requires operator-provided digest-pinned
artifacts, NAS identity, credentials and confirmed prerequisites. No production ECS
resources are provisioned by this guide's local tests.


Concrete worker JSON, pinned artifact layout, hash-lock preparation, offline build and
Compose environment/launch commands are in [the worker guide](../../docker/fleet/README.md).
The local acceptance path uses two actual host CLI workers with real TCP/PG/NAS/Docker;
the built Linux image additionally verifies its actual entry and Docker control. Rendered
Compose mounts and zero ports are checked, but full containerized Compose daemon
execution and production ECS/NAS deployment are not claimed by those local tests.
