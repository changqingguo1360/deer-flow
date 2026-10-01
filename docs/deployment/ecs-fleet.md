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

Install the trusted Fleet package through the existing extension manager in an
isolated/operator-controlled checkout. From backend, use the actual absolute source:

```bash
uv run --frozen --no-group extensions deerflow extensions install /absolute/deerflow/backend/packages/ecs-fleet --required
```

The manager snapshots local source into backend/extensions/sources, records the
extension dependency group and lock, and discovers its installed entry point.
Its default private Fleet config is inert. Before enabling Fleet, preserve/set this
operator-owned plugin record in the active root config.yaml:

```yaml
plugins:
  - name: ecs-fleet
    package: deerflow-ecs-fleet
    use: deerflow_ecs_fleet:install
    enabled: true
    required: true
    table_prefix: fleet_
    config:
      enabled: true
      jobs_enabled: false
      agents_enabled: false
      continuations_enabled: false
      nas_root: /srv/deerflow-data
      nas_identity: fleet-deployment
```

Replace that NAS example with the actual mounted root and matching pre-provisioned
sentinel before enabling Gateway Fleet. Configure approved profiles before opening
job admission; retain accepted work's storage and profiles when admission closes. Keep mcp_tasks.enabled=true and host PostgreSQL tracking available even
when jobs_enabled=false, so accepted work can recover. required:true makes a package
or install/config failure fatal; table_prefix protects existing Fleet tables during
host autogenerate even when the extension is disabled. The manager does not add the
prefix automatically. Rebuild/restart after package or startup configuration changes.
Retain the existing operator config during replacement; do not edit a business
checkout as a test fixture. Bootstrap host PostgreSQL first. Operator commands use an existing host
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
The local acceptance path covers two host CLI workers and two containerized public
workers launched through the committed Compose configuration. On macOS Docker Desktop,
actual verified HTTPS connects these containers to the loopback Gateway; independent
credentials/journals, PG tracking, NAS output and Docker stop evidence are exercised.
Worker containers publish no ports. Linux Compose configuration has not been independently
executed; production ECS/NAS deployment is outside this local acceptance.


## Explicit local B test gate

From the repository root, supply an isolated PostgreSQL fixture database, a running
local Docker engine, the verified built worker content ID and its actual host socket.
Retain the frozen base, CLI hash and artifact directory used for that build; the committed
Compose configuration requires these values even when launching without rebuilding:

```bash
TEST_POSTGRES_URI=<isolated-postgresql+asyncpg-uri> \
FLEET_TEST_CONTAINERS=1 \
FLEET_TEST_WORKER_IMAGE=sha256:<verified-local-image-content-id> \
FLEET_TEST_DOCKER_SOCKET=<absolute-local-docker-socket> \
FLEET_WORKER_BASE=python@sha256:<verified-base-digest> \
FLEET_DOCKER_CLI_SHA256=<verified-cli-sha256> \
FLEET_BUILD_ARTIFACTS=<absolute-existing-artifact-directory> \
backend/.venv/bin/python scripts/fleet_b_gate.py --report /tmp/fleet-b-report.xml
```

Choose a new report path; the runner refuses to overwrite an existing report. It
creates a fresh pytest report, selects the B test modules, clears inherited pytest
filters, and rejects a missing phase/module, empty report, skipped case or failure.
The printed counts come from actual testcase records, not declared XML summary totals.
Default optional integration skips remain available in the ordinary offline suite;
they cannot satisfy this explicit gate.

The B12 TCP proxy cuts both existing and new worker control connections while the
Docker job continues. An isolated HTTP mock container publishes no ports; its request
counter and the job's output counter independently record the business effects.
Physical watchdog stop precedes the database lease deadline. Recovery preserves
unknown execution, replays durable stopped proof and never starts the job again.

The test matrix includes installed Gateway startup and full local Compose daemon acceptance.
Session-admin node management and final full regressions remain outstanding for overall B. The delivery roadmap and implementation evidence
remain authoritative for B completion; C and continuations are still pending.
