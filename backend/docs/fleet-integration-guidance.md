## Optional ECS Fleet (locally accepted; flags off)

`packages/ecs-fleet` owns the optional `deerflow_ecs_fleet` extension and its private
`fleet_` tables/locked migration chain. Never register these models on host Base or
import app/the optional package from harness. Host-only worker POST routes require
node bearer authentication even when user authentication is disabled; management
retains session/admin/CSRF checks. Closing new-job admission must preserve accepted
work, durable tracking and reconciliation. Physical stop proof, rather than a lease
or cancellation request, releases charged capacity; unknown work never auto-retries.

Read [Fleet development contracts](../../docs/ecs-fleet-development.md) before changing
Fleet, its host bridge, worker, input/artifact or recovery paths. This guide owns the
transaction order, trusted identity, filesystem and result-acceptance details.
Use random-schema `tests/fleet` with an isolated TEST_POSTGRES_URI and explicit local
Docker opt-in. Required integration skips cannot pass the release gate. Consult
[deployment](../../docs/deployment/ecs-fleet.md) and the
[delivery roadmap](../../docs/superpowers/plans/2026-10-01-ecs-fleet-roadmap.md) for actual
verified scope. C01–C03 bind immutable task/placement launch identity, join core
admission and Fleet participation in the caller's SQL transaction, fence run/attempt
leases and shared capacity, and exclude remote runs from Local recovery. Default
Local behavior remains compatible; remote admission creates no local task.
Explicit Agent node profiles require positive agent_limit; defaults remain job-only.
C04: installed run_agent, private scopes, snapshots and container lifecycle; start
rechecks locked DB time; PG/Redis credentials precede MCP validation.
C05 fences AsyncPostgresSaver 3.1.1 mutations/sync aliases in the writer ownership
transaction. Remote schema checks are read-only; Local still migrates. Cache/delta
use CheckpointStateAccessor; duration/title (including late cancel) precede terminal writes.
C06 fences supported durable writes and retains original graph/callback cleanup under
one 120-second budget; see [acceptance](../../docs/ecs-fleet-c06-acceptance.md) and module guides.
C08 local acceptance (0018): [contracts](../../docs/ecs-fleet-c08-runtime.md).
C09 STOP/cancellation: [contracts](../../docs/ecs-fleet-c09-runtime.md).
C10 routing/queued admission uses original UoW callbacks and shared reservations:
[C10](../../docs/ecs-fleet-c10-runtime.md); [C11 summaries/drain](../../docs/ecs-fleet-c11-runtime.md).
BC09 owned UI/IM: [projection](../../docs/ecs-fleet-bc09-runtime.md). C12/BC01–BC10 locally accepted; see the delivery record for preserved verification scopes. Flags off.

Fleet history reads must remain observational: optional `_persist_run_history_metadata_background` uses the original atomic non-superseding thread reservation and skips remote bindings. Do not route this cache through the human checkpoint participant, which can cancel awaited jobs. Intentional checkpoint writes retain `reserve_checkpoint_write`. The installed combined main and concentrated fault now pass; final BC10 SPEC/QUALITY also pass at the local scope ([three-stage delivery](../../docs/ecs-fleet-delivery.md)) ([current evidence](../../docs/ecs-fleet-bc10-acceptance.md)). `FleetTaskRecovery.reconcile` must preserve canonical terminal tasks (succeeded/failed/cancelled/timed_out) under its locked task read: stale historical workspace pairs cannot replace a terminal winner or rewrite its budget reason.
