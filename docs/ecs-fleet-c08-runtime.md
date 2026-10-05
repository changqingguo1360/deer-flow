# C08 workspace and checkpoint runtime contracts

C08 has isolated local acceptance for workspace/checkpoint pairing, owner reads
and original installed full/delta initial, new-turn and branch execution. The
[acceptance report](ecs-fleet-c08-acceptance.md) distinguishes current affected-path
verification from retained historical full regressions. C09–C12, B/C continuations
and remote activation remain incomplete.
The [implementation evidence](superpowers/plans/2026-10-01-ecs-fleet-implementation-progress.md)
and [detailed plan](superpowers/plans/2026-10-03-ecs-fleet-c08-workspace-checkpoint.md)
record the exact accepted scope. Native preparation is not proof of a complete
installed Runner turn or new Node-session reconciliation.

## Accepted workspace points

A point binds an owner, thread, actual root checkpoint, original execution and
verified immutable NAS manifest. The Node seals outside the Runner's writable
workspace after original supervised writers settle. A prepared candidate grants
no file publication or writer reopening. Acceptance pairs the checkpoint and
manifest on the original fenced SQL transaction.

Stage files have `partial=true`; only an accepted final point has `partial=false`.
An old stage view remains partial after a newer final point is accepted. Paused
points preserve the original interrupt and pending-write identities. A core
terminal row alone is not an accepted workspace or physical-stop proof.

Finishing retains thread and capacity ownership until the authenticated original
process is physically stopped. The desired terminal outcome is immutable; a
transport stop reason cannot replace it. Final cleanup uses one cumulative
120-second deadline. Partial publication does not start or reset that deadline.

## Owner file reads

The original owner-scoped endpoint
`GET /api/threads/{thread_id}/artifacts/{path}` selects accepted C output for
`mnt/user-data/outputs/...`. `workspace_point_id` pins an immutable historical
view. `GET /api/threads/{thread_id}/fleet/manifests/{manifest_id}` returns accepted
C metadata with manifest, checkpoint, point, source-thread provenance and partial
classification. B job manifests retain their separate ownership and download
protocol.

The descriptor verifier checks original identity, the full manifest inventory and
content digests. The selected file is copied from a verified descriptor into a
private spool; the response holds those bytes instead of reopening a NAS path.
This prevents later path replacement from changing an already verified response.
Range, MIME, active-content attachment handling and bounded `.skill` member reads
retain the original file API contracts. Cancellation and exceptional exits must
close every held file. A missing C service or invalid accepted descriptor must not
fall back to Local files or a mutable attempt directory.

## Durable thread routing and admission

The host migration `0018_thread_execution_bindings`, following
`0017_personal_access_tokens`, owns neutral `thread_execution_bindings` rows. The
row stores owner/thread/backend, trusted parent identity, `source_workspace` and
`recovery_required`; it is not a Fleet-private table. Harness code invokes a
trusted application guard without importing app or the optional Fleet package.

Admission serializes the owner/thread on the original SQL transaction. Legacy
routing requires server-written run history and verified owner/checkpoint branch
lineage. Client thread metadata cannot assign or clear authoritative routing or
branch identity. Missing, conflicting or cyclic ancestry fails closed.

Queued, finishing and recovery tasks remain exclusive even when the core run is
terminal or its lease has expired. Local execution cannot take over a persisted
Fleet thread. Healthy terminal host checkpoint/branch operations retain their
original API; immutable accepted C output is not writable through the Local API.
Reads and exact original-run idempotency remain available during recovery.

A `store_only` cache record does not own an executor. Its admission must use the
trusted transactional store rather than stale Gateway pending/running status.
An actual Local executor retains the same-worker guard; durable active and
recovery states still reject new work.

## New attempts and branches

A new C launch freezes the original checkpoint selector and exact accepted source.
An implicit/latest selector remains implicit in the launch config; its workspace
source is separately bound to the accepted point and matching private checkpoint.
The start grant reauthenticates after file verification with fresh ownership,
lease and database time checks. Missing or changed source content persists fenced
recovery before any Runner start authorization.

Preparation clones `workspace`, `uploads` and `outputs` into new owned user-data,
with independent files and no hardlinks or aliases to Local/B workspaces. Every
retry revalidates the full original source while preserving legitimate runtime
changes in an already prepared destination.

A host branch persists server-owned child routing and exact source provenance
before copying. Failed preparation retains recovery and cleans the new destination.
Before the first child publication, reads may use only the child's trusted source
mapping. Once the child publishes, its own accepted point becomes the default;
explicit historical source views keep their immutable original classification.
Nested branches must prove each trusted target-to-source checkpoint mapping and
retain owner identity without fabricating child accepted points.

Cancelled branch preparation retains ownership of its copy, destination cleanup
and original host reservation release until they settle, including repeated
cancellation. Recovery stays durable; cancellation cannot leave detached cleanup
or interrupt reservation release and damage subsequent use of the host database
pool. These paths require separate actual HTTP, SQL and physical cleanup proof.

Task 5 accepted native HTTP/SQL/file tests cover these contracts.
Task 6 must additionally prove actual installed new-turn/branch materialization,
current-run private checkpoint stamps and legal new input execution. C09 owns
new Node-session reconciliation and cancellation/recovery approval flows; neither
native cloning nor the same-session Task 4 matrix establishes those capabilities.
