> Historical slice acceptance: preserve original cases, counts, source/image identities and scope below. Current B→C→combined status is in [the delivery record](ecs-fleet-delivery.md); later completion does not turn this record into a fresh current full gate.

# C09 isolated local acceptance — 2026-10-06

C09 implements remote interrupt, rollback, keyed resume and safe physical-stop reconciliation. Public remote activation stays closed until C10–C12 and C→B→C finish.

## Verified behavior

- First-winning cancellation retains the original task generation and charges capacity until physical STOP. Original Runner cleanup has one bounded 120-second budget; ordinary post-cancel durable writes remain forbidden. Remote wait204 requires durable STOP acknowledgement.
- Interrupt and rollback use original execution/checkpoint/workspace participants. Both cancellation/completion commit orders preserve the winner and immutable outcome. Rollback preserves the existing Local checkpoint semantics; it does not promise arbitrary filesystem rollback.
- Keyed resume admits a new run/generation on the same task only after the exact accepted paused checkpoint/workspace pair and old STOP/release. Actual cached side effects remain once; concurrent admission has one winner.
- Actual TCP partition retains thread exclusion and reservation before STOP acknowledgement. Stock Node restart stops residual processes, then reconciles the old private journal using current Node authentication plus original attempt identity. It grants no writer authority. Historical duplicate STOP is read-only and cannot release a newer reservation. Missing exact recovery sources remain recovery_required/unknown without automatic replay.

## Evidence and limits

Tasks1–3 have prior Root acceptance receipts under `.local/fleet-evidence/c09/`: `task1-root-runtime-acceptance-v1.json`, `task2-root-complete-acceptance-v1.json`, and `task3-root-whole-acceptance-v1.json`.

Task4 same-session partition evidence is `task4-partition-main-v5/`. Final stock restart main is `task4-restart-final-v1/`, image `sha256:960f86678b81020b3f850bcf0ab0ae1674a6f9ab62ee92730fb53ae13b5bc444`: one actual main passed, six installed wheels/784 members and thirteen participating source files verified. Necessary native checks passed2 and neighbors passed9. Sequential SPEC and QUALITY reviews found no unresolved production issue. Owned runtime handles and UUID databases settled.

The single broad existing run recorded 14,816 passed, 10 failed, 72 optional skipped and 58 deselected; its original failure receipts remain in `task4-final-offline-targets-v1/`. All ten failures subsequently passed focused rechecks in `task4-failed-targets-v1/`, `task4-failed-targets-v2/`, `task4-rollback-fault-v2/` and `task4-environment-targets-v1/`. These fix existing native fixture composition, explicit fault reachability, process timing and local-only test environment setup. The broad command is not relabelled as passing or claimed rerun. Required C09 actual mains and focused checks have no skips. Blocking-I/O passed75. Backend Ruff passed and 1,547 files are formatted.

The final installed inputs remain unchanged after five test-only repairs, so the valid main was not rebuilt or repeated. Three superseded diagnostic images were removed only after fresh zero-container-reference checks; final and dependency images remain. See [runtime contracts](ecs-fleet-c09-runtime.md) and the [detailed plan](superpowers/plans/2026-10-06-ecs-fleet-c09-control.md).
