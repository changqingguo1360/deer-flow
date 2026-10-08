> Historical slice acceptance: preserve original cases, counts, source/image identities and scope below. Current B→C→combined status is in [the delivery record](ecs-fleet-delivery.md); later completion does not turn this record into a fresh current full gate.

# C10 local acceptance — 2026-10-06

C10 authorized routing and Scheduler capacity tickets have passed local acceptance. B and C01–C09 remain accepted; C11, C12 and C→B→C remain incomplete. Public remote Agent startup remains closed. This acceptance covers native mounted ASGI HTTP, PostgreSQL and original Node claim, not a new Docker Agent execution or production activation.

## Delivered behavior

Typed local/remote/auto preference resolves through authenticated owner and frozen operator profile bindings. Client internal hints cannot choose ownership, leases or execution backend. Initial owner inputs use bounded no-follow capture and the existing immutable workspace manifest.

Scheduler reserves capacity before run admission, using the existing reservation ledger without fabricating an Attempt. No capacity leaves an occurrence queued with zero executing-budget use. Concurrent Scheduler admission creates one original run; actual Node claim transfers the same reservation to its real Attempt. Ticket recovery reuses the original run and inputs, retires proven never-started work, and preserves assigned capacity until physical STOP.

## Observed evidence

- Initial legal route RED: HTTP422 for missing execution field. Final participating-source main:1 passed/1.96s. Four C10 cases completed PASSED in the later interrupted backend run, covering main, authorization/cancellation, recovery and bounded capture.
- Actual PostgreSQL observations: forged_owner_accepted=false; queued_budget_usage=0; one admission-key run; one reservation with identical ID before/after claim; ticket_leaks_after_reconcile=0; identical original/retried run IDs; assigned reservation still reserved and stopped_at null.
- Claim/cancel race RED409 then GREEN202 with durable interrupt intent and held capacity. Capture RED consumed8 names at limit2/leaked descriptors, then GREEN consumed3 with zero leaks and no staging residue. Intermediate failures remain preserved at their original scope.
- Whole SPEC→QUALITY Ready; narrow closeout SPEC→QUALITY Ready. Root independently verified all37 final source/test hashes. Only production change after the final main was downgrade diagnostic wording; no main or image repeat was needed.
- User reduced testing scope. Whole-backend run is INTERRUPTED:2756 observed passes and6 failures, never a full-suite PASS. Exact failure rechecks:5 passed/41.45s and MCP1 passed/2.78s. Migration preservation assertions retain all original column bytes and check new nullable defaults; MCP fixture synchronization preserves actual owner/publication/resource barriers.
- Blocking-I/O75 passed/6.69s, exit0. Full-backend Ruff/format passed before the narrow delta; final37 affected files Ruff/format and diffcheck pass. Strict OpenSpec3 passed/0 failed.

Logs, natural-exit receipts, hashes, reviews and Root audits are retained under `.local/fleet-evidence/c10/`, especially `capture-final-v1`, `stage-interrupted-v1` and `targeted-closeout-v1`. Owned C10 schemas were absent after tests; test/MCP processes exited. A global inventory includes schema994 already recorded in earlier C08 evidence; it was preserved, with no claim of global schema absence.

## Remaining activation boundary

Never-assigned cancellation proves DB retirement and GET status, not C07 terminal SSE END: the current C07 identity requires an Attempt. Resolve this before public remote activation in C11/C12. Full Runner release and C→B→C acceptance remain separate requirements.
