# C12 acceptance status

C12 is locally accepted at source commit `8ac8c00a47864b03e84ca90c25a5921df412d3ac`. The production-image main and single concentrated fault case passed on 2026-10-06, then whole SPEC→QUALITY, final provenance review, Root source/evidence audit and static checks completed. The thin gate verified prior receipts; its future execution mode was not run in this slice. C01–C12 local implementation is complete; C→B→C remains incomplete. No production activation or ECS deployment is claimed.

## Current requirement coverage

| Requirement | Authoritative evidence | Status |
|---|---|---|
| Normal startup and authenticated remote admission | main-attempt-06 executed create_app/full lifespan and supported HTTP routes | Proven for isolated main |
| Original production recipe and installed runtime | production-build-01.log, production-build-receipt.json, actual-installed-compatibility.json | Build and732 installed members/current sources verified |
| Original provider/tool loop and Local parity | main-attempt-06/main-observations.json and executed case assertions | Proven for deterministic task |
| Checkpoint/outbox/terminal/artifact/END | Executed case checks real PG checkpoint/outbox and HTTP artifact/SSE; actual success and accepted workspace pair | Main and fault case proven at their stated scopes |
| Physical STOP and resource release | Actual Docker exited0/Pid0, persisted stopped_at, released reservation, settled Node report | Main and actual stock journal restart STOP/release proven |
| Persistent memory and Store fencing | Candidate reuse audits match unchanged source to observed C10 native PASSED cases | 27 participating source inputs currently matched across qualified native proofs; main performs no such writes |
| Redis outage, revoke/late write, restart | fault-attempt-05 actual case and observations | Proven for two owned runs; final review pending |
| Required cases have no skips; fresh release report | Thin gate prior-receipt verification exit0; fresh_execution=false | Recorded prior cases have no skips; future execution mode not run |
| Documentation/static checks/reviews/commit | C12 source commit and final review/static receipts | Locally accepted |

## Main evidence

Evidence directory: `.local/fleet-evidence/c12/main-attempt-06/`. `test.log` records one passed in17.56s (two existing websockets deprecation warnings); `command-receipt.json` records natural exit0. Root inspected the case, result log and actual observations. The image is ec9496 and compatibility3a9a81 as fully identified in [runtime notes](ecs-fleet-c12-runtime.md). Docker exited0, task/placement/run succeeded, STOP was persisted, reservation released, and accepted/final workspace IDs matched. Three remote and three Local provider calls chose bash→present_files→final; downloaded artifacts matched SHA256 b033213a4c522e4a1d2d2ab8aeef793ff2760e175e76bfd5c5853aef9cb045a2. Owned-schema receipt records created/dropped true.

Earlier attempts remain failures, not passing aggregates. Initial genuine RED is the obsolete startup rejection. Attempts02–03 diagnosed bootstrap configuration; attempt02's original container log was not captured. Attempts04–05 failed on incorrect HTTP-model fixture expectations about successful tool output. Those expectations were corrected against original tool contracts without rebuilding the image or adding cases. No broad suite or fault neighbor was run before the main passed.

Prior C10 aggregate remains INTERRUPTED. The memory/Store candidate audits reuse only observed individual native results whose participating sources match the accepted baseline, and require a final freeze check. They do not prove production-image memory/Store writes. Root final current-source matching is recorded in root-requirement-evidence-audit-v1.json. The initial dependency closure failure and original slow-but-successful build session remain separate evidence.


## Qualified delivery evidence reuse

Root inspected eight observed PASSED publisher cases in the original interrupted
C10 log and compared seven participating sources. Six files match byte-for-byte;
app/fleet/events.py differs only in C11's never-assigned END path. The actual
FleetStreamReader candidate_pointers/load_committed method ASTs used by publisher
cases match the baseline. The qualified audit is retained as
`.local/fleet-evidence/c12/candidate-redis-reuse-audit-v1.json`. This preserves
native delivery-retry evidence without claiming the whole events module unchanged
or proving the current production-image outage/revocation/restart scenario.

The planned single concentrated fault case uses two separately owned runs: one
can finish normally while Redis delivery is interrupted/recovered; another loses
authority during real execution and exercises journal restart/residual STOP.
A revoked run without a valid seal is not forced to emit normal END. Evidence must
identify which run and process proves each invariant. An auxiliary installed-saver
probe, if needed, proves rejection for the original stale identity but is not
represented as a write attempted by the original running Agent process.


## First concentrated fault attempt

`fault-attempt-01/test.log` records natural failure1 in42.40s. Root inspected
`delivery-recovery.json`: the delivery run succeeded, STOP persisted, reservation
released, accepted/final workspace pointers matched,20 events were committed
before Redis cutoff and17 remained unpublished during outage. Recovery observed
25 ordered unique hint IDs and the executed path had durable SSE END. Exact
comparison of delivered IDs with the committed SQL sequence set will strengthen
the corrected case; unique/sorted hints alone are not an all-events proof.

The second run entered the original provider/tool loop but its fixture rejected
a third tool result under the original two-tool budget; stock Node exited1 before
revocation. Thus no late-write/restart claim is made for this attempt. The same
concentrated case is being corrected with actual tool-result diagnostics and
cleanup that settles every owned process/container despite individual errors.
No new case, image or test matrix was introduced.


Second fault attempt remains failed: `fault-attempt-02/test.log` records1failed
in70.41s at the60-second delivery_waiting timeout, before Redis cutoff or the
revocation run. The earlier delivery proof is not relabeled as this attempt's
result. Only the initial provider call was recorded; the original removed
container's missing log cannot be reconstructed. The same case needs stop-before
log capture and observation of the execution task's early terminal result/error
while waiting for the model barrier, rather than assuming a missing signal means
the runner is still running. No runtime protocol, production image or extra case
is changed for this diagnostic correction.


Third fault attempt (`fault-attempt-03/command-receipt.json`) naturally exited1.
Root inspected the actual PostgreSQL AmbiguousColumnError: the new completeness
query joined run_events and fleet_event_outbox but selected/ordered unqualified
seq. Redis IDs originate from the committed outbox pointer; qualifying that
column fixes the assertion query without weakening the ID-set comparison or
changing runtime code. This attempt is not a fault acceptance PASS. Its original
runner log and command receipt are retained; the same concentrated case continues.


Fourth fault attempt naturally failed1 in39.21s. Root inspected the executed
assertion and original `fault-attempt-04/revoked-runner.log`. The actual running
Agent reports lost lease ownership, a rejected checkpoint journal flush and
skipped durable finalization because ownership was lost. This directly proves
a late checkpoint rejection by the original process; the independent installed
saver probe remains separately scoped. Run1's SQL sequence completeness assertion
also executed successfully after qualifying o.seq.

The failure is an overbroad whole-row fingerprint: outbox retained50 rows but
its hash changed while the original publisher could legitimately update
published_at; the other seven compared domains matched. The revised observation
must compare all immutable outbox fields and record published_at separately,
without omitting event identity/payload references or weakening row-count checks.
Stock journal restart and residual physical STOP/release remain unproven in this
attempt. It is still a failed concentrated case, not C12 acceptance.


## Concentrated fault GREEN

`fault-attempt-05/test.log` records1passed in40.21s, naturalexit0; the owned
schema receipt records created/dropped true. Root inspected actual delivery and
revocation observations and independently compared the installed audit with the
frozen wheels.732 Python members, copied bootstrap/collector and three approved
JSON assets match. The participating runtime/deps/recipe sources also match the
original main execution hashes. Receipt: root-c12-technical-audit-v1.json.

Delivery:20 events committed before cutoff;16 remained unpublished during outage.
The durable successful terminal/tail survived, then the original publisher delivered
25 IDs that the executed assertion compared against the committed SQL sequence set.
Revocation: the original stock Node was killed(-9); authorized credential/session
rotation retained reserved resources, stopped_at null and the accepted workspace
pointer. The original running process logged rejected checkpoint flush and skipped
finalization. The separately scoped installed saver also rejected its original stale
identity; all eight compared immutable mutation domains matched. Only publisher-owned
published_at was excluded from the outbox fingerprint and was recorded separately.

Before restart the actual container was Running=true and the long command's
supervisor/shell registration remained present. Stock Node CLI used the same durable
journal, stopped that container(exit137/Pid0), acknowledged STOP and released the
compute reservation. Task/placement became recovery_required; accepted workspace
was unchanged, final workspace remained null, and original run status remained
running. This does not manufacture END or automatic success. Historical registered
process rows remain historical DB records; actual namespace stop proves physical
quiescence, not a claim that those rows were rewritten to settled. The gate and
independent release reviews still precede C acceptance and C→B→C implementation.


## Thin gate and qualified inherited proofs

`scripts/fleet_c_gate.py --verify-receipts .local/fleet-evidence/c12` ran
naturallyexit0, saved as thin-c-gate-prior-receipts.json and its command receipt.
It verifies the two recorded natural successes, owned cleanup, executed source
provenance, original image build and actual installed bytes/assets. The result
explicitly says fresh_execution=false and does not execute pytest or native
fencing cases. Future default mode preflights the same explicit PG URI used by
the fixture, Redis binary, Docker and image, then executes only the two exact
cases into separate evidence directories and real per-case JUnit reports. The
report manifest does not fabricate a combined XML. Default future execution
mode has not been run in this slice; independent source review remains required.

Root revalidated20 memory/Store/Redis participating source hashes and7 C11
assigned-cancel source checks, all matching their qualified original evidence.
The audit is root-qualified-native-reuse-audit-v1.json. C11 proves original
authenticated assigned cancel, held resources before STOP and native reconciliation,
not a container run; C12 proves actual installed process/restart STOP separately.
C10's original aggregate remains INTERRUPTED and only its observed individual
PASSED cases are reused. Neither inherited proof nor the thin gate claims this
stateless production main performed persistent memory or Store writes.

## Gate artifact binding correction

The first whole SPEC review identified a missing frozen-manifest binding in prior-receipt verification. The corrected gate verifies the manifest SHA against the original build receipt, all11 listed artifact hashes, the closure wheel set, original recipe and pinned base identity, and the recorded offline build arguments. Root independently ran only this receipt verification (`root-gate-spec-fix-verification-v1.json`), with natural exit0 and fresh_execution=false. Neither runtime case nor the image build was rerun for this correction. The original review and earlier gate receipts remain preserved.

## Quality review cleanup correction

Whole QUALITY is Ready after one P2 correction: main finally now submits log collection to the existing aggregate cleanup helper, so discovery/log errors cannot skip STOP, execution/writer settlement or container removal. Original executed test bytes and hashes remain in the prior records; the reviewed cleanup was not rerun through the runtime. Gate qualification binds both hashes and compares the complete module AST after excluding only the main cleanup finally. Every other source change still rejects reuse. Root receipt-only verification naturally exited0 (`root-gate-post-review-verification-v1.json`, fresh_execution=false). The report does not attribute the reviewed test hash to the earlier main/fault executions.

Accepted source commit: `8ac8c00a47864b03e84ca90c25a5921df412d3ac`; all seven reviewed technical files match their committed blobs. This documentation receipt completes OpenSpec12.1–12.4 after the actual source commit. C→B→C BC01–BC10 remain required; no push, merge or real configuration activation occurred.
