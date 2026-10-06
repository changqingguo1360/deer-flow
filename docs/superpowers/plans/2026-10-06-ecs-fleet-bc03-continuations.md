# BC03 exactly-one durable continuation admission

Execute with Superpowers subagent-driven-development: sole source/test implementer, Root owns docs/OpenSpec/evidence/commits, fresh SPEC then QUALITY. User requires one main first, then at most one directly necessary concentrated boundary; no full test matrix or suite.

## Prerequisite/current source

BC02 accepted source `c08b8014801c762c3d2a9157e90ca1462a6a35f3`, receipt `05189df0`. Its native owning AgentRunner process established exact checkpoint/workspace/group proof and real original STOP barrier. Installed full C→B→C remains BC10; BC04–BC10 remain required.

WaitGroups.readiness is an observation only. Coordinator admission must revalidate its requirements under original shared SQL locks, plus child result/physical stop and thread/deadline/budget authority. AgentTaskRow retains current_run_id/generation/wait_group_id/continuation_budget. Immutable wait membership and accepted parent point remain historical truth after advancing current_run.

FleetRunAdmission already has trusted same-existing-task paused/input_required resume, original attempt/accepted point source checks, prepared workspace reference and core RunAdmissionUnitOfWork participation. Extend this actual host path for a trusted wait-group continuation; ordinary initial admission must not adopt arbitrary waiting tasks, and client metadata cannot choose a task/group/backend.

Current-source lock audit: ordinary RunRepository admission acquires the thread advisory/binding lock before the app guard locks AgentTaskRow; FleetRunAdmission has no before_thread_lock hook. A new task/group-first coordinator would reverse that order against human/Local guard paths. Resolve this through the existing neutral pre-thread hook (including relevant ordinary and rejected admission paths), preserve scheduled occurrence-before-task order, and document actual shared lock entry. Do not import app into the harness or add an independent check/create transaction.

Actual resumed-publication audit: FleetWorkspaceTerminalParticipant._is_stopped_resume_source currently permits only a paused prior point at generation-1. A BC03 yielded final source keeps the same generation, so admission alone would leave subsequent partial/final publication blocked. Extend only the exact persisted continuation receipt/source-point path with the existing old checkpoint/launch/STOP/reservation checks; preserve paused behavior and never authorize an arbitrary final point.

## Required behavior

1. Exactly one new run/placement/launch spec per stable group/key, same original task/generation; old success run stays terminal. No child submission or computation replay.
2. All legitimate child results settle with their real accepted manifest/stopped proof where required; cancelled staged jobs have no fictitious execution attempt. Parent exact checkpoint/workspace, physical STOP and reservation release are prerequisites.
3. One admission transaction locks original task/group then core thread/run, compares generation/current run/cancel/deadline/remaining budget and persists run/placement/source selector/current run/group receipt atomically. Never check readiness and separately create a run.
4. Concurrent coordinators and retry/restart after committed admission/lost reply recover the original admission. No process-local dedupe or shadow runtime.
5. New runner can schedule elsewhere, loads accepted original checkpoint/workspace, receives bounded child results as untrusted data and consumes them without resubmitting B. Keep host injection and harness/app boundary.
6. Wire the actual service lifecycle and original admission path; a repository helper invoked only from tests is insufficient. No operator activation is authorized.

## Execution

- [x] Audit current trusted existing-task admission/source preparation, UOW idempotency and host service lifecycle; resolve actual lock/wiring choices.
- [x] One real PostgreSQL main: meaningful RED then GREEN, results-before-seal plus concurrent/repeated admission; inspect exactly one new run/placement and unchanged old run/child counts.
- [x] After main GREEN, one concentrated result-after-seal/commit-restart boundary if needed. Preserve those semantic orderings without expanding the old matrix.
- [x] Per-attempt immutable literal argv/cwd/natural exit/preexecution source map, real SQL/NAS/process observations and owned UUID-schema cleanup; private PG URI through environment only.
- [ ] Final changed-file static checks, fresh SPEC→QUALITY, requirement/evidence mapping, capability docs, explicit source commit then OpenSpec3.1–3.4 receipt.

This plan is not execution evidence. Real installed B computation and complete installed C→B→C remain final combination requirements; native slice evidence cannot replace them.

Root inspected genuine main-red-4 naturalpytest1: actual total runs1 instead of required2 after original parent helper returns. Two native child shell processes exited0 and produced report.txt60bytes, accepted original manifests recorded; no host-written substitute result. Prior3 attempts are fixture failures. Detailed historical parent SQL/drop receipt was not retained; final GREEN must preserve those observations. Production continuation implementation now starts; BC03 remains unaccepted/unchecked.

Root inspected main-green-1 actual SQL/payload observations: two runs (success/pending), two placements with same original task/generation, budget2→1, stable receipt and two child-generated accepted manifests, valid paired history. Natural exit0 and owned-schema droppedtrue are recorded. This proves native admission only; resumed execution/publication remains the concentrated boundary. Boundary fixture failures and codec tuple thaw failure are retained as separate attempts, with no additional case. BC03 still awaits source freeze and independent SPEC→QUALITY.

Final qualification: qualified-main1passed7.30s and qualified-boundary1passed7.02s, bothnatural0 and independentowned-schema droppedtrue. All12changedPython exactcurrentSHA/Gitblob independently matched freeze and bothpreexecmaps. NewnativeAgentRunner on anothernode restoredoriginalsource, receivedoneframedcontinuationmessage, acceptednewfinalpoint and STOP/released; jobsremain2. SPECReady; QUALITYunderway. Threeoldmigrationhead testexpectations (4strings) updated separately, changed-file static0, runtimefreezeunchanged and no tests rerun for assertion-only edits.

QUALITYv1 found one P1 bounds defect: valid accepted artifact metadata can exceed64KiB and repeatedly block original continuation; auto-collected awaitedchildren can exceedformatter128cap. Sole repairauthor is implementing deterministicglobalbytebudget withwholeacceptedpath entries/explicittruncation and consistentoriginalawaitedadmissionlimit. No newslice or matrix. Requalifyonlyaffected existingmain/boundary againstchangedsource, thenSPEC/QUALITYrecheck; oldsourcequalification remains honestv1evidence.

BoundsrepairV2qualified: main1passed8.30s/boundary1passed11.09s natural0, ownedschemasdropped,14testedsource matchbothpreexecmaps. Onlypostqualificationchange is cachedhelperimportorder forRuffI001; exactreversepatch reconstructstestedhash and13othersunchanged. Currentstatic0, noformat-onlyruntime repeat. RecheckSPEC thenQUALITY againstcurrentmap+precisedelta beforecommit.
