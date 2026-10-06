# Current BC08 local acceptance

Source `d08f776375cd70776d0d0feda6a820a7eca65755` matches all14 reviewed Python blobs; fresh independent SPEC→QUALITY Ready. Main9.54s carries unchanged original Fleet semantics; corrected existing boundary13.61s naturally exited0 with both owned schemas dropped and actual Local fast completion. [Acceptance](../../ecs-fleet-bc08-acceptance.md) owns the precise native/source-only qualification. BC09–BC10 remain required. Historical execution plan and correction records follow.

# BC08 scheduled aggregate goals implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Existing user authorization covers implementation/local verification; Root owns documentation, independent SPEC→QUALITY and commits. The sole author owns source and every test handle. Execute one actual main first, then at most one concentrated necessary boundary; do not grow a matrix.

**Goal:** Keep a later scheduled occurrence durably queued while its prior aggregate Agent goal is unresolved, without execution-budget/ticket charges or false named-child deduplication.

**Architecture:** Reuse original scheduled ticket→placement→AgentTask association. An immutable private occurrence resolution receipt is created in original scheduled admission and resolved by original authenticated STOP/release, or exact original never-assigned retirement. Queue claim/fairness read unresolved receipts without acquiring AgentTask locks under scheduler parent locks. Initial core run completion remains occurrence-terminal; once-parent completion waits for the actual aggregate resolution.

**Tech Stack:** Existing Scheduler service/repositories, FastAPI authenticated launch, Fleet SQLAlchemy/Postgres/private migration, original native Node/executor/workspace publication and real child processes.

Baseline: BC07 source `097b789a39810be58ab0e894780b61221ba26421`, acceptance docs `6caba32d`. Independent SPEC and fresh QUALITY Ready;22 source blobs verified. Feature checkout `/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs`. OpenSpec `add-ecs-agent-job-continuations` / `Scheduled aggregate tasks preserve durable queue semantics`. BC09–BC10 remain required.

## Verified current seams

- `backend/app/fleet/scheduler_tickets.py`: `TicketAdmission.insert` first invokes real inner admission, then `consume` under original parent/occurrence locks. `reserve` is invoked before original queued→launching/ticket/physical budget updates. It must check unresolved goals before `approved_binding` can return None for adjacent Local selection.
- `backend/app/scheduler/service.py`: `handle_run_completion` currently maps any successful core run to successful occurrence and completed once-parent. `run_once` invokes original ticket reconciliation before draining queue. Preserve initial occurrence identity and original queued creation time.
- `backend/packages/harness/deerflow/persistence/scheduled_completion.py`: original capability/run metadata and exact persisted parent/occurrence/run association remain mandatory. Never pretend a continuation is the initial occurrence run.
- `backend/packages/harness/deerflow/persistence/scheduled_task_runs/sql.py`: `_associate_task_with_run` also projects once terminal status during restart repair. `backend/packages/harness/deerflow/persistence/scheduled_tasks/sql.py` contains once restart recovery. Both need neutral optional aggregate predicates; harness must not import Fleet.
- `backend/app/fleet/runner_context.py`: constructs real remote completion Scheduler service/repositories. Optional callbacks must be wired here as well as Gateway scheduler composition.
- `backend/app/fleet/ownership.py::_stopped_locked`: original authenticated task/run/placement/node/reservation/attempt locks, accepted final point, desired task state and `release_stopped` share one transaction. Add resolution after legitimate release/flush in this transaction, acquiring no scheduler parent/occurrence locks.
- `backend/app/fleet/admission.py::agent_candidates`: exclude blocked scheduler occurrences before fair candidate accounting; blocked work must not win projected priority.
- Existing builtin `fleet_jobs.py` and `mcp_driver.py` already validate operator `job_slot` and use SHA256(schedule identity + NUL + slot). Reuse this contract. Trusted schedule context is server-injected; no new dedupe mechanism is required unless the actual main exposes a defect.

## File ownership and interfaces

Sole author production files:

- Create `backend/app/fleet/scheduled_agent_tasks.py`: `FleetScheduledAgentTasks` original association, nonlocking blocker projection and trusted parent reconciliation.
- Create `backend/packages/ecs-fleet/deerflow_ecs_fleet/migrations/versions/f0017_scheduled_agent_tasks.py`; modify `persistence/models.py`: private immutable association/resolution receipt and database constraints.
- Modify `backend/app/fleet/scheduler_tickets.py`, `ownership.py`, `admission.py`, `runner_context.py`, `backend/app/scheduler/service.py` at the seams above.
- Modify neutral `backend/packages/harness/deerflow/persistence/scheduled_task_runs/sql.py` and `scheduled_tasks/sql.py` for optional predicate injection. Preserve existing capability checks in `scheduled_completion.py`; modify it only if an actual necessary omission is identified and reviewed.
- Inspect `backend/app/fleet/scheduled_jobs.py` and existing builtin/driver context propagation; modify only a reproduced needed defect.
- Create one module `backend/tests/fleet/test_bc08_fleet_agent_job_continuations.py` with main selector below and, after GREEN, sole concentrated boundary selector.

Service contracts defined for this slice:

```python
associate(session, *, task, occurrence, admitted, agent_task_id)
blocks(session, *, schedule_id, exclude_occurrence_id=None) -> bool
resolve_stopped(session, *, task, run, placement, attempt, point, reservation, now)
resolve_unassigned(session, *, task, run, placement, occurrence, tickets, now)
suppress_parent_completion(session, *, task_id, occurrence_id, run_id) -> bool
reconcile_once() -> int
```

`associate` validates the original consumed ticket, scheduled owner/task/occurrence and immutable initial admitted run/AgentTask identity. Persist pending association in the same original admission UoW; validate exact identities on reuse. Receipt stores original schedule, occurrence, user/thread, initial run/task/generation, then nullable immutable resolution kind/current run/generation/point/attempt/session/status/time. Pending→resolved is one-way; settled evidence cannot be rebound or reopened.

`resolve_stopped` uses only the already authenticated locked original objects after physical release. Require current exact accepted terminal checkpoint/workspace point, publication identity, matching actual STOP, reservation release and final aggregate status. `waiting_jobs`, `paused`, unknown/recovery-required and missing accepted pair remain unresolved. Store the original terminal evidence atomically before any subsequent human generation can reopen AgentTask; later human work cannot reopen the already consumed occurrence receipt. Acquire no scheduled parent/occurrence locks here.

`resolve_unassigned` applies only in original `retire_waiting`/`cancel_unassigned` authority: original pending/queued identity, no Attempt ever, actual terminal interruption/cancellation and released original tickets/reservations. Store distinct `unassigned_cancel` proof; do not fabricate a STOP or accepted point. Assigned cancellation retains the original actual STOP/pair authority and must not be inferred from a cancellation request.

`blocks` reads original unresolved associations, including conservative historical ticket associations without a new receipt. Exclude only the exact current occurrence where original reuse is legitimate. It acquires no AgentTask/receipt locks under scheduler parent; STOP writes receipt without acquiring scheduled parent, so there is no new reverse lock edge. Check before Local/remote routing and before tickets, nodes, queue claim and fairness charge.

Optional neutral `aggregate_completion_pending(session, *, task_id, occurrence_id, run_id)` callback accepts those original identity keys. None preserves original Local behavior. Validate original capability/run/parent/occurrence first. Initial core run becomes terminal occurrence to free execution budget; if the association remains unresolved, omit only premature once-parent terminal mapping. Restart association repair/recovery must consult the same callback rather than deriving aggregate completion from initial core success.

`reconcile_once` runs trusted host-side through original scheduler ticket reconciliation before queue drain/startup. Under original parent→occurrence locks, read immutable resolved original receipt and update only its matching once-parent; do not use latest continuation metadata to bypass original completion capability. Use bounded scans/revalidation and preserve existing non-Fleet behavior. Current unresolved goals keep later occurrences queued, coalesced and subject to original queue timeout.

## Execution steps

- [x] **Step1 — Freeze one main fixture.** Author prepares a complete fixture patch/source map/static receipts before Root application review. Define helpers `compose_scheduled_case`, `install_receipt_provider`, `execute_scheduled_initial` and `sql_observation` inside the module using real C10 original scheduler/API and BC02/BC03/BC07 native execution helpers. No shadow runtime or metadata/terminal/STOP writes.

Actual main `test_bc08_original_scheduled_goal_queues_next_occurrence`:

1. Original authenticated HTTP creates recurring reuse-thread schedule; operator config approves slots first/second→batch and existing native compatibility. Original scheduled launch/admission produces initial occurrence/core/spec.
2. Real controlled HTTP provider/native C submits two different approved slots and yields through original accepted pair/STOP. Attach the actual `ScheduledTaskService.handle_run_completion` to the native RunContext as production does; do not write metadata after admission.
3. Observe actual initial run.success and aggregate.waiting_jobs. Obtain persisted `next_run_at`; invoke real `ScheduledTaskService.run_once(now=...)` to trigger the later **scheduled** occurrence. Assert its trigger is scheduled. Concurrent actual manual HTTP triggers must coalesce this same queued row, preserving creation time and attempt_count.
4. Assert no new original run, held ticket, execution row or run-budget charge for waiting work. Named jobs have different original IDs/dedupe groups. Measure all facts from SQL/process/provider receipts.
5. Execute both B children as actual processes; dispatch and execute original continuation C with accepted result/source/STOP/release. Original reconciliation then admits the SAME queued occurrence through original claim/launch, with original age/identity retained.

- [x] **Step2 — Actual RED.** Run only the main selector at untouched BC07 baseline. Fixture/setup/import/connection/skip is not product RED. Repair fixture only before attributing behavior. Preserve raw command, applied source hashes, natural exit, actual SQL/HTTP/processes and owned cleanup. Author alone owns/polls every live handle; no restart due to observation timeout.

```bash
cd backend
.venv/bin/python -m pytest tests/fleet/test_bc08_fleet_agent_job_continuations.py::test_bc08_original_scheduled_goal_queues_next_occurrence -q -o addopts= --tb=short
```

- [x] **Step3 — Implement original path.** Prepare complete logical candidate/source maps including migration and all original callback wiring for Root review before application. Add durable receipt + original STOP/unassigned resolution, nonlocking queue/fairness blocker, once completion/restart hooks and production runner wiring. Retain original capability validation and namespace/schema ownership. No new background executor or alternate queue.
- [x] **Step4 — Main GREEN.** Apply the reviewed full patch and verify every applied fingerprint. Run the same main selector to natural termination. Root checks actual source and raw original schedule→C→B→C→queued-next admission evidence. Do not advance on a narrower mocked green.
- [x] **Step5 — One concentrated necessary boundary.** Only after main GREEN, freeze/review `test_bc08_restart_preserves_queue_deadline_and_resolution`. Recreate original Scheduler managers/repositories while a recurring goal waits and later occurrence is queued; verify identity/age/no-execution-charge persists. Expire via the original supported `now`/queue deadline path, with no timestamp mutation or resetting age. Include one once-schedule native waiting/restart→actual aggregate final STOP sequence to prove parent is not prematurely completed/cancelled by original repair and completes only after legitimate resolution. Reuse that owner for minimal immutable resolution/replay or supported later human resume observation; no extra graph/matrix unless an actual material defect requires it. Source inspect legitimate unassigned retirement resolution; add focused behavior only if required by a concrete failure.

```bash
cd backend
.venv/bin/python -m pytest tests/fleet/test_bc08_fleet_agent_job_continuations.py::test_bc08_restart_preserves_queue_deadline_and_resolution -q -o addopts= --tb=short
```

These node IDs are proposed registrations, not execution claims. Commands require the existing private0600 PG carrier; never print URI. Real process fixtures may reuse accepted task images; do not rebuild or rerun unrelated successful suites. Test owner cleans only owned schemas/processes.

- [x] **Step6 — Freeze/static/docs.** Ruff check/format-check and actual3.12 compile only changed Python files; Root diff whitespace, strict OpenSpec and changed guidance checks. Root synchronizes README/backend AGENTS/current evidence and OpenSpec8 tasks. Raw failures and exact execution/source qualification survive; do not claim installed bootstrap from native helpers.
- [x] **Step7 — Independent SPEC then QUALITY.** Fresh reviewers read current original seams and raw evidence. Fix material findings on smallest affected existing selector. Root verifies reviewed committed blobs and acceptance records; only then check OpenSpec8.1–8.4 and start BC09. No push, merge, activation or ECS deployment.

## Self-review

The original durable queue retains age, coalescing and zero executing-budget semantics. Resolution persists in the original authoritative STOP or verified never-assigned retirement transaction, avoiding process-local state and receipt reopening. Optional original completion/recovery callbacks cover once semantics and preserve Local behavior. Existing approved slot identity covers distinct named jobs. Main covers real due occurrence and completion-triggered admission; one boundary covers restart/deadline/once resolution, without expanding test count. BC09 UI/product summary and BC10 installed combined gate remain outside this slice. Current status: discovery complete, no BC08 source or tests yet.

## Main fixture review

One479-line main fixture was frozen and reviewed at SHA256 `7f213b9e0604d9c68ba69dc94555dd831d9678fceb1fa915c9b706c41e251574`, patch `124141778ce3d8fea3eb5138bb2f93a9bc3efbe61de0b867857d91b209f33dda`. Configured Ruff/check-format and Python3.12 compile exit0. Static review corrected C10 local configuration import, actual operator continuation allowance, reserved-mode eligibility of the existing native continuation worker, and synthetic peer contract version bound to immutable LaunchSpec.model_version. Original code/metadata/STOP protocols remain unchanged. Root authorized exact fixture application and baseline main-only execution; no product RED or production implementation is yet claimed.

## Baseline discovery correction

First native main naturally failed only on missing fixture routing config pointer, not product behavior. Original fair candidate projection already protects active aggregate tasks on the same reused thread; corrected main may therefore be baseline GREEN. Preserve that result honestly. After actual main GREEN, the already planned sole concentrated boundary may establish product RED for once-parent/restart or schedule-wide adjacent Local semantics before implementation. Step2 requires actual missing behavior evidence, not an invented recurring-main failure; main-first and unchanged test-count requirements remain binding.

## Verified baseline main GREEN

Corrected main fixture SHA256 `fd508cdcae25d683ac19671c07105bf4c165b14a7912c306228675bed9672a4e` passed at unchanged production baseline `9a262a82`: pytest8.93s, natural exit0, owned schema dropped. Root checked original initial STOP/release, two real B exits0 and successful original continuation STOP/release against raw process-chain evidence. Same queued identity/age survived and was admitted afterwards. Runtime scope and receipts are recorded in `docs/ecs-fleet-bc08-runtime.md` and `.local/fleet-evidence/bc08/main-baseline-02`.

Step2 remains incomplete pending a genuine product RED from the sole planned boundary. Main-first order is satisfied; author prepares only that boundary, then Root reviews before application/execution. No additional selectors or production changes are authorized yet.

## Verified sole boundary product RED

At unchanged `9a262a82`, reviewed boundary fixture `9cca1b65…` naturally failed at the first once-parent assertion (pytest6.33s, exit1, schema dropped): original parent completed despite aggregate waiting_jobs after original core success/STOP/release. Root inspected actual SQL/raw assertion. Step2 is satisfied by this material missing behavior, after main baseline GREEN. Later boundary segments did not execute and remain unqualified. Root authorized only complete production candidate authoring before application review; no production edits applied yet. OpenSpec8 remains unaccepted.

## Original completion outcome seam

Source review requires one additional neutral optional callback beside pending detection: `aggregate_completion_outcome(session, *, task_id, occurrence_id, run_id)` returns the immutable once-parent terminal status/error, or None. Initial core success and eventual aggregate failure/cancellation can differ. Original completion replay must validate original capability/metadata/parent.last_run_id/occurrence.run_id first, preserve the occurrence's original core outcome, and respect the parent's immutable aggregate outcome rather than overwrite it with initial core success. Callback defaults remain None for Local behavior.

The original host service constructor can install callbacks from existing `execution_admission` on its original repositories before startup/recovery; Gateway already supplies that service. Private runner and native fixture completion constructors need the equivalent explicit injection. No alternative scheduler or metadata rebinding is introduced. Candidate authoring remains in progress; this design note is not an implementation or runtime claim.

## Already-stopped waiting cancellation authority

Original `app/fleet/task_operations.py::cancel_owned_task` already validates the accepted await pair, exact original parent STOP/process/session and released reservation, absence of unresolved parent attempts, and authenticated owner/generation/idempotency operation. It then marks the goal cancelled and completes `TaskOperationReceipt` in that same transaction. No new terminal graph publication or new STOP occurs for this legitimate already-stopped waiting goal. Without recording this authority, its scheduled association would remain pending indefinitely.

Add `resolve_cancelled` in that original completed cancellation transaction after its receipt flush; store a distinct `cancelled` resolution kind plus original operation ID, source run/generation, accepted point/checkpoint and actual stopped attempt/session/release evidence. Preserve the immutable latch and acquire no schedule locks. Requested cancellation and `stop_only` human handoff are not terminal authority. This is additional authentic evidence, not a fabricated final point or STOP.

The existing concentrated boundary recurring owner can observe the same waiting goal's authenticated cancellation after its queue timeout and trusted reconciliation. Reuse that owner and selector; no extra executor or case matrix. Source/fixture candidate remains subject to complete Root review before application.

## Historical proof and bounded reconciliation

Reconciliation must select actionable receipts before a bounded LIMIT (or advance its scan); persistent resolved prefixes cannot starve later schedules. Historical association repair uses the exact original ticket/run/placement identity. Historical completion must follow immutable dispatched WaitGroup parent-to-continuation edges in the original owner/task/generation and validate original accepted source/checkpoint, continuation LaunchSpec source pair, then the first terminal accepted pair with actual physical STOP/released ledger. The latest `AgentTask.current_run_id` is not historical completion authority.

Current accepted-source callers retain `require_latest_checkpoint=True`. A historical observation-only path may read the exact persisted checkpoint by ID with original run metadata; it grants no start, resume, publication or tool authority. Missing or unreconstructible lineage remains conservatively unresolved with explicit qualification. Completed authenticated cancellation has its distinct original receipt authority. These source corrections are being authored, not yet applied or runtime-qualified.

## Exact reviewed implementation applied

Freeze02 fourteen-file patch `e7098893…` applied after Root review. Root independently matched every installed source fingerprint against configured static receipts and reviewed manifest. Original main function AST unchanged; only the original private callback helper wiring and same concentrated boundary cancellation observation changed in the fixture. Step3 reflects reviewed source application, not runtime acceptance. Step4 main-only verification is pending; subsequent sole boundary and independent SPEC→QUALITY remain mandatory. Full receipts/sources: `.local/fleet-evidence/bc08/production-freeze-02`.

## Verified post-implementation main

Original main `main-green-02`: one passed9.54s, naturalexit0/ownedcleanup; Root confirmed actual C→two realB→C, originalSTOP/releases, samequeuedidentity/age and zeroexecutioncharge until finalresolution. Import-only `main-green-01` failure and one-line modelconstraintfix remain qualified in runtime notes. Source: freeze02+modelSHA7a9c7c76… . Step4 verified; only existing concentrated boundary next. No expandedtestmatrix or unrelatedsuite rerun.

## Verified sole concentrated boundary

One existing boundary passed13.88s/natural0/twoownedcleanups. Root independently confirmed once waiting/restart/final/replay, recurring Localfresh-thread queued identity/age/zerocharge throughrestart and originaltimeout, plus samewaitingowner actualcompletedcancel operation/immutable resolution. Source-only historical/crossgeneration/failure/neverassigned paths retain explicit qualification; no extra matrix. Fresh SPEC reviewer dispatched on current final14source and actual raw main/boundary. Quality review follows only SPEC Ready.

## Fresh review result and focused correction

Independent SPEC Ready on final-source-01. Independent QUALITY Not Ready: one Important Local fast-completion regression in the host-wide aggregate trusted fence. Original Local worker may complete before launch bookkeeping; its parent run/thread association must retain original semantics. Same sole author prepares the smallest exact correction and an affected-path observation in the existing concentrated boundary. No BC09 implementation, expanded matrix or unchanged main rerun. Root exact candidate review precedes application; fresh SPEC then QUALITY follow the correction.

### Reviewed correction design

Limit host-side trusted aggregate completion to the original persisted `RunRow.kwargs_json.execution_backend == "fleet"` in the same parent writer transaction, in addition to installed callbacks and exact original completion IDs. Private mutation capability still takes the existing strict validation path regardless of backend. Fleet callbacks are recomputed under original locks, preserving pending→resolved races and immutable outcome rebasing. Local keeps the original completion-before-launch-bookkeeping semantics; caller metadata does not grant aggregate authority.

The existing sole boundary reuses its once owner after authentic final/replay: supported authenticated PATCH to Local/fresh-thread, then an actual original HTTP launch with a real attached Local LangGraph and original SQL checkpoint. An observation barrier around the original launch wrapper waits for the real worker completion before returning to scheduler bookkeeping. Core, occurrence, parent and completion writers remain original. Record actual before/after parent identity/status and ownership-lost state. No third owner, subprocess, selector or provider call. Fixture-only exact candidate review precedes RED; production correction exact review/application follows the demonstrated defect.

## Corrected final freeze and re-review

Local-fast-green-01 naturally passed13.61s with actual Local core/occurrence/worker success, no ownership loss and terminal once parent preserved across original bookkeeping; same original once Fleet chain and recurring queue/deadline/cancel segments passed, both owned schemas dropped. Root independently verified those raw records. Final-source-02 freezes all14 actual files after exact reviewed persisted-Fleet gate correction and import-order-only fixture cleanup; actual-path Ruff check/format and Python3.12 compile all0. Initial path-classification I001 and setup-only attempts remain recorded. Main9.54s carries unchanged original main/Fleet semantics; amended boundary runtime fixture and final fixture differ only normalized-AST-equivalent contiguous imports. Source authoring/runtime stopped for fresh independent SPEC→QUALITY re-review. Commit, acceptance and OpenSpec8 completion remain pending.
