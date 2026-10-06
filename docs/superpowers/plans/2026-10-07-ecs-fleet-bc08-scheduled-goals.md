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

- [ ] **Step1 — Freeze one main fixture.** Author prepares a complete fixture patch/source map/static receipts before Root application review. Define helpers `compose_scheduled_case`, `install_receipt_provider`, `execute_scheduled_initial` and `sql_observation` inside the module using real C10 original scheduler/API and BC02/BC03/BC07 native execution helpers. No shadow runtime or metadata/terminal/STOP writes.

Actual main `test_bc08_original_scheduled_goal_queues_next_occurrence`:

1. Original authenticated HTTP creates recurring reuse-thread schedule; operator config approves slots first/second→batch and existing native compatibility. Original scheduled launch/admission produces initial occurrence/core/spec.
2. Real controlled HTTP provider/native C submits two different approved slots and yields through original accepted pair/STOP. Attach the actual `ScheduledTaskService.handle_run_completion` to the native RunContext as production does; do not write metadata after admission.
3. Observe actual initial run.success and aggregate.waiting_jobs. Obtain persisted `next_run_at`; invoke real `ScheduledTaskService.run_once(now=...)` to trigger the later **scheduled** occurrence. Assert its trigger is scheduled. Concurrent actual manual HTTP triggers must coalesce this same queued row, preserving creation time and attempt_count.
4. Assert no new original run, held ticket, execution row or run-budget charge for waiting work. Named jobs have different original IDs/dedupe groups. Measure all facts from SQL/process/provider receipts.
5. Execute both B children as actual processes; dispatch and execute original continuation C with accepted result/source/STOP/release. Original reconciliation then admits the SAME queued occurrence through original claim/launch, with original age/identity retained.

- [ ] **Step2 — Actual RED.** Run only the main selector at untouched BC07 baseline. Fixture/setup/import/connection/skip is not product RED. Repair fixture only before attributing behavior. Preserve raw command, applied source hashes, natural exit, actual SQL/HTTP/processes and owned cleanup. Author alone owns/polls every live handle; no restart due to observation timeout.

```bash
cd backend
.venv/bin/python -m pytest tests/fleet/test_bc08_fleet_agent_job_continuations.py::test_bc08_original_scheduled_goal_queues_next_occurrence -q -o addopts= --tb=short
```

- [ ] **Step3 — Implement original path.** Prepare complete logical candidate/source maps including migration and all original callback wiring for Root review before application. Add durable receipt + original STOP/unassigned resolution, nonlocking queue/fairness blocker, once completion/restart hooks and production runner wiring. Retain original capability validation and namespace/schema ownership. No new background executor or alternate queue.
- [ ] **Step4 — Main GREEN.** Apply the reviewed full patch and verify every applied fingerprint. Run the same main selector to natural termination. Root checks actual source and raw original schedule→C→B→C→queued-next admission evidence. Do not advance on a narrower mocked green.
- [ ] **Step5 — One concentrated necessary boundary.** Only after main GREEN, freeze/review `test_bc08_restart_preserves_queue_deadline_and_resolution`. Recreate original Scheduler managers/repositories while a recurring goal waits and later occurrence is queued; verify identity/age/no-execution-charge persists. Expire via the original supported `now`/queue deadline path, with no timestamp mutation or resetting age. Include one once-schedule native waiting/restart→actual aggregate final STOP sequence to prove parent is not prematurely completed/cancelled by original repair and completes only after legitimate resolution. Reuse that owner for minimal immutable resolution/replay or supported later human resume observation; no extra graph/matrix unless an actual material defect requires it. Source inspect legitimate unassigned retirement resolution; add focused behavior only if required by a concrete failure.

```bash
cd backend
.venv/bin/python -m pytest tests/fleet/test_bc08_fleet_agent_job_continuations.py::test_bc08_restart_preserves_queue_deadline_and_resolution -q -o addopts= --tb=short
```

These node IDs are proposed registrations, not execution claims. Commands require the existing private0600 PG carrier; never print URI. Real process fixtures may reuse accepted task images; do not rebuild or rerun unrelated successful suites. Test owner cleans only owned schemas/processes.

- [ ] **Step6 — Freeze/static/docs.** Ruff check/format-check and actual3.12 compile only changed Python files; Root diff whitespace, strict OpenSpec and changed guidance checks. Root synchronizes README/backend AGENTS/current evidence and OpenSpec8 tasks. Raw failures and exact execution/source qualification survive; do not claim installed bootstrap from native helpers.
- [ ] **Step7 — Independent SPEC then QUALITY.** Fresh reviewers read current original seams and raw evidence. Fix material findings on smallest affected existing selector. Root verifies reviewed committed blobs and acceptance records; only then check OpenSpec8.1–8.4 and start BC09. No push, merge, activation or ECS deployment.

## Self-review

The original durable queue retains age, coalescing and zero executing-budget semantics. Resolution persists in the original authoritative STOP or verified never-assigned retirement transaction, avoiding process-local state and receipt reopening. Optional original completion/recovery callbacks cover once semantics and preserve Local behavior. Existing approved slot identity covers distinct named jobs. Main covers real due occurrence and completion-triggered admission; one boundary covers restart/deadline/once resolution, without expanding test count. BC09 UI/product summary and BC10 installed combined gate remain outside this slice. Current status: discovery complete, no BC08 source or tests yet.
