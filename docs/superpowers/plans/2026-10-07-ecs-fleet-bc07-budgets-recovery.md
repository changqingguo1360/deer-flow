# BC07 aggregate budgets and recovery implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Execute one main first, then at most one concentrated necessary boundary. Existing user authorization covers implementation and local verification; no additional approval ceremony is required for routine choices within this approved BC07 requirement.

**Goal:** Enforce task-lifetime run, token and child-job budgets before use, preserve spent/uncertain charges across continuation and generation changes, and expose deadline/crash recovery without admitting an unsafe continuation.

**Architecture:** Original admission transactions charge one durable task ledger. A neutral private model-call capability intercepts the actual certified provider request after serialization and before transport. Original publication, accepted checkpoint/workspace and physical STOP remain authoritative; the existing continuation scan owns bounded recovery adjudication.

**Tech Stack:** Existing Python/SQLAlchemy/Postgres, private Fleet migration, original LangGraph/BaseChatModel/provider SDK and stock Node harness.

Baseline: BC06 source `8abe7af9f9b4683f055ba577ee9b0b6cac686a45`, documentation `30424e748cd540aa354484aaf977f00747784a7e`. BC06 independent SPEC and corrected QUALITY Ready; worktree clean at start. OpenSpec: `add-ecs-agent-job-continuations` / `Aggregate budgets and nonautomatic crash recovery`. BC08–BC10 remain outside this slice.

## Verified seams and file ownership

The original per-run TokenBudgetMiddleware observes completed responses; it cannot establish a pre-call task hard cap. System model observers report terminal calls. Neither is the new enforcement authority. The common factory reaches lead, subagent, title, goal, compaction and default memory; explicit cached DeerMem construction requires the same call-time guard or a visible private preflight rejection. Existing `ModelConfig.context_window` is UI metadata and cannot certify input bounds.

Create these focused units:

- `backend/packages/harness/deerflow/models/call_budget.py`: optional opaque ContextVar capability and scope; no Fleet/app imports.
- `backend/packages/harness/deerflow/models/budgeted_provider.py`: approved request/stream adapters; retain the original BaseChatModel instance and its bindings/callbacks.
- `backend/packages/ecs-fleet/deerflow_ecs_fleet/task_budgets.py`: task counters, immutable admission/call receipts, reserve/settle/unknown transactions.
- `backend/packages/ecs-fleet/deerflow_ecs_fleet/migrations/versions/f0016_task_budgets.py`: frozen task limits, durable counters/receipts and recovery reasons.
- `backend/app/fleet/model_budgets.py`: host adapter binding original task/run/attempt authority to the neutral capability.
- `backend/app/fleet/task_recovery.py`: bounded deadline/unknown/budget reconciliation using existing scan lifecycle and lock order.
- `backend/tests/fleet/test_bc07_fleet_agent_job_dependencies.py`: one actual main and one concentrated necessary boundary, with actual receipts and cleanup.

Modify original seams only where needed:

- Fleet `config.py`, `persistence/models.py`, `persistence/agent_tasks.py`, `job_service.py`.
- Host `execution.py`, `continuations.py`, `task_admission.py`, `runner_context.py`; use original publication/completion seam if needed to produce an exact paused source.
- Harness `models/factory.py`; explicit DeerMem construction/context dispatch only if needed to prevent a bypass. Sync/thread calls must retain the same private capability.
- `README.md`, relevant existing module AGENTS guides and this slice's runtime/acceptance documents. Update OpenSpec7.1–7.4 only after actual evidence/commit.

## Protocol decisions

### 1. Frozen task ledger

Initial owned admission freezes operator limits on the task. New run IDs, human generations and automatic continuations do not reset them. Committed admitted runs and logical submitted jobs charge once, including detached submissions originating from this task. Idempotent reuse does not charge twice. Checkpoint/delete reservation operations do not themselves count as Agent runs. Physical CPU/memory reservations remain separate.

Run charging occurs inside the original initial/human/continuation admission transaction before new run/placement becomes committed. Job charging occurs inside original parent-fenced `FleetJobService.submit` before inserting a new logical job; owned existing submission reuse returns its original receipt.

```text
admitted_runs <= frozen_run_limit
submitted_jobs <= frozen_job_limit
spent_tokens + reserved_tokens <= frozen_token_limit
```

Row locking serializes each counter/receipt update on the original task. Preserve original task/thread/run/placement and jointly sorted node/reservation/attempt ordering. Limits cannot be enlarged by worker config or generation changes.

### 2. Certified model request before transport

The approved operator binding supplies a `budget_contract` identifying adapter, tokenizer/serialization revision, locally validated input upper bound, enforced combined output/reasoning bound, single completion and SDK retry limit. The implementation must demonstrate how its serialized text/tool input is bounded; approximate history counts and UI context-window values cannot authorize transport. Unsupported media, methods, framing or providers are rejected visibly before transport. The current Codex factory strips max_tokens and is therefore unbounded absent a separate enforced output contract.

Keep the original model object. Supported sync/async provider-client request methods consult the current capability after request serialization and before any network call. Cached models validate the current capability on every call. Structured/bound tools and callbacks retain their original API. Original externally retried requests receive a fresh ticket; SDK internal retries are disabled for guarded requests. Direct alternate memory models receive the same adapter or fail private preflight; no silent bypass is permitted.

The neutral interface is:

```python
reserve_sync(request_bound) -> ticket
reserve_async(request_bound) -> ticket
settle_sync(ticket, measured_usage)
settle_async(ticket, measured_usage)
mark_unknown(ticket, reason)
```

The host capability binds opaque original authority. Durable tickets include task, generation, run, attempt, node session, approved provider contract, serialized-request digest and unique call identity. A request bound is the validated input upper bound plus enforced maximum output/reasoning tokens. Reservation commits before HTTP. Stream adapters keep the ticket until authoritative completion and stream close. Duplicate settlement validates the exact original identity and immutable receipt.

A valid authoritative measured usage receipt transfers actual usage to spent and releases only unused reservation. Cancellation, provider errors, missing/invalid usage or lost original authority retain the full unresolved reservation. A later generation cannot refund spent or uncertain usage. Unsupported input/provider denial and exhausted budget decisions must persist their visible reason before the caller receives the failure.

### 3. Original STOP and recovery

A budget denial does not invent success, a checkpoint or STOP. Keep active publication identity until the original executor settles and matching STOP releases capacity. Use original terminal preparation to obtain a paired checkpoint/workspace if possible; only an exact accepted pair plus STOP permits paused/manual handling. Without that pair, expose recovery_required. Durable budget decisions are distinct from prematurely changing assigned task publication fields.

The existing continuation scan invokes bounded task recovery adjudication. A task deadline with unknown child work records an explicit reason and prevents new continuation; its unknown/quarantined physical reservations remain charged until original legitimate STOP reconciliation. Scan reconstruction does not clear uncertain budget reservations.

A parent crash after child submission and before consistent paired yield remains recovery_required. Later child success cannot substitute for the original accepted waiting pair or parent STOP/release. Neutral operation intents from BC06 also remain blocked until an exact recorded recovery decision; do not auto-clear a checkpoint/delete failure fence.

`AgentTasks.public_summary` exposes frozen limits, cumulative/uncertain usage and the durable blocked/recovery reason to the owned user. Do not create new broad admin workflows in this slice.

## Execution sequence and evidence

- [x] **Step1 — Main fixture first.** Register `test_bc07_cumulative_budget_original_execution` using original provider transport/stock graph/Node/PG composition. Exercise actual C→B→C across successive runs. Collect actual serialized requests, controlled-provider tokenizer/usage receipts, original SQL counters and provider call counts. No fixed usage constants may impersonate measurements. Assert counters survive continuation and a keyed human-generation change, and an exhausted run is rejected before original admission. Positive actual model reservation/usage and logical job accounting are part of the main; necessary negative token/job rejection checks follow in the sole concentrated boundary after main GREEN. Select the specific native/installed scope honestly; BC10 still owns final combined installed release qualification.
- [x] **Step2 — Actual RED.** Execute only that main selector once at the untouched baseline. A setup failure/skip is not product RED; repair only the fixture before classifying evidence. Capture current source map, command/exit, provider/SQL/STOP receipts and owned schema/process cleanup. No boundary or broad suite before main GREEN.
- [x] **Step3 — Implement original path.** Add durable ledger/migration and admission participants first; then private neutral request capability/adapters and original recovery scan/public summary. Maintain local behavior outside private scope. Prepare immutable logical patches and fingerprints for Root review before feature application; apply approved whole patches with existing authorization, never split to bypass automatic review.
- [x] **Step4 — Main GREEN.** Re-run the same main against actual current source. Keep live handles and poll them until natural termination; timeout alone does not permit restart. Qualify only observed scope. When actual production changes invalidate evidence, repeat only the affected selector.
- [x] **Step5 — One concentrated boundary.** Only after main passes, register `test_bc07_unknown_deadline_and_parent_crash` in the same module. Sequential necessary segments cover token-call and logical-job exhaustion rejecting transport/admission before side effects, unresolved streaming usage retaining reservation, unknown child reaching task deadline without releasing capacity, actual parent death after child submission before paired yield followed by child completion with no automatic continuation, and minimal concurrency/idempotent reserve/reuse if needed to establish the actual locking contract. Use actual processes/SQL/transport observations, not asserted expected dictionaries or manually pretending a process died.
- [x] **Step6 — Freeze and verify.** Ruff check/format-check only changed Python files, git diff whitespace, strict OpenSpec and changed guidance checks. Preserve raw historical failures, executed/final source maps and exact byte/AST deltas. Do not rebuild unchanged images or repeat successful suites for documentation/format-only edits.
- [x] **Step7 — Independent SPEC then QUALITY.** Review current production/new modules and actual qualification evidence. Fix material findings with the smallest affected existing selector. Root records reviewer independence/reuse limitations and verifies the committed reviewed blobs. Only after these gates mark OpenSpec7.1–7.4 complete, commit slice and acceptance records, then start BC08.

Main command, after the actual selector exists, from backend:

```bash
.venv/bin/python -m pytest tests/fleet/test_bc07_fleet_agent_job_dependencies.py::test_bc07_cumulative_budget_original_execution -q -o addopts= --tb=short
```

Concentrated boundary command, only after main GREEN:

```bash
.venv/bin/python -m pytest tests/fleet/test_bc07_fleet_agent_job_dependencies.py::test_bc07_unknown_deadline_and_parent_crash -q -o addopts= --tb=short
```

These are planned node IDs, not execution claims. Private PG URI remains in the existing0600 carrier; never print it. Test owner alone runs/polls tests. Clean only owned schemas/processes/images; preserve unowned databases and the original accepted images. No push, merge, operator activation or ECS deployment.

## Self-review

The frozen task ledger covers cumulative run/token/job limits. Certified request interception and durable pre-transport reservation cover the mandatory model-before-call gate, including cached/system/delegated calls. Immutable settlement and unknown retention cover nonrefundable spent/uncertain usage. Original accepted source/STOP plus bounded deadline/crash adjudication cover nonautomatic recovery. Main first and at most one concentrated boundary match the user's test priority. Current status: reviewed candidate applied; native main-green-01 and concentrated boundary-05 passed. Final conservative uncertainty projection and exact-AST formatting are applied;22 changed Python files pass Ruff/format and actualPython3.12 compile. Source is frozen for independent SPEC→QUALITY, which remains pending.

## Main fixture review clarification

The first main uses actual ChatOpenAI SDK HTTP transport with tokenizer measurements and the original BC02/BC03 native executor/publication/Node/PG helpers. It admits an automatic queued continuation, then uses the original same-task queued handoff for the fresh human generation; the fourth request explicitly resumes that same owned task. This avoids silently creating a fresh goal after success. Baseline future operator limits are proposed fixture configuration, not a claim that the old schema accepts them. Its initial product RED can establish the missing run limit only; source/receipt scope must say so. Main first exercises cumulative accounting and run denial; negative token/job guards remain required in the same concentrated boundary after main GREEN. This preserves the full requirement while following the user’s verification order. Full build_agent_environment private bootstrap is not invoked by the native helper; final installed combined wiring is a BC10 gate.

## Baseline execution record

main-red-01 fixture failure (missing mixed node advertisement), natural1/6.16s, is retained without product qualification. Corrected main-red-02 naturally exited1/10.07s: fourth same-task owned resume returned200 despite proposed run limit3; actual original parent→children→human Agent execution and queued automatic admission/cancellation were observed. Provider receipts measure three SDK requests2591tokens; owned schema dropped. At that RED baseline no production change had been applied. Token/job denials and recovery boundaries remain subsequent required evidence.

## Main GREEN record

The exact reviewed19-file candidate (18 production plus fixture version alignment)
was applied with every base/candidate/applied hash checked. main-green-01 naturally
exited0 in9.84s: fourth owned resume409, frozen limits3/2/1,000,000, admitted runs3
(two executed C runs; automatic queued2 cancelled), jobs2, generation2, actual
SDK requests3 and measured2593tokens, all receipts settled/reserved0. Original
publication/STOP/release and owned schema cleanup were recorded. Root independently
checked these receipts and all19 execution source hashes. Step7 remains pending; boundary-05 subsequently passed at its recorded native scope and final source/static verification completed.

## SPEC-01 focused correction

Fresh independent SPEC verified all22 frozen Python hashes, but did not approve regression52. Recorded Steps4–6 retain only their observed historical scopes. Extend the same main to three executed C runs with two actual intervening B/result/pair/STOP boundaries and fourth same-goal admission denial. Extend only token/job portions of the same boundary to new-run same-task exhaustion; the denied token request must fit a fresh frozen cap. Keep prior negative/usage/deadline/crash evidence intact; do not infer three executions from three admissions or add a matrix. No additional material source discrepancy was identified. Step7 stays open, OpenSpec7.2–7.4 unchecked; original author owns the focused correction and tests, then the same independent reviewer rechecks SPEC before fresh QUALITY.

## Focused correction evidence and current review

The unchanged production slice now has stronger repeated-execution evidence. `main-cumulative-03` naturally passed12.06s: C1→B1→C2→B2→C3, four real successful jobs, two accepted result/source boundaries, generation2, three admitted/executed C runs, five measured SDK requests7188tokens and reserved0, then fourth owned resume409. `boundary-cumulative-01` explicitly ran only token/job cross-run portions of the existing selector, naturally passed9.54s: actual token bound11713 fits fresh12000 but prior measured1606 exhausts cumulative13319; original new-run job submit refuses after two prior charged/accepted jobs. Both owners use original admission/claim/start authorization and do not claim an unlaunched second process STOP. Recovery portions retain boundary05 evidence with unchanged21 other Python files, exact helper AST and tail bytes. Historical revised-main fixture failure and redundant old-fixture run remain recorded. Final22 Python files freeze at patch `ac8de52a0ed5786b46bb6fefcc7f182ad18e9e15822eeab3c51b449ef25ec9bb`; all current hashes and static0 checked. Original independent SPEC reviewer is re-reviewing; QUALITY has not started, Step7 and OpenSpec7.2–7.4 remain open.

## Final local acceptance

Independent SPEC re-review and fresh QUALITY are Ready. Source `097b789a39810be58ab0e894780b61221ba26421` contains exactly22 reviewed Python blobs, independently verified after commit. OpenSpec7.1–7.4 may be closed at the documented native scope; final installed combined gate remains BC10. See [acceptance](../../../docs/ecs-fleet-bc07-acceptance.md).
