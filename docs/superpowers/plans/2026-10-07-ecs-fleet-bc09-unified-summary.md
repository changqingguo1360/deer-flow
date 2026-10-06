# BC09 Unified task summary and operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show the durable Agent goal independently of the current run, its related jobs/new runs and accepted result references, and offer generation-fenced goal cancel/resume with truthful pending/conflict presentation.

**Architecture:** Extend the original owner/thread-scoped `FleetTaskSummaries` projection and existing `core/fleet` card/hooks; use existing authenticated task cancel/resume endpoints. AgentTask remains the goal source, core run success never completes the goal, and HTTP mutations reuse original operation receipts. No new scheduler, execution protocol, frontend route or public RunStatus.

**Tech Stack:** Existing FastAPI/SQLAlchemy/PostgreSQL; React/TanStack Query/TypeScript; Rstest happy-dom; original native Agent/Node fixtures.

---

## Current source and authority

Feature checkout: `/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs`, branch `feature/personal-agent-ecs`. BC08 accepted source `d08f776375cd70776d0d0feda6a820a7eca65755`, acceptance docs `9e29ea40584218fd963cd258dd1317d491ba38c6`. BC09 is OpenSpec `add-ecs-agent-job-continuations`, requirement `Distinguish run completion from goal completion`, task9. BC10 installed combined release is mandatory and cannot be inferred from this UI/API slice.

Read module guides: backend/AGENTS.md, backend/app/channels/AGENTS.md, frontend/AGENTS.md, frontend/src/AGENTS.md and applicable deeper guidance. Root owns plans/evidence/docs/reviews/commits. One fresh author owns every source/test change and every runtime handle; Root exact candidate review precedes application. Main first, then one concentrated necessary boundary; no old suite matrix, default image rebuild or unrelated successful rerun. Existing human authorization covers local source edits/tests/commits; no push, activation or ECS deployment.

## Original seams and observed gaps

- `backend/app/fleet/task_summaries.py::FleetTaskSummaries.read/public` already separates AgentTask.state, current core status, physical STOP and held resources, but has no related job/run projection.
- `backend/app/gateway/routers/fleet_agent_tasks.py` already has owned GET/list and POST task cancel/resume with `expected_generation`/`idempotency_key`, original permissions and 404/409. Do not add another mutation route or bypass its participant.
- `frontend/src/core/fleet/api.ts/hooks.ts` currently cancels an original run through SDK; it lacks task operations and typed conflict handling.
- `FleetTaskSummary` only shows current run and suppresses cancellation when that run has succeeded, so waiting goals lack usable goal operations. Existing waiting Chinese label is `等待任务结果`; BC09 requires `等待计算`.
- `ThreadBackgroundTasks` already keeps Fleet summaries separate from existing B/MCP cards and enabled with MCP off. Preserve this integration and owner/thread query keys.

## Exact contract and file ownership

Author production files:

- Modify `backend/app/fleet/task_summaries.py`: bounded related projections inside the original owner-scoped read session; preserve budget/recovery/STOP fields. Change original router only for a demonstrated required omission; no mutation semantics rewrite.
- Modify `frontend/src/core/fleet/types.ts`, `api.ts`, `hooks.ts`, `presentation.ts`.
- Modify `frontend/src/components/workspace/fleet-task-summary.tsx` and `thread-background-tasks.tsx`.
- Modify `frontend/src/core/i18n/locales/en-US.ts` and `zh-CN.ts`.
- Create `backend/tests/fleet/test_bc09_fleet_unified_task_experience.py` and `frontend/tests/unit/core/fleet/unified-task-experience.dom.test.tsx`. Update existing Fleet DOM test mocks only when affected by the new real hook interface; no unrelated assertions.
- Create `backend/app/channels/fleet_summary.py`: dependency-neutral bounded formatter and authenticated read helper consuming the same original AgentTask API projection. Modify `backend/app/channels/manager.py` only at existing `/status`, original blocking final response and streaming final response points; no channel transport, dedupe, auth, pending follow-up or run lifecycle rewrite. Preserve owner/connection/thread metadata and existing artifact/clarification delivery.
- Root updates README, relevant AGENTS, development/runtime/acceptance docs, roadmap/progress/OpenSpec9.

Additive related transport contract (keep existing fields):

```typescript
export interface FleetRelatedJob {
  job_id: string;
  generation: number;
  parent_run_id: string;
  link_mode: "awaited" | "detached";
  state: string;
  accepted_manifest_id: string | null;
}
export interface FleetRelatedRun {
  run_id: string;
  generation: number;
  run_status: string;
}
// On FleetTask:
// jobs?: FleetRelatedJob[]; runs?: FleetRelatedRun[];
// jobs_truncated?: boolean; runs_truncated?: boolean;
export interface FleetGoalOperation {
  taskId: string;
  expectedGeneration: number;
  idempotencyKey: string;
}
```

Related rows come only from original JobLinkRow→JobRow and task RunPlacementRow→RunRow joins, matching exact user/thread/task/generation/source identity. Retain historical generations as labeled history, without treating them as current control authority. At most20 related jobs and20 related runs per selected task, deterministic ordering and truncation flags; fetch21 then expose20. Avoid fetching unlimited rows before slicing or per-task unbounded queries. List itself remains original max100/pagination. Never expose tool arguments, credentials, logs, private absolute paths or unaccepted result content. Manifest ID is only an accepted original identity, not invented file/download authority; validate manifest/job/attempt owner association before exposing it or reuse the existing accepted-reference projection. Existing current run link stays visible even when history is truncated.

Mutation transport:

```typescript
// POST /api/threads/{thread}/agent-tasks/{task}/cancel or /resume
const body = {
  expected_generation: operation.expectedGeneration,
  idempotency_key: operation.idempotencyKey,
};
// use the original shared authenticated fetcher and REST error envelope.
```

Operation UUID is generated for one logical action and retained through its in-flight attempt/retry; do not reuse it across different goal/generation/action or substitute run ID. An operation uses the displayed task generation. Disable repeated action while its mutation is pending. Success invalidates the original owner/thread query and appropriate original thread/run caches; HTTP409 also refetches and displays a translated conflict, without silently resending with a new generation. Auth401/403 retain normal authentication/permission behavior; 404 does not leak another owner. A cancellation request cannot locally mark cancelled/STOP confirmed or release resources.

Waiting goal shows `等待计算`/`Waiting for computation`, related jobs/manifest identities and original run history. Cancel goal applies to queued/running/waiting states through the task endpoint; resume offered only where original owned result-resume contract supports it (waiting/succeeded with legitimate lineage), never arbitrary paused/input/recovery auto-retry. Server remains final authority. Recovery/input-required display their specific state and existing honest confirmation/STOP semantics. Show pending operation and backend cancel intent distinctly from durable terminal outcome. Existing B cards and MCP-off Fleet querying remain functional.

## IM integration retained from the approved design

The approved first-principles design also requires IM goal status, and the original BC09 plan explicitly requires the same bounded task projection for IM result text. Add a dependency-neutral `app/channels/fleet_summary.py` helper; it must not import optional Fleet packages when Fleet is disabled. Read `GET /api/threads/{thread}/agent-tasks?limit=20` with the existing original `_owner_headers(msg)`/internal authentication identity; never substitute another user. Format only allowlisted task state/cancellation/STOP and bounded original job/run/accepted manifest identities from that safe projection. At most20 goals,20 links per goal and4096 UTF-8 bytes for the complete added text; deterministic truncation, no raw logs/tool arguments/errors as instructions. Use exact unchanged IDs or omit an overlong identifier, never manufacture a shortened link. Plain labels/IDs are sufficient when no operator public URL exists; do not invent a public URL from an internal Docker hostname.

Append this status in existing blocking and streaming FINAL outbound messages and existing `/status` replies; intermediate AI streaming remains unchanged. The parent `waiting_jobs` must say waiting for computation even if its initial run succeeded. Pending cancellation/unknown stays pending/needs confirmation, never completed from terminal core alone. Empty Fleet projection preserves ordinary Local/B response. Failed Fleet tracking is handled as unavailable status without failing the original response or claiming a physical stop; bounded network timeout. Fire-and-forget/background delivery keeps its original policy, using `/status` for the same owner projection; no new polling worker or unsolicited channel send path. Original GitHub no-auto-post policy stays unchanged.

The API main also exercises this original final/status integration through an in-memory original channel message bus and controlled HTTP response carrying the actual captured owned projection. It must record emitted text and owner/thread metadata, showing waiting and related identities without raw worker content. This is controlled IM integration qualification, not an external channel send or provider qualification. Add the corresponding pending/unavailable/empty-source observation to the same concentrated backend boundary, not a new selector or executor.

## Execution steps

- [ ] **Step1 — Freeze one main API/DOM fixture.** Reuse BC06 `_bc06_case`, `_bc06_http_host` and actual BC02/BC03/BC07 C/Node/checkpoint/job execution components. Existing approved profiles/slots and loaded runtime wiring must be real; preflight original native dependencies. API main builds actual C waiting on two named B jobs with genuine accepted checkpoint/workspace and physical STOP/release; GET original owned summary records run.success + task.waiting, distinct related jobs and original run. Complete original B jobs with accepted manifests; invoke authenticated task resume at actual generation via original route and inspect durable new-run/generation/source/result references without executing another graph solely for UI. Export the actual safe HTTP responses as bounded evidence for frontend consumption. DOM main renders original ThreadBackgroundTasks/card with TanStack hooks and mocked HTTP backed by those recorded contract responses; assert Chinese waiting label, no goal-complete label, related jobs/current and new runs, and original task-operation request body. Mock HTTP is UI transport qualification, not backend runtime evidence. Freeze full fixture/source map/static first; Root approval before application.

Registered selectors:

```bash
cd backend
.venv/bin/python3.12 -m pytest tests/fleet/test_bc09_fleet_unified_task_experience.py::test_bc09_owned_waiting_goal_summary_and_resume -q -o addopts= --tb=short
```

```bash
python3 scripts/pnpm.py rstest run tests/unit/core/fleet/unified-task-experience.dom.test.tsx --testNamePattern "waiting goal summary and task operations"
```

The author must confirm the installed Rstest option from local CLI before launch. These are proposed new registrations, not executed evidence. Backend requires existing private0600 PG carrier; never print URI. One native main and one DOM main are the two layers of the same user flow, not a backend runtime replacement.

- [ ] **Step2 — Genuine main RED.** Run only those main selectors at the accepted BC08 baseline. Source/setup/import/dependency failure or skip is not product RED; correct fixture first. Existing run/goal distinction may pass; missing bounded relation/operation/label behavior must establish the actual product gap. Preserve command/source/raw HTTP/SQL/process/cleanup and frontend failure; do not build an expected dictionary as an API substitute.
- [ ] **Step3 — Freeze minimal original implementation.** Prepare complete production/type/hook/component changes and affected old test mock adaptations, exact hashes and actual-path static receipts before application. Preserve original API authentication/fences/idempotent receipts and current presentation precedence. Root reviews the complete candidate and transaction/noninterference implications, then author applies exact bytes.
- [ ] **Step4 — Main GREEN.** Same API main first, then same DOM main. Confirm real accepted result references, generation and original queued resume admission; cancellation hook uses task ID/generation, never old terminal run cancellation. Record what actually ran. No claim of full private/installed bootstrap or live external provider.
- [ ] **Step5 — One concentrated necessary boundary.** Only after main GREEN, use one backend selector `test_bc09_owner_conflict_and_bounded_summary` in the same module and one DOM selector `pending conflict and confirmation states` in the same DOM file. Reuse existing owners/observed main rows where feasible: cross-owner read/mutation denial, stale generation409 with unchanged source rows, duplicate operation reuse and bounded related projection. Do not create20+ real executors for pagination; use original admitted/job-service durable paths only if a limit defect requires runtime rows. UI boundary uses explicitly controlled HTTP/input/recovery/cancel-pending contract states, shows honest labels/disabled actions, 409 translated conflict+refetch and no silent new-generation retry; keep permission denial visible and original B/MCP integration. Existing C11/BC06 authority evidence may carry when participating source is unchanged. No additional broad cases.
- [ ] **Step6 — Static/docs freeze.** Actual-path Ruff check/format and Python3.12 compile only changed Python; targeted frontend formatting plus mandatory `python3 scripts/pnpm.py check`. Run affected old Fleet DOM selector only if hook/mock source changed; no full frontend test suite. Root diff whitespace, strict OpenSpec and guidance checks; current/native/controlled-UI/source-only limits explicit.
- [ ] **Step7 — Fresh SPEC then QUALITY and commit.** Independent reviewers inspect final source/actual raw evidence in order. Fix only material findings and smallest affected existing selector. Root commits exact technical files, verifies every reviewed blob, then records acceptance and checks OpenSpec9.1–9.4. Only then enter BC10 installed combined release gate. No push/operator activation/deployment.

## Acceptance facts

The API and DOM must together prove `run_success_marks_goal_complete == false`, `waiting_goal_label == "等待计算"`, and `cancel_pending_visible == true`, from actual contract/DOM observations. Current UI actions target the durable goal with expected generation/idempotency. Related job/run/result identifiers are original, bounded and owned. A terminal core run, request success or frontend pending state proves neither aggregate completion nor physical STOP. Final BC10 remains required.
