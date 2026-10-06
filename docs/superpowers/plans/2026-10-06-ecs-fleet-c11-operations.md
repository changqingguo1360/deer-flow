# C11 task visibility and reversible admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax. Root owns shared documentation, evidence and commits; one implementer owns source/tests.

**Goal:** Expose understandable owned remote Agent task state in the existing thread UI, and close new C admission while retaining accepted work and reconciliation.

**Architecture:** Keep AgentTask distinct from public RunStatus and existing B/MCP tasks. A Gateway read projection joins authoritative task/current placement/run/Attempt/reservation state and returns an explicit nonsecret allowlist. The existing Fleet stream reader handles the zero-frame never-assigned terminal case; assigned history still uses original C07 identity and seal.

**Tech Stack:** Existing FastAPI/auth middleware, SQLAlchemy/PostgreSQL, React/TanStack Query/Rstest, offline Docker artifact recipe.

Baseline C10 commit `74003058d607e1f389d3cbf071c15ba44aa0c4f6`; approved requirements are first-principles design §10–11 and OpenSpec `remote-agent-operations` C11. This concretizes the already authorized design. Public startup remains closed until C12 release evidence, and no real configuration is enabled by this slice.

## Source audit and decisions

- `AgentTasks.public_summary` currently has no public transport. It omits assigned cancellation stored on core RunRow and physical resources. Do not serialize ORM records or launch specifications.
- `install_fleet_ownership` preserves control/read/recovery when new admission is closed. `claim_agent` currently also checks agents_enabled/jobs_enabled, incorrectly blocking already admitted queued C. Those new-work flags belong at routing/admission, not accepted placement claim. Keep all original session/compatibility/profile/capacity/start/lease checks.
- `ThreadBackgroundTasks` is mounted in both chat pages but currently returns null when MCP tasks are disabled. Add independent C querying to that existing panel, with separate C types. Preserve the existing B cards, counters, detail and cancellation behavior.
- C07 subscribes with identity=None while queued; on never-assigned terminal placement it currently returns without END. No Attempt may be fabricated for that case. A read of the original committed run/placement pair can prove an empty terminal stream only when owner/thread match, active_attempt_id is null, no historical Agent Attempt or committed remote frames exists, and the exact accepted terminal pair is valid. Missing/foreign/inconsistent mappings do not prove END. Assigned identity/cursor/history/seal behavior remains unchanged.
- Current production worker Dockerfile builds the Node daemon. The installed Agent bootstrap/collector live in `deerflow_ecs_fleet.worker`; production Agent image recipe must install frozen wheels and approved nonsecret bindings/contracts/skills, not test fixtures. Gateway/UI changes do not require repeating the existing Runner image main. Actual recipe build/release is verified with C12’s real Runner main if no earlier participating runtime change requires it.

## File responsibilities

- Create `backend/app/fleet/task_summaries.py`: owner-scoped bounded read projection using the current task, exact placement/run/Attempt and unreleased reservation ledger. Harness must not import the optional extension.
- Create `backend/app/gateway/routers/fleet_agent_tasks.py`: normal authenticated owner-checked GET list/detail under `/api/threads/{thread_id}/agent-tasks`; use existing `require_permission("runs", "read", owner_check=True)` and `get_current_user`. Mount in `app/gateway/app.py`. No node bearer or admin-only user transport, and no new cancel endpoint.
- Modify `backend/app/fleet/ownership.py`: allow claims for accepted queued placements with new-admission flags closed; do not remove enabled/runtime/session/profile/capacity authority checks.
- Modify `backend/app/fleet/events.py`: narrow authoritative never-assigned terminal read in FleetStreamReader and END branch in FleetGatewayBridge. Do not modify Runner event publication/fencing or assigned stream identities.
- Create `frontend/src/core/fleet/{types,api,hooks,presentation}.ts`: typed separate C summary, existing authenticated fetch transport, owner/thread query key, bounded polling and pure label precedence.
- Create `frontend/src/components/workspace/fleet-task-summary.tsx`; modify existing `thread-background-tasks.tsx`: render C summary and original run link/cancel affordance through existing run API; never infer physical stop from accepted cancel or terminal RunStatus. MCP switch does not hide C summaries. Avoid new admin/machine UI.
- Modify `frontend/src/core/i18n/locales/{en-US,zh-CN}.ts` and locale types if required: queue, remote execution, waiting results, needs confirmation, stopping/stop-unconfirmed, terminal task labels.
- Create `docker/fleet/agent.Dockerfile`: mirror the worker’s digest-pinned base/offline hashed artifact installation; install actual app/harness/extension-api/Fleet plus approved provider wheels. Place actual bootstrap and workspace collector at the exact `/opt/deerflow` paths; approved model/runtime/workspace JSON and skill files are immutable nonsecret artifacts. Use nonroot, `/workspace`, ENTRYPOINT python and exact isolated bootstrap CMD with provider gateway. No fixture modules or credentials in layers.
- Test `backend/tests/fleet/test_c11_remote_agent_operations.py`, `frontend/tests/unit/core/fleet/presentation.test.ts` and a compact actual component `.dom.test.tsx` matching module conventions. Reuse C10/auth/PG and original accepted Local/B fixtures; no shadow fleet_probe or expected-valued observations.
- Root documentation: README, relevant AGENTS within byte budgets, `docs/deployment/ecs-fleet.md`, runtime/acceptance evidence, original C plan/OpenSpec tasks. Do not edit CLAUDE.md.

## Public read contract

Return only task identity/state/current_run_id/generation, original run status, profile and location category, cancellation intent, recovery requirement, physical stop state and held-resource boolean. Cancellation intent includes task and core run intent. Physical stop is confirmed only from the original Attempt’s durable STOP; never-assigned work is explicitly not_started. Terminal task/run with held resources remains visible as stop unconfirmed. Exclude tokens, credentials, hashes, node sessions, private process references, launch payloads, secret references, arbitrary errors/outcomes and filesystem paths. Return no task belonging to another owner/thread. Missing optional Fleet returns an empty list for ordinary Local installations; a configured unready runtime is an explicit unavailable state, not a fabricated empty success. Bound list pagination and order unsettled tasks before recent settled history.

Example shape (the implementation must derive every value from actual rows):

```json
{"task_id":"goal","state":"running","current_run_id":"run","generation":1,"run_status":"running","profile":"remote","location":"remote","cancel_requested":true,"recovery_required":false,"stop_state":"unconfirmed","resources_held":true}
```

UI precedence: recovery-required/unknown → needs confirmation; active cancellation or terminal-but-held → stopping/stop unconfirmed; otherwise task state. Keep run success separate from task waiting_jobs and physical resource release. The UI submits cancellation through the original owned run endpoint; no new public RunStatus values.

## Main-first sequence

- [x] Read-only source audit: real route absence, query ownership, core cancellation source, panel gating, existing claim gate, never-assigned END and deployment assets established. No runtime side effects.
- [x] Write one genuine C11 main using actual PG, mounted authenticated routes and original admission/claim/start/cancel/STOP lifecycle. First legal owned summary request should fail because the route/projection is absent (404 is genuine missing capability only after normal thread/user setup succeeds). Missing imports/config/credentials/skip are setup failures, not RED.
- [x] Implement the main protocol. Admit C, observe queued summary, perform original claim/start, observe running state, cancel through original route and verify intent/resources remain held. Close new C admission using the frozen operator configuration installed on actual runtime/bridges; a new remote request returns503, existing accepted queued work can claim, original started work can renew/STOP/reconcile, original owned summaries stay queryable. Observe exact DB rows; do not write expected JSON directly.
- [x] Run that main to GREEN before adding subordinate checks. Preserve one genuine Local execution through original RunManager/run_agent and existing B submission/execution evidence (deterministic installed model/provider is allowed; AsyncMock run_agent alone is not full Local parity). Carry unchanged accepted Docker evidence with explicit scope; no repeat whole B suite or image matrix. If a participating path changes, run its actual main once.
- [x] Connect the actual returned summary contract to real frontend rendering. Test meaningful DOM behavior: C remains visible with MCP disabled, cancellation/terminal-but-held never says stopped, original B presentation remains. Keep pure state mapping in one compact node-project test file. Frontend DOM data may be a recorded actual public JSON fixture, clearly separate from backend DB proof.
- [x] Resolve never-assigned terminal SSE with one compact native PG check: subscribe queued, cancel/retire original accepted run with no Attempt, observe END only after committed matching terminal proof; reconnect also closes. Retained queued or owner/history mismatch does not produce END. Existing assigned cursor/seal checks remain intact; use existing narrow C07 checks rather than a new matrix.
- [x] Add only directly affected authorization/nonsecret/disable checks after main. Reuse existing B public-secret/presentation, C07 queued/changed-mapping and C10 cancellation cases where their source paths participate. No all-backend/all-Fleet command is required under the user’s corrected scope. Preserve failures and run only exact affected rechecks.
- [x] Document offline Agent build contract and release boundary. Run changed backend Ruff check/format, diffcheck, strict OpenSpec; frontend through absolute `scripts/pnpm.py` from frontend cwd for focused Rstest and pnpm check. No package installs or pulls unless a concrete authorized need is established.
- [ ] Whole SPEC→QUALITY followed by Root requirement/source/evidence audit. Fix actual findings and rerun only affected proof; no speculative expansion. Explicit slice commit only after Ready. Then check OpenSpec11.1–11.4; C12/BC remain unchecked and real startup flag remains closed.

## Ownership and evidence

One fresh implementer owns source/tests and retained runtime handles. Root owns this plan, OpenSpec, README/guides, acceptance report and commits. Cached venv/artifacts, explicit local NO_PROXY, isolated UUID schemas, no global Docker pruning. Save natural-exit logs/receipts, exact observed JSON, source hashes and cleanup under `.local/fleet-evidence/c11/` after Root inspection. Observation timeout is not process termination; resume the original handle. Never remove unowned schemas or label an interrupted full run as PASS.

Main RED reached (2026-10-06): actual session-cookie HTTP remote POST200 succeeded, then owned `/api/threads/thread-c10-http/agent-tasks` returned404 versus required200. Native PG fixture naturally exited; log1 failed/3.56s inspected by Root and preserved in `.local/fleet-evidence/c11/main-red-v1/`. This is missing summary transport, not setup failure. Main implementation has started; no secondary test or acceptance claim. Planned native start/STOP calls prove host protocol behavior only; actual process execution/STOP remain carried at prior C09 scope and require the C12 real Runner release main.

First main GREEN:1 passed/2.41s naturally. Root inspected mounted session-auth list/detail JSON and actual PG rows: queued/not_started; original claim/start/running with resources held; assigned cancellation intent from core run; new C503 after flags close; accepted queued placement still claims; original renewal control and STOP/reconcile remain available; original summary remains queryable. Evidence retained in `.local/fleet-evidence/c11/main-green-v1/`. Inherited fixture instantiated RunManager before control installation; C11 now constructs it after install, matching production. This is native host protocol proof, not actual container execution or whole C11 acceptance. Local/B parity, never-assigned SSE, UI, deployment and reviews remain pending.

Necessary SSE RED after first main GREEN: actual never-assigned cancellation committed, queued subscription returned StopAsyncIteration instead of END (1 failed/2.38s). Root read exact terminal log and retained `.local/fleet-evidence/c11/sse-red-v1/`. This isolates the original missing zero-frame terminal closure; assigned stream history/seal behavior is not redefined. Initial main participating5-file hashes are retained separately, before the added SSE case. No whole-suite run or new Runner build.

Necessary finite GREEN: never-assigned queued heartbeat→committed cancel→END and reconnect END passed1/2.52s; foreign owner and inconsistent pair do not prove END. Auth/nonsecret/optional absence/unready checks plus genuine original Local graph passed1/2.05s. Root inspected actual Local checkpoint message and durable success with agents_enabled=false/jobs_enabled=true; only external model was deterministic, original RunManager/run_agent/lead_agent executed. Evidence retained in `.local/fleet-evidence/c11/finite-green-v1/`. Two new UI tests complement existing B checks; reported focused4-file/16-case result is awaiting Root log inspection. Initial Local fixture failures were overwritten in author log and are explicitly documented in attempt-history rather than falsely preserved or relabeled. Formatting, B focused checks, final source/reviews and C11 acceptance remain pending.


Final source freeze (2026-10-06): native main-final-v3 passed1/3.92s after the actual
terminal-held projection correction; persisted Attempt launch spec records retained
C09 image960f and runtimef441b. Main-final-v2 corrected image metadata and passed1/3.02s;
first main GREEN retains its older3ec native-input scope, never relabeled as a Runner
execution. Existing DOM case extended for MCP-on-with-C/no-empty-hint passed1/3.672s,
frontend check exited0. Exactly3backend+2UI new cases; no added matrix. Nine real
neighbor cases passed in a batch whose recipe mirror failed; that batch remains
1failed/9passed, and the subsequent recipe failure also remains recorded. The
source-mirror case was removed; production recipe build/ABI/runtime proof stays C12.
Original focused UI16pass exists as structured original-tool-capture evidence; raw
stdout was not retained, explicitly documented. Root independently matched20 source,
test and fixture hashes and retained all final handoff bytes in
`.local/fleet-evidence/c11/final-handoff-v1/`. Thirteen fixture-owned schemas were
absent at final cleanup, with no unowned mutation. Guidance check24/0errors/6soft
warnings and strict OpenSpec3/0; wholeSPEC→QUALITY and slice acceptance remain pending.

Whole SPEC review Ready, no findings (2026-10-06). QUALITY review is now in progress; final Root acceptance and slice commit remain pending.

Whole QUALITY Ready, no findings. Root matched all20 final source hashes, inspected retained proof and completed local C11 acceptance. Explicit source commit follows; C12/BC and public activation remain pending.
