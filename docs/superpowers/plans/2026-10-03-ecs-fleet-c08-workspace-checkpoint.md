# C08 consistent workspace checkpoint boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task by task, with a sole implementer and sequential SPEC → QUALITY review. Steps use checkbox (`- [ ]`) syntax for tracking. The existing user authorization covers this C08 slice; Root owns shared docs, runtime coordination and final acceptance.

**Goal:** Accept C recovery points only as (checkpoint_id, immutable workspace_manifest_id, attempt_id), publish immutable partial files immediately through owner file API, and require recovery without automatic tool replay when pairing fails.

**Architecture:** Keep one original AgentRunner/run_agent execution. Use remote-only synchronous stock checkpoint durability, a real execution-owned writer barrier, trusted Node sealing outside the runner mount, and a generic same-session core terminal participant for joint pair/run/placement/task commit. Initial C04 inputs remain distinct; subsequent owned runs clone an accepted immutable version, never alias Local or B directories.

**Tech stack:** Existing Python/LangGraph stock Postgres saver, SQLAlchemy host persistence, private Fleet Alembic f0009, NodeDaemon/Docker containment, NAS descriptor copy/open validation, FastAPI owner/thread APIs. This is the source-audited implementation plan, not implementation or tested acceptance.

## Scope and alternatives

Chosen direction: Node staged-seal request protocol + original owner-fenced pair transaction. Alternative runner-seal below /workspace fails immutability because tool uid can change it; broad NAS mount enlarges tool authority and violates intended isolation. Alternative terminal-only physical-stop sealing is simpler but cannot deliver stage files or commit final pair at original runtime terminal boundary; do not use it as C08 completion.

Local runtime behavior retains optional hooks disabled; B worker/output/sealed manifest protocol remains unchanged. Exclude C09 physical cancellation/recovery approval UI, C10 routing, activation, and BC wait-group/job continuation. Add only unsafe recovery guards and accepted-version preparation necessary for C08.

## Proposed file responsibilities (new names are proposals, not existing source)

- New packages/ecs-fleet/deerflow_ecs_fleet/agent_workspace.py: C version manifest and descriptor-safe NAS immutable candidate storage/verified clone. Reuse artifacts descriptor primitives without widening B SealedManifest/sealed_parts_valid.
- New packages/ecs-fleet/deerflow_ecs_fleet/persistence/workspace_points.py: private publication request/candidate/pair rows and transactional helpers; receives host run locker, never imports app/core implementation.
- New packages/ecs-fleet/deerflow_ecs_fleet/migrations/versions/f0009_workspace_points.py + modify persistence/models.py: private schema/checks/FKs only.
- New app/fleet/workspace.py: host-bound stage/final lifecycle participant, request/candidate orchestration, C owner file service, pair restore validation and recovery admission hook. Reuse FleetMutationCapability original identity/fence.
- New harness/runtime/execution/workspace_boundary.py: generic optional boundary contracts/context, active file-call/native-thread accounting, real subprocess registration; no app/Fleet import.
- Modify harness/runtime/checkpointer/fenced_saver.py/async_provider.py: optional trusted callback after stock aput transaction closes. Modify runtime/runs/worker.py RunContext/stream durability and final preparation seam; no new graph loop.
- Modify harness/persistence/run/base.py/sql.py and runtime/runs/manager.py as needed: generic terminal/admission participant signatures; all relevant terminal/admission paths use same real SQL session. Root must locate exact methods before editing; source audit confirms RunRepository currently has no terminal participant.
- Modify sandbox/local/local_sandbox.py and tool/runtime boundary: track native writer entry/exit and retain launched groups under private execution scope; Local unscoped behavior unchanged. Account file writes in sandbox.write_file/update_file and original asynchronous to_thread owners.
- Modify app/fleet/runner_context.py: install trusted participant/barrier, same teardown budget; writer scopes around original graph/tools/subagents; freeze post-terminal file access. Modify app/fleet/mutation.py with narrow accepted-pair terminal bookkeeping branch, preserving same lock order and operations.
- Modify worker/agent_containers.py/daemon.py/client.py and gateway/routers/fleet_nodes.py: C-only authenticated pending request retrieval and candidate acknowledgement; descriptor seal outside /workspace mount; poll alongside watchdog, never B /complete.
- Modify worker/agent_workspace.py preparation: initial-input vs accepted-version union branch and verify complete version on reuse; never treat initial sha256 input ref as accepted C pair.
- Modify app/fleet/ownership.py/execution.py; persistence/agent_tasks.py: pair-aware stop/recovery state, exclusive recovery admission, summary semantics. Modify app/fleet/events.py only exact accepted-point/uncertain identity branches required by C08; retain C07 seal proof.
- Modify gateway/routers/fleet_artifacts.py or create C-only owned router: metadata/download of accepted C stage/final outputs; immutable manifest ID addressability. Existing B route behavior unchanged.
- New tests/fleet/c08_integration_fixture.py, test_c08_remote_agent_runtime.py, test_c08_workspace.py, test_c08_workspace_transactions.py, test_c08_writer_quiescence.py, test_c08_installed_workspace.py, build_c08_runner_image.py. Source audit names current fixtures to adapt.
- Root tracked docs: docs/ecs-fleet-c08-runtime.md (depth), README relevant paragraph, guide tiny depth links/necessary compression, final docs/ecs-fleet-c08-acceptance.md after acceptance. OpenSpec 8.1-8.4 stays unchecked until Root final acceptance.

## Interface/schema contract

Define frozen `WorkspaceBoundaryIdentity` by reusing complete RemoteMutationContext. `WorkspaceSealRequest` fields: request_id, owner user/thread, run/task/generation/attempt, launch digest, checkpoint_ns="", checkpoint_id, kind partial/final/paused, publication_key, sorted presented_paths, source workspace version, desired terminal status/error/stop_reason (final only). Request uniqueness: (attempt_id, checkpoint_id, kind, publication_key); identical retry returns same row, any changed immutable field conflicts. No bearer/private secrets in request payload.

Private rows:
1. fleet_workspace_requests: complete original identity, immutable payload digest, state requested/prepared/accepted/rejected, checkpoint ref, request kind, candidate manifest ID, timestamps, non-secret rejection reason. Node-specific current request read must authenticate original Agent attempt token/session/process ref; caller does not choose NAS prefix.
2. fleet_workspace_manifests: immutable ID/content hash, user/thread/task/run/generation/attempt, schema version, canonical sorted category/path/size/digest entries, total_bytes, sealed_at, NAS content prefix. Categories workspace/uploads/outputs; snapshot entire user-data necessary for resume, expose only requested outputs in public view. Do not snapshot .deer-flow config/secrets, process receipts or control markers. Include empty directory representation or explicitly preserve category roots; deleted files remain deleted on clone. Budget derives frozen profile output + input limits, never unbounded whole /workspace.
3. fleet_workspace_points: immutable point ID, complete owner/run/task/generation/attempt identity, root checkpoint_id, manifest_id, kind, publication_key, final outcome. Uniqueness on request plus one final accepted point per run; partial points never replace final completion. Composite owner/attempt FKs prevent cross-thread association. task.accepted_workspace_point_id and placement.final_workspace_point_id reference accepted points and are updated only in pair transaction. C09/BC pointers excluded.

C state checks explicitly add recovery_required and finishing on task/placement. Finishing retains task active-thread ownership until physical stop; final point stores immutable desired terminal outcomes. Agent attempt can retain unknown/quarantined as existing physical uncertainty; do not silently substitute those for public/private task recovery_required. AGENT_TASK_ACTIVE remains NOT IN the four terminal states; recovery_required retains unique owner/thread goal. Summary recovery_required = exact unsafe state (legacy unknown may remain unsafe), not ordinary input_required.

Host contracts (pseudocode signatures, all new proposed types defined above):
```python
class WorkspaceBoundary:
    async def checkpoint_committed(self, config, checkpoint, metadata): ...
    async def prepare_terminal(self, record, checkpoint_config): ...
    async def participate_terminal(self, session, record, prepared_point): ...
    async def reject_unmatched(self, reason): ...
```
`checkpoint_committed` runs after stock saver commit and only root namespace, for newly persisted artifact presentation changes. `prepare_terminal` closes writer admission permanently, settles real writers under existing teardown CleanupBudget, reads exact final root checkpoint, requests Node sealing and awaits prepared candidate. `participate_terminal` performs no NAS copy; validates candidate and original/latest checkpoint + owner on the same actual terminal writer transaction, advances accepted point/core run terminal/private placement and task finishing together. Final point stores desired task/placement outcome; physical stop applies it later. Repeat same final record is idempotent; changed checkpoint/outcome conflicts. Callback exceptions never fall through to success terminal persistence.

## Writer/checkpoint/seal ordering

Stage: tool writes and present_files produce a real Command; stock graph completes its superstep; C uses durability="sync"; stock aput finishes including upstream required pending-write dependencies; trusted checkpoint callback closes writer gate, waits all current original calls/native Tasks/subagents and contained process writers, then submits seal request. Node copies outside mount; Gateway validates bounded immutable candidate; runner accepts partial pair under original fence; owner HTTP bytes available before callback reopens gate/next step. Required proof: no next tool writer enters while current seal is incomplete. Do not accept a values-frame timestamp as checkpoint proof.

Final: graph stream/checkpoint Tasks settled; cancellation/rollback/title/duration writes settle; before actual first core terminal TX permanently close writers, settle external/background writers, prepare last exact checkpoint and candidate; terminal participant atomically accepts final pair/core outcome/placement and task finishing; it stores immutable desired final private outcomes. Then original scheduler/task-stop/thread bookkeeping runs with only exact original accepted-point authority; ordinary memory/store/checkpoint/event writes remain governed by original narrow contract. If those callbacks could write files, prohibit them through closed gate/remote profile contract; cannot claim quiescence while allowing an untracked service writer.

Linux proof must cover escaped sessions/background children. Track process identity by PID + start identity/cgroup, not recycled PID. The original producer must positively stop/join registered supervisors and their descendants before publication; trusted Node independently verifies a zero-child census before and after copy using a different-UID fixed helper. Only PID1 and the actual helper may remain; no blanket kill of host, PG or Redis. setsid descendants still in attempt container cgroup must be stopped. Native writer settlement uses actual retained Tasks/threads; cancellation of await never substitutes physical completion. Unsupported provider/filewriter profiles reject before admission rather than claim safe quiescence.

Filesystem candidate writes occur outside owner SQL locks. Node uses descriptor-open NAS sentinel and attempt-root identity from accepted claim, writes candidate temporary directory under `.fleet-agent-workspaces/{user}/{thread}/{task}/{attempt}/{manifest}` outside the runner-mounted attempt root, copies bounded data with O_NOFOLLOW/regular nlink1/stability/digest checks, fsyncs files/dirs/manifest, finalizes content-addressed directory by atomic no-overwrite publication. Gateway independently verifies candidate and rechecks fresh time after I/O before prepared acknowledgement. Runner revalidates exact original request/candidate identity/current checkpoint at pair accept; losing lease or changed head prevents accept. SQL rollback leaves unaccepted orphan candidate, never a completed run; cleanup must retain unsafe evidence.

Lock order remains goal admission advisory lock (when admitting) -> task -> core run -> placement -> node -> reservation -> attempt -> request -> point/manifest. Snapshot verification is outside pair TX. Pair acceptance checks fresh clock_timestamp after locks and after SQL flush. Checkpoint head is exact root namespace associated with current original run; inspect stock saver metadata/ancestry plus request ID and reject arbitrary older/stage ID for final. Store immutable checkpoint_id, not latest alias. No filesystem compensation writes inside checkpoint TX.

## Outcome/recovery matrix

- Normal success: accepted final point; run success and placement/task finishing in one TX, with immutable desired succeeded outcomes in final point. Resources and task thread exclusivity remain held until trusted actual physical stop. Then one idempotent stopped TX applies the point's succeeded outcome and releases reservation. C07 end seal accepts only original closed record matching this final point.
- Human graph interrupt / supported paused boundary: checkpoint with pending graph tasks is valid only when filewriter quiescence positive; accepted paused point; core interrupted, placement/task finishing until actual physical stop; stored desired task input_required/paused and per-run placement/attempt existing cancelled outcome. Original ClarificationMiddleware ask_clarification uses Command(goto=END), so next/tasks may be empty: use trusted current-run root tool observation plus the actual materialized request ToolMessage, preserving original request/card protocol. Historical/client/subgraph/suppressed requests do not pause the root. After stop, any future authorized run clones accepted version with exact checkpoint input. Do not implement recovery approvals/BC continuation.
- Ordinary tool/model failure with fully settled writers and consistent final checkpoint: accepted failed point, run error/timeout, placement/task finishing with immutable desired failed/timed_out outcomes; actual physical stop applies them. No replay. Inability to produce a matching point is unsafe recovery, never ordinary failed completion with mutable files.
- Kill before final sealing, network partition, missing/changed/deleted manifest, clone digest mismatch, post-lock lease expiry or checkpoint mismatch: no accepted final pair; task/placement recovery_required, automatic tool replay forbidden, existing partial remains immutable/readable and marked partial. Preserve exclusive task/thread and unproven resources; release physical units only after existing trusted stop proof, but do not clear recovery ownership.
- Kill after final pair commit before response/streamseal: accepted final remains immutable; repeated requests reuse same pair; trusted physical stopped recovery preserves exact final result, closes finishing to its stored target state and performs C07 durable closure. It cannot fabricate a new pair from last checkpoint.
- Node restart: existing daemon stops residual processes before reports, then handles original request/candidate idempotently; never launches replacement for recovery_required. Recovery does not become a scheduler continuation.
- Repeat start/run request: existing launch digest/owner identity enforced, no additional runner/tool execution. Stage retries require exact same request bytes. Conflicting terminal outcome or generation/owner fails without changing accepted data.

## Tasks, RED and verification commands (future execution by Root only)

### Task 1 — baseline-safe real scenario before implementation

- [x] Create C08 fixture by adapting c07_scenario, actual node_server, native_runtime_paths, actual original AgentRunner and stock lead model factory. Script model: first real tool writes `/mnt/user-data/outputs/partial.txt`; second calls original present_files; controller pauses before next actual model/tool; final tool changes output and writes final.txt; normal finish. Tool writes use original LocalSandbox tools, not fixture direct replacement graph. Capture original runtime module path/hash, PID/host/start event and real SQL owner rows. Fixture receipt paths stay outside user-data snapshot.
- [x] Test existing owner HTTP endpoint for phase file metadata/bytes; on C07 baseline request legitimate output before runner completes, assert HTTP200/real bytes plus a real accepted point; exact partial=true metadata and C-version URL binding are mandatory later GREEN additions (Task5). Expected RED is existing routed HTTP404/absence of accepted partial despite real write and real checkpoint, not missing fixture/API import/PG setup. RED kill companion observes current private unknown rather than required recovery_required, plus no final point; use existing stopped route first, not a new nonexistent function.
- [x] Use actual controller barrier after output/tool checkpoint but before sealing; kill child PID/container, wait its real process exit, call trusted existing stopped path and restart existing daemon/Gateway service. Count tool execution receipts before/after instead of expected field constants.

Run from checkout/backend with Root-provided TEST_POSTGRES_URI (do not print URI/password):
```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c08_remote_agent_runtime.py::test_stage_file_is_readable_before_next_original_step -vv --junitxml=../.local/fleet-evidence/c08-planning/red-stage.xml
PYTHONPATH=. uv run pytest tests/fleet/test_c08_remote_agent_runtime.py::test_killed_original_runner_requires_recovery_without_replay -vv --junitxml=../.local/fleet-evidence/c08-planning/red-kill.xml
```
Expected baseline FAIL at behavior assertions after all real setup succeeds, zero skips. If observed baseline cannot reach barrier, fix fixture and rerun, do not label setup failure RED.

### Task 2 — private schema and immutable C NAS version storage

- [x] Add f0009 after real f0008; model constraints/composite ownership match private metadata, private version table remains Fleet owned. Extend state CHECKs and index/public summary semantics explicitly.
- [x] Implement descriptor C storage independently from B, candidate marker and verified clone, retain complete canonical metadata. Test symlink/hardlink/FIFO/depth/entry/byte limit, source replacement during copy, digest mutation/deletion, empty categories, overwrite/idempotency conflict and NAS sentinel failure.
- [x] Test upgrade actual f0008 PG -> f0009, repeat startup and concurrent startup advisory lock, private host metadata boundary, downgrade refusal with recovery evidence if appropriate. Use no manual core migration edit.
```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c08_workspace.py tests/fleet/test_c08_workspace_transactions.py -vv
```
Expected RED/GREEN against actual descriptors/private PG rows, no SQLite substitute for ownership/locks.

### Task 3 — real writer barrier and Node staged-seal protocol

- [x] Implement optional private writer scope and native/child registry, preserve Local defaults. Bind C-only scope in runner environment and original sandbox operations. Extend Node polling/read/ack outside B /complete; do not add runner bearer or full NAS mount.
- [x] Test original bash creates real delayed background writer plus setsid child; prove real PID/start identity stops before candidate copy, file hash stable through subsequent wait. Test slow native write cancellation retains writer until physically settled. Race gate closure against new tool call and require prepared-only publication to keep tools and MCP session admission closed. Actual stage reopening after accepted point is verified in Task4, where the point transaction is implemented. Test cumulative120 deadline with original retained teardown task and isolated deadline behavior; no per-phase reset.
```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c08_writer_quiescence.py tests/fleet/test_c06_remote_agent_runtime.py -vv
```
Expected actual process receipts/census/physical status, no seeded stopped booleans.

### Task 4 — stock checkpoint stage boundary and final transaction participant

- [x] Add remote-only sync durability to both original astream branches, stock saver post-commit callback and artifact detection. Verify complete original checkpoint/pending rows before requesting stage. No stage callback for subgraph namespaces; no callback while saver SQL transaction/locks are held. Verify actual installed same-run writer/MCP reopening and reconnect only after the exact accepted partial point transaction; prepared-only candidate must keep admission closed. This carries the acceptance-dependent reopening test from Task3, without removing the requirement from C08.
- [x] Add generic trusted core terminal participant; prepare candidate after last duration/title/cancel checkpoint, same-session pair/core run terminal/private finishing write with before/after original fence. Cover persist_current_status/set_status_if_not_cancelled/update completion paths; avoid accepting two final outcomes. Cancellation raced at terminal lock triggers exact new preparation or explicit recovery, never old pair.
- [x] Add narrow post-pair bookkeeping and C07 identity states; final record closure+point must agree, streamseal failure uses original C07 physical stopped recovery. Generic writable operations still reject after accepted final; stage point cannot authorize terminal streamseal.
- [x] Test task/run lock contention beyond lease followed by rejection; fresh-clock after SQL flush; insert pair exception rolls entire core transition back; concurrent duplicate/conflicting requests; stale token/node/generation/owner; accepted point cannot be reassociated with another user/thread/run; stage/current head mismatch; cleanup error not silently swallowed by worker finally.
```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c08_workspace_transactions.py tests/fleet/test_c08_remote_agent_runtime.py tests/fleet/test_c07_event_transactions.py tests/fleet/test_c05_remote_agent_runtime.py -vv
```
Expected exact SQL snapshots unchanged on rejection/rollback, observed original connection/TX linkage, zero mandatory skips.

### Task 5 — owner file API, verified restore and recovery admission

- [x] Expose accepted stage/final immutable output metadata with manifest ID, checkpoint ID, partial=true unless final accepted; owner/thread auth exact, download through descriptor digest verifier. Stage partial view never changes to final in place. Do not point /mnt Local artifacts at NAS writable attempt.
- [x] Add accepted-version preparation to actual new C attempt: original checkpoint selector and source point immutable; clone all files into new owned attempt user-data, verify digests/category roots, no hardlink, no B/Local directory alias. Manifest missing/changed rejects before any Agent start grant and persists recovery state; preparation retries must reverify full accepted content.
- [x] Add shared host admission guard for Local/Fleet and state mutations: recovery task blocks run/start/resume/regenerate/update, while get/history/file download/idempotent same original run remain available. Add explicit tests for actual HTTP routes; no speculative router names.
```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c08_remote_agent_runtime.py tests/fleet/test_c08_workspace.py -vv
```
Expected real HTTP/SQL/files/process evidence for partial=true, wrong owner403/404, mismatch disallowed, no runner starts/tool replay, Local/B neighbor bytes/paths unaffected.

Task5 healthy branch and trusted source-view requirements (clarified before Task5 freeze):
- Keep all nonterminal Fleet tasks exclusive, including queued, finishing and recovery,
  independently of core terminal status or lease expiry. A Local run cannot take over
  a persisted Fleet binding. Distinguish trusted host mutations from starting Local
  execution: healthy terminal checkpoint/branch operations retain their original API.
- The original explicit branch endpoint must freeze owner-scoped parent checkpoint
  and accepted source point, persist server-owned child routing/origin, and clone C
  content into independent child-owned user-data. Never copy a Fleet parent's Local
  workspace or modify its immutable point. Source verification failure persists child
  recovery and cannot start a Runner. Client metadata cannot set or clear routing or
  authoritative branch identity; genuine server-created Local branches remain intact.
- Before a child's first publication, historical artifact reads may use only its
  exact trusted immutable source mapping, with original source point/checkpoint/
  manifest identity and explicit source-thread provenance. Arbitrary ancestor points
  are not authorized. Child publications become its default view; old explicit source
  views and their partial/final classification remain immutable.
- Default new-turn preparation preserves the original implicit/latest checkpoint
  selector while separately freezing its matching accepted workspace. A child's own
  publication supersedes origin for default execution and reads. Healthy nested
  branches verify every server-owned target/source mapping and owner without
  manufacturing a child accepted point.
- Same-Gateway store-only cache records defer to trusted SQL admission; actual Local
  executor guards and durable queued/finishing/recovery exclusion remain effective.
  A cancelled branch preparation retains ownership until its copy and cleanup settle,
  then propagates cancellation; a detached cleanup callback is not settlement proof.
- Native actual HTTP/SQL/files prove these paths. Task6 additionally proves original
  installed new-C and branch runtime materialization, current-run private stamp and
  legal new input execution. Native preparation does not establish full Runner or
  C09 new-session reconciliation success.

### Task 6 — fresh installed image and whole acceptance

- [x] Adapt actual build_c07_runner_image into build_c08_runner_image including new C08 fixture and actual changed packaged guides. Freeze all changed source/build inputs, inventory six wheel payloads and entrypoints; rebuild provider/runner bytes, leave original B image unchanged. Compare installed complete bytes against frozen wheel/source for each actual process.
```bash
PYTHONPATH=. uv run python tests/fleet/build_c08_runner_image.py --context /private/tmp/deerflow-c08-runner-build --tag deerflow-c08-runner:local
# Resolve the freshly built immutable image ID before executing the required gate.
FLEET_TEST_CONTAINERS=1 FLEET_AGENT_TEST_IMAGE="$C08_RUNNER_IMAGE_ID" C08_LINUX_EVIDENCE_DIR=../.local/fleet-evidence/c08-runtime PYTHONPATH=. uv run pytest tests/fleet/test_c08_stock_linux_workspace.py tests/fleet/test_c08_remote_agent_runtime.py -vv
```
The installed test file and environment names above reflect the actual Task6 fixtures. Required C07 historical producer scenarios additionally use the independently frozen BarrierModel variant via `FLEET_C07_BARRIER_TEST_IMAGE`; existing C07 stock-image cases use `FLEET_C07_TEST_IMAGE`. These are planned commands, not execution evidence.
A unique build context is required for each rebuilt freeze; helper must fail if an old context exists. Record actual image ID/source/wheel fingerprints. Do not reuse C07 image as C08 proof.
- [x] Run native/installed normal, paused, failed, real preseal kill, postcommit response loss, Node/Gateway restart, conflicting concurrent requests and manifest deletion/digest mutation. Keep actual files/canonical manifests/SQL row snapshots/HTTP captures/PID+waitpid or Docker physical stopped observations. Derived facts: published_partial_marked from HTTP metadata and checkpoint-bound SQL row; mismatched_restore_allowed from actual rejected clone/start and preserved recovery rows; automatic_replay_count from observed new runner starts/tool invocation receipts after restart, zero. Never assign these facts from expected values.
- [x] Root regression suite with required PG/image cases zero skip:
```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c08_*.py -vv
PYTHONPATH=. uv run pytest tests/fleet/test_c07_*.py tests/fleet/test_c06*.py tests/fleet/test_c05_remote_agent_runtime.py tests/fleet/test_c04_remote_agent_runner.py tests/fleet/test_c0[123]_remote_agent_admission.py -q
PYTHONPATH=. uv run pytest tests/fleet/test_b*.py -q
make test
make test-blocking-io
# Format only actually changed files; make lint below includes whole format --check.
make lint
git diff --check
```
Supply established original B image env and new C image env from actual tests; do not count environment skips as pass. Include harness boundary/checkpoint/scheduler/memory/extensions/subagent/Local shell neighbors called out by accepted C06/C07 evidence. Commands above are planned and were not run by this delegate.
- [x] Root independent source/spec/quality review whole slice; freeze/source input checks before and after every gate. Acceptance docs record actual counts/failures/skips, IDs and baseline-safe RED locations. Guidance validator uses existing budgets (backend28672, gateway40960), no budget increase. Strict OpenSpec validation uses existing change command; inspect installed executable before recording actual command.
- [x] Root explicit tracked file add and one accepted slice commit: `feat(fleet): c08 实现 C workspace 和 checkpoint 联合恢复点`. Do not make incremental implementation commits contrary to user's one accepted slice convention. Keep OpenSpec8.1-8.4 unchecked until Root accepts actual final source/runtime evidence. No activation or C09/BC work in this commit.


Root final acceptance — 2026-10-06:

- Final formatting supplement removes only35 f0009 SQL EOL spaces; narrow SPEC→QUALITY
  passes, original actual PG2 passes. Current stockd3b529/inventorye207 and Bfb884
  pass full byte checks, not new main6/B12/native24 execution; old main IDs remain exact.
  Immutable SOURCE14/RUNTIME4 plus sparse supplement v2 record this final scope.

- [C08 acceptance](../../ecs-fleet-c08-acceptance.md) records SOURCEv14, immutable
  supplemental RUNTIMEv4, fresh final SPEC→QUALITY and Root independent bindings.
  Historical full gates retain original IDs/bytes; current one-method ownership
  fix has native24 plus original stock6 and actual B12 Compose image1 closure.
- The old B image was absent before cleanup. Verified restored b4bf plus one
  necessary current COPY layer preserves all55 source/dependency/CLI contracts;
  no B job execution logic changes. Barrier/provider remain historical after
  this preparation-only change; actual selected SOURCEv14 main processes use stock20ea.
- Current runtime/observer/build ownership is settled, current residuals0;
  18 obsolete images removed without force/prune or container/volume deletion.
- Original commands above remain planned examples, not proof they were executed
  verbatim. Exact commands/counts/times/review hashes are in retained receipts.
- Documentation-only acceptance annotations are separately hashed, with no wheel
  input or installed target changes. The accepted slice is committed once;
  C09/BC/activation stay outside this C08 commit.

Root Task6 validation refinements — 2026-10-04:
- Automatic approval rejected broad `make format`: its actual recipe runs
  `ruff check . --fix` then `ruff format .`, allowing changes beyond Task6.
  Use narrow formatting for actually changed files and complete read-only
  `make lint` (whole Ruff check plus whole format --check) instead. Record exact
  executed commands; do not claim `make format` ran. No existing guide budget
  or test assertion is weakened by this routine validation substitution.
- The historical two native C08 producer fixtures predate the Linux Node sealing
  protocol. Native path relocation does not supply an actual Linux collector or
  publication consumer. Migrate those positive runtime scenarios to required
  fresh installed Linux execution, preserving accepted partial HTTP readability
  before the next original tool, later immutable reads, same physical runner
  kill, authenticated stopped/recovery and no automatic replay. Retain their
  historical errors separately. Do not fake a native collector, disable the
  production participant, or use skip/xfail as acceptance. Native-only source
  checks explicitly exclude installed scenarios; final required installed gates
  execute them with zero environment skips.
- Probe existing C07 producer regressions before source freeze; preserve accepted
  C07 behavior on the supported runtime when old native fixtures require the
  new C08 terminal pair. SQL/unit/non-runner coverage remains native.
- Run sequential, manifest-mutation and causal installed faults in both full and
  delta checkpoint modes. Logical service reconstruction is labelled as such;
  it is not an OS process restart claim.

Root Task6 finishing read integration correction — 2026-10-04:
Focused original paired-terminal tests establish accepted final pointers,
finishing task/placement and held original capacity; the existing reader rejects
their identity. Retained RED7 is an actual assertion failure window, not a
missing dependency or fixture setup error. Correct events.py read integration
with a shared correlated finishing predicate in identity, query-time pointer
validation and original read/publisher candidate guards where necessary. Require
the exact accepted final/paused point, both authoritative pointers, original
run/attempt/generation/frozen LaunchSpec, accepted request/candidate and current-run
root checkpoint/stamp; original attempt remains starting/running. This permits
only reading already committed history/seal while finishing. It grants no new
write authority, releases no reservation and does not widen recovery_required
event-read semantics or existing active/accepted/unknown behavior. Cover original
record and hydrated record, explicit pre-yield expected_seq authority after pair
or mapping mutation, no END before seal and retained capacity after valid seal.
Freeze the intentional production delta and rerun whole source/native gates,
independent source SPEC then QUALITY, fresh installed image and runtime gates.
Earlier recorded Task5 production invariance remains a historical observation;
it does not describe the source after this evidence-backed correction.

Root Task6 finishing seal query-time refinement — 2026-10-04:
Unapplied proposal v2 failed independent SPEC: seal identity preflight is not
query-time seal authority. The actual awaited seal SELECT must also require the
exact finishing predicate and selected frozen LaunchSpec digest matching the
original seal/identity digest. Preserve existing non-finishing semantics. Native
owned-PG legacy-active-to-invalid-finishing interleavings prove three baseline
seal leaks (task pointer, frozen attempt spec and exact root stamp); each observes
the actual original identity, one mutated row and returned seal1. They do not
prove physical stop or proposed-code GREEN. Accepted request outcome rewrites
are blocked by the original DB trigger and are a separate immutability negative,
not a reachable reader race. Initial RED7 proves the identity gap; its five
negative cases never reached their mutations. V3 proposal passes bounded
independent SPEC then QUALITY. Human explicitly approved exact v3 application
and continued verification with “同意修改” on 2026-10-04, resolving the earlier
automatic-review authorization rejection. Full SOURCE/build/runtime gates
remain required after application; no acceptance checkbox follows from review.

Root C07 stream-seal fault compatibility refinement — 2026-10-04:
Corrected after independently reading workspace.after_transition,
events.writer_seal and ownership.stopped/recover_locked: core terminal and the
accepted final workspace/checkpoint pair commit together, but writer_seal uses
a separate transaction. A real stream-seal insert failure can therefore leave
durable core success, an exact accepted final pair, task/placement finishing and
no stream seal. Preserve the historical C07 success/no-seal assertion and hold
the original NodeDaemon stopped RPC until actual physical exit is observed.
Before acknowledgment, assert both final pointers and exact checkpoint binding;
after dropping the fault trigger and releasing that original stopped RPC,
physical-stop recovery may produce success seal/END only for that accepted pair.
Assert the accepted point is unchanged, reservation is released, prior event tail
remains readable through cached/hydrated reads, and no extra tool/start occurs.
Unpaired success remains recovery/no END under the separate C08 negative tests.
The earlier inference that stream-seal insertion shares the terminal transaction
is withdrawn; it was not a production contract change or an accepted test change.

## Self-review and Root decision points

This draft covers stage/success/pause/failure/kill/restart/repeat pair semantics, source integration, writer settlement, mount isolation, owner-visible immutable partial outputs, fresh-lock/rollback/digest tests, Local/B neighbors and installed byte proof. It deliberately labels all new APIs/rows as proposed. No nonexistent API is asserted to exist and no runtime acceptance is claimed.

Root must close five exact design decisions before production edits: actual Linux child/cgroup identity census and unsupported profile policy; C-only Node request polling/ack transport using existing node bearer; core terminal participant/cancellation ordering after final checkpoint; post-pair exact C07/scheduler/observer authority without reopening writers; shared Local/Fleet recovery admission guard. These are necessary source gaps, not reasons to ask the user to approve the already accepted route again.

## Root refinements incorporated after draft source review

Root confirmed the staged Node protocol direction and explicit C manifest API scope. Use a separate C owner/thread/version manifest route; preserve original Fleet Job route/row shape and normal Local artifact routes. C11 UI summary is outside C08. Partial response includes partial=true plus nonsecret run/attempt/checkpoint summary; no raw tokens/private launch payload. A partial accepted point is read-visible only and cannot automatically resume or imply final completion.

Publication identity must not be derived from artifacts reducer list delta: the reducer deduplicates the same path, so a later real present_files tool call of an updated file must publish a new version. Extract the actual checkpointed root ToolMessage/tool_call_id and corresponding original present_files tool turn, or use a nonpublic trusted tool-execution observation carried to next root checkpoint. Derive stable request key from original run/attempt/checkpoint/published tool-turn identity; same tool-turn retry is idempotent, new turn same path is new version. Do not trust client config or stream values for this identity.

Control protocol detail:
- Publication request stores sealing claim nonce/lease and writer barrier epoch, original launch payload digest and exact root checkpoint. Add state sealing between requested and prepared. Node claims only requests on its current original accepted attempt/session/token; claim expiry cannot exceed attempt/execution deadline. Duplicate identical claim/read returns original request; stale claim ack cannot replace prepared/accepted candidate.
- Node polling uses original C renew/watchdog loop; while runner awaits stage/final seal, renew keeps attempt/run shared lease active. Continued waiting never ignores stop=true/cancel/lease deadline. Publication claim timeout is independent transport accounting bounded by original execution deadline; it never resets or extends attempt lease.
- Repeat ack after lost response returns the same candidate ID/digest if request+claim+content match. Different candidate bytes/digest/owner/checkpoint reject; an existing prepared candidate is reverified descriptor-wise instead of being recopied over. Ack does not accept final point or terminalize run. If SQL preparation rolls back, NAS candidate remains unaccepted, and retry can prove exact candidate identity.
- Gateway restart reloads durable requests/candidates; Node restart uses existing bootstrap residual physical stop first. Neither replays tool nor promotes a stage candidate to final. If original runner is gone before final pair acceptance, recovery_required wins even if candidate exists. If final pair already accepted, duplicate request/ack/stop must preserve it.
- Stage request/claim timeout closes execution safely with recovery_required rather than skip phase publication and continue. Final request wait is included in original teardown's one cumulative120 budget; original isolated deadline handling provides physical exit proof. Successful ordinary graph execution/partial publication must not prematurely start the final120 cleanup clock. Stage phases use bounded original task/execution deadline and unchanged watchdog; Root must explicitly test a long run after an early partial publication to prevent accidental early budget consumption.
- Original executor is attached by manager.attach_existing_executor at manager.py:840 and sets record.store_only=False near848. For terminal preparation, require that real attached record, original matching owner and record.finalizing=True; never prepare from Gateway store-only observation. RunManager.set_finalizing near1104 is in-process; it cannot alone block peer admission. Exact core writer participant and private recovery admission guard provide durable peer protection.
- Worker cancellation first can call _finish_cancellation near667 and write terminal through manager before generic finally. Audit every terminal_status_kwargs path when adding participant: success finalization only is insufficient. All first terminal transitions of started C execution must use prepared matching pair or typed unsafe recovery; cancellation action changes checkpoint/final outcome invalidate old prepared request. Early failure before first graph/checkpoint has no accepted output pair, becomes explicit recovery or verified never-started classification, never fake empty successful pair.

Existing strong process negative control: tests/fleet/c04_tool_probe.py directly launches a same-UID same-cgroup background child using subprocess.Popen(...start_new_session=True); every20ms it appends .ticks. It escapes parent shell PG. test_c04_remote_agent_runner.py:389-395 actually compares cgroup/uid and proves final ticks stop after actual daemon stop. Reuse this actual tool probe at C08 partial/final preseal barriers and add PID namespace proof; require ticks stop BEFORE copy. Do not alter historical C04 expected behavior to mask defects. Local/macOS probe branch does not spawn Linux child; unbound Local background shell semantics remain as originally implemented. Physical process containment proves escaped PG children; arbitrary plugin Python writer threads are not automatically trackable, so unsupported remote plugin/provider profiles must reject startup before side effects.

Required additional test names in planned C08 files: test_same_path_new_present_files_turn_publishes_new_version; test_ack_lost_response_reuses_original_candidate; test_server_restart_does_not_accept_unmatched_final_candidate; test_stage_timeout_does_not_resume_graph; test_partial_does_not_start_final_cleanup_clock; test_real_attached_record_required_for_final_pair; test_cancellation_checkpoint_race_never_accepts_old_pair; test_real_escaped_process_group_stops_before_seal. These tests use actual original runtime, clock/lock waits, Node HTTP and Linux writer receipts rather than preseeded successful facts.


## Root final terminal-state choice — authoritative over earlier generic wording

Accepting a final pair is not physical process completion. In the original core terminal writer TX, accept immutable final pair + core terminal result + task/placement `finishing`. `finishing` is a new private CHECK state, included in AGENT_TASK_ACTIVE; resource reservation remains active. The point records immutable desired task/placement terminal outcomes. Node actual stopped transaction verifies exact final point/original process identity and then applies those outcomes and releases physical capacity. This prevents a new Local or C run racing scheduler/thread/observer tail callbacks of a still-alive runner. Shared trusted admission/state-mutation guard locks and rejects finishing/recovery_required/other retained task states while permitting idempotent original-run read/reuse.

Only exact accepted final point + finishing + same original attempt grants existing narrow terminal bookkeeping/renew/streamseal authority; no checkpoint or events.* terminal writing. For graph pause, use existing per-run interrupted -> placement/attempt cancelled physical-stop semantics, storing task desired input_required/paused separately. Avoid introducing paused Attempt states/new cancel or user resume endpoints in C08. The final point's desired outcome is immutable and duplicate physical-stop calls never regress it. Add tests `test_finishing_pair_blocks_peer_local_admission_until_physical_stop`, `test_finishing_pair_blocks_peer_state_mutation`, `test_stop_applies_only_exact_final_pair_outcome`, and `test_graph_pause_point_preserves_existing_attempt_cancelled_semantics`.


## Root implementation decisions — 2026-10-03

This section resolves the five source gaps listed above and is authoritative over
older draft alternatives. Pre-slice HEAD is `3d920f2df47d2bfdbb71d775671e450c07c410fc`.
The C07 commit contains43explicitfiles; postcommit Root verified clean status and
unchanged production/tests/packagedresources/18inputs against acceptedv9.
C08 OpenSpec8 remains unchecked. At planning time no C08 behavior test/build/migration had run; executed evidence is recorded below.

### Complete boundary order

1. Private presentation observation is installed on the original `present_files`
   implementation, only when a host-bound workspace participant exists. Record
   actual successful tool execution identity from original ExecutionInfo, not
   client ToolMessage text, streamed values or artifact-list delta. Observation
   authorizes no file read or pair by itself. Subagent observations retain parent
   original run authority and wait for the next committed root superstep. Repeated
   observation of the same execution is idempotent; a new turn presenting the
   same path remains a new publication. Earlier run history is not a new tool
   execution. Only validate/output-approved paths enter the candidate.
2. Optional postcommit observer wraps stock fenced saver writes outside the
   original saver lock/transaction. Remote context selects sync durability in
   both existing astream branches, without accepting a client override. If the
   checkpoint cannot be positively bound to settled original tool executions,
   reject publication and stop progression; do not select a checkpoint by time.
   Nonroot checkpoint writes retain their existing protection and are not
   independent C workspace acceptance points.
3. The private writer controller closes new filesystem-call admission under its
   actual lock, positively joins retained in-flight native calls and owns the
   processes launched by the original sandbox. Finalization uses the original
   cleanup budget object; a partial publication uses original execution/transport
   limits without starting that final cleanup clock. The controller never treats
   cancellation of an await as completion of its native call.
4. The original controller first stops and positively joins its registered
   tool supervisors through original private pipes, including adopted setsid
   descendants. Trusted Node independently verifies exact container ID/StartedAt,
   image, original labels/profile, private namespaces and accepted sole mount,
   then uses a fixed different-UID helper for a bounded zero-child census before
   and after copy. Only PID1 and that actual helper may remain. Extra processes
   reject; Node does not signal user writers or kill unknown services. Use actual
   PID/PPID/start ticks and fixed-container namespace authority, with no name/path
   whitelist. Original c04 escaped writers must physically stop before copying.
   The runner alone cannot grant itself a successful census receipt. Unadapted
   remote plugin/provider filesystem writers reject preflight before graph/tool
   side effects. Local unscoped sandbox behavior stays unchanged.
5. Store a private owner-fenced request, authentic Node poll/claim and immutable
   candidate acknowledgement. File copy runs outside SQL execution locks, to a
   C sealed namespace outside the entire runner-mounted attempt root. The
   candidate is content-addressed, descriptor-safe and reverified on retry.
   Original runner accepts partial point only after exact checkpoint/candidate/
   barrier-epoch revalidation; no next tool step before this acknowledgement.
   Partial points expose immutable files but never grant restore/completion.
6. After all original final title/duration/rollback/cancellation checkpoint writes,
   final preparation permanently closes filewriters and awaits final candidate
   using the same cleanup budget. The generic terminal participant runs on the
   actual core writer Session, checks the original/latest root checkpoint and
   prepared candidate, inserts immutable final point and updates private
   task/placement to finishing together with core terminal status. Before/after
   SQL fences and lock-wait fresh clocks remain mandatory. No filesystem copy
   or separate core commit is allowed in this transaction.
7. Cancellation can invoke early terminal paths before generic finally. Audit
   `_finish_cancellation`, `persist_current_status`, `set_status_if_not_cancelled`,
   RunRepository.update_status/finalize_if_not_cancelled/update_run_completion.
   A started execution must prepare a matching point or enter typed unsafe
   recovery; it cannot bypass the participant. Cancellation changing the last
   title/checkpoint invalidates a previously staged final candidate. A proven
   never-executed admission may use an explicit no-work classification; it must
   not synthesize a successful empty checkpoint or manifest. Prove the class
   from original lifecycle/start/filewriter records, not a caller boolean.
8. The narrow accepted-final-point branch preserves original identity, launch,
   run/attempt lease and deadlines for existing terminal bookkeeping/renew/seal.
   Task/placement finishing remains exclusive until authentic physical stop;
   checkpoint/events.* writes never gain this branch. Post-terminal filesystem
   calls reject. Scheduler/observer adapters must positively declare/verify their
   supported non-filewriter contract; unsupported profiles reject at startup.
9. The trusted stopped transaction applies immutable target outcomes only for
   an exact accepted final point and accepted physical-stop identity. Otherwise
   it records recovery_required and never constructs a pair from the last
   checkpoint. A physical stop may release physical units under the existing
   proof rules; it does not clear unsafe thread recovery exclusivity. C07 replay
   remains readable for retained matching frames, but unmatched recovery cannot
   synthesize END.

### Shared admission and restore

Bind a neutral repository thread-operation guard from the host Fleet adapter.
Run it on the original admission/mutation transaction before core run row locks,
using the existing owner/thread advisory serialization and parent-first order.
It covers Local/Fleet run, start/resume/regenerate and state/file mutations.
Finishing, recovery_required and reserved human-input tasks block unrelated
operations while read/history/immutable file access and exact idempotent reuse
remain available. Test both callback-enabled ready Fleet with new admission
flags closed and startup persistence after process reconstruction. If Fleet is
removed/unavailable, retained reservation must fail closed rather than silently
becoming permission for Local checkpoint mutation; add a trusted neutral durable
reservation marker/read guard or fail readiness for retained Fleet reservations.
No client may set, clear or reuse that authority marker.

Accepted-version preparation uses an explicit trusted point selector and exact
root checkpoint; the worker verifies every manifest/file on every retry before
original Agent start authorization. New attempt data is copied, not hardlinked
or aliased; original inputs, Local directories and B outputs remain distinct.
Missing bytes/changed digest/head mismatch persists recovery_required and starts
no Agent. This creates no public resume/routing/C09 recovery-approval endpoint.

### Evidence and authorization

Root already owns and freshly probed local PostgreSQL16.12 PID21606/port15436
(databasefleet_c07, rolefleet_test) and Redis8.6 PID36597/port58929; details are
ignored at `.local/fleet-evidence/c08-planning/root-runtime-preflight.json`.
Runtime operations are serialized by Root. Every write/test/build in this
checkout uses require_escalated with an explicit Chinese C08 authorization
justification; the default cwd is another worktree and must not be used.
No broad schema/container/key cleanup. Source-audit and earlier draft remain
ignored under `.local/fleet-evidence/c08-planning/`. Exact actual test commands,
counts and failure classifications follow executed evidence; listed commands
are future plans and never claimed as already passed.

One accepted C08 slice commit follows sequential whole SOURCE and runtime SPEC
then QUALITY and independent Root whole verification. Shared Root progress and
OpenSpec8 task completion update only after actual acceptance. No C09-C12/BC
implementation or user-facing remote activation belongs to this slice.


### Durable reservation decision

Use a neutral server-owned reservation marker in original core run kwargs, bound
to the same original external execution identity; do not add a core schema column
or import Fleet into harness. A marker is accepted only from a trusted execution
plan/repository participant; public kwargs/config/metadata cannot set or clear it.
The original admission inserts it atomically with the Fleet task/placement.
Keep it active through finishing, unsafe recovery and reserved human-input state.
Only trusted exact-point physical-stop transitions may clear a completed task's
marker; unsafe recovery remains reserved even after physical capacity is freed.

The core's neutral default thread-operation check recognizes persisted external
reservations even without the optional plugin. With Fleet ready, the host guard
additionally locks the exact active private task first and validates accepted
version/operation authorization before core run locks. Both Local and Fleet
admission share neutral owner/thread PostgreSQL advisory serialization; retain
the private parent-first order and test absent-task simultaneous admissions.
Marker reads do not take a core-row lock ahead of the private parent task.
Host startup backfills legacy matching active Fleet reservations under parent
locks before serving admission, without editing core metadata or adding host
ORM tables to Fleet migrations. A missing/unavailable adapter cannot treat an
active persisted marker as permission for a Local run or checkpoint edit.

A normal completed C run can serve as an explicit source point for a later
trusted new C task on the same owned thread, once actual stop has released
completion exclusivity. The new attempt verifies/clones the exact accepted
version and checkpoint under its own identity. Human-input/unsafe tasks remain
reserved; C09/BC supplies later authorized resume/continuation policy. C08 does
not fabricate that public policy or silently bypass current task reservation.

Private publication intent comes from successful original tool execution
observation, with complete current-run ExecutionInfo and normalized output
paths. A committed root checkpoint consumes only the observed pending intents
for that actual superstep, including settled subagent results. It never
republishes checkpoint history solely because an old ToolMessage is present.
Persist stable publication identity once per actual tool turn; subsequent root
checkpoints carrying the same history and metadata-only duration writes do not
create another phase version. Tests must cover copied prior-run history and
same-path new turn as well as checkpoint/response retry.


## Task 1 baseline evidence — 2026-10-03

On C07 HEAD `3d920f2df47d2bfdbb71d775671e450c07c410fc`, the final same-source
native fixture produced two expected behavior failures, zero errors/skips in
9.28 seconds (`.local/fleet-evidence/c08-runtime/red-final-v2.xml` and log).
Real original write_file and present_files completed; their ToolMessage was
independently observed in the committed stock root checkpoint. The existing
owner artifact route returned404 despite real output bytes. The second case
killed only its owned native runner, waited physical exit(-9), used the existing
authenticated stopped HTTP with wire137, and rebuilt the actual FleetService;
private placement remained unknown instead of required recovery_required.
Original tool identities stayed equal before/after, start counts2→2, runner
starts1. Native results do not establish Linux containment or preseal quiescence.

Task1 receipt and fixture-install source hashes are under the same ignored
evidence directory. Setup failures (negative native exit code at HTTP validation,
missing fixture distribution, MCP module path and incomplete tool observer) are
preserved separately and do not count as behavior RED. The actual previously
installed C04 fixture package plus distribution metadata was copied into an
owned C08 extra path; the host environment was not modified and no image built.

Root independently checked raw XML, current fixture/test SHA and actual tool
identity/count evidence. Independent Task1 SPEC passed without blocking issues;
Independent Task1 QUALITY passed without blocking issues. Root static ruff check,
format check and diff check also passed. Task2 sole implementation is now authorized
for private schema/storage and targeted native tests; no container/image build.
Future GREEN must assert real partial=true metadata and exact owner/run/attempt/
checkpoint/immutable manifest links, immutable explicit C-version URL after
later writes, no final accepted point on kill and actual Linux writer evidence.
OpenSpec8 remains unchecked pending complete C08 acceptance.


## Task 2 dispatch — 2026-10-03

After sequential Task1 SPEC → QUALITY PASS, Root dispatched one fresh implementer
for f0009/private model schema, C-only descriptor snapshot/verified clone storage
and targeted real PostgreSQL/filesystem tests. Native runtime is exclusively
assigned to that implementer; no concurrent Root database tests. Core runtime,
Node protocol/lifecycle, C07 events, owner API and terminal transaction wiring
remain later tasks. No commit or OpenSpec8 completion is authorized before whole
C08 acceptance. Migration test inventory changes must preserve existing B
isolation/concurrency semantics.


## Task 2 first implementation review — 2026-10-03

First frozen foundation: five production files plus six tests (including minimal
B01/B02/B03/C01 migration expectations), eleven source hashes independently
matched by Root and SPEC. Actual unified report:78 passes, no failures/errors/
skips,47.070 seconds in XML and47.17 in pytest log, including real isolated
manager installation/native build, PostgreSQL upgrade/restart/autogeneration
and original B workspace neighbors. Earlier raw reports remain historical;
initial missing API setup failures do not establish filesystem behavior and
initial RED had no pre-recorded source freeze. Actual partial-final pointer and
earlier-entry/later-category replacement defects had separate RED then fixes.

SPEC passed. QUALITY found one P2: seal could publish an oversized (>2MiB)
manifest and irreversible request marker before its reader rejected it. The
producer must check metadata size before writing/publication. Root dispatched
the same sole implementer for actual long-path/zero-byte-file RED, early-bound
fix, whole targeted GREEN and a new source freeze, followed by SPEC → QUALITY
re-review. Task2 remains unaccepted; Task3 remains undispatched. OpenSpec8
remains unchecked. No C08 image has been built or whole runtime accepted.


## Task 2 foundation accepted; Task 3 dispatched — 2026-10-03

QUALITY P2 was reproduced using a real descriptor-only long-path tree:3,000
generated zero-byte files plus the original7-byte fixture output, all below
entry/path/data limits but metadata >2MiB. The observed failure was published
version/request marker remaining after rejection. The early length check fixes
this before any metadata write/publication. Negative input now leaves the
attempt publication directory empty; a valid >85% budget snapshot verifies
and restores twice. The preliminary error-message-regex mismatch is retained
separately and is not the behavior RED.

Final report `c08-task2/green-metadata-final.xml`:80 passed, zero failures/
errors/skips,41.742 seconds XML /41.84 pytest log. Root independently matched
all eleven final hashes and confirmed only the storage/test pair changed from
the previous78-case freeze. Sequential revised SPEC → QUALITY passed without
blocking issues. This accepts only Task2 private schema/storage foundation.
No C08 Node protocol, original checkpoint pairing, file API, Linux writer proof
or full runtime is accepted yet; OpenSpec8 remains unchecked.

One fresh Task3 implementer is now assigned optional C-only real writer scope,
trusted Node staged request/candidate protocol and focused native tests. Native
resources are exclusively assigned to it. Fresh development-image/Linux
preseal proof requires a later Root-coordinated window after stable SOURCE
SPEC → QUALITY. The existing cumulative120 cleanup budget is unchanged and
partial publication must never call its auto-starting deadline/remaining getters.


## Task 3 process ownership decision and restart continuation — 2026-10-03

The user authorized continuation after the interrupted system restart. The
feature checkout retains C07 HEAD and uncommitted C08 work. The same Task3 sole
implementer owns the native test window; no concurrent Root database tests or
Docker build is running. Actual native PostgreSQL remains available on15436.
The original cumulative monotonic final cleanup budget remains120 seconds; a
partial seal must never read/start that budget.

A C-only durable fleet_workspace_processes registry is added to the still
uncommitted f0009 migration and matching private metadata. Its identity includes
the full original attempt/run/task/generation/owner/node/session/launch fence,
PID plus proc start ticks, actual tool execution identity, trusted installed
source digest, startup nonce and registration time. Evidence is immutable; only
controlled registered-to-settled transitions are allowed. Registration is
owner-fenced and bounded by the frozen profile pids_limit. Migration head and
file count remain f0009 and9; installation/table inventory and private
autogeneration tests must include the additional table while retaining earlier
upgrade starting points. Task2 accepted evidence remains a historical foundation
receipt, not acceptance of the new Task3 schema revision.

Each original C Linux command uses a lightweight trusted per-tool subreaper
supervisor. Its actual Popen handle, PID/start ticks and installed-source nonce
are registered before a private OS-pipe startup gate releases the original
shell arguments. The supervisor adopts escaped setsid/double-fork descendants
and stays alive until they physically settle. This preserves ancestry after
the foreground command returns. The runner being PID1/subreaper alone cannot
distinguish orphan tool descendants from unknown services. Original Local
command/environment/output/timeout behavior and the stock Agent graph remain
in place; unbound Local execution keeps its current behavior.

Trusted Node independently verifies exact Docker ID/StartedAt, immutable grant
labels and PID namespace before and after the census. The original producer
first positively stops and joins its registered supervisors through their
original private OS pipes. Node then proves that no child remains; it does not
signal unknown services or user writers. This ordering supersedes the earlier
Node-side descendant signalling proposal. Unknown children reject sealing.
There is no host-wide kill, name-only allowlist or caller-supplied PID authority.
The collector itself requires its actual trusted execution identity.

The original C MCP pool must positively close/join before partial sealing. It
may reconnect for the next tool only under a trusted installed stateless/
reconnectable contract, not a client config declaration. The actual C04 MCP and
plugin/two-model scenarios remain required. Unsupported containment must reject
a seal rather than reinterpret native macOS evidence as Linux proof.

Current native evidence is incremental: writer-host checks cover cancellation
and physical writer settlement. Historical cleanup-neighbors.xml contains
24 C06 passes/zero skips. The separate activity-green.xml contains27 cases:
24 passes and3 platform skips (one Linux proc-fd case and two Windows cases),
so it does not establish Linux containment. Root read the original XML, including
process-schema-green.xml:39 passes/zero skips in5.232s for writer plus original
workspace transactions including actual f0009 upgrade. The subsequently added
parent-identity FK and explicit NULL rejection still require a new test run and
frozen source review. Stable Task3 source requires sequential SPEC then QUALITY and a later
coordinated fresh-image Linux proof that escaped background writes stop before
copying. Task3 and OpenSpec8 remain incomplete.


## Task 3 native foundation checkpoint — 2026-10-03

Root read native-foundation-green.xml:65 cases,62 passes,zero failures/errors,
3 platform skips,12.349 seconds. The skipped cases are one Linux proc-fd test
and two Windows command tests. This combines writer/activity foundation, six
actual PostgreSQL process-identity cases, original workspace transactions and
Local/subagent neighbors. No source freeze or Linux supervisor proof is claimed.
Subsequent changes require fresh tests.

The implementation now binds the original durable_tool_invocation_id(runtime)
through a private ContextVar at the original bash tool seam. Random startup
nonce remains separate from actual graph execution identity. Missing real
ExecutionInfo rejects C process execution. The actual Popen is retained by the
original controller immediately after construction; registration failures must
retain the original owner through the same final cleanup budget. Registration
rechecks real PID/start ticks/parent and actual Popen after SQL lock waits.
Partial sealing must join original supervisor Popen and pipe-drain handles
after Node acknowledgement using its independent execution deadline before
reopening writer admission.

At this checkpoint, staged publication SQL helpers, authenticated C poll/claim/
ack routes, trusted exact-container proc census, daemon staged copying and
fresh fenced acknowledgement, installed provider/plugin/MCP contracts and the
actual partial-budget service test remain to be implemented. The private app
workspace adapter is currently only process registration. No accepted point or
core terminal transaction wiring is claimed. Task3/OpenSpec8 remain unchecked.


## Task 3 original cleanup fixture correction — 2026-10-03

A subsequent C06 cleanup run failed before native writers/services were started:
the older host fixture supplied an empty grant, and the bootstrap fixture
supplied only launch_spec. The new private process binding requires the original
frozen profile and execution deadline. The fixture finally block waited for a
writer that never started and masked the initial missing-field failure with a
timeout. The interrupted broad log is retained; it is neither a complete valid
report nor a workspace behavior RED. The single-case diagnostic is retained
separately.

The shared C05 checkpoint_owner fixture now retains the existing actual
authorize_start return value as original_grant. C06 host/bootstrap calls receive
that same grant. No second authorization, arbitrary PID/deadline default or
weakened production fence was introduced; original assertions remain intact.
Root inspected this exact test diff and read cleanup-neighbors-grant-fixture-
current.xml:24 passes/zero failures/errors/skips in9.059s (pytest log9.13s).
These two changed C05/C06 test files join the Task3 source inventory and require
shared C05 fence neighbors during verification. This corrects test setup while
retaining the actual original cleanup resource-owner/120-second contracts.

Root also read task2-schema-neighbors-current.xml:107 passes/zero failures/
errors/skips in48.038s (pytest log48.11s), and confirmed Task2 snapshot storage
SHA remains unchanged. native-foundation-final-current.xml has62 passes and
3 platform skips in9.635s (pytest log9.71s). None of these reports establish
Linux supervisor execution or the still-unimplemented staged Node protocol.


## Task 3 durable protocol foundation — 2026-10-03

Root read c05-shared-fixture-neighbors-current.xml:130 passes/zero failures/
errors/skips in23.404s (pytest log23.53s), covering the full original C05 file
after the single authorization-response retention change.

WorkspaceRequests now supplies caller-owned transaction data helpers for create,
claim and prepared-candidate storage. It does not authenticate a Node, accept
a point or mutate a core run/task/placement. Actual PostgreSQL protocol-red.xml
has one missing-protocol assertion failure/zero errors; protocol-green.xml has
one pass. protocol-restart-red.xml has two cases/one assertion failure/zero
errors; protocol-restart-green.xml has two passes/zero skips in0.960s. An actual
NAS candidate plus SQL rollback leaves request sealing and zero durable manifest
rows; original-nonce/epoch/deadline replay can prepare the same verified version.
Changed nonce, epoch or candidate descriptor is rejected. A lease renewal uses
the exact same original identity and requires fresh host ownership fencing.

These tests manually expire a short claim lease to exercise data-helper replay;
their filenames do not establish real Node restart or lost HTTP response proof.
Authenticated poll/claim/ack, descriptor verification outside SQL with fresh
post-I/O fencing, actual journal restart/lost-ack and Linux process census remain
required. Original source is not frozen or accepted at this checkpoint.


## Task 3 installed workspace contracts and legacy recovery decision — 2026-10-03

Workspace contracts use a separate read-only installed file at
/opt/deerflow/workspace-contracts.json with exact schema_version1. The original
runtime-bundle retains its exact four-key schema. The new file binds the builtin
host and selected sandbox implementation, each actual bundled plugin
name/use/distribution and exact approved MCP connection/binding. Supported
contract enums cover host-supervised writers, fenced plugins without retained
user-data writers, and stateless/reconnectable MCP servers without retained
user-data writers. Client extension JSON cannot declare this authority; missing,
extra or drifted entries reject preflight. No resolved credentials enter the
file: connections bind non-secret fields and existing approved secret references.

installed_compatibility hashes the contract file's raw bytes alongside existing
complete installed wheel/provider/plugin/snapshot hashes, and returns optional
WorkerCompatibility.workspace_contract_version1 only after strict validation.
The default None preserves parsing of older three-field compatibility data but
does not establish workspace support. New Agent image preflight and actual
Gateway Agent claim both require version1, before graph side effects. B/Local
paths remain outside this marker. Updated native fixtures are trusted test data,
not substitutes for installed-image contract/source verification.

The Agent CLI must preserve recovery of already accepted attempts even when an
old image cannot accept new claims. A trusted Agent-only optional asynchronous
compatibility_loader on NodeClient defers image preflight until the first new
claim, after original NodeDaemon bootstrap stops residual containers and reports
its existing journal. Existing compatibility-dict ABI remains. Jobs cannot use
this loader; failure never falls back to a job claim or fabricated capabilities.
Existing original renew/stopped/recovery do not gain a marker condition. No
replacement runner or graph replay is authorized by this recovery path.

Root read MCP original-owner RED1 then GREEN3 and parallel-join RED1 then
66-pass/zero-skip neighbor XML. Scope admission RED2 is retained separately; an
intermediate83-case run has one exception-type assertion mismatch and is not
GREEN. Final mcp-admission-final-green.xml has83 passes/zero failures/errors/
skips in3.129s (log3.20s). Optional scope freeze/reopen uses the exact epoch and
positive join retains original owner references across cancellation. The actual
SDK/stdio fixture closes and reconnects the same pool; this is not Node child-
zero proof. Current private MCP submit service is constructed but not started;
Task3 does not enable it. Existing inflight calls/native directory creation and
same-scope session admission still require the original writer barrier.

Supervisor launch contract RED2 then GREEN2 verifies a fixed installed stdlib
source via python -I -S and refusal to skip an unverifiable live proc entry.
Mocked reap and native startup checks do not establish Linux process settlement;
actual fresh-image containment proof and complete Node staged protocol remain
required before Task3 acceptance.


## Task 3 restart receipt and contract regression — 2026-10-03

After the user confirmed continuation following the system restart, Root
rechecked the feature checkout and the existing active goal. Task3 remains the
same sole implementer task; no new authorization, replacement runtime or
completion status was introduced. The final cumulative cleanup budget remains
120 seconds. Root uses read-only source/evidence inspection while the implementer
owns the native runtime window.

Root independently read contracts-binding-final-green.xml:34 passes, zero
failures/errors/skips,1.923s; capability-admission-neighbors.xml:74 passes, zero
failures/errors/skips,8.801s. The contract parser rejects raw secret fields,
relative commands, credential-bearing command flags and URLs before advertising
workspace capability. The recovery-before-new-claim scenario uses the original
NodeDaemon and an actual owned native child, plus fixture HTTP/container edges;
it does not establish Docker containment or installed Linux-image support.

After the contract preflight fixture evolution, cleanup-contract-neighbors.xml
has17 passes, zero failures/errors/skips,7.063s:16 original host-teardown
parameter cases plus one original stream-phase case. This is a distinct scope
from the earlier24-case cleanup report. c05-contract-neighbors.xml has130
passes, zero failures/errors/skips,22.266s. Root inspected the original C06 test
diff: it retains the actual single authorization grant and original memory
roundtrip assertions; trusted native contract fixtures do not constitute
installed compatibility proof. The complete original cleanup/budget scope is
being rerun separately.

Root also identified two source invariants to close before staged protocol
acceptance: reopening must reject retained unsettled process handles, including
concurrent join/timeout retries; positive joining must settle both actually
registered shell and supervisor rows on failure/receipt/wait interruption paths
so historical rows do not consume the frozen active-process limit. The normal
LocalSandbox foreground wait already settles its shell row; the finding concerns
retained handles whose normal wait path was interrupted. These findings were sent to the same implementer.
Task3 remains unchecked pending implementation, review and fresh Linux proof.


Root subsequently read cleanup-contract-full-neighbors.xml:25 passes, zero
failures/errors/skips,8.174s. Its scope is the original24 cleanup/budget cases
plus the original stream-phase case. The separate17-case report above remains
a narrower receipt. publication-budget-red.xml has one actual assertion failure
and zero errors/skips; publication-budget-green.xml has three passes and zero
failures/errors/skips,1.929s. The private publisher uses the original capability,
controller and retained teardown owner, checks a persisted root checkpoint and
creates a durable request under the existing guard. The partial request/wait
path leaves the original final CleanupBudget deadline unset; the subsequent
final close starts that same budget. This is request/budget evidence, not Node
copying or point acceptance. Its current blanket cancel rejection must be
reconciled with exact cancellation-final authority in Task4; Task3 does not
claim cancellation-final completion.


The same implementer addressed the retained-process findings with
process-join-red.xml:two assertion failures, zero errors/skips,0.720s, followed
by process-join-green.xml:19 passes, zero failures/errors/skips,1.683s. Root
inspected the source: a deadline-bounded join lock serializes the original
handles, shielded actual join Tasks survive awaiter cancellation, and reopen
rejects retained handles or unfinished joins. Only physically joined registered
shell/supervisor identities are settled, including interrupted normal paths.
These are native controller/source receipts; Node identity/census/copy and
Linux execution remain outstanding, and this is not formal Task3 acceptance.


## Task 3 trusted collector transport decision — 2026-10-03

Historical proposal: the same-UID protection described below is superseded by
the producer-first/different-UID decision at the end of this document. Setting
nondumpable only inside Python main does not protect inherited descriptors
between process birth and main. Its earlier source tests remain scoped evidence.

The existing AgentContainers interface uses Docker CLI and does not expose an
Engine ExecInspect ID. Root approved keeping that original transport instead
of adding a separate Engine adapter. This is an implementation decision, not
proof of containment. Node must launch the actual fixed installed stdlib
collector via its own retained CLI Popen against the previously verified
immutable container ID, with python -I -S and a read-only fixed source.
Independent before/after inspection must match exact container ID, StartedAt,
image/profile and immutable grant labels. Installed collector bytes must match
the fresh wheel/image catalog; no runner-supplied source path or hash is trusted.

Before scanning, the Linux collector must successfully set
PR_SET_DUMPABLE=0 so remaining same-UID user processes cannot impersonate the
trusted receipt through its proc file descriptors or ptrace. A private stdin
payload binds a fresh per-invocation nonce, original request and barrier epoch.
The bounded exact receipt binds that nonce plus actual getpid/start_ticks and
PID-namespace inode, checked against /proc/1. Only this original trusted exec
identity can be excluded from the scan; argv, command names or installed paths
cannot whitelist an unknown process. Unverifiable live proc entries, unknown
children/services, incomplete receipts, nonzero exit or stderr prevent sealing.
The retained CLI completion is evidence of a real trusted exec, not an
ExecInspect receipt. Actual installed Linux negative cases must prove that an
unknown service cannot escape rejection by imitating collector identity.


Root read node-auth-red.xml:one actual assertion failure, zero errors/skips,
1.757s. The new original ASGI/actual PostgreSQL test first publishes an original
checkpoint-bound request through the private publisher, then calls the missing
authenticated Node poll route. Original claim token retention uses the same
accepted claim; no second claim is created. This establishes the missing Node
HTTP protocol behavior before implementation, not Gateway restart/lost-ack or
Linux containment evidence. The same Task3 implementer continues the service.


## Task 3 authenticated Node protocol first GREEN — 2026-10-03

The exact node-auth RED cause was403 from the original strict Node path
allowlist, before the missing authenticated protocol could run, rather than a
404 router assertion. Root read node-auth-green.xml:one pass, zero failures/
errors/skips,1.814s. The original authenticated ASGI/actual PostgreSQL path now
polls and claims the same durable request, returns an identical repeated claim,
and rejects changed nonce/epoch/digest/token. Complete raw core run rows remain
unchanged and no workspace point is accepted.

Root inspected FleetWorkspaceNodeService, the narrow fleet_auth allowlist change
and original ownership installation. Prepared-candidate verification opens and
hashes NAS descriptors outside SQL locks, then reauthenticates the original Node
and shared ownership guard with a fresh clock before acknowledgement. Node admin
state and task/run cancellation predicates are checked separately; no blanket
cancelled-final exception was added. Actual prepared HTTP, rollback, lost reply,
service reconstruction and lease-expiry-during-I/O tests remain in progress.
fleet_auth.py and the new Node protocol tests join the Task3 source inventory.

process-owner-final-red.xml contains one actual assertion failure, zero errors/
skips,0.750s; process-owner-green.xml has17 passes, zero failures/errors/skips,
2.071s. An actual completed join Task with an unsettled retained handle cannot
unwind the original resource owner. Pending cleanup retries preserve that owner
and the same budget until physical join, then unwind resources while retaining
the original failure. This is native resource-owner evidence, not Linux census
or complete staged seal acceptance. Task3 remains unchecked.


## Task 3 candidate acknowledgement foundation — 2026-10-03

Root read node-ack-green.xml:four passes, zero failures/errors/skips,2.397s.
Source inspection confirms the original authenticated HTTP/actual PostgreSQL
case plus three new scenarios: an actual NAS candidate is descriptor-verified
again after rebuilding FleetWorkspaceNodeService over the same database; a real
SQL division-by-zero after prepared writes rolls back manifest rows and keeps
the request sealing with no accepted point; original run/attempt lease UPDATEs
complete under a500ms lock timeout while descriptor verification is parked,
then prepared is rejected409 after verification resumes. This establishes that
NAS verification does not retain original SQL locks and acknowledgement retakes
actual lease authority afterward.

The lost-reply-labelled service case discards a successful ASGI response and
manually retries against the rebuilt service. It proves prepared idempotency and
actual repeated verification without copying; it does not yet prove an actual
NodeClient transport ReadError, original daemon/journal replay, Docker restart or
zero repeated runner/tool starts. The same implementer was asked to close that
remaining transport/lifecycle evidence through the actual daemon wiring.


Root additionally checked the original NodeDaemon bootstrap ordering.
open_session currently precedes local residual stop. The lazy compatibility
loader fixes new-image preflight ordering but does not prove offline/revoked
credential startup can physically stop residual executions before a failed
network/authentication call. This is an explicit C09 fault integration gap,
not a reason to broaden current acceptance claims. If required by a Task3
actual restart scenario, a minimal original-lifecycle fix must be reviewed.

A real Node restart physically stops the original runner. Prepared replay must
then fail the original inactive/stopped ownership fence and retain unaccepted
evidence; it cannot relax that fence or start a replacement just to demonstrate
idempotency. A Gateway service reconstruction while the original execution is
still alive may verify and acknowledge the same candidate. These different
restart states require separate real observations, rather than relabelling a
service-instance reconstruction as full Node recovery.


## Task 3 credential freshness decision — 2026-10-03

Node publication routes pass credential_id only from the authenticated
FleetNodePrincipal; JSON bodies cannot choose it. Root approved an exact
CredentialRow lock/check within the same transaction after original shared
owner locks. Existing credential revoke locks only that credential; issue locks
node before inserting a new credential, so this preserves the lock order.
Credential node/expiry/revoked_at must be rechecked before and after NAS I/O.

A credential lock wait must be followed by a fresh shared owner/lease check,
not merely fresh credential expiry. Grant remaining durations use that
post-wait fresh clock rather than the earlier authenticate timestamp. Actual
verification-time revoke/expiry and credential-lock-wait lease-expiry scenarios
are required. No new token, header or client-controlled credential body field
is introduced. This decision does not itself establish test or acceptance
evidence; implementation continues with the same Task3 owner.


Root read node-credential-final-green.xml:seven passes, zero failures/errors/
skips,3.975s. Actual credential revoke/expiry while NAS verification is parked
returns403; an actual credential FOR UPDATE wait spanning the original short
run/attempt lease returns409. Prepared state is not committed. Source inspection
then found a remaining natural-expiry window after this initial credential
check: the second shared guard or projection SQL can wait past credential expiry,
while final validation checks only run authority. Row locking does not stop
time expiry. The same implementer is closing final before-commit credential
freshness with an actual SQL-wait regression; the seven-case report is not
acceptance of that subsequent fix.

Root also required Linux pidfd-bound signalling for known registered tool
descendants. Reading fresh start_ticks then calling os.kill still has a PID
reuse window. Open the pidfd first, recheck actual start identity/namespace,
then signal that kernel handle and close it. Disappearance is handled as
disappearance; unsupported or unverifiable permissions fail closed. Apply this
C-only identity helper to trusted supervisor/collector signalling and original
supervised-command kill, preserving ordinary Local defaults. Native mocked
pidfd cases are source/identity-contract evidence; actual installed Linux
containment remains mandatory.


The natural-expiry defect now has actual node-final-auth-red.xml:one assertion
failure, zero errors/skips,3.080s, where a real SQL wait crossed credential
expiry but claim returned200. Root inspected final _fresh_auth calls after
projection/writes and read node-final-auth-green.xml:eight passes, zero
failures/errors/skips,5.547s. Final validation retakes shared lease authority
and checks the authenticated credential against the fresh clock on that same
transaction.

collector-source-red.xml has five assertion failures, followed by
collector-source-green.xml:five passes, zero errors/skips,0.630s. The fixed
stdlib collector recognizes exact registered supervisor descendants, rejects
unknown processes before signalling, leaves supervisors to positively reap,
and requires a final census containing only PID1 plus its actual trusted exec.
pidfd-source-red.xml has four failures, followed by pidfd-source-green.xml:
23 passes, zero failures/errors/skips,0.913s, combining writer/collector native
source contracts. Root inspected the pidfd open→fresh identity→bound signal→
close logic. These are mocked native contracts, not Linux containment proof.

The collector remains unwired to actual Node execution/image at this receipt.
The installed compatibility probe must also verify actual Linux kernel
subreaper/pidfd support before advertising new-claim capability, without user
effects. It remains lazy after original journal residual stop/recovery; it
must not regress legacy recovery ordering. Task3 acceptance stays pending.


Root read node-identity-neighbors-final.xml:11 passes, zero failures/errors/
skips,6.268s, including the original B bearer boundary neighbor. The earlier
10-pass/one-failure status assertion required accepting original stale-session
409 instead of expecting403; production authentication was not relaxed.

Kernel-probe mutation must preserve the original caller state.
installed_compatibility also executes inside original runner PID1 during
build_agent_environment. Permanently setting that PID nondumpable would make
its proc namespace unverifiable to the same-UID trusted collector. Root
required saving PR_GET_DUMPABLE and PR_GET_CHILD_SUBREAPER values, exercising
subreaper/nondumpable/self-pidfd/SIG0 checks, and restoring both original flags
in finally, including failures. Trusted collector main remains permanently
nondumpable for its own lifetime. This preservation requirement was sent to
the same implementer before kernel-probe implementation acceptance.


## Task 3 installed source and image preparation receipt — 2026-10-03

Root read kernel-flags-green.xml:14 passes/zero failures/errors/skips,0.572s;
installed-kernel-green.xml:49 passes/zero failures/errors/skips,2.020s; and
collector-installed-green.xml:51 passes/zero failures/errors/skips,2.260s.
The final scopes supersede different earlier source-only subsets without
relabelling those reports. Source inspection confirms original process flag
restoration, actual installed_compatibility calling the kernel check, fixed
collector descriptor checks and byte equality to the packaged collector source.
The compatibility digest includes approved-workspace-collector in addition
to complete original distribution/provider/contract inputs. These native
fixtures explicitly mock Linux capability where necessary; they are not
installed Linux kernel evidence.

Root read image-inputs-module-red.xml:one assertion failure/zero errors/skips,
0.623s, and image-inputs-green.xml:one pass/zero failures/errors/skips,0.643s.
The earlier image-inputs-red.xml package-name setup mistake is preserved and
is not feature RED. Root inspected build_c08_runner_image.py:it prepares a
fresh context with the original C04 plugin/MCP/two-model four-key bundle,
strict separate workspace contracts, exact collector source copy and distinct
C08 dependency/provider image names. Source preparation runs no Docker/build;
no actual fresh image or installed-byte catalog has been produced at this
checkpoint. Original B/C07 immutable images remain the historical references.
Actual AgentContainers trusted exec and NodeDaemon staged poll/copy/ack wiring
remain the implementation focus. Task3 stays unchecked.


## Task 3 producer-first and collector birth isolation revision — 2026-10-03

The user again authorized continuation after reboot. The original sole Task3
implementer retains the runtime window. Task3, all C08 parent gates and the
B→C→BC goal remain incomplete. No new image or installed Linux proof is claimed.

The original producer closes admission, positively stops and joins the known
per-tool supervisors through the original private OS pipes, and joins native
writes and the permitted MCP owner before requesting Node publication. A partial
uses the original execution deadline and never starts or reads CleanupBudget.
Final publication uses the existing one cumulative monotonic120-second budget.
The same AgentRunner, graph, controller and teardown resource owner remain.

Node performs a pure zero-child census in the original exact container. Only
PID1 and the actual trusted helper may remain; any other process rejects the
candidate. Node does not kill unknown services to manufacture quiescence.
Collector proc evidence uses PID, PPID, start_ticks and state, not other
processes' namespace symlinks requiring cross-UID ptrace permission. Its own
namespace comes from the fixed exact-container exec and /proc/self/ns/pid.
PID1 UID must match the frozen workload identity.

A helper created with the workload UID has a birth window before Python main
sets PR_SET_DUMPABLE=0: a same-UID service may open and retain its inherited
stdin/stdout descriptors, steal the nonce or forge a receipt. The trusted Docker
exec therefore uses a different UID: frozen nonzero workload UID selects0:0;
UID0 selects65534:65534 as a defensive branch. The existing profile validator
continues rejecting UID0; this revision does not change profile admission policy.
The helper retains permanent nondumpable protection as a second layer.

Before and after exec, Node verifies original CID, StartedAt, image, grant labels,
frozen Config.User, CapDrop ALL, NoNewPrivileges=true, default private PID mode
and exactly the accepted original /workspace mount. External PID/user namespace
configuration and host-proc exposure reject. No privileged flag, added capability,
new Engine adapter or runner is introduced. Repeat zero-child census after copy
prevents a candidate from passing solely on a pre-copy observation.

Required next evidence is real publisher partial stop/join without final-budget
activation, source UID-selection/census contracts, and later installed Linux
negative tests for inherited-FD reads and forged receipts. Earlier same-UID or
Node-signalling reports are historical source contracts, not proof of this
revised transport. Stable Task3 still requires SPEC followed by QUALITY review.


## Task 3 producer physical-stop and Node deadline receipts — 2026-10-03

Root independently read partial-producer-process-red.xml:one failure, zero
errors/skips,1.810s; the real retained native .ticks writer was still running
when durable request creation was reached. partial-producer-process-green.xml
has18 passes, zero failures/errors/skips,2.367s (four producer cases plus14
writer cases). The actual publisher now positively stops/joins retained original
process handles before partial durable SQL. The new native child test checks
physical exit, stable later tick hash and original budget._deadline remaining
None. It uses a native original-handle interface and does not prove the Linux
subreaper or container namespace. Final pre-request positive stop remains an
explicit integration requirement; zero-child census cannot substitute for it.

Root read node-lock-bound-green.xml:12 passes, zero failures/errors/skips,7.532s.
The Node operation is bounded by the remaining original task/execution/run and
attempt lease time, capped by its original RPC allowance. A real held task-row
lock rejects within the lease deadline, and the original SQL division-by-zero
rollback negative remains. These are real HTTP/PG foundation checks, not actual
NodeDaemon transport/restart or installed Linux execution acceptance.


## Task 3 revised collector RED — 2026-10-03

Root read node-birth-zero-red.xml:seven failures, zero errors/skips,2.081s.
Five new source contracts expose the missing zero-child/UID-selection routines;
two original native exact-container exec cases expose the missing different-UID
Docker exec arguments. The source-only UID0 defensive selector case does not
alter original profile admission. These failures establish the revised collector
implementation gap, not installed Linux inherited-FD exposure proof. The same
Task3 implementer continues implementation; Root runs no concurrent native
services or image build. Earlier source/signalling receipts remain historical.


## Task 3 original journal capacity integration constraint — 2026-10-03

Root source inspection found original AttemptJournal.MAX_RECORD_BYTES is1MiB,
while accepted Task2 snapshot metadata permits legal values above85% of2MiB.
Embedding a full legal candidate manifest in the original attempt record would
reject a valid snapshot. The Node staged path must preserve the existing B
journal limit and persist a bounded exact candidate identity/version/digest
pointer, with full Task2 descriptor verification before every resend, or use
a separately bounded private C candidate record. No arbitrary/latest path or
unchecked cached candidate can authorize publication. A near-limit legal
manifest journal/retry regression is required. The original NodeDaemon loop,
watchdog and bootstrap physical stop remain the execution authority; a stopped
original runner cannot become an active prepared acknowledgement by restart.


## Task 3 revised collector source inspection and intermediate run — 2026-10-03

Root source inspection confirms collector main now performs bounded proc-stat
zero-child inventories with actual workload/helper UIDs, own namespace and
cgroup evidence. Node exec uses the fixed different-UID selector and validates
private PID/user/cgroup settings, no added caps and no privileged mode. It does
not signal descendants. node-birth-zero-green.xml is an intermediate report,
not GREEN:19 cases,18 passes/one failure, zero errors/skips,2.076s. The remaining
old request fixture lacks the new UID fields and requires an explicit update;
production request validation must remain strict.

Root also inspected original controller.close_and_wait(final=True):it already
calls the deadline-bound retained-handle stop/join. This corrects the earlier
Root concern that final producer stopping was absent. An actual final-before-SQL
fixture is still required to verify ordering and the same original budget.
No installed Linux proof, stable Task3 review or C08 acceptance is claimed.


## Task 3 revised collector GREEN and candidate recovery decision — 2026-10-03

Root read node-birth-zero-final.xml:19 passes, zero failures/errors/skips,2.058s.
The old request fixture explicitly supplies both new UID fields; production
validation remains strict and requires no registered process remains. This is
source/native fixed-exec evidence, not actual Linux inherited-FD protection.
The partial/final native child before-SQL regression is expanded in
producer-final-before-sql.xml:19 passes, zero failures/errors/skips,2.626s. The
actual final path uses its existing original
close_and_wait(final=True) positive stop/join. It does not need a replacement
controller or separate budget.

Root approved a minimal trusted AgentWorkspaceVersions.recover(identity) read
API for exact lost-response candidate recovery. It reads only the full fixed
owner/task/attempt prefix and existing exact request marker, matches request
digest, opens the fixed manifest ID through descriptors, runs original full
verify and matches full ownership/private execution digest. Only an absent
marker or not-yet-created fixed parent returns None. Symlinks, mutated metadata,
missing candidate files, nonregular entries, permission/IO errors or conflicts
reject. A pure read cannot call the mkdir-owning _parent helper. Arbitrary/latest
paths are never candidate authority.

The original attempt journal remains1MiB and stores only bounded request IDs,
request digests, epochs/nonces and exact candidate version/digest pointers. It
stores neither full projection nor full identity: presented_paths alone may
contain4096 long paths. Reload complete identity from the original owner-fenced
poll/claim and compare exact request digest/epoch/original execution before
candidate recovery. Full legal near2MiB manifests are reloaded and verified
before retry. This changes previously reviewed Task2 storage source as a Task3
recovery delta: historical Task2 acceptance remains scoped, and the new delta
requires real filesystem RED/GREEN plus stable source SPEC then QUALITY review.
The same implementer keeps sole runtime ownership. Actual NodeDaemon staged
loop, transport loss, journal retry and installed Linux proof remain pending.


## Task 3 recovery delta SPEC rejection and daemon staging evidence — 2026-10-03

The original Task2 SPEC reviewer examined the frozen three-file recovery delta
and returned FAIL/P2=1. recover catches FileNotFoundError around the entire
_read(marker); an observed marker removed after open/read also raises that
exception during post-read stat, incorrectly returning None. Only initial
marker-open ENOENT may mean absence. Root sent this exact finding to the sole
implementer for actual descriptor-read deletion-race RED/GREEN. The freeze is
withdrawn only for review repair; no QUALITY review may start until revised
SPEC passes. Historical accepted Task2 scope is not retroactively expanded.

Root verified candidate-recover-frozen.xml:33 passes, zero failures/errors/skips,
7.455s. Removing the unique added recover method reproduced accepted Task2
storage SHA139121651a923e8e56c1f06f1b28d001c7a645190d3d36354988d1187dc6520f
before the required repair. That report does not cover the missing deletion race.

Actual NodeClient/ASGI/PG lost prepared-response test is now distinct from the
earlier manually discarded response. node-stager-red.xml has one failure,
zero errors/skips,1.815s; node-stager-green.xml one pass, zero failures/errors/
skips,1.840s. A real httpx.ReadError is raised after successful PG prepared
commit; service reconstruction reloads original journal pointer and fully
recovers the same candidate, seal count remains1 and points remain0. Its four
container-census receipts are explicitly native adapters, not actual Docker.

Original NodeDaemon execute-loop staging is separately wired with the existing
renew/watchdog task and a C-only optional publication adapter; B remains unbound.
Root read node-daemon-stage-red.xml:one failure, zero errors/skips,2.221s
(original loop never called the missing stage adapter before its outer timeout).
node-daemon-stage-green.xml has15 passes, zero failures/errors/skips,14.678s:
two staging cases plus13 original B journal/shutdown/publication neighbors.
A real native Popen runs once, the stage retries after its injected transport
failure, renewal stays active and original finally physically stops the child.
Combined actual HTTP/daemon and copy-cancellation negatives remain pending;
this separate lifecycle fixture does not establish Docker containment. Root also required complete verification on
same-instance cached-candidate retry and bounded cleanup of completed copy-task
results, while retaining actual unfinished copy tasks. No stable whole Task3
review, installed Linux evidence, C08 commit or acceptance is claimed.


## Task 3 storage recovery repair accepted and retry cache GREEN — 2026-10-03

The recovery delta passed the original SPEC re-review, then original QUALITY
review, both P1=0/P2=0. All three frozen SHAs match:

- storage:3138df37c57e7c030b5adf9d6b6b5e2e716ad5459858e2cd0e800528bd939847
- recovery test:77a12d8f61ec9bee361a32656016700f071f732babe63dd971144859866b8850
- old storage tests:5df01e88e1a44fc6768057d76e904581e5f7ef56ea82f5a628235b189404addf

Root read recover-marker-race-final.xml:34 passes, zero failures/errors/skips,
7.294s. _read missing_ok handles only initial os.open ENOENT; errors after open
are never absence. The actual post-fstat/pre-named-stat marker deletion gave
recover-marker-named-race-red.xml one failure, zero errors/skips,0.597s. The
earlier deletion triggered fstat nlink protection and passed in0.595s; that
preserved report is not business RED. The accepted near-limit test explicitly
requires metadata greater than85% of2MiB, reloads exact candidate and saves only
a bounded pointer in the existing1MiB journal. This acceptance covers the three
files' recovery delta, not whole Task3, Linux containment or full C08.

Root read node-stager-cache-red.xml:three cases/two failures, zero errors/skips,
2.283s, exposing same-instance retry missing complete recovery and completed
Task results remaining retained. node-stager-cache-green.xml has three passes,
zero failures/errors/skips,2.215s. Source now releases only physically completed
copy-task ownership and retries by full fixed-marker recovery, preserving real
uncompleted tasks. These use actual NodeClient/ASGI/PG with native census
adapters. Combined daemon/HTTP, cancellation/physical copy, publication journal
write settlement and installed Linux proof remain required.

Root source inspection additionally identified publication journal saves as
actual native writes: cancellation of await to_thread(save) must not allow an
older record rename after a newer stopped/reported record. The same original
attempt owner must serialize/positively join its publication journal writes;
B's existing contract stays unchanged. A parked-save/cancel/final-save race is
required before Task3 can be accepted.


## Task 3 physical copy and publication journal owner receipts — 2026-10-03

Root read node-journal-owner-red.xml:one failure, zero errors/skips,1.502s,
exposing the missing retained serial publication journal owner. The final
node-journal-owner-green.xml has19 passes, zero failures/errors/skips,11.916s
(seven staging cases, eleven Node/auth cases and one original B route neighbor).
Source inspection confirms a C-only save_record seam deep-copies the record
and retains an actual Task whose lock spans the complete native journal save.
Daemon Agent saves use that same seam; B uses its original to_thread path.

The real parked native-save case cancels the older awaiter, verifies a newer
final save remains waiting until the first physical write completes, observes
write order [False,True] and actual disk reported=True. Staging cases include
a real cancelled copy await with retained pending copy/no prepared response;
after release the exact marker recovers without resealing. Same-instance and
rebuilt-service HTTP ReadError retries fully recover with seal count1. An
actual identity with presented_paths above1MiB still journals only the five
bounded pointer fields. Container census is native-adapted, not Docker proof.
The earlier near-budget command selected a nonexistent test, exited4 with zero
tests and is preserved as a setup mistake, not business RED. Whole Task3 still
needs remaining guard/registry/CLI negative cases, stable source review and
actual installed Linux containment evidence.


## Task 3 original CLI shutdown writer join decision — 2026-10-03

The original asyncio.run shutdown cancels pending Tasks before joining its
default executor, while CLI finally closes the client and process flock first.
Shielded phase awaiters alone do not make that resource order a positive native
writer join. Root approved C-only join_writers at the original lifecycle seam:
close new stage admission, stop/join original active executions, then positively
join the retained actual copy/save owners before releasing original flock or
client context. Original B behavior remains unchanged.

The same actual join Task must survive cancellation of its awaiter. Timeout or
a second cancelled wait cannot declare physical completion or release the old
flock while a new daemon could open/rename that same private journal. Cleanup
join grants no prepared acknowledgement, renewed claim, replacement runner or
new mutation authority. It does not start/reset Runner CleanupBudget. Once the
original execution deadline is past, any late candidate remains unaccepted and
original recovery fencing applies.

Required actual native parked-copy/parked-save shutdown evidence observes client
and flock release waiting until physical writer completion, with repeated wait
cancellation preserving the same owner. This must be wired to original
NodeDaemon.run and actual CLI final resource order, not only an isolated adapter
helper. Registry SQL waits and fixed-exec cleanup negatives remain the same
Task3 implementation window; no Docker/installed Linux claim is made here.


## Task 3 CLI shutdown, process SQL and sandbox initialization receipts — 2026-10-03

Root read node-cli-writer-shutdown-red.xml:one actual failure, zero errors/skips,
1.584s, showing original CLI closed its control client while a native journal
writer remained alive. The first green-named report is intermediate:27 cases,
25 passes/two failures, zero errors/skips,17.818s, because new optional field
access masked original B fake-daemon shutdown failures. Production now gates
that access on settings.kind=agent; B fixtures and contract remain untouched.
node-cli-writer-shutdown-final.xml has28 passes, zero failures/errors/skips,
17.097s:real parked native copy and save cases plus original shutdown/entry
neighbors. Repeated cancellation preserves the same join owner and original
flock/client until physical completion. Both original Daemon.run and CLI final
resource order are wired; cleanup grants no new publication authority/budget.

Root read process-register-deadline-actual-red.xml:one failure, zero errors/
skips,2.215s, where actual held original task-row lock outlived registration's
execution deadline with the owned Popen still alive. The first red-named report
actually passed because a wrong schema early error was caught; it is preserved
as a setup mistake, not business RED. The intermediate20-case run had three
old private fake signature failures. Final process-register-deadline-final.xml
has20 passes, zero failures/errors/skips,3.325s. Actual registration SQL is
bounded by its original execution deadline, and final settlement uses the
caller's same original120-second deadline rather than resetting it.

Root read sandbox-initialization-writer-red.xml:two failures, zero errors/skips,
1.086s, from actual mkdir before gate admission and cancelled original provider
acquire hiding a live native writer. final218 passes, zero failures/errors/
skips,2.366s combine17 writer cases and201 original Local mount/security cases.
Original Local provider acquire/Paths.ensure_thread_dirs is actually parked;
its cancelled awaiter retains the writer, a partial barrier waits for physical
mkdir/cache completion and closed sync/async acquire rejects before mkdir.
Optional nested writer scopes reuse original admission and preserve unbound
Local return ABI. The intermediate217-case run had one native test PATH issue
for shell python; no production regression RED is claimed and the old test is
unchanged. Its final runner uses the original backend venv bin in PATH.

Remaining fixed-exec receipt/owner negatives and complete source inventory are
still Task3 work. Stable whole source requires fresh SPEC then QUALITY review;
installed Linux containment and the rest of C08 remain unverified.


## Task 3 strict fixed-exec receipt foundation — 2026-10-03

Root read node-receipt-duplicate-actual-red.xml:three cases/one failure, zero
errors/skips,2.293s, exposing duplicate JSON receipt fields being accepted.
The earlier red-named report had two setup errors and zero failures and is not
business RED. The intermediate green-named report had20 cases/one failure,
zero errors/skips,2.347s:the invalid receipt was now rejected before the old
expected after-inspect count, so the test expectation needed that stronger
ordering rather than weakening production parsing.

Root read node-receipt-duplicate-final.xml:25 passes, zero failures/errors/skips,
3.344s. Source uses strict duplicate-key parsing and retains bounded stdout/
stderr/wait Tasks under original CLI execution ownership, with stop cleanup
on failure. Native exact-container and flags fixtures remain explicitly
source contracts; this report does not prove actual Docker/helper exit or
inherited-FD isolation. Complete Task3 source inventory/freeze, sequential
whole-source SPEC/QUALITY and later fresh installed Linux containment remain
pending. C08 parent gates remain unchecked.


## Task 3 restart continuation and aggregate fixture correction — 2026-10-03

Root resumed the existing B → C → BC goal after the user-authorized restart;
Task3 remains in progress, with the original sole implementer owning the native
and Docker execution window. The original cumulative monotonic final cleanup
budget remains 120 seconds; partial publication never starts that clock.

Root independently read `native-source-target-current.xml`: 415 cases, 407
PASS, 8 FAIL, zero errors or skips, XML 129.670 seconds (log 129.77 seconds). All eight failures occur in
the original C06 isolated-bootstrap child fixture before writer entry: its
independent child still supplies the old three-field compatibility and empty
runtime bundle, without the installed workspace contract. This aggregate is
not described as wholly green or a business RED.

The implementer minimally evolves that child fixture with a temporary strict
contract, exact four-key empty runtime bundle and explicit compatibility marker;
the original service, memory, graph, 0.3-second injected fault and assertions
remain. Root independently read `c06-isolated-contract-fixture-final.xml`: all
eight affected cases PASS, zero failures/errors/skips, 20.763 seconds. The 407
previous passes retain their original aggregate report; no redundant full rerun
is required solely for this fixture-only correction.

The initial static report still records three test import lint findings and one
supervisor formatting finding. Root authorizes only import ordering and removal
of unused `MAX_RECORD_BYTES` in `test_c08_workspace_recovery.py`, plus formatting
the Task3 supervisor. The previously accepted three-file recovery SHA set
remains a historical review receipt; the test-only formatting delta and new SHA
must be included explicitly in the forthcoming whole Task3 source freeze and
sequential SPEC → QUALITY review. The frozen storage implementation and original
workspace tests remain unchanged. No whole Task3 acceptance, installed Linux
proof, fresh image build or C08 parent gate is claimed by these receipts.


## Task 3 whole-source freeze submitted for SPEC — 2026-10-03

The implementer has stopped source changes and returned the native execution
window. Root independently verified all 57 current production/test file SHA
values, all 57 retained `source-freeze/backend/...` byte copies, and all 13
report SHA values against `task3-source-freeze.json`; no mismatches occurred.
The file-list inventory digest is
`11d1ce688a5853bed2872113833c9d7f3b94fed4df26ac82294ad27bd08d3e36`;
the complete JSON file digest is separately
`d39934cc4a73266781660c392ce59e5dc638a77ed39301299f73113a1c5d188e`.
Root-managed plan documents are excluded from this production source freeze.

The final static log records all checks passing and all 57 files already
formatted. The approved recovery test import-only diff is retained as
`recovery-test-import-only.diff`; its new SHA is
`86d411c8d6d13921054e512496649441a70ddcdec294fc259a11225cabb1ef8a`.
Its 34-case recovery rerun passes with no skips in 7.452 seconds. The previous
accepted storage implementation and original workspace test SHAs are unchanged.

Whole-source SPEC review is now dispatched, followed only upon PASS by QUALITY.
This source gate excludes acceptance claims for the still-missing real Linux
installed collector, escaped child containment, birth UID/FD controls and the
combined original daemon/HTTP/NAS retry path. The existing actual HTTP/PG/NAS
test and daemon/native-child test are separate scopes. Fresh six-wheel bytes
and installed image proof remain pending, as do Task4/5/6. Task3 and all C08
parent gates remain unchecked until their required runtime evidence is accepted.


## Task 3 whole-source SPEC findings under repair — 2026-10-03

The first whole-source SPEC review has established two findings; source approval
is withheld and QUALITY has not started. The original 57-file freeze is retained
as the reviewed historical snapshot, rather than overwritten by repairs.

P1: publication calls `pool.close_scope_and_join` outside the original teardown
retained phase. On timeout or caller cancellation, actual MCP owners can remain
in `_scope_closing` while original teardown only checks workspace writer owners
and `pool.close_all` only sees `_entries`/`_inflight`. The resource stack can
therefore unwind before actual MCP tasks/children settle. The same implementer
must retain the original MCP join owner and prevent stack/resource closure until
positive settlement, including cancelled/timed-out publication regression.

P2: `wait_prepared` caps its wait/SQL/process join by caller and execution
deadlines but does not cap final/paused publication by the already started
original final cleanup deadline. The repair must use the original deadline for
final boundaries without reset; partial waiting must still never start or read
the final cleanup budget. An actual final-budget exhaustion regression is
required. Neither issue authorizes weakening existing fences, declaring runtime
acceptance, or starting Task4/5/6 early. The native execution window is assigned
back to the sole original Task3 implementer for RED → repair → GREEN and a new
explicit source freeze, followed by the same SPEC reviewer re-review.


## Task 3 MCP ownership and final deadline business RED — 2026-10-03

Root independently read `source-review-mcp-budget-business-red.xml`: four
cases, four failures, zero errors/skips, XML 3.946 seconds. The implementer
reports log elapsed 4.019 seconds separately. The two MCP cases call the actual
C04 stdio SDK session and park its original owner before context-manager exit;
after partial publication cancellation/timeout, original plugin, memory and
database resource callbacks are observed before that owner physically joins.
The other two final/paused cases publish legal requests, retain an already
started original budget with about 40ms remaining, then show prepared waiting
lasts about 201ms under the caller deadline. These are real business failures
for the two SPEC findings. Earlier attempts with two invalid final metadata
fixtures remain historical setup failures and are not counted as P2 RED.

The original sole implementer is repairing the same resource teardown/pool
scope ownership and final-wait deadline paths. Partial publication remains on
its original execution deadline and cannot start the final cleanup budget.
No production budget extension or new resource owner is authorized.


## Task 3 MCP ownership and final deadline repair first GREEN — 2026-10-03

Root independently read `source-review-mcp-budget-confirmed.xml`: 28 PASS,
zero failures/errors/skips, XML 11.355 seconds (log 11.42 seconds). The repair
binds original teardown to the same pool/scope and positively joins it before
plugin, memory and resource unwind. Pending evidence includes actual detached
`_scope_closing` owners, not just wrapper task completion. Partial cancellation
and timeout retain resource ownership without starting the final cleanup clock.
Final cancellation retains the same original MCP phase task; final budget
exhaustion preserves unsettled owners without granting a new budget. Final and
paused prepared waits now cap to the same already-started final deadline.

An intermediate final-timeout failure asserted task completion immediately
after releasing the parked actual owner; the test now positively awaits that
original owner through shield. Production settlement checks were not relaxed.
Original C06 cleanup and MCP pool neighbor regressions are next, then a new
explicit source freeze and same-reviewer SPEC re-review. This first GREEN does
not override the preceding SOURCE FAIL or authorize QUALITY/runtime acceptance.


## Task 3 source v2 frozen for same-reviewer SPEC re-review — 2026-10-03

Root verified the complete new `task3-source-freeze-v2.json`: 57 current source
SHAs and 57 independent `source-freeze-v2/backend/...` byte copies all match.
Exactly four files change from v1: original `runner_context.py`, publisher
`workspace.py`, MCP `session_pool.py`, and `test_c08_workspace_protocol.py`.
The other 53 source bytes and protected storage recovery files are unchanged.
The v2 file inventory SHA is
`190d956177abce8167784ee842e43212267ca9e2b3dab422a6220a8b3cf46966`;
the complete v2 JSON SHA is
`3e2e4f0a5cda3e7f9dd3882cb503d966c80f77fe802ff69f3b07ba2ee412ad91`.
The four exact `*.source-review-v2.diff` deltas and old v1 snapshots/reports are
preserved independently.

Root checked all four repair report SHAs and actual XML results: business RED
4 FAIL/0 error/skip (3.946s), confirmed repair 28 PASS/0 error/skip (11.355s),
original C06 cleanup/isolated target 33 PASS/0 error/skip (23.557s), original
MCP pool/default/timeouts/singleton neighbors 70 PASS/0 error/skip (2.363s).
The C06 selection actually collected 33 cases, not the initially planned 25.
These scopes can overlap earlier reports and are not summed into a new total.
The implementer reports all 57 source Ruff/format and diff checks clean and has
returned the native runtime window. Same-reviewer SPEC re-review is dispatched;
QUALITY remains withheld until it passes. Task3 runtime and C08 parent gates
remain unchecked, including Linux and combined original daemon/HTTP evidence.


## Task 3 SOURCE SPEC v2 PASS; QUALITY dispatched — 2026-10-03

The same SPEC reviewer has independently accepted v2 source: original P1/P2
resolved, no new P1/P2, all 57 current and snapshot SHAs matched, only the exact
four repair files changed. Actual owner retention, partial execution-only
deadline and final/paused same cumulative final deadline are confirmed from
source and the preserved RED/GREEN/neighbor reports. The reviewer performed
read-only inspection; no new test/runtime result is implied.

Root has now dispatched whole-source QUALITY against the exact v2 57-file
freeze. Runtime writers remain idle while this source review runs. The Task3
Linux/combined original daemon/HTTP gates and C08 parent tasks remain unchecked.
After SOURCE QUALITY approval, the same original Task3 implementer will resume
the coordinated native/Linux proof window; no Task4/5/6, commit, activation or
whole C08 completion is implied by SOURCE SPEC approval.


## Task 3 SOURCE QUALITY v2 detached-owner finding — 2026-10-03

This continuation revalidated the complete v2 current 57-file SHA set with no
mismatches and confirmed the QUALITY reviewer is actually running. The previous
goal turn made concrete progress: actual RED, ownership/deadline repairs, v2
freeze, same-reviewer SOURCE SPEC PASS and QUALITY dispatch.

QUALITY has identified an additional physical-owner gap in old MCP detach
paths. LRU/cross-loop eviction removes an original owner from pool entries
before awaiting its context-manager exit. Cancellation can leave that actual
owner under an unscoped teardown reaper; creation-unwind cancellation can also
pop inflight state while the actual owner remains alive. The new scope join
only inspects entries/inflight/scope-closing, so neither publication nor
teardown can prove these detached owners joined. Root independently inspected
the implicated production paths. QUALITY approval is withheld.

The same original Task3 implementer owns the native window for real parked SDK
owner RED → minimal repair → GREEN. Scope attribution must retain each victim's
original scope, including when LRU evicts a different scope than the requester.
If managed-scope registration is used, it must precede original tool discovery,
not rely solely on the publication-time barrier map. Every detach/unwind/close
path must preserve positive owner settlement for C; Local best-effort semantics
remain. The complete v2 freeze stays historical. After a new explicit source
freeze the order remains SPEC → QUALITY. Linux/fresh images/combined daemon HTTP
and Task4/5/6 remain pending and cannot start under a failed source gate.


## Task 3 SOURCE QUALITY v2 formal FAIL — 2026-10-03

The completed read-only QUALITY verdict is FAIL, P1=1/P2=0. Its sole finding
is the previously recorded missing original-scope ownership in all old
detach/close/eviction/unwind paths. In particular `close_all_sync` must also
retain C owners before clearing registries and signalling same-loop exits.
The original host must explicitly register the managed C scope before MCP
tool discovery; publication-time freeze alone arrives too late. Positive
completion/join must precede removal from C scope retention. Node census
cannot substitute this earlier required host settlement. No additional
blocking issue was found, and original final/paused same cumulative 120-second
source contract remains confirmed. The reviewer validated freeze/report bytes
and performed no runtime action.

The same implementer is reproducing and fixing this P1 under the exclusive
native window. V2 remains a historical rejected QUALITY freeze. The next new
freeze must be re-reviewed SPEC first, then QUALITY; parent Task3/C08 gates
remain unchecked and installed runtime work is not yet authorized by a pass.


## Task 3 detached SDK owner business RED and birth registration — 2026-10-03

Root independently read `mcp-detached-owner-actual-red.xml`: three failures,
zero errors/skips, XML 4.137 seconds (log 4.21 seconds). Each case uses actual
C04 stdio SDK owner context-manager exit parked while the original tool activity
has settled. LRU cancellation evicts the original C victim under a different
requester scope; cross-loop uses a real thread and foreign loop; creation
unwind cancels after actual initialization and injected rejection. LRU and
creation-unwind fail because original request SQL entered before the owner
settled. The cross-loop case instead fails because original teardown actually
closed early; it is not mislabeled as the same SQL assertion. Initial
`mcp-detached-owner-red.xml` contains three setup errors (XML 1.759s) and is not
a business RED.

The sole implementer is applying explicit host managed-scope registration before
MCP tool discovery and registering each original `(loop, owner_task)` at birth
under the existing pool lock. Thus later old pop/clear/evict/unwind paths cannot
lose its victim scope attribution, even before the publication barrier exists.
Positive join precedes removal; ordinary Local scopes preserve their original
behavior. Existing v2 source/report snapshots remain historical. The new scope
repair still requires GREEN, all-close entry-point and default neighbors, new
source freeze, SPEC re-review and QUALITY re-review. No installed Linux gate
is complete yet.


## Task 3 managed MCP birth ownership all-path GREEN — 2026-10-03

Root independently read `mcp-detached-allpaths-green.xml`: 97 PASS, zero
failures/errors/skips, XML 13.182 seconds. Ten actual C04 stdio SDK cases
cover LRU cancellation, real cross-loop, creation-unwind cancellation, repeated
cancellation and the six original close entry points: scope, session,
if-current, server, all and all-sync. Each parks the actual original owner,
lets the caller/tool activity settle, and verifies publication/teardown cannot
proceed before physical owner join. The report also covers MCP quiescence,
original publisher ownership/budget and Local/default pool neighbors; these
overlap existing evidence and are not added to any aggregate pass total.

The source fix retains managed original owners from birth under the existing
lock, preserving each victim's own scope. An owning-loop monitor positively
awaits the actual SDK owner through context-manager exit before removal.
Original host binding precedes MCP tool discovery; unbound Local scopes keep
original close ABI/timing. Original C06 cleanup neighbors and the complete
58-file v3 source freeze remain next, then SPEC → QUALITY re-review. The v2
QUALITY FAIL remains historical until the new bytes pass both reviews.


## Task 3 complete v3 source frozen for SPEC re-review — 2026-10-03

Root verified all 58 current source SHAs and independent
`source-freeze-v3/backend/...` copies against `task3-source-freeze-v3.json`,
without mismatch. Exactly the original `runner_context.py`, `workspace.py`,
`session_pool.py` and new `test_c08_mcp_detached_owners.py` differ from v2.
The other 54 files retain v2 bytes, including storage and final-wait repair.
V3 complete JSON SHA is
`556c4d355ae107e99a6f0d95153e6274db575e49eaa27b7bb6f3f4cf0b1423fe`;
its file inventory SHA is
`0dbd34f7c6362472af4d391f8a56bcd03210bfe7ee0885f8648606a67534f299`.
The four `*.quality-review-v3.diff` deltas and original v1/v2 snapshots remain.

Root checked the three quality-repair report SHAs and actual XML counts: valid
RED 3 FAIL/0 error/skip (4.137s), all-path GREEN 97 PASS/0 error/skip (13.182s),
original C06 cleanup neighbors 33 PASS/0 error/skip (22.463s). The complete
58-file static/format/diff checks are reported clean. The implementer has
stopped source/runtime actions and returned the native execution window.
Same-reviewer SPEC re-review is dispatched. QUALITY will follow only on its
PASS, and no Linux/image/combined daemon HTTP/C08 completion is implied.


## Task 3 SOURCE SPEC v3 PASS; QUALITY re-review started — 2026-10-03

The same SPEC reviewer independently passed v3 SOURCE with no new P1/P2,
confirmed 58 current and snapshot SHA matches, the exact four changed/new
files and 54 unchanged v2 files. Original-scope owner retention at birth,
positive join across legacy detach/close/unwind, partial execution-only bounds
and the same cumulative final 120-second deadline remain compliant.
Root has dispatched the same QUALITY reviewer against this exact v3 freeze.
There is no installed/runtime acceptance claim; no Task3 parent completion,
Task4/5/6 start, new image build or commit follows from SPEC alone.


## Task 3 SOURCE v3 accepted; installed Linux window dispatched — 2026-10-03

Sequential same-reviewer SPEC v3 and QUALITY v3 both PASS with P1=0/P2=0.
Both independently verify all 58 current and snapshot SHAs, the exact three
production changes/new SDK test and 54 unchanged v2 files. The QUALITY P1
detached-owner gap is resolved by trusted pre-tool scope binding, birth-time
owner retention and real owning-loop positive join. Local defaults, original
cumulative final 120 seconds and fixed metadata/journal bounds remain. This
accepts only the exact v3 SOURCE gate, not all Task3/C08 behavior.

Root has assigned the same sole Task3 implementer the exclusive native/Docker
window for fresh six-wheel catalog and installed-byte proof, original C04
Runner/graph/bash escaped-writer/MCP/plugin/memory/two-model containment, actual
different-UID collector/FD/unknown-service negatives and a combined original
NodeDaemon → actual NodeClient HTTP/PG → installed collector/NAS → lost prepared
reply → full descriptor recovery path. Existing native receipt fixtures and
two separate stager/daemon scopes cannot substitute the combined Linux proof.
New test/fixture bytes require a new explicit source freeze; any production or
packaged guide repair requires sequential source review and a fresh image,
while all old v1/v2/v3 evidence remains intact. Original B image is unchanged.

Task4/5/6 checkpoint observer/point acceptance/public API/final cancellation
authority remains unimplemented and is not fabricated by this window. Current
publication remains prepared-only, with zero accepted points. No commit, push
or activation is authorized by SOURCE PASS. Parent Task3 and OpenSpec C08
gates remain unchecked until the required real runtime evidence is accepted.


## Task 3 Linux slice and prepared-only authority decision — 2026-10-03

The implementer is preparing `c08_linux_fixture.py` as a trusted installed
test environment factory calling the original `build_agent_environment`, and
`test_c08_linux_workspace.py` as one actual original NodeDaemon/TCP NodeClient/
PostgreSQL/collector/NAS/lost-reply path. The actual original C04 ScriptedModel,
parent/child, MCP/durable MCP, plugin/memory and two-model sequence is retained;
observation/pause after real presentation does not replace AgentRunner, stock
run or graph. Original bash probe/setsid remains, with additional real delayed
and double-fork writers for PID/start/hash proof. New fixture/build entry-point
metadata is a distinct input freeze; accepted v3 production bytes cannot be
replaced silently. Fresh six-wheel payload and installed catalog/image IDs
remain required.

Root confirms prepared candidate is not an accepted point. No same-run writer
reopen or new MCP session is authorized after prepared; no fabricated point
can bypass this. Task3 may verify real same-pool SDK close/reconnect before
publication, then positive close and actual Node zero-child sealing, followed
by rejection of new calls while prepared-only. The actual accepted-point
transaction exists only in Task4, so the installed post-accept reopening test
is explicitly carried to that task's checklist. This resolves a dependency
in the plan without dropping the C08 requirement; whole C08 acceptance still
requires actual installed same-run reopen/reconnect after a real accepted pair.
No Task4 outcome, accepted point or file API is claimed by Task3's window.


## Task 3 actual Linux builds and fixture failures retained — 2026-10-03

The first actual fresh six-wheel build completed successfully. Immutable image
IDs are dependencies `sha256:c0b0a8ff9482709b5f1dd2291e8550ce3c93df91a3325b11b9ddc03f206b6a8b`,
provider `sha256:662a8ed942f8a103455242b3ef8b023edaff9beed3862f6fbfa2a1211ab21080`,
and Runner `sha256:c0b1f2da15ca2311f50bcec30545ff8c1d94f75f47c03c0800fe926cc347e910`.
Root independently compared the six-wheel member catalog with the actual
installed exact-member catalog: all 779 non-RECORD members match, zero byte
differences. Six wheel RECORD catalogs remain separate from pip-regenerated
installed RECORD; they are not falsely claimed byte-identical. Fixed collector
and contracts byte equality and installed compatibility are recorded in
`linux-installed-byte-comparison-v3-20261003a.json`. This proves installation
bytes for that image, not runtime acceptance.

Actual combined run a (`linux-combined-v3-20261003a.xml`) reports 1 FAIL,
0 error/skip, 7.350 seconds: the host fixture used an unknown profile
`timeout_seconds` field, rejected before Runner launch. Run b reports 1 FAIL,
0 error/skip, 107.646 seconds: the original parent/child tool sequence and real
SDK close/reconnect ran, but unsorted presented paths in the trusted test
fixture were rejected before publication. These are fixture failures, not
production business RED or green acceptance. Both logs/XML and earlier input
freezes remain intact. The host profile field is corrected to the original
`execution_timeout_seconds`; presented paths use the required canonical order.

Freeze 5 has JSON SHA
`6465c3cf760d72aa357115c4d666782bddb4616d341a1803fcc78c74af5c05e1`.
The installed fixture changed, requiring another fresh build with independent
context/tags `20261003b`; it completed with Runner ID
`sha256:47967e23411b4bb6ec8d6a7d0020cd88890db8b7a883fb1c1e300178cfdec319`.
A later host-only assertion detects reported boundary errors immediately and
is recorded in freeze 6; it does not change image payload. The sole implementer
is collecting this new image's complete installed-byte proof before actual
combined run c. Root does not poll implementer-owned execution sessions or run
concurrent tests. Final cleanup remains the original cumulative 120 seconds.
Task3, Task4/5/6 and OpenSpec C08 acceptance remain pending; prepared candidates
are still zero accepted points and never authorize reopening.


## Task 3 actual Linux collector rejects residual process — 2026-10-03

The second fresh image's installed comparison records 779 wheel content members
with zero mismatches. Actual combined run c reports 1 FAIL, 0 error/skip,
19.712 seconds; original NodeDaemon publication fails at the fixed collector.
Run d adds host-only observation of the original retained CLI readers and
reproduces 1 FAIL, 0 error/skip, 21.563 seconds. Its preserved
`linux-combined-v3-20261003d/partial/collector-cli.json` records exit 1, empty
stdout and `ValueError: Unknown process remains in original container` from
`validate_zero_census`; this is an actual contained runtime rejection.

Root independently reads the original Runner receipts: full six-name original
tool sequence, actual SDK close/reconnect, committed root checkpoint and a
partial publication request with final budget still unstarted. No candidate
copy/ack or accepted point is claimed. The sole implementer is diagnosing the
remaining real PID/start/state/parent identities, preserving the strict
only-PID1-plus-helper census and the rule that Node never kills unknown
processes. Any production repair requires a new complete source freeze,
sequential SPEC then QUALITY and another fresh image. V3 SOURCE PASS alone
does not accept this failed Linux path. Task3 and whole C08 remain pending.


## Task 3 residual actual MCP service identified — 2026-10-03

Owned-container diagnosis after the original rejected census adds one separate
read-only diagnostic process and records PID/start/state/parent/UID. Run e
shows a live non-zombie workload child plus PID1 and the explicitly identified
diagnostic. Run g's preserved process census independently identifies the
remaining live child as the original `fleet.c04_mcp_fixture` module: PID13,
PPID1, start ticks4868227, state S, UID501. Module names only explain the
diagnosis; they never grant process-kill or census-whitelist authority.

An intermediate host diagnostic failed on an embedded NUL argument (run f);
this is retained as a diagnostic-fixture error, not another business RED.
Host-only input freezes retain the observation changes. Production v3 is
still unchanged. The implementer is tracing the actual original MCP owner
and session shutdown path before implementing a source repair. The original
Node continues to reject unknown processes, and no copy/ack/point acceptance
is claimed. Full C08 remains in progress.


## Task 3 actual installed scope-alias defect isolated — 2026-10-03

The third fresh diagnostic image completed with immutable Runner ID
`sha256:3c6fd6baa196dc66066adf8a9f567658fce8c14ae9fd7d175478c004b67a2b6f`.
Its six-wheel comparison records 779 installed content members and no mismatch;
input freeze11 JSON SHA is
`ca220d0faed02ef953a0a4b8c75ea2c2a62421c0505f4065af4c04be0a9a2773`.
Root independently confirms accepted v3 production bytes remain unchanged;
the only mismatch among the old 58-file inventory is the explicitly revised
test image builder. Actual installed run h's original-pool receipt identifies
the cause: the publisher pool equals the current singleton, but has two live
C04 owners, one in `user-c04:thread-c04` and another in `default:default`.
Only the original user/thread scope is managed. This explains why positive
join of that scope alone leaves a real MCP service and why Node correctly
refuses publication.

Root requests a production TDD repair binding C MCP owner scope to the trusted
original mutation capability and rejecting aliases, while preserving Local
default behavior. Adding default scope to a whitelist or globally closing
unrelated owners is not an acceptable repair. Actual failed Linux receipts
remain the runtime RED anchor; a new complete source freeze must pass SPEC
then QUALITY before another fresh image and real Linux rerun. This diagnosis
is not Task3 acceptance; prepared-only and full B → C → BC scope remain.


## Task 3 trusted MCP tool identity repair RED and first GREEN — 2026-10-03

Root independently verifies `mcp-bound-identity-red.xml`: two real SDK test
failures, zero error/skip, 2.095 seconds. Missing runtime creates an owner in
`test-user-autouse:default` rather than the original capability's user/thread;
a closed original gate fails to reject a tool call. These corroborate the
actual installed default-scope residual. The initial repaired report
`mcp-bound-identity-green.xml` has 4 PASS, zero failure/error/skip, 2.246 seconds.

The proposed original-tool repair captures the trusted mutation context and
writer controller during construction under the private C tool snapshot,
binds them outside the settled-activity wrapper, and uses immutable original
user/thread identity for session and filesystem scope. A conflicting active
owner/controller rejects; Local tools keep their original runtime/default
behavior. Root has requested the corresponding durable-submit entry be
checked for the same missing-context gate failure before freezing the whole
source. No new SOURCE acceptance or Linux PASS follows from these first four
tests. A complete new freeze and sequential SPEC/QUALITY remain required,
followed by fresh image and actual Linux verification.


## Task 3 corresponding durable-submit gate RED and combined GREEN — 2026-10-03

The original durable-submit wrapper has the equivalent missing-controller
context boundary. Root verifies `mcp-bound-submit-business-red-2.xml`: 1 FAIL,
zero error/skip, 2.622 seconds, specifically DID NOT RAISE RuntimeError after
a real original SDK submission and original TaskService SQL return despite
the closed original controller. Three preceding attempts failed on import,
constructor and missing-table fixture setup and remain separate; their labels
never make them business RED.

Root verifies `mcp-bound-combined-green.xml`: 5 PASS, zero failure/error/skip,
3.572 seconds, covering ordinary missing-runtime, closed-gate, different
context/controller and the real durable-submit closed-gate case. The repair
uses the same trusted host binding for ordinary and background-submit tools,
retains the original private submitter and immutable user/thread/run, and
does not start another TaskService poll loop. Whole-source neighbors/freeze
and sequential SPEC/QUALITY remain pending; no fresh repaired image or
actual Linux acceptance is yet claimed.


## Task 3 SOURCE v4 frozen; SPEC re-review dispatched — 2026-10-03

Root independently verifies the complete v4 freeze: 63 current files and
63 separate snapshot copies match every recorded SHA. Freeze JSON SHA is
`787a7e3905e506e64d0c6a1dd194432141aed05f10e85fc828418b4b2bc67a14`;
file inventory SHA is
`5a0ad51f3e22e35028f8a4ef192525f9018200ea95eade423a259302606e4c6e`.
Of 58 old v3 paths, only MCP tools.py production and the explicitly revised
test image builder changed; 56 retain their old bytes. Five new tracked
inputs are the bound-identity test, three Linux fixture/probe/host-test inputs
and test-plugin metadata. Protected storage/source bytes remain unchanged.

Root verifies current native repair reports: scope/Local/SDK neighbors
114 PASS, zero failure/error/skip, 14.122 seconds; original C06 cleanup and
C08 publisher neighbors 44 PASS, zero failure/error/skip, 29.860 seconds.
Overlapping first GREEN reports are not added to these counts. Final static
log reports all checks passed and 62 Python files already formatted; initial
two E501 and fixture formatting failures remain historical.

The same SPEC reviewer is now reviewing the complete v4 source and evidence,
including original trusted owner/controller binding of both ordinary MCP
call and durable submit, Local preservation, actual owner join, partial/final
budgets, pure zero-child collector and bounded original protocols. All live
execution ended and the implementer keeps source frozen. QUALITY follows
only SPEC PASS, then a new fresh v4 image and actual Linux acceptance.
Task3/OpenSpec C08 remain unchecked; Task4–6 are not started.


## Task 3 SOURCE SPEC v4 FAIL: allowed HTTP/SSE bypass — 2026-10-03

The same SPEC reviewer completes v4 with P1=1: ordinary non-stdio tools in
MCP tools.py956–963 are appended without trusted owner/controller binding or
settled workspace activity. Production workspace contracts explicitly allow
HTTP/SSE at workspace_contracts.py39–50, so the original stdio fixture cannot
exclude these transports from the C boundary requirement. A C HTTP/SSE call
can lose the original controller, run after closed gate or under another
execution without actual-call settlement. All 63 current/snapshot SHAs match;
the stdio/submit repair, partial/final budget and protected storage evidence
remain valid, but whole v4 SOURCE is not accepted. QUALITY does not start.

Root dispatches the same implementer to extend the shared trusted binding and
actual owner settlement to ordinary allowed HTTP/SSE SDK coroutines, including
their generated synchronous wrappers. Real transport closed-gate, foreign-owner
and cancellation-through-SDK-cleanup evidence is required; Local transport
behavior stays unchanged. V4 and all old evidence remain preserved. A new
complete v5 source freeze must pass SPEC then QUALITY before fresh image or
Linux acceptance. Task3/C08 and all later C/BC outcomes remain pending.


## Task 3 HTTP/SSE first real transport run: mixed failure evidence — 2026-10-03

The implementer adds `test_c08_mcp_remote_transport.py` using actual loopback
FastMCP/uvicorn and original adapter discovery and SDK calls. The first run
`mcp-http-sse-business-red.xml` is terminal: 6 FAIL, zero error/skip,
242.724 seconds (log 242.82). Root independently inspects assertion messages
and logs rather than trusting its filename. Only the two closed-gate cases
are valid business RED: HTTP and SSE each DID NOT RAISE RuntimeError after
actual call. The other four foreign-owner/cancellation cases fail at tool
discovery: original 60-second session-init timeout yields no tool, causing
StopIteration before the intended assertion. They do not prove those business
boundaries and are not counted as four additional valid REDs.

The SDK-cleanup observer also must mark physical completion only after the
original SDK context exits successfully, retaining the original cancellation
exception; a statement after async-with can be skipped by propagated
CancelledError. Root requests precise observer and loopback service lifecycle
diagnosis and stable actual foreign-owner/cancellation/Local cases. Old
report remains intact. The old handle ended, not restarted on observation
timeout; Root only interrupts/re-dispatches the long-unresponsive implementer
agent to recover coordination, without terminating/restarting any shell/native
process. Its first action must revalidate any current actual tool handle.
V4 remains formally SPEC FAIL P1=1, QUALITY not started, original cumulative
120 unchanged, and no Linux acceptance, image rebuild or commit is claimed.


## Task 3 real HTTP/SSE lifecycle corrected; six valid business REDs — 2026-10-03

The next service-owner run ends with mixed failures: the HTTP manager was
accessed before lazy app creation (three setup failures), one SSE closed-gate
case reaches valid business RED, and two SSE discovery failures remain.
These are kept separately. The actual installed sse_starlette server carries
a process-global AppStatus.should_exit flag from a prior Uvicorn shutdown;
the fixture resets its repeated-server lifecycle flag before each sequential
start. HTTP app creation now precedes original session_manager.run. A shorter
fixture discovery timeout diagnoses this without changing production defaults.

Root independently verifies `mcp-http-sse-lifecycle-red.xml`: 6 FAIL, zero
error/skip, 3.740 seconds (log3.92). Every actual discovery succeeds in one
process across the six sequential original SDK/server cases. Both transports
fail closed-gate and foreign-owner DID NOT RAISE assertions (four valid REDs).
Both cancellation cases have a live original SDK owner parked at its actual
exit with active_count=0 instead of1 (two valid REDs). The original SDK
__aexit__ observer sets completion only after actual successful exit and
rethrows the original cancellation. No per-case process split or transport
substitution is used.

The sole implementer now extends ordinary allowed non-stdio SDK calls with
the existing outer trusted binder and actual settled activity, retaining
original transport, metadata/schema and sync-wrapper path. Real positive
missing-context and Local HTTP/SSE cases are added before v5 source freeze.
V4 remains SPEC FAIL until the new complete snapshot passes sequential
SPEC/QUALITY, followed by fresh installed image/Linux proof. Full scope and
the original cumulative 120 seconds remain unchanged.


## Task 3 SOURCE v5 frozen; HTTP/SSE GREEN and SPEC dispatched — 2026-10-03

Root independently verifies 64 current files and 64 distinct snapshot copies
against every SHA in `task3-source-freeze-v5.json`. JSON SHA is
`8f61db66cd6164d6b740b40300acf972cc8bfd04aa4e7e8af88875243208527e`;
inventory SHA is
`2f6f93ea1ccf8f88db4078454e59cd08ee345df2ee02843bec3b937236566db0`.
Compared with v4, only tools.py production changes and the new real remote
transport test are added; all other62 old paths retain their bytes.

The ordinary non-stdio SDK coroutine now reuses trusted construction-time
owner/controller binding outside settled activity. Actual SDK contexts stay
in the original call task through their complete __aexit__; no HTTP/SSE pool,
substitute transport or separate cleanup owner is introduced. Local transport
behavior remains unchanged. Root verifies all recorded raw-report SHAs and
final XML: `mcp-http-sse-bound-final.xml`10 PASS/5.117s,
`mcp-http-sse-scope-neighbors.xml`114 PASS/14.061s, and
`mcp-http-sse-cleanup-neighbors.xml`44 PASS/27.544s, each zero error/skip.
Transport tests cover closed-gate, foreign-owner, real cancelled SDK cleanup,
missing-context positive and Local actual HTTP/SSE; overlapping earlier green
reports are not added. Final63 Python static/format/diff checks pass.

The same SPEC reviewer is now reviewing complete cumulative v5 source and
protocol constraints. The implementer has ended all runs and freezes source.
QUALITY follows only formal SPEC PASS, and a new fresh installed image plus
actual combined Linux evidence still follows both reviews. Old v3 Linux
failures/three image catalogs are not relabeled v5 acceptance. C08 Task3 and
OpenSpec gates remain unchecked; Task4–6 remain unimplemented.


## Task 3 SOURCE SPEC v5 PASS; QUALITY re-review started — 2026-10-03

The same SPEC reviewer formally passes complete SOURCE v5, P1=0/P2=0,
resolving the ordinary allowed HTTP/SSE bypass. It independently verifies
64 current/snapshot matches, exact tools.py/new test delta and62 unchanged
old paths; trusted binding precedes activity admission and actual SDK cleanup
remains in the original call task. The generated sync entry, original schema/
metadata, Local, stdio and durable-submit behavior are retained. It confirms
6 effective transport REDs→10 PASS,114 scope neighbors and44 cleanup neighbors,
all zero error/skip, while preserving mixed-failure classifications and all
original cumulative deadline/join/collector/protected storage requirements.

Root has dispatched the same QUALITY reviewer against this exact complete
v5 freeze, strictly after SPEC PASS. No live native/Docker window is opened
from SPEC alone. Whole Task3, fresh installed v5 images, actual combined
Linux acceptance and Task4–6 remain pending.


## Task 3 SOURCE v5 accepted; fresh installed Linux window dispatched — 2026-10-03

Sequential same-reviewer SPEC v5 and QUALITY v5 both formally PASS with
P1=0/P2=0. QUALITY independently verifies all64 current/snapshot SHAs and
report SHAs, the actual6 transport RED→10 final PASS,114 original MCP scope
neighbors and44 cleanup/publisher neighbors,63 Python static checks, and
all cumulative positive join/budget/collector/journal/protected storage
requirements. This accepts only the exact complete v5 SOURCE gate.

Root assigns the same sole implementer the exclusive native/Docker window
for fresh six wheels and distinct dependency/provider/Runner images, complete
installed-byte/fixed collector/contracts/original runtime-origin proof, and
the original full C04 graph→actual producer stop/join→original daemon/TCP
NodeClient/PG→zero-child census/NAS→lost committed prepared reply→fixed-marker
recovery path. Six real original and added writer PID/start/ns identities
must prove settlement before request SQL/copy; brief hash stability alone
is insufficient. Actual unknown-service/UID/ns/container-identity/helper
private-descriptor negative evidence remains required.

Partial never starts final cleanup budget; final uses the original cumulative
120 seconds. Publication stays prepared-only with0 accepted points and no
reopen, while actual post-accepted-point writer/MCP reopening remains Task4.
Old v3 Linux FAIL and all prior images/catalogs remain intact; they cannot
stand in for v5 acceptance. Any production repair needs another new source
freeze/review/fresh image. Root owns docs and independently verifies output,
without concurrent runtime testing or polling implementer-owned sessions.
Task3/OpenSpec8.1–8.4 remain unchecked; Task4–6 and later full C/BC remain.


## Task3 installed v5 runtime progress and reboot continuation — 2026-10-03

Root resumed the same B → C → BC objective after the interruption. SOURCE v5
remains accepted; Task3 as a whole remains unchecked pending final runtime
observations and review. No C08 commit or activation is claimed.

Fresh context `linux-build-v5-20261003v5a` produced Runner image
`sha256:01fdb6481fa3f401c15ab5707a166619be6817a14c5c85f607d7a6ffb7b01cc8`.
Root independently matched all17 prepared input hashes and779 actual installed
wheel members, with zero mismatches; the six RECORD files were handled separately.
The original B image and older v3 images and failures are retained.

The first v5a combined run had one fixture timing failure: it observed the first
lost response before the original Node recovery retry. It is not a production
RED and is retained. The host-only wait correction has its own input freeze;
`linux-combined-v5-20261003v5b.xml` has2 passes, zero failures/errors/skips,
43.678 seconds. Both actual original graph scenarios have6 writer identities,
4 registered supervisor/shell rows settled before request creation, only PID1
and the collector in before/after census, one seal,3 recover calls, one lost
prepared response, zero accepted points and late claim409. Partial publication
leaves final cleanup unstarted; final publication uses the existing budget.

Root independently read the actual negative reports and receipts:

- `linux-negative-v5-20261003a.xml`:1 pass,22.442 seconds, zero bad or skipped.
  An unknown service is rejected and remains alive until its original private
  pipe is closed and it is positively joined. Workload attempts to read/write
  helper stdin, read helper memory and attach ptrace are denied. Invalid collector
  input exits1. This proves denial, not successful publication with an unknown
  process. Helper actual UID/start receipt will be strengthened separately.
- `linux-profile-negative-v5-20261003a.xml`:1 pass,22.155 seconds, zero bad or
  skipped. Mismatched expected UID, memory and PID limit are rejected against
  the same unchanged actual running CID/StartedAt. This is profile mismatch
  evidence, not physical mutation of the workload UID or namespace.
- `linux-installed-kernel-deny-v5b-proof.json`: actual installed compatibility
  rejects with ValueError before graph startup when PR_SET_DUMPABLE is denied.
  The earlier blanket-prctl seccomp attempt failed OCI startup with125 and is
  retained as a fixture failure, not compatibility evidence.

Root approved stronger installed six-writer ancestry observations: actual PID,
start ticks and parent chain must terminate at the registered original supervisor;
birth PPID and current PPID are recorded separately. The publication observer must
run after positive original process join and before the first publication SQL
transaction. Observing after `_validate` only proves before request INSERT, because
`_bound_sql` and guard SQL already ran inside `sf.begin()`. Original publication,
Runner, graph and SQL remain in place. Changed installed fixtures require fresh
input freeze/image and sequential incremental SPEC then QUALITY review.

No same-CID restart or second graph replay is authorized for this negative fixture.
Current OpenSpec does not require an isolated physical restart test. Actual immutable
inspection/census, native per-field drift rejection, runtime profile mismatch,
escape denial and stopped old claim/ACK rejection cover their stated boundaries.
Sequential containers reused a PID namespace inode; inode alone is not proof of
container identity, and no dynamic same-CID namespace change is claimed.
Task4 retains exact checkpoint/candidate acceptance and actual admission reopening;
Task3 prepared-only candidates do not manufacture an accepted point.


## Root planning validation — 2026-10-04

Root inspected `/opt/homebrew/bin/openspec validate --help` and ran the actual
`openspec validate add-ecs-remote-agent --type change --strict --no-interactive`.
It exited0: the change is valid. This checks specification structure, not runtime
completion. Root also compared the64 frozen SOURCE v5 paths with current bytes:
all27 production files remain unchanged; the installed Linux fixture and host
Linux test are the two modified frozen paths. The new negative observer is an
additional test input. These fixture changes must be bound to the new runtime
freeze/image before accepting Task3. OpenSpec8.1–8.4 remain unchecked.


## Task3 actual escape and late acknowledgement negative — 2026-10-04

Root independently parsed `linux-escape-negative-v5-20261003a.xml`:1 pass,
23.775 seconds, zero failures/errors/skips. The actual installed workload's
unshare and setns calls returned EPERM; setuid was denied. Actual UID501 and
PID namespace remained unchanged. The same live original prepared candidate
received HTTP409 for a forged nonce; after physical stop, its complete prepared
acknowledgement also received409. This is actual rejection evidence without
restarting or replaying a graph. The earlier positive late claim409 and this
late acknowledgement409 cover separate endpoints.

SOURCE v6 contains65 files: compared with the accepted v5, only the installed
Linux fixture and host Linux test changed, plus a new host negative observer.
All27 production paths remain unchanged. Root independently matched all65
current file SHAs. Incremental SOURCE review precedes the fresh ancestry image;
final installed runtime review remains required afterward.


## Task3 increment SOURCE v6 SPEC accepted — 2026-10-04

The corrected v6 freeze JSON SHA is
`ed7416d23955ad6fb771f4b9b0ad2231e1486650bc609a576af192332148563b`;
its inventory SHA is
`c15f74d517871b755f8902d745253f05513e38d6846488cc2a8f1a20e1916cb8`.
Root independently verified65 current paths,65 snapshots and25 report SHAs,
all matching. The JSON now accurately distinguishes old v3, actual built v5
and not-yet-built v6 inputs. Sequential SOURCE SPEC v6 passes with no new P1/P2;
QUALITY v6 has been dispatched after that approval. No new ancestry runtime
acceptance is claimed until the fresh v6 image executes the observer.


SOURCE QUALITY v6 subsequently passed with P1/P2=0. The sole implementer has
been dispatched for the fresh unique v6 context, six wheels, complete installed
byte comparison and original partial/final Linux runs containing the new ancestry
and pre-transaction physical join observation. Final runtime SPEC then QUALITY
remain required; Task3/C08 are not yet accepted or committed.


## Fresh v6 actual build window — 2026-10-04

The sole implementer owns actual exec79187 for unique context
`linux-build-v6-20261004v6a` and tags `task3-v6-20261004v6a`. Root observed its
new build log reaching the Docker dependency image build after preparation;
this is an actual build window, not a proposed command. SOURCE v6 remains
frozen. Complete installed-byte verification precedes new original partial/final
runs with mandatory `C08_EXPECT_ANCESTRY=1`. Historical v5 negative reports are
retained against their exact inputs; final v6 runtime acceptance remains pending.


## Task4 source entrypoints confirmed while v6 builds — 2026-10-04

Root inspected the actual stock source to prepare the next task, without changing
production while Task3 is frozen. `RunRepository.update_status`,
`finalize_if_not_cancelled` and `update_run_completion` each own their own original
AsyncSession and explicit commit; the trusted terminal participant must run on that
same session before commit, rather than add an independent Fleet transaction.
The no-cancellation finalize branch that does not update a row must not accept a
point. Completion bookkeeping after a prior matching accepted final transition
must remain narrow and idempotent.

`run_agent` has both single-mode and multi-mode original astream paths. Its
terminal tail writes duration/title checkpoints before final status and may do a
second title checkpoint when cancellation wins. Candidate preparation must follow
the latest of these actual root writes; an earlier successful candidate cannot
silently authorize a later cancelled checkpoint or outcome.
The existing `before_terminal_mutations` ordinary exception branch logs and
continues. A new C-only workspace preparation failure must stop that transition
or persist explicit recovery through its fenced authority, preserving ordinary
Local behavior. It cannot fall through to a successful core terminal row.

`FencedAsyncPostgresSaver.aput` currently delegates through `_operation` into
the original `_cursor`; the latter owns saver lock, connection transaction and
cursor. A workspace post-commit notification must be outside all of these
contexts. Actual pending-write durability and callback ordering require tests,
not an assumption that scheduling `aput` means its SQL is committed. Current
`WorkspaceRequests` expressly only creates/claims/prepares candidates and never
accepts a point, so Task4 must implement the exact pair transaction separately.
These observations refine existing Task4 requirements; they do not mark any
Task4 implementation or acceptance complete.


## Fresh v6 first actual run failed after ancestry observation — 2026-10-04

Build exec79187 completed naturally with0. Runner actual image ID is
`sha256:a6e3a8b32728e479ee0640bce48e171c6762fed011cd140fa2db2ffef4347791`.
Root independently verified six frozen wheel archives and779 actual installed
non-RECORD members, fixed files, all installed skills and7 fixtures with zero
mismatches. Dependency/provider IDs are recorded in the immutable v6 comparison.

Actual run exec56769 finished. Its helper script printed pytest exit1 but did
not propagate that return code, so outer script0 is not test success. Root parsed
`linux-combined-v6-20261004v6a.xml`:1 failure,0 errors/skips,29.115 seconds,
stopping before final. The actual trace fails in Node `_same_container` after
copy, rejecting changed containment. The new actual ancestry observation did
run successfully before that later failure:6 writer identities reached their
2 original supervisors; original Popen returncodes0/0 and capture threads joined;
all6 old PID/start identities absent before the first original publication SQL
transaction; partial final budget unstarted. This is partial evidence inside a
failed run, not combined acceptance.

`_same_container` compares the complete runner receipt including instantaneous
process state; the collector's own repeated identity check instead compares
PID/PPID/start/UID and separately rejects dead states. R/S drift is a source-backed
hypothesis, not yet the demonstrated cause. Root approved host-only immediate
bounded census capture against the same immutable image with a separate input
freeze, before any production repair. Any repair must keep every stable identity,
namespace/cgroup/profile fence and independent dead-state rejection, demonstrate
actual RED→GREEN, then complete new SOURCE review and fresh image verification.
Task3 remains unchecked; v5 positives do not override this new actual failure.


## Actual containment failure cause demonstrated — 2026-10-04

The sole implementer's diagnostic exec9063 exited pytest1 and preserved both
actual successful census receipts in
`linux-census-diagnostic-v6-20261004b/partial/collector-censuses.json` against
separate host observation inputs and the same immutable v6 image. Root independently
read the2 frames: only runner.state differs, S→R. Actual PID1, PPID0,
start ticks5698669, UID501, CID, StartedAt, image, launch fingerprint, PID namespace
and cgroup digest all match. This is a demonstrated false rejection of normal
scheduling state drift by original `_same_container` full-dict comparison.

Root authorized the minimal stable identity comparison while independently
rejecting dead states Z/X/x for each receipt. All PID/PPID/start/UID and original
container/image/profile/namespace/cgroup fences remain mandatory. Original-code
native RED will use actual captured observations; committed regressions must be
self-contained, follow the real schema and never depend on ignored `.local`
files or commit private nonce/token raw data. Stable identity drift and dead-state
negatives must remain. New production bytes require complete v7 freeze, sequential
SOURCE SPEC then QUALITY and a fresh installed image; old failures remain.


## Scheduling-state original-code native RED independently checked — 2026-10-04

Root parsed `census-scheduler-state-red.xml`:23 cases,9 failures,14 passes,
zero errors/skips,0.870 seconds. Both actual-sample S↔R comparisons fail;
old comparison also does not independently reject six dead/unverifiable states
or missing state. The other14 identity/shape cases pass. This is not23 failures.
The tracked JSON fixture contains only stable container and process observations,
with no request nonce, token or private identity. Root independently verified
its actual raw source SHA and both retained observation projections match the
ignored Linux diagnostic byte-for-byte for their retained fields. The regression
is self-contained in the repository and does not depend on `.local` at test time.
Production fix/GREEN/new complete source freeze and image remain pending.


## Stable process comparison fix and native GREEN — 2026-10-04

The missing-field tests were strengthened to begin with the same actual live
frame on both sides; otherwise unrelated S/R drift masked absent fields in the
original implementation. Root parsed the retained final original-code RED:
23 cases,13 failures,10 passes,0 errors/skips,0.825 seconds. The earlier9-failure
report remains historical and is not relabeled.

Production changes only `_same_container`: compare PID/PPID/start/UID as stable
process identity; require complete integer identity fields and a known live Linux
state independently in each frame. CID/StartedAt/image/launch fingerprint,
collector namespace and cgroup comparisons remain. Original native stager samples
only gain actual collector PPID0/stateS shape. Root parsed focused GREEN:
32 cases,zero failures/errors/skips,8.763 seconds. Affected7-file neighbors run
belongs solely to implementer exec46490. Complete v7 freeze, source dual review,
fresh image and final installed runtime acceptance remain pending.


Root subsequently parsed `census-state-affected-neighbors.xml`:80 cases,
zero failures/errors/skips,23.352 seconds. This covers the affected7-file native
scope and is not added to the overlapping focused32 count. Complete v7 source
freeze and installed runtime validation still follow; no C08 gate is accepted
from these native reports alone.


## Complete repair SOURCE v7 freeze and SPEC dispatch — 2026-10-04

Root independently matched67 current files,67 snapshots and31 report SHAs.
SOURCE v7 JSON SHA is
`bc984bd9eccab24813de8f035e45a8654c39210e8d17c3bc4f828d50577773b4`;
inventory SHA is
`488b8ebb75173519bdc4c62769b22cc82249be5171374dd30ef86191247c8ac3`.
The only production delta since v6 is `workspace_publication._same_container`.
Other old-path changes are immediate host census capture and original native
stager fixture shape; the portable test/actual nonprivate JSON are new inputs.
The sole implementer confirmed actual native exec46490 ended0 and no live tools.
Complete SOURCE SPEC v7 has been dispatched; QUALITY and fresh image follow
only that approval. V6/v5 image/runtime reports remain accurately historical.


## Complete SOURCE v7 SPEC accepted — 2026-10-04

Sequential complete SOURCE SPEC v7 passes with no new P1/P2. It independently
matched67 current files/snapshots and31 XML SHA/counts, actual Linux raw and
portable nonprivate sample projection, the sole stable-comparison production
delta and preserved original MCP/settlement/deadline/collector/storage contracts.
QUALITY v7 has been dispatched only after that approval. Fresh v7 build/runtime
remains gated on QUALITY; whole Task3 and later C08 tasks remain incomplete.


Complete SOURCE QUALITY v7 passes with P1/P2=0 after SPEC approval. The sole
implementer is dispatched for fresh v7 context/images, complete installed-byte
comparison and original partial/final combined Linux runtime verification.
The helper must propagate pytest's real exit code; historical v6 outer0/inner1
is retained accurately. Runtime dual review and Task3 acceptance remain pending.


Task4 guard entrypoint audit also confirms `_FleetExecutionGuard` currently
permits task queued/running and placement claimed/running; `allow_terminal` only
broadens the core run status. It does not authorize the proposed finishing states.
The new final pair participant and narrow post-pair operations therefore require
explicit exact accepted-point/outcome authority, not simply passing the existing
allow_terminal flag or broadly opening all writes. The original task→run→placement
→node→reservation→attempt lock order and fresh clock remain authoritative.


## Fresh v7 installed runtime passed; final runtime SPEC dispatched — 2026-10-04

Build1201 and runtime56982 actually completed0; the new wrapper propagates pytest0.
Root independently parsed final XML:2 passes,zero failures/errors/skips,
51.262 seconds. Runner immutable ID is
`sha256:51f041ce79022c56f866522b4819bca814b4c8ceefb1d3e310453a88c0eaad14`.
All6 wheels/779 actual installed members match; Root additionally mapped31 frozen
packaged source paths directly to their installed bytes, all matching. Fixed
files, all skills and7 original/new fixtures also match.

Root independently verified both actual proofs:6 writer ancestry chains terminate
at2 original registered supervisors with original64-hex tool execution IDs;
all6 old PID/start identities are absent before the first publication sf.begin,
original Popen returncodes0/0 and capture threads physically joined. The observer
precedes original request publication by the original call chain. Registry
settled_at <= request_created_at uses one PostgreSQL clock; host census-before-copy
uses one native Node monotonic clock. Container and host monotonic values are
never compared across domains.

Each scope records one seal,3 recover calls,one lost prepared response,11 renewals,
one original start,zero points and late claim409. Partial6/final5 actual censuses
contain only PID1 plus actual UID0 helper, different from workload UID501; exact
CID/StartedAt/image remain stable. Partial final budget remains unstarted; final
starts the same original budget. Both prepared candidates keep admission closed.
All new runner census states happen to beS: actual S→R reproduction remains the
retained v6 Linux RED and self-contained native regression, not a claimed fresh
v7 R/S observation.

Final runtime index SHA is
`69cbafd2af7fd4cc0c537013035217f79818f556207b4da0df7b501aab4b84e3`.
Root matched26 build inputs,45 evidence files and their snapshots; SOURCE v7
is unchanged. Final runtime SPEC is dispatched; QUALITY follows only approval.
The final candidate is produced while the original model is deliberately paused;
stock terminal pairing remains Task4. Node stop result unknown is not a raw Docker
state snapshot. Actual stop plus fresh Docker Running=false is directly asserted
in the frozen test connected to passing XML; no persisted raw poststop State is
claimed. Point count0 is a projection after actual SQL COUNT assertion, not raw SQL
row evidence. Historical v5 negatives remain tied to their original inputs/image.
No Task3 gate is checked before final runtime review, and C08 is not complete.


Final runtime SPEC v7 passes with no new P1/P2. It independently confirms exact
freeze/copies/source/install linkage and actual process/Node/SQL/clock-domain
observations, including accurate historical negative and projection limits.
Final runtime QUALITY is dispatched after SPEC approval. Task3 may be accepted
only after that quality gate; Task4–6 and full C08 remain pending.


## Task3 foundation accepted; Task4 unlocked — 2026-10-04

Root accepts Task3 against complete SOURCE v7 and final runtime v7 after sequential
SPEC then QUALITY in both scopes; all final reviews report P1/P2=0. Root's own
source/input/report/install/process/clock checks agree. The two Task3 checklist
items are checked. This accepts original writer settlement and Node staged-seal
foundation, with the accurately recorded runtime scope and limits above.
OpenSpec8.1–8.4/full C08 remain unchecked, because accepted-point transactions,
stock terminal closure, owner file API, verified restore, recovery admission and
whole release gates still require Task4–6. No intermediate commit or activation.
The next implementer must preserve this foundation and use a fresh task context;
any changed foundation bytes become new Task4 source, not a relabeled v7 proof.


## Task4 dispatched to fresh sole implementer — 2026-10-04

Fresh agent `/root/c08_task4_impl` receives the complete Task4 requirements,
accepted Task3 provenance, actual core store/saver/worker/guard entrypoints,
original monotonic120 budget and source/image review gates. It solely owns new
Task4 production/test changes and native/Docker windows; the old Task3 implementer
is completed and must not mutate the foundation concurrently. Root owns shared
planning/OpenSpec documents and acceptance. First work is actual source self-review
and TDD for stock checkpoint post-commit stage acceptance and same-session terminal
pairing, preserving original Local/B behavior. No Task4 implementation/acceptance
is yet claimed. The accepted Task3 freeze remains historical when Task4 changes
those integration files; it will not be relabeled as current whole C08 evidence.


## Task4 in progress; reboot continuation — 2026-10-04

User explicitly resumed after a system restart. Root confirmed the existing goal
remains active and continued the original feature worktree and sole Task4 owner,
without starting competing native or Docker execution. B → C → BC scope and the
original cumulative120-second final cleanup budget remain unchanged.

The neutral terminal participant is now connected to the three original
RunRepository writes in their original AsyncSession/transaction. Before/after
hooks surround the actual row mutation; a cancellation CAS with no updated row
does not accept a point. Initial neutral RED followed an import fixture repair
and is retained as tool transcript, not invented XML evidence. The original
fenced PostgreSQL saver root callback is outside its transaction/lock, and the
native test reads actual checkpoint/pending-write rows from another connection.
Its actual feature RED is1 failure/0 errors/skips (2.888 seconds), followed by
GREEN1 pass (1.681 seconds), in saver-red.xml and saver-green.xml.

Under .local/fleet-evidence/c08-task4, pair-red.xml records9 actual failures,
0 errors/skips (3.449 seconds), for the missing original terminal participant
after real NAS preparation and PostgreSQL setup. Historical pair-green-v1.xml
has9 passes (3.383 seconds), but its stale-token test had an ineffective early
return and is not token-rejection proof. The implementer removed it and reran.
Root independently parsed pair-green-v2.xml:21 passes,0 errors/skips,5.849 seconds
=14 native PostgreSQL terminal-pair cases plus7 neutral SQLite cases. The owned
execution62332 naturally exited0. The native cases exercise all three original
terminal writes, atomic core/pair rollback on insertion failure, exact identity
rejection, actual task/run row-lock waits beyond the lease, a PostgreSQL insert
trigger delay crossing the lease followed by fresh-clock rollback after flush,
concurrent duplicate/conflicting outcomes, and cancellation requiring a newly
prepared matching outcome. These are preliminary component results, not full
stock worker or Task4 acceptance.

Worker tests are still being developed; an initial frozen RunContext assignment
fixture error is classified separately from behavior RED. Evidence filenames
from that working window are not accepted freezes. Neither Root nor reviewers
may infer feature coverage solely from a nonzero exit or a report filename.
The implementer owns the active worker execution window and its natural exit.
No Task4 image has been built or accepted.

Remaining integration must preserve operator full/delta checkpoint mode. Raw
delta saver channel_values can contain sentinels, so trusted present_files turn
detection needs the original materialized checkpoint state/actual execution
identity. A new tool turn presenting the same path is a new stage; retry of the
same turn is idempotent. The final root can be an original duration/title/update
checkpoint, including interrupted-title metadata source=update; pairing must
prove exact run/ancestry and the last root rather than assuming source=loop.
Partial reopening follows actual accepted-point commit. Final finishing retains
capacity and has narrowly scoped bookkeeping/stream-seal authority until actual
Node stop. Worker wiring, cleanup failure behavior, frozen full/delta integration
evidence, and sequential whole-source SPEC then QUALITY are still outstanding.
OpenSpec8.1–8.4 and full C08 remain unchecked; no intermediate commit/activation.


Task4 worker/presentation component evidence update: Root independently parsed
worker-red-behavior.xml:5 tests,3 actual feature failures,2 Local passes,0 errors/
skips,1.021 seconds; owned execution53194 naturally exited1. The failures are
remote single/multi stream durability and missing terminal preparation. Initial
FrozenRunContext fixture assignment errors are excluded. present-red.xml has
2 tests,1 actual trusted-message-identity failure and1 Local pass,0 errors/skips,
1.006 seconds (execution11147 exit1). worker-present-green-v2.xml records7 passes,
0 errors/skips,1.186 seconds (execution10244 exit0). The earlier combined window
35375 had misplaced exception indentation and is not accepted behavior proof.
These tests establish component hooks only; actual private host wiring, partial
accept/reopen and final preparation remain in progress. No image/freeze review
or full Task4 acceptance is implied.

Root additionally confirmed original rollback cancellation targets core error
with 'Rolled back by user', rather than interrupted. Remote terminal deferral
must include the original rollback checkpoint write, and accepted final outcome
checks must handle an exact newly prepared rollback outcome or explicit typed
unsafe recovery. Existing Local ordering and behavior remain the baseline.


## Task5 entrypoint audit for later handoff — 2026-10-04

Root performed read-only source mapping while Task4 remains the sole production
implementer. This does not start Task5 or relax its Task4 prerequisite. Existing
owner GET is app/gateway/routers/artifacts.py:get_artifact, with threads/read
owner-check and internally authenticated owner normalization. Both ordinary
files and .skill archive-member reads currently resolve the original mutable
thread virtual path. C accepted versions therefore need exact private point/
checkpoint/version selection before those mutable branches, while retaining
Local/B behavior and original MIME, Range, disposition and owner authorization.
A C URL must not silently follow a later output or fall back to mutable Local
bytes; partial metadata and historical bytes must identify their exact point.

Original artifact PUT uses reserve_artifact_write through RunManager's
reserve_thread_operation. Thread delete/branch/state and regeneration preparation
use the same neutral thread-operation layer or existing runtime entrypoints;
source scope must cover each actual mutation path, not just the remote admission
router. RunRepository.create_thread_operation_atomic invokes the trusted optional
admission participant before original core run row locks, but without it the
existing core active-run check has no lasting Fleet finishing/recovery authority.
FleetRunAdmission.prepare currently serializes only Fleet goals with its own
owner/thread PostgreSQL advisory domain and locks the private task. Later Task5
must apply one shared neutral owner/thread admission domain to both Local/Fleet,
recognize trusted persisted external reservations with the plugin absent, and
preserve private parent-before-core lock order. Public kwargs/config/metadata
cannot supply or clear this reservation. Exact original idempotent reuse and
immutable read/history remain allowed; unrelated mutations remain rejected.

Original DockerAgentDriver.prepare_workspace delegates to AgentWorkspaceSnapshots
for first-launch .fleet-agent-inputs; its existing private prepared marker allows
a retry shortcut for that original input contract. An accepted C point requires
its own exact root/checkpoint/version selector and a new verified clone contract:
verify source descriptor/files and destination on every retry before granting
Agent start, including after a prepared marker already exists. Do not repurpose
the old initial-input marker as sufficient accepted-C restore proof. Later tests
must mutate/remove source or clone after one successful preparation and assert
recovery_required with zero graph/tool starts; new attempts must own independent
files rather than hardlinks or aliases. These are existing plan requirements
mapped to authoritative source, not new C09/BC routing or approval endpoints.


## Task4 partial acceptance and original host wiring — 2026-10-04

Root independently verified partial-metadata-red.xml:2 actual failures,0 errors/
skips,3.785 seconds (owned execution18935 natural exit1). A wrong-run checkpoint
metadata row was previously not rejected; a prepared partial had no exact point
accept/reopen path. Earlier partial-red.xml included duplicate-PK checkpoint
fixture trouble and is not substituted for this corrected feature RED.

Current host source wires the actual original RunRepository terminal participant,
fenced saver post-root-commit callback, original worker accessor binding/final
preparation and remote-only sync durability. Private publication observes actual
ExecutionInfo identities and checks the materialized root ToolMessage. Partial
pair acceptance runs in the original publisher session_factory transaction;
managed MCP admission and the writer controller reopen only after sf.begin exits
successfully. Exact current-root/run metadata and original presentation ancestry
checks are now added; the original fenced saver stamps the private current run
identity on its actual checkpoint transaction. These statements describe current
source, not yet accepted whole stock-graph behavior.

Root independently parsed pair-worker-green-v3.xml:24 passes,0 errors/skips,
9.026 seconds, natural execution19848 exit0. Scope is16 native PostgreSQL pair
tests +1 native saver callback test +5 worker unit tests +2 presentation unit
tests. It is not24 native graph runs, a fresh installed image or all Task4 gates.
The next owned window exercises actual stock worker full/delta graph integration;
exact accepted-final stopped/renew/stream-seal authority still requires tightening
and runtime proof. No Task4 SOURCE freeze, SPEC/QUALITY acceptance, image build,
intermediate commit or OpenSpec gate completion is claimed.


## Task4 closure audit findings before coherent freeze — 2026-10-04

Root read the actual original stopped/renew/C07 recovery entrypoints and sent
concrete integration gaps to the sole implementer. The pre-C08 stopped branch
derives successful completion from core terminal status alone; it must instead
validate the exact accepted final/paused point and apply immutable desired
outcomes. An unpaired core terminal result cannot authorize completion or END.
Both task.accepted_workspace_point_id and placement.final_workspace_point_id
must be assigned in the same original terminal transaction, not left for stopped
to infer. Finishing cleanup renewal must accept only this exact original point/
identity; running=True cannot turn finishing back into running. A real graph
pause needs core interrupted, accepted paused point, task input_required/paused
and original per-run placement/attempt cancelled semantics. A constructed paused
point with core success is only a component test of stored desired task outcome
and does not prove that real graph-pause contract.

The current partial callback also introduced an ancestry prevalidation sf.begin
before publish closes/positively joins the actual original writers. Root flagged
this against the accepted Task3 ordering evidence: all original writers must be
physically settled before the first original publisher publication transaction.
The appropriate integration is ancestry validation in publish's first fenced
transaction after original physical settlement, plus fresh validation at point
acceptance. Do not weaken the existing six-writer ancestry/pre-first-SQL observer
to make a reordered implementation pass. These are pre-freeze findings; fixes,
behavior tests and subsequent complete source review remain required.


Task4 closure component GREEN update: owned stopped RED57327 naturally exited1,
stopped-red.xml has3 cases,2 actual failures/1 pass,0 errors/skips,3.102 seconds.
Paused desired task state was incorrectly inferred as succeeded from core success;
finishing cleanup renewal was rejected as stale generation. Owned pointer/recovery
RED43526 naturally exited1; pointers-recovery-red.xml records2 actual failures,
0 errors/skips,2.488 seconds. Root independently parsed terminal-pair-green-v4.xml:
22 native PostgreSQL pair tests pass,0 errors/skips,7.575 seconds, owned execution
28323 naturally exits0. Scopes overlap earlier reports and are not summed.

Current source assigns both task/placement authority pointers atomically, uses
an exact immutable accepted-final helper for finishing renewal/physical stop and
C07 recovery, and sends an unpaired stopped core to recovery_required without END.
Source workspace lineage is now initial frozen workspace_manifest_ref then actual
accepted candidate.manifest_id, not launch-spec digest/request alias. The ancestry
prevalidation transaction was removed; ancestor is passed into publish and checked
in its original first transaction after physical writer quiescence. These source
changes and component tests resolve the preceding entrypoint findings within
their measured scope; full/delta stock graph/pause and fresh installed Linux
foundation ordering are not yet proven. Complete SOURCE freeze/reviews and runtime
acceptance remain pending; OpenSpec8.1–8.4/full C08 remain unchecked.


## Task4 actual stock graph namespace defect and native GREEN — 2026-10-04

Initial stock-graph-first.xml failed4 cases because the fixture omitted
threads_meta. It is not feature RED. Subsequent fixture-v2/v3/v4 failures are
retained with their respective setup/diagnostic limitations. The corrected
owned execution13124 naturally exited1: stock-graph-observed-red.xml has1 actual
failure,0 errors/skips,2.828 seconds (pytest log2.95 separately). Original ambient
owner binding and explicit materialized successful ToolMessage assertions now
precede the failure. Two actual present_files success messages had ordinary
generated IDs and no pending publication intents; the points assertion found
only final instead of two partial plus final.

Root independently read the installed Pregel _algo.py: root ExecutionInfo uses
node:task_id even with no parent graph namespace. Treating every nonempty
execution checkpoint_ns as a subgraph therefore suppressed original root tool
intents. The persisted saver root namespace remains empty; these two namespace
meanings must not be conflated. Owned execution19016 naturally exited1;
present-root-namespace-red.xml has3 units,1 actual failure/2 passes,0 errors/skips,
0.962 seconds (log1.03 separately). Current source validates the actual task-ID
suffix and installed NS_END/NS_SEP grammar, permits a root node task, and rejects
nested parent namespaces without a tool-name whitelist.

Root independently parsed stock-graph-namespace-green.xml:7 passes,0 errors/skips,
4.466 seconds (log4.55 separately), execution69022 natural exit0. Scope is4 native
stock graphs (full/delta × single/multi stream) plus3 namespace units. Each native
graph verifies two actual successful ToolMessages, distinct same-path tool-turn
keys, two immutable partial manifests and one final, production authenticated
ASGI HTTP/NAS candidate preparation, closed gates, core success, retained
finishing, exact final latest root and one C07 seal. This supplements future
installed lead-agent/NodeDaemon/Linux census acceptance: output bytes are fixture
prewritten, candidate sealing is driven directly by the test, and ASGI HTTP is
not a TCP daemon. It is not installed actual write_file/escaped-writer proof.

The prototype test reused stock-observation-full-1.json and GREEN overwrote its
earlier RED projection. Root had independently read all original bytes/fields
and SHA before overwrite. Root reconstructed the exact previously observed
serialization, required SHA equality before writing, and retained the original
bytes separately as root-observed-red-full-1.json, SHA
54cf89ef1181b6c93903ed52d3c58930d66ff1fe42aca41b4d5165773dfadef5. This is a retained
exact historical observation, not a newly generated expected result; current
GREEN projections must not be cited as RED evidence. Later execution evidence
requires unique per-window names. True graph pause, cancellation/rollback,
complete source freeze/reviews and installed runtime still remain pending.


## Task4 paused checkpoint observations and resume invariant — 2026-10-04

Root independently parsed present-owner-red.xml:4 units,1 actual failure/3 passes,
0 errors/skips,1.121 seconds, owned execution15341 natural exit1. A controller
now binds one immutable private execution context from the original publisher;
presentation observation validates current private context plus runtime owner/
thread/run and ExecutionInfo thread. present-owner-green.xml has8 passes,0 errors/
skips,5.723 seconds (execution73071 exit0):4 native success graphs plus4 units;
4 pause cases were deselected. No production owner fallback was changed.

Initial pause execution71827 failed a mistakenly changed success assertion and
is fixture history. Corrected76882 exits1; stock-pause-behavior-red.xml has1 actual
failure,0 errors/skips,2.324 seconds. Two original successful tool presentations
and both partial points precede the failure: a true compiled interrupt was
incorrectly accepted as final rather than paused. Remote-only materialized
pending-task classification now defers hidden goal continuation, uses core
interrupted and prepares paused/task input_required/per-run placement cancelled.
Execution25105 exits1; stock-pause-green-v1.xml has4 native success passes and
1 pause failure,0 errors/skips,5.339 seconds. The original interrupted-title raw
checkpoint clone changed checkpoint ID/step and lost pending interrupts.

The implementer added exact (name,path) pending-task remapping around remote
interrupted-title writes, keeping the Local default unchanged and rejecting
missing/ambiguous mappings. Root independently parsed stock-pause-green-v2.xml:
13 passes,0 errors/skips,7.399 seconds, owned2697 exit0. Scope is8 native graphs
(full/delta × single/multi × success/true interrupt) plus5 worker units. It proves
visible materialized pending tasks/interrupts, paused/core interrupted, desired
input_required/cancelled, retained finishing, final current root and seal in that
fixture. It does not yet prove that saved interrupt identities can receive a
real keyed resume, nor all raw duration/title clone variants.

Root checked installed types.py interrupt and Pregel _scratchpad: interrupt ID
is the hash of the executing task namespace; keyed resume only matches that
current namespace hash. Copying old Interrupt values unchanged to new task IDs
can therefore produce visible interrupts whose advertised IDs cannot resume
the new tasks. Root requested a bounded real compiled-graph resume contract test
for the original metadata helper, without adding C09 public resume policy or
starting a new final C runner. Duration metadata-only raw clones also change
checkpoint ID/step and require coverage, including nonzero duration and an
already-existing title. No full Task4 acceptance follows from visible interrupts
alone. Preserve last actual root and Local behavior; support safe true root pause
rather than redefining it as permanent recovery. Complete source review and
installed execution remain pending.


## Task4 native keyed resume and cached parallel work — 2026-10-04

Root independently read the bounded original compiled-graph test and XML. Owned
60409 naturally exits1; pending-resume-behavior-red.xml records1 actual failure,
0 errors/skips,2.733 seconds: using the advertised final interrupt ID still
re-interrupted the copied task. Owned25580 naturally exits1;
pending-duration-red.xml has1 actual failure,0 errors/skips,2.027 seconds: the
original duration raw clone lost its interrupt. Earlier35889 bind TypeError is
fixture history and is excluded from feature RED. The current neutral optional
remote preservation helper maps exact original/new task name/path, uses installed
Interrupt.from_ns to rebuild the copied task's advertised namespace identity,
and retains the original value and RESUME data. Local defaults remain unchanged.

Owned59819 exits0; pending-resume-stock-green.xml has13 native passes,0 errors/
skips,9.698 seconds:4 full/delta × title/duration explicit keyed-resume contracts
plus9 stock-worker cases. Owned55273 exits0; pending-parallel-green.xml has8
native passes,0 errors/skips,3.593 seconds:full/delta × title/duration × single/
parallel tool turns. Original compiled graph, saver and metadata helpers are
used. Before explicit Command input, the question execution count stays1; keyed
resume completes with the actual answer and no remaining next/interrupt. The
question's expected protocol reentry makes its count2, while the parallel
completed-tool execution counter remains1 and its cached ToolMessage survives.
This proves original cached parallel work is not re-executed by this explicit
fixture resume; it is not a new production C09 endpoint/AgentRunner or a Linux
restart automatic-replay proof. Historical and new overlapping scopes are not
added together.

Root also parsed stock-stopped-duration-green-v2.xml:9 native passes,0 errors/
skips,8.584 seconds, owned64486 exit0. Eight actual success/interrupt graphs plus
a duration-write fault verify physical-stopped handler SQL applying desired
input_required/task, cancelled placement/attempt and released reservation for
pause, with corresponding normal success outcomes. Actual fixture invokes the
trusted handler with its own proof flag; installed NodeDaemon/Docker physical
stop remains a separate required gate. Execution29453 process_ref fixture error
is not production RED. Cancellation/rollback, migration failure without END,
complete source freeze/reviews and fresh installed runtime remain outstanding.


## Task4 original cancellation and first-failure preservation — 2026-10-04

Root independently read the original worker branches, native stock fixture and
XML after restart. Historical87776 exits1; stock-cancellation-first.xml has3
failures,0 errors/skips,3.091 seconds, caused by the callback argument fixture.
Historical93653 exits1; stock-cancellation-behavior-red.xml has3 failures,0 errors/
skips,3.569 seconds: executor-restricted request_cancel incorrectly simulated an
external cancellation authority. These are fixture history, not feature RED.
Corrected13554 exits1; stock-cancellation-behavior-v2.xml has3 failures,0 errors/
skips,3.719 seconds. Two assertions incorrectly read the ownership-lost record
rather than the original desired terminal outcome. The rollback-fault case is
an actual feature defect: the original rollback exception was swallowed and
replaced by terminal preparation failure. Execution29944 exits1;
stock-cancellation-green.xml has2 passes/1 failure,0 errors/skips,3.271 seconds:
finally publish_end still replaced the typed first failure with OwnershipRejected.

Remote cancellation rollback faults now preserve a typed first failure and mark
ownership lost. The remote finally path neither publishes END nor schedules
stream cleanup after ownership loss; the Local default is unchanged. Owned65162
naturally exits0; stock-cancellation-green-v2.xml has3 native passes,0 errors/
skips,3.475 seconds (pytest log3.54). The fixture uses original external durable
RunRepository.request_cancel and original manager cancellation signal after two
accepted partial turns. Preparation observes original interrupted or Rolled back
by user outcomes; cancellation, rollback and rollback fault do not accept a final
point or stream seal. Trusted stopped-handler SQL retains task/placement
recovery_required and releases the physical reservation. This fixture passes its
own physical_stopped proof flag; it is not installed Docker stop evidence.

Owned97902 naturally exits0; task4-focused-green-v5.xml records60 passes,0 errors/
skips,20.750 seconds (pytest log20.90). Scope is44 native cases (13 stock,22 pair,
1 saver,8 pending resume) plus16 neutral units (7 SQLite,5 worker,4 presentation).
Overlapping earlier scopes are not summed. Whole source freeze, sequential SPEC
then QUALITY, fresh installed
runtime and Task4 acceptance remain pending. OpenSpec8.1–8.4 stay unchecked;
Task5/6, C09–C12, BC and activation remain outstanding. Final cleanup retains the
original cumulative120 seconds, without per-phase resets.


## Task4 remaining original finally paths before source freeze — 2026-10-04

Root source audit found additional original remote finalization paths needing
explicit resolution before the complete Task4 source gate. The edit-replay
rollback catch still logged generic exceptions, and the late-cancellation
interrupted-title catch still logged failures before attempting new preparation.
The completion-update catch also treated generic participant/SQL failures as
nonfatal. The sole implementer is checking actual reachability and adding
remote-only typed fail-closed behavior plus meaningful original worker evidence
where required. Local behavior must remain unchanged. This is an implementation
follow-up, not a completed source review or acceptance. No reviewer or installed
image build starts until the whole coherent Task4 source freeze is ready.


## Task4 C07 neighbor source integrity follow-up — 2026-10-04

Root independently parsed task4-neighbors-first.xml:253 cases,8 failures,
0 errors/skips,34.241 seconds. The failed expectations concern the previous
core-terminal-only C07 seal fixtures, which now require an exact accepted final
point. A later c07-paired-neighbor-green.xml records27 passes,0 errors/skips,
9.117 seconds; natural execution status and exact current bytes still require
implementer confirmation. These are not a green253-case neighbor gate.

A subsequent current-source read found test_c07_event_transactions.py overwritten
with the stock Task4 graph fixture, rather than a limited paired-authority fixture
adaptation. At this observation its18,382 bytes had SHA256
1c5207dc1997dc7889166217d907db261072a59d7c9b055bb3340b59244328e4.
Root immediately notified the sole implementer to restore the complete original
C07 coverage and retain only justified positive-fixture changes, keeping the old
core-only behavior as a meaningful rejection case. No source freeze or acceptance
can rely on that transient overwritten file or the earlier27-case report. Root
did not mutate production/tests or poll the implementer's sessions.


## Task4 neighbor restoration and actual completion SQL RED — 2026-10-04

The sole implementer confirmed13032 naturally exits1 for the253-case neighbor
window (245 passes/8 failures), and81420 exits0 for the prior correctly adapted
27-case C07 window. The accidental C07 overwrite came from reusing the stock
fixture text variable for the second output path. The implementer restored HEAD's
complete C07 file, then retained only exact prepared-pair/original same-transaction
RunRepository positive setup, the new ownership rejection expectation, and an
explicit core-terminal-without-point negative. Root independently read the
restored diff:33 additions/5 deletions, with original event/pointer/rollback tests
retained. The accidentally written bytes are archived only as debugging history.
A fresh current-byte C07 rerun is still required; old27-pass evidence is historical.

Owned46393 naturally exits1. completion-sql-red.xml records1 actual failure,
0 errors/skips,2.617 seconds (pytest log2.69): a real PostgreSQL completion trigger
raised, but the original RunManager.update_run_completion swallowed the SQL
exception, so the stock worker failed to raise its typed workspace failure.
The neutral trusted-participant path must propagate this failure without changing
Local default policy or importing Fleet into the harness. A completion fault
occurs after a legitimate accepted final pair: it must preserve that immutable
point while preventing live-worker END. Subsequent authenticated physical-stop
C07 recovery may use that exact accepted point; it must not relabel an unpaired
or partial-only execution as complete. Manager/source-guide changes and real
original-worker evidence belong in the complete Task4 freeze and reviews.


## Task4 original manager CAS routing defect — 2026-10-04

Owned47613 naturally exits1; final-checkpoint-tail-red.xml records2 failures,
0 errors/skips,3.123 seconds (pytest log3.19). The edit-replay case actually enters
the original finally rollback path: its rollback fault is logged and an exact
pair is subsequently accepted, rather than raising the required typed failure.
The late-title case uncovers an earlier routing issue and is not yet a pure
late-title failure proof. Both the production remote host and native fixture
construct a RunManager with heartbeat disabled by default. Its original
set_status_if_not_cancelled then bypasses finalize_if_not_cancelled CAS and
calls update_status; when cancellation wins, the old success candidate is
rejected by the terminal participant, the manager absorbs the rejection, and
the actual late-title path never runs.

The sole implementer is retaining the original CAS implementation and enabling
it through a neutral trusted terminal-participant contract independently of
heartbeat settings. Local without a participant retains its existing routing.
Completion SQL propagation uses the same neutral boundary, without app/Fleet
imports. A zero-row cancelled CAS must not execute after_transition or accept
its old prepared point. The corrected late-title path still needs its own
actual fault proof before source freeze. Existing current-status, conditional
status and completion paths remain required; the whole Task4 source/runtime
gates and all later delivery work stay pending.


## Task4 corrected manager and original fault paths GREEN — 2026-10-04

Root independently read the corrected original worker/manager and current XML.
Owned11226 naturally exits0; final-tail-completion-green.xml records3 native
passes,0 errors/skips,3.646 seconds (pytest log3.76). Remote edit rollback and
actual late-title faults preserve typed first failures; a completion SQL trigger
fault is propagated through the original manager and worker. The latter retains
the already accepted immutable final point, emits no live seal, and the trusted
stopped-handler fixture later applies the exact successful outcome and C07 seal
without changing the point. This handler fixture remains distinct from raw
Docker physical-stop evidence. Local without a trusted participant keeps its
prior best-effort behavior and cancellation routing.

Owned62689 naturally exits0; task4-coherent-green-v6.xml records94 passes,
0 errors/skips,28.690 seconds (pytest log28.84). Scope is78 native cases:16 stock,
25 pair,28 restored C07,8 pending resume and1 saver; plus16 neutral units:7 SQLite,
5 worker and4 presentation. Earlier overlapping60/27 scopes are not summed.
The current C07 file retains the original coverage, adapts positive closure
through the original same-transaction participant and adds core-only rejection.
The whole neighbor window remains assigned to sole implementer52003; Root does
not poll its handle or run another native window.

The fresh installed Task4 fixture must observe automatic first presentation
publication rather than Task3's controlled model-pause/manual publication.
Root inspected the old fixture: installing its six-writer/pre-first-SQL observer
only after the parent reaches its final model pause would now occur too late.
Observation must bind before the actual root callback publishes, preserve the
original six-writer ancestry and positive join proof, then delegate the original
publisher/accept transaction. A subsequent actual root write and MCP invocation
must demonstrate same-owner reopening/reconnect only after partial commit,
followed by a second same-path presentation and a true normal/pause terminal
pair. No image build occurs before complete source SPEC then QUALITY. Task4,
Task5/6, full C08 and later C/BC acceptance remain pending.


## Task4 whole neighbor GREEN and actual clarification protocol — 2026-10-04

Owned52003 naturally exits0; Root independently parsed
 task4-neighbors-green-v2.xml:324 passes,0 errors/skips,34.400 seconds (pytest
log34.51). Scope includes restored C07/C05, original RunRepository/RunManager,
checkpoint state/lineage and stock subgraph/no-checkpointer paths. This completes
that exact current-byte neighbor window; overlapping94/60 scopes are not added.
No other native window was active when the implementer reported this exit.

Preparing the real installed lead-agent fixture exposed a missing product path.
Root independently read ClarificationMiddleware and its guide: the original
ask_clarification produces a ToolMessage with artifact.human_input and returns
Command(goto=END). It is not a LangGraph interrupt, so snapshot.next/tasks can
be empty while the Agent is actually waiting for human input. The earlier native
compiled interrupt tests prove that separate graph protocol; they cannot stand
in for stock ask_clarification pause semantics.

The sole implementer is adding a minimal neutral trusted observation at the
original middleware ToolCallRequest runtime boundary. Original ExecutionInfo
and private current owner/thread/run must identify a successful root human-input
intent, matched to its actual materialized ToolMessage in the current run.
Historical human-input artifacts, client context, nested subgraph asks and
suppressed clarification must not independently pause the root. Existing
request/tool-call and answered-card/journal protocol associations stay intact.
Worker and publisher must classify this actual stock waiting state as a paused
point/task input_required/core interrupted, while preserving actual compiled
interrupt support and Local default behavior. Native original middleware
full/delta × stream-shape RED/GREEN and subsequent affected neighbors are
required before the complete source freeze. The prior324-case window is evidence
for its earlier bytes, not future middleware changes. No C09 public resume policy,
new runner, image build, source acceptance or C08 checkbox follows from this audit.


## Task4 native stock human-input observation isolation — 2026-10-04

Root independently read the original middleware/controller/publisher and XML.
stock-clarification-red.xml records1 actual wrong-kind failure,0 errors/skips,
2.708 seconds: the original ask request produced final rather than paused.
stock-clarification-green.xml records4 passes,0 errors/skips,4.751 seconds,
covering full/delta × single/multiple stock stream modes with the actual
ClarificationMiddleware. Owned2764 naturally exits1 for the RED and6535 naturally exits0 for the GREEN,
confirmed by the sole implementer. These scopes remain native stock fixtures,
not installed Linux execution.

Owned84918 naturally exits1 for human-observation-negatives.xml:1 failure,
0 errors/skips,2.027 seconds. Direct ainvoke omitted the actual runtime context;
this is a fixture authority error, not feature RED. Owned73273 naturally exits0;
human-observation-negatives-v2.xml records8 native passes,0 errors/skips,
3.274 seconds (pytest log3.36). It uses the actual original compiled graph,
clarification middleware and PostgreSQL saver in full/delta. Nested graph asks
and suppressed clarification do not create root observations; wrong runtime
owner is rejected before a ToolMessage; original card request ID equals the
actual ToolMessage ID and provider tool-call correlation is preserved.

The historical case first obtains a real ask checkpoint, then binds a fresh
controller to another private run identity and verifies the old snapshot does
not grant waiting-input authority. This is observer/run-identity isolation,
not a second admitted RunManager/AgentRunner execution. Root did not relabel it
as a new-run end-to-end test. True compiled graph interrupts and keyed resume
remain a separately proven protocol. The latest source handles stock ASK END
without replacing the original request/card or response protocol. Whole affected
neighbors for these new middleware bytes, complete source freeze/reviews and
fresh installed execution still remain pending. No native handle was active when
the implementer reported73273; subsequent windows remain solely its ownership.


## Task4 current human-input neighbors and build-input RED — 2026-10-04

Owned69673 naturally exits0; Root independently parsed
 task4-human-coherent-v7.xml:172 passes,0 errors/skips,34.479 seconds (pytest
log34.57). Actual scope is90 native cases (20 stock,8 human observation,25 pair,
8 pending resume,1 saver,28 C07) plus82 neutral/Local cases, including the original
clarification/human-input/drop-siblings neighbors. Earlier overlapping94/324
counts are not summed. Original card/request ID and Local suppression/mixed-tool
behavior remain covered alongside the actual remote ASK END classification.

Owned89453 naturally exits1; stock-image-inputs-red.xml has2 source-only cases,
1 actual failure/1 pass,0 errors/skips,0.667 seconds. The prepared build inventory
is missing c08_stock_linux_fixture.py and its exact plugin entrypoint. No Docker
or native PostgreSQL call was made by this source-only window. The implementer
is now adding the early-bound installed stock observer and complete original
terminal lifecycle fixture plus build inputs; it has not yet built or frozen a
new runtime. The source-only inventory test cannot substitute for fresh wheel/
installed byte comparison or real TCP/NodeDaemon/Docker behavior. Whole Task4
source SPEC then QUALITY must precede the new image, and runtime review must
precede Task4 acceptance/Task5. OpenSpec8.1–8.4 remain unchecked.


## Task4 installed observer source preflight — 2026-10-04

Root read the new c08_stock_linux_fixture.py before source freeze. It observes
original automatic root callback/partial acceptance and delegates production
methods, preserving original full C04 lead/child checks and six-writer ancestry.
Root caught two concrete construction/query errors: direct assignment to frozen
RunContext.prepare_terminal and JOIN against nonexistent request.request_id.
The sole implementer corrected these with dataclasses.replace and the original
WorkspaceRequestRow.id join; it also corrected the budget remaining() call.
Prepared-only probes now catch the exact original writer ownership/barrier
exception and McpScopeBarrierClosed, so an unrelated startup/config error cannot
be counted as proof that gates stayed closed.

Owned14169 naturally exits0; stock-image-inputs-green.xml records2 source-only
passes,0 errors/skips,0.642 seconds. The prepared inventory now contains the exact
new observer bytes and installed c08-stock entrypoint; Docker invocation is
prohibited in that test. Root independently parsed stock-installed-preflight.xml:
4 passes,0 errors/skips,1.910 seconds (pytest log2.01), owned82123 natural
exit0 confirmed. Scope is1 real PostgreSQL committed pair/request query,1 actual
frozen RunContext construction with a stub host builder, and2 source-only input
checks. It is not an installed AgentRunner or Docker gate. Full daemon lifecycle
normal/pause tests and source inventory are still being prepared. Complete source
SPEC then QUALITY must pass before any new image build; whole C08/later stages
remain unaccepted and the original cumulative120 cleanup budget is unchanged.

## Task4 accepted source and actual installed runtime — 2026-10-04

Root accepts Task4 after full SOURCEv9 SPEC then QUALITY and RUNTIMEv7 SPEC
then QUALITY, all P1/P2=0. SOURCE manifest SHA
`aa4bed8212fa34f0c14db845dbe60815dce672d8fe8fac80796d631530983d6b`:
138 sources/115Python/122reports/44production targets. Runtime manifest SHA
`94998d9ddf2682d4bda0d674a89d9fd23c3b15d44415edd376f39e133b1ee122`:
fresh image `sha256:2d423f3b18b1f5838648702809d94b1821ee055b4b3a35890a8728b703f2bf68`,
sixwheels779unique nonRECORD bytes and44source targets verified. Actual original
strict full/delta × normal/ASK four-case gate passes90.866seconds, zero skips.
Eachcase actual firstSQL sixwriterjoin, two accepted partial/reopen boundaries,
MCP reconnect, exact final/paused core+finishing pair/bothpointers, SDK join,
original completeclose with unchanged cumulative120deadline and rawDockerExit0
verified independently by Root and both reviewers. Six historical failedimages
and native failure/green windows remain retained, counts not summed.

This accepts Task4 only. Task5/6, fullC08/OpenSpec8.1–8.4, commit, C09/newsession
restart, BC and activation remain incomplete. Frozen SOURCEv9 contains the
normative plan bytes before this acceptance annotation; historical hashes are
not rewritten. Task5 begins with this new documented acceptance baseline.
