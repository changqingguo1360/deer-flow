# BC10 Installed combined release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Qualify the actual installed combined B/C release through two original workers, across-node C→B→C execution, partition/cancel/restart safety and an operation manual.

**Architecture:** Reuse original production image recipes/private bootstrap, normal Gateway lifespan, stock mixed Node CLI and original PostgreSQL/Redis/NAS stores and protocols. One installed main precedes one concentrated required fault boundary; a real frontend browser observes the same owned flow. Existing B/C acceptance retains its scope and must not be relabeled fresh execution.

**Tech Stack:** Original Python3.12/FastAPI/PostgreSQL/Redis, Docker execution images, shared NAS, scripted model, Next.js/Playwright through `scripts/pnpm.py`.

---

## 分级交付（2026-10-07 用户要求调整）

P0 可演示主干已交付，本地提交 `be92d9cf`。接下来按 P1 可靠性验收、P2 发布完善依次收尾，分别汇报完成状态。原 BC10 完整验收要求保留，P0 完成不代表 BC10 或生产发布已验收。

| 级别 | 范围 | 完成标准 | 当前安排 |
| --- | --- | --- | --- |
| P0 核心可用 | 修复已证实的镜像及模型配置缺口，运行一条真实双节点 C→B→C 主干及同流程页面 | B 结果被 C 接收并完成同一任务；正常路径真实 STOP、资源释放、无重复执行有证据；必要变更检查及简短运行说明 | 已完成；保留现有证据，不重复扩大主干测试 |
| P1 可靠性验收 | 完整镜像源码一致性；一个集中断网、取消、重启恢复场景；聚合验收证据；最终 SPEC/QUALITY 审查 | 原 BC10 必要故障与恢复要求有真实证据，修复影响可靠性的缺陷 | 下一阶段；限定下列四项，不扩展测试矩阵 |
| P2 发布完善 | 剩余 README、模块指南、进度/发布文档全面同步，运行维护材料整理 | 文档与最终能力及限制一致 | 后置；生产 ECS 部署需另行授权 |

### P1 收尾顺序与范围上限

1. 更新含旧 Gateway router 的执行镜像，核对安装后的源码与当前提交一致。镜像变更后复验同一条主干一次，复用原浏览器流程；已通过且未受影响的 B/C 验收不重跑。
2. 只做一个集中故障场景，覆盖真实断网、取消和 Gateway/Worker 重启，并验证无 STOP 证据时拒绝释放、有真实 STOP 后允许释放。保留原120秒安全预算，不拆成故障矩阵。
3. 核验聚合证据，从实际 XML、退出收据及源码映射推导通过、跳过和资源释放结果，复用已有 B/C 证据并标明范围。
4. 最终 SPEC、QUALITY 各一轮。只有阻碍本级验收的具体缺陷触发修正和受影响检查；其他改善记录到 P2 或后续迭代。

P1 完成即交付可靠性验收结果，不等待 P2 全面文档整理。P2 同步发布文档及运维材料；生产 ECS 部署、额外故障矩阵、性能压测和新功能均不纳入本轮收尾。

基本正确性、正常路径资源释放和必要代码检查属于 P0，不能为赶进度跳过。故障场景及完整发布资格未完成时必须明确标注，OpenSpec task10 不提前勾选。已通过且源码未受影响的 B/C 检查不重跑；不增加测试矩阵，不建立新一轮无关审查。

## Authority and ownership

Feature checkout `/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs`, branch `feature/personal-agent-ecs`. BC09 source `be343091cd16aae604dd689f445cc0f0d082f504`, acceptance docs `329648e6c63abf88e8884305ae77991f7ee670f6`; all16 reviewed source blobs matched. B, C01–C12, BC01–BC09 retain their recorded local scopes. OpenSpec `add-ecs-agent-job-continuations` task10 / `Unified release gate demonstrates C B C execution` and original master BC10 define this gate; no native-only substitute or narrowing of the final goal.

Root owns plans/docs/reviews/evidence/commits. A fresh sole author owns all source/test edits, every image build, runtime launch/poll and owned cleanup. Exact frozen candidates are reviewed before application. Existing user authorization covers local edits/builds/tests/commits; no push, merge, production ECS activation or external messages. Author reads actual feature-checkout module guides; the default checkout's monorepo paths must not substitute for actual feature paths.

The user requires main first and minimal directly affected verification. Preserve all named deliverables without a broad matrix. Do not repeat unchanged passed selectors, B/C suites or image builds. A timeout is an observation, not proof of termination: author polls the original handle and records natural exit/cleanup. No shadow runtime, synthesized expected observation or skipped required case.

## Original seams and deliverables

- Original stock worker: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/__main__.py` (`WorkerSettings.kind` supports `job|agent|mixed`, private settings, original mixed claim/daemon/container driver/journal).
- Reuse `backend/tests/fleet/c12_integration_fixture.py` normal Gateway, original account/session/SQL/private configuration and stock-node launch semantics; `test_bc05_fleet_agent_job_continuations.py` installed single-slot lifecycle and deterministic child provider. Extend only required fixture interfaces; do not copy a second authority implementation.
- Production build/qualification: `scripts/fleet_agent_build.py`, `scripts/fleet_c_gate.py`, `docker/fleet/agent.Dockerfile`, `docker/fleet/worker.Dockerfile`. Compare installed members/current source and original recipe receipts; old BC05/C12/B-base images are prerequisites, not fresh BC10 proof. Build only the necessary source-qualified image(s), never a tag per case.
- Create `backend/tests/fleet/test_bc10_fleet_unified_task_experience.py`: actual installed main and one concentrated fault boundary; final node IDs must be confirmed after fixture design, not treated as already executed.
- Create `backend/tests/fleet/test_bc_acceptance.py`: aggregate gate reads actual collected IDs/PIDs/SQL/manifests/STOP/release and source qualification. No hardcoded success facts; distinguish carried evidence from fresh execution and fail on missing required evidence/skips.
- Create `frontend/tests/e2e/fleet-continuation.spec.ts`: one real browser check against the same original authenticated Gateway/Next.js flow, with no mocked task/API success. Reuse original `frontend/playwright.config.ts` external-server mode or minimally configure the needed real backend; the existing replay config must not replace this installed Gateway with a replay/test-seeded service. Host commands run through `python3 scripts/pnpm.py`.
- Root synchronizes `docs/deployment/ecs-fleet.md`, README/module guides, development/runtime/acceptance records, roadmap/progress/OpenSpec10. Operation manual preserves flags-off default and stages jobs→agents→continuations, drain before rollback, histories/manifests/unresolved reservations retained.

## Required proof

Main uses two separately identified original stock workers, genuine PostgreSQL/Redis/shared NAS and scripted model. A real remote C submits original named B work, accepts a checkpoint/workspace pair, yields with physical STOP and released capacity; original B processes publish accepted manifests; the original continuation runs on a different node and consumes accepted results to finish the same durable goal. Use original authenticated machine policy/drain/admission to make node change observable; never assign SQL rows or invent placement. Record every task/run/job/attempt/node/session ID, actual PID/container identity, accepted checkpoint/workspace/manifests and ledger/writer ownership. Determine `c_b_c_across_nodes`, duplicate effects, writer overlaps and capacity leaks from those facts.

The browser observes the same owned waiting/completed goal, real related jobs/runs/result IDs and truthful goal/run/STOP presentation. Existing BC09 controlled DOM evidence remains distinct. Do not run a second compute graph solely to create UI screenshots.

Only after main GREEN, the same concentrated boundary injects actual control-plane partition/recovery, authenticated cancellation and worker/Gateway restart using original durable stores/journals. Physical execution must stop under the original lease/cancellation rules; restoring connectivity must not duplicate child effects or admit concurrent thread writers. Manual quarantine release without genuine STOP proof is refused; accepted release uses the original STOP evidence, not a DB timestamp mutation. Record natural processes/SQL/reservations and actual refused/accepted API outcomes. Preserve the original120-second safety budget; do not shorten protocol limits merely to pass a test faster.

No missing environment, unregistered fixture or setup error counts as product RED. If baseline already satisfies this gate, record observed baseline success honestly and do not invent a failing product requirement. Fixture/integration failures are corrected separately; only demonstrated product gaps authorize minimum protocol edits with tests.

## Steps

- [x] **Step1 — Design and freeze installed main.** Fresh author provides exact lifecycle, two-worker routing, image/current-source qualification, original auth/private stores and cleanup design. Root reviews design before fixtures/builds. Freeze fixture patch and configured static results before application.
- [ ] **Step2 — Execute the installed main.** Author preflights genuine dependencies, builds only required source-qualified recipes and runs the single main node ID. Capture raw evidence before assertions, natural exit and exact owned cleanup. Correct demonstrated gaps minimally; main must pass before faults. No automatic second graph or broad test suite.
- [ ] **Step3 — Real browser and aggregate main evidence.** Same owned flow through actual Next.js/authenticated Gateway, named Playwright artifact and aggregate collector. Record native/browser/source distinctions and actual selection/skips. If servers must remain alive for browser, freeze that orchestration in Step1; do not rebuild/reexecute the main just because post-test capture was omitted.
- [ ] **Step4 — One concentrated necessary fault boundary.** Freeze exact partition/cancel/restart/STOP-quarantine fixture after main GREEN. Author owns handles and natural cleanup; no matrix. Fix only material failures and rerun only affected selector.
- [ ] **Step5 — Freeze/static/operation manual.** Configured actual-path Ruff/check-format/Python3.12 for changed files, targeted frontend format and real frontend check if affected, diff whitespace/strict OpenSpec/guidance checks. Freeze full source/image/member/receipt mapping. Explicitly account for every required artifact and gate; no false fresh-execution or skipped-case claims.
- [ ] **Step6 — Fresh SPEC then QUALITY and local commit.** Independent review of final source and raw installed/browser/fault evidence. Minimal fixes and affected validation only. Root commits exact reviewed blobs, verifies them, records acceptance and checks OpenSpec10 from real evidence. The overall goal remains active until its full requirement-by-requirement completion audit passes.

## Current state

P0 已通过 main09 的真实双节点 C→B→C 和同流程浏览器检查：主干1passed79.84s，浏览器1passed43.56s；重复副作用、线程双写、容量泄漏均为0，真实 STOP/持久释放及自有清理完成。必要静态检查通过，见 [P0 交付记录](../../ecs-fleet-bc10-p0.md)。

P0 修正了配置、镜像依赖和读取历史误触发取消的问题。当前正常 Gateway 使用最新本地 router；保留的执行镜像内未使用的旧 Gateway router 尚未更新，因此完整当前镜像成员资格仍归 P1。上面的完整 BC10 步骤不会因 P0 演示通过而提前勾选。

B、C01–C12、BC01–BC09 保留已记录的验收范围。P1 的一个集中故障验收、完整镜像/聚合证据和最终审查尚未执行，P2 文档全面同步后置。OpenSpec task10 保持未完成，无部署或 operator activation。
