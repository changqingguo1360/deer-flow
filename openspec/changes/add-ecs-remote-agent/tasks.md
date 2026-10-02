# C：完整远程 Agent tasks

前置：add-ecs-fleet-jobs 验收通过，表与协议已迁移。详见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-c-remote-agent.md)。
只有实际实施、审查与验证完成的项目才勾选；规划或 CLI 校验成功不代表实现完成。

## 1. C01 扩展远程放置模型与启动描述

- [x] 1.1 写并运行 backend/tests/fleet/test_c01_remote_agent_admission.py，确认 C01 行为测试 RED。
- [x] 1.2 完成计划列出的接口、事务和部署接线；满足 `Versioned remote launch specification`。
- [x] 1.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 1.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 2. C02 建立 Local/Fleet 后端契约与原子准入

- [x] 2.1 写并运行 backend/tests/fleet/test_c02_remote_agent_admission.py，确认 C02 行为测试 RED。
- [x] 2.2 完成计划列出的接口、事务和部署接线；满足 `Atomic remote admission with local parity`。
- [x] 2.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 2.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 3. C03 统一 claim 与 run ownership 续约

- [x] 3.1 写并运行 backend/tests/fleet/test_c03_remote_agent_admission.py，确认 C03 行为测试 RED。
- [x] 3.2 完成计划列出的接口、事务和部署接线；满足 `Single execution owner across placement and run`。
- [x] 3.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 3.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 4. C04 启动复用 run_agent 的完整 runner

- [x] 4.1 写并运行 backend/tests/fleet/test_c04_remote_agent_runner.py，确认 C04 行为测试 RED。
- [x] 4.2 完成计划列出的接口、事务和部署接线；满足 `Full runtime execution on worker`。
- [x] 4.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 4.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 5. C05 实现 checkpoint 事务内 fencing

- [x] 5.1 写并运行 backend/tests/fleet/test_c05_remote_agent_runtime.py，确认 C05 行为测试 RED。
- [x] 5.2 完成计划列出的接口、事务和部署接线；满足 `Fenced checkpoint writes including pending writes`。
- [x] 5.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 5.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 6. C06 覆盖 memory、扩展和最终状态写入

- [ ] 6.1 写并运行 backend/tests/fleet/test_c06_remote_agent_runtime.py，确认 C06 行为测试 RED。
- [ ] 6.2 完成计划列出的接口、事务和部署接线；满足 `All remote durable mutations respect ownership`。
- [ ] 6.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 6.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 7. C07 持久事件 outbox 与可恢复 SSE

- [ ] 7.1 写并运行 backend/tests/fleet/test_c07_remote_agent_runtime.py，确认 C07 行为测试 RED。
- [ ] 7.2 完成计划列出的接口、事务和部署接线；满足 `Committed ordered remote events`。
- [ ] 7.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 7.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 8. C08 实现 C workspace 和 checkpoint 联合恢复点

- [ ] 8.1 写并运行 backend/tests/fleet/test_c08_remote_agent_runtime.py，确认 C08 行为测试 RED。
- [ ] 8.2 完成计划列出的接口、事务和部署接线；满足 `Consistent workspace checkpoint boundary`。
- [ ] 8.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 8.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 9. C09 远程取消、人工中断与故障隔离

- [ ] 9.1 写并运行 backend/tests/fleet/test_c09_remote_agent_operations.py，确认 C09 行为测试 RED。
- [ ] 9.2 完成计划列出的接口、事务和部署接线；满足 `Remote cancellation and safe recovery`。
- [ ] 9.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 9.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 10. C10 路由 preference 与 Scheduler 票据接入

- [ ] 10.1 写并运行 backend/tests/fleet/test_c10_remote_agent_admission.py，确认 C10 行为测试 RED。
- [ ] 10.2 完成计划列出的接口、事务和部署接线；满足 `Authorized routing and queued scheduler budget`。
- [ ] 10.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 10.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 11. C11 C 任务摘要、部署和本地回归

- [ ] 11.1 写并运行 backend/tests/fleet/test_c11_remote_agent_operations.py，确认 C11 行为测试 RED。
- [ ] 11.2 完成计划列出的接口、事务和部署接线；满足 `Remote task visibility and reversible enablement`。
- [ ] 11.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 11.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 12. C12 C 故障验收门槛

- [ ] 12.1 写并运行 backend/tests/fleet/test_c12_remote_agent_operations.py，确认 C12 行为测试 RED。
- [ ] 12.2 完成计划列出的接口、事务和部署接线；满足 `C release gate covers all remote mutation paths`。
- [ ] 12.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 12.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。


## C01 actual evidence — 2026-10-02

Foundation only: immutable private launch/task/placement data, caller-owned session
repositories and actual host prerequisite guards. Gateway still refuses remote Agent
activation; C02 admission, C03 claims and later runner/fences remain unchecked.

Genuine RED: five behavior failures (missing pinned agent runtime accepted; actual
Gateway accepted four incomplete persistence/event/heartbeat configurations), recorded
in implementation thread outputs. No standalone RED5 log was retained. Two additional
actual model/version rejection failures: /private/tmp/c01-model-red.log. Fixture/import
and timestamp mismatch failures were not counted as behavior RED.

Implementation GREEN23/0; installed7/f0007 and neighboring B97/0. Independent root:
C01 23 passed/0 skipped (1.82s), /private/tmp/fleet-c01-root.xml and .log; retained
old worker B gate259/0 (123.34s), /private/tmp/fleet-c01-root-b-gate.xml and .log.
Default make test13208 passed,197 optional skipped,1 deselected,19 known warnings
(271.45s), /private/tmp/fleet-c01-root-full-test.log. C01 required PostgreSQL scenarios
were independently executed without skips. Blocking-I/O75/0 (5.68s); guidance/thread
contracts92/0 (1.82s); all backend Ruff1390 clean. Spec and quality/security review
approved. This slice commit includes these records; exact hash follows in progress.


## C02 actual evidence — 2026-10-02

Trusted internal backend admission only; Gateway still refuses agents_enabled.
Single SQL UoW joins core run and Fleet task/spec/placement. Actual run insertion
was observed before placement fault; errors and CancelledError leave all four tables
empty. Remote pending admissions have no Gateway owner/lease or local task.
Local baseline211/0; C02 eighteen scenarios plus neighbors438/0.

Initial trusted-entry RED1: /private/tmp/c02-entry-red.log (production keyword absent).
Separate sensitivity negative control intentionally used independent transactions,
and the rollback test failed on retained run count1; /private/tmp/c02-atomic-negative-control.log.
Correct SQL was restored byte-for-byte before GREEN. Fixture/advisory binding mistakes
were not counted as original behavior RED. Three independently started processes
use a release barrier and return one associated run/task/spec/placement group.

Root C01+C02 PG41/0 (4.97s), /private/tmp/fleet-c02-root.xml and .log; retained
worker B gate259/0 (122.79s), /private/tmp/fleet-c02-root-b-gate.xml and .log.
Default make test13211 pass,212 optional skip,1 deselected,19 known warnings
(270.34s), /private/tmp/fleet-c02-root-full-test.log. Required C01/C02 PG cases
were separately executed without skips. Blocking-I/O75/0 (4.39s), guidance/thread
92/0 (1.94s), all backend Ruff1395 clean, strict OpenSpec3/3 and diffcheck clean.
Spec and quality/security reviews approved. This slice commit contains the evidence;
its exact hash is recorded in implementation-progress afterwards. C03 and later
claims/runner/routing/fences remain unchecked.


## C03 actual evidence — 2026-10-02

Spec and quality/security review approved thirteen frozen source/test files.
Four unique genuine RED behaviors (overlapping logs are not additive): queued remote
run wrongly terminalized by Gateway recovery (c03-recovery-red.log); actual admin
Agent capacity registration returned422 (c03-red.log); raw github_token accepted
(c03-secret-red.log); cross-node Agent start leaked kind through503 rather than403
(c03-node-scope-red.log). All logs reside in /private/tmp. Fixture/configuration
failures are excluded. Focused GREEN33/0 (4.33s), c03-final-green.log; neighboring
GREEN486/0 (23.19s), c03-neighbor-green.log (before the final cross-node test).

Root C01-C03 PostgreSQL74/0 (7.65s), fleet-c03-root.xml/.log; unchanged retained-image
B gate259/0 (121.67s), fleet-c03-root-b-gate.xml/.log. Default backend13217 passed,
239 optional skipped,1 deselected,19 known warnings (270.58s), fleet-c03-root-full-test.log.
Required PostgreSQL/container tests were independently executed without skips.
Blocking-I/O75/0 (4.75s), fleet-c03-root-blocking-io.log; guidance/boundary tests26/0
(1.37s), fleet-c03-root-guidance.log; thread route contracts61/0 (1.70s),
fleet-c03-root-thread-contract.log. Full backend Ruff1397 clean; strict OpenSpec3/3.
Actual guidance checker has0errors/4soft chain-size warnings; the same four paths
warn at parent HEAD, with no new warning category/path. Strict-warnings therefore
is not reported as passing. Diff check clean.

Claim/renew atomically bind core run and attempt to identical leases and shared
capacity. Local SQL recovery/ownership mutations exclude remote placements and
server-owned nonlocal backend labels; hydration and scheduler recovery preserve them.
Node bearer kind dispatch verifies scope, explicit Agent profiles require positive
capacity, legacy defaults remain jobs, and accepted renewal survives closed flags.
No migration change. Raw runtime github_token now requires out-of-band references.
Gateway activation stays closed. C04 runner, complete cancellation/physical stop,
Agent read/reconcile and all remaining C/BC work are outstanding.


## C04 independent local acceptance — 2026-10-02

Formal spec and quality/security re-review approved the final52file freeze.
The two P1 repairs have actual PG unchanged-row lock-wait and decoded/query/legacy
credential RED/GREEN evidence. Original genuine REDs and the intermediate optional
Redis loading regression remain separate, retained logs; no fixture failure or
API absence was counted as a behavioral RED. Redis parsing stays lazy, with no
new dependency version. Gateway activation is still closed; C05-C12/BC are pending.

Child final92/0/2warnings76.10s and related313/0/3warnings17.14s:
/private/tmp/c04-clock-secret-fix-final-full-matrix.log and final-neighbor.log.
Root final runner92/0 (61.67s), C01-C03
74/0 (11.30s), unchanged retained-image B gate
259/0 (120.37s), with actual required PostgreSQL and
container scenarios independently executed without skips. All root logs/XML use
/private/tmp/fleet-c04-root-final-*. Full backend: 13287 passed, 261 skipped, 1 deselected, 19 warnings in 274.70s (0:04:34)
Blocking-I/O75/0 (4.01s), boundary/thread
74/0 (2.41s). Full backend Ruff clean,
final guidance0errors/4 existing soft chain-size warnings, strict OpenSpec3/3 and
diffcheck clean. The same four warning paths/codes exist in parent HEAD; no new
warning category/path. Strict-warnings is not reported as passing. Final make
format left all1414 Python files unchanged.
Optional default-suite skips do not replace the independently run required cases.

Immutable runner sha256:d553a22390438717b7c19a8c417eeb53f393ddc981693cb454afeaba3c792d00; alternate provider sha256:c95135de1a1383c9239c859026fea0d64b2ad37e0e8b1b30d472b58752c77ed8.
Exact source/image binding: /private/tmp/c04-clock-secret-fix-source-freeze.json.
Original B image sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8
was not rebuilt. This is local Docker/PostgreSQL acceptance, not production ECS
or user-facing activation. The slice contains this evidence; exact commit is
recorded after committing. The full C change is not IMPLEMENTED or archived.

C04 verified implementation commit: `991a97fd0f3c5b3a6216124c5327d88b5ff48f95`. Post-document boundary/thread checks
74 passed/0 skipped (2.66s); OpenSpec3/3 and diffcheck clean. All52 frozen source
hashes match the independently verified snapshot. C05-C12 and BC remain pending.


C05 local acceptance: specification and quality/security reviews Approved; original
behavior RED3 retained separately from setup errors. Independent root checkpoint
130/0, runner92/0, C01-C03 74/0 and unchanged-image B259/0 all pass with required
real PG/container cases and no skips. Full backend13288pass/390optional skips,
blocking-I/O75/0, boundary/thread74/0; Ruff1417/strict OpenSpec3/3/diff clean.
Final guidance0errors/4existing AG002 warnings, no new path/category; strict-warnings
not claimed passing. Eight-source-file freeze and immutable images are recorded at
/private/tmp/c05-source-freeze.json. See implementation progress for detailed timing,
rollback/race/materialization/title/schema evidence and retained logs.
C06-C12/BC remain unchecked; Gateway activation is closed. Source commit follows.
