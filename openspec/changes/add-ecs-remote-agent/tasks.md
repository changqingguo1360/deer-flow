# C：完整远程 Agent tasks

前置：add-ecs-fleet-jobs 验收通过，表与协议已迁移。详见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-c-remote-agent.md)。
只有实际实施、审查与验证完成的项目才勾选；规划或 CLI 校验成功不代表实现完成。

## 1. C01 扩展远程放置模型与启动描述

- [x] 1.1 写并运行 backend/tests/fleet/test_c01_remote_agent_admission.py，确认 C01 行为测试 RED。
- [x] 1.2 完成计划列出的接口、事务和部署接线；满足 `Versioned remote launch specification`。
- [x] 1.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 1.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 2. C02 建立 Local/Fleet 后端契约与原子准入

- [ ] 2.1 写并运行 backend/tests/fleet/test_c02_remote_agent_admission.py，确认 C02 行为测试 RED。
- [ ] 2.2 完成计划列出的接口、事务和部署接线；满足 `Atomic remote admission with local parity`。
- [ ] 2.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 2.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 3. C03 统一 claim 与 run ownership 续约

- [ ] 3.1 写并运行 backend/tests/fleet/test_c03_remote_agent_admission.py，确认 C03 行为测试 RED。
- [ ] 3.2 完成计划列出的接口、事务和部署接线；满足 `Single execution owner across placement and run`。
- [ ] 3.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 3.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 4. C04 启动复用 run_agent 的完整 runner

- [ ] 4.1 写并运行 backend/tests/fleet/test_c04_remote_agent_runtime.py，确认 C04 行为测试 RED。
- [ ] 4.2 完成计划列出的接口、事务和部署接线；满足 `Full runtime execution on worker`。
- [ ] 4.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 4.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 5. C05 实现 checkpoint 事务内 fencing

- [ ] 5.1 写并运行 backend/tests/fleet/test_c05_remote_agent_runtime.py，确认 C05 行为测试 RED。
- [ ] 5.2 完成计划列出的接口、事务和部署接线；满足 `Fenced checkpoint writes including pending writes`。
- [ ] 5.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 5.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

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
