# B/C：等待、继续与统一产品交付 tasks

前置：add-ecs-remote-agent 验收通过，B/C 独立执行均可用。详见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-bc-continuations.md)。
所有勾选项是未来实施，不因规划/校验成功而勾选。

## 1. BC01 建立依赖与等待组持久模型

- [ ] 1.1 写并运行 backend/tests/fleet/test_bc01_fleet_agent_job_dependencies.py，确认 BC01 行为测试 RED。
- [ ] 1.2 完成计划列出的接口、事务和部署接线；满足 `Owned immutable child links and wait groups`。
- [ ] 1.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 1.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 2. BC02 实现 await 工具与安全让出屏障

- [ ] 2.1 写并运行 backend/tests/fleet/test_bc02_fleet_agent_job_dependencies.py，确认 BC02 行为测试 RED。
- [ ] 2.2 完成计划列出的接口、事务和部署接线；满足 `Durable cooperative yield releases execution resources`。
- [ ] 2.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 2.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 3. BC03 实现 exactly-one continuation 准入

- [ ] 3.1 写并运行 backend/tests/fleet/test_bc03_fleet_agent_job_continuations.py，确认 BC03 行为测试 RED。
- [ ] 3.2 完成计划列出的接口、事务和部署接线；满足 `Idempotent continuation after all results settle`。
- [ ] 3.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 3.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 4. BC04 统一结果 delivery owner 与通知互斥

- [ ] 4.1 写并运行 backend/tests/fleet/test_bc04_fleet_agent_job_continuations.py，确认 BC04 行为测试 RED。
- [ ] 4.2 完成计划列出的接口、事务和部署接线；满足 `Exclusive result delivery path`。
- [ ] 4.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 4.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 5. BC05 共用公平调度与最小池无死锁

- [ ] 5.1 写并运行 backend/tests/fleet/test_bc05_fleet_agent_job_continuations.py，确认 BC05 行为测试 RED。
- [ ] 5.2 完成计划列出的接口、事务和部署接线；满足 `Fair shared scheduling and bounded child wait`。
- [ ] 5.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 5.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 6. BC06 取消、用户输入和 generation 竞态

- [ ] 6.1 写并运行 backend/tests/fleet/test_bc06_fleet_agent_job_dependencies.py，确认 BC06 行为测试 RED。
- [ ] 6.2 完成计划列出的接口、事务和部署接线；满足 `Generation fences continuation and user edits`。
- [ ] 6.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 6.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 7. BC07 跨 run 预算、deadline 与恢复裁决

- [ ] 7.1 写并运行 backend/tests/fleet/test_bc07_fleet_agent_job_dependencies.py，确认 BC07 行为测试 RED。
- [ ] 7.2 完成计划列出的接口、事务和部署接线；满足 `Aggregate budgets and nonautomatic crash recovery`。
- [ ] 7.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 7.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 8. BC08 Scheduler 目标阻塞与命名子任务

- [ ] 8.1 写并运行 backend/tests/fleet/test_bc08_fleet_agent_job_continuations.py，确认 BC08 行为测试 RED。
- [ ] 8.2 完成计划列出的接口、事务和部署接线；满足 `Scheduled aggregate tasks preserve durable queue semantics`。
- [ ] 8.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 8.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 9. BC09 交付统一任务摘要与操作入口

- [ ] 9.1 写并运行 backend/tests/fleet/test_bc09_fleet_unified_task_experience.py，确认 BC09 行为测试 RED。
- [ ] 9.2 完成计划列出的接口、事务和部署接线；满足 `Distinguish run completion from goal completion`。
- [ ] 9.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 9.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 10. BC10 组合端到端和运维交付验收

- [ ] 10.1 写并运行 backend/tests/fleet/test_bc10_fleet_unified_task_experience.py，确认 BC10 行为测试 RED。
- [ ] 10.2 完成计划列出的接口、事务和部署接线；满足 `Unified release gate demonstrates C B C execution`。
- [ ] 10.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 10.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。
