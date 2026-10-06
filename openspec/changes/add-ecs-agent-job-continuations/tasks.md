# B/C：等待、继续与统一产品交付 tasks

前置：add-ecs-remote-agent 验收通过，B/C 独立执行均可用。详见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-bc-continuations.md)。
BC01、BC02 已完成本地源码/原生验收，源码提交分别为 `ce92de4ab1f47eeac25e03544019bb7d619b23dd`、`c08b8014801c762c3d2a9157e90ca1462a6a35f3`；BC03 已完成本地原生验收，源码提交 `87e0d428`；BC04 已完成本地原生验收，源码提交 `f24a88d7`；BC05–BC10 尚未完成。各阶段先主干，再一个必要集中边界，无全量重跑。

## 1. BC01 建立依赖与等待组持久模型

- [x] 1.1 写并运行 backend/tests/fleet/test_bc01_fleet_agent_job_dependencies.py，确认 BC01 行为测试 RED。
- [x] 1.2 完成计划列出的接口、事务和部署接线；满足 `Owned immutable child links and wait groups`。
- [x] 1.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 1.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 2. BC02 实现 await 工具与安全让出屏障

已按[当前源码接线计划](../../../docs/superpowers/plans/2026-10-06-ecs-fleet-bc02-yield.md)完成本地验收，源码提交 `c08b8014801c762c3d2a9157e90ca1462a6a35f3`。主干1passed6.59s、边界1passed9.18s，自然退出0，独立 SPEC→QUALITY Ready。BC03–BC10 仍未完成。

- [x] 2.1 写并运行 backend/tests/fleet/test_bc02_fleet_agent_job_dependencies.py，确认 BC02 行为测试 RED。
- [x] 2.2 完成计划列出的接口、事务和部署接线；满足 `Durable cooperative yield releases execution resources`。
- [x] 2.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 2.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 3. BC03 实现 exactly-one continuation 准入

已按[当前源码接线计划](../../../docs/superpowers/plans/2026-10-06-ecs-fleet-bc03-continuations.md)完成本地验收，源码提交 `87e0d42876db67c01afc58c37a359afc28a22175`。V2 原主干1passed8.30s、同一集中边界1passed11.09s，自然0/自有schema清理；全局摘要bounds修复后独立 SPEC→QUALITY Ready。保留精确import-only资格差异和单独迁移断言维护记录；不运行原矩阵。BC04–BC10仍未完成。

- [x] 3.1 写并运行 backend/tests/fleet/test_bc03_fleet_agent_job_continuations.py，确认 BC03 行为测试 RED。
- [x] 3.2 完成计划列出的接口、事务和部署接线；满足 `Idempotent continuation after all results settle`。
- [x] 3.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 3.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 4. BC04 统一结果 delivery owner 与通知互斥

当前接线按[BC04 最小执行计划](../../../docs/superpowers/plans/2026-10-06-ecs-fleet-bc04-delivery.md)：唯一持久 link_mode 归属、认领 LIMIT 前过滤、原服务/内部 launcher 复核；一个实际主干先行，之后一个集中必要边界。最终main12.52s/boundary8.44s自然0/自有schema清理，7个reviewed blobs与源码提交 `f24a88d7142e83a4a57077a1cafaa4411c067d42` 匹配，独立SPEC→QUALITY Ready。历史RED的NULL判据与缺SQL限制如实保留；boundary实际legacy claims提供原无策略claimquery的SQL事实。完整安装入口仍待BC10。

- [x] 4.1 写并运行 backend/tests/fleet/test_bc04_fleet_agent_job_continuations.py，保存 BC04 失败记录及历史RED资格限制；最终集中边界实际原claim查询/legacy leases另有SQL事实，不将初始NULL断言错误说成充分产品RED。
- [x] 4.2 完成计划列出的接口、事务和部署接线；满足 `Exclusive result delivery path`。
- [x] 4.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 4.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

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
