# B/C：等待、继续与统一产品交付 tasks

前置：add-ecs-remote-agent 验收通过，B/C 独立执行均可用。详见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-bc-continuations.md)。
BC01、BC02 已完成本地源码/原生验收，源码提交分别为 `ce92de4ab1f47eeac25e03544019bb7d619b23dd`、`c08b8014801c762c3d2a9157e90ca1462a6a35f3`；BC03 已完成本地原生验收，源码提交 `87e0d428`；BC04 已完成本地原生验收，源码提交 `f24a88d7`；BC05 已本地验收（源码 `c153ad19`）；BC06 已本地验收（源码 `8abe7af9`）；BC07 已本地验收（源码 `097b789a`）；BC08 已本地验收（源码 `d08f7763`）；BC09–BC10 尚未完成。各阶段先主干，再一个必要集中边界，无全量重跑。

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

按[当前实际接线计划](../../../docs/superpowers/plans/2026-10-06-ecs-fleet-bc05-shared-scheduling.md)推进：显式reserved/serial、持久类别轮转、实际current-session mixed worker、一槽installed C→B→C主干先行；之后至多一个集中必要边界。源码已实现并冻结；installed主干main-green-8（1passed38.76s）和唯一native边界boundary-2（1passed4.89s）自然退出0，自有schema已清理；独立SPEC已Ready。独立QUALITY已Ready；源码 `c153ad1951fcf1f8edaa5f5a4a0483d9228c3346` 的22个reviewed blobs一致，BC05本地验收完成。原main完整prefix与后来追加boundary分别保留资格，不重复成功测试。

- [x] 5.1 写并运行 backend/tests/fleet/test_bc05_fleet_agent_job_continuations.py，确认 BC05 行为测试 RED。
- [x] 5.2 完成计划列出的接口、事务和部署接线；满足 `Fair shared scheduling and bounded child wait`。
- [x] 5.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 5.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 6. BC06 取消、用户输入和 generation 竞态

已按[实际事务接线计划](../../../docs/superpowers/plans/2026-10-07-ecs-fleet-bc06-generation.md)完成 BC06 本地验收，源码 `8abe7af9f9b4683f055ba577ee9b0b6cac686a45` 的23个 reviewed Python blobs一致。main-green-05 主干1passed11.96s；quality-green-01 同一集中边界1passed33.00s、八个 owned schema 清理，首次非waiting queued/assigned请求409且完整SQL不变。SPEC Ready；QUALITY P1 经两行原事务guard修复和独立复审后 Ready。原生范围和历史失败详见[验收报告](../../../docs/ecs-fleet-bc06-acceptance.md)；BC07–BC10仍未完成。

- [x] 6.1 写并运行 backend/tests/fleet/test_bc06_fleet_agent_job_dependencies.py，确认 BC06 行为测试 RED。main-red-06 实际 session HTTP409 证明 waiting human admission 缺口；自然退出1/自有schema清理，不代表全部 generation/resume 行为均已执行。
- [x] 6.2 完成计划列出的接口、事务和部署接线；满足 `Generation fences continuation and user edits`。
- [x] 6.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 6.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 7. BC07 跨 run 预算、deadline 与恢复裁决

已按[实际预算与恢复计划](../../../docs/superpowers/plans/2026-10-07-ecs-fleet-bc07-budgets-recovery.md)完成本地验收，源码 `097b789a` 的22个 reviewed Python blobs一致。主干 main-cumulative-03 三次真实 C / 两批 B 自然0/12.06s；token/job 跨 run 部分边界自然0/9.54s，历史 boundary05 恢复范围在未改源码上保留。独立 SPEC→QUALITY Ready；完整安装组合仍待 BC10，见[验收记录](../../../docs/ecs-fleet-bc07-acceptance.md)。

- [x] 7.1 写并运行 backend/tests/fleet/test_bc07_fleet_agent_job_dependencies.py，main-red-02实际同task第四次恢复HTTP200证明累计run上限缺口；自然1/10.07s，自有schema已清理。此RED不代表token/job拒绝或恢复边界均已验证。
- [x] 7.2 完成计划列出的接口、事务和部署接线；满足 `Aggregate budgets and nonautomatic crash recovery`。
- [x] 7.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 7.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 8. BC08 Scheduler 目标阻塞与命名子任务

已按[当前接线计划](../../../docs/superpowers/plans/2026-10-07-ecs-fleet-bc08-scheduled-goals.md)完成本地验收，源码 `d08f776375cd70776d0d0feda6a820a7eca65755` 的14个 reviewed Python blobs一致。原主干9.54s保留未改主干/Fleet路径资格；唯一集中边界在 Local 快速完成兼容性修复后1passed13.61s、自然0/两自有schema清理。Fresh SPEC→QUALITY Ready；支持 schedule-wide queued/age/zero-charge、once真实目标完成与重启、Local完成早于记账、原队列超时及认证waiting取消。历史/跨generation/failure/neverassigned路径仅源码审查；完整安装组合仍待BC10。详见[验收记录](../../../docs/ecs-fleet-bc08-acceptance.md)。

- [x] 8.1 写并运行 backend/tests/fleet/test_bc08_fleet_agent_job_continuations.py，确认 BC08 行为测试 RED。
- [x] 8.2 完成计划列出的接口、事务和部署接线；满足 `Scheduled aggregate tasks preserve durable queue semantics`。
- [x] 8.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 8.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 9. BC09 交付统一任务摘要与操作入口

按[当前实际接线计划](../../../docs/superpowers/plans/2026-10-07-ecs-fleet-bc09-unified-summary.md)推进：原 owned task projection 增加 bounded jobs/runs/accepted refs；现有前端任务卡提供 generation/idempotency 目标取消/继续和409刷新；既有 IM final/status 使用同一安全摘要，保留 Local/B/GitHub原策略。先实际 API+DOM 主干，再唯一必要集中边界；BC10完整安装组合仍必需。

- [ ] 9.1 写并运行 backend/tests/fleet/test_bc09_fleet_unified_task_experience.py，确认 BC09 行为测试 RED。
- [ ] 9.2 完成计划列出的接口、事务和部署接线；满足 `Distinguish run completion from goal completion`。
- [ ] 9.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 9.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 10. BC10 组合端到端和运维交付验收

- [ ] 10.1 写并运行 backend/tests/fleet/test_bc10_fleet_unified_task_experience.py，确认 BC10 行为测试 RED。
- [ ] 10.2 完成计划列出的接口、事务和部署接线；满足 `Unified release gate demonstrates C B C execution`。
- [ ] 10.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 10.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。
