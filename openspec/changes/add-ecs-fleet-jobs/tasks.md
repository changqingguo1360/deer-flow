# B：持久 Job 与共用 Fleet 基础 tasks

前置：无；在现有个人 ECS worktree 开始。详见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-b-jobs.md)。
所有勾选项是未来实施，不因规划/校验成功而勾选。

## 1. B01 建立可选包、配置和协议边界

- [ ] 1.1 写并运行 backend/tests/fleet/test_b01_fleet_foundation.py，确认 B01 行为测试 RED。
- [ ] 1.2 完成计划列出的接口、事务和部署接线；满足 `Optional installation and strict configuration`。
- [ ] 1.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 1.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 2. B02 建立独立迁移链和真实故障测试夹具

- [ ] 2.1 写并运行 backend/tests/fleet/test_b02_fleet_foundation.py，确认 B02 行为测试 RED。
- [ ] 2.2 完成计划列出的接口、事务和部署接线；满足 `Independent migration and persisted execution identity`。
- [ ] 2.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 2.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 3. B03 打通节点凭据与宿主 worker 路由

- [ ] 3.1 写并运行 backend/tests/fleet/test_b03_fleet_foundation.py，确认 B03 行为测试 RED。
- [ ] 3.2 完成计划列出的接口、事务和部署接线；满足 `Node scoped authentication without session bypass`。
- [ ] 3.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 3.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 4. B04 实现原子容量预留与节点生命周期

- [x] 4.1 写并运行 backend/tests/fleet/test_b04_fleet_foundation.py，确认 B04 行为测试 RED。
- [ ] 4.2 完成计划列出的接口、事务和部署接线；满足 `Shared atomic capacity and draining`。
- [ ] 4.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 4.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 5. B05 实现 staged 提交、去重和跟踪握手

- [ ] 5.1 写并运行 backend/tests/fleet/test_b05_fleet_durable_jobs.py，确认 B05 行为测试 RED。
- [ ] 5.2 完成计划列出的接口、事务和部署接线；满足 `Tracked idempotent submission before execution`。
- [ ] 5.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 5.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 6. B06 实现守护进程、启动授权与本地 watchdog

- [ ] 6.1 写并运行 backend/tests/fleet/test_b06_fleet_durable_jobs.py，确认 B06 行为测试 RED。
- [ ] 6.2 完成计划列出的接口、事务和部署接线；满足 `Authorized execution and stop on lease loss`。
- [ ] 6.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 6.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 7. B07 实现不可变输入、产物校验和读取授权

- [ ] 7.1 写并运行 backend/tests/fleet/test_b07_fleet_durable_jobs.py，确认 B07 行为测试 RED。
- [ ] 7.2 完成计划列出的接口、事务和部署接线；满足 `Attempt isolated artifacts and accepted manifest`。
- [ ] 7.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 7.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 8. B08 实现取消、unknown 和状态对账

- [ ] 8.1 写并运行 backend/tests/fleet/test_b08_fleet_durable_jobs.py，确认 B08 行为测试 RED。
- [ ] 8.2 完成计划列出的接口、事务和部署接线；满足 `Honest cancellation and uncertain execution`。
- [ ] 8.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 8.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 9. B09 暴露受控工具并复用长期任务通知

- [ ] 9.1 写并运行 backend/tests/fleet/test_b09_fleet_job_integration.py，确认 B09 行为测试 RED。
- [ ] 9.2 完成计划列出的接口、事务和部署接线；满足 `Durable user task tracking and result notification`。
- [ ] 9.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 9.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 10. B10 接入定时 job 去重和任务可见性

- [ ] 10.1 写并运行 backend/tests/fleet/test_b10_fleet_job_integration.py，确认 B10 行为测试 RED。
- [ ] 10.2 完成计划列出的接口、事务和部署接线；满足 `Scheduled job deduplication and truthful UI`。
- [ ] 10.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 10.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 11. B11 提供可重复部署、兼容检查与迁移回退说明

- [ ] 11.1 写并运行 backend/tests/fleet/test_b11_fleet_job_integration.py，确认 B11 行为测试 RED。
- [ ] 11.2 完成计划列出的接口、事务和部署接线；满足 `Reproducible deployment and safe disabling`。
- [ ] 11.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 11.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 12. B12 B 集成故障验收与进入 C 的门槛

- [ ] 12.1 写并运行 backend/tests/fleet/test_b12_fleet_job_integration.py，确认 B12 行为测试 RED。
- [ ] 12.2 完成计划列出的接口、事务和部署接线；满足 `B release gate verifies real side effects`。
- [ ] 12.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 12.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## Execution evidence

B is in progress; see [implementation evidence](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-implementation-progress.md).
B04 RED: 8 failing tests before FleetScheduler existed; GREEN: all 8 pass on real Postgres.
Unchecked items retain incomplete wiring, release gates or commit-level acceptance requirements.


Current B05/B06 partial status: canonical retry tracking, graph invocation identity,
commit-failure compensation and worker attempt HTTP/lease components are verified.
Real Docker verifies launch once, watchdog stop and refusal to stop unmanaged containers.
307 component/adjacent tests pass with zero skips; full B06 daemon and B release gates
remain pending. See the implementation evidence linked above. Do not equate partial
component evidence with a completed requirement or mark remaining checkboxes early.
