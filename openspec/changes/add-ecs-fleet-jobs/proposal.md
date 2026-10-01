## Why

耗时工具计算不能占满控制面，也不能依赖聊天连接或提交 run 的生命周期。先交付持久 job，并从第一天保留 B/C 共用身份和资源账。

## What Changes

- B01：建立可选包、配置和协议边界。
- B02：建立独立迁移链和真实故障测试夹具。
- B03：打通节点凭据与宿主 worker 路由。
- B04：实现原子容量预留与节点生命周期。
- B05：实现 staged 提交、去重和跟踪握手。
- B06：实现守护进程、启动授权与本地 watchdog。
- B07：实现不可变输入、产物校验和读取授权。
- B08：实现取消、unknown 和状态对账。
- B09：暴露受控工具并复用长期任务通知。
- B10：接入定时 job 去重和任务可见性。
- B11：提供可重复部署、兼容检查与迁移回退说明。
- B12：B 集成故障验收与进入 C 的门槛。

## Capabilities

### New Capabilities

- `fleet-foundation`: 可选包、独立迁移、节点鉴权与共用资源账。
- `fleet-durable-jobs`: 持久提交、start 授权、隔离产物、取消与未知执行。
- `fleet-job-integration`: 长期工具/定时任务/用户可见性与 B 交付门槛。

### Modified Capabilities

无已归档 OpenSpec 基线条目。本变更用新增 capability 定义新增行为，不伪造已有 MODIFIED requirement。

## Impact

- 前置条件：无；在现有个人 ECS worktree 开始。
- 目标 worktree：`~/.codex/worktrees/deerflow2/personal-agent-ecs`；不改主 checkout。
- 具体文件与 TDD 步骤见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-b-jobs.md)。
- Postgres、NAS、worker 容器和现有 DeerFlow runtime 受影响；默认 feature flags 关闭，已有 local 路径保持兼容。
- 仅规划已完成，任务清单全部未实施；不得 archive 或写 IMPLEMENTED 标记。
