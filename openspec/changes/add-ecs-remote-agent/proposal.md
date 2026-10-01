## Why

完整推理和工具循环也需要在 ECS 上运行。B 的程序执行不能代替 Agent 的 checkpoint、run ownership 与用户取消语义。

## What Changes

- C01：扩展远程放置模型与启动描述。
- C02：建立 Local/Fleet 后端契约与原子准入。
- C03：统一 claim 与 run ownership 续约。
- C04：启动复用 run_agent 的完整 runner。
- C05：实现 checkpoint 事务内 fencing。
- C06：覆盖 memory、扩展和最终状态写入。
- C07：持久事件 outbox 与可恢复 SSE。
- C08：实现 C workspace 和 checkpoint 联合恢复点。
- C09：远程取消、人工中断与故障隔离。
- C10：路由 preference 与 Scheduler 票据接入。
- C11：C 任务摘要、部署和本地回归。
- C12：C 故障验收门槛。

## Capabilities

### New Capabilities

- `remote-agent-admission`: 完整远程启动描述、后端准入、ownership 与调度。
- `remote-agent-runtime`: 完整 run_agent、持久写 fence、事件流和文件恢复点。
- `remote-agent-operations`: 取消、恢复、部署和远程 Agent 验收。

### Modified Capabilities

无已归档 OpenSpec 基线条目。本变更用新增 capability 定义新增行为，不伪造已有 MODIFIED requirement。

## Impact

- 前置条件：add-ecs-fleet-jobs 验收通过，表与协议已迁移。
- 目标 worktree：`~/.codex/worktrees/deerflow2/personal-agent-ecs`；不改主 checkout。
- 具体文件与 TDD 步骤见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-c-remote-agent.md)。
- Postgres、NAS、worker 容器和现有 DeerFlow runtime 受影响；默认 feature flags 关闭，已有 local 路径保持兼容。
- 仅规划已完成，任务清单全部未实施；不得 archive 或写 IMPLEMENTED 标记。
