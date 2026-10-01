## Why

远程 Agent 必须能等待计算任务而不占满机器，并在结果到达后安全继续同一目标。需要解决取消、重复通知、新用户输入和容量死锁。

## What Changes

- BC01：建立依赖与等待组持久模型。
- BC02：实现 await 工具与安全让出屏障。
- BC03：实现 exactly-one continuation 准入。
- BC04：统一结果 delivery owner 与通知互斥。
- BC05：共用公平调度与最小池无死锁。
- BC06：取消、用户输入和 generation 竞态。
- BC07：跨 run 预算、deadline 与恢复裁决。
- BC08：Scheduler 目标阻塞与命名子任务。
- BC09：交付统一任务摘要与操作入口。
- BC10：组合端到端和运维交付验收。

## Capabilities

### New Capabilities

- `fleet-agent-job-dependencies`: 子任务归属、安全让出、generation 与累计预算。
- `fleet-agent-job-continuations`: 幂等续跑、独占结果交付、公平调度与 Scheduler 衔接。
- `fleet-unified-task-experience`: 统一用户任务状态、操作入口与组合故障验收。

### Modified Capabilities

无已归档 OpenSpec 基线条目。本变更用新增 capability 定义新增行为，不伪造已有 MODIFIED requirement。

## Impact

- 前置条件：add-ecs-remote-agent 验收通过，B/C 独立执行均可用。
- 目标 worktree：`~/.codex/worktrees/deerflow2/personal-agent-ecs`；不改主 checkout。
- 具体文件与 TDD 步骤见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-bc-continuations.md)。
- Postgres、NAS、worker 容器和现有 DeerFlow runtime 受影响；默认 feature flags 关闭，已有 local 路径保持兼容。
- 仅规划已完成，任务清单全部未实施；不得 archive 或写 IMPLEMENTED 标记。
