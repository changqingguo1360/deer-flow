# ECS Fleet B → C → B/C 实施总览

2026-10-07：B01–B12、C01–C12、BC01–BC10已按顺序完成本地实施与验收。BC10源码 `06ae5f44`，current02镜像、main13真实双Worker跨节点闭环/同流程浏览器、boundary03唯一集中故障、combined03聚合及最终SPEC→QUALITY均通过。P0/P1/P2分级收尾完成，所有flags仍默认关闭。完整34项要求、原始资格、命令与路径映射见[三阶段交付](../../ecs-fleet-delivery.md)，BC10细节见[验收](../../ecs-fleet-bc10-acceptance.md)。无生产ECS/NAS或live模型验收。

| 顺序 | OpenSpec change | Superpowers 计划 | 完成门槛 |
|---|---|---|---|
| B | [add-ecs-fleet-jobs](../../../openspec/changes/add-ecs-fleet-jobs/proposal.md) | [B：持久 Job 与共用 Fleet 基础](2026-10-01-ecs-fleet-b-jobs.md) | B 阶段实际故障验收通过，无必需测试跳过 |
| C | [add-ecs-remote-agent](../../../openspec/changes/add-ecs-remote-agent/proposal.md) | [C：完整远程 Agent](2026-10-01-ecs-fleet-c-remote-agent.md) | C 阶段实际故障验收通过，无必需测试跳过 |
| BC | [add-ecs-agent-job-continuations](../../../openspec/changes/add-ecs-agent-job-continuations/proposal.md) | [B/C：等待、继续与统一产品交付](2026-10-01-ecs-fleet-bc-continuations.md) | BC 阶段实际故障验收通过，无必需测试跳过 |

三个 change 是同一个目标的串行分解。B 包含共用基础；C 复用资源账并接入完整 Agent；最后的 B/C change 完成让出、结果通知互斥、续跑与统一用户体验。不能完成 B 后把 C 变成可选。

## 规划时核对出的实现约束

- 扩展管理 router 只能 session 鉴权，worker bearer 入口必须在 host 核心加窄路由与鉴权。
- 扩展 migration 遵守模块 AGENTS：service.start 中在 host bootstrap 后以 advisory lock 运行独立 Alembic；不加入 host Base。
- 现有 McpTaskService 提交后保存 tracking，B 增加 staged 门槛解决孤儿执行窗口。
- C 必须改核心 admission/checkpointer/ownership，不能用普通扩展 lifecycle 回调代替。
- C pending checkpoint writes、memory 和有状态扩展必须纳入 fencing；不支持的 provider/profile 拒绝 remote。
- waiting 是 Agent task 状态；原 run 正常终止，新 run 按稳定键继续，不复活终态 run。

## 依赖与回退

OpenSpec CLI 的 artifact DAG 不自动执行跨 change 依赖。本表和各 proposal 的前置条件是人工/执行者门槛；依序完成并验证，之后才 archive 相应 change。所有新增 capability 独立命名，避免提前 MODIFIED 尚未归档的基线。

| 开关 | B 阶段 | C 阶段 | 组合阶段 |
|---|---|---|---|
| jobs_enabled | 验收后可开 | 保留 | 保留 |
| agents_enabled | 关闭 | 验收后可开 | 保留 |
| continuations_enabled | 关闭，awaited 返回 422 | 关闭，C 只交 detached job | 验收后可开 |

开关控制新工作准入；关闭不应切断已接收工作的续约、完成和对账。先 drain，再确认物理停止，保留表与产物，不进行破坏性回退。

## 需求到计划追踪

### B

| ID | 需求 | 任务 |
|---|---|---|
| B01 | [Optional installation and strict configuration](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-foundation/spec.md) | 建立可选包、配置和协议边界 |
| B02 | [Independent migration and persisted execution identity](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-foundation/spec.md) | 建立独立迁移链和真实故障测试夹具 |
| B03 | [Node scoped authentication without session bypass](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-foundation/spec.md) | 打通节点凭据与宿主 worker 路由 |
| B04 | [Shared atomic capacity and draining](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-foundation/spec.md) | 实现原子容量预留与节点生命周期 |
| B05 | [Tracked idempotent submission before execution](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-durable-jobs/spec.md) | 实现 staged 提交、去重和跟踪握手 |
| B06 | [Authorized execution and stop on lease loss](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-durable-jobs/spec.md) | 实现守护进程、启动授权与本地 watchdog |
| B07 | [Attempt isolated artifacts and accepted manifest](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-durable-jobs/spec.md) | 实现不可变输入、产物校验和读取授权 |
| B08 | [Honest cancellation and uncertain execution](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-durable-jobs/spec.md) | 实现取消、unknown 和状态对账 |
| B09 | [Durable user task tracking and result notification](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-job-integration/spec.md) | 暴露受控工具并复用长期任务通知 |
| B10 | [Scheduled job deduplication and truthful UI](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-job-integration/spec.md) | 接入定时 job 去重和任务可见性 |
| B11 | [Reproducible deployment and safe disabling](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-job-integration/spec.md) | 提供可重复部署、兼容检查与迁移回退说明 |
| B12 | [B release gate verifies real side effects](../../../openspec/changes/add-ecs-fleet-jobs/specs/fleet-job-integration/spec.md) | B 集成故障验收与进入 C 的门槛 |

### C

| ID | 需求 | 任务 |
|---|---|---|
| C01 | [Versioned remote launch specification](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-admission/spec.md) | 扩展远程放置模型与启动描述 |
| C02 | [Atomic remote admission with local parity](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-admission/spec.md) | 建立 Local/Fleet 后端契约与原子准入 |
| C03 | [Single execution owner across placement and run](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-admission/spec.md) | 统一 claim 与 run ownership 续约 |
| C04 | [Full runtime execution on worker](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-runtime/spec.md) | 启动复用 run_agent 的完整 runner |
| C05 | [Fenced checkpoint writes including pending writes](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-runtime/spec.md) | 实现 checkpoint 事务内 fencing |
| C06 | [All remote durable mutations respect ownership](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-runtime/spec.md) | 覆盖 memory、扩展和最终状态写入 |
| C07 | [Committed ordered remote events](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-runtime/spec.md) | 持久事件 outbox 与可恢复 SSE |
| C08 | [Consistent workspace checkpoint boundary](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-runtime/spec.md) | 实现 C workspace 和 checkpoint 联合恢复点 |
| C09 | [Remote cancellation and safe recovery](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-operations/spec.md) | 远程取消、人工中断与故障隔离 |
| C10 | [Authorized routing and queued scheduler budget](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-admission/spec.md) | 路由 preference 与 Scheduler 票据接入 |
| C11 | [Remote task visibility and reversible enablement](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-operations/spec.md) | C 任务摘要、部署和本地回归 |
| C12 | [C release gate covers all remote mutation paths](../../../openspec/changes/add-ecs-remote-agent/specs/remote-agent-operations/spec.md) | C 故障验收门槛 |

### BC

| ID | 需求 | 任务 |
|---|---|---|
| BC01 | [Owned immutable child links and wait groups](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-agent-job-dependencies/spec.md) | 建立依赖与等待组持久模型 |
| BC02 | [Durable cooperative yield releases execution resources](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-agent-job-dependencies/spec.md) | 实现 await 工具与安全让出屏障 |
| BC03 | [Idempotent continuation after all results settle](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-agent-job-continuations/spec.md) | 实现 exactly-one continuation 准入 |
| BC04 | [Exclusive result delivery path](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-agent-job-continuations/spec.md) | 统一结果 delivery owner 与通知互斥 |
| BC05 | [Fair shared scheduling and bounded child wait](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-agent-job-continuations/spec.md) | 共用公平调度与最小池无死锁 |
| BC06 | [Generation fences continuation and user edits](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-agent-job-dependencies/spec.md) | 取消、用户输入和 generation 竞态 |
| BC07 | [Aggregate budgets and nonautomatic crash recovery](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-agent-job-dependencies/spec.md) | 跨 run 预算、deadline 与恢复裁决 |
| BC08 | [Scheduled aggregate tasks preserve durable queue semantics](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-agent-job-continuations/spec.md) | Scheduler 目标阻塞与命名子任务 |
| BC09 | [Distinguish run completion from goal completion](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-unified-task-experience/spec.md) | 交付统一任务摘要与操作入口 |
| BC10 | [Unified release gate demonstrates C B C execution](../../../openspec/changes/add-ecs-agent-job-continuations/specs/fleet-unified-task-experience/spec.md) | 组合端到端和运维交付验收 |

## 规划校验与执行命令

在目标 worktree 根运行：

```bash
OPENSPEC_TELEMETRY=0 openspec validate --all --strict --no-interactive
OPENSPEC_TELEMETRY=0 openspec status --change add-ecs-fleet-jobs
OPENSPEC_TELEMETRY=0 openspec status --change add-ecs-remote-agent
OPENSPEC_TELEMETRY=0 openspec status --change add-ecs-agent-job-continuations
```

CLI status的artifact complete只表示规划文件齐备；当前三份tasks已根据实际实施与验收完成，不能仅由CLI状态推断功能通过。历史master计划中的probe/node ID草图由后续实际源码接线计划及注册case替代，见交付记录。按用户要求不重跑未受影响的B/C gate，不扩展故障矩阵。源分支和worktree本地保留；规范archive是后续独立步骤，无push/merge/部署。
