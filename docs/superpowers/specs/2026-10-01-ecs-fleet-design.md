# 个人 Agent 系统 · ECS 机器池托管 设计文档

日期：2026-10-01
状态：待评审
分支：`feature/personal-agent-ecs`（worktree: `~/.codex/worktrees/deerflow2/personal-agent-ecs`）

## 1. 需求背景

基于 DeerFlow 搭建个人 agent 系统：控制面部署在云端一台 ECS 上，另有一个固定的
ECS 机器池作为工作节点，agent 任务可以托管到这些机器上执行。

### 已确认的决策（与需求方逐条对齐过）

| # | 决策点 | 结论 |
|---|--------|------|
| 1 | 执行位置 | **混合**：短交互任务走本机/工具级；长耗时、高资源任务整机级投到 ECS |
| 2 | 机器来源 | **固定池**：已有 ECS 手动注册，不调云 API 开/释放机器 |
| 3 | 控制面位置 | **Gateway 上云**：一台 ECS 跑 Nginx + Frontend + Gateway + Postgres + Redis |
| 4 | 任务路由 | **全自动调度**：系统按任务画像 + 节点容量自动选机，用户不感知机器 |
| 5 | 跨机文件 | **NFS/NAS 共享卷**：所有节点挂同一 NFS，`/mnt/user-data` 天然跨机可见 |
| 6 | 机器画像 | **同构通用机**：无 GPU/高配区分，调度只看负载/空闲度，标签体系从简 |
| 7 | 远程执行方案 | **方案一：轻量 node-agent + 拉取模式**（DB 租约，worker 纯出站） |
| 8 | 交付节奏 | **分期**：第一期工具级（EcsSandboxProvider），第二期整机级（node-agent） |

## 2. 现状盘点（DeerFlow 已有能力）

关键结论：**agent run 永远在 Gateway 进程内执行**（asyncio task），sandbox 只是
bash/文件等工具的执行端；代码库中不存在任何「机器池 / host / worker」概念。

可直接复用：

- `SandboxProvider` 插件点（`backend/packages/harness/deerflow/sandbox/sandbox_provider.py`）：
  `sandbox.use` 按类路径反射解析，自定义 provider 无需改框架；`SandboxConfig`
  `extra="allow"` 透传自定义字段。现有六种模式（Local/AIO/BoxLite/Tenki/OpenSandbox/E2B）。
- `WarmPoolLifecycleMixin`（`community/warm_pool_lifecycle.py`）：warm pool、idle 回收、容量上限。
- `AcquireSerializer`（`sandbox/acquire_serialization.py`）：per-key 串行化 acquire/release。
- 扩展系统（`backend/packages/extension-api`）：7 种贡献类型，其中 `service`
  （Gateway 生命周期后台服务，deps 含 DB session_factory）和 `routers`（FastAPI
  路由，带 `resolve_principal`/`require_admin`）是机器池注册表的正路。
- Scheduler（`backend/app/scheduler/service.py`）：DB 租约认领（`claim_due_tasks`）、
  occurrence 状态机 `queued → launching → running → 终态`、唯一活跃约束
  `uq_scheduled_task_run_active`、`queue_timeout_seconds`——是 node-agent 领取
  任务和 run placement 状态机的直接参照。
- 内部启动通道 `launch_scheduled_thread_run`（`backend/app/gateway/services.py`）：
  内部身份构造 run、`context.non_interactive` 仅内部可信——调度 hint 的防伪模式照此办理。
- 多实例协调原语：`run_ownership` 租约心跳、Redis stream_bridge 事件桥、sandbox
  ownership 租约——第二期 runner 事件回流全靠它们。

缺口（需自建）：

1. 执行位置不可迁：没有 run placement / remote-runner 抽象。
2. 无机器生命周期模型：注册、容量、健康、回收都没有持久化模型。
3. Provisioner 是 K8s-only（`docker/provisioner/app.py`），不管普通 VM。
4. 无机器维度的资源/成本归集。

## 3. 总体架构与数据流

交付形态：独立 Python 扩展包 `deerflow-ecs-fleet`（参照
`examples/deerflow-extension-example/`），经 `config.yaml` 顶层 `plugins:` 加载，
不改主框架代码。

```
                        浏览器 / IM（飞书、Telegram…）
                                  │ HTTPS
                                  ▼
┌───────────────────── 主节点 ECS（控制面） ─────────────────────┐
│  Nginx :2026 ──► Frontend :3000                               │
│       │                                                       │
│       └─► Gateway :8001（FastAPI 进程）                        │
│            ├─ RunManager：run 生命周期、租约（沿用现有）         │
│            ├─ ★ AutoRouter：读机器画像+容量 → 选目标节点        │
│            ├─ Scheduler：定时任务 → 走同一 run 通道（沿用现有）   │
│            ├─ ★ MachineRegistry（扩展）：注册/心跳/容量/标签     │
│            ├─ Postgres：threads/checkpoints/runs               │
│            │      + ★ machines 表 + ★ run_placements 表        │
│            └─ Redis：stream_bridge 事件桥 / 租约心跳（沿用现有）  │
└───────┬───────────────────────────────────┬─────────────────┘
        │ 路径A：工具级（短交互）              │ 路径B：整机级（长/重任务）
        │ Gateway 内 LLM 循环不动             │ 整个 run 迁到 worker
        ▼                                     ▼
┌────────── Worker ECS 1..N（固定池，云内互通） ──────────────────┐
│  ★ node-agent（轻量守护进程，第二期）                           │
│    · 注册 + 10s 心跳（CPU/内存/标签/活跃 run 数）                │
│    · 长轮询 Gateway 领取整机级任务（DB 租约，防重复）            │
│    ├─ AIO sandbox 容器 :8080  ◄── 路径A：EcsSandboxProvider     │
│    └─ runner 子进程  ◄── 路径B：node-agent 拉起完整运行时        │
│        · checkpoint ──► 主节点 Postgres（云内直连）             │
│        · run 事件   ──► 主节点 Redis stream_bridge              │
└───────────────────────────────────────────────────────────────┘
   NFS 共享卷：所有节点挂载，/mnt/user-data 跨机可见
```

数据流（按时间序）：

1. 浏览器 → Nginx → Gateway 创建 run；AutoRouter 按任务画像 + 节点容量决定
   路径 A/B 与目标机。
2. 路径 A：LLM 循环留在 Gateway；`EcsSandboxProvider` 把工具调用经云内 HTTP
   落到目标机的 AIO 容器执行。
3. 路径 B：Gateway 只写 `run_placements`（pending），不启动本地 asyncio task；
   node-agent 凭租约领走，本地拉起 runner 执行；事件写 Redis，Gateway 的
   StreamBridge 照常推 SSE——前端无感知。
4. node-agent 每 10s 上报心跳与容量；节点失联 → 租约过期 → run 标
   `interrupted`，按策略重调度。
5. Scheduler 触发的定时任务同样过 AutoRouter，定时重任务天然落到 worker。

## 4. 第一期设计：MachineRegistry + EcsSandboxProvider（工具级）

### 4.1 `machines` 表（Postgres）

`id (pk) / name (unique) / endpoint / labels (jsonb) / capacity_max_runs /
status (online|offline|draining) / last_heartbeat_at / agent_version / created_at /
updated_at`

第一期由管理员通过 API 手动注册；健康状态由 Gateway 侧扩展 service 每 15s
主动探测各机 AIO 端点维护。

### 4.2 扩展贡献（走扩展系统正路）

- **service**：Gateway 生命周期后台服务，跑健康探测循环，写 `machines` 表。
- **routers**：`GET /api/machines`、`POST /api/machines`、`DELETE
  /api/machines/{id}`、`GET /api/machines/{id}`，强制 session 鉴权 + `require_admin`。
  第一期只出 API，不做前端页面。

### 4.3 `EcsSandboxProvider`

- 实现现有 `SandboxProvider` ABC（`acquire/acquire_async/get/release/
  sync_agent_skills/reset/shutdown`），复用 `WarmPoolLifecycleMixin` 与
  `AcquireSerializer`。
- 每台 worker 预置一个常驻 AIO sandbox 容器（:8080）；`acquire` 时从注册表挑
  健康机（v1 策略：活跃 sandbox 数最少优先；支持显式指定机器）。
- 配置：`sandbox.use: deerflow_ecs_fleet.sandbox:EcsSandboxProvider`，自定义字段
  经 `SandboxConfig` 的 `extra="allow"` 透传。
- sandbox 身份沿用 `(user_id, thread_id)` 派生（`sandbox/identity.py`）。

### 4.4 AutoRouter v0（显式指定）

run 创建时支持 `context.sandbox_target=<machine_name>` 显式选机；缺省走注册表
默认策略。不做自动决策。

### 4.5 第一期效果

聊天中指定「到 ecs-2 上跑」，该 thread 的所有 bash/文件工具落在那台机器；
文件经 NFS 在本机同样可见。

## 5. 第二期设计：node-agent + 整机级执行 + AutoRouter 全自动调度

### 5.1 `run_placements` 表（Postgres）

`run_id (pk) / target (local|node_id|auto) / status
(pending|claimed|running|succeeded|failed|interrupted) / lease_owner /
lease_expires_at / required_labels (jsonb) / dispatched_at / finished_at / reason`

唯一活跃约束照抄 `uq_scheduled_task_run_active` 模式，保证一个 run 至多被
一台机器领走。

### 5.2 AutoRouter v1（全自动）

run 创建路径上的决策点：

- 输入：任务来源（交互 / scheduler 触发）、显式 hint
  （`context.preferred_target`，仅内部身份可信，防伪模式同 `non_interactive`）、
  各节点心跳容量。
- 规则：交互式 → local；scheduler 触发或显式声明 → 健康节点中活跃 run 数最少者；
  无健康节点 → 排队（复用 `queued` + `queue_timeout_seconds` 语义，不做
  skip-on-overlap）。
- 路径 B 的 run 在 Gateway 侧不启动 asyncio task，只落库等领取。

### 5.3 node-agent（worker 守护进程，复用 deerflow 包）

- 启动向 Registry 注册并接管心跳（10s，容量 + 活跃 run 数 + agent_version），
  携带内部 API token。
- 长轮询 `POST /api/node/claims` 领取任务 → DB 租约（`lease_seconds=120`，
  到期未续约自动可回收）。
- 领到后拉起 runner 子进程（deerflow 独立 entrypoint）：`graph.astream` 在
  worker 本地执行；checkpoint 直连共享 Postgres；run 事件写共享 Redis
  stream_bridge——Gateway SSE 转发与前端完全无感知，这是关键复用点。
- runner 的工具执行仍落 worker 本机 AIO sandbox，隔离不降级。
- run 期间由 agent 代持租约续约；子进程退出 → 上报 outcome → placement 终态。

### 5.4 错误处理

- worker 宕机/失联：心跳断 → Registry 标 offline → 租约过期 → run 标
  `interrupted`（与现有 run 生命周期一致），placement 重新 pending 参与再调度。
- Gateway 重启：pending/claimed 均在 Postgres，不丢；重复领取由唯一约束 +
  租约双重防护。
- NFS 不可用：runner 启动前挂载探测，fail fast 并回报原因。
- 版本漂移：注册上报 `agent_version`，不兼容拒绝 claim。

### 5.5 安全

- worker 零入站端口，全部出站连接。
- node-agent → Gateway 走内部 token。
- `preferred_target` 等调度 hint 仅内部身份可设（客户端传入会被丢弃）。

## 6. 测试策略（backend 强制 TDD，`backend/tests/`）

单测：

- Registry 状态机（注册/心跳/离线/draining）。
- AutoRouter 决策矩阵（来源 × 容量 × hint）。
- 租约过期、重复 claim、唯一约束。
- `EcsSandboxProvider` 的 acquire/release/机器故障转移。

集成测试：

- provider 对本地 docker 起的 AIO 容器跑 sandbox 契约测试（第一期）。
- fake node-agent 起真 runner 跑通「提交 run → 领取 → 执行 → 事件回流 →
  前端可见」全链路（第二期）。
- worker 中途杀进程，验证 interrupted + 重调度（第二期）。

## 7. 分期交付

| 期 | 内容 | 验收 |
|----|------|------|
| 一 | machines 表 + Registry 扩展（service+routers）+ EcsSandboxProvider + NFS 挂载约定 + AutoRouter v0 | 聊天指定机器后工具落在该 ECS；机器 API 可查健康状态 |
| 二 | run_placements 表 + AutoRouter v1 + node-agent + runner entrypoint + 重调度 | 定时/重任务自动落到 worker；前端无感知看流式输出；worker 宕机可恢复 |

## 8. 后续衔接

本文档评审通过后，转为 openspec change（`openspec/changes/add-ecs-fleet/`：
proposal.md + design.md + tasks.md，需先 `openspec init`），再用 superpowers
writing-plans 出实施计划。
