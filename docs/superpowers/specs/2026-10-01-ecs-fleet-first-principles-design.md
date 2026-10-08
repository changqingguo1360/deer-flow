# 个人 Agent 与 ECS 工作池：持久 Job 与远程 Agent 的统一设计

> 阅读说明（2026-10-08）：本文保留原设计时的状态与拟议名称；当前本地实现/验收范围见[交付记录](../../ecs-fleet-delivery.md)，实际字段、约束和迁移见[库表参考](../../ecs-fleet-database.md)。

日期：2026-10-01
状态：B/C 并存实施依据；B 实施中，尚未通过阶段验收
关联：[原 ECS Fleet 设计](2026-10-01-ecs-fleet-design.md)

本文件独立于原设计，在第一性原理分析的基础上，同时设计 B（持久 job）与 C（完整
remote Agent runner）。两者均属于本次目标，不是二选一，也不再将 C 留作可选需求。
文中新增接口、状态、表和配置均为拟议契约，实施时须分别完成核心接入与验证。

## 1. 需求与基本决策

用户给出目标后，系统利用已有 ECS 完成工作，不需要手动选机或保持网页打开；能够
跟踪执行、取消、取得结果，并在结果到达后继续推理。

### 1.1 两类执行能力

| 能力 | 执行单位 | 决策位置 | 典型工作 |
|---|---|---|---|
| B：持久 job | 明确输入、命令、预算、输出的程序任务 | 调用它的本地或远程 Agent | 批处理、构建、数据清洗 |
| C：远程 Agent | 完整 Agent run：模型调用、规划、工具循环 | ECS 上的 runner | 多轮分析、调查、根据结果决定下一步 |

B/C 可以同时部署、共享机器，也可以组合为“远程 Agent C 提交计算 job B”。模型
通常通过 API 调用，C 不意味着在 ECS 上部署模型权重。远程部署也不自动意味着
控制面离线时可无限自治；本版采用统一的有租约执行策略。

### 1.2 已确定范围

- 固定同构 ECS 池，管理员注册；不调用云 API 创建或释放机器。
- 控制面 ECS 运行 Nginx、Frontend、Gateway、Postgres、Redis；本版仍为单控制面部署。
- node-agent 出站拉取；宿主机共享 NAS；任务容器只挂授权的子目录。
- 保留本地 Agent 执行：快速交互可以留在控制面，B 承接计算，C 承接完整远程推理。
- 自动选机由系统完成；执行类别由受控入口和任务 profile 决定，不依赖模型准确预测任意任务的耗时。
- 支持从提交开始远程执行，以及在正常保存状态后以新 run 继续；不做任意运行中进程热迁移。
- B/C 共用节点身份、容量、attempt、租约和隔离机制，分别维护业务状态。

### 1.3 不变量

1. 同一用户/线程同时至多一个有写入权的 Agent run，包括 local 与 C。
2. 同一执行尝试只有一个有效 owner；租约过期不能证明旧进程已经死亡。
3. 所有 B/C 资源预留来自同一本账，不能各算各的机器容量。
4. 未获 start 授权不能执行；结果不明的已启动尝试默认不自动重跑。
5. 终态 run 不重新变为 pending；后续推理使用新 run，并记录关联。
6. 完成以持久状态及接受的产物为准，不以心跳、stdout 或 SSE 结束单独判定。

## 2. 从第一性原理推导的架构

远程计算、持久后台执行和远程自主推理是三个不同需求。B 解决前两个；C 负责完整
推理循环。把 C 包装成普通 shell job 会遗漏 checkpoint、取消和会话语义；把所有 B
都变成 Agent run 又会引入不必要的模型调用和生命周期成本。

因此采用：**共享 Fleet 控制层 + 两类执行器 + 现有 DeerFlow 会话运行时**。

```text
浏览器 / IM / Scheduler
          │
          ▼
Gateway：鉴权、线程互斥、run 准入、任务管理、结果通知
    ├─ Local Agent run ───────────────────────────┐
    └─ Fleet Agent backend → C run placement      │ submit job
                                │                ▼
Fleet Service：节点、队列、统一资源预留、租约    B job + 长期任务跟踪
                                │                │
                       node-agent 出站 claim/renew/report
                                │
                 ┌──────────────┴─────────────────┐
                 │                                │
           C runner 进程                     B job 容器
           模型/规划/工具                    明确程序任务
                 ├─ 独立本机工具容器              │
                 └─ submit B job ─────────────────┘

Postgres：Agent 状态、checkpoint、持久事件、Fleet 状态
Redis：现有实时流传输；不作为新执行队列的唯一来源
NAS：版本化输入、独立执行目录、接受后的产物
```

同一台 ECS 可运行 C 与 B；运维也可通过 profile 将部分容量保留给特定执行类别。
A（远程同步 sandbox）作为现有短工具模式保留，但不作为实现 B/C 的必经前置项目。

## 3. 对象模型与状态归属

### 3.1 四个不同对象

- **Thread**：用户会话，沿用现有身份、历史和文件授权。
- **Run**：一次 Agent 推理执行，本地或远程，沿用现有公共 run 状态与 SSE。
- **Agent task**：本设计新增的远程目标跟踪对象，关联一个或多个连续 run，可处于等待
  子 job、暂停或人工处理状态；不是第二套 checkpoint 引擎。
- **Job**：程序计算任务，可由 local run 或 C run 提交，独立于提交者 run 的存活时间。

每个 C 任务初始建立一个 Agent task 和一个 run；没有子任务等待时通常只有这一个 run。
C 等待 B 后继续时，同一个 Agent task 关联新的 run。B-only 场景不要求额外创建 Agent task。

### 3.2 唯一权威

| 状态 | 权威 |
|---|---|
| Agent run 准入、公开状态、checkpoint | DeerFlow 核心运行时与共享 Postgres |
| B job 状态、结果 manifest | Fleet Service |
| C 的位置、attempt、物理资源 | Fleet Service；与 run ownership 原子衔接 |
| C 跨 run 的目标、等待与取消意图 | Agent task coordinator |
| B 的用户侧长期任务投影和通知 | McpTaskService，从 Fleet 结果收敛 |
| 容器与进程是否停止 | node-agent 实际核对，失联视为未知 |
| 容量 | Postgres 中未释放 reservation，心跳仅辅助健康与排序 |

## 4. 代码基础与必须承认的核心改动

已有 `McpTaskDriver.submit/get_status/cancel` 与 `McpTaskService` 可以承接 B 的持久
跟踪；当前 Gateway startup 显式注册 driver，需新增受控 Fleet driver 接线。

当前 Gateway 在 run 准入后直接创建本地 asyncio task。现有扩展 service、routers、
生命周期回调没有替换执行后端的公共契约，因此 C 必须增加核心接口，不能承诺纯扩展。

| 层 | 拟议改动 |
|---|---|
| 扩展包 `deerflow-ecs-fleet` | 节点、session 管理 API、调度策略、B driver、部署适配 |
| Gateway/core | 窄 worker bearer API 与节点鉴权、RunExecutionBackend、原子 remote admission、远程取消/ownership、Agent task continuation |
| Harness | 复用完整 run_agent；持久写入 fencing；远程事件适配；受控 await_fleet_jobs 工具 |
| node-agent | 节点会话、claim/start/renew、容器/runner 启停、残留进程核对 |
| 前端/IM | 保留 run 流；新增统一任务摘要中的 waiting/unknown/cancel-pending 与关联 job/run 链接 |

不通过覆写同路径路由、monkey-patch 或直接调用裸 `graph.astream` 替代完整生命周期。
新增任务状态的展示需要明确的 UI/IM 接入，不承诺所有新增状态“前端完全无改动”。
Fleet 独立表使用私有 metadata、`fleet_` 前缀和独立 Alembic 链。按仓库迁移指南，
ExtensionService.start 在宿主 bootstrap 之后、Fleet readiness 之前持 Postgres advisory lock
执行升级，多个 service 启动必须串行化；核心字段走现有 Alembic。扩展管理路由保持
session/admin 鉴权，node bearer 协议由宿主专用路由校验，不能让扩展路由绕过 session。

## 5. 共用数据模型与协议

### 5.1 表与约束

| 表 | 主要字段 |
|---|---|
| `fleet_nodes` | id、name、admin_state、last_seen_at、session_id、protocol_version、runtime_digest、allocatable_cpu/memory、agent_limit |
| `fleet_jobs` | id、user_id、thread_id、source_run_id、tracking_task_id、idempotency_key、dedupe_group、spec、state、active_attempt_id、deadlines、accepted_manifest |
| `fleet_agent_tasks` | id、user_id、thread_id、state、current_run_id、generation、deadline、continuation_budget、wait_group_id、cancel_requested_at |
| `fleet_run_placements` | run_id(pk)、agent_task_id、requested_backend、node_id、profile、state、active_attempt_id、launch_spec_ref、queue_deadline |
| `fleet_attempts` | id、kind(job/agent)、job_id 或 run_id、attempt_no、node_id、node_session_id、token、state、lease_expires_at、process_ref、workspace_ref、outcome |
| `fleet_reservations` | id、node_id、attempt_id、cpu、memory、agent_units、state、released_at |
| `fleet_job_links` | agent_task_id、generation、job_id、mode(detached/awaited)、wait_group_id、delivery_state |
| `fleet_wait_groups` | id、agent_task_id、generation、state、sealed_job_ids、continuation_key、checkpoint_ref、workspace_manifest_ref |

attempt 的 job_id/run_id 必须恰有一个非空，并与 kind 一致；分别建立外键、attempt_no
唯一索引与“至多一个活跃 attempt”约束。并发保护还必须包含事务锁和条件更新，不能仅靠唯一索引。

用户域 submission key 唯一；非空 dedupe_group 至多一个未解决 job；每线程至多一个
非终态 Agent task。新用户输入可暂停该 task，之后由用户继续或关闭，不能创建竞争目标。

### 5.2 Worker 控制协议

register-session、heartbeat、claim、start、renew、progress、complete、cancel-ack。
claim 返回 kind、attempt/token、资源预留、版本要求与受限执行描述。返回的资源是授权值，
worker 不能自行扩大或更换用户/线程。

默认心跳 10 秒、续约 30 秒、租约 120 秒；服务端用 DB 时间，worker 用单调时钟设置
提前停止期限。node session 每次守护进程启动重建，旧 session 不能续新尝试的租约。
重复 start/complete 幂等；获得 start 授权但启动结果不明，按可能已执行处理。

### 5.3 状态模型

| 对象 | 状态 |
|---|---|
| B job | staged → queued → claimed → running → succeeded/failed/cancelled；异常分支 unknown/quarantined |
| C placement | queued → claimed → running → succeeded/failed/interrupted；异常分支 recovery_required |
| Agent task | queued → running → waiting_jobs → queued（新的 run）；也可 paused/input_required、succeeded/failed/cancelled/recovery_required |
| 公共 Agent run | 沿用 pending/running/success/error/timeout/interrupted |
| reservation | reserved/active/quarantined/released |

cancel 是持久 intent，不以“请求已接受”冒充远端已经停止。公共 run 终态与物理资源
释放可存在时间差；待清理资源必须继续计入 reservation，并展示停止待确认。

## 6. 共用调度：容量、公平性与防等待死锁

### 6.1 资源账与准入

- operator 定义标准 B/C profile 的 CPU、内存、超时与镜像；值必须依据机器规格配置。
  C 的预留包含 runner、全部本地 subagent 和本机工具容器的总预算。
- 同一节点的 B/C claim 必须使用同一事务资源账，同时锁定执行项与 node 预留计数，
  CPU/内存之和不超过 allocatable；agent_units 不超过 agent_limit。
- 控制面系统资源先扣除；无资源时排队。不能因心跳报告低 CPU 就超卖内存预算。
- claimed、running、未确认停止的 quarantine 都占资源；waiting_jobs 只有在旧执行已确认
  停止后才不占执行资源。C 提交 B 时，B 单独预留，不能挤入父 C 的已有容器预算。

### 6.2 排序与队列

B/C 各自 FIFO，队列同时非空时按类别轮转，跳过暂不适配的节点；只调度版本兼容、
健康、enabled 的节点。定期提升久候项优先级；首版不实现抢占，所有执行都有期限。
指定节点不可用时排队，不偷偷改派。queue deadline 不因 claim 失败或重启而重置。

默认 queue timeout 为 30 分钟，操作员可配置。任务 profile 超过全池任何单节点能力时
提交即拒绝，不让不可能执行的任务永久排队。

### 6.3 C 等待 B 时避免占满池

长任务不能用同步轮询工具阻塞 C。`await_fleet_jobs` 是持久让出控制的操作：保存
checkpoint 与 workspace、结束当前 run、停止工具进程、释放 C 资源，再进入 waiting_jobs。
结果到达后排队申请新的 C 资源。具体原子边界见第 9 节。

此外为 B 保留至少能放入一个标准 B job 的全池容量：新 C 的准入不得耗尽这份保留量。
部署校验确认保留资源在一个实际节点上可用，不能只看跨节点资源总和。资源不足以同时
容纳标准 C 与保留 B 的小池采用显式串行模式：允许 C 临时使用这份保留资源，但同一
时刻只运行一项 B 或 C；C 提交异步 B 后必须让出再执行 B。串行模式不保留额外同步
等待接口，两个类别均按轮转队列获得执行机会。

首版不允许 B 再提交 B/C，不提供 C→远程 C 的递归派发；C 内现有本地 subagent 在
父 attempt 预算内运行。这样执行依赖只能是 C→B，避免循环等待。

## 7. B：持久程序任务

### 7.1 JobSpec 与提交

包含 schema_version、允许镜像、argv/脚本引用、只读输入版本、资源 profile、执行期限、
网络策略及输出规则。身份从用户鉴权上下文派生；C 提交时由当前 attempt token 绑定
parent run/task/generation，不能让 runner 或模型任意指定父身份。

1. 可信 wrapper 生成稳定 tracking ID 与提交幂等键，重试沿用；tool_call_id 需结合
   user/run/服务端调用身份，不能视为全局唯一。
2. driver 持久化 staged job；此时不可 claim。
3. McpTaskService 保存 tracking row。reconciler 核验 job/user/thread/归属一致且跟踪
   已提交后，才转 queued；有关父 task 已取消时关闭 staged，不启动。
4. staged 无跟踪记录默认 10 分钟超时关闭；迟到 tracking 收到明确失败。
   现有 submit 后保存的补偿取消只是辅助，不能代替该可执行性门槛。

### 7.2 执行、产物与安全

每 attempt 独立非特权容器，容器名称由 attempt ID 派生；输入使用固定快照，只读挂载，
输出写 `jobs/<job_id>/attempts/<attempt_id>/outputs`。不挂整个 NAS 或可变会话工作目录。

worker 等待容器及子进程全部退出、封存结果后，提交带路径、大小、摘要和退出码的
manifest。服务端校验当前 token、授权目录、路径穿越、符号链接及限额，DB 接受为完成点。
旧 attempt 文件即使留在 NAS 也不能覆盖已接受结果。用户通过带 user/thread 授权的文件
API 读取，后续编辑需显式导入副本。

B 不持有模型管理凭据、Agent checkpoint 或控制面数据库权限。需要业务凭据时由
profile 引用限定范围凭据，不继承父 C 的全部运行时环境。

### 7.3 状态与结果通知

Fleet Driver 将 queued/执行中/终态投影为现有长期任务的 submitted/working/对应终态。
unknown 保留跟踪退化信息；确认需人工处理时投影 failed，同时 Fleet 保留隔离资源记录。

detached job 复用现有幂等通知 run；awaited job 由第 9 节 coordinator 消费结果，禁止
同时走普通通知路径启动另一条推理。两种交付共享持久 delivery receipt，类型在提交时确定。

## 8. C：完整远程 Agent 执行

### 8.1 核心 RunExecutionBackend

新增显式 Local/Fleet backend 契约。Gateway 统一鉴权、线程互斥、幂等 admission 和取消；
Local 保留现有流程，Fleet 将 run、placement、launch spec 与 Agent task 的关联原子提交。
远程路径不创建本地 agent task，不能先落 run 再 best-effort 写 placement。

排队阶段由 dispatcher 持有 run ownership；claim 事务将 run owner 与 placement attempt
绑定并更新同一到期时间。node-agent 续约同时条件更新 run/attempt；不得两套心跳独立延租。
现有 orphan recovery 必须识别该 ownership，避免把健康远程 run 错判为本地失踪任务。

### 8.2 LaunchSpec 与运行时

LaunchSpec 固定 user/thread/run/task/generation、assistant、input、归一化 config、
stream modes、interrupt 参数、recursion limit、模型/技能/插件版本、资源与期限。
当前公开 runs.kwargs_json 为脱敏记录，不能直接充当完整远程启动参数。

runner 复用 `run_agent` 的启动屏障、Agent 组装、middleware、memory、subagent、
checkpoint、统计与完成清理；不是简化的 graph.astream 脚本。镜像和配置快照有版本
摘要，不兼容拒绝 claim。密钥通过独立 secret reference 注入，不进入公开 run API。

runner 与本机工具容器分开：runner 是可信服务进程；工具通过独立 AIO 或现有适配器
在非特权容器运行，其端口仅 worker 本机可达，不对控制面开放。

### 8.3 持久化、fencing 与事件

C 的可信 runner 经私网访问共享 Postgres，以复用现有 runtime/checkpointer；这与 B
仅通过 Fleet API 的权限边界不同。runner 不持有 schema/admin 权限，工具容器不继承连接。
初版不新建通用 checkpoint RPC 系统。worker 基础设施受信任，token fencing 防旧进程
误写，不把该设计描述为对恶意已获数据库权限节点的隔离。

所有 C 的 checkpoint 写入、pending writes、run/thread 终态、持久事件、memory 写入
及有状态扩展提交，都需经带 owner/token 校验的写入适配层。校验与写入在同一数据库
事务内锁定 ownership，不能“先查有效、再另开事务写”。不支持此契约的有状态扩展
禁止在 C profile 启用；外部集成副作用不因此获得 exactly-once 保证。

先提交持久事件，再通过带序号的 outbox 发布到现有 Redis StreamBridge；Gateway
过滤失效 attempt，支持游标重放和按序去重。runner 不直接把未经接受的原始事件写 Redis。
终态和结束事件可由 reconciler 根据 DB 补发；网络回包丢失不能触发第二次执行。

### 8.4 C 的文件与恢复点

C 使用 attempt 专属可写 workspace，初次由会话授权文件快照准备；虚拟路径沿用
`/mnt/user-data/workspace/uploads/outputs`，但底层不与 B 输出或其他 attempt 共写。
发布文件时生成不可变产物版本，通过 manifest 接受后立即供 Gateway 文件 API 读取；
未结束 run 的产物标为阶段产物，不宣称任务已完成。

在正常结束或让出等待时，停止所有工具写入并封存 workspace manifest，与最终
checkpoint ID 一起记录。后续 run 由该版本创建新工作目录，必要时显式导入 B 的结果。
异常退出若 checkpoint 与文件无法形成一致恢复点，进入 recovery_required，不能
仅根据最后一个 checkpoint 自动重播有副作用工具。

### 8.5 完成、取消与故障

- 最终 checkpoint/文件封存完成后，原子提交 run、placement 和 task 的下一状态；只有
  确认 runner/工具进程组退出后释放资源。重报 outcome 幂等。
- 普通用户停止当前 C run 同时暂停所属 Agent task、递增 generation，阻止旧等待结果
  自动续跑；运行中公开状态沿用现有取消语义，物理清理状态另行追踪。
- `cancel_agent_task` 永久取消目标、禁止所有新 continuation，并请求取消未完成 awaited jobs；
  detached jobs 保留。失联子 job 保持停止待确认，不能伪报已停止。
- graph 人工中断进入 task.input_required，沿用现有 checkpoint/resume 入口、用新的 run
  准入恢复，不把它当作 worker 故障。后台非交互模式仍由可信服务端设置。
- start 后失联：停止续约、拒绝迟到写入、隔离 attempt 和线程恢复权限。只有确认旧 runner
  与工具已停止并核对副作用后，才允许新 run 恢复；首版无自动故障重放。

## 9. C→B 组合协议：提交、等待、继续

### 9.1 子任务归属

`submit_fleet_job` 支持两种服务端校验后的 link mode：

- detached：提交后独立执行，C 可继续或结束；结果按普通长期任务通知。
- awaited：作为当前 Agent task 的依赖，登记 task/generation 与 job link，结果交给 coordinator。

Agent task 不能在尚有未消费 awaited job 时标为 succeeded；若模型正常结束但未调用
await 工具，运行时为这些依赖建立等待组并执行相同的安全让出流程。子结果必须经过
后续 run 消费，才能完成父目标；失败结果可由 Agent 解释后结束，不强制自动重试。

模式确定后不在执行中随意切换。wait group 是一次封存的 job 集合，首版只有 all-settled
策略：全部到达终态后把成功、失败、取消结果一起交给 Agent 决策，不让任一失败隐式
重跑其他任务。unknown/quarantined 需要人工处置或任务 deadline，不无限静默等待。

### 9.2 让出与继续的时序

1. C 提交一个或多个 awaited jobs；这些 job 可先完成，结果始终持久保存。
2. C 调用 `await_fleet_jobs(job_ids)`；可选 IDs 使用提交工具返回的公开 `task_id`，
   服务端解析到原 job 并校验当前 task/generation 的 awaited 归属；不传则收集所有剩余依赖。
   写入 preparing wait group，并请求运行时在工具消息已保存的安全边界结束本次 run。
3. 运行时停止后续 graph 步骤、完成 checkpoint、停止本地工具并封存 workspace。
   在原终结事务中提交 run.success、精确 checkpoint/workspace 配对及 waiting_jobs 的
   目标状态；task/placement 沿用已实现的 finishing 屏障。工具返回“已等待”的状态需
   完整进入会话，不能留下未配对的 tool call。
4. worker 确认旧 runner 和工具进程全部退出；原 STOP 事务按已接受配对应用
   task.waiting_jobs/placement.succeeded 并释放 C reservation。wait group 此前不得
   启动 continuation，即使全部 B 已经完成。
5. coordinator 从持久状态同时检查：全部依赖终态、父 task 未取消/暂停、generation 未变、
   当前 run 已终态、资源清理已确认、线程允许新准入、预算/deadline 未耗尽。
6. 用稳定键 `(agent_task_id, generation, wait_group_id)` 原子创建新的 run/placement、
   更新 current_run_id 并标记 group 已派发。新 run 可以调度到另一节点。
7. 新 runner 载入接受的 checkpoint/workspace，把 child 结果作为不可信数据注入，继续
   同一目标。旧 run 保持 success，不重新变 running，也不重新提交已存在的 jobs。

这里 success 表示本次 Agent run 正常结束；Agent task 仍显示“等待计算”，不表示用户
目标已经完成。这需要新的运行时受控让出机制及任务摘要展示，不把现有 graph interrupt
直接等同于 success，也不宣称现有 McpTaskService 已具备该 continuation 协议。

BC03 首版新 awaited 提交在原父任务事务中限制每个 parent run 最多128个，先检查再解析输入或创建工作；同键重取不额外占容量，detached 不计入该限制。续跑摘要先保留全部 job/state/manifest 引用，再按64KiB UTF-8 总预算添加可选错误、文件详情并标记省略；文件路径保留完整接受值。历史较大等待组不得静默删成员，其部署兼容需明确核查。

### 9.3 竞态和用户插入消息

- 结果先到、wait 后封存：coordinator 从 DB 对账，不能只依赖一次易丢的回调。
- 重复结果、Gateway 崩溃：continuation admission 幂等键确保只创建一个后续 run；
  job 结果交付与普通通知使用互斥的 delivery owner，避免双重推理。
- 用户在 waiting 期间发起新修改性对话：准入事务暂停旧 task 并递增 generation，
  普通用户 run 获得线程写入权；旧结果保留但不再自动继续旧目标。用户之后显式继续时
  基于最新线程状态重新确认目标，并创建新 generation；只读查状态不暂停。需要继续使用
  旧子任务结果时，服务端显式迁移 link 到新 generation 并记录审计，不重新提交同一 job。
- 其他后台通知 run 在 task.running/waiting_jobs 期间延后投递，不能抢先修改恢复点；
  thread 删除/回滚/编辑历史同样要暂停或关闭 task 并使旧 generation 失效。
- continuation 与取消/新用户准入共用 task/thread 锁顺序；谁先提交即决定有效 generation。
  continuation 已启动时用户走现有 busy/cancel 语义，不能双写线程。
- awaited job 已提交、C 在让出前崩溃：进入 recovery_required，保留 job 跟踪；不得在
  未确认旧 C 停止与恢复点之前自动续跑。child 完成不会替代这项确认。
- task 可在受控配置的预算内多次 C→B→C；累计 run 次数、模型 token、job 数和总 deadline
  都跨 continuation 计数。到限进入人工处理，不通过新 run 重置配额。

## 10. 路由、Scheduler 和对外语义

### 10.1 路由规则

服务端接受用户的 execution preference（local/remote/auto）与允许的 task profile，
并归一化成可信后端选择。profile 明确远程时走 C，快速交互默认 local；两者均能提交 B。
机器名只是可选管理员约束，普通用户不需要了解节点。

Scheduler 来源不等于重任务：定时定义可以指定 remote Agent profile，也可本地 run
只提交 B。内部 owner/non_interactive/placement/token 不能由客户端 context 伪造。
初版按显式 profile 和来源默认值自动选后端，开放式自然语言资源分类不作为上线前提。

### 10.2 Scheduler 队列与预算

沿用原 Scheduler occurrence 状态及 `queue_timeout_seconds`、`recursion_limit`。
C 无容量时 occurrence 保持 queued，不提前创建一个长期占 Scheduler 执行预算的 remote run。

有容量时使用短期 reservation ticket，经原幂等 launch 入口原子消费，创建 run/placement；
失败由 reconciler 回收/接续，等待年龄不重置。ticket 用同一资源账，claimed/running 才
进入实际执行预算，Scheduler launching 的短租约及现有全局预算保持有效。

一次 occurrence 的结果仍表示它触发的 Agent run 结果；后续 C continuation 由 Agent task
跟踪，不无限保持原 occurrence.running。API/UI 同时提供关联 task 状态，避免把 run.success
显示为整项研究已完成。默认同一 scheduled task 只允许一个未解决 Agent task，后续
occurrence 留在 durable queued，受原 queue timeout 限制，不 skip-on-overlap。

B-only 的定时脚本按预定义命名 job slot 生成可信 dedupe_group；同一 slot 的未解决 job
不能被后续定时触发重复提交。C task 内允许多个命名子 job，不用一个全局 schedule key
把合法扇出错误地合并。

## 11. 网络、安全、故障和运维

### 11.1 网络与权限

| 主体 | 允许访问 |
|---|---|
| node-agent | 控制面私网 HTTPS、镜像仓库、NAS；绑定 node 凭据可轮换撤销 |
| B 容器 | 授权文件和 profile 允许的业务出口；无 Agent DB/模型管理凭据 |
| C runner | 控制面 API、受限 Postgres、模型及配置允许的服务；运行时密钥不透传工具容器 |
| C 工具容器 | 当前 attempt 文件视图与批准业务出口；本机 AIO；无 Docker socket/运行时 DB 凭据 |

worker 无公网入站，node-agent 控制协议全出站。C 直连 Postgres 也是 worker 出站，但
增加了控制面数据库的私网访问面，需独立网络规则。所有写 API 验证 node/session/attempt。

### 11.2 故障处置

| 故障 | B | C |
|---|---|---|
| 未获 start 的 claim 过期 | 撤销 token、回收并重排 | 同左，同时修正 run ownership |
| start 后网络分区 | unknown、隔离资源、不重派 | recovery_required，隔离资源和线程恢复权限 |
| Gateway 重启 | 恢复 staged/queue/通知，不重复执行 | 恢复 ownership/placement/wait group，保留有效远程 owner |
| worker 重启 | 先盘点残留容器再 claim | 同时盘点 runner 与本地 subagent/工具进程组 |
| NAS 故障 | 停止，禁止回退到本地空目录 | 同左；无法匹配 checkpoint 与文件则人工恢复 |
| complete 回报丢失 | 返回已接受 manifest | 返回已提交终态，按 DB/outbox 补流结束 |
| Redis 故障 | 任务状态仍在 DB | 持久事件保留，实时流退化；不重复运行 |
| 外部副作用结果未知 | 人工核对 | 人工核对，不从 checkpoint 盲目重播 |

worker 失去续约在期限前停止新工具调用并终止对应进程组/容器；若无法证明停止，保留
隔离预留。即使旧 token 无法写 DB，仍不能撤销已经发出的外部请求或 NFS 写入。
首版仅对未授权 start 的尝试自动重派；正常 C→B→C continuation 不属于故障重试。

### 11.3 运维

admin_state 与健康分开；drain 禁止新预留，已有执行结束后升级。校验协议、harness、
插件、技能和镜像版本，不只比较一个 agent_version。记录 task/run/job/attempt/node
关联，监控队列、隔离资源、续约失败、取消待确认、continuation 积压和通知失败。

Postgres/NAS 备份并演练恢复；控制面故障期间不接新任务，worker 租约到期停止。
本版 C 提供远程完整推理，但不承诺控制面离线自治。日志/产物设限额和保留期，
unknown/recovery_required 数据须保留到人工裁决，不按普通完成任务自动回收。

## 12. 实施阶段与验收

交付顺序用于降低集成风险，B/C 都是目标；完成 B 不代表整个需求完成。

| 阶段 | 交付 | 验收门槛 |
|---|---|---|
| 0：共同基础 | 节点身份、B/C 通用 attempt/reservation、NAS 隔离、迁移 | 并发资源账不超卖；旧 session/未授权 start 拒绝执行 |
| 1：B 闭环 | staged/跟踪握手、job 容器、manifest、通知 | 关闭网页/结束提交 run 后任务仍完成并回到原会话 |
| 2：C 闭环 | backend、原子 admission、完整 runner、fencing、事件/取消/文件 | 本地与远程 run 契约等价；远程失联不重复执行、不迟到写状态 |
| 3：B/C 组合 | awaited links、wait group、让出与幂等 continuation、共享公平调度 | C 提交 B 后释放资源，结果到达继续推理；最小池不等待死锁 |
| 4：统一产品与运维 | 任务摘要、Scheduler 接入、取消/恢复入口、故障演练 | 用户可分辨 run/目标/job 完成，能查看与处理停止待确认 |

### 必须测试的协议边界

- B submit 成功而 tracking 保存失败，staged 永不执行；补偿失败也不产生孤儿执行。
- B/C 并发 claim、隔离容量、最小池与资源保留；任何情况下不双重超卖。
- C admission 在事务边界崩溃；不产生无 placement 的远程 run，不启动本地重复执行。
- C 完整 run_agent 契约：多轮模型/工具、subagent、技能、checkpoint、memory、流重连、
  取消、人工 interrupt/resume、产物和统计，不以仅运行一个 mock graph 验收。
- 旧 C runner 失联但仍运行：checkpoint pending writes、memory、事件、终态迟到写入全部拒绝。
- wait group 结果先到、让出先到、重复通知、释放前崩溃、continuation admission 回包丢失。
- 当前 C 让出后，新 run 在另一节点恢复工作目录并读到 B 结果；旧 run 保持终态。
- 用户输入/取消与 continuation 竞态；generation 失效阻止旧目标自动复活。
- awaited 与 detached 的取消差异；子 job 未确认停止时不得展示全部已取消。
- Scheduler waiting 不占执行预算、票据恢复、关联 Agent task 未解决时后续 occurrence 排队。
- 跨用户文件、符号链接/路径穿越、运行时凭据不进入工具容器、版本漂移拒绝 claim。
- 验收同时检查 DB、实际进程、文件内容及副作用次数，不能仅凭 UI 状态判断恢复成功。

## 13. 下一步与实现依据

用户已指定两种能力都做，先 B、再 C。对应规划已拆成 B（含共用基础）、C、B/C
continuation 三个串行 OpenSpec change 与 Superpowers 实施计划；入口见
[实施总览](../plans/2026-10-01-ecs-fleet-roadmap.md)。各阶段保持同一资源和身份契约，
列出核心改动、迁移、故障测试和验收门槛。规划完成不表示代码已实现或已批准部署。

### 本地代码依据

- `backend/packages/harness/deerflow/mcp/tasks/driver.py`、`models.py`：长期任务 driver 和状态模型。
- `backend/app/mcp_tasks/service.py`：先远端提交后保存跟踪、轮询、取消与幂等结果通知。
- `backend/app/gateway/app.py`：长期任务 driver 的 startup 注册。
- `backend/app/gateway/services.py`：统一 run 准入与本地 asyncio task 启动；远程分支需要改造。
- `backend/packages/harness/deerflow/runtime/runs/manager.py`、`worker.py`：ownership、取消和完整生命周期。
- `backend/packages/harness/deerflow/runtime/runs/schemas.py`：现有公开 run 状态，不含 waiting_jobs。

本文依据代码接口进行设计核对；尚未运行新增能力的实现测试。实现时同步 README 与
相关 AGENTS.md，不能将设计草案中的接口当作已经可用的产品能力。
