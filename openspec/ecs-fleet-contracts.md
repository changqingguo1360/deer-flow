# ECS Fleet 规划共享契约

适用于 B → C → B/C 三个 change。这里固定接口和测试观测口径；不代表现有代码已提供这些 API。
实施遇到签名冲突时先更新相关 proposal/design/plan，再修改实现；不得以方便为由取消 fencing 或原子性。

## 1. 包和身份

扩展名 `deerflow-ecs-fleet`，import `deerflow_ecs_fleet`，入口 `deerflow_ecs_fleet:install`，
表前缀 `fleet_`。启用拓扑要求 Postgres；没有 Fleet 插件的普通 local 安装必须正常启动。

- UserPrincipal：现有用户 session；绑定 user_id。管理员管理 Fleet 不等于普通用户可选任意 profile。
- NodePrincipal：独立随机凭据，服务端存 hash/expiry/revoked_at，绑定 node_id。
- AttemptPrincipal：已鉴权 NodePrincipal + node_session_id + attempt_id + token；权限不能超过原 job/run。
- C runtime：可信 worker 进程，可访问受限共享数据库；工具容器绝不继承该身份或数据库连接。

Host worker API 路由不能通过匿名 path allowlist 简单绕过已有 auth。需要认证成功后构造窄
NodePrincipal，且用户/管理员路由不接受它。extension router 仍只接收 session 身份。
根路径、编码路径、撤销、混合 cookie/bearer、auth disabled 模式均要测试。

## 2. 路由与响应约定

以下都位于 Gateway 原生 `/api` 命名空间；Nginx 现有 proxy 规则不再添加重复前缀。

| 路由 | 身份 | 请求/语义 |
|---|---|---|
| `POST /api/fleet/machines` | admin session | 注册容量与 profile allowlist；不允许模型创建机器 |
| `PATCH /api/fleet/machines/{node_id}` | admin session | enabled/draining/disabled，删除有活动资源返回 409 |
| `POST /api/fleet/node/session` | NodePrincipal | 新 session，旧 session fence；先上报残留资源 |
| `POST /api/fleet/node/heartbeat` | node+session | health/version/进程观测，不直接修改 capacity 使用量 |
| `POST /api/fleet/node/claims` | node+session | 长轮询，返回一份带 kind 的执行授权，或 204 |
| `POST /api/fleet/node/attempts/{id}/start` | attempt | 授权已持久化后才允许启动；重复同 token 幂等 |
| `POST /api/fleet/node/attempts/{id}/renew` | attempt | 原子 lease 更新；返回 cancel intent；失效返回 409 |
| `POST /api/fleet/node/attempts/{id}/complete` | attempt | 持久 outcome/manifest；晚到旧 owner 返回 409 |
| `POST /api/fleet/node/attempts/{id}/stopped` | node+session | physical stop 证据；不凭这个接口授予过期 owner 状态写入权 |
| `POST /api/fleet/jobs` | user / 有效 C attempt | user 来自鉴权，C 的 parent 来自 LaunchSpec；提交 staged job |
| `GET /api/fleet/jobs/{id}` | owner user | 执行状态、取消意图、接受产物；越权统一 404 |
| `POST /api/fleet/jobs/{id}/cancel` | owner user | durable intent，返回 202；不宣称进程已停止 |
| `GET /api/fleet/artifacts/{manifest_id}/{path}` | owner user | 只读 accepted manifest 中安全路径 |
| `GET /api/fleet/agent-tasks/{id}` | owner user | 目标状态/current_run/generation/children/恢复原因 |
| `POST /api/fleet/agent-tasks/{id}/cancel` | owner user | expected_generation；永久取消目标和未完成 awaited children |
| `POST /api/fleet/agent-tasks/{id}/resume` | owner user | expected_generation；确认 paused/input_required 的继续意图 |

C 提交 job 走 host 验证 attempt 后再调用同一 service；不是让普通用户 endpoint 接受任意
worker token。GET/complete 的幂等重试返回已接受结果，不能以租约已自然过期否定历史完成事实。
非法 profile/参数 422；错误身份 401/403；owner 资源不可见 404；并发/失效 token 409；新工作
特性禁用返回 503。关闭新工作开关后 renew/complete/stopped 对既有工作仍可用。

## 3. 稳定 wire 形状

所有 ID 为服务端不透明字符串，datetime 为 UTC ISO；CPU 毫核与内存 MiB 为正整数。
提交内容的 user/thread/owner 字段若与认证归属不符，服务端拒绝而非盲信客户端值。

### B JobSpec v1

```json
{
  "schema_version": 1,
  "task_name": "Build report",
  "profile": "batch-standard",
  "code_artifact_id": "artifact-code-1",
  "argv": ["python", "/inputs/analyze.py"],
  "input_manifests": ["manifest-data-1"],
  "execution_timeout_seconds": 1800,
  "queue_timeout_seconds": 1800,
  "link_mode": "detached"
}
```

image digest、网络出口、文件限额和凭据引用由 profile 解析。示例 ID 仅为合成测试数据。
B/C 独立阶段 awaited 422；组合阶段由有权的 C attempt 提供 awaited，coordinator 绑定父身份。
稳定 submission idempotency key 来自可信调用身份，不由模型自由决定。

### Claim v1

```json
{
  "protocol_version": 1,
  "kind": "job",
  "attempt_id": "attempt-1",
  "node_session_id": "session-1",
  "token": "opaque-attempt-token",
  "lease_expires_at": "2026-10-01T12:02:00Z",
  "resources": {"cpu_millis": 1000, "memory_mib": 2048, "agent_units": 0},
  "spec_ref": "launch-spec-1",
  "output_prefix": "jobs/job-1/attempts/attempt-1/outputs"
}
```

示例资源值只用于测试，生产不能据此自动配置。C 使用 kind=agent、agent_units=1，
资源包括 runner、全部本地 subagent 和工具容器，不允许绕过同一 reservation 另算一份。

### C LaunchSpec v1

必须携带：schema_version、run_id、agent_task_id、generation、user_id、thread_id、assistant_id、
input、normalized_config、stream_modes、stream_subgraphs、interrupt_before、interrupt_after、
recursion_limit、execution_deadline、runtime_digest、skill_snapshot、plugin_snapshot、
workspace_manifest_ref、secret_refs。公开 API 只给脱敏摘要；DB/模型凭据不明文放 runs.kwargs_json。

### Agent task summary

```json
{
  "task_id": "agent-task-1",
  "state": "waiting_jobs",
  "current_run_id": "run-1",
  "generation": 1,
  "cancel_requested": false,
  "recovery_required": false,
  "children": [{"job_id": "job-1", "mode": "awaited", "state": "running"}]
}
```

公共 RunStatus 不新增 waiting_jobs。run.success 可以与 task.waiting_jobs 同时存在；UI 必须区分。

## 4. 事务与时间

- 全局锁序：schedule/occurrence → agent task → wait group → thread/run → job/placement → node → reservation/attempt。
- 未涉及层跳过，多行同类按 ID 排序。不能持 node 锁再反向获取 run/task 锁。
- DB 所有权判断使用数据库当前时间；Postgres 长事务判断应使用当前 wall clock 而不是一直固定在 BEGIN 的时间。事务内获取 row lock 后重新读时间。
- claim/renew 同事务维护 attempt 与 run ownership；lease 过期后不能凭旧 owner 值延租。
- physical stopped 证明独立于结果写入权：旧尝试可以被对账确认停止，但不能因此恢复其写结果权限。
- C checkpoint saver 必须支持锁 owner 与写 checkpoint/pending writes 共用连接与事务。普通 wrapper 检查后调用内部另开连接的 saver 不满足契约。
- 第三方非事务 memory provider 和不声明 remote-safe 的有状态插件初版 C fail closed，不假装纳入 DB fence。

## 5. Continuation 原子条件

仅在全部 child 终态、group sealed、旧 run 终态、进程 stopped、reservation released、
父 generation 一致、无取消、deadline 和累计预算未耗尽且线程可准入时创建新 run。

稳定键为 `(agent_task_id,generation,wait_group_id)`；准入事务同时写新 run/placement、
current_run_id 和 delivery receipt。结果先到时存在 DB；不能依赖单次易丢事件。
用户新修改性消息先暂停旧目标并递增 generation，旧 continuation 无效；普通后台通知
在 running/waiting 期间延后。rollback/delete 同样必须 fence generation。

## 6. 真实验证前置和结果记录

release gate 之前至少执行以下预检；不要输出 URI 或凭据：

```bash
test -n "$TEST_POSTGRES_URI"
test "$FLEET_TEST_CONTAINERS" = 1
docker info > /dev/null
```

C 与组合阶段还需要 `test -n "$TEST_REDIS_URL"`。backend Makefile 的默认 suite 可能跳过
integration，必须另运行阶段验收测试并核对 collected/passed/skipped。CI 不配置真实外部
ECS，全部使用本地容器与隔离 DB schema。

每个阶段生成报告记录命令、commit、通过/失败/跳过数、关键 DB row/PID/manifest 证据。
报告中的日志需脱敏；测试模拟业务副作用用本地 stub HTTP 服务计数，不能发送真实邮件或调用生产 API。
