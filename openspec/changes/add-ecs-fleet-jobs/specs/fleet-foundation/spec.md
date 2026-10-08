## ADDED Requirements

### Requirement: Optional installation and strict configuration

The system SHALL satisfy B01: 原系统正常启动且不导入 Fleet；错误 Fleet 配置启动即失败，不静默降级。
实施必须遵循以下边界：包入口为 deerflow_ecs_fleet:install，table_prefix=fleet_；单独的 extensions dependency group。测试通过 uv --with 本地包加载，默认安装仍可禁用；不得让 harness import app 或可选扩展。

#### Scenario: B01 observable contract
- **WHEN** 未安装 Fleet 或显式 disabled 时启动原系统；启用时缺少 Postgres/profile
- **THEN** 原系统正常启动且不导入 Fleet；错误 Fleet 配置启动即失败，不静默降级

#### Scenario: B01 regression evidence
- **WHEN** 构建两个 subprocess：未安装扩展/disabled 与 enabled；enabled 用 sqlite、负数预算、未知协议字段逐项触发校验；确认普通启动不创建 fleet 表。
- **THEN** 实际观测满足：disabled_imports_fleet = false；invalid_configs_rejected = true；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Independent migration and persisted execution identity

The system SHALL satisfy B02: 只升级一次独立链；宿主不创建/删除扩展表；测试在隔离 Postgres schema 执行。
实施必须遵循以下边界：Migration 在 ExtensionService.start、host bootstrap 之后、worker API ready 之前运行；用 asyncio.to_thread 或 async connection.run_sync 避免阻塞。所有 fleet 主键、UTC deadline、attempt/job 唯一索引和可空 run_id 的 XOR 约束在此创建，agent kind 在 B 阶段拒绝 claim。

#### Scenario: B02 observable contract
- **WHEN** 两个 Gateway 同时启动扩展迁移，或宿主 autogenerate 检查现有 fleet 表
- **THEN** 只升级一次独立链；宿主不创建/删除扩展表；测试在隔离 Postgres schema 执行

#### Scenario: B02 regression evidence
- **WHEN** 采用 test_scheduled_task_postgres.py 的随机 schema/清理方式；并发启动两个 ExtensionService；断言 fleet_alembic_version 唯一和 private metadata，重启后提交记录仍可读。
- **THEN** 实际观测满足：migration_heads = 1；host_owns_fleet_tables = false；restart_preserves_rows = true；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Node scoped authentication without session bypass

The system SHALL satisfy B03: 全部越权请求 401/403；管理请求仍受 session/CSRF；有效节点只可操作自己的 worker 端点。
实施必须遵循以下边界：不能将扩展 router 加到 public/CSRF 全局白名单。worker 认证成功后只对该精确宿主路由使用 bearer 语义，禁止 auth-disabled 模式让 worker 端点无凭据执行。

#### Scenario: B03 observable contract
- **WHEN** 节点 A 凭据访问节点 B 的 attempt、用户管理路由或过期 session；管理员正常注册节点
- **THEN** 全部越权请求 401/403；管理请求仍受 session/CSRF；有效节点只可操作自己的 worker 端点

#### Scenario: B03 regression evidence
- **WHEN** 使用 ASGITransport 发送有效/无效 bearer、cookie、混合凭据以及 root_path 变体；撤销后再次 renew；检查响应和 DB 未发生越权修改。
- **THEN** 实际观测满足：cross_node_status = 403；revoked_status = 401；management_requires_session = true；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Shared atomic capacity and draining

The system SHALL satisfy B04: 只有一个 claim；draining 无新预留，隔离资源计入使用量，带执行节点不能删除。
实施必须遵循以下边界：以 CPU 毫核、内存 MiB 整数计账；B/C 保留统一 kind/agent_units 字段。全局公平调度在 BC05 增强，B 初版 FIFO。

#### Scenario: B04 observable contract
- **WHEN** 两个 dispatcher 同时争抢最后一个槽位；节点 draining 或有未知执行
- **THEN** 只有一个 claim；draining 无新预留，隔离资源计入使用量，带执行节点不能删除

#### Scenario: B04 regression evidence
- **WHEN** 两个独立 Postgres session 用 asyncio.gather 同时 claim；capacity 只够一项。隔离 winner 后再次 claim；drain、心跳恢复、删除依次核验。
- **THEN** 实际观测满足：claims_won = 1；claim_while_quarantined = null；draining_after_heartbeat = true；不得用硬编码期望值代替真实状态或进程证据
