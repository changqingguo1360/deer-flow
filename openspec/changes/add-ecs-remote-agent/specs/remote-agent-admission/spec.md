## ADDED Requirements

### Requirement: Versioned remote launch specification

The system SHALL satisfy C01: 拒绝不完整配置；规范化启动输入包含完整 run 参数且公开副本脱敏。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: C01 observable contract
- **WHEN** 启用 remote 时配置缺少 checkpoint/事件持久化或 LaunchSpec 含公开密钥
- **THEN** 拒绝不完整配置；规范化启动输入包含完整 run 参数且公开副本脱敏

#### Scenario: C01 regression evidence
- **WHEN** 序列化含 recursion、interrupt、stream_modes 的 LaunchSpec 后重建；模拟技能/插件 digest 漂移；查看公共 run/任务 JSON 无 secret。
- **THEN** 实际观测满足：launch_fields_roundtrip = true；incompatible_claim_rejected = true；public_secret_count = 0；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Atomic remote admission with local parity

The system SHALL satisfy C02: 全部回滚或全部提交；远程无本地 task；local 行为不变且线程互斥保留。
实施必须遵循以下边界：backend registration 契约定义在 harness、app 负责注入，harness 不导入 Fleet 包。移植前先固化 local 准入/取消回归测试。

#### Scenario: C02 observable contract
- **WHEN** 故障发生在 run 写入与 placement 写入之间，或同幂等键重复提交
- **THEN** 全部回滚或全部提交；远程无本地 task；local 行为不变且线程互斥保留

#### Scenario: C02 regression evidence
- **WHEN** 真实准入路径注入 placement insert 错误；独立进程用同 key 重试；spy 仅观察 local task 创建次数，不替代真实 admission 存储。
- **THEN** 实际观测满足：orphan_remote_runs = 0；local_tasks_for_remote = 0；runs_for_key = 1；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Single execution owner across placement and run

The system SHALL satisfy C03: run 与 attempt 一起更新或都不更新；有效远程执行不被本地 recovery 抢占。
实施必须遵循以下边界：锁序与 B 扩展执行项→node 顺序兼容；跨计划新加 parent task 锁必须排在 run 之前，禁止逆序获取。

#### Scenario: C03 observable contract
- **WHEN** claim 或 renew 过程中节点 session/token 被替换，Gateway 重启运行孤儿恢复
- **THEN** run 与 attempt 一起更新或都不更新；有效远程执行不被本地 recovery 抢占

#### Scenario: C03 regression evidence
- **WHEN** 两个 SQL session 模拟 stale renew/owner takeover；重启 Gateway hydration 与 scheduler recovery；断言 run/attempt 相同 token/expiry。
- **THEN** 实际观测满足：split_owner_rows = 0；live_remote_interrupted = false；old_session_renewed = false；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Authorized routing and queued scheduler budget

The system SHALL satisfy C10: 合法 profile 选择 Fleet；内部字段丢弃；无容量 occurrence queued 不占执行预算。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: C10 observable contract
- **WHEN** 客户端指定 remote profile 或伪造内部 hint；定时 C 没有容量
- **THEN** 合法 profile 选择 Fleet；内部字段丢弃；无容量 occurrence queued 不占执行预算

#### Scenario: C10 regression evidence
- **WHEN** 参数矩阵 local/remote/auto 与权限；两个 scheduler 用真实 PG 争票据；模拟消费票据事务崩溃，验证 run admission key 与 reservation 回收。
- **THEN** 实际观测满足：forged_owner_accepted = false；queued_budget_usage = 0；ticket_leaks_after_reconcile = 0；不得用硬编码期望值代替真实状态或进程证据
