## ADDED Requirements

### Requirement: Durable user task tracking and result notification

The system SHALL satisfy B09: 结果通过幂等通知回到原 thread；无重复业务执行；任务工具按服务可用性暴露。
实施必须遵循以下边界：B 阶段仅 detached；awaited 参数明确 422，直到 BC 完成再开放，禁止创建无人消费依赖。

#### Scenario: B09 observable contract
- **WHEN** 原 Agent run 已结束、会话暂忙或 Gateway 重启后 B 完成
- **THEN** 结果通过幂等通知回到原 thread；无重复业务执行；任务工具按服务可用性暴露

#### Scenario: B09 regression evidence
- **WHEN** 用 scripted model 真实 run 提交，再结束 run；阻塞 thread admission、完成 job、重启 service、解除阻塞；验证同一结果通知 receipt 与 job 容器启动次数。
- **THEN** 实际观测满足：accepted_notification_runs = 1；job_starts = 1；secret_in_public_payload = false；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Scheduled job deduplication and truthful UI

The system SHALL satisfy B10: 复用既有工作不重复提交；界面保留 tracking_degraded/cancel_requested，不显示已停止。
实施必须遵循以下边界：前端先写纯状态映射单测，再 DOM/交互用例；不新增机器管理 UI。

#### Scenario: B10 observable contract
- **WHEN** 相同 schedule 的同一 job slot 未结束又触发，或取消尚未确认
- **THEN** 复用既有工作不重复提交；界面保留 tracking_degraded/cancel_requested，不显示已停止

#### Scenario: B10 regression evidence
- **WHEN** 内部 scheduler launch 提供可信 schedule ID，客户端伪造同名字段无效；连续触发同 slot；前端用 unknown/cancel-pending 数据渲染任务摘要。
- **THEN** 实际观测满足：scheduled_container_starts = 1；forged_schedule_accepted = false；unknown_label = "需要确认"；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Reproducible deployment and safe disabling

The system SHALL satisfy B11: 旧协议拒绝 claim；停止新工作但保留持久记录；能查到未停止资源。
实施必须遵循以下边界：C 镜像入口本阶段不实现；Docker 测试镜像可无外网依赖，生产镜像 digest 由部署者提供，不提交真实凭据。

#### Scenario: B11 observable contract
- **WHEN** 升级时旧 worker 协议不兼容，或 operator drain/disable Fleet
- **THEN** 旧协议拒绝 claim；停止新工作但保留持久记录；能查到未停止资源

#### Scenario: B11 regression evidence
- **WHEN** 构建镜像并验证 digest；本地两 worker 与 NAS fixture 部署；drain 后无新 claim；disable 后查 DB/日志仍有可定位记录；核对所有端口显式绑定。
- **THEN** 实际观测满足：old_protocol_claims = 0；drained_node_claims = 0；worker_public_ports = 0；不得用硬编码期望值代替真实状态或进程证据

### Requirement: B release gate verifies real side effects

The system SHALL satisfy B12: 所有必需测试实际运行且通过；跳过真实数据库/容器测试不能宣布 B 完成。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: B12 observable contract
- **WHEN** 运行 staged 故障、Postgres 竞争、Docker/NAS/网络分区测试矩阵
- **THEN** 所有必需测试实际运行且通过；跳过真实数据库/容器测试不能宣布 B 完成

#### Scenario: B12 regression evidence
- **WHEN** 用本地 TCP 故障代理断开控制协议且保留脚本执行；统计文件写入与外部 mock 服务请求次数；清理临时容器、测试 schema，不操作真实 ECS。
- **THEN** 实际观测满足：duplicate_side_effects = 0；required_cases_skipped = 0；untracked_starts = 0；不得用硬编码期望值代替真实状态或进程证据
