## ADDED Requirements

### Requirement: Full runtime execution on worker

The system SHALL satisfy C04: 模型循环确在 worker，状态统计与本地路径一致；工具容器不获得运行时密钥。
实施必须遵循以下边界：实施代码必须逐项传递 stream_modes/stream_subgraphs/interrupt_before/interrupt_after，不能复制第二套 graph 执行栈。

#### Scenario: C04 observable contract
- **WHEN** 远程运行包含多轮工具、技能与本地 subagent 的 scripted Agent
- **THEN** 模型循环确在 worker，状态统计与本地路径一致；工具容器不获得运行时密钥

#### Scenario: C04 regression evidence
- **WHEN** 同一 scripted 模型与输入分别运行 Local/Fleet；比较最终消息、usage、checkpoint 引用、artifact 内容；记录 runner PID/host，不以单个 mock graph 代替。
- **THEN** 实际观测满足：remote_model_location = "worker"；local_remote_results_equal = true；tool_db_credentials = false；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Fenced checkpoint writes including pending writes

The system SHALL satisfy C05: 同一事务 owner 校验拒绝全部迟到写，checkpoints/blobs/writes 不改变。
实施必须遵循以下边界：先枚举当前 saver 写方法并加入 parameterized test；cached/delta saver 仍经相同底层写事务，沿用 CheckpointStateAccessor。这个任务未通过前不启用 C。

#### Scenario: C05 observable contract
- **WHEN** 旧 attempt 在租约过期之后执行 aput/aput_writes/delete 等写入
- **THEN** 同一事务 owner 校验拒绝全部迟到写，checkpoints/blobs/writes 不改变

#### Scenario: C05 regression evidence
- **WHEN** 真实 Postgres saver 覆盖每个写入口，故障屏障放在 token 校验与 SQL 写之间；并发替换 owner，确认锁/事务排他而非先查后写。
- **THEN** 实际观测满足：stale_checkpoint_changes = 0；stale_pending_write_changes = 0；check_and_write_same_transaction = true；不得用硬编码期望值代替真实状态或进程证据

### Requirement: All remote durable mutations respect ownership

The system SHALL satisfy C06: 迟到持久状态被拒绝；不支持 fencing 的有状态扩展不能启用 remote profile。
实施必须遵循以下边界：不支持事务的第三方 memory provider 在初版 C 禁用；提供 noop/已适配 backend，不能声称普通 checkpoint fence 自动保护外部服务。

#### Scenario: C06 observable contract
- **WHEN** 租约过期但 memory 后台线程、扩展回调或 finalizer 仍返回结果
- **THEN** 迟到持久状态被拒绝；不支持 fencing 的有状态扩展不能启用 remote profile

#### Scenario: C06 regression evidence
- **WHEN** 延迟 memory 结果到 ownership_lost 之后；测试 callback 执行线程传播 attempt context；启用不支持远程写契约的插件应启动失败。
- **THEN** 实际观测满足：stale_memory_commits = 0；stale_finalization_commits = 0；unsafe_plugin_enabled = false；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Committed ordered remote events

The system SHALL satisfy C07: 按持久 seq 补发，去重并过滤旧 attempt；终态可补 end，不重启 Agent。
实施必须遵循以下边界：不把尚未提交原始 token 直接写 Redis；namespace/subgraph 帧保持现有协议。持久事件限额沿用 run events 规则。

#### Scenario: C07 observable contract
- **WHEN** DB 已提交事件但 Redis publish 失败或回包丢失，客户端重连
- **THEN** 按持久 seq 补发，去重并过滤旧 attempt；终态可补 end，不重启 Agent

#### Scenario: C07 regression evidence
- **WHEN** 阻断 Redis，产生事件和终态；恢复后重放 cursor；重复 outbox ack，检查事件顺序/ID/终态和 runner 启动数。
- **THEN** 实际观测满足：ordered_unique_events = true；runner_starts = 1；terminal_end_recovered = true；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Consistent workspace checkpoint boundary

The system SHALL satisfy C08: 只接受关联 checkpoint 的封存 manifest；异常进入 recovery_required，不重播工具。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: C08 observable contract
- **WHEN** C 正常结束/暂停或发布阶段文件，或异常导致文件与 checkpoint 版本不匹配
- **THEN** 只接受关联 checkpoint 的封存 manifest；异常进入 recovery_required，不重播工具

#### Scenario: C08 regression evidence
- **WHEN** 真实工具写文件后发布中间产物；正常结束接受最终 manifest；故意在文件封存前 kill runner，检查没有自动恢复。
- **THEN** 实际观测满足：published_partial_marked = true；mismatched_restore_allowed = false；automatic_replay_count = 0；不得用硬编码期望值代替真实状态或进程证据
