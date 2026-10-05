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

#### Scenario: C08 original human-input pause
- **WHEN** 原 root Agent 经 ClarificationMiddleware 产生当前运行的 ask_clarification 请求并返回 Command(goto=END)，或原图发生真实 interrupt
- **THEN** 系统依据绑定原 owner/thread/run 的可信工具执行观察及实际 materialized checkpoint 识别暂停；ask_clarification 即使 next/tasks 为空也保留原 request/tool-call/answered-card 协议，接受 paused 恢复点并记录 core interrupted；等待人类输入时 task input_required，一般图暂停时 task paused。历史消息、客户端字段、subgraph 请求或 suppressed clarification 不得独立暂停 root。最终点和 core 同事务进入 finishing，持有排他及容量至真实物理停止，再应用原 per-run placement/attempt cancelled 结果；不得自动重播工具或另起 runner。

#### Scenario: C08 regression evidence
- **WHEN** 真实工具写文件后发布中间产物；正常结束接受最终 manifest；故意在文件封存前 kill runner，检查没有自动恢复。
- **THEN** 实际观测满足：published_partial_marked = true；mismatched_restore_allowed = false；automatic_replay_count = 0；不得用硬编码期望值代替真实状态或进程证据


#### Scenario: C08 trusted branch source and recovery admission
- **WHEN** A healthy physically stopped Fleet thread branches through the original owner-authorized API, or a new C execution selects an accepted workspace version
- **THEN** The host freezes the exact owner-scoped source checkpoint/point and persists child routing/origin; preparation verifies all source content on every retry and clones into an independent owned destination without Local/B aliases or hardlinks. Missing or changed content persists fenced recovery before Runner start. Historical child file reads use only the exact trusted immutable source mapping and expose source provenance; old source points are never rewritten.
- **AND** Client metadata cannot set/clear execution routing or authoritative branch identity. All nonterminal Fleet tasks retain exclusion despite core terminal status or expired leases; recovery blocks run/start/resume/regenerate/state mutations while owner reads remain available. Healthy terminal host branch/state operations preserve their original capability without admitting a Local run or mutating accepted NAS outputs.

- **AND** An implicit/latest new-turn selector remains unchanged while the workspace source is separately frozen to its matching accepted checkpoint. Child-owned publications supersede origin for default execution and file reads. Healthy nested branches prove trusted target/source mappings and owners without fabricated accepted points. Stale store-only Gateway cache state cannot override durable SQL admission; actual Local executors and durable active/recovery tasks retain their guards. Cancellation returns only after owned branch copy and cleanup settle, with recovery retained.

#### Scenario: C08 original writer settlement before publication
- **WHEN** The original Agent publishes partial or final workspace files
- **THEN** Its original controller closes writer admission, stops registered tool supervisors through original private OS pipes and positively joins them, native writes and permitted MCP owners. Node independently proves only PID1 and its actual trusted helper remain before and after copy; any other process rejects. Killing unknown services cannot manufacture successful quiescence.

#### Scenario: C08 trusted collector birth isolation
- **WHEN** The original NodeDaemon starts the fixed installed collector in the same immutable container
- **THEN** The helper uses a UID different from the frozen workload and verifies original CID, StartedAt, image, User, CapDrop ALL, NoNewPrivileges, private PID namespace and the sole accepted /workspace mount. The workload cannot read or forge the private nonce or receipt. Nondumpable set inside Python main does not replace birth-time UID isolation. Existing non-root profile admission remains unchanged.

#### Scenario: C08 partial and final cleanup deadlines
- **WHEN** An early partial publication is followed by continued execution, or final cleanup begins
- **THEN** Partial stop/join, SQL and acknowledgement wait use the original execution deadline and never start final cleanup. Final stop/join and publication share the original cumulative monotonic120-second budget without per-phase reset. Lock waits beyond actual leases reject and roll back; cancellation of a waiter never substitutes physical resource settlement.
