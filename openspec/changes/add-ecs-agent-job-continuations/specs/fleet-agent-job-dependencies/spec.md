## ADDED Requirements

### Requirement: Owned immutable child links and wait groups

The system SHALL satisfy BC01: 只接受当前授权归属；sealed group 不能增删；每组有稳定 continuation key。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: BC01 observable contract
- **WHEN** C 提交 awaited/detached，试图 await 其他用户/目标/generation 的 job
- **THEN** 只接受当前授权归属；sealed group 不能增删；每组有稳定 continuation key

#### Scenario: BC01 regression evidence
- **WHEN** 两个真实 task/用户经原身份创建子 job；私有 capability-bound seal 试图加入其他归属的 job；重复/并发 seal；旧 generation 提交及写入后失权。
- **THEN** 实际观测满足：cross_task_rejection_type = "PermissionError"；cross_task_group_count = 0；sealed_group_changed = false；group_count_for_key = 1；旧 generation/写入后失权留下的持久行计数不变。此私有入口不声明 HTTP 状态码；不得用硬编码期望值代替真实异常、SQL 行或进程证据

### Requirement: Durable cooperative yield releases execution resources

The system SHALL satisfy BC02: 在完整 ToolMessage/checkpoint/文件封存后结束 run；确认进程退出才释放资源并等待。
实施必须遵循以下边界：不能抛普通异常假装 success；不能把 LangGraph interrupt 未完成 tool call 直接标 success。所有剩余 awaited jobs 自动组成等待组，不允许父 task 提前 succeeded。

#### Scenario: BC02 observable contract
- **WHEN** C 调用 await 或正常结束但仍有未消费 awaited jobs
- **THEN** 在完整 ToolMessage/checkpoint/文件封存后结束 run；确认进程退出才释放资源并等待

#### Scenario: BC02 regression evidence
- **WHEN** 模型提交两个 awaited job 后调用 await；检查 tool call/message 成对、最终 checkpoint、workspace、run.success/task.waiting_jobs；阻止 stopped ack 时不允许 continuation。
- **THEN** 实际观测满足：unpaired_tool_calls = 0；old_run_status = "success"；task_state = "waiting_jobs"；resume_before_stop_ack = false；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Generation fences continuation and user edits

The system SHALL satisfy BC06: 旧 generation 不续跑；awaited 取消传播，detached 保留；新用户 run 不与旧目标双写。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: BC06 observable contract
- **WHEN** 用户在 waiting 时发新消息/回滚/删除，或 cancel task 与结果同时提交
- **THEN** 旧 generation 不续跑；awaited 取消传播，detached 保留；新用户 run 不与旧目标双写

#### Scenario: BC06 regression evidence
- **WHEN** 用事务 barrier 枚举 cancel-before-admit/admit-before-cancel；waiting 用户消息先提交；后台通知在 waiting 时不抢线程；显式继续允许收编旧结果但不重跑 job。
- **THEN** 实际观测满足：old_generation_continues = false；detached_cancelled_by_parent = false；thread_double_writes = 0；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Aggregate budgets and nonautomatic crash recovery

The system SHALL satisfy BC07: 预算不随新 run 重置；到限显式人工处理；没有一致恢复点不得自动继续。
实施必须遵循以下边界：模型调用前强制累计 token 上限，不能仅结束后统计；接受的未用预留可释放，已花费额度不可回滚。

#### Scenario: BC07 observable contract
- **WHEN** 多次 C→B→C、unknown 子任务、或 C 在提交 B 后让出前崩溃
- **THEN** 预算不随新 run 重置；到限显式人工处理；没有一致恢复点不得自动继续

#### Scenario: BC07 regression evidence
- **WHEN** 连续三组运行耗尽 token/run/job 预算；unknown job 到 task deadline；提交 child 后 kill C，再完成 child，确认未自动 continuation。
- **THEN** 实际观测满足：budget_reset_on_new_run = false；unknown_silent_forever = false；resume_after_parent_crash = false；不得用硬编码期望值代替真实状态或进程证据
