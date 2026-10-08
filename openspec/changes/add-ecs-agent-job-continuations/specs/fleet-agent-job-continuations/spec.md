## ADDED Requirements

### Requirement: Idempotent continuation after all results settle

The system SHALL satisfy BC03: 只创建一个新 run；同目标新 run 可换节点，旧 run 保持终态，不重复提交子 job。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: BC03 observable contract
- **WHEN** 结果先到/后到、重复通知、coordinator 在提交后回包前崩溃
- **THEN** 只创建一个新 run；同目标新 run 可换节点，旧 run 保持终态，不重复提交子 job

#### Scenario: BC03 regression evidence
- **WHEN** 先一个真实 result-before-seal 主干包含两个 coordinator 争组和重复请求；主干通过后一个必要集中边界覆盖 seal-before-result/restart-after-admission；统计新 run/placement 和 B starts，不扩展测试矩阵。
- **THEN** 实际观测满足：continuation_runs_per_group = 1；old_run_reactivated = false；child_resubmissions = 0；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Exclusive result delivery path

The system SHALL satisfy BC04: 结果只交 coordinator，detached 正常通知，不创建第二条修改线程的 run。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: BC04 observable contract
- **WHEN** awaited job 完成的同时普通 McpTaskService notification 扫描它
- **THEN** 结果只交 coordinator，detached 正常通知，不创建第二条修改线程的 run

#### Scenario: BC04 regression evidence
- **WHEN** 两个真实轮询服务竞争同一结果，记录 launched run；主干通过后，以一个集中必要边界验证 delivery receipt 提交前后的恢复、提交后回复丢失及忙线程暂缓后收敛，不展开逐边界测试矩阵。
- **THEN** 实际观测满足：duplicate_modifying_runs = 0；awaited_generic_notifications = 0；detached_delivered = true；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Fair shared scheduling and bounded child wait

The system SHALL satisfy BC05: 异步让出使 B 可运行；轮转不饿死任一类；保留资源以单节点可容纳计算。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: BC05 observable contract
- **WHEN** 所有 C 槽位等待 B，或单节点无法同时容纳标准 B/C
- **THEN** 异步让出使 B 可运行；轮转不饿死任一类；保留资源以单节点可容纳计算

#### Scenario: BC05 regression evidence
- **WHEN** 模拟加真实一槽容器池：C 提交 B 后退出释放资源，B 完成，再运行 C；持续 B/C 到达测试轮转；跨节点碎片总量不能当可用单节点保留量。
- **THEN** 实际观测满足：single_slot_sequence = ["agent", "job", "agent"]；capacity_oversold = false；category_starved = false；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Scheduled aggregate tasks preserve durable queue semantics

The system SHALL satisfy BC08: 后续 occurrence queued，不 skip、不占执行预算；不同命名 child 不被错误 dedupe。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: BC08 observable contract
- **WHEN** 同一 schedule 的 Agent task 等待子 job，下一次 occurrence 到期
- **THEN** 后续 occurrence queued，不 skip、不占执行预算；不同命名 child 不被错误 dedupe

#### Scenario: BC08 regression evidence
- **WHEN** 前 occurrence run.success 但 task.waiting；触发第二 occurrence，检查 queue age 与 budget；完成父目标后继续；提交两个不同 job slots。
- **THEN** 实际观测满足：waiting_occurrence_state = "queued"；waiting_execution_budget = 0；distinct_named_jobs = 2；不得用硬编码期望值代替真实状态或进程证据
