## ADDED Requirements

### Requirement: Distinguish run completion from goal completion

The system SHALL satisfy BC09: 显示等待计算及关联 jobs/new runs；提供目标取消/继续与停止待确认，不把目标显示完成。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: BC09 observable contract
- **WHEN** 旧 run.success 但父目标 waiting_jobs，用户查看依赖或点击停止目标
- **THEN** 显示等待计算及关联 jobs/new runs；提供目标取消/继续与停止待确认，不把目标显示完成

#### Scenario: BC09 regression evidence
- **WHEN** API contract 与 DOM：run success/task waiting、input_required/recovery_required、cancel pending；mock HTTP 用户继续/取消，检查权限与 stale generation 冲突展示。
- **THEN** 实际观测满足：run_success_marks_goal_complete = false；waiting_goal_label = "等待计算"；cancel_pending_visible = true；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Unified release gate demonstrates C B C execution

The system SHALL satisfy BC10: 全过程无重复副作用、无线程双写、无容量泄漏；具备操作手册与完整测试证据。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: BC10 observable contract
- **WHEN** 实际远程 Agent 提交 B、让出、换节点继续，并注入分区/取消/重启
- **THEN** 全过程无重复副作用、无线程双写、无容量泄漏；具备操作手册与完整测试证据

#### Scenario: BC10 regression evidence
- **WHEN** 真实双 worker、PG、Redis、NAS fixture 和 scripted model；收集 run/task/job IDs、PID、manifest 和 SQL 行；断开控制面再恢复；手动解除隔离必须有 stop 证明。
- **THEN** 实际观测满足：duplicate_side_effects = 0；thread_double_writes = 0；required_cases_skipped = 0；c_b_c_across_nodes = true；不得用硬编码期望值代替真实状态或进程证据
