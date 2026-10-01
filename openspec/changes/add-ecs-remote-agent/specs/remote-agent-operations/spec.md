## ADDED Requirements

### Requirement: Remote cancellation and safe recovery

The system SHALL satisfy C09: 任务暂停/输入等待状态正确；停止未确认继续占资源；故障重试需先确认旧执行停止。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: C09 observable contract
- **WHEN** 取消当前 C run、graph 人工中断、started worker 分区
- **THEN** 任务暂停/输入等待状态正确；停止未确认继续占资源；故障重试需先确认旧执行停止

#### Scenario: C09 regression evidence
- **WHEN** cancel 与 completion 两种提交顺序；graph interrupt/resume 经新 run；旧 worker 网络隔离保持 shell 写入，检查线程恢复 reservation 阻止第二写者。
- **THEN** 实际观测满足：unconfirmed_resources_released = false；resume_new_run = true；concurrent_thread_writers = 1；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Remote task visibility and reversible enablement

The system SHALL satisfy C11: 状态可理解且不泄露凭据；关闭仅禁止新 C，B/local 正常，已有 C 可对账。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: C11 observable contract
- **WHEN** 用户查看 remote run 的位置/停止状态，或 agents flag 关闭
- **THEN** 状态可理解且不泄露凭据；关闭仅禁止新 C，B/local 正常，已有 C 可对账

#### Scenario: C11 regression evidence
- **WHEN** UI 对 pending/running/recovery_required 与 cancel-pending 纯映射测试；Local 全链路与 B 套件回归；disable C 后继续查询活跃 C。
- **THEN** 实际观测满足：b_local_regressions = 0；public_secret_count = 0；disabled_new_remote_status = 503；不得用硬编码期望值代替真实状态或进程证据

### Requirement: C release gate covers all remote mutation paths

The system SHALL satisfy C12: checkpoint/memory/outbox/终态全部 fence；取消/产物/local parity 通过后才进入组合阶段。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: C12 observable contract
- **WHEN** 执行真实 runner、PG、Redis、容器及断网测试矩阵
- **THEN** checkpoint/memory/outbox/终态全部 fence；取消/产物/local parity 通过后才进入组合阶段

#### Scenario: C12 regression evidence
- **WHEN** 复用 B 故障代理并增加 Redis outage；旧进程晚到写、再启动 attempt、子进程残留核对；计入所有 SQL 表和真实进程证据。
- **THEN** 实际观测满足：required_cases_skipped = 0；stale_mutation_count = 0；local_parity = true；不得用硬编码期望值代替真实状态或进程证据
