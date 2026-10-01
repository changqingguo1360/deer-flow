## ADDED Requirements

### Requirement: Tracked idempotent submission before execution

The system SHALL satisfy B05: 无 tracking 的 job 不可 claim；重复 key 指向同一个 job；staged 超时关闭。
实施必须遵循以下边界：调用身份组合 user_id/run_id/持久 invocation_id；不把 provider tool_call_id 当全局键。受控 job snapshot 不含密钥。

#### Scenario: B05 observable contract
- **WHEN** 远端 job 保存成功但 McpTaskService tracking 保存失败，或相同提交请求重试
- **THEN** 无 tracking 的 job 不可 claim；重复 key 指向同一个 job；staged 超时关闭

#### Scenario: B05 regression evidence
- **WHEN** 故障注入在 driver.submit 返回之后、tracking create 之前；两次提交同一 key；推进 DB 时间到 staged deadline；检查 worker 启动计数为 0。
- **THEN** 实际观测满足：jobs_for_key = 1；container_starts = 0；untracked_state = "failed"；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Authorized execution and stop on lease loss

The system SHALL satisfy B06: 相同 attempt 至多一个容器；未授权不执行；失联尝试隔离，重启先核对再 claim。
实施必须遵循以下边界：仅 node-agent 宿主访问容器引擎；容器无 socket。未 start 的失租才允许自动重新排队。worker reconcile 必须在开启 claim 循环之前完成。

#### Scenario: B06 observable contract
- **WHEN** start 回包丢失或租约失效，worker 重启发现残留容器
- **THEN** 相同 attempt 至多一个容器；未授权不执行；失联尝试隔离，重启先核对再 claim

#### Scenario: B06 regression evidence
- **WHEN** 临时 Docker 运行计数文件脚本；重复 start、丢弃回包、切断续约通道；验证实际 PID/container、启动次数、quarantine 与未释放 reservation。
- **THEN** 实际观测满足：starts_per_attempt = 1；unapproved_starts = 0；lost_attempt_reassigned = false；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Attempt isolated artifacts and accepted manifest

The system SHALL satisfy B07: 越权被拒；只有当前 attempt 的封存输出可被接受；产物 API 校验 user/thread。
实施必须遵循以下边界：校验与读取避免 TOCTOU：封存目录不可再由旧容器写入，读取不重新跟随可替换 symlink。NAS sentinel 缺失不得回退本地同名目录。

#### Scenario: B07 observable contract
- **WHEN** 脚本访问其他线程目录、提交符号链接/越界 manifest，或旧 attempt 迟到完成
- **THEN** 越权被拒；只有当前 attempt 的封存输出可被接受；产物 API 校验 user/thread

#### Scenario: B07 regression evidence
- **WHEN** 真实临时 NAS 目录与两个用户；只读输入、独立输出；构造 ../ 与 symlink 链；封存后重复 complete 并用另一用户下载。
- **THEN** 实际观测满足：cross_user_status = 404；escaped_manifest_accepted = false；accepted_manifest_count = 1；不得用硬编码期望值代替真实状态或进程证据

### Requirement: Honest cancellation and uncertain execution

The system SHALL satisfy B08: 取消请求不等于 stopped；只有 stop ack 后 cancelled；未知执行不自动复制。
实施必须遵循以下边界：在并发、重启及重复请求下保持同样语义，不依赖进程内状态保证排他。

#### Scenario: B08 observable contract
- **WHEN** 运行中取消与完成竞态，worker 失联，恢复后迟到回报
- **THEN** 取消请求不等于 stopped；只有 stop ack 后 cancelled；未知执行不自动复制

#### Scenario: B08 regression evidence
- **WHEN** 分别安排 cancel/complete 事务先后；服务端撤销 token 后回放旧 outcome；worker 持续写计数时断网，确认没有新 attempt 启动。
- **THEN** 实际观测满足：cancel_request_is_stop_proof = false；duplicate_execution_count = 0；stale_completion_accepted = false；不得用硬编码期望值代替真实状态或进程证据
