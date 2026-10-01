# B：持久 Job 与共用 Fleet 基础 tasks

前置：无；在现有个人 ECS worktree 开始。详见 [Superpowers 计划](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-b-jobs.md)。
仅在实际实施和验收后勾选；规划或 CLI 校验成功不能代替功能验收。未勾选项仍需完成，部分 slice 的证据记录在文末。

## 1. B01 建立可选包、配置和协议边界

- [ ] 1.1 写并运行 backend/tests/fleet/test_b01_fleet_foundation.py，确认 B01 行为测试 RED。
- [ ] 1.2 完成计划列出的接口、事务和部署接线；满足 `Optional installation and strict configuration`。
- [ ] 1.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 1.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 2. B02 建立独立迁移链和真实故障测试夹具

- [ ] 2.1 写并运行 backend/tests/fleet/test_b02_fleet_foundation.py，确认 B02 行为测试 RED。
- [ ] 2.2 完成计划列出的接口、事务和部署接线；满足 `Independent migration and persisted execution identity`。
- [ ] 2.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 2.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 3. B03 打通节点凭据与宿主 worker 路由

- [ ] 3.1 写并运行 backend/tests/fleet/test_b03_fleet_foundation.py，确认 B03 行为测试 RED。
- [ ] 3.2 完成计划列出的接口、事务和部署接线；满足 `Node scoped authentication without session bypass`。
- [ ] 3.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 3.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 4. B04 实现原子容量预留与节点生命周期

- [x] 4.1 写并运行 backend/tests/fleet/test_b04_fleet_foundation.py，确认 B04 行为测试 RED。
- [ ] 4.2 完成计划列出的接口、事务和部署接线；满足 `Shared atomic capacity and draining`。
- [ ] 4.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 4.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 5. B05 实现 staged 提交、去重和跟踪握手

- [ ] 5.1 写并运行 backend/tests/fleet/test_b05_fleet_durable_jobs.py，确认 B05 行为测试 RED。
- [ ] 5.2 完成计划列出的接口、事务和部署接线；满足 `Tracked idempotent submission before execution`。
- [ ] 5.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 5.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 6. B06 实现守护进程、启动授权与本地 watchdog

- [x] 6.1 写并运行 backend/tests/fleet/test_b06_fleet_durable_jobs.py，确认 B06 行为测试 RED。
- [ ] 6.2 完成计划列出的接口、事务和部署接线；满足 `Authorized execution and stop on lease loss`。
- [ ] 6.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 6.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 7. B07 实现不可变输入、产物校验和读取授权

- [x] 7.1 写并运行 backend/tests/fleet/test_b07_fleet_durable_jobs.py，确认 B07 行为测试 RED。
- [x] 7.2 完成计划列出的接口、事务和部署接线；满足 `Attempt isolated artifacts and accepted manifest`。
- [x] 7.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 7.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 8. B08 实现取消、unknown 和状态对账

- [x] 8.1 写并运行 backend/tests/fleet/test_b08_fleet_durable_jobs.py，确认 B08 行为测试 RED。
- [x] 8.2 完成计划列出的接口、事务和部署接线；满足 `Honest cancellation and uncertain execution`。
- [x] 8.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 8.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 9. B09 暴露受控工具并复用长期任务通知

- [x] 9.1 在 test_b09_fleet_tools.py 与 test_b09_fleet_public_status.py 观察新增控制边界 RED；test_b09_fleet_job_integration.py 验证完整真实链路 GREEN。
- [x] 9.2 完成计划列出的接口、事务和部署接线；满足 `Durable user task tracking and result notification`。
- [x] 9.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 9.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 10. B10 接入定时 job 去重和任务可见性

- [ ] 10.1 写并运行 backend/tests/fleet/test_b10_fleet_job_integration.py，确认 B10 行为测试 RED。
- [ ] 10.2 完成计划列出的接口、事务和部署接线；满足 `Scheduled job deduplication and truthful UI`。
- [ ] 10.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 10.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 11. B11 提供可重复部署、兼容检查与迁移回退说明

- [ ] 11.1 写并运行 backend/tests/fleet/test_b11_fleet_job_integration.py，确认 B11 行为测试 RED。
- [ ] 11.2 完成计划列出的接口、事务和部署接线；满足 `Reproducible deployment and safe disabling`。
- [ ] 11.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 11.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 12. B12 B 集成故障验收与进入 C 的门槛

- [ ] 12.1 写并运行 backend/tests/fleet/test_b12_fleet_job_integration.py，确认 B12 行为测试 RED。
- [ ] 12.2 完成计划列出的接口、事务和部署接线；满足 `B release gate verifies real side effects`。
- [ ] 12.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [ ] 12.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## Execution evidence

B is in progress; see [implementation evidence](../../../docs/superpowers/plans/2026-10-01-ecs-fleet-implementation-progress.md).
B04 RED: 8 failing tests before FleetScheduler existed; GREEN: all 8 pass on real Postgres.
Unchecked items retain incomplete wiring, release gates or commit-level acceptance requirements.


Current B05/B06 partial status: canonical retry tracking, graph invocation identity,
commit-failure compensation and worker attempt HTTP/lease components are verified.
Real Docker verifies launch once, watchdog stop and refusal to stop unmanaged containers.
319 component/adjacent tests pass with zero skips. B06 daemon/client and private
journal now exercise real TCP Gateway loss and committed-start response loss;
restart stop reconciliation gates claim. B06 startup orphan/concurrent shutdown
coverage, deployable worker/NAS integration and B release gates remain pending. See the implementation evidence linked above. Do not equate partial
component evidence with a completed requirement or mark remaining checkboxes early.

B06 6.1 RED/GREEN evidence: test_b06_fleet_durable_jobs.py now exercises the actual
HTTP/DB/container chain rather than FleetProbe. Missing-daemon RED was observed before
implementation; lost-start-response RED exposed a claim-admission gap that is fixed.
The isolated Postgres/Docker regression command in implementation evidence passes
319 tests, zero skipped. Keep 6.2–6.4 unchecked until the remaining task-level acceptance
and deployment boundaries are complete. This slice is committed as
`feat(fleet): add durable worker daemon and restart stop barrier`.


B06 residual/shutdown follow-up: startup attempts all owned stops before surfacing
missing-journal/engine errors; real Docker proves foreign-node containers remain
untouched. A real TCP/Postgres daemon loop stops both concurrent container executions
on shutdown and records physical stop before capacity release. 322 component/adjacent
tests pass, zero skipped. Private credential/operator startup and B07 NAS integration
remain pending; keep whole-task completion unchecked.


B07 filesystem foundations are in progress. test_b07_workspace.py registers 16
actual filesystem cases: explicit NAS identity sentinel, descriptor traversal,
attempt/grant scope, independent sealed copies and bounded verified reads. RED was
observed before implementation and for foreign-job claim tampering. Combined regression
passes 338 tests, zero skipped. Input registration/readonly mounts, manifest persistence,
complete HTTP and owner/thread download remain pending; B07 7.1–7.4 remain unchecked.


B07 accepted-manifest/publication progress: test_b07_fleet_durable_jobs.py RED preceded
FleetManifests; real HTTP and real Docker publication also have observed RED/GREEN.
371 component/adjacent tests pass, zero skipped (full command in implementation evidence).
Current active stopped attempt + token/session + live DB lease/deadline + independently
verified sealed files gate the atomic completion commit; duplicate completion returns
one accepted manifest. Host downloads check threads:read, thread ownership and manifest
user/thread. Worker persists a sealed manifest before complete and replays a lost
accepted completion after restart, updating the original McpTaskService row to completed.
Immutable input registration/read-only mount integration remains incomplete, so 7.2–7.4
and the B release gate remain unchecked. This slice is committed as
`feat(fleet): accept sealed manifests and publish worker results`.


B07 immutable input acceptance follow-up: input versions persist in f0003_inputs;
thread-owned session/CSRF uploads, submission/claim pinning, code artifacts, readonly
Docker mounts and accepted-output reuse are implemented. Independent review failures
for version relabelling and extra materialized files/directories now have RED/GREEN
regressions. Full Fleet/adjacent scope: 384 passed, zero skipped, four warnings;
backend Ruff lint/format1354 files and OpenSpec strict3/3 pass. B07 task acceptance
is complete for the isolated real PostgreSQL/HTTP/Docker/NAS fixture scope. Prior
paragraphs are historical slice checkpoints. Public deployment and B12 release gate
remain unaccepted, and C/continuations remain pending. This follow-up slice is
`feat(fleet): pin immutable inputs and mount verified versions read-only`.


B08 cancellation/deadline slice: stopped-before-complete cancellation and unavailable-node
queue expiry had observed RED/GREEN, as did starvation by 101 non-expired queued jobs.
Real HTTP/Docker tests hold physical stop acknowledgement before DB commit and revoke
persisted node credentials, proving cancellation remains non-terminal/capacity charged
until stop is durable, and unknown never re-executes. Combined verification: 392 passed,
zero skipped; backend Ruff lint/format1356 files and OpenSpec strict3/3 pass. Independent
review approves this slice. Keep B08 8.2–8.4 unchecked: operator recovery management is
still pending. Slice: `fix(fleet): reconcile cancellation only after durable physical stop`.


B08 operator recovery acceptance follow-up: independent f0004_recovery audit, real
PostgreSQL concurrent/idempotent resolution and audit-write rollback, admin-session
only real TCP APIs with CSRF/server-derived actor, and actual Docker unknown→operator
closure→same tracking row failed→worker admission recovery all pass. No late success
or stop message can replace the recorded uncertain outcome. Explicit concurrent
cancel/complete barriers verify both transaction orders. Final Fleet/adjacent command:
406 passed, zero skipped, four existing warnings; backend Ruff lint/format1361 files and
OpenSpec strict3/3 pass. Independent review approves code/security. B08 acceptance is
complete for these isolated local services; prior partial paragraphs are historical.
B release gate/B09–B12/C/continuations remain pending. Slice:
`feat(fleet): audit operator resolution of stopped unknown jobs`.


## B09 controlled submission slice — 2026-10-02

The core submitter bridge and `submit_fleet_job` builtin now bind through host
startup after ready Fleet service and persistent MCP task tracking validation.
No optional-package or app import enters the harness. Approved job profile names
are discoverable without exposing deployment settings. Server graph context owns
user/thread/run/invocation identity; B remains detached. Closing new-job admission
hides the tool while the Fleet driver continues polling accepted jobs.

Observed RED/GREEN covers missing tool/runtime binding, missing profile discovery
and unconditional tool visibility with subagent support enabled. Real Postgres plus
a checkpointed ToolNode graph proves a replay after post-submit response loss creates
one job/tracking row; a later turn reusing the provider call ID creates a second job.
Independent review found the unconditional subagent tool registration; it was removed
and re-reviewed after tests exercised both subagent configurations and single registration.

Focused tool/authorization checks: 17 passed. Final expanded Fleet/adjacent scope:
448 passed, zero skipped, four existing warnings, 70.76 seconds. Backend Ruff lint
and format check (1365 files) and git diff whitespace check pass. This slice does
not yet prove full run_agent submission followed by worker completion, busy-thread
notification retry, service restart and exactly one accepted notification receipt.
B09 remains partial; its full acceptance checkboxes and B release gate remain open.


### B09 uncertain-status disclosure regression — 2026-10-02

Unknown/quarantined Fleet snapshots previously included the internal job handle in
input_required, which the user task detail and notification event forwarded. Two
observed RED tests reproduce that disclosure using the actual public projections.
The adapter now returns reconciliation instructions without that handle; public
tracking ID and honest uncertain state remain available. Focused Fleet driver,
cancellation, public status and MCP task route regression: 19 passed. Scoped Ruff
lint and format checks pass after import sorting. Full B09 notification acceptance
remains pending; this fix does not alter internal ownership or operator recovery.


## B09 real submission and notification acceptance — 2026-10-02

`test_b09_fleet_job_integration.py` replaces only the external model call. A real
lead-agent HTTP run executes `submit_fleet_job` through start_run/run_agent and
finishes before the worker runs. A real TCP worker, local Docker and isolated
Postgres produce one sealed accepted manifest; its actual count file contains one
start and its report contains the expected worker output. The original tracking row
retains the authenticated source user/thread/run identity.

A real checkpoint-write admission reservation keeps completion notification pending
without counting busy admission as a failure. The task service is reconstructed;
a notification launch is committed through the real start_run path and its response
is deliberately lost. A second task-service reconstruction retries the stable key
and receives the same persisted run. Direct host SQL inspection finds one successful
notification run and one owner-scoped run.delivery receipt; Fleet SQL records one
job, attempt, manifest and delivered event version. Replaying the same launcher
again preserves those identities. Public task detail, thread snapshot and notification
event contain neither the node credential, NAS prefix nor private job handle.

This is new integration GREEN evidence on the existing protocol. Genuine RED/GREEN
was observed earlier for the missing controlled tool/bridge/host binding, profile
visibility and uncertain-status disclosure; integration test fixture corrections
are not counted as feature RED. Focused chain: 1 passed, two upstream websocket
warnings, 6.05 seconds after cleanup review. Fleet/adjacent plus real runtime lifecycle regression:
459 passed, zero skipped, four existing warnings, 47.71 seconds. Full backend Ruff
lint and format check: 1367 files clean; OpenSpec strict validation: 3/3; whitespace
check clean. Independent reviews assess the actual implementation and evidence.

Scope: Fleet and tracking use isolated Postgres; host runs/events use isolated
SQLite with the database event-store backend. These are task-service restarts while
Gateway stays running, not full-process restart or business ECS/NAS deployment.
B09 is locally accepted. B10–B12/B release gate and all C/continuation tasks remain
pending; no change is archived or marked IMPLEMENTED.

B09 final review: admission is held before the actual Docker job completes. Cleanup
now guarantees Fleet shutdown despite earlier cleanup errors and bounds TCP server
shutdown; the focused real chain passed again after that change. Spec and quality
re-reviews approve the local slice. Acceptance commit: `test(fleet): verify real job notification lifecycle`.
