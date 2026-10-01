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

- [x] 10.1 在 test_b10_scheduled_jobs.py 观察定时身份、slot、重放边界 RED；实际运行链路在 test_b09_fleet_job_integration.py 新增 B10 场景验证 GREEN。
- [x] 10.2 完成计划列出的接口、事务和部署接线；满足 `Scheduled job deduplication and truthful UI`。
- [x] 10.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 10.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

## 11. B11 提供可重复部署、兼容检查与迁移回退说明

- [x] 11.1 在 test_b11_worker_entry.py 观察入口、operator 与 shutdown RED；test_b11_fleet_job_integration.py / test_b11_worker_image.py 验证真实进程、镜像链路 GREEN。
- [x] 11.2 完成计划列出的接口、事务和部署接线；满足 `Reproducible deployment and safe disabling`。
- [x] 11.3 行为测试 GREEN，执行相邻回归和真实集成前提检查。
- [x] 11.4 同步实际能力文档、格式检查并提交该 slice；保存验收证据。

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


## B10 scheduled named slots and truthful task visibility — 2026-10-02

FleetConfig.scheduled_job_slots defaults empty and maps bounded operator names to
existing job profiles. The model can select an approved slot but cannot supply a
schedule identity or dedupe group. The actual scheduler forwards its context mode;
only internal launch passes trusted_schedule_id/trusted_schedule_mode to start_run.
Raw context and configurable copies lose both reserved keys, including auth-disabled
external callers. Fleet scheduled jobs require reuse_thread; fresh_thread_per_run is
rejected before the first submission, preserving canonical thread tracking.

A per-owner schedule+slot advisory transaction lock precedes job locks, including
empty groups. Original invocation retry is checked first, even after a terminal cycle
or newer occurrence. New occurrences reuse staged/queued/claimed/running/unknown/
quarantined work; pending cancellation still occupies the slot. Reuse keeps original
arguments, tracking ID/name/source run. TaskSubmission.reuse_existing selects a
read-only host lookup outside compensation; missing original tracking causes a retry
without cancelling older accepted work. The original invocation can still repair its
own uncommitted tracking row. Terminal work permits a new occurrence.

Quality review caught a lost-response replay gap in cross-occurrence reuse. Two genuine
Pg REDs showed replay binding to a new job after the original became terminal or a
newer cycle was active. f0005_job_invocations now persists immutable owner/key/thread/
source run/requested spec/group -> canonical job receipts for every admission decision,
including reuse, and backfills existing canonical jobs. All submissions serialize
owner+invocation before group/job locks; receipt lookup precedes active-group lookup.
Replay never changes its job binding; conflicting request identity is rejected. Host
service coverage preserves the original terminal tracking/name/run without compensation.

Public task statuses remain unchanged. execution_uncertain is additive and specific
to Fleet input_required + execution_unknown. The UI maps it to Needs confirmation /
需要确认, retains active polling and pending cancellation, preserves degraded tracking,
and gives terminal status precedence over stale flags. Ordinary MCP input requests
and old payloads without the flag keep their prior presentation. No machine UI added.

RED evidence: seven initial backend failures covered absent slot config/public flag
and real Pg active-group uniqueness failures; host canonical group reuse, trusted
context helper and tool schema also had behavior failures before implementation.
The mode review fix had three further REDs: missing actual scheduler mode forwarding,
fresh mode reaching submission, and raw forged mode surviving scrub. UI pure mapping
had ten RED assertions before implementation; actual card DOM tests had two behavior
failures for unknown badge/details before wiring, alongside two existing behavior passes.
The full runtime acceptance was added GREEN; fixture/collection failures are not RED.

Actual runtime acceptance extends test_b09_fleet_job_integration.py with
test_real_scheduled_slots_http_boundary_and_one_docker_execution. It exercises the
production internal launch and original run_agent; only external LLM replies are
scripted. Four forged external HTTP payloads cannot establish schedule identity/mode.
Two internal scheduled occurrences complete their Agent runs while the original job
remains unfinished. Both return original tracking/name/source run. The real TCP node
client and Docker job write the counter once; Pg records one job, attempt, accepted
manifest and tracked notification. This is local isolated PG/SQLite/NAS fixture/Docker,
not production ECS/NAS or a full Gateway process restart.

Existing B07 input test now waits at most two seconds for a claim rather than asserting
on its first SKIP LOCKED result; background reconciliation may briefly hold that row.
It still requires actual admission and verifies the frozen input snapshot. No scheduler
business behavior was changed for this test stabilization.

Frontend: 26 passed, zero skipped/todo; pnpm check (lint + tsc) passed. Dependencies
installed offline with frozen lockfile from local cache; no dependency/lock changes.
UI specification and quality reviews approved. Backend specification review approved
supported reuse_thread scope after early-mode rejection. Final backend quality review
and expanded configured regression are recorded below after completion.

Final review: backend specification and quality re-review approve the immutable receipt
fix. Focused PG B10/foundation/driver: 26 passed, including a populated f0004→f0005
migration. Real Agent/TCP/Docker B09+B10: 2 passed, two known deprecation warnings.
The final expanded configured regression: 679 passed, zero skipped, four existing
warnings. Backend Ruff check and format check pass (1370 Python files).
OpenSpec strict validation: 3/3 changes pass. git diff --check passes.

Final backend command (from backend, local isolated fixture URI supplied):

```bash
PYTHONDONTWRITEBYTECODE=1 TEST_POSTGRES_URI=<local-test-uri> FLEET_TEST_CONTAINERS=1 .venv/bin/python -m pytest tests/fleet tests/test_mcp_task_service.py tests/test_mcp_task_repository.py tests/test_mcp_task_models.py tests/test_mcp_task_ordinary_driver.py tests/test_mcp_task_tool_wrapping.py tests/test_mcp_tasks_router.py tests/test_auth_middleware.py tests/test_csrf_middleware.py tests/test_pat_auth.py tests/test_extension_config.py tests/test_extension_api_contracts.py tests/test_gateway_startup.py tests/test_gateway_lifespan_shutdown.py tests/test_authorization_tool_filter.py tests/test_extension_app_loading.py tests/test_runtime_lifecycle_e2e.py tests/test_gateway_services.py tests/test_scheduler_config.py tests/test_scheduled_task_service.py tests/test_scheduled_task_lifecycle.py tests/test_scheduled_task_queue.py tests/test_scheduled_task_claims.py tests/test_scheduled_task_dispatch_race.py tests/test_scheduled_task_postgres.py -q -p no:cacheprovider --tb=short --show-capture=no
```

Frontend commands (repo root, both PASS; 26 scoped tests):

```bash
python3 scripts/pnpm.py rstest run tests/unit/core/background-tasks tests/unit/components/workspace/thread-background-tasks.dom.test.tsx
python3 scripts/pnpm.py check
```

B10 acceptance covers supported reuse_thread scheduled Fleet slots. No production
worker/ECS/NAS deployment or remote Agent execution is claimed. All feature flags
remain disabled by default; B11 deployment and B12 release gate remain pending before C.
Implementation commit: `6236636c` — `feat(fleet): deduplicate scheduled slots with durable invocation receipts`.


## B11 public worker/operator and offline image — 2026-10-02

Public POSIX worker entry reads bounded JSON settings and an owned/private regular
credential file, validates NAS identity, separates private persistent state, and locks
one daemon incarnation before opening a session. Signals request graceful stop; shutdown
fails nonzero if a stop acknowledgement or completion is pending. Spec review caught
and fixed the stopped-zero-exit/server-running/no-manifest gap; it now uses the same
completion recovery condition as bootstrap. Existing journals remain available for replay.

The trusted operator CLI uses private database settings and existing locked Fleet
migrations on the existing host schema. Public register/issue/revoke/drain/disable/
enable/status commands replace handwritten SQL. Issue reserves an exclusive mode600
file, fsyncs bytes/directory, prints only credential ID, and revokes on failed delivery.
Register does not overwrite an existing budget; disable rejects charged capacity.
History, stopped proof and unreleased resources remain queryable after disabling.

Two actual host CLI workers run against real TCP/Gateway node routes, isolated Postgres,
NAS fixture and Docker. Old protocol and stale claims are rejected; draining node has
zero starts; the active node executes one script/output. Closing admission while it is
charged preserves accepted work, and disabling charged capacity fails. Accepted results
remain recorded after completion/disable. Real SIGTERM during a running Docker job
proves stop, durable acknowledgement and release_stopped capacity while unknown history
remains retained. Correct physical-stop release assertions are not labeled feature RED.

Genuine RED: two missing runnable-entry assertions, absent public registration/CLI,
missing enable choice, unreported shutdown and the stopped-zero-exit completion gap.
Fixture/dataclass corrections and initial incorrect quarantine-after-proved-stop test
expectations are not behavioral RED evidence. Final focused B11: 20 passed, zero skipped,
two existing websockets deprecation warnings, 10.59 seconds.

The worker-only image copies the exact Fleet source, installs hashed offline pydantic/
httpx wheels, and contains the verified real static Linux Docker CLI. No Gateway or C
entry is provided. A Dockerfile-specific whitelist excludes unrelated config/runtime data.
Image entry --help and real DockerContainers.inspect through its Linux CLI/socket are
verified; actual PortBindings are empty. Rendered Compose preserves identical absolute
host/NAS path and has zero worker ports, private state/credential binds and no auto-created
host paths. Frozen CP314/Linux aarch64 artifact reference lock and complete build commands
are in docker/fleet/README.md; other platforms need their own reviewed matching artifacts.

Actual offline build base:
python@sha256:51dafde81dbdb6ebde285137a295cf18a47ca95234fe388a343719cb97305b3d
Static CLI source: https://download.docker.com/linux/static/stable/aarch64/docker-29.2.0.tgz
CLI archive SHA256: e1590e656abaf2dfe8a1d724d99b82644446fe24844f438ea000e08006774717
Final local worker image ID: sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8
Built worker/__main__.py SHA256 equals final source:
b3a24d92198f16f436169be841b4067424b958eaf73cb3e8e1b7ebd9b38a9f06

Final expanded B01–B11/Gateway/notification/scheduler regression: 699 passed, zero
skipped, four existing warnings, 66.07 seconds. Command is the B10 expanded command
above with these additional explicit prerequisites:

```bash
FLEET_TEST_WORKER_IMAGE=sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8
FLEET_TEST_DOCKER_SOCKET=<local-host-docker-socket>
```

FLEET_TEST_CONTAINERS=1 and the isolated TEST_POSTGRES_URI remain required. B11 entry,
real worker integration and image files are included by tests/fleet. Full Ruff check and
format check pass (1375 Python files), diff whitespace passes, OpenSpec strict validates
all three changes. Spec and quality reviews approve the scoped local deployment slice.
B10 replay message follow-up 9cdd7924 also has 26 passing graph/PG tool tests; wording
preserves terminal truth instead of calling a replayed completed submission active.

Scope: two supported host CLI workers, actual image entry/Docker control, and rendered
Compose. Full containerized Compose daemon execution and production ECS/NAS are not
claimed. B12 must finish the release fault/gate acceptance before C. Feature flags remain
disabled by default; no production credentials/images are supplied or deployments made.
Implementation commit: `054d7007` — `feat(fleet): add public worker entry and reproducible deployment helpers`.


## B12 partition/runner slice — 2026-10-02

Real TCP control cut retains running Docker execution, then watchdog proves physical
stop before lease expiry. Independent file and external HTTP counters each remain one;
PG retains one attempt/zero untracked starts, unknown recovery never re-executes.
The explicit runner refuses missing prerequisites, skipped/failed/incomplete actual
JUnit and inherited test filters. Root final configured B matrix: 213 passed, zero
skipped, two existing warnings, 80.11 seconds. Genuine gate RED/GREEN and final
11 passing pure guard tests are recorded in implementation evidence. Spec and quality
reviews approve this slice. B12 remains unchecked pending foundational startup/package
acceptance, full Compose daemon and final required offline targets. C/BC stay pending.


## B01/B02 installed Gateway startup follow-up — 2026-10-02

Enabled Fleet is now preflighted before generic optional extension loading: required:true,
table_prefix:fleet_, and NAS root/identity are mandatory for the supported Gateway path.
Closing jobs admission keeps readiness and durable MCP tracking requirements in force.
Disabled Fleet remains inert; standalone operator configuration is unchanged.

Actual ExtensionManager installation runs in an isolated checkout/virtual environment.
Tests inspect installed entrypoint and five migration assets, enter the installed Gateway
lifespan twice, observe ready service and SQL driver binding, persist node state across
restart, and prove required missing-package failure. Host autogenerate uses actual host
metadata with a negative control: Fleet tables are protected by the declared prefix,
while an unrelated probe table remains visible. Each PG fixture owns its schema.

Spec and quality reviewers approved this slice. Root independently ran startup, actual
installation and Fleet tools: 18 passed, zero skipped, 28.03 seconds. Four changed Python
files pass Ruff check/format; OpenSpec strict validates all three changes; diff check
passes. Earlier implementer broader scope: 26 passed, zero skipped. Full containerized
Compose and final full B regression remain pending; this does not complete B/C/BC.
