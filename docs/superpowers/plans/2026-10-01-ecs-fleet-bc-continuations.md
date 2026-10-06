# B/C：等待、继续与统一产品交付 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:subagent-driven-development only if the user explicitly chooses delegation. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按先 B、再 C、再组合的顺序完成 B/C：等待、继续与统一产品交付，保持每一步可测试、可回退。

**Architecture:** 共用 Fleet 节点/attempt/资源预留，分离 Job 与 Agent run 的业务状态；复用已有长期任务和完整 run_agent 生命周期。核心提供受控身份与事务入口，扩展负责 Fleet 适配。

**Tech Stack:** Python 3.12、FastAPI、LangGraph、SQLAlchemy/Postgres、独立 Alembic、Docker、NFS/NAS、Redis、Next.js/React、pytest、Rstest。

---

**前置：** add-ecs-remote-agent 验收通过，B/C 独立执行均可用。
**工作目录：** `/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs`。
**需求来源：** [OpenSpec proposal](../../../openspec/changes/add-ecs-agent-job-continuations/proposal.md)、[tasks](../../../openspec/changes/add-ecs-agent-job-continuations/tasks.md)、[统一设计](../specs/2026-10-01-ecs-fleet-first-principles-design.md)。
**计划状态：** 尚未执行；所有测试输出均为期望，不是已经运行的结果。

共享签名与 wire 协议：[Fleet 契约](../../../openspec/ecs-fleet-contracts.md)。

## Repository decisions locked for implementation

1. 扩展根目录 `backend/packages/ecs-fleet/`，import `deerflow_ecs_fleet`。其依赖、entrypoint、版本由包负责；默认本地执行不强依赖启用 Fleet。
2. ExtensionService.start 在宿主 bootstrap 后运行独立 `fleet_alembic_version` 迁移，使用 Postgres advisory lock；私有 metadata 与声明的 `table_prefix: fleet_` 避免宿主误删表。不能将 Fleet 表加入宿主 Base。
3. worker bearer 路由由宿主 `backend/app/gateway/routers/fleet_nodes.py` 实现，扩展管理路由继续 session + admin + CSRF；不增加匿名 public 前缀。
4. Fleet runtime/protocol 不 import app。需要宿主能力时使用显式依赖注入；共享事务由宿主与扩展 repository 共同接受同一个 AsyncSession，而非各自提交。
5. 本机控制面只跑单 Gateway 时仍须测试多 SQL session 竞争。Postgres 是执行协议唯一真相源，Redis 丢失不能导致重跑。
6. `unknown`/`quarantined`/`recovery_required` 保留资源或线程恢复 reservation；只有证明旧执行停止才能放开下一写者。租约不是外部副作用 exactly-once。
7. 所有 host frontend 命令从任意目录通过 `python3 scripts/pnpm.py` 调用，它会自动定位 frontend。每个代码任务同步 README 和对应模块 AGENTS 中实际新增能力，禁改 CLAUDE.md。

## Shared transaction order

若涉及 schedule，先锁 schedule/occurrence；然后 Agent task/generation、wait group、thread/run、job/placement、node、reservation/attempt；不涉及的层跳过。任何新路径必须匹配相同顺序。多 job 按 ID 排序。C 的 run owner 与 attempt lease 在同一事务更新。数据库写 fence 必须与被保护写操作同事务，不能用先查后写替代。

## Test execution and evidence

按用户后续要求，先跑通每个 slice 的一个实际主干，再验证少量直接受影响的关键边界。这里不要求整套 backend/Fleet 重跑；已有证据按参与源码和输入匹配复用，实际失败或源码变动才触发定向重验。所有命令在目标 worktree 执行；backend 命令在 `backend/` 中，选择当前任务的明确 node ID：

```bash
.venv/bin/python -m pytest tests/fleet/<current_test_file>.py::<main_case> -q -o addopts= --tb=short
```

上述占位命令是执行模板，不是现有可执行测试名或通过证据。实现时从真实测试文件选择 node ID；先主干 RED→GREEN，再最少必要邻接检查。静态检查只覆盖改动文件；格式/文档变动不触发镜像重建。BC 开始实施仍以前置 C12 验收完成为准。

集成测试采用既有 `TEST_POSTGRES_URI` 环境变量，不在命令或文档中保存真实 URI；Redis 用 `TEST_REDIS_URL`，真实容器测试开关 `FLEET_TEST_CONTAINERS=1`，仅指向本地测试环境。对缺少环境的单测可 skip，但 release gate 必须显式预检并失败，不能把全部 skipped 当 PASS。使用随机 schema、临时容器名与独立 NAS fixture，finally 中清理；不得连接真实业务 ECS 或删除真实 NAS 数据。

前端相关任务在 repo 根运行：

```bash
python3 scripts/pnpm.py rstest run fleet
python3 scripts/pnpm.py check
```

每项任务先新增行为测试、确认 RED，再最小实现、确认 GREEN 和相邻回归；最后更新需求对应 task checkbox 并提交单项改动。下面测试代码是任务的行为判据，`FleetProbe` 只负责把真实 HTTP/DB/worker 故障场景转换为可断言观测，不得实现替代调度器或把 expected 字典直接返回。

## FleetProbe 测试夹具契约（B02 创建，各任务扩展）

`backend/tests/fleet/probe.py` 的基础记录器只分派真实场景、校验输出类型；场景实现放在各任务测试文件，不能读取 expected 值合成输出：

```python
from collections.abc import Awaitable, Callable
from typing import Any

class FleetProbe:
    def __init__(self) -> None:
        self._cases: dict[str, Callable[[], Awaitable[dict[str, Any]]]] = {}

    def register(self, name: str, run: Callable[[], Awaitable[dict[str, Any]]]) -> None:
        if name in self._cases:
            raise ValueError(f"Duplicate fixture case: {name}")
        self._cases[name] = run

    async def exercise(self, name: str) -> dict[str, Any]:
        result = await self._cases[name]()
        if not isinstance(result, dict):
            raise TypeError("Scenario must return observed evidence")
        return result
```

`conftest.py` 提供 function-scoped 随机 Postgres schema、真实 ASGI client、临时文件树、可控时钟与服务 teardown；跨进程测试使用数据库时钟及事务 barrier。场景通过 `probe.register(case_id, exercise)` 注册闭包，闭包从这些真实依赖取得证据。worker 测试区分真实 Docker integration 与无外部依赖的纯策略单测，不能用 Docker mock 通过物理停止验收。

每个任务的“场景搭建”是该闭包的操作顺序；“测试判据”是独立于被测实现的断言。B01 的包/配置用例使用独立 subprocess 测试，在 B02 后才统一接入 probe。计划中的核心实现块描述精确算法/SQL边界，不是可直接粘贴的全量生产文件；执行者需在该任务内补全与现有具体 API 的接线并通过列出的行为断言。

## 文件责任与步骤

下列 Create 为规划新增，Modify 为已有文件或前置任务已创建文件；每项 test 单独命名。核心算法块是实现约束，不能单独粘贴作为完整交付。

### Task BC01: 建立依赖与等待组持久模型

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/migrations/versions/f0004_continuations.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/job_links.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/wait_groups.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/models.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/job_service.py`
- Test: `backend/tests/fleet/test_bc01_fleet_agent_job_dependencies.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-agent-job-dependencies` / `Owned immutable child links and wait groups`。

- [ ] **Step 1 — 场景搭建与失败测试。** 两个 task/用户创建子 job；交叉 await 拒绝；重复 seal 返回原组；模拟旧 generation 提交，查看未新增记录。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc01_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC01")
    assert observed['cross_task_link_status'] == 403
    assert observed['sealed_group_changed'] == False
    assert observed['group_count_for_key'] == 1
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc01_fleet_agent_job_dependencies.py::test_bc01_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# link ownership = task_id + generation + server-bound parent run + job_id.
# wait_groups stores sealed job IDs, all-settled policy, checkpoint/manifest and delivery owner.
# f0004 includes unique continuation_key and task/generation/job link constraint.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc01_fleet_agent_job_dependencies.py::test_bc01_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc01 建立依赖与等待组持久模型"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `1.1` 至 `1.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC02: 实现 await 工具与安全让出屏障

**Files:**
- Create: `backend/packages/harness/deerflow/tools/builtins/fleet_await.py`
- Create: `backend/packages/harness/deerflow/runtime/execution/yield_control.py`
- Modify: `backend/packages/harness/deerflow/runtime/runs/worker.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/agent_runner.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/recovery_points.py`
- Test: `backend/tests/fleet/test_bc02_fleet_agent_job_dependencies.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-agent-job-dependencies` / `Durable cooperative yield releases execution resources`。

- [ ] **Step 1 — 场景搭建与失败测试。** 模型提交两个 awaited job 后调用 await；检查 tool call/message 成对、最终 checkpoint、workspace、run.success/task.waiting_jobs；阻止 stopped ack 时不允许 continuation。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc02_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC02")
    assert observed['unpaired_tool_calls'] == 0
    assert observed['old_run_status'] == 'success'
    assert observed['task_state'] == 'waiting_jobs'
    assert observed['resume_before_stop_ack'] == False
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc02_fleet_agent_job_dependencies.py::test_bc02_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Persist preparing group; return bounded tool message; stop graph at safe boundary.
# Seal workspace + checkpoint; atomically finalize run/placement and waiting task.
# Admission readiness = group.sealed AND old_run_terminal AND stopped_ack AND reservation_released.
```

不能抛普通异常假装 success；不能把 LangGraph interrupt 未完成 tool call 直接标 success。所有剩余 awaited jobs 自动组成等待组，不允许父 task 提前 succeeded。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc02_fleet_agent_job_dependencies.py::test_bc02_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc02 实现 await 工具与安全让出屏障"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `2.1` 至 `2.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC03: 实现 exactly-one continuation 准入

**Files:**
- Create: `backend/app/fleet/continuations.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/continuation_payload.py`
- Modify: `backend/app/fleet/execution.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/agent_tasks.py`
- Test: `backend/tests/fleet/test_bc03_fleet_agent_job_continuations.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-agent-job-continuations` / `Idempotent continuation after all results settle`。

- [ ] **Step 1 — 场景搭建与失败测试。** 对 result-before-seal、seal-before-result、restart-after-admission 运行矩阵；两个 coordinator 同时争组；统计新 run/placement 和 B starts。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc03_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC03")
    assert observed['continuation_runs_per_group'] == 1
    assert observed['old_run_reactivated'] == False
    assert observed['child_resubmissions'] == 0
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc03_fleet_agent_job_continuations.py::test_bc03_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Lock task -> group -> thread/run; compare generation, state, deadline, budgets.
# Require all child results terminal and stopped/manifest proof; use stable continuation_key.
# One admission transaction inserts new run/placement, moves current_run and records receipt.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc03_fleet_agent_job_continuations.py::test_bc03_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc03 实现 exactly-one continuation 准入"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `3.1` 至 `3.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC04: 统一结果 delivery owner 与通知互斥

**Files:**
- Create: `backend/app/fleet/delivery.py`
- Modify: `backend/app/mcp_tasks/service.py`
- Modify: `backend/app/fleet/job_tracking.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/job_links.py`
- Test: `backend/tests/fleet/test_bc04_fleet_agent_job_continuations.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-agent-job-continuations` / `Exclusive result delivery path`。

- [ ] **Step 1 — 场景搭建与失败测试。** 两个真实轮询服务竞争同一结果，记录 launched run；在 delivery receipt 提交各边界重启；忙线程暂缓后收敛。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc04_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC04")
    assert observed['duplicate_modifying_runs'] == 0
    assert observed['awaited_generic_notifications'] == 0
    assert observed['detached_delivered'] == True
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc04_fleet_agent_job_continuations.py::test_bc04_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Persist delivery owner before task becomes claimable: generic_notification or wait_group.
# Generic poll may update projection but must not enqueue notification for coordinator-owned result.
# Receipts are durable; ownership cannot be changed by raw model arguments.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc04_fleet_agent_job_continuations.py::test_bc04_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc04 统一结果 delivery owner 与通知互斥"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `4.1` 至 `4.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC05: 共用公平调度与最小池无死锁

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/admission_policy.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/scheduler.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/config.py`
- Test: `backend/tests/fleet/test_bc05_fleet_agent_job_continuations.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-agent-job-continuations` / `Fair shared scheduling and bounded child wait`。

- [ ] **Step 1 — 场景搭建与失败测试。** 模拟加真实一槽容器池：C 提交 B 后退出释放资源，B 完成，再运行 C；持续 B/C 到达测试轮转；跨节点碎片总量不能当可用单节点保留量。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc05_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC05")
    assert observed['single_slot_sequence'] == ['agent', 'job', 'agent']
    assert observed['capacity_oversold'] == False
    assert observed['category_starved'] == False
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc05_fleet_agent_job_continuations.py::test_bc05_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Multi-capacity: C admission preserves one fitting B profile on a real node.
# Serial mode: one B or C at a time; C has no synchronous long-job wait API.
# Round-robin categories + FIFO within category; all profiles/deadlines finite.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc05_fleet_agent_job_continuations.py::test_bc05_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc05 共用公平调度与最小池无死锁"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `5.1` 至 `5.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC06: 取消、用户输入和 generation 竞态

**Files:**
- Create: `backend/app/fleet/task_admission.py`
- Modify: `backend/app/gateway/services.py`
- Modify: `backend/app/gateway/routers/thread_runs.py`
- Modify: `backend/app/fleet/agent_control.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/cancellation.py`
- Test: `backend/tests/fleet/test_bc06_fleet_agent_job_dependencies.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-agent-job-dependencies` / `Generation fences continuation and user edits`。

- [ ] **Step 1 — 场景搭建与失败测试。** 用事务 barrier 枚举 cancel-before-admit/admit-before-cancel；waiting 用户消息先提交；后台通知在 waiting 时不抢线程；显式继续允许收编旧结果但不重跑 job。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc06_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC06")
    assert observed['old_generation_continues'] == False
    assert observed['detached_cancelled_by_parent'] == False
    assert observed['thread_double_writes'] == 0
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc06_fleet_agent_job_dependencies.py::test_bc06_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Lock task/thread in same order for message, cancel, rollback, delete and continuation.
# New user mutation pauses task and increments generation before user run admission.
# Cancel task prevents new continuation; request stop awaited children, leave detached alone.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc06_fleet_agent_job_dependencies.py::test_bc06_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc06 取消、用户输入和 generation 竞态"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `6.1` 至 `6.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC07: 跨 run 预算、deadline 与恢复裁决

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/task_budgets.py`
- Create: `backend/app/fleet/task_recovery.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/agent_tasks.py`
- Modify: `backend/app/fleet/continuations.py`
- Test: `backend/tests/fleet/test_bc07_fleet_agent_job_dependencies.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-agent-job-dependencies` / `Aggregate budgets and nonautomatic crash recovery`。

- [ ] **Step 1 — 场景搭建与失败测试。** 连续三组运行耗尽 token/run/job 预算；unknown job 到 task deadline；提交 child 后 kill C，再完成 child，确认未自动 continuation。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc07_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC07")
    assert observed['budget_reset_on_new_run'] == False
    assert observed['unknown_silent_forever'] == False
    assert observed['resume_after_parent_crash'] == False
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc07_fleet_agent_job_dependencies.py::test_bc07_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Atomic counters on agent_task: admitted_runs, reserved_tokens, spent_tokens, jobs.
# Reserve each run/job budget before admission; reconcile measured usage after fenced finish.
# Deadline/quarantine => paused/recovery_required with visible reason, no new run.
```

模型调用前强制累计 token 上限，不能仅结束后统计；接受的未用预留可释放，已花费额度不可回滚。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc07_fleet_agent_job_dependencies.py::test_bc07_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc07 跨 run 预算、deadline 与恢复裁决"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `7.1` 至 `7.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC08: Scheduler 目标阻塞与命名子任务

**Files:**
- Create: `backend/app/fleet/scheduled_agent_tasks.py`
- Modify: `backend/app/scheduler/service.py`
- Modify: `backend/app/fleet/scheduler_tickets.py`
- Modify: `backend/app/fleet/scheduled_jobs.py`
- Test: `backend/tests/fleet/test_bc08_fleet_agent_job_continuations.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-agent-job-continuations` / `Scheduled aggregate tasks preserve durable queue semantics`。

- [ ] **Step 1 — 场景搭建与失败测试。** 前 occurrence run.success 但 task.waiting；触发第二 occurrence，检查 queue age 与 budget；完成父目标后继续；提交两个不同 job slots。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc08_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC08")
    assert observed['waiting_occurrence_state'] == 'queued'
    assert observed['waiting_execution_budget'] == 0
    assert observed['distinct_named_jobs'] == 2
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc08_fleet_agent_job_continuations.py::test_bc08_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# scheduler blocker = active occurrence OR unresolved associated agent_task.
# Never keep old occurrence.running just to wait across continuation runs.
# Coalesce on existing queued occurrence; timeout uses original scheduled queue deadline.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc08_fleet_agent_job_continuations.py::test_bc08_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc08 Scheduler 目标阻塞与命名子任务"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `8.1` 至 `8.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC09: 交付统一任务摘要与操作入口

**Files:**
- Create: `frontend/src/core/fleet/hooks.ts`
- Create: `frontend/src/core/fleet/presentation.ts`
- Create: `frontend/tests/unit/core/fleet/presentation.dom.test.tsx`
- Create: `backend/app/gateway/routers/fleet_agent_tasks.py`
- Modify: `frontend/src/components/workspace/fleet-task-summary.tsx`
- Modify: `frontend/src/core/fleet/types.ts`
- Modify: `frontend/src/core/i18n/locales/en-US.ts`
- Modify: `frontend/src/core/i18n/locales/zh-CN.ts`
- Modify: `backend/app/gateway/app.py`
- Test: `backend/tests/fleet/test_bc09_fleet_unified_task_experience.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-unified-task-experience` / `Distinguish run completion from goal completion`。

- [ ] **Step 1 — 场景搭建与失败测试。** API contract 与 DOM：run success/task waiting、input_required/recovery_required、cancel pending；mock HTTP 用户继续/取消，检查权限与 stale generation 冲突展示。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc09_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC09")
    assert observed['run_success_marks_goal_complete'] == False
    assert observed['waiting_goal_label'] == '等待计算'
    assert observed['cancel_pending_visible'] == True
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc09_fleet_unified_task_experience.py::test_bc09_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# GET task returns goal state + current_run + job links + accepted results + generation.
# POST cancel/resume uses expected_generation; stale edit returns 409 then client refetches.
# IM result text uses same bounded task projection, not raw worker log as instructions.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc09_fleet_unified_task_experience.py::test_bc09_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc09 交付统一任务摘要与操作入口"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `9.1` 至 `9.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task BC10: 组合端到端和运维交付验收

**Files:**
- Create: `backend/tests/fleet/test_bc_acceptance.py`
- Create: `frontend/tests/e2e/fleet-continuation.spec.ts`
- Modify: `README.md`
- Modify: `backend/AGENTS.md`
- Modify: `frontend/AGENTS.md`
- Modify: `docs/deployment/ecs-fleet.md`
- Test: `backend/tests/fleet/test_bc10_fleet_unified_task_experience.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-unified-task-experience` / `Unified release gate demonstrates C B C execution`。

- [ ] **Step 1 — 场景搭建与失败测试。** 真实双 worker、PG、Redis、NAS fixture 和 scripted model；收集 run/task/job IDs、PID、manifest 和 SQL 行；断开控制面再恢复；手动解除隔离必须有 stop 证明。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc10_contract(fleet_probe):
    observed = await fleet_probe.exercise("BC10")
    assert observed['duplicate_side_effects'] == 0
    assert observed['thread_double_writes'] == 0
    assert observed['required_cases_skipped'] == 0
    assert observed['c_b_c_across_nodes'] == True
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_bc10_fleet_unified_task_experience.py::test_bc10_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Final gate = B gate + C gate + C->B->new C on another worker + user/cancel races.
# Rollout flags in order: jobs_enabled -> agents_enabled -> continuations_enabled.
# Revert flags only after drain; preserve histories, manifests and unresolved reservations.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 只重跑本任务的实际主干 node ID，确认副作用和数据库结果符合断言；主干通过后，按源码变动选择最少必要的关键邻接验证。复用仍匹配的历史证据，不从 expected 值构造结果。

```bash
.venv/bin/python -m pytest tests/fleet/test_bc10_fleet_unified_task_experience.py::test_bc10_contract -q -o addopts= --tb=short
```

期望：当前主干与明确选定的必要邻接验证 PASS；集成环境缺失必须记录，release gate 不得通过。以上计划 node ID 须在实际测试创建后确认，不能作为已执行证据。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 仅对改动的 Python 文件运行 Ruff format/check；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): bc10 组合端到端和运维交付验收"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `10.1` 至 `10.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

## 阶段结束检查

- [ ] 所有本阶段 OpenSpec SHALL 均有测试证据；上一阶段回归继续通过。
- [ ] PostgreSQL 并发与实际容器/NAS 故障测试非跳过通过；C/组合还需要 Redis 与完整 runner。
- [ ] 实际组合主干通过，少量必要故障邻接与改动文件静态检查通过；前端变更完成 check。无需机械重跑完整 backend/Fleet 套件。
- [ ] 禁用新 admission 后已有执行仍能对账；无孤儿容器或悄悄释放的 quarantine。
- [ ] 在测试报告中明确环境限制；未通过门槛不得执行后继阶段的上线操作。

本计划按 inline executing-plans 交接，不自动发起子代理或开始实施。用户要求开始后，先执行 B01。
