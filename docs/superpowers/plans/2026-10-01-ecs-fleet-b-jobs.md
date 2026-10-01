# B：持久 Job 与共用 Fleet 基础 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:subagent-driven-development only if the user explicitly chooses delegation. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 按先 B、再 C、再组合的顺序完成 B：持久 Job 与共用 Fleet 基础，保持每一步可测试、可回退。

**Architecture:** 共用 Fleet 节点/attempt/资源预留，分离 Job 与 Agent run 的业务状态；复用已有长期任务和完整 run_agent 生命周期。核心提供受控身份与事务入口，扩展负责 Fleet 适配。

**Tech Stack:** Python 3.12、FastAPI、LangGraph、SQLAlchemy/Postgres、独立 Alembic、Docker、NFS/NAS、Redis、Next.js/React、pytest、Rstest。

---

**前置：** 无；在现有个人 ECS worktree 开始。
**工作目录：** `/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs`。
**需求来源：** [OpenSpec proposal](../../../openspec/changes/add-ecs-fleet-jobs/proposal.md)、[tasks](../../../openspec/changes/add-ecs-fleet-jobs/tasks.md)、[统一设计](../specs/2026-10-01-ecs-fleet-first-principles-design.md)。
**计划状态：** B 实施中；下面代码与测试输出是实施指南，实际证据见 [实施进度](2026-10-01-ecs-fleet-implementation-progress.md)。

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

所有命令在目标 worktree 执行；backend 命令在 `backend/` 中：

```bash
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not integration and not live'
PYTHONPATH=. uv run pytest tests/fleet -q -m integration
make format
make lint
make test
make test-blocking-io
```

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

### Task B01: 建立可选包、配置和协议边界

**Files:**
- Create: `backend/packages/ecs-fleet/pyproject.toml`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/__init__.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/config.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/protocol.py`
- Modify: `backend/pyproject.toml`
- Modify: `backend/uv.lock`
- Modify: `config.example.yaml`
- Test: `backend/tests/fleet/test_b01_fleet_foundation.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-foundation` / `Optional installation and strict configuration`。

- [x] **Step 1 — 场景搭建与失败测试。** 构建两个 subprocess：未安装扩展/disabled 与 enabled；enabled 用 sqlite、负数预算、未知协议字段逐项触发校验；确认普通启动不创建 fleet 表。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest
from pydantic import ValidationError
from deerflow_ecs_fleet.config import FleetConfig

def test_b01_contract():
    config = FleetConfig()
    assert not config.enabled
    assert not config.jobs_enabled
    with pytest.raises(ValidationError):
        FleetConfig(lease_seconds=60, renew_seconds=30)
    with pytest.raises(ValidationError):
        FleetConfig(agents_enabled=True, jobs_enabled=False)
    with pytest.raises(ValidationError):
        FleetConfig(continuations_enabled=True, agents_enabled=False)
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b01_fleet_foundation.py::test_b01_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。B01 首次可用独立包导入/配置测试确认缺失模块；B02 后接入统一 probe。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
from pydantic import BaseModel, Field, model_validator

class FleetConfig(BaseModel):
    enabled: bool = False
    jobs_enabled: bool = False
    agents_enabled: bool = False
    continuations_enabled: bool = False
    lease_seconds: int = Field(default=120, ge=30)
    renew_seconds: int = Field(default=30, ge=1)
    queue_timeout_seconds: int = Field(default=1800, ge=1)

    @model_validator(mode="after")
    def validate_dependencies(self):
        if self.renew_seconds * 2 >= self.lease_seconds:
            raise ValueError("renew interval must be less than half the lease")
        if self.continuations_enabled and not self.agents_enabled:
            raise ValueError("continuations require agents")
        if self.agents_enabled and not self.jobs_enabled:
            raise ValueError("agents require jobs")
        return self

# Extension startup separately checks the host DB backend is Postgres before readiness.
```

包入口为 deerflow_ecs_fleet:install，table_prefix=fleet_；单独的 extensions dependency group。测试通过 uv --with 本地包加载，默认安装仍可禁用；不得让 harness import app 或可选扩展。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b01_fleet_foundation.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b01 建立可选包、配置和协议边界"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `1.1` 至 `1.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B02: 建立独立迁移链和真实故障测试夹具

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/base.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/models.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/migrations/env.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/migrations/versions/f0001_jobs.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/service.py`
- Create: `backend/tests/fleet/__init__.py`
- Create: `backend/tests/fleet/conftest.py`
- Create: `backend/tests/fleet/probe.py`
- Test: `backend/tests/fleet/test_b02_fleet_foundation.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-foundation` / `Independent migration and persisted execution identity`。

- [x] **Step 1 — 场景搭建与失败测试。** 采用 test_scheduled_task_postgres.py 的随机 schema/清理方式；并发启动两个 ExtensionService；断言 fleet_alembic_version 唯一和 private metadata，重启后提交记录仍可读。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b02_contract(fleet_probe):
    observed = await fleet_probe.exercise("B02")
    assert observed['migration_heads'] == 1
    assert observed['host_owns_fleet_tables'] == False
    assert observed['restart_preserves_rows'] == True
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b02_fleet_foundation.py::test_b02_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
fleet_metadata = MetaData()
# extension migration transaction; use a fixed documented advisory key:
# SELECT pg_advisory_xact_lock(73462101)
# Alembic version_table="fleet_alembic_version", target_metadata=fleet_metadata
# f0001: fleet_nodes, fleet_jobs, fleet_attempts, fleet_reservations,
# fleet_credentials, fleet_artifact_manifests. No create_all on host Base.
```

Migration 在 ExtensionService.start、host bootstrap 之后、worker API ready 之前运行；用 asyncio.to_thread 或 async connection.run_sync 避免阻塞。所有 fleet 主键、UTC deadline、attempt/job 唯一索引和可空 run_id 的 XOR 约束在此创建，agent kind 在 B 阶段拒绝 claim。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b02_fleet_foundation.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b02 建立独立迁移链和真实故障测试夹具"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `2.1` 至 `2.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B03: 打通节点凭据与宿主 worker 路由

**Files:**
- Create: `backend/app/gateway/fleet_auth.py`
- Create: `backend/app/gateway/routers/fleet_nodes.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/node_credentials.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/management.py`
- Modify: `backend/app/gateway/auth_middleware.py`
- Modify: `backend/app/gateway/csrf_middleware.py`
- Modify: `backend/app/gateway/app.py`
- Test: `backend/tests/fleet/test_b03_fleet_foundation.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-foundation` / `Node scoped authentication without session bypass`。

- [x] **Step 1 — 场景搭建与失败测试。** 使用 ASGITransport 发送有效/无效 bearer、cookie、混合凭据以及 root_path 变体；撤销后再次 renew；检查响应和 DB 未发生越权修改。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b03_contract(fleet_probe):
    observed = await fleet_probe.exercise("B03")
    assert observed['cross_node_status'] == 403
    assert observed['revoked_status'] == 401
    assert observed['management_requires_session'] == True
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b03_fleet_foundation.py::test_b03_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Host owns /api/fleet/node/*; extension admin routes stay session authenticated.
# Resolve normalized request route before credential handling.
# node principal = (node_id, credential_id); never a user/admin principal.
# Persist hash(token), expiry, revoked_at; compare token in constant time.
# Session-only or ambiguous mixed credentials cannot authorize worker writes.
```

不能将扩展 router 加到 public/CSRF 全局白名单。worker 认证成功后只对该精确宿主路由使用 bearer 语义，禁止 auth-disabled 模式让 worker 端点无凭据执行。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b03_fleet_foundation.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b03 打通节点凭据与宿主 worker 路由"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `3.1` 至 `3.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B04: 实现原子容量预留与节点生命周期

**Files:**
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/nodes.py`（复用 B03 的节点生命周期边界）
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/reservations.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/scheduler.py`
- Test: `backend/tests/fleet/test_b04_fleet_foundation.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-foundation` / `Shared atomic capacity and draining`。

- [x] **Step 1 — 场景搭建与失败测试。** 两个独立 Postgres session 用 asyncio.gather 同时 claim；capacity 只够一项。隔离 winner 后再次 claim；drain、心跳恢复、删除依次核验。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b04_contract(fleet_probe):
    observed = await fleet_probe.exercise("B04")
    assert observed['claims_won'] == 1
    assert observed['claim_while_quarantined'] == None
    assert observed['draining_after_heartbeat'] == True
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b04_fleet_foundation.py::test_b04_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# One DB transaction, fixed lock order: execution row -> node row -> reservation.
# SELECT node FOR UPDATE; sum unreleased reservations(cpu,memory,agent_units).
# Reject if any resource exceeds allocatable; insert reservation AND attempt.
# Health updates never change admin_state. Release requires stopped_at proof.
```

以 CPU 毫核、内存 MiB 整数计账；B/C 保留统一 kind/agent_units 字段。全局公平调度在 BC05 增强，B 初版 FIFO。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b04_fleet_foundation.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b04 实现原子容量预留与节点生命周期"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `4.1` 至 `4.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B05: 实现 staged 提交、去重和跟踪握手

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/jobs.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/job_service.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/mcp_driver.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/reconcile.py`
- Create: `backend/app/fleet/job_tracking.py`
- Modify: `backend/app/gateway/app.py`
- Modify: `backend/app/mcp_tasks/service.py`
- Modify: `backend/packages/harness/deerflow/mcp/tasks/runtime.py`
- Test: `backend/tests/fleet/test_b05_fleet_durable_jobs.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-durable-jobs` / `Tracked idempotent submission before execution`。

- [x] **Step 1 — 场景搭建与失败测试。** 故障注入在 driver.submit 返回之后、tracking create 之前；两次提交同一 key；推进 DB 时间到 staged deadline；检查 worker 启动计数为 0。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b05_contract(fleet_probe):
    observed = await fleet_probe.exercise("B05")
    assert observed['jobs_for_key'] == 1
    assert observed['container_starts'] == 0
    assert observed['untracked_state'] == 'failed'
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b05_fleet_durable_jobs.py::test_b05_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# driver.submit: INSERT job(state='staged') ON CONFLICT(user_id,key) RETURN existing.
# reconciler: lock job; join tracking by task_id, user_id, thread_id, remote_task_id.
# tracking committed AND not cancelled => queued; staged deadline elapsed => failed.
# NEVER claim staged, even if compensating cancel failed.
```

调用身份组合 user_id/run_id/持久 invocation_id；不把 provider tool_call_id 当全局键。受控 job snapshot 不含密钥。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b05_fleet_durable_jobs.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b05 实现 staged 提交、去重和跟踪握手"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `5.1` 至 `5.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B06: 实现守护进程、启动授权与本地 watchdog

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/__init__.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/daemon.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/client.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/watchdog.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/containers.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/attempts.py`
- Modify: `backend/app/gateway/routers/fleet_nodes.py`
- Test: `backend/tests/fleet/test_b06_fleet_durable_jobs.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-durable-jobs` / `Authorized execution and stop on lease loss`。

- [x] **Step 1 — 场景搭建与失败测试。** 临时 Docker 运行计数文件脚本；重复 start、丢弃回包、切断续约通道；验证实际 PID/container、启动次数、quarantine 与未释放 reservation。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b06_contract(fleet_probe):
    observed = await fleet_probe.exercise("B06")
    assert observed['starts_per_attempt'] == 1
    assert observed['unapproved_starts'] == 0
    assert observed['lost_attempt_reassigned'] == False
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b06_fleet_durable_jobs.py::test_b06_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# granted start is durable before launch; container name = "fleet-" + attempt_id.
# launch uses argv (shell=False), configured image digest, non-root and resource caps.
# watchdog uses monotonic lease deadline; stop process group AND tool container.
# until stopped is proved: quarantine reservation, do not advertise it as idle.
```

仅 node-agent 宿主访问容器引擎；容器无 socket。未 start 的失租才允许自动重新排队。worker reconcile 必须在开启 claim 循环之前完成。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b06_fleet_durable_jobs.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b06 实现守护进程、启动授权与本地 watchdog"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `6.1` 至 `6.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B07: 实现不可变输入、产物校验和读取授权

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/artifacts.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/workspace.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/manifests.py`
- Create: `backend/app/gateway/routers/fleet_artifacts.py`
- Modify: `backend/app/gateway/app.py`
- Test: `backend/tests/fleet/test_b07_fleet_durable_jobs.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-durable-jobs` / `Attempt isolated artifacts and accepted manifest`。

- [x] **Step 1 — 场景搭建与失败测试。** 真实临时 NAS 目录与两个用户；只读输入、独立输出；构造 ../ 与 symlink 链；封存后重复 complete 并用另一用户下载。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b07_contract(fleet_probe):
    observed = await fleet_probe.exercise("B07")
    assert observed['cross_user_status'] == 404
    assert observed['escaped_manifest_accepted'] == False
    assert observed['accepted_manifest_count'] == 1
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b07_fleet_durable_jobs.py::test_b07_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# input manifest pins immutable versions; mount only allowlisted paths read-only.
# Seal outputs after all writers exit; validate using dirfd/no-follow traversal.
# In transaction: check token + lease + state; insert accepted manifest; finish job.
# Repeated completion for already accepted attempt returns existing manifest.
```

校验与读取避免 TOCTOU：封存目录不可再由旧容器写入，读取不重新跟随可替换 symlink。NAS sentinel 缺失不得回退本地同名目录。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b07_fleet_durable_jobs.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b07 实现不可变输入、产物校验和读取授权"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `7.1` 至 `7.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B08: 实现取消、unknown 和状态对账

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/cancellation.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/recovery.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/reconcile.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/job_service.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/attempts.py`
- Test: `backend/tests/fleet/test_b08_fleet_durable_jobs.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-durable-jobs` / `Honest cancellation and uncertain execution`。

- [x] **Step 1 — 场景搭建与失败测试。** 分别安排 cancel/complete 事务先后；服务端撤销 token 后回放旧 outcome；worker 持续写计数时断网，确认没有新 attempt 启动。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b08_contract(fleet_probe):
    observed = await fleet_probe.exercise("B08")
    assert observed['cancel_request_is_stop_proof'] == False
    assert observed['duplicate_execution_count'] == 0
    assert observed['stale_completion_accepted'] == False
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b08_fleet_durable_jobs.py::test_b08_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# cancel intent fences new start immediately.
# queued + never started => cancelled; running => cancel_requested until stop ack.
# started + unreachable => unknown/quarantined, retain reservation and dedupe group.
# DB-terminal result can win before cancel; report that outcome without rewriting it.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b08_fleet_durable_jobs.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b08 实现取消、unknown 和状态对账"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `8.1` 至 `8.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B09: 暴露受控工具并复用长期任务通知

**Files:**
- Create: `backend/packages/harness/deerflow/tools/builtins/fleet_jobs.py`
- Create: `backend/app/fleet/__init__.py`
- Create: `backend/app/fleet/runtime.py`
- Modify: `backend/packages/harness/deerflow/tools/builtins/__init__.py`
- Modify: `backend/app/mcp_tasks/service.py`
- Modify: `backend/app/gateway/app.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/mcp_driver.py`
- Test: `backend/tests/fleet/test_b09_fleet_job_integration.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-job-integration` / `Durable user task tracking and result notification`。

- [x] **Step 1 — 场景搭建与失败测试。** 用 scripted model 真实 run 提交，再结束 run；阻塞 thread admission、完成 job、重启 service、解除阻塞；验证同一结果通知 receipt 与 job 容器启动次数。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b09_contract(fleet_probe):
    observed = await fleet_probe.exercise("B09")
    assert observed['accepted_notification_runs'] == 1
    assert observed['job_starts'] == 1
    assert observed['secret_in_public_payload'] == False
```

- [x] **Step 2 — 控制边界 RED（已观察）与完整集成验证。** 在 backend 执行：

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/fleet/test_b09_fleet_tools.py tests/fleet/test_b09_fleet_public_status.py -q -p no:cacheprovider
```

实际：缺少受控工具/bridge/宿主绑定，以及 uncertain 状态泄露内部 handle 时观察到行为 RED，最小实现后 GREEN。完整真实链路测试是新增 GREEN 验证；夹具或测试假设修正不算 feature RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# submit_fleet_job uses server current-user/current-run context, not model identity.
# Driver maps queued->submitted, active->working, completed/failed/cancelled normally.
# Keep existing notification retry/dead-letter and untrusted event envelope.
# No installed submitter => do not expose list/cancel/submit Fleet tools.
```

B 阶段仅 detached；awaited 参数明确 422，直到 BC 完成再开放，禁止创建无人消费依赖。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b09_fleet_job_integration.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b09 暴露受控工具并复用长期任务通知"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `9.1` 至 `9.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task B10: 接入定时 job 去重和任务可见性

**Files:**
- Create: `backend/app/fleet/scheduled_jobs.py`
- Create: `frontend/src/core/background-tasks/fleet.ts`
- Create: `frontend/tests/unit/core/background-tasks/fleet.test.ts`
- Modify: `backend/app/gateway/services.py`
- Modify: `frontend/src/core/background-tasks/types.ts`
- Modify: `frontend/src/components/workspace/thread-background-tasks.tsx`
- Modify: `frontend/src/core/i18n/locales/en-US.ts`
- Modify: `frontend/src/core/i18n/locales/zh-CN.ts`
- Test: `backend/tests/fleet/test_b10_scheduled_jobs.py` and `test_b09_fleet_job_integration.py::test_real_scheduled_slots_http_boundary_and_one_docker_execution`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-job-integration` / `Scheduled job deduplication and truthful UI`。

- [x] **Step 1 — 场景搭建与失败测试。** 内部 scheduler launch 提供可信 schedule ID，客户端伪造同名字段无效；连续触发同 slot；前端用 unknown/cancel-pending 数据渲染任务摘要。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b10_contract(fleet_probe):
    observed = await fleet_probe.exercise("B10")
    assert observed['scheduled_container_starts'] == 1
    assert observed['forged_schedule_accepted'] == False
    assert observed['unknown_label'] == '需要确认'
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b10_scheduled_jobs.py -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# dedupe_group = authenticated schedule_id + named job_slot, not free-form model input.
# occurrence success describes Agent submission run; B result remains separate.
# UI uses cancel_requested + tracking_degraded; no invented public RunStatus values.
```

前端先写纯状态映射单测，再 DOM/交互用例；不新增机器管理 UI。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b10_scheduled_jobs.py tests/fleet/test_b09_fleet_job_integration.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b10 接入定时 job 去重和任务可见性"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `10.1` 至 `10.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

B10 execution note: named Fleet slots require scheduler reuse_thread. The internal
scheduler context mode is trusted explicitly; fresh_thread_per_run is rejected before
submission to preserve original-thread tracking. f0005 invocation receipts persist
reuse decisions across terminal/new cycles. Real assertions are read directly from
production runtime, PostgreSQL and TCP/Docker fixtures; a generic FleetProbe is not used.
Observed RED/GREEN and full verification are recorded in implementation-progress.md.

### Task B11: 提供可重复部署、兼容检查与迁移回退说明

**Files:**
- Create: `docker/fleet/compose.yaml`
- Create: `docker/fleet/worker.Dockerfile` and `.dockerignore`, platform-specific runtime hash lock
- Create: `docker/fleet/README.md`
- Create: `docs/deployment/ecs-fleet.md`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/__main__.py` and `operator.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/nodes.py`
- Test: `backend/tests/fleet/test_b11_worker_entry.py`, `test_b11_worker_image.py`
- Modify: `README.md`
- Modify: `backend/AGENTS.md`
- Modify: `frontend/AGENTS.md`
- Modify: `config.example.yaml`
- Test: `backend/tests/fleet/test_b11_fleet_job_integration.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-job-integration` / `Reproducible deployment and safe disabling`。

- [x] **Step 1 — 场景搭建与失败测试。** 构建镜像并验证 digest；本地两 worker 与 NAS fixture 部署；drain 后无新 claim；disable 后查 DB/日志仍有可定位记录；核对所有端口显式绑定。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b11_contract(fleet_probe):
    observed = await fleet_probe.exercise("B11")
    assert observed['old_protocol_claims'] == 0
    assert observed['drained_node_claims'] == 0
    assert observed['worker_public_ports'] == 0
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b11_worker_entry.py -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Configuration defaults: all Fleet feature flags false; profiles explicit.
# Upgrade: drain -> prove stopped -> backup -> host bootstrap -> fleet locked migration.
# Disable: stop admission, keep reconciler until attempts accounted, retain tables/artifacts.
# No destructive down-migration while accepted work or quarantine rows exist.
```

C 镜像入口本阶段不实现；Docker 测试镜像可无外网依赖，生产镜像 digest 由部署者提供，不提交真实凭据。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b11_fleet_job_integration.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b11 提供可重复部署、兼容检查与迁移回退说明"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `11.1` 至 `11.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

B11 execution note: actual host CLI workers, image entry/Linux Docker control and
rendered Compose are verified. The real process chain was added GREEN after entry/
operator/shutdown REDs; fixture failures are not RED. Full containerized Compose daemon
acceptance remains for B12, alongside the required fault/gate matrix. Detailed commands,
artifact source/digests, counts and local scope are in implementation-progress.md.

### Task B12: B 集成故障验收与进入 C 的门槛

**Files:**
- Create: `backend/tests/fleet/test_b_acceptance.py`
- Create: `backend/tests/fleet/fault_proxy.py`
- Modify: `docs/deployment/ecs-fleet.md`
- Test: `backend/tests/fleet/test_b12_fleet_job_integration.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `fleet-job-integration` / `B release gate verifies real side effects`。

- [x] **Step 1 — 场景搭建与失败测试。** 用本地 TCP 故障代理断开控制协议且保留脚本执行；统计文件写入与外部 mock 服务请求次数；清理临时容器、测试 schema，不操作真实 ECS。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_b12_contract(fleet_probe):
    observed = await fleet_probe.exercise("B12")
    assert observed['duplicate_side_effects'] == 0
    assert observed['required_cases_skipped'] == 0
    assert observed['untracked_starts'] == 0
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b12_fleet_job_integration.py::test_b12_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Gate B: PostgreSQL concurrency + real container process assertions + NAS isolation.
# Record command, environment prerequisites, collected/passed/skipped counts.
# Only after this gate enable planning execution for change add-ecs-remote-agent.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_b12_fleet_job_integration.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): b12 B 集成故障验收与进入 C 的门槛"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `12.1` 至 `12.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

## 阶段结束检查

- [x] 所有本阶段 OpenSpec SHALL 均有测试证据；上一阶段回归继续通过。
- [x] PostgreSQL 并发与实际容器/NAS 故障测试非跳过通过；C/组合还需要 Redis 与完整 runner。
- [x] `make test`、`make test-blocking-io`、`make lint` 通过；前端变更完成 check。
- [x] 禁用新 admission 后已有执行仍能对账；无孤儿容器或悄悄释放的 quarantine。
- [x] 在测试报告中明确环境限制；未通过门槛不得执行后继阶段的上线操作。

本计划按 inline executing-plans 交接，不自动发起子代理或开始实施。用户要求开始后，先执行 B01。


## B05/B06 execution adjustments

Implementation uses real ASGI/SQL repositories and LangGraph instead of a generic
FleetProbe. B05 tests are split into test_b05_driver.py, test_b05_retries.py,
test_b05_invocation.py and test_b05_fleet_durable_jobs.py. The invocation helper lives
in harness/deerflow/mcp/tasks/invocation.py; stable tracking identity is an opt-in field
on TaskSubmission, with host create_idempotent validating the same owner/run/handle.

B06 component evidence lives in test_b06_attempts.py, test_b06_worker_routes.py and
test_b06_containers.py. The daemon client and private journal now have real TCP Gateway-loss and
committed-start response-loss coverage in test_b06_fleet_durable_jobs.py. Bootstrap
proves stops and replays pending acknowledgements before claims. Journal permissions,
node ownership, late renewal and ungranted stop replay are covered. 319 adjacent
regressions pass, zero skipped. Startup orphan/concurrent shutdown and operator
worker/NAS integration still prevent whole-task acceptance. Launch profile snapshots add migration f0002_launch_spec; they prevent
an operator profile edit between claim and start from changing reserved execution.


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


B07 accepted-manifest/publication adjustment: complete uses a separately committed
physical stopped proof, then validates sealed files and inserts the accepted manifest
in one locked transaction; it rechecks Postgres clock after NAS I/O. Real HTTP tests
and real Docker worker publication/restart lost-response tests now complement the
filesystem tests. Five concurrent completion calls produce exactly one manifest;
the original long-task tracking row becomes completed. NAS identity is explicit and
checked before migrations. Downloads use host threads permission/ownership and
manifest user/thread filtering. 371 adjacent tests pass without skips. Immutable input
registration/read-only mounts still prevent whole B07 task acceptance; B08–B12 and all
C/continuation tasks remain outstanding.


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


### B03 management adapter clarification — 2026-10-02

The missing management path is implemented as host fleet_management.py HTTP adapter plus
Fleet package management.py facade, preserving actual session-admin+CSRF middleware and
the package/app boundary. API signatures and f0006 profile compatibility are fixed in
openspec/ecs-fleet-contracts.md. Trusted operator CLI does not replace HTTP registration.
This corrects router placement, not the required behavior; implementation/review evidence
remains pending for this follow-up.


## Final B local acceptance and historical step clarification — 2026-10-02

All runtime requirements and local release checks pass: [acceptance report](../../../docs/ecs-fleet-b-acceptance.md).
B01/B02/auth initial RED records are absent; completed historical RED steps mean the
recorded limitation plus current follow-up RED/GREEN evidence, not retroactive execution
of the original example commands. B12 added existing-protocol integration GREEN and
genuine gate RED/GREEN. C/BC are not checked by completion of B; proceed to C without
claiming production deployment.
