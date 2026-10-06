# C：完整远程 Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:subagent-driven-development only if the user explicitly chooses delegation. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按先 B、再 C、再组合的顺序完成 C：完整远程 Agent，保持每一步可测试、可回退。

**Architecture:** 共用 Fleet 节点/attempt/资源预留，分离 Job 与 Agent run 的业务状态；复用已有长期任务和完整 run_agent 生命周期。核心提供受控身份与事务入口，扩展负责 Fleet 适配。

**Tech Stack:** Python 3.12、FastAPI、LangGraph、SQLAlchemy/Postgres、独立 Alembic、Docker、NFS/NAS、Redis、Next.js/React、pytest、Rstest。

---

**前置：** add-ecs-fleet-jobs 验收通过，表与协议已迁移。
**工作目录：** `/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs`。
**需求来源：** [OpenSpec proposal](../../../openspec/changes/add-ecs-remote-agent/proposal.md)、[tasks](../../../openspec/changes/add-ecs-remote-agent/tasks.md)、[统一设计](../specs/2026-10-01-ecs-fleet-first-principles-design.md)。
**计划状态：** C01 已完成基础实现、审查与本地验证；C02 已完成可信内部原子准入；C03 已完成所有权与本地恢复隔离；C04 已完成真实 runner 的实施、双阶段审查与独立本地验收；C05 已完成 checkpoint 同事务隔离的实施、双阶段审查与独立本地验收；C06 已完成完整持久写隔离、120 秒累计清理期限、双阶段审查与独立验收，提交 `5e936510`；C07 已完成同事务 outbox、终态封口、持久 SSE 回放、双阶段审查及独立本地验收；C08 已完成完整源码审查、历史完整回归与当前受影响路径闭环，最终 SPEC→QUALITY 和 Root 本地验收通过；C09 已完成孤立本地验收；C10 已按[详细主干计划](2026-10-06-ecs-fleet-c10-routing.md)完成本地验收，C11 已按[实际接线计划](2026-10-06-ecs-fleet-c11-operations.md)完成本地验收并提交 `f9fb8d3a`，C12 待执行。完成项以 OpenSpec tasks 和 implementation-progress 中的实际证据为准。下面示例中的判据与命令仍是计划，不代表已经通过。

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

**用户执行顺序（2026-10-06，覆盖后续 C10–C12/组合）：** 每个阶段先实现并跑通一条真实端到端主干，再验证该变更直接影响的必要分支。下文矩阵是待覆盖的行为说明，不要求在主干完成前铺开测试，也不要求为每个组合新增独立用例。优先复用现有测试；仅实际失败、源码变化或未决风险才扩大或重跑验证。文档、格式及未参与运行的测试修改不触发主干镜像重建。按用户最新范围修正，不再每阶段运行整套后端；收尾采用真实主干、直接受影响的既有用例及格式/静态检查，已暴露失败定向闭环。全量历史或中断结果保留原始状态，不能改称全部通过。

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

### Task C01: 扩展远程放置模型与启动描述

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/launch_spec.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/placements.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/agent_tasks.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/migrations/versions/f0007_agents.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/config.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/models.py`
- Test: `backend/tests/fleet/test_c01_remote_agent_admission.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-admission` / `Versioned remote launch specification`。

- [x] **Step 1 — 场景搭建与失败测试。** 序列化含 recursion、interrupt、stream_modes 的 LaunchSpec 后重建；模拟技能/插件 digest 漂移；查看公共 run/任务 JSON 无 secret。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_c01_contract(fleet_probe):
    observed = await fleet_probe.exercise("C01")
    assert observed['launch_fields_roundtrip'] == True
    assert observed['incompatible_claim_rejected'] == True
    assert observed['public_secret_count'] == 0
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c01_remote_agent_admission.py::test_c01_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# f0007 after f0006_nodes: fleet_run_placements, fleet_agent_tasks, launch specs, recovery reservations.
# Store immutable launch spec + secret references; never raw credentials in run kwargs.
# C enabled requires shared Postgres, db run_events, ownership heartbeat and compatible profiles.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c01_remote_agent_admission.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c01 扩展远程放置模型与启动描述"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `1.1` 至 `1.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task C02: 建立 Local/Fleet 后端契约与原子准入

**Files:**
- Create: `backend/packages/harness/deerflow/runtime/execution/__init__.py`
- Create: `backend/packages/harness/deerflow/runtime/execution/contracts.py`
- Create: `backend/packages/harness/deerflow/runtime/execution/local.py`
- Create: `backend/app/fleet/execution.py`
- Modify: `backend/app/gateway/services.py`
- Modify: `backend/packages/harness/deerflow/runtime/runs/manager.py`
- Modify: `backend/packages/harness/deerflow/persistence/run/sql.py`
- Test: `backend/tests/fleet/test_c02_remote_agent_admission.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-admission` / `Atomic remote admission with local parity`。

- [x] **Step 1 — 场景搭建与失败测试。** 真实准入路径注入 placement insert 错误；独立进程用同 key 重试；spy 仅观察 local task 创建次数，不替代真实 admission 存储。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_c02_contract(fleet_probe):
    observed = await fleet_probe.exercise("C02")
    assert observed['orphan_remote_runs'] == 0
    assert observed['local_tasks_for_remote'] == 0
    assert observed['runs_for_key'] == 1
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c02_remote_agent_admission.py::test_c02_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# RunAdmissionUnitOfWork holds one AsyncSession/transaction.
# Core create_or_reject accepts transaction participant for extension placement insert.
# Local backend remains default; remote commits run+placement+agent_task together.
# Do not call repository methods that open independent sessions inside that transaction.
```

backend registration 契约定义在 harness、app 负责注入，harness 不导入 Fleet 包。移植前先固化 local 准入/取消回归测试。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c02_remote_agent_admission.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c02 建立 Local/Fleet 后端契约与原子准入"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `2.1` 至 `2.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task C03: 统一 claim 与 run ownership 续约

**Files:**
- Create: `backend/app/fleet/ownership.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/attempts.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/placements.py`
- Modify: `backend/packages/harness/deerflow/runtime/runs/manager.py`
- Modify: `backend/app/gateway/deps.py`
- Test: `backend/tests/fleet/test_c03_remote_agent_admission.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-admission` / `Single execution owner across placement and run`。

- [x] **Step 1 — 场景搭建与失败测试。** 两个 SQL session 模拟 stale renew/owner takeover；重启 Gateway hydration 与 scheduler recovery；断言 run/attempt 相同 token/expiry。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_c03_contract(fleet_probe):
    observed = await fleet_probe.exercise("C03")
    assert observed['split_owner_rows'] == 0
    assert observed['live_remote_interrupted'] == False
    assert observed['old_session_renewed'] == False
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c03_remote_agent_admission.py::test_c03_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# claim: lock thread operation/run -> placement -> node -> reservation.
# update run.owner_worker_id = attempt identity; use same expiry for run and attempt.
# renew requires matching node/session/token AND unexpired lease on both rows.
# dispatcher only owns queued remote runs; cannot keep active worker lease alive.
```

锁序与 B 扩展执行项→node 顺序兼容；跨计划新加 parent task 锁必须排在 run 之前，禁止逆序获取。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c03_remote_agent_admission.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c03 统一 claim 与 run ownership 续约"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `3.1` 至 `3.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task C04: 启动复用 run_agent 的完整 runner

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/agent_runner.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/agent_environment.py`
- Create: `backend/app/fleet/runner_context.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/daemon.py`
- Reuse unchanged: `backend/packages/harness/deerflow/runtime/runs/worker.py`
- Test: `backend/tests/fleet/test_c04_remote_agent_runner.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-runtime` / `Full runtime execution on worker`。

- [x] **Step 1 — 场景搭建与失败测试。** 同一 scripted 模型与输入分别运行 Local/Fleet；比较最终消息、usage、checkpoint 引用、artifact 内容；记录 runner PID/host，不以单个 mock graph 代替。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
# Actual scenario:
# test_actual_daemon_runs_real_lead_graph_in_independent_linux_container[parity]
assert ready["host"] != socket.gethostname()
assert parity["local"] == parity["remote"]  # actual messages/tool calls/usage
assert parity["todos_equal"] and parity["has_parent_refs"]
assert parity["artifacts"] == actual_remote_artifact_bytes
assert all(probe[key] == "denied" for key in (
    "environ", "fd", "mem", "ptrace", "db_without_password", "db_wrong_password"
))
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c04_remote_agent_runner.py::test_actual_node_start_freezes_agent_authorization_without_starting_core_run -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

C04 的实际失败入口还包括 workspace 引用、terminal cleanup 与模型绑定/凭据
隔离负控；证据以保留的日志为准。完整容器测试必须显式设置
TEST_POSTGRES_URI、FLEET_TEST_CONTAINERS=1 和 FLEET_AGENT_TEST_IMAGE 为实际核验的
不可变镜像，并具备 Local 对比所需的锁定 postgres extra。缺少依赖、环境或
Docker opt-in 的 skip 不构成验收。上述代码是实际观测断言摘要，不是影子图或
不存在的 fleet_probe fixture。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# runner bootstrap builds RunContext, stores, identity and complete normalized config.
# await run_agent(bridge=bridge, run_manager=manager, record=record, ctx=ctx, agent_factory=factory,
#                 graph_input=spec.input, config=spec.config,
#                 stream_modes=spec.stream_modes, stream_subgraphs=spec.stream_subgraphs,
#                 interrupt_before=spec.interrupt_before, interrupt_after=spec.interrupt_after)
# All subagents/tools remain in the parent reservation cgroup and process lifecycle.
```

实施代码必须逐项传递 stream_modes/stream_subgraphs/interrupt_before/interrupt_after，不能复制第二套 graph 执行栈。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c04_remote_agent_runner.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c04 启动复用 run_agent 的完整 runner"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `4.1` 至 `4.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。



C04 locked implementation scope also includes trusted RunManager attachment,
worker/client.py and bootstrap entry, Agent Docker/container adapter, minimal
host ownership authorize_start/stopped and node route dispatch, plus actual
Linux runner image/test fixtures. These are necessary to reuse the existing
daemon/watchdog/journal, not a second runtime. See the C04 shared contract.
Control credentials use private FD bootstrap, redacted tool configuration and
verified Linux non-dumpable process protection; pure environment filtering is
insufficient. Required container/full graph tests cannot be replaced by mocks.
### Task C05: 实现 checkpoint 事务内 fencing

**Files:**
- Create: `backend/packages/harness/deerflow/runtime/execution/fence.py`
- Create: `backend/packages/harness/deerflow/runtime/checkpointer/fenced_saver.py`
- Modify: `backend/packages/harness/deerflow/runtime/checkpointer/async_provider.py`
- Modify: `backend/app/fleet/runner_context.py`（可信 host fence 注入）
- Modify: `backend/packages/harness/deerflow/runtime/runs/worker.py`（中断 title 写先于 durable terminal）
- Test: `backend/tests/fleet/test_c05_remote_agent_runtime.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-runtime` / `Fenced checkpoint writes including pending writes`。

- [x] **Step 1 — 场景搭建与失败测试。** 真实 Postgres saver 覆盖每个写入口，故障屏障放在 token 校验与 SQL 写之间；并发替换 owner，确认锁/事务排他而非先查后写。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

实际测试文件为 `backend/tests/fleet/test_c05_remote_agent_runtime.py`。
本 slice 不使用尚未注册的 `fleet_probe.exercise("C05")` 示例：先建立真实
Postgres ownership 与 saver fixture，再用实际三表快照、锁等待、backend PID
和事务标识断言。至少覆盖：

- 已安装 AsyncPostgresSaver 的 `aput`、`aput_writes`、`adelete_thread`，以及
  同一实例的三个同步 alias（由外部线程调用）。普通/特殊 pending writes、
  blob、root/subgraph namespace 都进入真实 SQL。
- 旧 token、generation、owner、node/session、active attempt、已释放 reservation、
  stopped attempt、过期或终态以及跨 thread 写入被拒绝；三张 checkpoint 表不改变。
- 真实锁前等待期间过期/接管，以及校验后另一连接接管被阻塞直到写事务结束。
  记录实际同连接/事务；用独立 guard 连接负控证明测试可捕获先查后写。
- 第一条 blob/delete SQL 后异常和 CancelledError 回滚全部修改且释放锁。
- full/delta 与 cache 路径沿用 CheckpointStateAccessor 物化；完整 Linux runner
  回归验证实际 host factory 注入，不能仅手工构造 fenced saver。

当前锁包为 langgraph-checkpoint-postgres 3.1.1；未实现的 copy/prune/
delete_for_runs 保留 NotImplementedError，不扩展新的 CRUD 能力。独立同步
PostgresSaver 保留 Local 用途，不宣称已被 remote fence 保护。


- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c05_remote_agent_runtime.py -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# One DB transaction/connection for lock(owner) + validation + saver SQL writes.
# A wrapper calling stock saver on its independent connection is NOT sufficient.
# Implement transactional saver adapter for current installed saver write surface.
# Enforce lease against DB clock; reject BEFORE mutation, commit while holding owner lock.
```

先枚举当前 saver 写方法并加入 parameterized test；cached/delta saver 仍经相同底层写事务，沿用 CheckpointStateAccessor。这个任务未通过前不启用 C。


**C05 实施前锁定边界（2026-10-02，尚未验收）：**

neutral harness callback 接收本次 writer 的实际 psycopg cursor 和写目标；
身份由可信 host 闭包绑定原 token stamp 与 immutable LaunchSpec，不能来自
checkpoint config。按既有锁序锁 task → run → placement → node → reservation →
attempt，锁后以 clock_timestamp() 校验完整身份、active 状态和全部截止时间。
显式 connection.transaction() 必须同时包住校验和 stock SQL，并持锁至提交。
读取沿用 stock saver，fence 安装在 CachedHistorySaver 的 inner saver。

stock setup 含 CREATE INDEX CONCURRENTLY，不能直接放进上述事务。可信
Gateway/Local initializer 保留原有 setup；remote fenced factory 不建 schema 或
执行迁移，只读验证已初始化的完整 migration/table readiness，缺失、过旧或
未知版本拒绝。Gateway 在 extension service 启动前已有实际 make_checkpointer
初始化路径；仅 NodeServer 的测试须先用真实可信 initializer 初始化，不能
手工伪造 migration 记录。远程 setup 无 DDL 的失败场景须验证数据库未改变。

已有 interrupted title fallback 应先于 durable terminal 落库，保留原先
prior-finalizing/later-run、ownership 与 edit-replay 条件。terminal cleanup 续租
不授权 checkpoint 写入。以上只保护 checkpoint，C06 其他持久写仍待实施。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c05_remote_agent_runtime.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c05 实现 checkpoint 事务内 fencing"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `5.1` 至 `5.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task C06: 覆盖所有远程持久写入口

**OpenSpec:** `remote-agent-runtime` / `All remote durable mutations respect ownership`。

实际入口审计发现原文件清单漏掉 Run/ThreadMeta/event repositories、Store TTL、同步定义、scheduler 与 MCP tracking，以及 extension.start/stop。此前 `fleet_probe` 和 raw token 示例不再作为实施依据。使用已锁定的[详细 C06 计划](2026-10-02-ecs-fleet-c06-durable-mutations.md)，按 C06a → C06b → C06c 顺序执行；子步骤不代表整个 C06 完成。

- [x] **Step 1 — 真实行为 RED。** 实际 PostgreSQL 延迟写、原 attempt 替换、回调线程与不安全插件启动场景；不统计夹具错误或 skip。
- [x] **Step 2 — C06a。** 中性实例绑定 capability；Run/ThreadMeta/events 同事务 fence 与限定终态 bookkeeping，完成双阶段审查。
- [x] **Step 3 — C06b。** 真实 Store put/delete/vector/TTL 和同步 Agent definitions 同事务 fence；remote setup 只读。
- [x] **Step 4 — C06c。** 实际 adapted memory、extension 生命周期/线程传播、scheduler 两个事务及 runner MCP tracking；不支持的有状态后端启动拒绝。
- [x] **Step 5 — 完整 GREEN 与回归。** 全部实际 PG/container 场景零跳过、完整 runner 正常终态、本地兼容、原 B 镜像回归及 backend 检查。
- [x] **Step 6 — 审查、证据、提交。** 完整 spec/quality 批准后记录 source/image 绑定、实际日志与 commit；再勾选 OpenSpec 6.1–6.4。对外开关继续关闭。

### Task C07: 持久事件 outbox 与可恢复 SSE

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/outbox.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/event_bridge.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/migrations/versions/f0008_event_outbox.py`
- Modify: `backend/packages/harness/deerflow/runtime/events/store/db.py`
- Modify: `backend/app/gateway/deps.py`
- Test: `backend/tests/fleet/test_c07_remote_agent_runtime.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-runtime` / `Committed ordered remote events`。

当前源审计确认 RunJournal 与实际 SSE 帧分离；C07 必须持久保存实际帧并在尾部结束后窄封存 END。迁移追加于现有 `f0007_agents`，不得覆盖 B 的历史 `f0003`。下面 `fleet_probe` 示例仅为原始判据，实际实施使用真实 DB/HTTP/进程观察，不以该占位夹具充当测试证据。OpenSpec 7.1–7.4 已完成；实际证据见 C07 acceptance。

实际源审计后的文件职责、事务接口、窄 END 封存、cursor 校验和真实故障测试见[详细 C07 计划](2026-10-03-ecs-fleet-c07-events.md)。详细计划取代下方原始 `fleet_probe` 测试示例作为实施依据；所有 C07 步骤已通过本地验收，后继 C08–C12 保持未完成。

- [x] **Step 1 — 场景搭建与失败测试。** 阻断 Redis，产生事件和终态；恢复后重放 cursor；重复 outbox ack，检查事件顺序/ID/终态和 runner 启动数。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_c07_contract(fleet_probe):
    observed = await fleet_probe.exercise("C07")
    assert observed['ordered_unique_events'] == True
    assert observed['runner_starts'] == 1
    assert observed['terminal_end_recovered'] == True
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c07_remote_agent_runtime.py::test_c07_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Persist event + fleet outbox pointer in the same fenced transaction.
# Publisher reads committed outbox, forwards stable event id/seq, retries idempotently.
# Gateway subscription deduplicates and validates active/accepted attempt identity.
```

不把尚未提交原始 token 直接写 Redis；namespace/subgraph 帧保持现有协议。持久事件限额沿用 run events 规则。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c07_remote_agent_runtime.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c07 持久事件 outbox 与可恢复 SSE"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `7.1` 至 `7.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task C08: 实现 C workspace 和 checkpoint 联合恢复点

**Files:**
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/agent_workspace.py`
- Create: `backend/packages/ecs-fleet/deerflow_ecs_fleet/recovery_points.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/artifacts.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/worker/agent_runner.py`
- Test: `backend/tests/fleet/test_c08_remote_agent_runtime.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-runtime` / `Consistent workspace checkpoint boundary`。

实际源审计后的边界、可信 Node 封存、writer 收敛、原始 checkpoint/terminal 事务和真实故障验证见[详细 C08 计划](2026-10-03-ecs-fleet-c08-workspace-checkpoint.md)。它取代下方通用占位示例作为实施依据。C07 已验收提交 `3d920f2d`；所有 C08 步骤仍未完成。

- [ ] **Step 1 — 场景搭建与失败测试。** 真实工具写文件后发布中间产物；正常结束接受最终 manifest；故意在文件封存前 kill runner，检查没有自动恢复。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_c08_contract(fleet_probe):
    observed = await fleet_probe.exercise("C08")
    assert observed['published_partial_marked'] == True
    assert observed['mismatched_restore_allowed'] == False
    assert observed['automatic_replay_count'] == 0
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c08_remote_agent_runtime.py::test_c08_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# recovery point = (checkpoint_id, immutable workspace_manifest_id, attempt_id).
# stop all tool writers -> seal workspace -> finalize pair in owner-fenced transaction.
# next run clones accepted version; local conversation paths never alias B output dirs.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c08_remote_agent_runtime.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c08 实现 C workspace 和 checkpoint 联合恢复点"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `8.1` 至 `8.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task C09: 远程取消、人工中断与故障隔离

- [x] 按[详细 C09 计划](2026-10-06-ecs-fleet-c09-control.md)完成 Task1–4：原执行 interrupt、rollback/取消完成 CAS、同 task 新 run keyed resume、实际断网与 stock Node 重启 STOP 对账。
- [x] 使用原 run_agent/checkpoint/workspace/STOP 生命周期；取消不直接改变 generation 或释放容量，普通取消后写入仍被拒绝，受控清理累计最多120秒。
- [x] 完成真实主干、必要相邻检查、既有失败定向复核及 SPEC→QUALITY/Root 验收；不把原完整回归失败改标为通过，不重跑有效主干或扩展矩阵。
- [x] 同步文档、静态检查和 C09 slice。实际证据与限制见 [C09 验收](../../ecs-fleet-c09-acceptance.md)。

此节替代原 C09 示例文件清单和 pause/generation 伪代码；原示例不构成实际接口或额外交付物。公有 remote activation 继续关闭，后续 C10–C12 和 C→B→C 未完成。

### Task C10: 路由 preference 与 Scheduler 票据接入

**Files:**
- Create: `backend/app/fleet/routing.py`
- Create: `backend/app/fleet/scheduler_tickets.py`
- Modify: `backend/app/scheduler/service.py`
- Modify: `backend/app/gateway/services.py`
- Modify: `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/reservations.py`
- Test: `backend/tests/fleet/test_c10_remote_agent_admission.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-admission` / `Authorized routing and queued scheduler budget`。

- [x] **Step 1 — 场景搭建与失败测试。** 参数矩阵 local/remote/auto 与权限；两个 scheduler 用真实 PG 争票据；模拟消费票据事务崩溃，验证 run admission key 与 reservation 回收。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_c10_contract(fleet_probe):
    observed = await fleet_probe.exercise("C10")
    assert observed['forged_owner_accepted'] == False
    assert observed['queued_budget_usage'] == 0
    assert observed['ticket_leaks_after_reconcile'] == 0
```

- [x] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c10_remote_agent_admission.py::test_c10_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [x] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# ticket acquisition reserves capacity with short expiry; occurrence stays queued until launch.
# launching uses existing scheduler lease/global budget; consume ticket atomically with run admission.
# no capacity => durable queue; preserve original queue age and recursion_limit.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [x] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c10_remote_agent_admission.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [x] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c10 路由 preference 与 Scheduler 票据接入"
```

- [x] **Step 6 — 记录结果。** 在 OpenSpec `10.1` 至 `10.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task C11: C 任务摘要、部署和本地回归

**Files:**
- Create: `frontend/src/core/fleet/types.ts`
- Create: `frontend/src/core/fleet/api.ts`
- Create: `frontend/src/components/workspace/fleet-task-summary.tsx`
- Create: `frontend/tests/unit/core/fleet/presentation.test.ts`
- Create: `docker/fleet/agent.Dockerfile`
- Modify: `frontend/src/components/workspace/thread-background-tasks.tsx`
- Modify: `frontend/src/core/i18n/locales/en-US.ts`
- Modify: `frontend/src/core/i18n/locales/zh-CN.ts`
- Modify: `README.md`
- Modify: `backend/AGENTS.md`
- Modify: `frontend/AGENTS.md`
- Modify: `docs/deployment/ecs-fleet.md`
- Test: `backend/tests/fleet/test_c11_remote_agent_operations.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-operations` / `Remote task visibility and reversible enablement`。

- [ ] **Step 1 — 场景搭建与失败测试。** UI 对 pending/running/recovery_required 与 cancel-pending 纯映射测试；Local 全链路与 B 套件回归；disable C 后继续查询活跃 C。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_c11_contract(fleet_probe):
    observed = await fleet_probe.exercise("C11")
    assert observed['b_local_regressions'] == 0
    assert observed['public_secret_count'] == 0
    assert observed['disabled_new_remote_status'] == 503
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c11_remote_agent_operations.py::test_c11_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# AgentTaskSummary {task_id,state,current_run_id,cancel_requested,recovery_required}
# Never extend public RunStatus with waiting_jobs; task state is a separate resource.
# Disable-new-work flag must not disable completion/renew/reconcile for existing work.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c11_remote_agent_operations.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c11 C 任务摘要、部署和本地回归"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `11.1` 至 `11.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

### Task C12: C 故障验收门槛

实际源码/制品审计后按[详细C12生产主干计划](2026-10-06-ecs-fleet-c12-release.md)执行。该计划取代下方`fleet_probe`示例与全量命令；先新production recipe和正常startup真实主干，再必要故障邻居。C12仍未完成。

**Files:**
- Create: `backend/tests/fleet/test_c_acceptance.py`
- Modify: `docs/deployment/ecs-fleet.md`
- Test: `backend/tests/fleet/test_c12_remote_agent_operations.py`
- Docs: `README.md`、`backend/AGENTS.md`；涉及前端时同步 `frontend/AGENTS.md`。

**OpenSpec:** `remote-agent-operations` / `C release gate covers all remote mutation paths`。

- [ ] **Step 1 — 场景搭建与失败测试。** 复用 B 故障代理并增加 Redis outage；旧进程晚到写、再启动 attempt、子进程残留核对；计入所有 SQL 表和真实进程证据。

测试判据（该任务注册的场景必须从实际 DB/HTTP/进程收集以下事实）：

```python
import pytest

@pytest.mark.integration
@pytest.mark.asyncio
async def test_c12_contract(fleet_probe):
    observed = await fleet_probe.exercise("C12")
    assert observed['required_cases_skipped'] == 0
    assert observed['stale_mutation_count'] == 0
    assert observed['local_parity'] == True
```

- [ ] **Step 2 — 运行 RED。** 在 backend 执行：

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c12_remote_agent_operations.py::test_c12_contract -vv
```

期望：尚未实现的对应行为断言失败；不能以夹具未注册、连接失败或被 skip 作为有效 RED。

- [ ] **Step 3 — 实现这一条最小协议路径。** 在 Files 对应模块完成以下事务/控制边界，再接入既有调用点；不要另写影子运行时。

```python
# Gate C requires transaction-fenced checkpoint AND memory/extension compatibility.
# A passing mock-runner test alone cannot open agents_enabled in deployed config.
```

重复请求、故障恢复和相邻 Local/B 路径必须使用同一持久状态源。

- [ ] **Step 4 — 验证 GREEN 与相邻回归。** 重跑该测试文件，确认观察到的副作用和数据库结果符合断言；同时执行该阶段已有测试，不从 expected 值构造实际结果。

```bash
PYTHONPATH=. uv run pytest tests/fleet/test_c12_remote_agent_operations.py -vv
PYTHONPATH=. uv run pytest tests/fleet -q -m 'not live'
```

期望：新行为与已有 Fleet 回归 PASS；集成环境缺失必须记录，release gate 不得通过。涉及 UI 的步骤再执行 `python3 scripts/pnpm.py rstest run fleet` 和 `python3 scripts/pnpm.py check`（repo 根）。

- [ ] **Step 5 — 文档、格式和 slice 提交。** 更新实际已实现能力，不提前宣称后继阶段完成。backend 运行 `make format`、`make lint`；检查 `git diff --check`；用显式文件路径 `git add` 本任务源码/测试/文档后执行：

```bash
git commit -m "feat(fleet): c12 C 故障验收门槛"
```

- [ ] **Step 6 — 记录结果。** 在 OpenSpec `12.1` 至 `12.4` 对应项记录测试命令、通过/跳过数、commit ID；只在实际执行后勾选。不能仅靠 CLI artifacts done 判断实现完成。

## 阶段结束检查

- [ ] 所有本阶段 OpenSpec SHALL 均有测试证据；上一阶段回归继续通过。
- [ ] PostgreSQL 并发与实际容器/NAS 故障测试非跳过通过；C/组合还需要 Redis 与完整 runner。
- [ ] `make test`、`make test-blocking-io`、`make lint` 通过；前端变更完成 check。
- [ ] 禁用新 admission 后已有执行仍能对账；无孤儿容器或悄悄释放的 quarantine。
- [ ] 在测试报告中明确环境限制；未通过门槛不得执行后继阶段的上线操作。

本计划按 inline executing-plans 交接，不自动发起子代理或开始实施。用户要求开始后，先执行 B01。


C execution prerequisite: B local acceptance passed 2026-10-02. C01 foundation is locally verified at d0ebd0f8; C02 is locally verified at 9a60c310; C03 is locally verified at 65600e04; C04 runner implementation has passed formal spec/quality review and independent local acceptance; C05 checkpoint fencing has passed formal reviews and independent local acceptance; C06-C12 remain outstanding. C01 adds f0007 after actual f0006, preserving f0002_launch_spec.

### C01 foundation clarification

Shared LaunchSpec/Snapshot/WorkerCompatibility/resources signatures and prerequisite
boundaries are fixed in openspec/ecs-fleet-contracts.md. C01 is durable foundation,
not remote execution availability: Gateway agents_enabled remains fail closed until
actual runner and fencing capabilities are wired. Repositories use caller-owned
sessions for C02 atomic admission. Preserve B v1 job profile grants for existing images.


C01 verification: root23/0, retained B worker gate259/0, default backend13208 pass
with197 optional skips, blocking-I/O75/0, guidance92/0 and Ruff1390 clean.
See OpenSpec tasks and implementation-progress for logs and honest RED limitations.
Public HTTP task integration, claims, runner and write fences are later tasks.


C02 verification: root C01+C02 PG41/0; B retained-image gate259/0; default
backend13211 pass/212 optional skips; blocking-I/O75/0; guidance92/0; Ruff1395clean.
Initial entry RED and separate atomic rollback negative control are distinguished
in OpenSpec tasks. C02 does not activate remote execution or claims.


## C07 accepted evidence — 2026-10-03

See [C07 acceptance](../../../docs/ecs-fleet-c07-acceptance.md). Sequential final
SOURCE and runtime SPEC/QUALITY C0/I0/M0; frozen1597/18, six wheel inventories
and16 full installed proofs match. Independent Root cached/hydrated × writer/
physical-stop seal four cases use original PID1 hardened entry, one start, zero
restart, naturalexit0 and ordered tail/END. Normalbackend13310pass/913defaultskip/
1deselect; blocking75/boundaries74; lint/format1441/guidance24/0errors/2existing
softwarnings/strictOpenSpec3/diff pass. Native89/C01-C06742/Local422 executed v6
and original B261 executed v8 retain their original scope; image-dependent15
and current-source managerinstall1 reran on v9. Exact identities and original
failed intermediate reports are in implementation progress. C08-C12/BC and
remote activation remain pending. Slice commit ID is recorded after committing.
