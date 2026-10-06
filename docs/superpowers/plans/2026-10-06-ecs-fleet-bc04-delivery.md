# BC04 Exclusive Result Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox tracking.

**Goal:** 同一 awaited 结果只进入等待组 continuation；detached 与普通 MCP 结果仍由现有通知服务可靠交付。

**Architecture:** 使用原事务中已持久化、不可变的 JobLinkRow.link_mode 作为交付归属的唯一依据：awaited 对应 wait_group，detached/unlinked 对应 generic_notification。Gateway 在 ready Fleet driver 注册时向原 McpTaskRepository 注入中立认领过滤与归属读取；普通通知服务和内部启动入口都复核该依据。沿用原通知幂等键、租约和等待组 continuation receipt，不增加第二个执行器或可变归属存储。

**Tech Stack:** Python asyncio, SQLAlchemy, PostgreSQL, 原 Gateway/RunManager/McpTaskService，原原生 B worker 与 Agent fixture。

---

用户已授权顺序实施与现有子代理工作流；当前计划落实原 BC04，不重新询问执行方式。BC03 源码基线为 87e0d428、文档基线为 1879a791。BC04 本地原生验收与独立 SPEC→QUALITY 已完成；源码提交 `f24a88d7142e83a4a57077a1cafaa4411c067d42` 的7个reviewed blobs已核对，BC05–BC10 仍必需。

## 实际接线与文件责任

- Create `backend/app/fleet/delivery.py`：宿主归属适配器；仅此适配层读取 optional Fleet ORM。使用相关 NOT EXISTS 查询排除 awaited tracking task；读取不得锁 Fleet task/job，避免 MCP row → Fleet job 的逆锁序。
- Modify `backend/packages/harness/deerflow/persistence/mcp_tasks/sql.py`：中立、可选的 SQL predicate/owner-reader 注入；在 claim_notification_work 的 LIMIT 前过滤。默认未安装策略保留普通 MCP/Local 行为，harness 不导入 app 或 Fleet package。
- Modify `backend/app/fleet/job_tracking.py`：register_fleet_driver 在 ready runtime 条件下绑定策略；实际 app.state.mcp_task_repo 此时已存在，早于 McpTaskService 构造。私有 Agent binding 仍只提交，不启动后台通知服务。
- Modify `backend/app/mcp_tasks/service.py`：_notify_one 在处理已有 claim 和启动之前复核同一持久归属，不能从 driver_data、模型参数或热切换 flags 决定归属。
- Modify `backend/app/gateway/services.py`：launch_mcp_task_notification_run 在 start_run 前以 task_id/user/thread 复核，阻断直接内部调用绕过普通服务检查。
- Inspect `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/job_links.py`、`job_service.py`：原 JobLinks.attach 已在 tracking task 可认领前提交；仅有实际缺口才修改，不为原文件列表机械改动。
- Test `backend/tests/fleet/test_bc04_fleet_agent_job_continuations.py`：一个真实主干、至多一个集中必要边界。
- Docs `docs/ecs-fleet-bc04-runtime.md`、`docs/ecs-fleet-bc04-acceptance.md`、`docs/ecs-fleet-development.md`、`README.md`、相关 AGENTS、OpenSpec tasks/progress。

## Task 1: 原始主干 RED

- [x] 写真实 test_bc04_exclusive_result_delivery，复用原持久父任务和实际子执行，不使用 fleet_probe 或返回固定 observed 的代替实现。
- [x] 运行有效 RED；main-red-2 在两个实际 McpTaskService.run_once 后，awaited dispatch_version == 0 断言失败；未保存完整 row 快照，不把它单独称为已证实整数 claim 值。main-red-1 是重复 driver binding 夹具失败，保留但不算产品 RED。

实际判据代码位于当前测试，核心查询为：

```python
await asyncio.gather(*(service.run_once(now=datetime.now(UTC)) for service in services))
async with item.engine.connect() as connection:
    awaited = list((await connection.execute(text(
        "SELECT m.* FROM mcp_tasks m JOIN fleet_jobs j ON j.tracking_task_id=m.id "
        "JOIN fleet_job_links l ON l.job_id=j.id WHERE l.link_mode='awaited'"
    ))).mappings())
    assert len(awaited) == 2 and all(row["status"] == "completed" for row in awaited)
    assert all(row["dispatch_version"] is None for row in awaited)
    assert await connection.scalar(text("SELECT count(*) FROM fleet_wait_groups WHERE continuation_run_id IS NOT NULL")) == 1
```

Exact runner command from the backend working directory:

```bash
BC04_ATTEMPT=main-red-2 /Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend/.venv/bin/python /private/tmp/bc04_run.py
```

有效 RED 实际结果自然退出 1；原断言曾用 0 代表未认领，实际 schema 未认领值是 NULL。main-green-1 暴露该断言错误，保留为未验收失败；上方判据已改为 is None。RED 没有逐行 SQL receipt，原断言错误使 RED 对实际认领值的证明有限；原未过滤源码和后续实际 SQL/run 证据共同审查缺口，不能回填历史 observations。runner 的私有 PG carrier 通过环境传递、不打印 URI；每次 attempt 单独目录，执行前保存源 SHA、argv、cwd、执行后保存自然退出和自有 schema 清理。已有 attempt 不覆盖。

## Task 2: 唯一归属与原调用点

- [x] 实现宿主 canonical owner 查询及中立 repository hooks，并接入实际 register_fleet_driver。查询逻辑如下；实现中的 adapter 方法名在源码 receipt 记录，不能把此逻辑另做影子运行时。

```python
from sqlalchemy import exists, select
from deerflow.persistence.mcp_tasks.model import McpTaskRow
from deerflow_ecs_fleet.persistence.models import JobLinkRow, JobRow

awaited_link = exists(select(JobLinkRow.job_id).join(
    JobRow, JobRow.id == JobLinkRow.job_id
).where(
    JobRow.tracking_task_id == McpTaskRow.id,
    JobRow.user_id == McpTaskRow.user_id,
    JobRow.thread_id == McpTaskRow.thread_id,
    JobLinkRow.link_mode == "awaited",
))
generic_notification_predicate = ~awaited_link
```

归属读取关联同一个 tracking_task_id 和原 user/thread（Fleet job 唯一 tracking 约束为 user_id+tracking_task_id），校验实际 user/thread；awaited 返回 wait_group，detached/unlinked 返回 generic_notification。原 immutable link 早于可认领 tracking row，复核后不存在合法归属变更窗口。不得给普通服务持有 Fleet task/job 锁。

- [x] 在原 claim SQL 添加 predicate，位置必须在 LIMIT 之前。普通投影仍可更新 awaited terminal 状态；既有 MCP claim 的 dispatch 前复核，直接 internal launcher 同样复核。复核拒绝不得启动 run 或将 awaited 结果标为普通通知已交付。
- [x] 主干补全真实 detached delivery：原 B 提交/结果入库，经原 Gateway launcher、RunManager SQL 创建实际通知 run，实际终态后以原 receipt 确认 delivered。两个服务与 continuation 协调扫描竞争，SQL 证明 awaited 普通启动 0、同组 continuation 1、detached delivered、同线程同时修改 run 不重复。回调仅收集真实结果，不返回假 run ID。
- [x] 原同一个主干 main-green-5 达到 provisional GREEN：1 passed7.10s、自然0、自有schema已清理。仍需收尾 gate finally/evidence 改动后的最终源码资格；不增加测试矩阵、不重跑 BC01–03/B/C 全套、不构建未变更需求的镜像。

```bash
BC04_ATTEMPT=main-green-5 /Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend/.venv/bin/python /private/tmp/bc04_run.py
```

期望该选择器 1 passed、自然退出 0，并保存真实 SQL/进程/NAS/cleanup 观察。失败后只因实际修复再执行，保留失败证据。

## Task 3: 一个集中必要边界

- [x] 主干通过后，在同一真实调用链集中验证忙线程 Conflict 延后、通知 receipt 已提交后回复丢失、服务重建/租约恢复。原幂等键 mcp-task:{task_id}:{dispatch_version}:{dispatch_attempt} 必须收敛到同一个实际 run，不创建第二条修改 run。保护已有 claim 与直接 launcher 绕过时的 awaited 路径。
- [x] 仅添加一个集中边界函数，在 runner 的 BC04_SELECTOR 指向该实际函数；保存确切命令到证据/验收文档。测试实现必须收集原 run/notification receipt，不能把预期值填进 observations。
- [x] 每个 attempt 的证据分目录，source-map 需覆盖实际跨层修改和所用 helpers。schema/process 在自然结束后清理，不能删除其他人的 schema。

## Task 4: 冻结、复审与交付

- [x] 冻结测试参与文件 SHA/Git blobs 与前置 source-map 比对；任何 postqualification 差异单独记账，不能把旧执行说成新源证明。
- [x] 只对实际变更 Python 运行 Ruff check / format --check，运行 git diff --check、严格 OpenSpec validate 与 guidance 校验。无运行时变化的格式/文档不触发旧案例重跑。
- [x] 新鲜 SPEC 审查先核对 Exclusive result delivery path 全部要求，之后新鲜 QUALITY 审查；有真实问题修复与必要重验，未解决不交给下一阶段。
- [x] 记录本地原生范围和未覆盖的 installed production/Gateway 全启动范围；后者仍由 BC10 实际证明，不能用 fixture 证明替代。
- [x] 源码提交后核对 blobs，再勾选 OpenSpec 4.1–4.4、写文档 receipt；不 push、不部署、不打开 operator flags。继续 BC05。

## 计划自审

规格覆盖：认领前持久唯一归属→Task2；awaited 协调与 detached 通知→Task2 主干；重复/重启/忙线程收敛→Task3；实际数据库/run 证据→Task1–4；兼容普通 MCP/Local→未注入默认逻辑和最终源码复审。仅两个案例，不执行原稿的逐边界矩阵。类型关联使用真实 JobRow.tracking_task_id、JobLinkRow.job_id/link_mode 与 McpTaskRow.id，沿用原 run/notification receipt，没有新的 owner 列或迁移。

最终源码资格：主干main-final-2 1passed12.52s，集中边界boundary-3 1passed8.44s，均自然0/自有schema清理。最终7个修改Python SHA/Git blobs与两次预执行map匹配，无postqualification源码改动。159个原tracked helper保留HEAD未修改继承资格（不是运行159个测试）。_agent_e2e_helpers boundary历史map缺失单独记录，不补造历史预执行证明。独立SPEC→QUALITY均Ready；源码提交 `f24a88d7142e83a4a57077a1cafaa4411c067d42` 已核对全部7个reviewed blobs。

实际两个选择器命令（backend cwd，runner读私有carrier环境，不打印URI）：

```bash
BC04_ATTEMPT=main-final-2 /Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend/.venv/bin/python /private/tmp/bc04_run.py
BC04_ATTEMPT=boundary-3 BC04_SELECTOR=tests/fleet/test_bc04_fleet_agent_job_continuations.py::test_bc04_busy_lost_reply_restart_recovers_original_receipts /Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend/.venv/bin/python /private/tmp/bc04_run.py
```

这些是已执行命令记录，不要求重复执行或覆盖已有attempt目录。
