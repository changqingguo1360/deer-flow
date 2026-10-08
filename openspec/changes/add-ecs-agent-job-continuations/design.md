## Context

统一设计：[B/C ECS 设计](../../../docs/superpowers/specs/2026-10-01-ecs-fleet-first-principles-design.md)。
当前阶段：B/C：等待、继续与统一产品交付。前置：add-ecs-remote-agent 验收通过，B/C 独立执行均可用。

## Goals / Non-Goals

目标：交付本 change 的所有 SHALL 场景并保存故障证据。后继阶段使用相同 identity/reservation 语义。
本次写的是规划工件；不部署、不修改真实配置、不自动启动执行、不把后继阶段视为可选。

共享协议：[Fleet 契约](../../ecs-fleet-contracts.md)。

## Decisions

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

## Stage boundaries

开启 awaited + all-settled；C 保存状态后结束原 run，确认进程停止再创建新 run。不复活旧终态 run，不提供 B→C 或远程 C→C 递归。

## Risks / Trade-offs

- 核心接入必需，不能用纯扩展承诺掩盖 run/worker 身份边界。
- 控制面单点；失联按租约停止，不承诺离线自治或外部工具 exactly-once。
- 所有实际启动后的异常默认人工恢复；cooperative continuation 是正常路径，不是故障重放。
- 真实 PG/Docker/NAS/Redis 测试成本高但不可由 mock 替代；缺少环境只影响验证，不得虚报通过。

## Migration and rollout

先 core bootstrap，再 extension locked migration，再 readiness；按 jobs -> agents -> continuations 开旗。
回退时停止新 admission、drain、确认残留执行并保留历史/manifest；不自动 drop 表，不重置幂等键。
所有关闭开关必须保留已接收工作的 renew/complete/reconcile 能力直到安全清理完成。

## Open questions

无阻塞规划的问题。实际节点规格、镜像 digest、私网地址和 NAS export 由部署配置提供；算法、契约和验收不依赖在文档中硬编码这些环境值。
