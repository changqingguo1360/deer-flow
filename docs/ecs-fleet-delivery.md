# ECS Fleet 三阶段本地交付记录

2026-10-07，按原顺序完成 **1. B持久Job/worker → 2. C完整远程Agent → 3. C→B→C等待续跑与统一产品交付**。B与C并存，共用持久身份、调度和资源账；完整C可提交B、物理停止让出，再由新的C run在另一节点继续同一目标。最终组合源码提交为 `06ae5f447251fc799edf5ccaafa21adba9829091`，最终九技术文件与审查冻结字节一致。

用户要求的分级收尾也已完成：P0主干可用、P1可靠性验收、P2必要文档与交付状态同步。保留 `feature/personal-agent-ecs` 本地分支和现有worktree；本轮不push、merge或激活生产配置。三份OpenSpec实现记录完成，规范archive是后续独立动作。

## 需求与原始证据

最终文档审核直接核对34项SHALL、原计划314条命名artifact记录（含重复引用）、103个新增文档链接，以及三份tasks的48/48/40个完成项。

以下34项以原SHALL定义为准。所有行均在记录的本地scope验收，原始用例和观察不能被当成当前源码全量重跑。具体进程、SQL、checkpoint、manifest和审查限制在链接报告及下述原始目录中。

| ID | 交付要求 | 已核实证据 | 验证范围 |
| --- | --- | --- | --- |
| B01 | 可选包与严格配置 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B02 | 独立迁移与身份持久化 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B03 | 节点鉴权与管理session隔离 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B04 | 原子资源账与drain | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B05 | tracking前置及幂等提交 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B06 | 启动授权、journal及失租停止 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B07 | 输入/输出隔离与manifest接受 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B08 | 取消、unknown与人工对账 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B09 | 长期任务跟踪与独占通知 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B10 | 定时slot去重和真实UI状态 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B11 | 离线镜像、CLI、端口与安全关闭 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| B12 | 真实分区/Compose副作用发布门槛 | [B历史验收](ecs-fleet-b-acceptance.md)；原261条XML中对应B测试类 | 原PostgreSQL/Docker/NAS/Compose；无必需skip |
| C01 | 规范化LaunchSpec及非密钥公共副本 | [C01证据](ecs-fleet-c06-acceptance.md)；原742条C01–C06 XML及C06安装过程 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C02 | 原子准入及Local兼容 | [C02证据](ecs-fleet-c06-acceptance.md)；原742条C01–C06 XML及C06安装过程 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C03 | run/attempt共同所有权 | [C03证据](ecs-fleet-c06-acceptance.md)；原742条C01–C06 XML及C06安装过程 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C04 | 原run_agent完整模型/工具循环 | [C04证据](ecs-fleet-c06-acceptance.md)；原742条C01–C06 XML及C06安装过程 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C05 | 所有saver写入口事务fence | [C05证据](ecs-fleet-c06-acceptance.md)；原742条C01–C06 XML及C06安装过程 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C06 | memory/Store/扩展/终态写入fence | [C06证据](ecs-fleet-c06-acceptance.md)；原742条C01–C06 XML及C06安装过程 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C07 | 持久outbox与有序SSE/END | [C07证据](ecs-fleet-c07-acceptance.md)；formal-v9原4条installed/root过程及原89条native | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C08 | checkpoint/workspace配对与安全恢复 | [C08证据](ecs-fleet-c08-acceptance.md)；c08-task6原24native、6stock、1B Compose及原先完整scope | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C09 | 取消/中断/新run恢复/STOP隔离 | [C09证据](ecs-fleet-c09-acceptance.md)；c09实际中断/rollback/resume/partition/restart及有限失败重查 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C10 | 授权路由和Scheduler容量票据 | [C10证据](ecs-fleet-c10-acceptance.md)；c10原HTTP/PG主干、实际PASSED原case与必要失败重查 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C11 | 安全摘要、关闭准入及Local/B兼容 | [C11证据](ecs-fleet-c11-acceptance.md)；c11原HTTP/Local/SSE/记录响应DOM与关闭准入观察 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| C12 | 生产配方runner与故障发布门槛 | [C12证据](ecs-fleet-c12-acceptance.md)；c12 main-attempt-06 / fault-attempt-05原自然退出与日志 | 保留各slice原生/安装/控制DOM范围；不补造完整套件通过 |
| BC01 | 子job归属、sealed等待组与稳定key | [BC01验收](ecs-fleet-bc01-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC02 | 完整ToolMessage/checkpoint后协作让出 | [BC02验收](ecs-fleet-bc02-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC03 | 恰好一个continuation及换节点恢复 | [BC03验收](ecs-fleet-bc03-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC04 | awaited/coordinator与detached通知互斥 | [BC04验收](ecs-fleet-bc04-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC05 | 一槽闭环、公平轮转及单节点容量 | [BC05验收](ecs-fleet-bc05-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC06 | generation/用户操作/取消竞态 | [BC06验收](ecs-fleet-bc06-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC07 | 累计预算、deadline及非自动崩溃恢复 | [BC07验收](ecs-fleet-bc07-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC08 | 定时父目标阻塞、命名children与Local完成兼容 | [BC08验收](ecs-fleet-bc08-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC09 | 目标与run区分、取消/继续及UI/IM摘要 | [BC09验收](ecs-fleet-bc09-acceptance.md) | 原生主干/集中边界与源码资格；BC05含安装一槽闭环 |
| BC10 | 安装后双Worker闭环、集中故障、证据与运维 | [BC10验收](ecs-fleet-bc10-acceptance.md) | 当前双stock Worker/真实PG Redis/共享测试NAS/scripted模型/同流程浏览器；最终独立SPEC→QUALITY |

B原始承接报告：`.local/fleet-evidence/c07-restart-2e1633848e2b/formal-v8/root-gates/b-gate.xml`，实际261 passed/0skip，35个原B测试类；C01–C06报告：同一根的`formal-v6/root-gates/c01-c06.xml`，实际742 passed/0skip。旧B259条验收报告保持原scope，不被改名为261条。C12两个原case各1passed；没有原XML，不补造XML。

BC01–BC09每级实际主干和集中边界的日志、自然退出收据及观察仍在`.local/fleet-evidence/bc01`至`bc09`。本轮直接读取原日志：分别2.73/2.04、6.59/9.18、8.30/11.09、12.52/8.44、38.76/4.89、11.96/33.00、12.06/9.54、9.54/13.61、5.96/3.29秒，各后端选择1passed，无skip。源码资格、后追加边界和历史故障复用遵守各报告的明确限定，不能从这些时间推断完整安装或全部变体。

最终BC10证据：`.local/fleet-evidence/bc10/main-13`、`boundary-03`、`combined-03`、`p1-current-images-02`、`p1-final-closeout`及`root-final`。主干65.331秒、同流程真实浏览器26.776秒、集中故障pytest153.18秒、原纯聚合5.56秒，各必需选择1passed/0skip。实际跨节点闭环成立，重复副作用/线程双写/容量泄漏均0；无STOP拒绝409，原journal STOP与容量释放后200 fail_stopped。取消目标及预算在Gateway生命周期重建/请求重放后保持一致。

两份镜像各870安装成员/758当前Python模块匹配；Worker额外68模块及supervisor匹配。当前9源、静态、自然退出和最终独立审查在`p1-final-closeout/final-technical-manifest.json`冻结，SHA256 `91abe2d335d0015cc97d86c229e34b9425232f89217d175f197a012b544f434c`。原B/C与当前源码差异通过后来已验收slice及当前BC10增量资格关联；这不是当前完整B/C gate重跑。

## 原计划路径与实际接口

原计划含待接线文件名和probe草图。以下映射保留原行为要求，以实际实现/观察替代草图，不创建空文件满足名称。

| 原计划名称 | 实际实现／验证入口 |
| --- | --- |
| `backend/tests/fleet/probe.py` / `fleet_probe.exercise` | `backend/tests/fleet/conftest.py`的隔离PG；各真实test及c04/c12夹具直接记录DB/HTTP/进程/NAS；原`test_b_acceptance.py`与当前`test_bc_acceptance.py`汇总真实证据 |
| `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/jobs.py` | 原`job_service.py`负责提交事务；`persistence/models.py`、`attempts.py`、`reservations.py`负责持久模型及执行账 |
| `backend/packages/ecs-fleet/deerflow_ecs_fleet/recovery_points.py` | `persistence/workspace_points.py`、`agent_workspace.py`和`worker/workspace_publication.py`共同执行配对、接受和恢复边界 |
| `backend/tests/fleet/test_c_acceptance.py` | `scripts/fleet_c_gate.py`、原`test_c12_remote_agent_operations.py`的两个真实生产runner用例及其原收据；未来执行模式未在C12 slice运行，不冒称运行 |
| `frontend/tests/unit/core/fleet/presentation.dom.test.tsx` | `frontend/tests/unit/core/fleet/unified-task-experience.dom.test.tsx`及原panel用例；真实安装页面由`frontend/tests/e2e/fleet-continuation.spec.ts`验证 |

原B计划中短写的`operator.py`指可选包根的operator入口，`test_b11_worker_image.py`指backend/tests/fleet下的原用例；B10短写node ID在原B09集成模块真实存在。根`.dockerignore`为实际构建输入，不另造docker/fleet同名文件。

## 实际命令与完成检查

原Superpowers `test_*_contract`和probe代码是接线草图，只有创建后的注册node ID作为实际命令。历史B/C/BC01–BC09逐case argv/cwd和源码map见原记录。BC10实际命令从backend运行，以冻结的私有测试环境和显式原证据目录为前提；下列是已执行命令的入口记录，不要求再次运行已通过的计算图：

```bash
.venv/bin/python3.12 -m pytest tests/fleet/test_bc10_fleet_unified_task_experience.py::test_bc10_installed_two_stock_workers_main
.venv/bin/python3.12 -m pytest tests/fleet/test_bc10_fleet_unified_task_experience.py::test_bc10_partition_cancel_restart_boundary
.venv/bin/python3.12 -m pytest tests/fleet/test_bc_acceptance.py::test_bc_combined_release_aggregate
```

原命令的所有argv、自然退出、日志SHA和XML以main13/boundary03/combined03收据为准。浏览器与同一backend主干联动，主机pnpm始终通过绝对路径`scripts/pnpm.py`、cwd frontend。未改的BC09 DOM邻接和P0 frontend check保留原scope，不为文档再重复运行。当前改变的三个Python文件Ruff check/format、Python3.12编译和diff检查通过；另外六个未改技术文件仍匹配原检查字节。

最终文档检查包括`OPENSPEC_TELEMETRY=0 openspec validate --all --strict --no-interactive`、三个`openspec status --change`、`python3 scripts/check_agent_guidance.py`和`git diff --check`。status的4/4仅表示规划artifact齐备；实现完成必须同时有完整tasks、源提交、运行/原始观察和验收资格。最终文档检查与逐项artifact审核保存在`root-final/bc10-p2-completion-audit.json`及`p2-check-*.log`。Guidance检查24份指南、0errors、8个soft warnings；不声称strict-warnings通过。

## 运维和验收限制

[部署手册](deployment/ecs-fleet.md)和[未知执行恢复](ecs-fleet-recovery.md)规定默认flags关闭；jobs→agents→continuations按阶段开启；关闭组合模式前先drain并确认已接收工作、真实STOP和资源账，保留历史、manifest、checkpoint及未决隔离资源。租约到期、取消请求或缺少容器都不能补造STOP。无法确认副作用的执行人工关闭为failed，不接受旧输出或自动复制原attempt。

本次使用本地Docker两个逻辑Worker节点、真实PostgreSQL/Redis、共享NAS测试挂载和scripted模型。Gateway重启为同一宿主PID内完整app/runtime/lifespan和TLS监听重建；集中取消覆盖停止后的waiting/queued continuation，运行中C取消的独立历史证据保留C09范围。未声称两台物理ECS、真实NAS部署、live模型服务、外部IM发送、性能压测或生产激活。

缺失的B01–B03最早RED历史、原中断／失败的完整套件、fixture错误及未验证的变体均保留明确限制，当前GREEN不回造历史。BC10 main11原失败unknown/schema仍留存，其三份容量已由原journal STOP真实释放；最新main13/boundary03自有容器、进程、schema、TLS均清理，无遗留运行handle。
