# BC10 联合交付验收

2026-10-07：**P0 核心闭环与 P1 可靠性验收已通过。** 当前镜像、真实主干、唯一集中故障、原有证据聚合及最终独立 SPEC→QUALITY 均通过，无关键审查阻塞。P2 必要文档同步及完整交付账目已完成；不把本地验收称为生产部署。

## 当前证据与范围

| 要求 | 真实证据 | 已核实结果 |
| --- | --- | --- |
| 安装后源码一致 | `.local/fleet-evidence/bc10/p1-current-images-02/` | 两份镜像各870安装成员、758当前 Python 模块匹配；Worker 另68模块及 supervisor 匹配 |
| 跨节点 C→B→C | `main-13/main-raw.json`、`main.xml`、`main-command.json` | 主干1 passed，65.331秒；两个 stock mixed Worker、真实 PG/Redis、共享 NAS 测试挂载、scripted model |
| 同流程真实页面 | `main-13/browser.log`、`browser-observations.json`、截图 | 1 passed，26.776秒，skip/unexpected/flaky均0；使用同一目标，未另跑计算图 |
| 分区、取消、重启 | `boundary-03/boundary-observations.json`、`boundary.xml`、`main-command.json` | 单一集中故障1 passed，pytest153.18秒；保留原120秒安全预算 |
| 真实 STOP 后才可解除隔离 | boundary03 原HTTP、journal及SQL | 无STOP时409；原Worker重放STOP并释放容量后200；结果仍为failed，不补造成功或接受旧输出 |
| 取消终态和预算持久一致 | boundary03 原取消/重启/重放的 task 与 budget 行 | cancelled保持不变，预算行保持一致；修复 recovery 覆盖终态的缺陷 |
| 副作用、线程写入和容量 | main13 / boundary03 原进程、SQL及manifest | 重复副作用、线程双写、容量泄漏均0 |
| 自有资源清理 | 两次 `owned-cleanup.json`、`owned-schema.json` | errors/remaining/live_handles为空，测试schema清理，TLS关闭 |
| 原 B/C gate | `combined-03/carried-b-c.json`及原收据 | 原 B XML实际261通过；C12原主干和故障各1通过，无原XML；只承接原执行范围 |
| 操作手册 | [部署](deployment/ecs-fleet.md)、[恢复](ecs-fleet-recovery.md) | 默认flags关闭，jobs→agents→continuations，关闭前drain与对账，保留历史和未决隔离资源 |

上述 main13 / boundary03 证据目录均位于 `.local/fleet-evidence/bc10/`。Root 独立核验记录持久保存在 `root-final/`。聚合检查从原日志、XML、自然退出收据、源码映射和原始运行事实推导结果，不把已生成摘要作为唯一通过依据。

当前 Agent 镜像为 `sha256:4aa3140a1a5d2be2ffb057c7b507e302877b593da369ae48d45748e348943b47`，Worker 镜像为 `sha256:7dbf55296f221b54af21565a371acb61232bc4ac433fa0aa9e172396a1472d17`。被替代的两份旧镜像已按精确ID删除，无全局prune或其他镜像/卷删除。

## 验证边界

- 当前故障验证的是等待/排队 continuation 的取消；不声称重新验证运行中C取消的全部路径。
- Gateway 重启为同一宿主进程内完整app/runtime/lifespan、服务和TLS监听重建；Worker使用同一安装镜像与原持久journal重启，不把Gateway重建称作新的OS进程。
- 原B/C通过记录保持其历史执行范围。变化的参与源码通过后来已验收slice及当前BC10补充覆盖；没有重跑完整当前B/C gate。原B259条报告和后来实际261条承接报告分别保留，不能互相重命名。
- main13执行原夹具版本；之后仅修正boundary目标策略及断言，正常路径AST一致。原夹具和资格对比均已保留。
- 真实PostgreSQL、Redis及Docker在本地运行；共享NAS为测试挂载，模型为scripted model。未完成生产ECS/NAS部署或真实模型服务验收。
- 失败记录保留。main11失败后的三份容量通过原journal STOP释放，原unknown/schema作为失败历史保留；不删除SQL事实制造通过。

## 交付分级

P0、P1已通过；P1聚合原case 1 passed，5.56秒，skip0，Root与两位独立审查者重算结果一致。最终九技术文件冻结清单及静态收据位于 `.local/fleet-evidence/bc10/p1-final-closeout/`，两份最终审查记录位于 `root-final/bc10-final-spec-review.json` 和 `root-final/bc10-final-quality-review.json`。本级只做原主干和唯一集中故障，未增加场景或重跑完整B/Cgate。

P2文档同步及完整OpenSpec账目已完成，task10依据真实证据勾选。组合源码提交 `06ae5f447251fc799edf5ccaafa21adba9829091`，九审查字节与提交一致；[三阶段交付](ecs-fleet-delivery.md)列明34项要求及原计划路径/命令映射。后续状态以本记录与[执行计划](superpowers/plans/2026-10-07-ecs-fleet-bc10-installed-release.md)为准，诊断历史见[运行记录](ecs-fleet-bc10-runtime.md)。
