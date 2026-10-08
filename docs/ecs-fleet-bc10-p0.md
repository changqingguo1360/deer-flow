# B/C 联合主干 P0 交付

2026-10-07，P0 本地可演示主干通过；完整 BC10 发布验收仍待 P1。按用户要求，先交付核心闭环，可靠性验收和完整发布文档后置。

## 已验证的闭环

main09 使用两个原始 stock mixed Worker 容器、真实 PostgreSQL/Redis、本地共享 NAS 测试挂载、脚本模型及正常 Gateway：C 在节点 A 提交 B，接受工作区/checkpoint 后停止并释放资源；B 实际执行并提交接受的结果；C 在节点 B 恢复、消费同一结果，完成同一持久任务。正常路径三个执行容器均有运行 PID、物理 STOP 和持久资源释放证据。

同一个真实登录页面显示等待与完成状态，以及关联任务、run 和接受的结果 ID。没有为页面另跑计算图。

| 检查 | 结果 |
| --- | --- |
| 单一主干 pytest | 1 passed，79.84 秒，无 skip |
| 同流程真实浏览器 | 1 passed，43.56 秒，无 skip |
| 跨节点 C→B→C | true |
| 重复业务副作用 / 同线程并行写 / 容量泄漏 | 0 / 0 / 0 |
| 自有容器、进程、TLS、测试 schema 清理 | 已完成，无残留 handle |
| 必要静态检查 | Ruff、格式、Python 编译、frontend eslint/tsc 通过 |

本地证据目录：`.local/fleet-evidence/bc10/main-09/`。主要文件是 `main-command.json`、`main.xml`、`main-raw.json`、`main-derived.json`、`browser.log`、`browser-observations.json`、两张页面截图及清理收据。Root 独立重算主干结果并核对实际 Worker 镜像、浏览器统计和原始日志哈希；未重复运行成功主干。

任务：`agent-32a3a15575fc459691ffbe90c1fd99e3`；B：`be085c38-f905-455b-9d1d-cf8009a649b8`；接受结果：`65d215f7-e9fe-43d8-b049-f6b78c9afd92`。

## 本轮修正与运行前提

补齐当前模型预算契约及输出上限4096；修正 Agent 镜像构建用户、当前 wheel 替换及 skills 目录；Worker 构建必须具备当前 harness 依赖。Stock Worker 通过私有 CA 的正常 HTTPS 接入，原认证、租约和容器入口保持有效。测试机需具备锁定 Playwright1.59.1 的 headless shell1217；首次页面编译等待60秒，原120秒安全预算未改变。

实际主干发现并修复了一处产品问题：读取聊天历史时的后台缓存更新误走人为 checkpoint 修改准入，导致取消等待中的 B。现在缓存更新使用原子、非 superseding 的 thread reservation，远程绑定跳过该可选写入；用户主动 checkpoint 修改继续使用原有准入与保护。

## 交付范围与后续

当前 Gateway 使用修正后的本地源码；保留的 Agent/Worker 镜像执行路径未改变，但其 wheel 内仍含未使用的旧 Gateway router。P0 不声称全体镜像成员已与最新源码一致。P1 必须补齐完整镜像源码资格、一个集中断网/取消/重启恢复场景、聚合证据核验和最终 SPEC/QUALITY。完整 BC10/OpenSpec task10 仍未验收。

P2 完成其余发布文档和运维材料。所有 Fleet flags 仍默认关闭；未进行生产 ECS 部署、push 或 merge。完整失败诊断和资格边界见 [运行记录](ecs-fleet-bc10-runtime.md)；分级安排见 [执行计划](superpowers/plans/2026-10-07-ecs-fleet-bc10-installed-release.md)。
