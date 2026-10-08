# ECS Fleet 主干整合记录

日期：2026-10-08。目标仓库为用户 GitHub fork `changqingguo1360/deer-flow` 的 `main`，以及 Gitee `wangw1360/deerflow2` 的 `master`。不向上游 bytedance 仓库提交或合并。

本地 Fleet 文档交付基线为 c46d9907，库表文档为 3d7752ea；随后保留 Gitee 初始化提交 a816a744 的历史，整合其 PR 模板，使用项目实际 README 和忽略规则。GitHub fork 主干基线 c35022e1 比 Fleet 开发基线新增 120 个提交，本轮通过真实 merge 保留双方历史。

## 合并兼容修正

- Gateway 保留 Fleet 准入/所有权/准备远程流，与主干项目继承、内部调用权限、trace 和幂等重试保护共同工作。
- 持久化保留远程写入 fence；创建 executor thread 时绑定新的线程 incarnation；新 claim_unowned/set_project 入口禁止远程能力误用。
- 运行时保留 Fleet 终态、事件流和工作空间写入约束，同时接入主干消息序号、取消/清理与 MCP promotion。
- `0022_fleet_main_merge` 合流两条已发布宿主迁移链，保留所有 revision ID；自身不执行 DDL。Fleet 独立 f0001–f0017 链保持不变。
- 每请求 trace 不参与重试身份变化：执行继续使用原 admission 的冻结 trace；所有其他执行输入仍严格比较。现有 C02 重试用例以不同请求 trace 复现失败，再验证修正和改变输入拒绝。
- 合并后的指南超过硬限制，完整说明移到明确链接的模块文档，不删减要求。

## 本轮实际验证范围

- 前端锁文件安装新增 Papa Parse 依赖；`scripts/pnpm.py check` 最终退出 0。
- 持久化迁移/bootstrap/项目/线程/MCP/run/agent：210 passed。
- 运行时相关批次：241 passed、320 passed；SSE 修正选择 3 passed；取消/清理选择 14 passed。批次可能重叠，不相加宣称独立用例总数。
- Gateway 首轮 315 passed / 39 failed；迁移合流及兼容夹具修正后，失败选择重查分别 39 passed、8 passed，不声称重跑完整后端套件。
- 真实 PostgreSQL C02、BC06、BC07：22 passed，154.09 秒；无 skip。覆盖远程准入/重试、generation、取消和累计预算。
- Python 3.12 解析 371 个变更 Python 文件，无语法错误；变更无冲突标记、diff 空白检查通过。修改范围 Ruff/format 通过。
- OpenSpec 严格验证 3 passed / 0 failed；24 份开发指导检查 0 errors / 7 soft warnings。

原安装镜像、浏览器和集中故障验收继续保持[原交付记录](ecs-fleet-delivery.md)的历史源码范围。本轮没有重新构建安装镜像，也未完成阿里云真实 ECS/NAS 部署验证。新配置 flags 仍默认关闭；发布新镜像前需按[部署手册](deployment/ecs-fleet.md)构建与验证相应产物。

## 普通 CI 与专用验收入口

主干普通 CI 未提供 Fleet 安装镜像、节点凭据、隔离数据库 URL、原始证据目录或真实 Gateway，因此 core backend shards 明确排除 `tests/fleet`，不冒充 Fleet 验收通过。默认安装 collection 仍收集全部测试，并执行不依赖部署资源的 Fleet 配置与元数据隔离检查（本地 22 passed）。Fleet 必须走部署手册中的显式 gate 和已记录安装主干/故障入口；本配置没有删除或跳过其必需用例。

普通 DOM CI 不选择需要真实 HTTP 夹具的 BC09 验收文件；设置 `BC09_HTTP_FIXTURE` 后恢复选择，缺少内部必需数据仍失败。普通 demo Playwright 不选择 BC10 安装验收；设置 `BC10_THREAD_ID` 后恢复选择，其其他 required() 输入检查保留。

新增选择配置验证：普通 Fleet presentation 1 passed / 0 skipped；原 main HTTP 夹具下 BC09 主干 1 passed，另一边界因名称选择被过滤。前端 check 通过。Buzz 原失败时序项在本地重查 1 passed，不据此宣称整个云端 CI 通过。

修正 PR 的定向合并复查：8 个兼容测试文件合跑 578 passed / 1 skipped（PostgreSQL 专用项未配置）；0018 合流保护检查单独 1 passed；离线 Fleet 基础检查 22 passed；指南测试 12 passed，24 份指南 0 errors / 0 warnings。保留全部指南内容并移到必读模块文档，未提高预算上限。
