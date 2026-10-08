> Historical slice acceptance: preserve original cases, counts, source/image identities and scope below. Current B→C→combined status is in [the delivery record](ecs-fleet-delivery.md); later completion does not turn this record into a fresh current full gate.

# C08 workspace / checkpoint 验收

日期：2026-10-06。状态：源码与运行证据已通过 Root 验收，最终 SPEC→QUALITY 均无发现；此文档随单个 C08 slice 提交。

C08 将实际 root checkpoint 与 Node 封存的不可变 workspace manifest 在原有 fenced SQL 事务中配对。partial 只有被接受后才可读取；final/paused 保留 finishing、线程排他与资源预留，直到原执行物理停止。新一轮 C 执行与合法分支从已验证的恢复点复制工作区，并运行原有 Runner。FINAL 使用同一个累计 120 秒期限，partial 不启动或重置它。

本次范围为隔离的本地 C08 验收。C09–C12、C→B→C 组合及 operator activation 尚未完成；Gateway 仍拒绝公开启用远程 Agent。

## 当前源码与运行证据

证据目录：`.local/fleet-evidence/c08-task6/`。冻结清单不会因验收注释而重写。

- SOURCE v14：`task6-source-freeze-v14.json`，SHA `deb2570ff6e1f491867503b52b1c0c34f3a17fd6b6dd9f572f052738372ea79b`。202 个文件、171 个 Python 文件、55 个 Task5 后累计差异，773 个 wheel 输入、765 个安装源码目标。Root 独立校验 10,692 引用 / 9,786 唯一路径，哈希和大小错误为 0；完整源码 SPEC→QUALITY 均无发现。
- 运行补充 v4：`task6-runtime-freeze-v4-final-v1.json`，SHA `3c58252ca63dc2435cfc176fbc760eb658e8a6a504d924db1b1a70884469fa69`。Root 独立校验 976 引用 / 966 唯一路径，错误为 0。它明确引用历史完整回归和本轮受影响路径，没有将旧镜像结果改标为当前源码执行。
- 最终 combined SPEC：`task6-combined-spec-v14-review.json` / `.md`，无发现；最终 combined QUALITY：`task6-combined-quality-v14-review.json` / `.md`，无发现；Root 完整验收及实际 commit 见 `task6-c08-whole-root-acceptance-v1.json`。

Q13-1 曾使取消后的原生工作区 writer 继续复制/fsync，但 daemon 已释放锁。修复仅改变 `AgentContainers.prepare_workspace`：initial/accepted 两条路径强引用并 shield 原生任务，重复取消也必须等待真正完成，保留第一次取消及原生错误。其他方法/import AST 不变；773 个包输入与 55 个 B COPY 文件各只有这一个源码成员改变。ECS wheel 的对应 RECORD 更新，另外五个 wheel 原样保留。

| SOURCE v14 修复的必要验证 | 实际结果 | 原始记录 |
| --- | --- | --- |
| 原生 writer 所有权与相邻准备路径 | 24 PASS，0 fail/error/skip，3.599905375 秒 | `task6-prepare-writer-green-v3.xml` / `-receipt.json` |
| C 原有 initial / new-turn / branch，各 full/delta | 6 PASS，0 fail/error/skip，193.35438829100167 秒 | `task6-ownedwriter-stock-main6-v1.xml` / `-receipt.json` |
| B 原有 Compose complete，实际使用新镜像 | 1 PASS，0 fail/error/skip，7.200802584004123 秒 | `task6-ownedwriter-B-compose-complete1-v1.xml` / `-receipt.json` |

原生回归在修复前有 4 个真正的提前释放 flock RED，修复后覆盖 initial/accepted × success/fsync-error，并验证实际复制、fsync、marker 和锁释放顺序。信号通过原注册 callback 控制触发；RPC/container doubles 不证明真实 Docker SIGTERM 或 SQL 准入。中间 22 PASS / 2 FAIL 来自过宽的测试错误注入，保留为测试脚手架失败。

SOURCE v14 主干使用的 C 镜像为 `sha256:20eab5b9b781e02f095f6c74dbca53e3a97782677c308e252ff86b6063b45931`，六 wheel 的安装 inventory 为 `f57e2814308e47c83ba3ca87582ae24d931467eaf30c9b9389f3bfbf3a5fc13f`。6 个 case 包含 6 个父容器和 4 个子容器，10 个实际独立容器均保留原始 782 成员校验、SQL/HTTP 恢复点与物理停止记录。四个子执行各有实际当前 run stamp 与 materialized model input；没有从 case 数推算进程数。Node entry observer 只观察到其中 6 次，未声称覆盖全部 10 次。

SOURCE v14 主干使用的 B 镜像为 `sha256:d95f09595e5062f5336b42c60f84dc936def8c8bcaff047ba388da8d75e5d42d`。薄 COPY 层基于已核实的 `b4bf95e0...`，前后 inspect 绑定同一 immutable base；全部 55 个 COPY 与当前源码一致，12 个依赖版本 / 303 个不可变成员及 Docker 29.2.0 CLI 字节保持一致，原 Entrypoint 和无公开端口保持一致。实际 Compose/HTTPS/SDK job 场景消费该镜像。另有已加载的 B11 host case 1 PASS，明确仅为宿主 Python worker，不计入 B 镜像 gate。首次 `FROM sha256:...` 被 BuildKit 当成远程仓库引用而超时，记录为构建脚手架失败；后续用已存在唯一 local tag 固定原 base，没有替代 artifact 下载。

## 最终格式补充

首次提交前的 staged diff 检查发现新增 f0009 SQL 字符串有行尾空格，提交未发生。最终只删除 35 行各一个行尾 ASCII 空格；SQL token、约束和引号内值均不变，规范化行尾空格后整个 SQL 与 AST 等价。此变化单独记录在 `task6-formatting-source-runtime-supplement-v2.json`（SHA `254eef41bef1ccef469b653b71d6eeb349ee202b37e2d897dd1c00c76e163827`）；完整 773 输入 / 765 目标只有该 migration 的稀疏覆盖，原冻结清单不改写。Root 独立校验 1,655 引用 / 848 唯一路径，错误为 0。窄 SPEC→QUALITY 均无发现。

两个原有实际 PostgreSQL 升级/并发启动与拒绝降级用例均 PASS，0 fail/error/skip，自然退出 0，耗时 2.003104459 秒，记录为 `task6-format-migration-native-v2.xml` / `-receipt.json`。当前 C 镜像为 `sha256:d3b529c84e99a31c787f6f1137f760ea03ac2b17a5be5743469fd5040980aaff`，inventory 为 `e2073f129dbe6f8a7c6ead1d7baa615b9544a7eb08dbcce38f53760d66fdb830`；当前 B 镜像为 `sha256:fb88422c0ab4c2b4b2b6e18e4b39aefe5a5d6d3cf83ac6603e3121e2cfa66b94`。实际隔离容器完成六 wheel 782 成员及 B 55 COPY / 303 依赖成员核验，另五个完整 wheel 与 SOURCE v14 相同。它们是字节核验，不是重新执行 main6/B12/native24 或历史完整套件；上文实际主干仍绑定原 20ea/d95 镜像与证明。

## 保留的历史完整回归

历史 SOURCE v13 / RUNTIME v3 保存原始源码、镜像 ID、case 与结果。最终 v13 SPEC 通过，但 QUALITY 发现 Q13-1，Root 明确 NOT ACCEPTED；本轮针对该唯一变化完成当前源码审查及必要主干闭环。

| 历史 gate | 实际结果 | 原始记录 |
| --- | --- | --- |
| stock 全矩阵 | 20 PASS | `task6-diagnostic-stock-full-v3.xml` |
| 迁移的原有 history | 4 PASS | `task6-diagnostic-c08-history-v3.xml` |
| Barrier / native pairing | 21 PASS | `task6-diagnostic-c07-barrier-full-v1.xml` |
| C07 stock recovery | 4 PASS | `task6-diagnostic-c07-stock-v1.xml` |
| typed MCP / staged SQL | 5 PASS | `task6-diagnostic-staged-typed-v1.xml` |
| B 完整原有 gate | 261 PASS | `task6-B-required-restored-v1.xml` |
| 最终 C04 installed / native | 15 PASS / 81 PASS | `task6-C04-required-installed-v5.xml` / `task6-C04-final-native-v1.xml` |
| typed fixture / native | 48 PASS | `task6-staged-typed-source-native-v2.xml` |
| C08 native / neighbors | 957 PASS / 517 PASS | `source-native-full-v12.xml` / `source-neighbors-v12.xml` |
| backend default | 14,769 PASS / 72 optional SKIP | `task6-default-offline-v5.xml` |
| strict blocking-I/O | 75 PASS | `task6-blocking-io-v10.xml` |

这些历史 gate 无失败/错误，required gate 无 skip。前四项正好为 49 个不同 required case（45 physical / 4 native），不代表 49 个不同进程。typed final1 与 typed5 重叠；native 1,474 已包含在 default 14,769 中，不相加。default/broad 套件早于最终 opt-in C04/typed fixture，后续 15/81 与 5/48 分别覆盖最终 fixture。此次准备 writer 的单方法修复没有自动重跑完整 49/default/B261，也没有将它们宣称为新字节验证。

原始 16 个 stock 父容器的 FINAL 证据（14 final / 2 paused）绑定实际同一累计 120 秒期限与物理停止，不是故意耗尽 120 秒的测试。Gateway restart 表示 durable PG/NAS 上的 service 重建，不是 Gateway OS 重启或 C09 reconciliation。Node 新 session 证明残余停止/拒绝，不提供 resume 绕过。历史 79 条保存的安装校验引用存在重复，不表示 79 个进程。最终 C04 fixture 保留原 111 个断言中 108 个 AST，3 处明确适配 C08 recovery、paused stop 与 original output ownership。

## 清理、文档与提交

本轮运行/build/observer 均自然结束，owned handles 已回收；当前 C/B 容器残留为 0。累计删除 18 个旧功能镜像，删除前均无容器引用，未使用 force/prune，未删除容器或 volume。保留 6 个自建功能镜像：当前 C/B、B 父层、Barrier、provider、缓存依赖。其他项目与共享基础镜像未动。清理 v3 与最终清单 v2 为冻结之后的独立 sidecar，历史已删除镜像 ID 仍保持真实，不改标。

验收后的 README、runtime 文档、开发指引、计划与 OpenSpec 注释单独记录哈希；不修改冻结清单，且不改变打包输入；只有上述已审查格式补充覆盖 773 个输入和 765 个目标中的一个 migration。提交只包含已审查 C08 slice，不启用远程功能，不包含 C09/BC 实现。
