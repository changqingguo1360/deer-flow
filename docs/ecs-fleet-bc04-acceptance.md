# BC04 本地原生验收记录

BC04 已完成本地原生验收。最终主干 main-final-2 和集中边界 boundary-3 已通过；Root 已核对7个修改Python文件与两次执行前source-map一致。独立 SPEC→QUALITY 均 Ready；源码提交后核对 blobs 并更新 OpenSpec 4.1–4.4。BC05–BC10 和完整安装入口 C→B→C 尚未交付。

范围：结果交付归属由原 immutable link_mode 决定；awaited 只交原 coordinator，detached 仍由原普通通知服务交付。原实际接线见[运行说明](ecs-fleet-bc04-runtime.md)，实施步骤见[当前计划](superpowers/plans/2026-10-06-ecs-fleet-bc04-delivery.md)。源码/文档基线分别为 87e0d428/1879a791，BC04 当前尚未提交。

| 要求 | 当前证据 | 状态 |
| --- | --- | --- |
| tracking 可认领前不可变归属持久化 | 原 JobLinks.attach 提交顺序与 canonical link_mode；独立最终源码审查已通过 | 独立 SPEC→QUALITY 已核对 |
| awaited 更新投影而不被普通路径认领 | main-final-2 的实际两个通知服务、main-sql.json；boundary-3 的持久 legacy claims 防护 | 原生通过，独立 SPEC→QUALITY Ready |
| detached 实际通知交付 | 原 B submit/claim/子 shell/STOP/manifest、原 Gateway launcher/start_run/run_agent、SQL delivered/success | main-final-2 原生通过，独立 SPEC→QUALITY Ready |
| 原等待组只创建一个 continuation | main-final-2/boundary-3 SQL wait_group receipt/count | 原生通过，独立 SPEC→QUALITY Ready |
| 忙线程延后、提交前后恢复及回复丢失幂等收敛 | boundary-3 的 busy/lost-reply/committed-run/recovered-tracking 和最终 SQL | 原生通过，独立 SPEC→QUALITY Ready |
| 当前源码、自然退出、自有资源清理 | main-final-2、boundary-3 与 root-final-source-audit.json | 两次自然0、schema清理，7个修改Python SHA匹配 |
| 独立规格/质量审查 | spec-review.json，然后 quality-review.json | 均 Ready，无待修复项 |

实际运行历史保存于 `.local/fleet-evidence/bc04/`，每个 attempt 独立目录，未覆盖：

- main-red-1：重复 driver binding 夹具失败，不是产品 RED。
- main-red-2：awaited dispatch_version == 0 断言失败；未认领实际值为 NULL，且缺逐行 SQL receipt，因此该失败对认领值的证明有限，不回填整数观察、不单独据此声称产品 RED 已充分证明。
- main-green-1：未认领 NULL 的断言修正前失败，不是通过。
- main-green-2：detached poll 尚未到期，查询缺 run；失败保留。
- main-green-3：真实通知 run 已创建，但宿主夹具缺实际 LoadedExtensions 能力；成功通知也会清空临时 notification_run_id，永久 run 历史需按原 metadata 关联。失败保留。
- main-green-4：夹具导入错误并留下未释放 timing gate，teardown 挂起；仅中断自有 pytest，exit2、KeyboardInterrupt、121.61s，schema dropped=true。原 runner exit.json 的 natural_exit 字段表示子进程返回码，不能将该尝试描述为自然成功。
- main-green-5：1 passed7.10s，真实 runner 返回0，schema `fleet_test_9696546500d74480a08fc01f7107529e` dropped=true。Root 独立核对 output/exit/schema，记录于 root-main-green5-audit.json。后续 gate finally、证据收集和集中边界仍会改变测试源，不能把此尝试当最终源资格。

- main-final-1：1 passed13.20s，自然0，新增实际 main-sql.json 和 detached-process-nas.json。SQL 显示两条 awaited completed、dispatch_version=NULL、notification lease=NULL；一个原等待组 continuation；detached 原通知 run success、event_version=notified_version、delivery=delivered。实际子 shell 自然0，accepted report.txt15bytes。随后归属查询增加原 user/thread 关联，因此该尝试仍不代表最终修改后的源码资格。
- boundary-1：夹具 body helper 重复传入 interrupt_before，TypeError，1 failed8.82s，自然1；不是产品边界失败或通过。原 source/output/schema receipt 保留。
- boundary-2：两服务每批只poll一个，最初两行均awaited，detached仍submitted；忙线程断言尚未到达预期阶段，保留失败。boundary-3在同一阻塞run期间做两次真实到期批次，未增加案例。

主干使用原 SQL RunManager/RunRepository、SQL thread store、普通通知租约/receipt 和真实子执行。通知图是确定性 LangGraph 夹具，checkpointer/store/event bridge 采用现有原生夹具组件。它不证明 installed production Agent 镜像、完整普通 Gateway 启动、外部模型推理或最终组合发布入口；这些仍由 BC10 实际验收。不执行全量旧测试矩阵、不因文档或格式改动重建镜像。

最终资格：main-final-2 为1 passed12.52s，boundary-3 为1 passed8.44s；两者自然0、自有schema已清理。Root核对最终7个修改Python SHA与两次预执行map一致。boundary-3中的awaited历史认领为实际SQL legacy-bc04 leases，复核阻止通知启动；同线程实际阻塞run释放后恢复，丢失回复后的实际通知run ID在新repository/manager中复用，最终delivered。_agent_e2e_helpers在该boundary历史map中缺失，须以未修改基线复用单独记账，不能补称历史预执行已覆盖。独立审查已 Ready；提交 receipt 随后补充。

冻结目录为 .local/fleet-evidence/bc04/freeze：changed-source.json记录7个修改Python SHA/Git blob与两次预执行map匹配；unchanged-helper-provenance.json记录159个原tracked helper与HEAD字节一致（不是159个新增/重跑案例）。其中 _agent_e2e_helpers 的boundary历史map缺失明确为false，main为true。static.json记录7个修改文件的Ruff check、format --check和git diff --check退出0。Root独立核对位于root-final-source-audit.json。green4 interruption.json标明自有PID SIGINT/exit2，保留原exit记录。
