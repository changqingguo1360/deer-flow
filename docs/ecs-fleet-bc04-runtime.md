# BC04：结果交付归属与通知互斥

当前状态：最终主干 main-final-2（1 passed12.52s）与集中边界 boundary-3（1 passed8.44s）已通过，自然0、自有schema清理；7个修改Python文件与两次预执行源码map一致。独立 SPEC→QUALITY 已 Ready，BC04 本地原生验收完成；源码提交 `f24a88d7142e83a4a57077a1cafaa4411c067d42`。BC05–BC10 仍未完成。本页描述当前接线，不代表完整生产镜像、正常 Gateway startup 或 C→B→C 发布验收已通过。

原 Fleet child 提交事务在 tracking task 可认领前写入不可变 JobLinkRow.link_mode。awaited 对应 wait_group，结果只能由原等待组 continuation 协调器交付；detached 与无 Fleet link 的普通 MCP task 使用 generic_notification。客户端 driver_data、模型参数和热切换配置不能改写交付归属。

ready Fleet runtime 的 register_fleet_driver 向原 McpTaskRepository 绑定宿主 FleetDeliveryPolicy。其相关 NOT EXISTS 谓词在通知 claim 查询内排除 awaited task，SQL 过滤先于 LIMIT 生效，避免 awaited row 占满认领批次而阻塞 detached task。普通 polling 仍更新原 tracking 投影。harness 只接收中立策略，不导入 app 或 optional Fleet package；未绑定策略时保持既有普通 MCP/Local 行为。

原 McpTaskService._notify_one 在处理历史 claim 前复核（集中边界以实际持久 legacy claims 验证）；原 launch_mcp_task_notification_run 在 start_run 前再复核实际 task/user/thread。归属查询不持有 Fleet task/job 锁，不改变已有 job→tracking 锁序。私有 Agent driver 仍只提交，不启动另一套后台通知服务。

普通交付沿用原 notification lease、retry、busy-thread Conflict 延后和稳定幂等键 mcp-task:{task_id}:{dispatch_version}:{dispatch_attempt}。等待组沿用原 continuation_run_id/dispatched_at receipt。成功通知确认会清空当前 notification_run_id，验收不能把该可清空指针当作永久 run 历史；实际通知 run 可按原 runs.metadata_json 的 mcp_task_notification.task_id 关联。

当前主干使用原 PostgreSQL/RunManager、两个真实 McpTaskService、原 continuation coordinator、真实子 shell 执行及 accepted manifests，detached 通知调用原 Gateway launcher/start_run/run_agent。通知图是确定性 LangGraph 夹具，不能据此宣称完整 Agent 推理或生产安装链已验收。

执行按[当前 Superpowers 计划](superpowers/plans/2026-10-06-ecs-fleet-bc04-delivery.md)：一个主干先通过，然后至多一个集中必要边界。历史失败及 source-map 保存于 .local/fleet-evidence/bc04；最终主干/边界和独立 SPEC→QUALITY 已通过；OpenSpec 4.1–4.4 已在源码提交核对后据实际证据勾选，并保留历史RED限制。BC10 负责最终正常部署入口和组合链路验收。
