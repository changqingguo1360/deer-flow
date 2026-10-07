# ECS Fleet 未知执行对账

持久 job/worker 与完整远程 Agent 已分别完成本地验收，组合等待续跑已推进至 BC09。
范围与证据见 [B 验收](ecs-fleet-b-acceptance.md)、[C12 验收](ecs-fleet-c12-acceptance.md)
和 [BC09 验收](ecs-fleet-bc09-acceptance.md)。当前仍需完成 BC10 的安装后组合验证；
生产 ECS/NAS 部署尚未验收。以下接口已有本地真实 PostgreSQL、HTTP 和 Docker 验证，
BC10 将进一步验证组合执行中的断连、停机证明与人工解除隔离。

## 处理顺序

1. 用管理员 session 读取 `GET /api/fleet/recovery/jobs?limit=100&offset=0`。
   列表给出 job/attempt/node、取消意图、`stop_confirmed` 与 `capacity_released`；
   继续页使用 offset。不会返回 worker token、执行参数或 NAS 内部路径。
2. 如果停机尚未确认，先由该节点 worker 的恢复流程停止残留容器，再以当前节点 session
   上报 stopped。凭据撤销后须使用新节点凭据；不能把租约到期或取消请求当作停机证明。
   没有证明时 unknown 仍隔离并保留资源，不自动重派。管理 API 没有强制标记 stopped 的入口。
3. 已确认停机且资源释放后，核对实际副作用与是否需要另一次独立执行。
   向 `POST /api/fleet/recovery/jobs/{job_id}/resolve` 发送下列内容：

```json
{
  "expected_attempt_id": "当前列表中的 attempt ID",
  "side_effects_reviewed": true,
  "note": "记录已核对的进程与实际副作用"
}
```

该请求需要有效管理员 session，以及 CSRF cookie 与匹配的 `X-CSRF-Token`。
操作者来自服务端身份，不能在 body 中指定。PAT、内部服务、node bearer 或关闭鉴权后的
默认身份均不能代替管理员 session。没有真实停机记录、资源未释放、attempt 已改变或
已有成功结果时，返回 409，不改变执行状态。非管理员返回 403；匿名返回 401。

成功响应包含 job_id、attempt_id、state=failed 与 recovery_event_id。
这表示停机后无法确定的执行结果已经人工核对并关闭；不会接受旧输出、补造成功或重新
执行原 attempt。相同操作者与相同 note 重试返回同一审计记录，改写历史请求返回 409。
`GET /api/fleet/recovery/jobs/{job_id}/events` 可查看操作者、原因与时间。

## 关闭之后

原长期任务通过已有轮询路径变为 failed，保持同一个 tracking ID。节点下一次 heartbeat
或重新 bootstrap 核验无剩余未知执行后可恢复准入。历史 attempt、实际 outcome、产物与
审计记录保留；如需再执行，必须发起新的独立提交，不能复用原 attempt 或提交去重键。
关闭新工作 jobs_enabled 不会切断已有工作的对账。B 阶段开关与部署仍须遵守
[交付路线](superpowers/plans/2026-10-01-ecs-fleet-roadmap.md)。
