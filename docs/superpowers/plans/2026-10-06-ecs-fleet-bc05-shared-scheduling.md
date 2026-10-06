# BC05 Shared Scheduling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox tracking.

**Goal:** 一份持久公平调度与资源账本驱动 B/C 共存；单槽按实际 C→B→C 执行，多容量保留一个标准 B 任务可用的真实节点。

**Architecture:** 原 B scheduler、C ownership 与 Scheduler capacity tickets 在原执行/父记录锁之后、排序 Node 锁之前进入同一个 private scheduling policy。类别轮转只在新的真实 reservation 提交时推进，已有 ticket 消费不重复计数。一个 mixed stock worker 复用原 daemon、client、journal、flock、AgentContainers 与发布器，按实际 kind 分派准备、输入和完成路径。

**Tech Stack:** Python asyncio, PostgreSQL/SQLAlchemy, 原 FastAPI node routes, Docker 原 production Agent recipe, 原 B/C resource reservations。

---

基线：BC04 source f24a88d7142e83a4a57077a1cafaa4411c067d42，receipt 529819da84d31df4ede2caf971d3e5463a5fffaf，工作区 clean。BC05已本地验收，源码 `c153ad1951fcf1f8edaa5f5a4a0483d9228c3346`，22 reviewed blobs一致。BC06–BC10 仍必需。用户已授权顺序执行与既有子代理工作流；无需再询问执行方式。

## 配置与持久接口

`backend/packages/ecs-fleet/deerflow_ecs_fleet/config.py` 增加以下字段，在 continuations_enabled 条件下验证：

```python
scheduling_mode: Literal["reserved", "serial"] = "reserved"
reserved_job_profile: str | None = None
```

唯一 approved job profile 可推断标准 B；多个必须由 operator 指定 reserved_job_profile，指向 kind=job 的已批准 profile。不得选最小 profile 降低标准 B 要求。continuations_enabled=false 的原纯 B/C 配置不引入新强制字段要求。模型不控制 mode/profile；默认开关仍关闭。

reserved：每次新的 C reservation 后，至少一个 live/current-session/B-capable/allowlist-eligible 的实际节点保留指定 B CPU+memory 配额。该配额可以已经被 B 合法使用；不能在 active B 之外再强制永久空闲一个额外 B 槽位，否则两槽池的持续 B 到达会使 C 饥饿。C 实际 fits 检查仍计入所有未释放 B/C/ticket/unknown，不能超卖；B 配额保护检查按单节点 total 减 C/ticket 与 hypothetical C 分配，确认 C 不占用标准 B 配额。C 与 B 可在不同真实节点分别容纳；不得拼凑跨节点 CPU/memory。资源不足时排队，不自动降级 serial。

serial：全池最多一份未释放 B/C reservation，包含 held tickets、unknown/quarantined 等未证明停止记录；不是每节点一个。C 异步提交后按原 BC02 STOP/release 让出，B 完成后原 BC03 admission 恢复 C。没有同步长任务等待 API。

`persistence/models.py` 与新 `migrations/versions/f0014_shared_scheduling.py`：private SchedulingRow singleton id=shared，next_kind=job/agent；NodeRow.claim_kinds 为当前 session 的可执行类别 JSON。仅 private Fleet metadata，不注册 host Base。f0014 接在 f0013_continuation_receipt 后，实际随机 schema 升级验证。

`nodes.py`：open_session 清空 claim_kinds、agent_compatibility、runtime_digest，避免旧 Agent 广告使新的 job-only worker 被 C 队列阻塞。authenticated claim 广告锁定当前 node/session，分别记录 job、agent 或两者；job-only 不保留旧 Agent readiness。能力广告不替代 operator allowlist/approved profile/兼容版本。

`admission_policy.py` 的中立候选与策略接口：

```python
@dataclass(frozen=True)
class QueueCandidate:
    kind: str
    key: str
    created_at: object
    profile: str
    node_ids: frozenset[str]
```

SharedAdmissionPolicy(config, *, agent_candidates=None).lock(session) 返回原事务的 AdmissionWindow；window 提供 permits(kind, key, node, profile, *, candidate=None) 和 reserved(kind)。agent_candidates 是宿主注入的 async callback，返回实际可准入 C 候选；optional package 不导入 app/core models。只读取相反类别的持久队列，不 FOR UPDATE 它们。严格按 eligible 候选原 age/key FIFO，不能绕过另一个事务锁着的更早 eligible 候选；不能被不兼容的最早64条永久挡住。

## 实际文件职责

- Create `backend/packages/ecs-fleet/deerflow_ecs_fleet/admission_policy.py`：持久类别轮转、eligible FIFO、serial 全池限制和 reserved 实际节点预算。
- Modify `config.py`、`nodes.py`、`persistence/models.py`；Create `migrations/versions/f0014_shared_scheduling.py`：上述 operator policy 和当前 session capability/private singleton。
- Modify `scheduler.py`：B 原 queued/inputs/attempt/reserve 路径接 common policy，不另造执行队列。
- Create `backend/app/fleet/admission.py`：宿主 actual C candidate reader，读取 queued placement/task/core run，以及 eligible scheduled occurrence；保留 current task/generation、pending/no owner/no lease/cancel/deadline、版本/节点兼容条件。
- Modify `backend/app/fleet/ownership.py`：安装同一 candidate reader；原 C claim 在原 scheduled parent→occurrence→task→run→placement 后进入 policy，再按排序 nodes/ticket/reservation。最初 node capability 广告独立 TX 先结束。
- Modify `backend/app/fleet/scheduler_tickets.py`：reserve 时 common policy；只有实际新 held reservation 推进 turn。后续 consumed ticket→attempt 使用已有账本，不因 next_kind=job 拒绝或重复 advance；保留原 lease、occurrence budget、用户/线程和稳定幂等键。
- Modify `backend/app/gateway/routers/fleet_nodes.py`：ClaimRequest 与实际 router 支持 mixed，优先读取持久类别，再调用原 B/C claim；实际事务重新验证，不能靠进程内轮转保证公平。
- Modify `worker/client.py`、`worker/__main__.py`：支持 kind=mixed，一个 session/client/private settings/flock/journal，仍要求 Agent approved image/private config。
- Modify `backend/app/fleet/agent_control.py`：独立 observer 只在单次 observe_original_control 的1秒只读查询超时后按原owner重试；每轮bound不变，不吞后续writer-stop/settlement超时、真实OwnershipRejected、SQLAlchemyError或外部CancelledError，不更新租约/权限/执行预算。read/prepare 原语义不变。新增 mutation guard/observer 诊断仅固定stage/predicate/error_type/已知函数行号，原异常与校验保留。
- Modify `worker/agent_environment.py`、`worker/agent_runner.py` 与原 host `runner_context.py`：中立可选 execution_scope 在既有 mutation/workspace scope 后、创建 executor task 前进入，仅传递私有 Fleet submitter/yield ContextVars，工具发现、执行与 before_model 继承同一已绑定能力；setup/cleanup 的 cancellation settlement scope 不得扩到正常执行。安装主干实际覆盖这个原位接线。
- Modify `worker/daemon.py`：新增 agent_prepare_workspace 参数与 job workspace 共存，按 claim.kind 分派 prepare/inputs/bootstrap-completion/seal；Agent 发布仍走原 writer ownership/shutdown join。原 AgentContainers.launch 已将非agent grant交给原 DockerContainers，无需新 container runner。
- Modify `backend/app/fleet/runner_context.py`：严格验证 approved runtime plugin snapshot 后，内置 ecs-fleet/deerflow-ecs-fleet/deerflow_ecs_fleet:install 控制面项由原 bind_private_fleet_driver 绑定提交/让出能力，不再进入私有 runner 的通用 Gateway 插件安装；同 use 的异常身份拒绝，其他 approved plugins 保持原加载路径。安装主干必须实际覆盖此接线，不能跳过验证或启动 Fleet 后台服务/迁移。
- Create `backend/tests/fleet/test_bc05_fleet_agent_job_continuations.py`，必要时创建单一 BC05 composition helper。最多两个实际案例。既有 migration-head 断言仅改新 head 字符串并记录为维护，不扩旧测试套件。
- Docs：当前计划、BC05 runtime/acceptance、Fleet development、README、相关 AGENTS、OpenSpec tasks/progress/roadmap。

## Task 1: 原主干 RED 与必要安装制品

- [x] 审计真实 B/C/ticket claim 与 daemon 路径，确认没有持久 category turn，固定 job/agent worker 缺混合准备/完成接线；确认 AgentContainers 已复用 B launch。
- [x] 决定显式 reserved/serial、指定标准 B profile、current-session capabilities 和既有锁序；不用按资源动态退化 serial。
- [x] 创建一个实际主干函数 `test_bc05_single_slot_cbc_main`。使用实际 stock mixed worker、原 installed AgentContainers、原 B DockerContainers 和当前源码 production Agent image；一 node budget=max(C,B)、max_parallel=1，实际 submit/await/publication/STOP/release/B completion/原 continuation C。保存实际容器/SQL序列，不把预期 [agent,job,agent] 直接填入 observations。
- [x] main-red-1 自然1/1failed12.99s，自有schema清理；原 authenticated TCP mixed /claims 返回422（只接受job/agent），实际主干要求200。旧C12image仅preflight，不称已执行CBC容器。main RED 若在缺失的 mixed/settings/route/product interface 处失败，应说明是新增功能缺口；错误 fixture、配置拼写、URI缺失或 skip 不算有效 RED。保留原失败，不反复补造历史记录。
- [x] 准备必要的新 production build context `.local/fleet-evidence/bc05/production-artifacts`。冻结当前 app/harness/Fleet wheels与其源码成员；extension-api/provider/base/model-bindings/skills 只有核对未修改才复用。workspace-contracts 必须与新 runtime-bundle 的 Fleet descriptor 同步，增加相同身份及原 fenced-no-retained-user-data-writer 契约，冻结新 SHA，不能称 C12 unchanged。C12 runtime-bundle.plugins=[]，不能作为新私有Fleet submit能力的unchanged资产：BC05新runtime-bundle明确加入 name=ecs-fleet、distribution=deerflow-ecs-fleet、use=deerflow_ecs_fleet:install；冻结新metadata和实际installed plugin snapshot，与原launch spec一致。隔离测试私有plugin配置不得启动Gateway services/reconcilers或迁移，不代表operator部署激活。禁止把旧 C12 installed image 当当前 BC01–05 源码证明。

原 production recipe 保持 `docker/fleet/agent.Dockerfile` 不变；其 SHA 为 fffbb8a0a4231162379f83698108c0a4b2870ccc41445433c585ae6820dd4e58；固定 base 为 deerflow-c08-dependencies@sha256:da399e40963d6d8ad1beff9bbe592e40a327f7417b14c6e929e4c61a4804ff5c。实际构建使用 named context agent_artifacts、network=none、pull=false、SHA256SUMS/hash-locked requirements/pip check。这一必要新镜像只服务当前主干，不机械重建旧镜像/重跑 C12。

主干选择器从 feature backend cwd 执行：

```bash
/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend/.venv/bin/python -m pytest tests/fleet/test_bc05_fleet_agent_job_continuations.py::test_bc05_single_slot_cbc_main -q -s
```

执行 runner 从 mode0600 `/private/tmp/bc01-test-postgres-carrier.json` 注入 PG 环境；不打印 URI。实际 TEST_POSTGRES 为原 native 15436 服务，不新建数据库容器。每次独立 attempt 目录执行前保存 SHA/Git blobs、argv/cwd，后保存自然 exit、SQL/NAS/container/journal/cleanup；不得覆盖旧 receipts 或删除非本任务资源。

首次 installed main-green-1 自然1/10.25s，停在实际 installed compatibility 预检，没有 C/B claim 或 CBC 容器证据；同 image 无 secret 诊断确认 workspace-contracts.plugins 仍为空，与新 bundle 的 Fleet descriptor 不匹配。严格校验正确拒绝；修正安装制品，不放宽 validator。旧构建与失败 attempt 原证据保留，未形成主干 GREEN。

build-4 实际构建自然0，镜像 `sha256:c374cc209171cdfbd954c566f4e07c8c8b0c9e30a1708b722b7d465865828eae`；Root独立核对735条build源码SHA全部匹配，bundle/workspace-contracts插件身份一致，仅为制品核对，不是735测试或容器验收。main-green-2 自然1/19.40s，自有schema清理；该attempt的实际输出是 Unknown job，没有证明CBC完成。初步猜测默认1800秒超过批准180秒导致失败，此判断随后撤回：main-green-3 实际ToolMessage证明 submit_fleet_job is not a valid tool，正常执行任务未继承Fleet/yield ContextVars。main-green-3 自然1/13.63s，自有schema清理。请求明确execution_timeout=120、queue_timeout=120仍作为合法fixture配置保留，但它不是修复本次根因的证据。

build-5 含中立 execution_scope 修复，实际自然0，镜像 `sha256:500d841a5285b851c2fb662554056dd5cd377fd24be1ef43c2ded3b5cb5aab8b`；Root再次核对735条build source SHA与当前一致。main-green-4 自然1/77.97s，在首model之前实际fenced mutation拒绝，未provider call/child job，不能记CBC通过。自有schema dropped，container-cleanup errors=[]、remaining=[]；具体拒绝operation/ownership状态仍在定位，不因超时现象放宽fence。

同一主干 main-green-5 自然1/30.74s，仍在provider前被撤权；新增guard诊断未记录reject，因此尚不能确认是哪条fence判据失败。原AgentRunner控制observer会把任意未捕获异常转为ownership lost，已批准仅增加error_type/已知函数行号的脱敏诊断，未改变失败处理语义。fence snapshot在STOP后取得，不能冒充reject瞬间状态证明；同一次主干仍未验收，boundary尚未执行。

main-green-6 自然1/24.02s，自有schema和容器清理。Root独立读取实际container诊断：error_type=TimeoutError，调用帧为observe_original→observe→observe_original_control→asyncio.timeout.__aexit__，没有guard reject。证实原只读控制观察1秒超时被observer当作ownership lost，取消了首model前C。已授权仅单次control read TimeoutError重试，不放宽真实fence；其必要正/反向验证限定同主干与一个集中boundary。

build-8 实际自然0，镜像 `sha256:b25bba66da679ba1b6c4cf742bfca08977b0f9b81b510a2d6c0b6d70519ad747`。main-green-7 自然1/38.24s，schema/container清理；实际observer timeout retry已出现，C/B两个执行分别物理exit0、STOP/released，B accepted manifest与continuation admission已达成。第三claim后fixture错误地在每step调用stager.join_writers，提前关闭长期writer，save_record因此失败，第三容器未launch；三个owned_refs不是三个容器执行证明。只修fixture为每step确认pending writes settled、最后shutdown统一join，复用同镜像同主干；尚不能记完整CBC通过。

## Task 2: 原路径实现与一个主干 GREEN

- [x] 实现上述逐文件接口，在原 execution/scheduled-parent locks 之后锁 singleton，再锁共同排序的 Node；不得 node→singleton、singleton→scheduled parent 或相反 queue FOR UPDATE。
- [x] eligible 读取使用实际版本/allowlist/session capabilities/resources/deadlines/cancellation/core ownership；只在成功新 reservation 同事务推进 turn，回滚不能消耗 turn。已有 ticket 已 charge 的后续 claim 保留原 fence，无第二次 advance。
- [x] 当前安装源、真实 HTTP node claims 和 actual one-slot container sequence 达到主干 GREEN：main-green-8 自然0/1passed38.76s，三个实际容器exit0/STOP/released、原continuation结果引用回流，schema/container清理。
- [x] 持续 B/C eligible 到达的轮转/FIFO/资源账本观测集中到Task3唯一必要boundary；成功main不因新增独立boundary函数而机械重跑。区分已执行容器spine与后续native调度SQL范围。
- [x] 主干通过前不执行边界、不新增矩阵、不运行 B/C/Fleet 全套。无实际源码/失败/未解决资格问题，不重复成功案例。

## Task 3: 一个集中必要边界

- [x] 主干通过后，仅一个 `test_bc05_shared_turn_and_reserved_capacity`：并发/重建 policy 原持久 turn 收敛、held ticket 不重复 charge/advance、同类别 eligible FIFO、不兼容 head 不饿死、reserved 真实节点及跨节点碎片不能伪造配额、active B 合法使用其配额时 C 不饥饿、serial unknown 不释放容量，以及此次实际组合修复所需的控制read timeout重试/真实撤权不吞。主干若已记录自然retry，复用正向证据，边界只补hard rejection。复用同一真实 repositories/locks，不创建影子模拟执行器。
- [x] 静态/格式在最终资格前完成。每个 attempt 独立 source-map/SQL/cleanup；只因实际修复再执行同一选择器，不增加独立案例。

```bash
/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend/.venv/bin/python -m pytest tests/fleet/test_bc05_fleet_agent_job_continuations.py::test_bc05_shared_turn_and_reserved_capacity -q -s
```

实际 boundary-2 自然0/1passed4.89s：四轮原生竞争claim呈job/agent轮转、各类别FIFO、scheduler重建；active B quota/单节点容量与碎片拒绝/serial unknown charge/ticket实际一次转移及turn不重复/原inactive SQL authority rejection均有记录。未start-authorized allocations保持charge直到自有schema drop，不能称物理STOP证明。该边界证明inactive authority拒绝，不冒充真实撤权场景。boundary-1的fixture session factory属性错误保留，未算产品资格。

## Task 4: 冻结与交付

- [x] 最终所有变更源码与实际 installed wheel/image、主干/边界预执行 maps 对齐；未修改继承证据按原范围复用，历史 map缺失不得回填。migration-head 断言维护不称其旧案例已执行。
- [x] Ruff check/format --check只对变更 Python；git diff --check、严格 OpenSpec、guidance 检查。没有源码变化不重跑或重建。
- [x] 独立 SPEC，然后 QUALITY（SPEC已Ready；新建QUALITY代理多次返回thread limit，改由未参与BC05实现/SPEC的旧审计代理接收独立新任务，如实记录代理复用）；真实问题修复后只做必要重新资格，不在未解决审查时进入 BC06。
- [x] 记录当前能力/精确限制；BC05 actual installed spine不能替代 BC10 normal production startup/full combined gate、完整 crash qualification。默认关闭 flags，无 operator activation/ECS部署。
- [x] 源码提交并核对 blobs 后勾选 OpenSpec5.1–5.4，提交文档 receipt；继续 BC06。不得 push/merge/publish。

## 计划自审

Fair shared scheduling：commonpolicy接 B/C/ticket，持久turn与eligibleFIFO见Task2；serial实际单槽无等待死锁见Task1–2；reserved标准profile/单个真实节点见配置和Task3；physicalSTOP/unknown账本见原接口与Task3；兼容性见feature gate/sessioncapability/原daemon与ticket处理；实际安装源码与证据见Task1/4。原 fleet_probe 是历史示意，不是执行 fixture。未实现接口不作为已完成事实。

本地验收结论：SPEC与QUALITY均Ready，质量审查未发现可行动问题。QUALITY代理因新建thread limit复用未参与BC05实现/SPEC的旧审计代理，本记录不称新鲜代理。原计划所有边界结论以实际acceptance范围为准：控制反向只证明inactive SQL authority拒绝，不称真实revocation/物理STOP；CLI/fullentrypoint及BC06–10仍待完成。
