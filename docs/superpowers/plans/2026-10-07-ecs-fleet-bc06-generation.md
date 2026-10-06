# BC06 Generation and User Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 用户取消或修改目标时，旧 generation 不再自动继续；awaited 取消传播、detached 保留；用户 run 与旧目标不双写，显式继续可使用已接受旧结果而不重跑子 job。

**Architecture:** 在原 RunAdmissionUnitOfWork 和 thread-operation durable reservation 中加入中立可选 participant，由 Gateway 的可信来源工厂选择 Fleet task participant。复用 goal advisory → ordered task → thread/binding 锁序、原 continuation 和取消/STOP 路径。等待父执行已 STOP/release 才在实际操作事务内改变 generation；已 assigned execution 保留原 generation，先持久 cancel，确切 STOP 后再 supersede。

**Tech Stack:** 原 FastAPI Gateway、RunManager、SQLAlchemy/PostgreSQL、LangGraph checkpoint/workspace、optional ecs-fleet package。

---

基线：BC05 source `c153ad1951fcf1f8edaa5f5a4a0483d9228c3346`，receipt `33e3fb99`，feature/personal-agent-ecs。B、C01–12、BC01–05本地验收完成；BC06–10仍必需。已授权执行及子代理工作流，不另询问执行选择。fresh代理创建返回thread limit时复用未参与该实现的审计代理，并如实记录。

## 实际源码审计与职责

- `backend/app/fleet/execution.py`: `fleet_before_thread_guard` 已锁 goal advisory/ordered tasks；`fleet_thread_admission_guard` 现在对 waiting 人类消息和 checkpoint/delete 只拒绝。`FleetRunAdmission.validate_paused_resume` 只授权 keyed Command.resume/paused source，保留它的原验证，不放宽为 waiting-final。
- `backend/packages/harness/deerflow/persistence/run/sql.py`: 原准入事务顺序 before-thread → thread advisory → binding/guard → participant.prepare → core INSERT → participant.insert。`check_thread_admission` 是另一独立事务，只作 early guard；不得在此先提交 generation 再创建用户 run。
- `backend/packages/harness/deerflow/runtime/execution/contracts.py`: ExecutionPlan 目前把 participant 与 store_only 强绑定。保持 remote 必须 participant，但允许 local/thread mutation 的中立 participant。harness 禁止导入 Fleet。
- `backend/packages/harness/deerflow/runtime/runs/manager.py`: 原 `reserve_thread_operation` 必须可接收可信 participant，在同一 durable reservation 准入事务内改变目标；SQL/内存 store 无 participant时行为不变。
- Create `backend/app/fleet/task_admission.py`: owned task mutation、source receipt、事务参与者和可信操作工厂。它不另建 execution queue。
- `backend/app/gateway/services.py`, `backend/app/gateway/thread_admission.py`, `backend/app/gateway/routers/thread_runs.py`, `backend/app/gateway/routers/threads.py` （checkpoint mutation实际位于threads.py及services.py的原reservation wrapper）: 在真正 human run/checkpoint/delete reservation 接线；early gate仅观测。内部通知/scheduler不得获得 human supersession 权限。由服务端显式调用来源决定，不能信body.metadata/context。正常用户HTTP与可信IM人类消息应保留对应语义，不能因internal认证一刀切排除全部IM。
- `backend/app/gateway/routers/fleet_agent_tasks.py`: 原GET保留；增加同线程 owned task cancel 和 explicit resume 写入口，分别检查 runs:cancel / runs:create（需要中断活跃 run时还检查cancel权限）。请求显式携带 expected_generation 与稳定 idempotency key，禁止 stale请求作用于当前新generation。
- `backend/app/fleet/agent_control.py`, `backend/packages/ecs-fleet/deerflow_ecs_fleet/job_service.py`, `cancellation.py`: 复用原 assigned/unassigned cancellation；抽取原 job cancel 的锁内实现，使 awaited传播参加调用者同一事务，不能nested独立commit。
- `backend/packages/ecs-fleet/deerflow_ecs_fleet/persistence/models.py` 与 Create `migrations/versions/f0015_task_operation_receipts.py`: 私有持久 task-operation receipt，用原事务保存owned task/user/thread、operation/idempotency、source/target generation、accepted source point/checkpoint、旧group、admitted run等身份；唯一稳定operation key，重复不二次升generation。只保存受信身份，不保存客户端授权标记或secret。
- `backend/app/fleet/workspace.py`: `_is_stopped_resume_source` 原paused/automatic dispatched同generation规则保持；增加仅由可信新operation receipt授权的waiting-final source分支，精确核对旧task/group/point/checkpoint/STOP/release/新run/newgeneration，不授权任意终态workspace。
- Test create `backend/tests/fleet/test_bc06_fleet_agent_job_dependencies.py`；docs 当前计划、runtime/acceptance、README/backend guide、OpenSpec/progress/roadmap。

## 必须满足的原事务协议

```python
# 原UoW内的操作顺序（不是独立预提交）：
# 1. goal advisory + ordered tasks; 校验owned task与expected_generation。
# 2. 读取稳定operation receipt：相同请求复用；冲突拒绝。
# 3. old waiting source锁group/run/placement、按job ID锁awaited children，
#    最后锁node/reservation/attempt，确认原STOP/release/accepted source。
# 4. 必须无旧live/unknown writer；不能按run终态/lease失效释放。
# 5. 对exact immutable awaited links调用原locked cancel；detached不加入。
# 6. task进入paused并generation+=1，写operation receipt；旧group/link不改身份。
# 7. 同事务插入用户operation/run，复用原task的新generation（不能创建第二active task）。
# 8. 任何guard/insert/unique/权限失败一起rollback。
```

新 run 的 Fleet source只能来自原 accepted source，execution profile/binding保持host决定。旧generation automatic continuation继续使用原校验并拒绝；不得让它通过新用户 receipt。显式继续由用户 owned route准入，把旧已settled child result references送入原 bounded untrusted summary/graph input，不调用submit，不增加JobRow/child attempts，不伪造旧group dispatched到新generation。

```python
# admission-first处理必须保留原execution身份：
# unassigned: 原control取消queued placement/ticket，确认无live charge，再supersede。
# assigned: 原control持久cancel -> 等待原matching process STOP/release
#           -> 原操作UoW重验expected_generation后supersede。
# unknown/unconfirmed STOP: 拒绝新操作并继续charge；不得提前升generation。
```

明确任务取消：一次owned receipt锁定取消目标generation，停止自动continuation，传播exact awaited links，detached保留。已有assigned执行需要原cancel/STOP握手，不以task.state直接释放。重复取消/timeout/重启从持久receipt读取原结果；新generation不得受旧取消请求影响。

## Task 1: 一个实际主干 RED

- [ ] 读取相关module AGENTS与本计划；记录原函数/接口现状，必要的新接口仅在上述职责内实现，不扩散到BC07–09。
- [ ] 创建唯一主干 `test_bc06_user_message_supersedes_waiting_goal`。复用BC03 checkpoint_owner/owner_environment、BC02真实graph/checkpoint/workspace/yield/STOP helper，以及C12 login/c04 node_server原认证session HTTP。原host app复用native owner环境并挂原run/auth routes，实际生产install_fleet_ownership必须安装新接线；不得仅在fixture手塞factory。该组合不是normal create_app/lifespan startup，完整当前安装入口仍待BC10。先实际yield到waiting且STOP/release，在旧等待期间让一个awaited child经原completion/publication路径完成并accepted，保留另一个未settled child；再通过正常用户HTTP发新消息，记录同task generation、新core run/placement、旧group与未settled child原cancel。完成/取消的全部旧成员保留，旧coordinator不能创建新run；明确用户resume收编accepted结果引用，jobs/childattempts数不增加。不得用旧fleet_probe或硬编码observations。
- [x] 原主干RED选择器先运行，保存实际缺口（原409/缺写route/原guard拒绝）；fixture配置/连接错误或skip不算产品RED。

```bash
# feature backend cwd；由mode0600 carrier注入原native15436 PG，不打印URI
/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend/.venv/bin/python -m pytest tests/fleet/test_bc06_fleet_agent_job_dependencies.py::test_bc06_user_message_supersedes_waiting_goal -q -s
```

每次 `.local/fleet-evidence/bc06/<attempt>/` 执行前保存当前参与文件SHA/Git blobs、argv/cwd；后保存自然exit、实际SQL/HTTP/graph/checkpoint/workspace/cleanup。保留原失败。只能drop本次receipt确认的schema，不碰其他PG/容器。native helper中的recorded STOP不能说成新物理容器执行；若实际新增execution runner/installed bytes需要qualification，则构建当前必要image和执行当前主干，不能用旧image冒充新源码。无runtimebyte变化不重建。

## Task 2: 原接口实现与同主干 GREEN

- [ ] 中立participant接线允许thread operation参加原UoW；remote store_only仍必须完整participant，Local无participant行为不变。
- [ ] 实现owned task operations、stable receipts、精确awaited传播与可信human来源接线；保留内部notification/scheduler原reject semantics。
- [ ] 实现waiting新用户run同事务newgeneration复用原task；explicit resume newreceipt授权accepted旧final source和原bounded结果引用。
- [ ] 实现assigned-first取消握手，不提前改generation，不改变STOP/reconciliation校验。
- [ ] 运行同主干直到必要实际修复后的GREEN；先主干通，再执行Task3。成功后不因新增boundary或文档重复运行。

## Task 3: 至多一个集中必要边界

- [ ] 主干通过后创建 `test_bc06_generation_cancel_and_operation_races`：在原SQL UoW真实barrier中观察cancel-before-admit与admit-before-cancel，后者集中检查unassigned及assigned STOP前/后；STOP前generation/current_run保持，charge不释放；原确认STOP后才user supersede。unknown同邻居保持拒绝与charge。
- [ ] 同函数检查stable取消重复、stale generation拒绝、awaited传播/detached保留；复用BC04实际MCP notification服务证明waiting通知不能借human participant抢线程。通过正常rollback/delete durable reservation观测generation更新与准入失败rollback，不创建独立矩阵案例。
- [ ] 精确验证workspace newreceipt授权/旧automaticreceipt不能跨generation；保持原permission/ownedroute规则。源校验负向集中此boundary，不扩大成旧Fleet/C suite。

```bash
/Users/wenbinwang/.codex/worktrees/deerflow2/personal-agent-ecs/backend/.venv/bin/python -m pytest tests/fleet/test_bc06_fleet_agent_job_dependencies.py::test_bc06_generation_cancel_and_operation_races -q -s
```

## Task 4: 冻结、审查与交付

- [ ] 改动Python Ruff check/format --check；git diff --check、严格OpenSpec、guidance检查。migration-head旧assertions仅维护，不声称旧案例已运行。
- [ ] 对齐最终source/current资格，保留main原prefix与boundary独立map，不回填历史。已成功案例只因runtime源码变化/失败/实际未解决资格才重跑。
- [ ] 独立SPEC→QUALITY；修复实际审查问题，不能用作者自审替代。若tool无法freshspawn，记录独立既有agent新任务的复用事实。
- [ ] source commit并核对reviewed blobs，然后更新OpenSpec6.1–6.4及文档receipt。BC07–10仍必需，无push/merge/operator激活/ECS部署。

## 需求对照

旧generation不能automatic继续：原coordinator校验+actual operation同TXgeneration变更。用户run不双写：原thread reservation和liveSTOP握手。awaited取消/detached保留：immutable scoped links+原cancel锁。waiting消息/rollback/delete：实际操作participant而非earlyguard预提交。显式继续旧结果：newownedreceipt/source验证+boundedacceptedrefs/nochildresubmit。cancel/admission竞争与重复/重启：持久receipt及原UoW barrier/重建服务。一个主干与一个集中边界；BC07预算/崩溃恢复、BC08scheduler、BC09产品UI、BC10fullcombinedgate不借本阶段宣布完成。

## 实际执行记录（尚未验收）

main-red-01自然退出1，自有schema receipt确认created/dropped。失败是继承fixture启用continuations后缺reserved_job_profile，在原SharedAdmissionPolicy中KeyError；没有生产源码变更，不算BC06产品RED。先修本阶段fixture显式配置，同一个主干继续建立真实HTTP/准入缺口。主干scope为原native graph/checkpoint/workspace/STOP及原handler/session authenticatedTCP，不称normal Gateway startup/installed image；生产factory必须经原ownership installer接线，最终当前完整安装入口仍由BC10验证。

Further inherited fixture failures remain preserved: main-red-02 subprocess import path; main-red-03 private payload reserved profile; main-red-04 original stream bridge missing (authenticated HTTP503). All naturally exited1 and dropped their owned schema. Attempt04 actually reached native Python child exit0, original graph/checkpoint/workspace STOP acknowledgment, released reservation and waiting SQL state, but never reached user admission; none is BC06 product RED or GREEN.

main-red-05 remained a fixture failure (abstract StreamBridge construction). main-red-06 then established the actual product RED: naturally exited1; authenticated session POST to the original thread_runs handler returned409 `Thread has unfinished remote execution or recovery` despite native parent exit0, accepted workspace, core success/task waiting, matching STOP and released reservation. Its owned schema was dropped. This proves only the waiting human-run admission guard gap, not generation transition or explicit resume. Task2 implementation now proceeds; no GREEN/boundary/review acceptance is claimed.

主干前提修正：human supersession必须取消尚未settled awaited，不能取消后为了结果refs又把同job改success。因此一个child在新消息前实际accepted，另一个在新消息TX取消；显式继续收编已accepted refs并包含cancelled成员，以当前新human run accepted source继续，不回退旧checkpoint，不重新提交job。此调整不撤销main-red-06的原waiting guard缺口证明，也不要求机械重复已证明RED。

Task2 initial waiting-message patch was initially rejected by automatic approval review because of authenticated admission/cancellation/data-integrity scope. The complete exact four-file candidate was prepared outside the feature checkout and independently reviewed; nonwaiting routing, generic callback compatibility, job ownership and source-state checks were corrected. Reassessment approved the same logical patch (SHA256 a66a31056b62cd083779fb0d014e9dcbc958f62cea50e1179f877540259237ee), which the author applied. This was not split to bypass rejection. Only the first waiting-message path is present; complete task operations, explicit continuation and main/boundary qualification remain pending.

The owned explicit-resume action was separately rejected before writing. Its complete six-file candidate was independently reviewed and narrowed to fresh waiting/succeeded final sources, retaining original keyed human-answer paths; exact owner/backend/generation receipt reuse and fixed migration uniqueness were added. Reassessment approved and applied the same logical patch SHA256 bdf0f067cbb401773f39406bc347876d3c776eab2889fd1d928270a81ce1d72e. The author is now qualifying the complete main path; other task operations and the sole boundary remain pending. No GREEN or acceptance is recorded yet.

## 原接口核对后的事务边界

原 `reserve_thread_operation` 准入事务先提交 durable reservation，再在 reservation 持有期间执行实际 checkpoint mutation/delete；两者不是同一事务。BC06 participant 必须保证 generation、awaited cancellation、operation receipt 与 reservation 准入同事务成功或回滚。准入之后的实际 mutation 失败不能宣称回滚已提交的 supersession。集中边界需观察实际成功/失败以及留下的持久 intent/source 状态，继续保持原 writer/recovery fence；后续 BC07–10 的恢复要求仍保留。新 checkpoint 没有对应 accepted remote workspace 时，必须明确 recovery_required 或原 binding 拒绝，不能捏造 accepted source。任务当前 expected_generation 与历史 workspace/placement source generation 分别校验，不改历史身份使两者假装相同。

assigned task cancel 在原 controlled paused publication 前，仅保存 requested operation receipt 和原 run cancel request，保留 task generation/state/current_run/cancel_requested_at，以允许原 publication、STOP/release 完成。确切停止后才完成 terminal/supersession；unknown charge 保留。trusted IM human authority 只来自绑定 owner 的真实人类 ingress，普通 internal token 或客户端 header/metadata 不够；最终具体实现仍待完整候选审阅。

main-green-02 自然退出0，1passed8.11s；Root 核对 parent/新 human run 的原 checkpoint/workspace/STOP/release、一个 accepted child 与一个 cancelled child，以及 HTTP/SQL explicit resume 的 current source/result references。该历史主干没有执行 explicit resumed run，故不能代表完整 BC06 主干。作者已在同一主干补实际 resumed execution，剩余 Task2 源码完整后进行一次必要当前源码重验；集中边界仍未执行。审阅记录 `.local/fleet-evidence/bc06/main-green-02/root-partial-main-audit.json`，历史 source-map 不回填。

### 未 assigned 的连续人类操作来源

一次已准入的新消息尚未 assigned，又收到用户修改时，原 unassigned cancel → supersede 要求仍适用。不能永久拒绝该真实分支并宣称已满足目标。当前 run 没有新 attempt/workspace，实际 accepted source 可能属于更早 generation；新 receipt 的 expected/source task generation 与实际 source-point generation 必须分开。资格必须由当前被取消 run 的原 core/placement cancellation、无 attempt/charge、冻结 LaunchSpec、原 admitted operation receipt（精确 task/user/thread/current generation/current run/source point/checkpoint）以及该 source 本身的完整原 accepted request/manifest/checkpoint/digest/STOP/release 共同证明。必要的持久 predecessor/source-point-generation 身份属于同一 BC06 operation receipt 迁移。消费原 stopped source 只能使用这条具体受信来源链，不能宽泛允许任意 olderpoint；原 paused Command 和 automatic continuation 的资格范围保持。该接线仍待完整候选、同主干及集中边界验证，不是已实现/通过声明。

The remaining complete 19-file logical patch was approved by automatic review and applied unchanged (SHA256 `0d8a8acb67574a79a917a217a930ab03ff9e9872f6e614572a71088ecffcec2f`). Root independently checked all 19 actual production files against the immutable reviewed map; every SHA matched. Application evidence is in `.local/fleet-evidence/bc06/remaining-candidate-review/apply-result.json`, with independent verification in `root-applied-source-audit.json`. This is source/application verification, not formal SPEC/QUALITY acceptance.

Current-source main-green-03 naturally exited1 and dropped its owned schema. Parent, accepted child, newer human execution and explicit-resume admission reached their checks, but the added resumed execution reused a helper that registered the same node twice, causing `fleet_nodes_pkey`. It does not qualify the complete main. The author is correcting that fixture minimally and rerunning the same sole main selector; the concentrated boundary and formal reviews remain pending. No test matrix or new Docker image is introduced.

main-green-04 is the first complete current-source main: natural exit0, 1passed12.53s, own schema created/dropped. Root verified all 419 pre-run source hashes against actual files and independently inspected the original assertions, HTTP200 receipts, SQL task reuse/generation transition, accepted-child manifest, and native explicit resumed execution. Parent, newer human run and explicit resumed run have original STOP timestamps and released reservations; the resumed checkpoint retains the newer human message and bounded old accepted manifest references, including the cancelled-member observation. Job and child-attempt counts remain2/1; old-generation automatic dispatch is rejected. The model is controlled, and this proves native/original-handler ownership composition only, not normal Gateway lifespan, installed image, ECS or live-model artifact reasoning. Root receipt: `.local/fleet-evidence/bc06/main-green-04/root-main-audit.json`. The historical failed and partial attempts remain unchanged. Only the concentrated Task3 boundary and subsequent formal reviews remain for this stage's qualification; no successful-main rerun is required for boundary append/docs alone.

boundary-01 naturally exited1 (10.87s); both entered owned schemas were dropped. Root inspected the actual cancellation-first barrier/SQL and HTTP observations: two awaited jobs cancelled, one genuinely submitted detached job stayed queued without cancel intent, repeat cancel reused its receipt, stale cancel returned409, create-only PAT returned403 and unsigned internal input returned409. This failed attempt qualifies only that passed prefix; it does not establish assigned STOP, neutral mutations, IM, unknown or later source-chain execution. Admit-first exposes an actual pre-read/admission race: automatic continuation commits queued after human observed waiting, and final strict human admission returns409. Existing safety fencing remains; the requested handoff is incomplete. A narrow typed-conflict rollback/STOP/re-admission candidate is under preparation; no source relaxation or acceptance is claimed. Root receipt `.local/fleet-evidence/bc06/boundary-01/root-partial-boundary-audit.json`.

The complete four-file race fix (SHA256 `b5158ee8cdecd820f0421ef2d0540a7ac564cfc4f3fb6896ce0fa2df567e1416`) passed independent candidate review and automatic approval, and was applied unchanged. Root checked all four applied hashes. It retries only the typed exact-owned-generation handoff after original admission rollback, through original STOP/permission/source revalidation, at most once. Neutral operations retry reservation admission, never the subsequent mutation.

Because production admission changed, main-green-05 performed the necessary current-source requalification: natural exit0, 1passed11.96s, own schema dropped. Root independently verified419 source hashes and all three actual native Agent runs with original STOP/releases, retained newer message and explicit accepted-result continuation. The main's existing import ordering was fixed before this necessary run; historical green04 bytes/maps remain unchanged. Scope remains original authenticated-handler/installed-ownership native composition, not normal startup/image/ECS. The same sole boundary selector is now being rerun at boundary-02; no boundary success or formal review acceptance is claimed. Root receipt `.local/fleet-evidence/bc06/main-green-05/root-main-audit.json`.
