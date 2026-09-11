# Agent Runtime 设计方案与系统能力边界调研

- 日期：2026-09-11
- 性质：调研 / 设计笔记（未实施的方案文档）
- 目标：在 DeerFlow 基础上构建个人通用 agent（可调 tool / skill / MCP，可换
  runtime），扩展为办公助手，并评估企业级化路径。

---

## 0. 结论摘要

1. **Tool / Skill / MCP 调用是现成的**，单点聚合、扩展即插即用，不需要新建框架。
2. **"换 runtime"要拆成三层看**：模型后端（已有，纯配置）、外部 agent 引擎
   （已有 ACP 通道）、顶层图工厂（缺一个显式的 `AgentRuntime` 抽象，接缝已存在）。
3. **执行核能力密度高、安全内核达到企业单租户水准**；边界集中在组织层
   （无 team/租户）、知识层（无内建 RAG）和运营层（无指标/限流/审计表）。
4. **个人版 / 团队版 / 企业版可以是同一套代码的三个配置档位**，核心零分叉；
   企业缺口大多可用自带 extension 机制外挂补齐。

---

## 1. 现状架构盘点：可复用的接缝

### 1.1 模型层（层次 A：Model Runtime）—— 已完全可插拔

- 注册表 = `config.yaml` 的 `models:` 列表，每项 `use: <类路径>`（如
  `langchain_openai:ChatOpenAI`），由 `create_chat_model()` 工厂反射实例化：
  `backend/packages/harness/deerflow/models/factory.py:174`、
  `deerflow/reflection/resolvers.py`。
- 整个 agent 栈只对 LangChain `BaseChatModel` 接口说话。
- 已适配后端：`patched_openai`（OpenAI 兼容网关）、`patched_deepseek`
  （**DeepSeek / Z.AI GLM / Kimi / Doubao 都走这一个类**）、Anthropic、Gemini、
  Ollama、`vllm_provider.VllmChatModel`（私有化）、`mindie_provider.MindIEChatModel`
  （华为 MindIE 私有化）、`openai_codex_provider.CodexChatModel`
  （OAuth 直连 ChatGPT Codex Responses API——"外部引擎包成模型"的现成先例）。
- 按任务路由已完备：lead、每个 subagent（`subagents/config.py:27` 的
  `model: 'inherit'` 默认继承）、工具性模型（title / summarization /
  verification / goal evaluator）均可独立配模型。
- 能力标记已有：`supports_thinking` / `supports_vision` / `context_window` /
  `pricing`，以及 `when_thinking_enabled/disabled` 条件参数合并。
- 配方见 `config.example.yaml:120-694`（每厂商注释）。

### 1.2 工具 / Skill / MCP 聚合—— 单点、即插即用

- 工具总装单点：`deerflow/tools/tools.py:59` `get_available_tools()`，
  合并 内置工具 + config 声明工具 + MCP 工具 + ACP 外部 agent 工具，按名去重。
- 新工具只需实现 LangChain `BaseTool`；异步工具自动获得同步包装
  （`tools/sync.py`）。
- MCP：`deerflow/mcp/`（stdio/SSE/HTTP 三 transport，OAuth，**每用户独立凭证**
  `user_scoped_auth.py` 缺失即拒绝，deferred loading 按需暴露 schema，
  持久化长任务运行时：租约 / 取消栅栏 / 通知重试 / dead letter）。
  服务器清单在 `extensions_config.json`，运行时可经 Gateway API 修改。
- Skill 三重暴露：prompt 索引（`skills/describe.py`）+ `/slash` 激活中间件
  （`agents/middlewares/skill_activation_middleware.py`）+ `allowed_tools`
  策略中间件。skill 安装有静态安全扫描（`skills/skillscan/`，zip 炸弹 /
  路径穿越 / 危险代码，CRITICAL 阻断安装）和质量审查器
  （`skills/public/skill-reviewer/`）。

### 1.3 外部 agent 引擎通道（层次 B：Engine Runtime）—— 已有

- ACP（Agent Client Protocol）：`tools/builtins/invoke_acp_agent_tool.py` +
  `config/acp_config.py`。Codex CLI、Claude Code 均有现成 adapter
  （`config.example.yaml:1644-1677` 的 `@zed-industries/codex-acp` 配方）。
- **关键先例**：ACP 会话会把 DeerFlow 自己的 MCP servers 转发给外部引擎，
  即外部 runtime 也能吃到同一套 MCP 工具面。

### 1.4 顶层图工厂接缝（层次 C）—— 唯一真正要新建的抽象

- Gateway 只假设 `factory(config=...) -> graph | LeadAgentAssembly`
  （`unwrap_agent_graph` 兼容两者）；当前写死在
  `backend/app/gateway/services.py:584` `resolve_agent_factory()` 返回
  `assemble_lead_agent`。
- subagent 侧由 `SubagentExecutor._create_agent()` 构建，同样吃
  `create_chat_model` + 中间件链。

### 1.5 仓库自带的插拔模式（新抽象应照抄的模板）

`SandboxProvider` 是最干净的范式（`deerflow/sandbox/sandbox_provider.py:14`）：
ABC + capability flags + 配置 `use:` 类路径 + `resolve_class` 反射 + 线程安全
单例 + 注入钩子。同一模式在 memory 后端、guardrail provider、checkpointer、
store、tracing、authz 上重复多次。**runtime 抽象应复用此风格，不发明新机制。**

---

## 2. 设计方案：三层 Runtime

### 2.1 层次 A：Model Runtime —— 已有，只补 "capability manifest"

GLM / DeepSeek 纯配置接入，零代码。增强项：把散落的能力标记规范化为显式
manifest，供上层按能力裁剪 prompt 与工具注入，并在模型列表 API
（`app/gateway/routers/models.py`）暴露。

```yaml
models:
  - name: glm-4.7
    use: deerflow.models.patched_deepseek.PatchedChatDeepSeek   # OpenAI 兼容
    model: glm-4.7
    base_url: https://open.bigmodel.cn/api/paas/v4
    api_key: ${ZHIPU_API_KEY}
    supports_thinking: true
    context_window: 200000
  - name: codex
    use: deerflow.models.openai_codex_provider.CodexChatModel   # 外部引擎包成模型
```

### 2.2 层次 B：Engine Runtime —— 已有，补状态/事件归一化

把 Codex CLI 等整个外部 agent 进程当可委托执行器（ACP 路线）：

- **工具协议统一**：自定义工具若要给外部引擎用，发布为 MCP server
  （仓库已把 DeerFlow MCP 转发进 ACP session）。
- **会话连续性**：外部引擎无 checkpoint；ACP adapter 需把引擎消息流归一化
  映射回 `ToolMessage` / 统一事件协议（StreamBridge 已假设事件统一），
  这是该层唯一要写的实质代码。

### 2.3 层次 C：Agent Runtime 抽象 —— 核心新设计（仿 SandboxProvider）

在 harness 层新增（建议 `deerflow/runtimes/`）：

```python
class AgentRuntime(ABC):
    """一个完整 agent 执行栈：模型 + 工具面 + 图结构。"""
    capabilities: RuntimeCapabilities   # vision / thinking / subagent / checkpoint 支持

    @abstractmethod
    def build_graph(self, config: RunnableConfig, app_config: AppConfig) -> CompiledStateGraph | AgentAssembly: ...

    @abstractmethod
    async def normalize_event(self, raw) -> StreamEvent: ...  # 事件流归一到统一协议
```

接入方式复刻 sandbox：

- config 新增 `runtime.use: <类路径>`（默认
  `deerflow.agents.lead_agent.agent.DefaultLeadRuntime`，即包一层
  `assemble_lead_agent`，存量行为零变化）；
- `resolve_agent_factory()` 改为读配置返回对应 runtime，单例缓存；
- 事件归一化协议是核心资产：新 runtime 只要吐归一化事件流，前端 / SSE /
  持久化 / IM 渠道全部无感。

### 2.4 横切关注点

- **能力协商**：不同 runtime 能力不同（vision / thinking / 工具调用格式 /
  上下文窗口），capability manifest 驱动 prompt 与工具注入裁剪。
- **会话连续性**：外部引擎 runtime 必须实现 checkpoint 语义的映射（至少
  run 级恢复），否则中断/恢复/审计降级。
- **路由策略**：lead 用强模型、subagent / 工具性任务用便宜模型——机制已有，
  只需在 runtime profile 里声明默认路由。

---

## 3. 系统能力与边界全景

### 3.1 能力图谱

**Agent 执行核（最强）**：LangGraph `create_agent` + 20+ 中间件链
（动态上下文/记忆注入、Skill 激活与工具策略、摘要压缩、计划模式、token
统计与预算、循环检测、标题、视图图像、MCP 路由、防重复调用、护栏/鉴权、
澄清）。HITL 内建：澄清支持 `free_text / single_choice / choice_with_other /
form`，表单字段覆盖文本/数字/下拉/多选/日期，可经 IM 往返。每线程隔离 +
双模式 checkpoint（full/delta），事件流 LangGraph 兼容 SSE。

**模型接入**：见 §1.1；配方级支持国内外主流与私有化（vLLM / MindIE）。

**工具生态**：单点聚合（§1.2）；社区工具 10+ 搜索/抓取 provider、Playwright
浏览器自动化、RAGFlow 知识检索，统一 SSRF 防护
（`community/url_safety.py`：DNS 解析后拦私网/环回/云元数据地址）。

**沙箱**：7 个 provider——local（宿主子进程）、AIO（Docker）、E2B（云）、
OpenSandbox（云 microVM）、BoxLite（真 microVM，唯一有 CPU/内存配额）、
Tenki、K8s provisioner。统一虚拟路径（`/mnt/user-data/{workspace,uploads,outputs}`、
`/mnt/skills` 只读投影）、密钥环境清洗（`*KEY*/*TOKEN*` 类变量默认剔除）、
输出脱敏。**local 不是隔离边界（代码自认），默认禁 bash；真隔离必须容器/VM。**

**记忆**：按 `(agent, user)` 分桶、**天然跨线程共享**；run 结束后异步 LLM
抽取 facts；DeerMem 后端有置信度 / 时效衰减 / 纠错事实保证注入 / BM25 检索；
5 后端可换（deermem/mem0/honcho/openviking/noop），注入预算 2000 tokens。

**Subagent**：全功能子代理（独立模型/中间件链）、容量控制（默认每 run 6 个，
钳 1–50；并发 1–64）、durable batch、防重复委派账本；委派任务条件走
"untrusted 通道"并做标签中和（`subagents/report_contract.py`）。

**安全治理**：
- 认证（强）：OIDC SSO（discovery/JWKS/JIT 开通 + 邮箱域限制）、GitHub
  OAuth、PAT（6 scope + 默认拒绝路由边界）、CSRF、登录限流锁定。
- 授权（强）：可插拔 fail-closed RBAC（`authz/runtime.py`），管五类资源
  （tools/models/skills/sandbox/mcp_servers/routes），**每次工具调用运行时
  二次校验**（`authz/adapter.py`），独立 guardrail 层（pre-tool-call
  allow/deny，fail_closed 默认开）。
- 隔离（中）：per-user 文件租界（`config/paths.py`）、DB 行级 user_id、
  每用户 MCP 凭证、skill 白名单强制进沙箱。
- 输入防御：框架标签 HTML 转义与边界帧化、子代理报告 untrusted 通道、
  tool receipt 防篡改校验、read-before-write 版本门。

**触达与交互**：9 个 IM 渠道（飞书/Slack/Telegram/Discord/钉钉/企微/微信/
Nostr/GitHub webhook），每渠道 run policy（is_interactive / 递归上限 /
凭证 provider）；确定性线程映射、忙时追问缓冲、文件双向传输。前端：流式
渲染、HITL 表单、浏览器会话实时接管、语音输入（仅 ASR）、web 预览 iframe、
i18n 中英。

**运营**：定时任务（持久化队列 / 多实例租约 fencing / 幂等派发）、成本账单
console（`/console/usage` 按用户×模型出 token 与费用）、Helm chart
（gateway + 前端 + nginx + Postgres + Redis + K8s 沙箱 provisioner + `$VAR`
密钥注入，兼容 Vault/ExternalSecret）。

**防失控数字**：递归钳 `max_recursion_limit`（默认 1000）；token 预算
per-run 200k（80% 警告，到顶剥 tool_calls 自然停，stop reason
`budget_capped`）；循环检测相同调用 3 警 / 5 停；bash 超时 600s；
write_file 单次 80KB；上传 10 文件 × 50MB。哲学：**保护机制让 run 自然
终态化，不抛异常硬杀**。

### 3.2 硬边界（架构级，配置改不掉）

1. **无组织层**：user 是最大租户单位；无 workspace/team/org 实体、无按租户
   DB/加密/配额分离。多团队共享部署无数据边界。
2. **无内建 RAG**：全仓库无 embedding/向量库；知识检索靠外部（RAGFlow 工具、
   MCP、OpenViking）。文档理解 = markitdown / pymupdf4llm 转 markdown 后
   文件工具读取。
3. **无跨线程 agent 协作**：subagent 生命周期绑定单次 run；跨线程共享状态
   只有记忆。
4. **单 worker 拥有会话态**：浏览器会话、暖池沙箱仅在单进程内存；gateway
   多副本不成熟（chart 默认 `replicas: 1`，跨 pod cancel/dedup 依赖未完成的
   run-control 工作，参见 issue #3948）。调度器与沙箱所有权已有 Redis/
   Postgres 租约协调，浏览器/暖池没有。
5. **local 沙箱永远不是隔离**（设计立场：要么接受，要么换 provider）。
6. **harness/app 依赖防火墙**（`tests/test_harness_boundary.py` CI 强制）。
7. **无 TTS / 语音输出**、无桌面级 computer use（Playwright 浏览器是上限）、
   i18n 仅中英。
8. **已知隔离旁路**：PVC 挂载的 provisioner 部署绕过禁用 skill 隔离（等动态
   PVC 物化）；宿主 bash provider 可绕过虚拟路径契约。

### 3.3 软缺口（不改架构可补）

1. 指标监控 + 限流（`backend/docs/TODO.md` 已列；无 Prometheus、无按用户
   速率限制）。
2. 审计日志表（run 事件可落库，但管理员/配置/凭证变更无 append-only 审计）。
3. 按用户配额（预算 per-run，拦不住"一人无限开 run"）。
4. BYOK（模型 key 全部署共享；可照抄 MCP `user_scoped_auth.py` 模式）。
5. RAG（工具面与护栏已支持，做成 `knowledge_search` 类工具即可）。
6. 用户/角色管理 API 与 RBAC UI（角色目前只在 config.yaml，文件系统持有）。
7. LDAP / SAML / MFA / SCIM；更多上传格式；更多语言。

---

## 4. 企业级八维度评估

| 维度 | 评级 | 要点 |
|---|---|---|
| 认证 | **强** | OIDC/GitHub/PAT/CSRF/限流；缺 LDAP/SAML/MFA/SCIM |
| 授权/RBAC | **强** | 可插拔 fail-closed，五类资源 + 调用级校验；角色单标量、无 IdP 组映射 |
| 多租户隔离 | **中** | 严格 per-user 隔离 + 每用户 MCP 凭证；**用户之上无组织实体** |
| 审计合规 | **中** | run 事件落库、guardrail 拒绝事件、密钥脱敏；**无管理操作审计表、无 PII/保留期治理** |
| 可观测/成本 | **中** | LangSmith/Langfuse/Monocle(OTel)；按用户×模型成本账单现成；**无 Prometheus、无按人配额** |
| 部署/HA | **中** | 正经 Helm、$VAR 密钥、多实例调度租约、run 所有权心跳；**多副本 gateway 未支持**、捆绑 DB 单实例 |
| 模型治理 | **中** | `model:use` RBAC、vLLM/MindIE 私有化；**共享 key、无 BYOK、无按人花费配额** |
| 管理面 | **中** | MCP/skill/扩展 admin API、skill 供应链审查；**无用户/角色管理 API、config.yaml 仅文件持有** |

**企业化缺口按优先级**（1–3 可用 extension 机制外挂，不碰核心）：

1. 管理员审计日志（task_lifecycle + observer + router 的 extension）。
2. 按用户配额/限流（extension 中间件 + RunRow 聚合）。
3. SSO 组→角色映射（改 `auth/oidc.py` + `user_provisioning.py` 的 JIT 逻辑，改动小）。
4. Gateway 多副本（真正的 HA 硬缺口，先跟踪上游 run-control 进展）。
5. 数据面运维（外接托管 PG/Redis、备份、事件/检查点保留期 TTL）。
6. 用户/角色管理 API + RBAC UI。
7. BYOK / 每用户模型凭证。

**总体判断**：单组织、SSO、私有模型、按人隔离的企业部署**现在即可行**，
前提是接受 RBAC/配置由文件管理、审计与配额用 extension 自建；多团队、
多副本 HA、完整合规需要组织层数据模型与 run-control 多 pod 支持。

---

## 5. 个人版 → 办公助手 → 企业版：一套抽象，三个 profile

| | 个人助手 | 团队版 | 企业版 |
|---|---|---|---|
| 形态 | 嵌入式 `DeerFlowClient`（`deerflow/client.py:130`）/ 单机 compose | compose prod | Helm + K8s |
| 数据 | sqlite + 本地沙箱 | Postgres + Docker 沙箱 | 外接托管 PG/Redis + K8s provisioner 沙箱 |
| 身份 | 本地登录或关认证 | OIDC SSO | OIDC + 组→角色映射 + PAT |
| 模型 | GLM/DeepSeek 云端 API | + Codex/多模型路由 | + vLLM/MindIE 私有化，`model:use` RBAC |
| 治理 | 无 | admin 门禁 API | + 审计 extension + 配额 extension + skill 审查流 |

**办公助手的现成骨架**：文件理解（上传即转 markdown：PDF/PPT/Excel/Word）、
定时任务（non-interactive 执行）、IM 触达（5+ 平台）、记忆。缺口只有：
文档写回类 skill（沙箱 Python + python-docx/openpyxl/python-pptx，做成 2–3
个 skill）与日历/邮件/网盘 MCP。

**架构原则**：办公能力 100% 走 skill + MCP，个人差异走 memory +
per-user skills（`skills/custom/`），harness 核心一行不动。`AgentRuntime`
抽象与企业化正交：runtime 换"执行栈"，企业层挂 extension 与授权 provider
两条已有插拔轴，互不干扰。

---

## 6. 落地路线图

1. **阶段 0（零代码）**：纯配置接入 GLM/DeepSeek，`make dev` 跑通，确认
   thinking/vision 开关行为。
2. **阶段 1（小）**：capability flags 规范化为 manifest；`routers/models.py`
   按能力过滤暴露给前端。
3. **阶段 2（核心）**：`AgentRuntime` 抽象 + `runtime.use` 注册，默认实现包
   `assemble_lead_agent`，接缝 `resolve_agent_factory`。
4. **阶段 3（双线并行）**：
   - 功能线：办公 skill 包（文档写回/周报/纪要）+ 日历邮件 MCP；
   - 企业线：审计 extension（纯外挂，最快出活）+ OIDC 组映射。
5. **阶段 4**：按用户配额 extension；上 K8s 前先评估 gateway 多副本上游进展
   （唯一建议"先看上游再做"的项）。

---

## 7. 附录：关键文件索引

| 主题 | 文件 |
|---|---|
| 图工厂/接缝 | `backend/packages/harness/deerflow/agents/lead_agent/agent.py`、`backend/app/gateway/services.py:584` |
| 模型工厂 | `backend/packages/harness/deerflow/models/factory.py`、`models/openai_codex_provider.py`、`models/vllm_provider.py`、`models/mindie_provider.py` |
| 工具聚合 | `backend/packages/harness/deerflow/tools/tools.py` |
| MCP | `backend/packages/harness/deerflow/mcp/`（client/cache/user_scoped_auth/tasks） |
| Skills | `deerflow/skills/`（describe/types/projection/skillscan/review）、`agents/middlewares/skill_activation_middleware.py` |
| ACP | `deerflow/tools/builtins/invoke_acp_agent_tool.py`、`config/acp_config.py` |
| 沙箱 | `deerflow/sandbox/`（sandbox_provider/tools/security/env_policy/local）、`community/{aio,e2b,opensandbox,boxlite,tenki}_sandbox/` |
| 记忆 | `deerflow/agents/memory/`（manager/backends）、`agents/middlewares/memory_middleware.py` |
| 认证/授权 | `backend/app/gateway/auth_middleware.py`、`app/gateway/auth/`、`deerflow/authz/`、`deerflow/guardrails/` |
| IM 渠道 | `backend/app/channels/`（manager/base/run_policy/connection_identity） |
| 运行时防护 | `deerflow/agents/middlewares/{token_budget,loop_detection,input_sanitization,tool_result_sanitization}_*.py`、`subagents/report_contract.py` |
| 部署 | `deploy/helm/deer-flow/`、`docker/`、`backend/app/scheduler/service.py` |
| 扩展机制 | `backend/packages/extension-api/deerflow_extension_api/contracts.py`、`deerflow/extensions/`（loader/manager/stack） |
| 路线图 | `backend/docs/TODO.md`、`CHANGELOG.md` |
