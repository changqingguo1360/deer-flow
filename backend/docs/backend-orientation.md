## Project Overview

DeerFlow is a LangGraph-based AI super agent system with a full-stack architecture. The backend provides a "super agent" with sandbox execution, persistent memory, subagent delegation, and extensible tool integration - all operating in per-thread isolated environments.

**Architecture**:
- **Gateway API** (port 8001): REST API plus embedded LangGraph-compatible agent runtime
- **Frontend** (port 3000): Next.js web interface
- **Nginx** (port 2026): Unified reverse proxy entry point
- **Provisioner** (port 8002, optional in Docker dev): Started only when sandbox is configured for provisioner/Kubernetes mode

**Runtime**:
- `make dev`, Docker dev, and production all run the agent runtime in Gateway via `RunManager` + `run_agent()` + `StreamBridge` (`packages/harness/deerflow/runtime/`). Nginx exposes that runtime at `/api/langgraph/*` and rewrites it to Gateway's native `/api/*` routers.
- Gateway streams `write_file` and `str_replace` argument deltas in bounded batches for multi-mode `messages-tuple` consumers; single-mode message consumers retain the original per-chunk contract. Non-message frames flush pending batches, and `values` remains an optional complete-state snapshot rather than a prerequisite for batching.
- With `stream_subgraphs`, subgraph frames keep their namespace in the SSE event name (`values|<ns>`, LangGraph Platform style) instead of impersonating root frames — a delegated subagent inherits the parent checkpoint namespace, so publishing its `values` snapshot as bare `values` replaces the whole thread view in SDK clients (#4399). Root-only consumers (file-tool chunk batcher, subagent event persistence, LLM error-fallback detection) ignore namespaced frames. The web frontend does not request subgraph streaming; subtask progress rides root-namespace `task_*` custom events.
- Background subagent identity is deliberately split: the provider `tool_call_id` remains the correlation key for `ToolMessage`, `task_*` SSE events, persisted lifecycle events, frontend cards, and the public `ExtensionData.scope_id` contract (stored as `SubagentResult.external_task_id`), while `SubagentExecutor.execute_async()` generates a full server-side `execution_id` for `SubagentResult.task_id`, the process-wide registry, polling, cancellation, timeout handling, and cleanup. Provider IDs are not globally unique across parent runs, so they must never become registry ownership keys; scheduler closures retain their own `SubagentResult` rather than resolving ownership again through the mutable registry. Terminal subagent token usage travels in the current run's `ToolMessage.additional_kwargs` and is attributed from message state, never through a process-global provider-ID cache.
- Scheduled-task executions must reuse that same Gateway run lifecycle. The scheduler may decide *when* work runs, but it must dispatch through the existing run path rather than introducing a parallel execution stack. Scheduled launches pass `scheduler.recursion_limit` (default 1000, matching the web UI's `recursion_limit: 1000`, clamped by `max_recursion_limit`) via `launch_scheduled_thread_run`; the value is read from `get_app_config()` at dispatch, so a YAML edit applies to the next scheduled run without a Gateway restart.
- The background scheduler is single-instance by default. `scheduler.multi_instance=true` opts into lease-aware recovery across Gateway instances and requires shared Postgres, `run_ownership.heartbeat_enabled=true`, and `run_events.backend=db`; otherwise startup rejects the configuration. Live scheduled runs are preserved when a peer starts; expired launch claims return to the durable queue, expired run leases are atomically taken over, stale launch writes are fenced by lease ownership, and the Postgres advisory-locked budget makes `max_concurrent_runs` a shared global cap for `launching`/`running` rows.
- Long-running MCP work uses durable SQL task tracking, fenced polling/cancellation and idempotent result notification outside the Agent loop. Read [MCP integration contracts](../packages/harness/deerflow/mcp/AGENTS.md) before changing this shared runtime or its Fleet adapter.

- Long-running MCP work uses a separate durable task runtime (`McpTaskService` + `mcp_tasks`, lease-based recovery) rather than keeping remote task IDs or status polling inside the Agent loop; only submit remains Agent-visible, the database is the source of truth, and `ThreadState` receives only a bounded current-thread projection. Full contract (leases, cancellation fencing, delivery idempotency, management-tool exposure): [packages/harness/deerflow/mcp/AGENTS.md](../packages/harness/deerflow/mcp/AGENTS.md).
- MCP task notification retries, dead-lettering, and the cancel endpoint's worker-stopped 503 are part of that same contract — see [packages/harness/deerflow/mcp/AGENTS.md](../packages/harness/deerflow/mcp/AGENTS.md).
- Scheduled-task dispatch enforces at most one non-terminal occurrence per task through `uq_scheduled_task_run_active` (`task_id WHERE status IN ('queued','launching','running')`). `queued` is durable and survives restart; `launching` carries a short owner/expiry lease and is the only state that may call the normal Gateway launch path; `running` is associated with the durable run. Each occurrence also supplies a stable run-admission idempotency key, so a recovered launch retry reuses the same durable run. A reused-thread `ConflictError` moves `launching` back to `queued`, while non-conflict launch errors become terminal `failed`. Waiting rows do not consume `max_concurrent_runs`; the atomic queue claim enforces the budget. Repeated triggers coalesce on the one active row, and same-thread FIFO treats older `queued`, `launching`, and `running` rows as blockers. The task definition stays immutable for all three active states because queue admission, PATCH/resume, pause, and delete serialize on the parent task row before touching the occurrence row. Pause/delete atomically interrupt existing `queued` rows and reject `launching`/`running` rows; PATCH/resume reject every active state, and mutation errors advertise pause cancellation only for `queued` work. A manual trigger may queue and run while the parent schedule remains paused. Recovery and multi-instance reconciliation lock task/run pairs in deterministic task-id/run-id order and must reconstruct `run_id`, `started_at`, and the live error state before releasing the short launch claim. Launch/failure/timeout bookkeeping changes the occurrence and its parent task in one parent-first transaction so a peer cannot claim the released task between those writes. Queue timeout marks the occurrence failed and advances a scheduled occurrence so it cannot immediately requeue forever; repository write boundaries coerce serialized task timestamps before binding SQL `DateTime` fields.
- `extensions_config.json` is written at runtime by the Gateway (`PUT`/`PATCH /api/mcp/config`, the MCP enable switch, skill updates), so the production compose mounts it read-write while `config.yaml` stays `:ro`; Helm copies its ConfigMap seed into a writable home-volume directory before Gateway starts. Every read-modify-write holds both `extensions_config_write_lock` and the sidecar advisory `extensions_config_file_lock`, because the process-local lock alone loses updates across workers. Docker mounts the compose file as its own mount point, and Linux refuses `rename()` over a mount point with `EBUSY` even when the mount is writable — so `atomic_write_extensions_config` keeps the temp-file-plus-rename path and falls back to an in-place overwrite only on `EBUSY`. That fallback is deliberately non-atomic (a crash mid-write truncates the file); it exists because the alternative is a write that can never succeed, and only its first occurrence per target is logged at warning level. Any other `errno` still propagates. Pinned by `tests/test_compose_extensions_config_writable.py`, `tests/test_extensions_config_atomic_write.py`, and `tests/test_helm_extensions_config_writable.py`.

**Project Structure**:
```
deer-flow/
├── Makefile                    # Root commands (check, install, dev, stop)
├── config.yaml                 # Main application configuration
├── extensions_config.json      # MCP servers and skills configuration
├── backend/                    # Backend application (this directory)
│   ├── Makefile               # Backend-only commands (dev, gateway, lint)
│   ├── langgraph.json         # LangGraph Studio graph configuration
│   ├── packages/
│   │   ├── extension-api/     # public, host-independent extension contracts (import: deerflow_extension_api.*)
│   │   └── harness/           # deerflow-harness package (import: deerflow.*)
│   │       ├── pyproject.toml
│   │       └── deerflow/
│   │           ├── agents/            # LangGraph agent system
│   │           │   ├── lead_agent/    # Main agent (factory + system prompt)
│   │           │   ├── middlewares/   # middleware components (see Middleware Chain section)
│   │           │   ├── memory/        # Memory extraction, queue, prompts
│   │           │   └── thread_state.py # ThreadState schema
│   │           ├── sandbox/           # Sandbox execution system
│   │           │   ├── local/         # Local filesystem provider
│   │           │   ├── sandbox.py     # Abstract Sandbox interface
│   │           │   ├── tools.py       # bash, ls, read/write/str_replace
│   │           │   └── middleware.py  # Sandbox lifecycle management
│   │           ├── subagents/         # Subagent delegation system
│   │           │   ├── builtins/      # general-purpose, bash agents
│   │           │   ├── executor.py    # Background execution engine
│   │           │   └── registry.py    # Agent registry
│   │           ├── tools/builtins/    # Built-in tools (present_files, ask_clarification, view_image, review_skill_package)
│   │           ├── mcp/               # MCP integration (tools, cache, client)
│   │           ├── integrations/      # Managed first-party integration installers (e.g. Lark CLI skill pack)
│   │           ├── extensions/        # Python plugin loader, registry, placement, and isolation
│   │           ├── models/            # Model factory with thinking/vision support
│   │           ├── skills/            # Skills discovery, loading, parsing
│   │           ├── config/            # Configuration system (app, model, sandbox, tool, etc.)
│   │           ├── community/         # Community tools (search/fetch/scrape, image search, AIO sandbox)
│   │           ├── reflection/        # Dynamic module loading (resolve_variable, resolve_class)
│   │           ├── utils/             # Utilities (network, readability)
│   │           └── client.py          # Embedded Python client (DeerFlowClient)
│   ├── app/                   # Application layer (import: app.*)
│   │   ├── gateway/           # FastAPI Gateway API
│   │   │   ├── app.py         # FastAPI application
│   │   │   └── routers/       # FastAPI route modules (models, mcp, memory, skills, uploads, threads, artifacts, agents, suggestions, channels)
│   │   └── channels/          # IM platform integrations
│   ├── scripts/benchmark/       # Standalone reproducible backend benchmarks
│   ├── tests/                 # Test suite
│   └── docs/                  # Documentation
├── frontend/                   # Next.js frontend application
└── skills/                     # Agent skills directory
    ├── public/                # Public skills (committed)
    └── custom/                # Custom skills (gitignored)
```
