### Gateway API (`app/gateway/`)

FastAPI listens on port 8001; health: `GET /health` (liveness) and `GET /health/ready` (readiness; concurrently probes the ORM engine behind `database:` plus the effective LangGraph checkpointer/Store backend - the legacy `checkpointer:` section, otherwise derived from `database:`, resolved from the startup config snapshot recorded on `app.state` - beneath a single bounded deadline, with connection-opening probes serialized behind a strict per-process gate, 503 while either is unreachable or the startup backend cannot be resolved, `not_configured` for process-local backends such as `backend=memory`). Set `GATEWAY_ENABLE_DOCS=false` to disable the default `/docs`, `/redoc`, and `/openapi.json` endpoints.

Durable MCP notifications use internal Agent runs. Keep their trusted delivery instruction outside the user-input boundary, and frame serialized remote events as untrusted before model invocation. Strict thread existence/ownership admission dead-letters events whose task outlives its deleted chat instead of recreating the thread.

CORS is same-origin by default when requests enter through nginx on port 2026. Split-origin or port-forwarded browser clients must opt in with `GATEWAY_CORS_ORIGINS` (exact origins); Gateway `CORSMiddleware` and `CSRFMiddleware` both read that variable so browser CORS and auth-origin checks stay aligned. Those clients also need `CORS_EXPOSED_HEADERS` (`csrf_middleware.py`): run-creating routes return the run's id in `Content-Location`, which is not CORS-safelisted, so JS cannot read it unless it is exposed — and the LangGraph SDK resolves run metadata from that header alone, so withholding it breaks `useStream`'s `onCreated` and thread-gated actions.

Browser auth sessions are owned by `app.gateway.auth.session_cookie`. Login accepts a `remember_me` form flag, but the Gateway never stores passwords. `SessionCookiePolicy` persists the `HttpOnly access_token` cookie only for HTTPS/trusted-forwarded HTTPS, direct-host localhost HTTP, or explicit operator opt-in for insecure persistence; public HTTP sandbox URLs degrade to session cookies. Session-creating handlers stamp the final `max_age` on `request.state`; CSRF cookie creation mirrors it so the double-submit pair expires together, including re-issue after password changes and OIDC callbacks. A small `HttpOnly` preference cookie preserves the remember choice across re-issues. Logout clears all auth cookies and suppresses CSRF re-issue on the logout response.

Personal Access Tokens (`app.gateway.auth.pat`, `Authorization: Bearer dfp_...`) run as their owning user: an invalid Bearer is a hard 401 with no cookie fallback, which keeps `CSRFMiddleware`'s Bearer skip safe (origin checks still run). Scopes narrow within the allowlisted threads/runs/projects routes (including `POST /api/threads/{id}/move`); every other authenticated route 403s PAT callers (admin included). PAT management and `/change-password` require session auth; only SHA-256 digests are stored (`0017`).

Thread→project membership is written by thread creation (`POST /api/threads` with
a validated `project_id`), branch creation (the new row inherits the source
thread's project; an archived/deleted project degrades the branch to unassigned
instead of failing), and explicit moves (`POST /api/threads/{id}/move`); run
admission never modifies membership. The server-reserved `deerflow_project_id`
metadata key is a read-only exposure of the `threads_meta.project_id` column and
is stripped from client writes.

Localhost persistence deliberately reads the direct request `Host` and ignores `Forwarded` / `X-Forwarded-Host`. Scheme and auth-origin reconstruction still consume forwarding headers. The bundled nginx sets `X-Forwarded-Proto`, but preserves an upstream HTTPS value and does not overwrite every forwarded header, so the outer trusted proxy must replace or strip client-supplied forwarding headers before traffic reaches DeerFlow.

Standalone local LangGraph Studio is recognized only through the upstream
`Auth.types.StudioUser` principal type, never by its reusable identity string.
The type is resolved once at import; an older SDK without it degrades to normal
owner scoping.
For that principal's assistant reads/searches, `langgraph_auth.add_owner_filter`
selects genuine server-registered assistants plus assistants owned by Studio;
all other resources remain owner-scoped. Assistant create/update handlers make
both `user_id` and `created_by=user` server-owned, because LangGraph gives
`created_by=system` privileged ownership semantics during run creation. The
custom application module in `langgraph_studio.py` is imported before the
locked in-memory runtime lifespan. At that pre-runtime boundary it derives
genuine system assistant IDs from the CLI-provided graph registry, removes
their persisted active/version rows so graph registration recreates them, and
demotes every other legacy `created_by=system` marker in both active assistants
and version history. This must happen before runtime 0.30.0 loads and purges
system-marked rows; a user application lifespan is too late. LangGraph executes
this file-backed custom app without first registering its module in
`sys.modules`; keep its annotations eager so dataclass processing remains
compatible with that loader, and preserve the direct file-loader regression
test. An empty graph registry or absent persistence file is a no-op, while
persistence parse/write errors fail startup closed. The harness requires
in-memory runtime 0.30.0 or newer, and a persisted store containing no
expected registered assistant row emits a drift warning so changes to
LangGraph's internal persistence contract are observable. With current
create/update writes and all legacy versions sanitized, ordinary
owner-scoped assistant version selection remains enabled.

**Routers**:

| Router | Endpoints |
|--------|-----------|
| **Models** (`/api/models`) | `GET /` - list models; `GET /{name}` - model details |
| **Features** (`/api/features`) | `GET /` - UI capabilities: hot-reloaded agents, guarded browser, startup MCP tasks, and separate batch repository/worker states so history stays readable without a worker |
| **Console** (`/api/console`) | Read-only cross-thread observability for the current user (the data layer for an operations dashboard or external monitoring): `GET /stats` - headline counters (runs/threads/agents/tokens/cost); `GET /runs` - paginated run history joined with thread titles (per-run cost); `GET /usage` - zero-filled daily token series + per-model breakdown with spend. Queries `runs`/`threads_meta` directly as a reporting layer (no new `RunStore` methods); requires a SQL database backend — returns 503 on `database.backend: memory`. Real-cost estimation reads optional `models[*].pricing` (`currency`, `input_per_million`, `output_per_million`, `input_cache_hit_per_million`; `ModelConfig` is `extra="allow"`, so no schema change) and prices each run from its `token_usage_by_model` input/output split. Pricing is **cache-aware**: `RunJournal` accumulates prompt-cache hits from `usage_metadata.input_token_details.cache_read` into a sparse `cache_read_tokens` bucket key (also threaded through `SubagentTokenCollector` → `record_external_llm_usage_records`), and cache-hit input tokens are billed at `input_cache_hit_per_million` (omitted → billed at the miss price, a conservative upper bound). All priced models must use one currency; mixed currencies disable cost reporting and leave cost/currency fields null instead of producing invalid aggregates. Legacy rows fall back to run-level totals at `model_name`; unpriced models yield `cost: null` and cost fields are null when no pricing is configured |
| **MCP** (`/api/mcp`) | GET /config - raw/masked; PUT /config - bulk; PATCH /config - toggle; POST /config/servers - add; PUT /config/server - replace; DELETE /config/servers/{server_name:path} - bodyless. Validate expanded, save raw; reload/reset; invalid -> 400. |
| **MCP Tasks** (`/api/threads/{id}/mcp-tasks`) | `GET /` - current user's durable tasks for one owned thread; `GET /{task_id}` - bounded result/input/status-error/cancellation-error detail, including cancellation attempt count, without remote task IDs or driver configuration |
| **Skills** (`/api/skills`) | `GET /` - list; `GET /{name}` - inspect; `PUT /{name}` - toggle; `POST /install` - install a thread-local .skill archive; `POST /install/upload` - admin-only multipart, authorized before parsing and capped at a 100 MiB file plus 1 MiB framing; `POST /reload` - invalidate process-local cache after trusted filesystem changes |
| **Subagents** (`/api/subagents`) | Admin managed-worker CRUD and listing. |
| **Integrations** (`/api/integrations`) | `GET /lark/status` - inspect managed Lark/Feishu CLI integration state, including `sandbox_runtime_mode` / `sandbox_runtime_ready` (whether `lark-cli` will actually be present in the sandbox at chat time); `POST /lark/install` - admin-only install of the official `lark-*` managed skill pack; `POST /lark/config/start` and `/lark/config/complete` - internal first-time Lark connection setup; `POST /lark/config/credentials` - atomically switch the caller's per-user Lark app after validating the new `app_id`/`app_secret` through the official CLI's live tenant-token probe, revoke/remove the previous OAuth tokens, and restore the prior credential tree if the switch fails; `POST /lark/auth/start` and `/lark/auth/complete` - browser device-flow user authorization without terminal access, with optional `domains` / exact `scope` for incremental permission grants. Config and auth flows carry a server-issued, per-user generation persisted under the credential lock; a rejected direct switch leaves the current generation unchanged, stale completions return 409, and browser re-registration uses the same token-clearing/revocation transaction as direct credential switches. |
| **Memory** (`/api/memory`) | `GET /` - memory data; `POST /reload` - force reload; `GET /config` - config; `GET /status` - config + data |
| **Uploads** (`/api/threads/{id}/uploads`) | `POST /` - upload files (auto-converts PDF/PPT/Excel/Word); non-mounted sandbox sync uses a non-releasing request lease; `GET /list` - list; `DELETE /{filename}` - delete |
| **Threads** (`/api/threads/{id}`) | `DELETE /` - remove DeerFlow-managed local thread data after LangGraph thread deletion; `POST /branches` - branch a completed assistant turn with a replay checkpoint; inherited titles take next-free displayed sibling suffixes, including explicit/renamed ones, while explicit titles stay unchanged. Durable `branch` admission rejects races. Workspace files are not checkpointed, so the branch only best-effort copies the current workspace when branching from the **latest** turn (`workspace_clone_mode="current_thread_best_effort"`); branching from an older/historical turn skips the copy (`workspace_clone_mode="skipped_historical_turn"`) so the branch never inherits files that only exist in a later timeline. Thread-scoped runtime channels (`sandbox`, `thread_data`) are not copied onto the branch: the parent's `sandbox_id` binds path mappings and the release lifecycle to the parent's workspace, so the branch lazily acquires its own sandbox instead. Branch creation also seeds the new thread's run-event feed from the branch checkpoint's visible messages (`history_seed_mode` in the response): the thread feed reads run_events, not checkpoints, so without the seed the inherited history disappears from the UI after the branch's first run (#4380). Seeded rows are grouped into one synthetic run per inherited turn (`branch-seed-{thread_id}-{n}`, a new turn opening at every persisted human message, including an allowlisted hidden `ask_clarification` reply) because `run_id` is a turn identity to the feed's consumers, not a provenance tag: regenerating an inherited answer supersedes that row's whole `run_id` in `GET /messages/page`, so one shared id for the entire seed deleted the complete inherited history on a branch's first regenerate (#4458); `GET /goal`, `PUT /goal`, `DELETE /goal` - read, set, and clear the active thread goal; `POST /compact` - manually summarize older active context into `summary_text` and retain the recent message window, blocked while a run is in flight; unexpected failures are logged server-side and return a generic 500 detail |
| **Artifacts** (`/api/threads/{id}/artifacts`) | `GET /{path}` - stream regular text and binary artifacts with `FileResponse`, including byte-`Range` 206/416 behavior used by bounded text previews and media seeking; active content types (`text/html`, `application/xhtml+xml`, `image/svg+xml`) are always forced as download attachments to reduce XSS risk; `?download=true` still forces download for other file types. `PUT /{path}` atomically replaces an existing UTF-8 text file under `/mnt/user-data/outputs` when its expected SHA-256 still matches; active runs conflict, and non-mounted sandbox providers receive the same update under a request lease. The outputs-only rule is `path_utils.resolve_outputs_confined_path`, shared with IM-channel attachment delivery: it collapses `..` before the prefix check and re-checks the resolved host path against the resolved outputs root, since `resolve_thread_virtual_path` only confines to `user-data/`; a percent-encoded `..` or a symlink planted in `outputs/` must not reach a sibling `uploads/` file. Atomic replacement applies the existing POSIX permission handling when descriptor-based APIs are available and otherwise keeps the platform-native temporary-file permissions (Windows). |
| **Suggestions** (`/api/suggestions`) | `GET /config` - returns global suggestions config boolean; `POST /threads/{id}/suggestions` - generate follow-up questions; rich list/block model content is normalized and inline reasoning (`<think>...</think>`, including unclosed/truncated blocks from reasoning models like MiniMax-M3) is stripped before JSON parsing |
| **Input Polish** (`/api/input-polish`) | `POST /` - rewrite a composer draft before it is sent. This is a short authenticated `runs:create` LLM request using `input_polish` config; it does not create a LangGraph run, persist a message, or modify thread state. Shares the non-graph one-shot LLM path (`deerflow.utils.oneshot_llm.run_oneshot_llm`) with the suggestions route so model build + Langfuse metadata + invoke stay in one place; validates the same stripped view of the draft it sends to the model, and preserves literal `<think>` substrings in the rewrite (`strip_think_blocks(truncate_unclosed=False)`) |
| **Thread Runs** (`/api/threads/{id}/runs`) | `POST /` - create background run; `POST /stream` - create + SSE stream; `POST /wait` - create + block. Before the first journaled run, seed an empty feed from a checkpoint so legacy checkpoint-only history keeps its order and visibility; skip absent checkpoints or populated feeds. `POST /regenerate/prepare` - prepare clean input + checkpoint metadata for regenerating the latest completed or interrupted assistant answer, carrying the latest non-empty thread title in graph input so resuming an older checkpoint cannot roll back a later manual rename (#4457); `POST /edit-regenerate/prepare` - prepare a checkpoint replay from the latest editable human turn with a replacement user message and edit replay metadata; it carries the current thread title the same way, but only when the replay base already has one — an untitled base belongs to a thread the title middleware has not named yet, so pinning the current title there would keep a name generated from the prompt the edit just replaced; `GET /` - newest 100 runs as an array; `GET /page` - keyset history page `{data, has_more, next_before_created_at, next_before_run_id}`; `GET /{rid}` - run details; `POST /{rid}/cancel` - cancel; `GET /{rid}/join` - join SSE; `GET /{rid}/stream` hides action/wait; GET action 405 pre-owner; POST needs `runs:cancel`; `GET /{rid}/messages` - paginated per-run messages `{data, has_more}`; `GET /{rid}/events` - full event stream; `GET /{rid}/workspace-changes` - workspace/output file change summary and optional diffs; `GET/POST /{rid}/artifacts/archive` - receipt manifest / bounded ZIP; `GET /../messages` - legacy thread message array; `GET /../messages/page` - backward thread-global `seq` history page with middleware/subagent-AI/successful-regenerate/edit-replay filtering and page-run-scoped feedback enrichment; subagent AI callbacks remain available through run events while parent `task` ToolMessages stay visible for card restoration; `GET /../token-usage` - aggregate tokens plus an optional `context_usage` percentage. Context usage approximately counts messages from the latest materialized thread state through `build_thread_checkpoint_state_accessor`, so full and delta checkpoint modes expose the same input. The percentage uses the latest run's model and its `context_window`. |
| **Feedback** (`/api/threads/{id}/runs/{rid}/feedback`) | `PUT /` - upsert feedback; `DELETE /` - delete user feedback; `POST /` - create feedback; `GET /` - list feedback; `GET /stats` - aggregate stats; `DELETE /{fid}` - delete specific |
| **Runs** (`/api/runs`) | `POST /stream`, `/wait` - stateless runs requiring `runs:create`; optional body `thread_id` is owner-checked. Scheduled-task create/update/resume/trigger also require `threads:write` plus `runs:create`. `GET /{rid}/messages`, `/feedback` - run messages/feedback |
| **GitHub Webhooks** (`/api/webhooks/github`) | `POST /` - receive GitHub App / repo webhook deliveries. Verifies `X-Hub-Signature-256` against `GITHUB_WEBHOOK_SECRET`; exempt from auth + CSRF because authenticity is enforced by HMAC. The route is fail-closed: mounted only when `GITHUB_WEBHOOK_SECRET` is set, or when explicit dev opt-in `DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS=1` is set. Recognized events include `ping`, `issues`, `issue_comment`, `pull_request`, `pull_request_review`, and `pull_request_review_comment`; unknown events return 200 with `handled=false`. Fan-out runtime failures return 503, keeping the delivery recorded as failed for manual/API/scripted redelivery (GitHub does not automatically retry any failed delivery, 5xx included); permanent/non-retryable conditions such as `channels.github.enabled: false`, unknown events, malformed payloads, or unavailable channel service return 200 with a skipped/handled response. |
| **GitHub Event-Driven Agents** | Custom agents can declare a `github:` block in their `config.yaml` to bind to repos and event triggers. Webhook fan-out publishes one `InboundMessage` per matching binding to the channel bus; `GitHubChannel` routes those messages through `ChannelManager`. The response `dispatch` summarizes matched/fired/skipped agents. |

Thread identifiers use the shared `deerflow.utils.thread_id` contract
`^[A-Za-z0-9_-]{1,64}$`. Caller-provided opaque IDs remain supported; UUIDs
are generated only for `None`, while explicit empty strings fail validation.
Gateway creation and state-producing request boundaries, embedded-client
entry points, filesystem/upload/event-store consumers, scheduled launches,
and the standalone Provisioner enforce the same contract before persistence
or workspace initialization. Route-addressable legacy IDs remain accepted by
pure reads and cleanup/control endpoints; deleting one best-effort removes
metadata and checkpoints but skips local filesystem cleanup, so the raw value
is never interpolated into a host path. New runs, workspace/sandbox
operations, and other state-producing mutations remain blocked.

**Message feed seq** (#4666): streaming `values` frames, `GET
/threads/{id}/state`, and `POST /threads/{id}/history` stamp serialized
messages with `additional_kwargs.deerflow_seq` so clients can place
checkpoint-kept messages against the paged feed; the REST reads resolve the
store via `threads.py::_optional_run_event_store` (a feed-less deployment
still reads threads), and `services.py::normalize_input` strips the
server-owned key from client input (#4380). Mechanism and identity rule:
`packages/harness/deerflow/runtime/AGENTS.md`.

**Workspace change review**: `packages/harness/deerflow/workspace_changes/`
captures a pre-run and post-run snapshot of the thread-owned `workspace` and
`outputs` directories. `runtime/runs/worker.py` performs the filesystem scan via
`asyncio.to_thread` and writes a `workspace_changes` event with category
`workspace` when changes exist. Uploads are intentionally excluded. Text diffs
are size-limited; binary, large, and sensitive-looking paths are persisted as
metadata only. Internal process-feedback directories never count as changes:
the scanner's `EXCLUDED_DIR_NAMES` drops `BROWSER_FRAMES_DIRNAME` (transient
browser screenshots) and `TOOL_RESULTS_DIRNAME` (the tool-output budget
middleware's default externalization subdir, `constants.py` is the shared
source of truth for both writers and the scanner), and the worker threads the
configured `tool_output.storage_subdir` through the snapshot capture as an
extra excluded dir name so custom storage locations stay excluded too.

**Run delivery receipts**: the worker derives delivery requirements from the
run's workspace snapshots rather than a client request option (files
created/modified under `/mnt/user-data/outputs`, minus internal
process-feedback exclusions) and idempotently persists a run-scoped
`run.delivery` receipt before the terminal run status; missing or
unverifiable `present_files` coverage downgrades the run to error, while runs
without changed outputs keep ordinary chat behavior. Journal mechanics
(callback attribution, receipt idempotency and retries, orphan recovery):
`packages/harness/deerflow/runtime/AGENTS.md`. Multi-worker deployments
require `run_events.backend: db` for shared, ordered delivery events; the
startup gate rejects process-local memory and JSONL event stores when
`GATEWAY_WORKERS > 1`.

**RunManager / RunStore contract**: Read [run and lifecycle development contracts](../../docs/gateway-lifecycle-guidance.md) before changing run storage or routing.

**Branch/regenerate checkpoint invariant**: `app/gateway/checkpoint_lineage.py`
walks `parent_config` rather than globally ordered checkpoint history so replay
anchors stay on the selected lineage after regenerations create sibling branches.
New conversation branches persist the pre-user replay anchor before their visible
head through the state mutation graph, which preserves materialized state in both
full and delta checkpoint modes. Only an explicitly absent legacy parent link may
use chronological compatibility lookup; cycles, dangling links, and depth-limit
exhaustion fail closed. Existing single-checkpoint branches are never repaired by
copying a raw checkpoint because delta state is not self-contained in one tuple.
Both lookups additionally require the replay base to be a **settled** checkpoint
(`has_pending_tasks` — no scheduled `next` tasks). A checkpoint with pending tasks
is a mid-run snapshot: resuming from it replays the writes of the node that was
about to run. Message ids alone cannot exclude those, because middleware may
rewrite a message's id inside the run that produced it — `DynamicContextMiddleware`
moves the first user turn to `{id}__user` and gives `{id}` to the injected
reminder, so every checkpoint written before it holds the same prompt under an
unmatched id. Selecting one of those re-added the original prompt *after* the
edited one, and the model answered the question the edit was replacing (#4531).
`next` is not derivable on the degraded raw-checkpoint read path, which reports no
tasks; absence of evidence stays permissive there rather than failing closed.
Edit replay resolves its base through the same lineage-first path as regenerate;
it must pass `head_checkpoint` or it silently degrades to the chronological scan
that cannot tell sibling branches apart.

- Optional Fleet remote streams prepare before response headers/cancel: invalid cursors are 400, missing unconsumed history 410; runtime history or accepted-mapping loss closes without END. Replay uses original placement pointers; Assigned END requires the matching immutable seal and consumed tail. Preparation checks the suffix once; runtime pointer pages/checks are bounded to128. Remote `/wait` prepares `(record, None)` to retain queued owner binding, including taskless records; store-only observers never cancel on disconnect/EOF. Seal repeats reject immutable conflicts transactionally. Local semantics stay unchanged; C12 is locally accepted. See [remote stream contracts](../../docs/RUN_EVENT_STREAM.md) for retention, authority, publisher and fixture details.

Fleet STOP requires `_NODE_PATH` bearer auth and grants no writes;
[C11 summary](../../../docs/ecs-fleet-c11-runtime.md), [C10 routing](../../../docs/ecs-fleet-c10-runtime.md).

Before changing the behavior covered below, read [the detailed development contracts](../../docs/gateway-lifecycle-guidance.md). This guide is required for changes in this directory.
