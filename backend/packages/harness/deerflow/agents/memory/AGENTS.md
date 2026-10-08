### Memory System

This directory owns memory capture, storage, retrieval, prompt injection, and model-driven memory tools.

#### Main components

- `manager.py` defines the backend-neutral `MemoryManager` contract.
- `agents/middlewares/memory_middleware.py` queues filtered conversations for passive capture.
- `summarization_hook.py` connects memory work to the summarization lifecycle.
- `tools.py` provides `memory_search`, `memory_add`, `memory_update`, and `memory_delete`.
- `backends/deermem/` contains the default local backend.
- `backends/mem0/`, `backends/openviking/`, and `backends/honcho/` contain optional adapters.

`cancel_by_agent` cancels only pending debounce contexts in one user scope.
`user_id=None` selects only the legacy no-user root.
`agent_name=None` selects all agent buckets in that user scope.
It does not interrupt a context after `_process_queue` removes it from `_items`.
Broader cancellation must iterate known user scopes.

Focused updater tests live in `backend/tests/test_memory_updater.py`.
Backend-specific tests use `backend/tests/test_<backend>_memory_backend.py`.

#### Identity and isolation

Resolve users with `resolve_runtime_user_id(runtime)` in middleware and tools.
This keeps Gateway and standalone LangGraph runs in the same user scope.

**Configuration** (`config.yaml` → `memory`):
- `enabled` / `injection_enabled` - Master switches
- `mode` - Operation mode: `middleware` (default passive background extraction) or `tool` (experimental model-driven memory tools). Modes are mutually exclusive.
- `storage_path` - DeerMem storage root; one global summary JSON lives under each user and Markdown facts remain under agent buckets
- `storage_class` - `file` or a dotted `MemoryStorage` class; invalid persistent backends fail fast
- `strict_user_scope` - Require `user_id` for all storage access (default `false` for no-auth/legacy compatibility)
- `manifest_filename` - User-global summary JSON filename (kept for configuration compatibility)
- `file_lock_timeout_seconds` - Scope-lock wait; Markdown facts and the recovery journal are required storage invariants rather than configurable modes
- `retrieval_adapter` - `fts5` by default, empty to disable, or a dotted factory receiving `DeerMemConfig` and returning a retrieval-port implementation
- `debounce_seconds` - Wait time before processing (default: 30)
- `shutdown_flush_timeout_seconds` - Hard budget (seconds) reserved for draining the memory backend's pending-update buffer on Gateway graceful shutdown (default: 30; 1–300). Each pending item does one LLM call, so large IM batches may need more. The Gateway lifespan calls `MemoryManager.shutdown_flush(timeout)` after channels/scheduler stop and after waiting at most one additional second for the derived retrieval warm-up; the backend short-circuits on an idle buffer, so the host calls it unconditionally (no pending/processing gate). The retrieval wait does not reduce this canonical flush budget. The combined shutdown hooks, brief retrieval wait, flush budget, and scheduling margin must fit inside the pod's K8s `terminationGracePeriodSeconds` (gateway Helm chart default: 45s) or K8s SIGKILLs the drain mid-flight.
- `model_name` - LLM for updates (null = default model)
- `max_facts` / `fact_confidence_threshold` - Fact storage limits (100 / 0.7)
- `fact_eviction_policy` / `fact_eviction_shadow_enabled` - Capacity policy (`confidence` default; opt-in `hybrid-v1`) and non-enforcing hybrid comparison audit
- `eviction_confidence_weight` / `eviction_confirmation_weight` / `eviction_access_weight` - Hybrid weights (0.65 / 0.25 / 0.10; must sum to 1.0)
- `eviction_confirmation_half_life_days` / `eviction_access_half_life_days` - Confirmation and query-heat decay windows (90 / 30 days)
- `eviction_correction_reserved_fraction` / `eviction_correction_reserved_max` - Bounded minimum correction capacity (0.10 / 10; unused slots are released)
- `eviction_audit_max_entries` - Metadata-only capacity audit bound per user/agent scope (200; 0 disables)
- `max_injection_tokens` - Token limit for prompt injection (2000)
- `token_counting` - Token counting strategy for the injection budget: `tiktoken` (default, accurate but may download BPE data from a public endpoint on first use — can block for a long time in network-restricted environments, see issues #3402/#3429) or `char` (network-free CJK-aware char estimate, never touches tiktoken)
- `staleness_review_enabled` - Enable proactive staleness pruning of aged facts (default: `true`; only triggers when aged candidates exist)
- `staleness_age_days` - Age in days before a fact becomes a staleness candidate (default: 90; range: 30–365)
- `staleness_min_candidates` - Minimum aged candidates required to trigger a review cycle (default: 3; range: 1–50)
- `staleness_max_removals_per_cycle` - Maximum facts removed in a single cycle; lowest-confidence entries are kept when the LLM requests more (default: 10; range: 1–50)
- `staleness_protected_categories` - Fact categories that are never pruned by staleness review (default: `["correction"]`)
- `staleness_max_lifetime_multiplier` - Creation-time cap multiplier for a fact's LLM-assigned `expected_valid_days`: stored value is clamped to `staleness_age_days × multiplier` so the model cannot defer first review indefinitely (default: 20.0; range: 1.0–100.0). Default 20.0 (90 × 20 = 1800 d ≈ 5 years) is generous enough to support the very-stable prompt tier without needing multiple review cycles to escape the cap.
- `staleness_max_extension_days` - Absolute upper bound (in days) on `expected_valid_days` after a lifetime extension (`staleFactsToExtend`). Applied at write time as `min(days_since + extend_by, staleness_max_extension_days)`. Uses an absolute ceiling rather than the multiplier because extensions are deliberate review decisions; prevents `timedelta` overflow and LLM misfire from permanently deferring a fact (default: 3650 = 10 years; range: 90–36500).
- `consolidation_enabled` - Enable memory consolidation (default: `true`; no extra API call — runs in the same LLM invocation as the normal memory update)
- `consolidation_min_facts` - Minimum facts in a category to trigger consolidation review (default: 8; range: 3–30)
- `consolidation_max_groups_per_cycle` - Maximum categories the LLM can merge in one cycle (default: 3; range: 1–10; also controls the LLM's prompt instruction)
- `consolidation_max_sources` - Maximum source facts per merge group; prevents over-merging (default: 8; range: 2–20)
- `watermark_max_keys` - Soft cap on the in-memory conversation-watermark cache (one entry per distinct thread/user/agent). A bounded LRU: when over capacity the least-recently-used entry is dropped, and a dropped key re-extracts one batch on that thread's next turn (same as a restart). Bounds memory in long-lived gateways handling many threads (default: 4096; 0 = unbounded)


### Remote execution memory boundary (C06c)

The private remote factory resolves backend compatibility before `from_config` or
warm-up. `MemoryManager.remote_mutation_mode` defaults to `unsupported`; noop is
stateless, while an operator-trusted adapted backend declares `transactional`.
A declaration does not fence file storage or an external SDK. Existing DeerMem,
mem0, Honcho and OpenViking implementations are not atomic remote-write adapters.
Local backend selection and the Local singleton retain their existing behavior.

`make_remote_memory_manager` supplies private host transaction hooks separately
from private `backend_config`. Remote public AppConfig exposes only validated
`failure_policy.read` (`fail_closed`/`fail_open`), preserving prompt behavior;
constructor options stay private. `memory_manager_scope` selects the bound manager
before consulting the Local singleton. Missing or different original execution context
rejects access instead of falling back to an unbound manager. Adapted backends
use `BoundMutationTransactions` on the actual writer connection/transaction and
hold original execution locks through commit or rollback. Memory writes are
active-only; read-only recall does not grant later write authority.

Capture the original context and bound capability at enqueue, retain both per
item across raw threads and shutdown drain, and separate attempts when coalescing.
Validate again after extraction, inside the actual write transaction. Memory CRUD
tools and emergency summarization hooks propagate nonretryable `OwnershipRejected`
instead of returning an ordinary error string or swallowing it. The configured
PostgreSQL fixture proves real durable behavior; it is not a new default memory
product. C06 acceptance evidence belongs to the external plan; activation stays closed.


The remote host wires `RunContext.before_terminal_mutations` to drain the private
manager's original queued work before durable terminal status is committed.
This preserves the normal `MemoryMiddleware.aafter_agent` -> `manager.aadd`
queue path; models and tools do not manage the queue's lifecycle. Resource close
after terminal does not authorize remaining memory writes. Local hosts without
that optional hook retain their asynchronous policy. Installed verification of
both middleware and tool modes is required before full C06c acceptance.


The host locks the original memory-user domain after six execution locks and
before fresh clock validation. The same writer transaction revalidates its original
capability after ORM flush and before commit, so expiry during target-table/row waits
rolls back the write. Backend callbacks cannot choose advisory keys.

Remote teardown requires positive native-worker quiescence before service stop
and resource unwind. `shutdown_flush(False)` or an exception does not prove
worker completion; default `close()` is not a drain. The private host retains
original resources/phase Tasks and retries within the first monotonic 120s total
budget, keeping isolated-worker deadline exit armed. Memory extraction and owned
observers can enqueue each other: drain to joint quiescence using original-scope
enqueue/actual-Task completion revisions, then repeat after service stop. Only
then call memory close. Local/Gateway best-effort shutdown is unchanged. Verify
real configured native queue/SQL barriers, positive completion, round-trip enqueue
and isolated deadline exit with independent PG rollback/connection/lock checks.

BC07 candidate: private memory construction rejects explicit alternate model/LLM
settings before effects. Default factory models consult the opaque task budget at
every request; directly cached DeerMem models reject private calls unless supported.
Trusted queue/thread dispatch must preserve execution ContextVars. Native budget
main passed; memory/installed budget wiring remains within subsequent qualification.

Server-owned `langgraph_auth_user_id` takes precedence over ordinary client identity.
Lead-agent construction normalizes it with `make_safe_user_id`.
Memory, custom agents, user skills, skill policy, and prompt assembly reuse that identity.
Gateway removes client-supplied `langgraph_auth_user` and `langgraph_auth_user_id` before graph construction.

Gateway memory routes use `_resolve_memory_user_id(request)`.
Trusted IM requests can act for the connection owner.
Other requests use `get_effective_user_id()`.
Only `AuthMiddleware` can authorize the internal owner header.

No-auth mode uses `DEFAULT_USER_ID`, which is `"default"`.
An absolute `storage_path` opts out of the default per-user root.

DeerMem uses this layout:

```text
{base_dir}/users/{user_id}/memory.json
{base_dir}/users/{user_id}/agents/{agent_name}/facts/{sha256-prefix}/{fact-id}.md
```

`memory.json` stores only shared summaries, revision data, and timestamps.
It never stores facts or a fact index.
Each Markdown file stores one fact with YAML front matter.

Custom agent files share the per-user agent directory.
The legacy shared agent layout is read-only fallback data.

DeerMem maps a missing agent name to `__default__`.
That name is reserved and cannot identify a custom agent.
Public agent names use lowercase canonical form.

#### Operating modes

`memory.mode: middleware` is the default passive mode.
`MemoryMiddleware` queues filtered user and final assistant messages.
It captures `user_id` when it enqueues work.
This identity survives the background timer boundary.

`memory.mode: tool` registers the four memory tools.
The model chooses when to search or change facts.
Tool mode still uses `MemoryMiddleware` for passive writes on supported remote backends.

Middleware injection includes shared summaries and the selected agent's facts.
Tool-mode injection includes only shared summaries.
Tool mode leaves agent facts behind `memory_search`.
`memory.injection_enabled: false` disables the complete injected block.

#### DeerMem storage contract

`FileMemoryStorage` owns canonical storage and the retrieval adapter.
Do not reach into its private adapter state from higher layers.

The repository supports fact CRUD, summary updates, migration, search, and index lifecycle operations.
Targeted writes change only the selected Markdown files.
Whole-document `load` and `save` remain compatibility operations.

`apply_changes()` returns `complete: false` with fact deltas.
It never labels a partial cache as a complete memory document.
Public callers reload only when their response contract requires a complete document.

Writes use a user lock, shared revision, fact revisions, and a recovery journal.
Point operations can rebase only when all original fact preconditions still hold.
Snapshot operations must reload and recompute after a manifest conflict.
Use the typed conflict classes instead of matching exception text.

The weak lock cache must not retain inactive user scopes.
Cache validation uses the manifest metadata and persisted revision.
Out-of-band Markdown edits require `reload()`.
POSIX atomic replacement must sync the parent directory.

DeerMem converts storage conflicts to the public `MemoryManager` error types.
The Gateway maps conflicts to HTTP 409.
The Gateway maps storage corruption to a stable HTTP 500 response.

#### Migration

A normal default-manager read migrates legacy facts into `__default__`.
It adopts an old `lead-agent` bucket only when no custom-agent config exists.
Unexpected files stop migration and remain on disk.

The v1-to-v2 migration is one-way during application operation.
Operators must stop DeerFlow and snapshot the storage root before migration.
Every destructive migration first writes a verified `{manifest_filename}.v1.bak` file.
Missing or mismatched backups abort migration without changing v1 data.
Delete legacy agent JSON only after safe summary adoption or equality checks.
Summary conflicts keep the source file and return an error.

Run the proactive migration from `backend/`:

```bash
PYTHONPATH=. python scripts/migrate_memory_markdown.py --all-users --dry-run
```

Remove `--dry-run` to migrate.
Use repeated `--user-id` options for exact source identities.
Use `--storage-path` for a non-default DeerMem root.
The command is idempotent and continues after per-user failures.
It returns a nonzero status when any user fails.

The older isolation migration remains available:

```bash
PYTHONPATH=. python scripts/migrate_user_isolation.py --dry-run
```

#### Retrieval

`retrieval_adapter` owns indexing and retrieval.
DeerMem selects persistent SQLite FTS5 by default.
An empty value selects the substring fallback.

SQLite index data lives below `.retrieval/` and remains rebuildable.
Chinese tokenization uses `jieba` only with the `memory-zh` extra.
Malformed facts are logged and skipped during rebuild.
A fatal rebuild failure keeps lazy retry active.
A corrupt persistent database is deleted and recreated once.

Storage sends adapter updates after it releases durable locks.
Adapter failures mark the scope dirty.
Search then uses canonical substring matching until rebuild succeeds.

Gateway startup schedules `DeerMem.warm_retrieval()` without delaying readiness.
The first search can rebuild its exact scope.
Shutdown waits one second for retrieval warm-up.
It reserves the full configured timeout for canonical memory flush.
The Gateway closes the derived SQLite connection after that flush.

#### Extraction safety

Extraction labels proposals with `scope`, `durability`, and `authority`.
Automatic writes accept only user-scoped, durable, descriptive facts.
Summary prose must be user-scoped and descriptive.
Missing labels reject that item without stopping unrelated updates.

Contradiction removals include `id`, `scope`, `reason`, and optional `replacementFactIndex`.
Task-scoped and project-scoped removals fail closed.
A paired removal requires its replacement to pass every write gate.
Tool-mode CRUD does not use the extraction gate.

Custom prompt directories must include the same classification fields.
Old templates cause extraction writes to fail closed.
The rejection counter and high-rejection warning expose this condition.

#### Capacity and review

All automatic, manual, tool, and import paths use `deermem/core/eviction.py`.
`confidence` is the default capacity policy.
`hybrid-v1` is opt-in and uses confidence, confirmation freshness, and access heat.
Shadow mode records disagreement while enforcing confidence-only selection.

Only deterministic message processing can confirm a fact.
The updater's `factsToReinforce` output supplies only the fact binding.
The deterministic gate matches a human message in the last six filtered batch messages.
It does not require a separate signal-to-fact match.
Search increments access heat only for facts it returns.
Prompt injection and `get_context()` do not increment access heat.

Usage and audit sidecars live below the agent `.metadata/` directory.
They must not change canonical Markdown timestamps or revisions.
Write audits only after canonical persistence succeeds.
User delete and clear operations must remove matching sidecar data.

Staleness review reuses the regular updater call.
It can keep, remove, or extend eligible aged facts.
Protected categories and non-aged facts cannot become removal targets.
Apply the per-cycle removal cap after candidate validation.
Do not extend a fact proposed for removal, even when the cap keeps that fact.
Extension bounds must prevent date overflow.

Consolidation also reuses the regular updater call.
Source facts must exist and cannot overlap across groups.
Enforce the source-count and confidence limits at apply time.
Use the newest source creation time for the merged fact.
Use the earliest source review deadline for its next review.

#### Remote backends

Strict reads use the backend-neutral `MemoryReadError`.
Backends declare their policy through `read_failures_are_fatal_for_config()`.
`DynamicContextMiddleware` preserves that policy at its injection timeout.
Policy methods must use only in-memory config. The non-loading lookup returns
unknown for a cold backend; discovery/config reload runs inside the existing
timed injection worker. Per-call policy state cannot leak across runs, and
timeout handling never submits more executor work. Unknown policy fails closed.
The prompt loader retains `MemoryManagerError` + `fail_closed` compatibility
for third-party backends that have not adopted the typed error.
The base policy resolver also honors legacy `fail_closed` at the timeout boundary;
other settings remain permissive unless the backend overrides the resolver.

OpenViking uses the maintained `langchain-openviking` package.
Keep it in middleware mode.
One API key is bound to one configured DeerFlow owner.
Reject another owner before remote access.

DeerFlow owns capture timing, the recall query, and the transcript cursor.
The package owns transport, message conversion, batching, and Session commits.
One DeerFlow thread maps to one stable OpenViking Session.
Store bounded hash-only cursors below `{storage_path}/openviking/sessions/`.

Async OpenViking entry points must offload synchronous SDK and file operations.
Shutdown must drain active work before closing the recorder client.
Pass an empty `extra_headers` mapping to prevent configuration-added transport headers.
Do not add embedded OpenViking imports, root-key access, or trusted identity headers.

Honcho is a remote HTTP adapter for user-model memory.
It creates one workspace per resolved `user_id`.
A missing user fails closed to no memory.
Its async methods offload synchronous HTTP work with `asyncio.to_thread`.
The default read failure policy logs and returns no results.
`failure_policy.read: fail_closed` rethrows recall failures.

Honcho configuration rejects non-finite or non-positive timeouts.
It also rejects non-positive character budgets during construction.

#### Run identity and token counting

Each run hashes its effective hidden memory block.
The run records one `context:memory` event with `content_sha256`.
The full memory text stays in checkpoint state.

Only current `DynamicContextMiddleware` output can establish first-run memory identity.
Checkpoint reuse requires the block to exist before the run.
Gateway input handling removes forged dynamic-context markers.

`prompt.py::_count_tokens` controls the injection budget.
Default `tiktoken` mode loads and caches its encoding lazily.
A failed load uses character estimation for a 600-second cooldown.
Concurrent callers use character estimation while one load is active.
Set `memory.token_counting: char` to prevent network access.

#### Configuration

The schema lives in `deerflow/config/memory_config.py`.
Do not duplicate its complete field list here.

Keep these cross-component constraints in sync:

- The shutdown flush budget is between 1 and 300 seconds.
- The pod grace period must include retrieval wait, flush time, and shutdown margin.
- `retrieval_adapter` selects FTS5, a custom factory, or the empty fallback.
- Eviction weights must total `1.0`.
- `watermark_max_keys: 0` makes the conversation watermark cache unbounded.
- A dropped watermark can re-extract one batch on the next turn.
