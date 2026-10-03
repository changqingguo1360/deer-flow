# Run Event Stream

The run event stream is DeerFlow's append-only record of what happened during
an agent run. Producers write through `RunEventStore`; history, debug, subtask,
memory-audit, and workspace-review consumers read projections of the same rows.

The machine-readable contract is
`contracts/run_event_stream_contract.json`. Canonical event names and
categories live in `deerflow.runtime.events.catalog`; conformance tests require
the runtime catalog and JSON contract to match exactly.

## Record Envelope

Every persisted event has these required fields:

| Field | Meaning |
| --- | --- |
| `thread_id` | Thread that owns the event. |
| `run_id` | Run that produced the event. |
| `seq` | Store-assigned sequence, strictly increasing within a thread. |
| `event_type` | Fixed event name or documented dynamic pattern. |
| `category` | Consumer-routing bucket. |
| `content` | Event payload, normally a string or JSON object. |
| `metadata` | Filterable or audit metadata. |
| `created_at` | Timezone-aware ISO-8601 timestamp. |

Backends may return additional fields. `DbRunEventStore`, for example, returns
`user_id` and may add serialization markers such as `content_is_json` to
metadata. Consumers must ignore unknown envelope and metadata fields.

`event_type` is limited to 32 characters and `category` to 16 characters by the
database schema. Catalog-backed definitions enforce the same limits before
writing so they cannot emit values that only the memory or JSONL store accepts.

`seq` is thread-global, not run-local. Memory and database stores assign it
monotonically for their supported deployment modes. JSONL only provides this
guarantee within one process; shared multi-process deployments must use the
database store.

## Categories

`category="message"` means an event is eligible for a message projection; it
does not guarantee that the row is visible in the UI. Thread-history APIs also
filter middleware model calls, subagent AI responses, and superseded regenerate
runs, and the frontend honors message-level visibility markers such as
`hide_from_ui`. Subagent events remain available through the run-events
endpoint; parent `task` ToolMessages remain in thread history so subtask cards
can restore their terminal status after reload.

All other categories are excluded from message projections and are available
through run-event or specialized APIs:

| Category | Purpose |
| --- | --- |
| `trace` | Execution evidence. |
| `outputs` | Root graph completion output. |
| `error` | Callback-observed failure evidence. |
| `middleware` | Middleware state-change audit evidence. |
| `context` | Effective hidden-context identity. |
| `subagent` | Subagent lifecycle and step history. |
| `workspace` | Workspace/output file-change evidence. |

## Producers

`RunJournal` emits callback-derived events:

| Event type | Category | Producer |
| --- | --- | --- |
| `run.start` | `trace` | Root `on_chain_start()` |
| `run.end` | `outputs` | Root `on_chain_end()` |
| `run.error` | `error` | `on_chain_error()` |
| `llm.human.input` | `message` | First persisted lead-agent human input |
| `llm.ai.response` | `message` | `on_llm_end()` |
| `llm.tool.result` | `message` | `on_tool_end()` |
| `llm.error` | `trace` | `on_llm_error()` |
| `context:memory` | `context` | `record_memory_context()` |
| `middleware:{tag}` | `middleware` | `record_middleware()` |

Current middleware tags are `guardrail`, `safety_termination`,
`skill_activation`, and `skill_secrets`. The pattern is intentionally open so
new middleware tags are additive. Because the full event type is limited to 32
characters and `middleware:` uses 11, a tag must contain 1-21 characters.

### Opaque Run Outputs

`run.end.content` is the root graph output and is intentionally opaque. Its
nested representation is not currently identical across storage backends:

- `MemoryRunEventStore` retains the original Python container and nested
  values.
- `JsonlRunEventStore` and `DbRunEventStore` serialize through
  `json.dumps(default=str)`, so nested values that are not directly JSON
  serializable are read back as strings.

Consumers may use `run.end` as completion evidence, but must not depend on
backend-identical nested output values. Normalizing those values would be a
separate runtime compatibility change rather than part of this current-state
contract.

`subagents/step_events.py::subagent_run_event()` maps streamed `task_*` chunks
to persisted events. The worker batches them through `put_batch()`:

| Event type | Source chunk | Required content |
| --- | --- | --- |
| `subagent.start` | `task_started` | `task_id`, `description` |
| `subagent.step` | `task_running` | `task_id`, `message_index`, `kind`, `text`, `truncated`; AI steps add `tool_calls`, tool steps add `tool_name` |
| `subagent.end` | terminal `task_*` | `task_id`, `status`; optional model, usage, result/error, and truncation fields |

Terminal subagent status is one of `completed`, `failed`, `cancelled`, or
`timed_out`.

Malformed lifecycle chunks are not persisted. Every chunk requires a non-empty
string `task_id`; `task_running` additionally requires a non-negative integer
`message_index` and a message object.

`workspace_changes.record_workspace_changes()` writes `workspace_changes` in
category `workspace` when a run changed files. Its string content is a summary;
the structured versioned summary, file list, and limits live in
`metadata.workspace_changes`.

The JSON contract defines required and optional payload fields using JSON
Schema. It is the authoritative field-level reference.

## Consumers

| Consumer | Read path and behavior |
| --- | --- |
| Frontend thread history | `GET /api/threads/{thread_id}/messages/page` scans `list_messages()`, removes middleware rows, subagent AI responses, and superseded regenerate runs, then applies frontend message visibility rules. |
| Per-run message clients | Thread-scoped and stateless run message endpoints call `list_messages_by_run()`. |
| Run debug/audit | `GET /api/threads/{thread_id}/runs/{run_id}/events` calls `list_events()` and supports `event_types`, `task_id`, `limit`, and `after_seq`. |
| Historical subtask cards | Fetch `subagent.step` through the run-events endpoint, filtered and paginated by `task_id`. |
| Memory audit | Filters run events to `context:memory` and compares `content_sha256`; full memory text is not duplicated into the event store. |
| Workspace review | `GET /api/threads/{thread_id}/runs/{run_id}/workspace-changes` projects the latest `workspace_changes` payload. |

Token and cost summaries are not reconstructed by reading event rows.
`RunJournal` accumulates usage while callbacks fire, and the worker writes the
aggregates to `RunRow`.

External Langfuse/LangSmith tracing is a parallel callback pipeline, not a
`RunEventStore` consumer. It is correlated through trace metadata rather than
being derived from these rows.

Evaluation consumers discussed in #4243 are planned rather than present in
this tree. They should read evidence through `list_events()` and treat the
compatibility and terminal-state limits below as part of that integration.

## Compatibility

The existing mixture of dot-separated, colon-separated, and bare-word names is
frozen. This contract documents current behavior; it does not normalize names.
A rename, removal, category change, required-field removal, or required-field
type change is breaking and needs an explicit versioned migration or dual-write
period.

Adding a new event type or optional field is additive. Consumers must ignore
unknown event types and unknown optional fields. Producers must add a catalog
entry, update the JSON contract and this document, and extend the conformance
tests in the same change.

`ai_message` is a read-only legacy alias for `llm.ai.response`. Current
producers never emit it. Category-based message projections and store queries
for the last visible AI message recognize previously persisted alias rows, so
the `/messages/page` endpoint also attaches feedback correctly. The legacy
`/messages` endpoint still returns those rows but only enriches feedback for the
canonical name. Legacy aliases live outside the canonical catalog and must not
be used by new producers.

## Known Gaps

- Tool-call intent is embedded in `llm.ai.response.content.tool_calls`; it is
  not a first-class event. A missing or timed-out result may have no dedicated
  outcome event.
- `run.end.metadata.status` is only a root graph completion marker and is
  always `success`. `RunRow.status` remains authoritative for lifecycle state,
  and worker loss may leave no terminal event.
- Nested non-JSON values in `run.end.content` have backend-dependent
  representations: memory retains Python values, while JSONL and database
  stores read them back as strings.
- Loop detection and deferred-tool promotion do not currently emit middleware
  events.
- Journal attribution, token accounting, and external tracing metadata still
  depend on manual instrumentation at several LLM call sites.


## Optional remote stream frames

`stream.frame` in category `stream` records the actual SSE frame as a structured
`{"event": name, "data": value}` envelope. Event names preserve subgraph namespace
suffixes. These rows are excluded from message-history projections. Private
attempt identity and delivery state belong in Fleet tables, never public content
or metadata. The event row and private pointer commit in the original writer
transaction; Redis receives only committed run/attempt/sequence delivery hints.

For `events` and `debug` frames (including namespace suffixes), the full envelope's
UTF-8 size uses the same `json.dumps(default=str, ensure_ascii=False)` serializer
as DbRunEventStore, including its default spaces and envelope overhead. Frames
over `max_trace_content` reject before either persistence or publication; no
JSON truncation or metadata bypass is allowed. `values`, `messages-tuple` and
`updates` retain their payload without a new global 10240-byte transport limit.

Remote cursors are canonical `fleet.v1.<run-base64url>.<attempt-base64url>.<seq>`
with unpadded UTF-8 base64url components and a positive decimal bigint sequence.
An absent cursor requests complete history for the accepted original identity.
Malformed, foreign, stale or deleted cursors return HTTP 400 before any SSE
headers. A retained cursor or cursorless request returns HTTP 410 with detail
`Remote stream history is unavailable` if a matching original private SSE
pointer after the consumed cursor has lost its matching public `stream.frame`.
This applies to active and sealed runs. A deleted public event cannot be
recreated from its pointer. Retention at or before a retained consumed cursor
does not block resuming the remaining tail; semantic journal sequence gaps
are valid and do not imply missing stream history.

Preparation validates all remaining matching history once before headers.
During a subscription, the reader selects at most128 original private pointers
before left-joining public frames, so a missing host frame occupies its pointer
slot rather than being skipped by pagination. Pre-yield existence checks only
cover the current prefetched pointer window and one exact original authority
pointer; they do not repeatedly scan the entire remaining history or hydrate
launch payloads. A mapping change closes the original subscription before its
next cached frame is emitted.

Retention within the current prefetched page closes it with EOF and no synthetic
END. Missing frames in a later page are detected when that page is fetched;
healthy earlier pages may still be delivered. Reconnection reports 410 when its cursor remains valid, or
400 if that cursor itself was deleted. A real seal with no frames still allows
END. A sealed run emits END only after an empty pointer page and a matching
consumed seal highwater. The existence checks read pointer identity/sequence and
host row matching, not the full event payload. Publisher target lookup remains separate: a missing
host frame is not acknowledged or reconstructed.
The optional private sequence floor preserves monotonic allocation after host
retention. Local cursor and gap rules remain unchanged.

A durable private seal stores the final pointer sequence after live original
record finalizing clears and SQL is terminal. The reader drains frames through
that sequence before emitting the existing idless `end` with data null. Trusted
accepted physical-stop observations can separately recover an omitted seal;
unknown/quarantined or unproven stops never imply END and never restart an Agent.
