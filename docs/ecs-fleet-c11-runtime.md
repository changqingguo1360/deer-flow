# C11 task visibility and admission draining

C11 is locally accepted. Native authenticated HTTP/PostgreSQL task-summary and admission-drain main, never-assigned END, genuine Local execution and focused UI checks passed. Whole SPEC→QUALITY is Ready with no findings and Root verified retained evidence. C12 and C→B→C remain incomplete. Public Gateway startup still rejects remote Agent activation.

## Owned summaries

`GET /api/threads/{thread_id}/agent-tasks` lists bounded owner-scoped summaries; `GET /api/threads/{thread_id}/agent-tasks/{task_id}` reads one owned goal. These use normal run-read permission and thread ownership. Missing optional Fleet returns an empty list; a configured runtime whose tracking is unavailable returns503.

The separate AgentTask resource reports task state/current run/generation, run status, profile/location category, cancellation intent, recovery requirement, stop state and held-resource state. It does not extend public RunStatus or serialize launch payloads, credentials, private process references or arbitrary outcomes. Cancellation includes the core run intent because assigned cancellation need not change task.cancel_requested_at.

STOP and resource release are separate from run status. A never-assigned run is not_started, not physically stopped. An assigned run with no durable STOP remains unconfirmed and may retain capacity even if its run is terminal. Recovery-required goals remain visible for user attention.

## Disable new work

Closing agents_enabled rejects new remote admissions with503. Accepted queued placements retain claim eligibility; original session, compatibility, profile, capacity, start and lease checks remain. Existing queries, cancellation, renewal, STOP and reconciliation stay installed. Closing jobs_enabled independently does not prevent accepted C from claiming. Globally unloading Fleet still requires draining accepted work first.

The main uses actual PostgreSQL and mounted session-auth HTTP. It observes queued→claimed/start/running, assigned cancellation with capacity held, new admission503 after flags close, accepted queued claim, original renewal/STOP/reconcile and continued owned query. Native STOP acknowledgement tests the host protocol; it does not prove an actual container ran or exited.

## UI, stream and release boundary

The existing task panel currently gates B/MCP presentation. C11 adds independent remote Agent summary querying to the same panel; focused UI verification passed; directly affected existing B public-status and presentation checks retain their finite original evidence. A successful run must not imply a completed waiting goal or released physical resources.

The earlier never-assigned terminal cancellation RED returned EOF without END. The focused corrected case now proves END on cancellation and reconnect. The fix requires a matching committed owned run/placement terminal pair and proof of no Attempt or committed frames. Missing or inconsistent identity cannot prove closure. Existing assigned cursor/history/seal semantics remain authoritative.

The production Agent recipe uses offline hashed artifacts and the actual isolated bootstrap/collector; its build is not claimed by native checks. C11 Gateway/UI changes do not require rebuilding the retained Agent image. Actual production recipe/runtime release proof belongs to C12, before public activation.

See [the C11 plan](superpowers/plans/2026-10-06-ecs-fleet-c11-operations.md) and [OpenSpec tasks](../openspec/changes/add-ecs-remote-agent/tasks.md). Evidence is retained under `.local/fleet-evidence/c11/`; no full-backend suite is claimed for this slice.


The final native main was rechecked after a conservative projection fix: terminal
work with any unreleased original-run reservation remains stop-unconfirmed. Its
persisted Attempt launch spec records image `sha256:960f86678b81020b3f850bcf0ab0ae1674a6f9ab62ee92730fb53ae13b5bc444`
and runtime compatibility `sha256:f441b654dc0cada31ca1955768dfd5d321db27dfaace2cfb8a4ca0a5e30c5aef`.
These are accepted native inputs, not evidence of executing that image in C11.
Exactly three new backend cases and two new frontend cases cover the slice. Original
failed recipe-mirror checks and the interrupted historical regression retain their
original status; removed mirror assertions are replaced by C12's actual build/runtime
requirement, not relabeled as passing execution.
