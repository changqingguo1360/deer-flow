# C10 routing and scheduler capacity contracts

C10 has passed local acceptance; see [the acceptance report](ecs-fleet-c10-acceptance.md). The native ASGI HTTP/PostgreSQL main and directly affected permission/recovery/capture checks have passed locally. This is native routing/ticket acceptance, not a new Docker Agent execution. Public Gateway startup still rejects `agents_enabled`; C11–C12 and the C→B→C acceptance remain pending.

## Execution selection

Run creation and scheduled-task creation accept a typed `execution` object:

```json
{"execution": {"preference": "remote", "profile": "remote"}}
```

`preference` is `local`, `remote` or `auto`. Auto without a profile defaults to Local. Remote requires a permitted agent profile. A deployment-owned `agent_bindings` entry supplies explicit user grants, model/version, runtime/skill/plugin compatibility, secret references and continuation budget. A client or Node declaration cannot create that grant. Original bound resume keeps its accepted backend; conflicting explicit selection is rejected.

The authenticated owner remains authoritative. Server-owned backend, placement, token and lease hints are removed from user context; client `non_interactive` cannot turn an ordinary request into a trusted scheduled run. Only the original trusted Scheduler launch uses scheduled non-interactive context and dispatch-time recursion limits.

## Initial input snapshot

Before taking admission SQL locks, Gateway captures that owner's thread `workspace` and `uploads` into the existing immutable NAS manifest contract. Traversal uses descriptor-relative no-follow opens, rejects hardlinks and special files, bounds entries and bytes, and checks file identity around copying. The existing worker snapshot reader validates the manifest and content. Retrying a retained scheduled run uses its accepted original manifest rather than redefining inputs from later host edits.

## Scheduled capacity

The original Scheduler global budget lock and parent/occurrence locks remain the admission entry. Without eligible capacity, an occurrence stays `queued`, retains its queue age, creates no scheduled core run and consumes no execution budget.

An eligible Node needs a current online session, an allowed profile, matching installed compatibility and available resources. Compatibility comes from the existing claim advertisement, is fenced to that session and is cleared on session rotation. The operator binding still owns permission.

A short scheduler ticket creates one reservation in the same B/C resource ledger. Original run admission consumes it in the same transaction as the run and placement; the occurrence's stable admission key prevents duplicate runs. Node claim creates the real Attempt and transfers that reservation, avoiding a second allocation.

Expired unassigned tickets release only capacity that was never assigned to an Attempt. A retained pending run is requeued with its run ID and queue age intact. Once an Attempt exists, physical STOP remains the release authority. Pause, deletion, queue timeout and cancellation must retire proven never-started work without leaving a live thread admission lock; the callback leaves Local lifecycle handling to the original repositories.

## Verification boundaries

Evidence is retained under `.local/fleet-evidence/c10/`. The final-source complete main passed in1.96s; its35-file source freeze matches its retained receipt; final37-file hashes are independently recorded after the narrow regression delta. Two necessary aggregate checks exposed and fixed no-attempt cancellation, original-input expiry retry and replacement-ticket rollback. Strengthened checks passed in8.80s: same original run/snapshot, committed replacement-ticket consumption, pause/delete/queue-timeout retirement and retention of assigned capacity without STOP. Cache HTTP status verification passed1/1.82s and the existing bound-resume regression passed1/3.40s. An intermediate aggregate failed on a wrong cache field assumption; its original failure is preserved and the exact external-case follow-up closes it. SPEC and QUALITY reviews are Ready. The full-backend run was interrupted under the user’s corrected test scope; blocking-I/O passed75. Six exposed existing failures were closed by targeted checks (5 passed/41.45s plus1 passed/2.78s). Whole and narrow SPEC→QUALITY are Ready; the interrupted run is never represented as a full-suite PASS.

Never-assigned cancellation proves that no computation received authority; it does not manufacture an Attempt, STOP acknowledgment or C07 stream seal. Existing C07 stream identities depend on an Attempt. Pending/never-started SSE behavior remains an explicit public activation gate, and the current cancellation check does not prove terminal SSE END.

Implementation detail and remaining checkboxes live in the [C10 plan](superpowers/plans/2026-10-06-ecs-fleet-c10-routing.md) and [OpenSpec tasks](../openspec/changes/add-ecs-remote-agent/tasks.md).

Review fixes also have exact evidence: claim/cancel handoff passes with durable interrupt intent while assigned capacity remains held; bounded capture consumes at most limit+1 entries and closes source descriptors when destination acquisition fails. Their RED and GREEN logs remain separate. No Runner image was rebuilt for these host changes.

Verification scope correction: the whole-backend run was interrupted at the user’s request to reduce testing scope; it is not a full-suite PASS. All four C10 cases passed before interruption, and blocking-I/O completed 75 passed. All six failures from existing Fleet cases have targeted passing rechecks. No full-suite continuation is planned; C10 is locally accepted under this corrected scope.
