# C11 local acceptance

C11 source is frozen against C10 baseline `74003058d607e1f389d3cbf071c15ba44aa0c4f6`.
Whole SPEC→QUALITY is Ready with no findings. Root independently matched the20
frozen source/test/fixture hashes and inspected actual logs, receipts and observations.
C11 is locally accepted and committed as `f9fb8d3ada2751555310ac126e605754a012d310` (32 explicit files). Public remote activation stays closed. C12 and C→B→C
remain required next work.

## Behavior and authoritative evidence

| Requirement | Evidence and scope |
|---|---|
| Owned nonsecret summaries | Mounted session-auth HTTP with actual PostgreSQL rows; legal list/detail, strict11-field allowlist, foreign owner/thread404, absent optional runtime[], unready503. |
| Admission drain | New remote503 after flags close; accepted queued claim, original cancel/renew/STOP/reconcile and owned query remain available. Native host protocol only. |
| Physical state | Queued/not_started; running/resources held; accepted cancellation retains capacity; durable STOP/reconcile gives recovery-required with released reservation. Terminal+held projection remains unconfirmed. |
| Local parity | Original RunManager/run_agent/lead-agent graph, durable success and actual checkpoint message with C closed; only external model deterministic. |
| B parity | Directly affected existing B public-status/presentation checks; unchanged original full B/container acceptance retained at [B acceptance](ecs-fleet-b-acceptance.md) scope. |
| Never-assigned SSE | Queued heartbeat→original committed cancellation→END; reconnect END; foreign owner/inconsistent terminal pair cannot prove END. Assigned history/seal remains unchanged. |
| UI | Actual recorded HTTP JSON feeds existing panel; C visible with MCP off; original-run cancel; held terminal is stop-unconfirmed; MCP-on existing C does not show empty hint. |
| Production recipe | Offline hashed approved artifacts, installed bootstrap/collector, isolated nonroot entry, deployment instructions. Actual new recipe image build/runtime not executed; required C12 gate. |

## Finite execution results

Exactly3 new backend cases and2 new UI cases. Main before neighbors; no full backend
or all-Fleet run for this slice.

- Genuine main RED:1failed/3.56s after legal admission200, missing summary route404.
- First main GREEN:1passed/2.41s with historical3ec image as native metadata; no
  process/container execution. This input provenance remains explicit.
- Corrected-input native main-v2:1passed/3.02s. Final native main-v3 after projection
  code fix:1passed/3.92s, naturalexit0. Actual persisted Attempt launch spec image
  `sha256:960f86678b81020b3f850bcf0ab0ae1674a6f9ab62ee92730fb53ae13b5bc444`, runtime
  `sha256:f441b654dc0cada31ca1955768dfd5d321db27dfaace2cfb8a4ca0a5e30c5aef`.
- SSE genuine RED:1failed/2.38s, StopAsyncIteration instead of END; GREEN1/2.52s.
- Authorization/real Local:1passed/2.05s.
- Neighbor backend batch:1failed/9passed/4.42s. Failed source-mirror recipe case
  later failed again and was removed as low-value implementation mirroring.
  Original batch and recheck remain FAILED, not reclassified. Actual production
  build/ABI/runtime proof remains C12.
- Original focused UI:4files/16passed (2new+14existing), original tool-capture
  structured receipt only; full stdout was not saved locally. Final affected DOM:
  1passed/3.672s, no skip, naturalexit0. Final frontend check naturalexit0.
- Final backend Ruff/format/diff checks naturalexit0. Guidance24/0errors/6softwarnings;
  strict OpenSpec3passed/0failed.

## Provenance and cleanup

Retained evidence: `.local/fleet-evidence/c11/final-handoff-v1/` including actual JSON,
original failures, attempt-history, exact command/exit receipts, frozen20-file source
manifest and final fixture-owned cleanup. Root independently matched all20 hashes in
`root-source-verification-v1.json`. Intermediate overwritten logs are explicitly
listed as not retained; no reconstruction is represented as original bytes.

Thirteen fixture-owned schemas were absent at final observation. Unowned schemas
were not mutated; this is no claim that all historical test schemas are absent.
Native admission/STOP calls prove host protocol, not actual Agent process launch or
physical exit. C09 container evidence retains its own original image/source scope.
No pull, install, global prune, real configuration activation, push or merge occurred.

Whole SPEC and QUALITY reviewers performed read-only reviews without new tests or runtime mutations. No open C11 findings remain.
