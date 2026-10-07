> Historical slice acceptance: preserve original cases, counts, source/image identities and scope below. Current B→C→combined status is in [the delivery record](ecs-fleet-delivery.md); later completion does not turn this record into a fresh current full gate.

# C07 committed remote events acceptance

C07 is accepted locally on 2026-10-03 after sequential whole SOURCE and final runtime SPEC/QUALITY reviews and independent Root verification. This slice implements durable remote event delivery and replay. C08–C12 and B/C continuations remain outstanding; Gateway remote admission stays closed. No production ECS deployment is reported. The existing cumulative 120-second cleanup deadline is unchanged.

## Accepted behavior

Actual serialized SSE frames and private outbox pointers commit together under the original execution fence. Local event stores retain their existing behavior and do not import optional Fleet code. Migration `f0008_event_outbox` appends only private Fleet tables. Thread sequence allocation includes retained private pointers, preventing sequence reuse after public retention.

Readers replay committed PostgreSQL frames with canonical immutable run/attempt cursors; Redis supplies optional wake hints. Cursor errors return HTTP 400 before headers. Missing unconsumed history returns HTTP 410 before headers; loss or identity change during a stream closes it without END. Bounded 128-pointer pages preserve semantic sequence gaps and consumed-prefix retention. Exact namespace and payload JSON survive replay; the complete persisted trace/debug envelope obeys the configured UTF-8 byte limit without imposing a new limit on values/messages.

END requires a matching immutable terminal seal after its tail is consumed. A live seal requires the original registered record, terminal SQL status, settled finalization and retained ownership. Missing seals recover only from authenticated accepted physical-stop evidence in the original stopped transaction or trusted startup scan. Unknown, quarantined, expired or unproven exits do not synthesize END. Per-run publisher serialization, decimal Redis sequence comparisons, bounded delivery, lost replies and ACK retries preserve idempotent hints without holding execution locks across Redis. Owned publisher/recovery tasks settle before resources close.

## Source, build and installed process evidence

Pre-slice HEAD: `a78c7c962ad0cc0a9cefb6823c1e8f21d907457b`. Frozen v9 source: 1,597 files and 18 build inputs; JSON SHA256 `f8ec819c05ec8525ebf32e16a8a459c4f985974559a33a420b3a70082dc498c0`. Historical SOURCE preparation flags remain unchanged; actual build and runtime records establish the later execution stages.

Fresh six-wheel Runner image: `sha256:3ec2730fef8d751f0e37187251a6c038cc9ef49e26fbb9239f4c288bd90d69ae`. Provider: `sha256:a30f2da9421be72d2de70e06b76aa220bc2ecb75d2dd8fe0ad5283dda7ed0115`. Original accepted B Worker image remains `sha256:c307f97d272054ed15a08476208d03311e10c3f893ab1f1ee8d3eae8376d2ea8` and was not rebuilt.

Final manifest SHA256: `35a84b8d84afcb7cfe235f5f5063b76ebca0a25efed437576ad10efcf1097ea1`. Whole-gate sidecar: `1b25d536cbdf89990052f100ae53a160419306cd8ed9fb895f17dfcb7c082867`. Final handoff: `a4c52620153f1d2da657aecd4a1641cec912b4329adaaf2a4b92309d1e3cd447`. All evidence lives under `.local/fleet-evidence/c07-restart-2e1633848e2b/`; final files and Root logs are in `formal-v9/`.

Six complete wheel inventories match source, including packaged guides and installed metadata/entry points. Sixteen complete byte proofs cover two never-started image inspections, four implementation C07 processes, six actual neighboring stopped processes and four independent Root C07 processes. Root verified 14,138 hash comparisons; this count includes source/build/wheel checks as well as installed payload comparisons. Independent SPEC verified 752 wheel source payloads and 11,748 installed payload comparisons.

The four independent Root cases are cached/hydrated × writer seal/omitted seal. They use NodeDaemon/AgentContainers and the original `python -I -S /opt/deerflow/libexec_bootstrap.py` at PID1, read-only root, no installed-path override, one execution start, zero restart and natural exit 0. Actual DB/HTTP/process receipts show ordered unique replay and recovered terminal END. Omitted-seal cases prove safe natural exit followed by authenticated physical-stop seal recovery, not arbitrary SIGKILL recovery.

## Actual validation scope

| Gate | Actual result and execution scope |
| --- | --- |
| Independent Root v9 installed C07 | 4 passed, zero failures/errors/skips |
| Implementation v9 installed C07 | 4 passed, zero failures/errors/skips |
| New-image C01–C06 neighbors | 15 passed, zero failures/errors/skips; all image-dependent parameterizations |
| Current-source ExtensionManager installation | 1 passed, zero failures/errors/skips; B01 package installation, not old Worker execution |
| Root native C07 | 89 passed, zero skips; executed v6, covered source unchanged |
| Root C01–C06 | 742 passed, zero skips; executed v6, image-dependent cases rerun v9 |
| Root Local neighbors | 422 passed, zero skips; executed v6, covered source unchanged |
| Root original B gate | 261 passed, zero skips; executed v8 with unchanged original B image |
| Guide/observer correction | 223 passed, zero skips; actual targeted v9 correction |
| Normal `make test` | 13,310 passed, 913 default-suite skips, 1 deselected, 19 warnings; 284.06 seconds |
| `make test-blocking-io` | 75 passed; 3.91 seconds |
| Boundaries | 74 passed, zero skips |
| Ruff / format | Clean; 1,441 files already formatted |
| Agent guidance | 24 guides, zero errors, two existing AG002 effective-chain soft warnings |
| Strict OpenSpec / diff | Three changes passed; diff check clean |

Root driver naturally exited 0 and checked frozen source/input hashes before and after every gate. Normal backend execution used ordinary repository defaults. Its optional skips do not substitute for the explicitly executed required PostgreSQL/container cases. Historical successful scopes retain their actual versions and original reports; final reports do not relabel them as v9 executions. Earlier genuine REDs, setup errors, failed intermediate builds and whole-suite regressions remain separately documented in implementation progress.

Guide files all fit their individual soft budgets. The Gateway effective chain is 83,993 bytes and runtime chain 85,226 bytes, above the existing 81,920-byte soft limit and below 98,304-byte hard limit; strict-warnings is not claimed. Saved PG/Redis resource handles are historical identity records, not fresh liveness assertions.

Final runtime SPEC and QUALITY approval and Root acceptance are recorded in implementation progress. Subsequent acceptance bookkeeping changes only nonpackaged documentation and OpenSpec task tracking; production Python, test files, packaged resources, build inputs and wheel bytes remain unchanged.
