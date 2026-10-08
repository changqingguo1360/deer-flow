# C12 production Runner and release gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. One implementer owns source/tests/runtime handles; Root owns shared docs, OpenSpec, evidence and commits. Steps use checkboxes.

**Goal:** Deliver C through normal Gateway startup and a production-recipe Agent image, then prove the directly affected delivery/revocation/restart boundaries before beginning C→B→C.

**Architecture:** Preserve pre-import host configuration checks, replace the obsolete unconditional C rejection with post-start validation of the actual ready durable participants. Build `docker/fleet/agent.Dockerfile` using frozen approved artifacts and a digest-pinned audited Linux base; stock Node/AgentContainers launch its installed gateway environment. Actual PostgreSQL, original run_agent and provider/tool loop, durable SSE/artifacts and STOP/resource release provide the main proof. Historical fencing evidence remains at its original scope and is reused only after source/input matching.

**Tech Stack:** Existing FastAPI create_app/lifespan/session auth, PostgreSQL/checkpointer/Store, Redis publisher/fault proxy, original Fleet daemon/Docker runner, offline uv wheels and BuildKit named context.

Baseline `cffccd599bc0983b2e7060d1c6d207fd088ba08b`, C11 source `f9fb8d3ada2751555310ac126e605754a012d310`. Approved scope is OpenSpec `remote-agent-operations` C12 and first-principles C release requirements. Real operator config stays untouched. This is local isolated release evidence, not production ECS deployment or permission to enable real user configuration.

## Source audit and main-first scope

- [x] Audit completed read-only: create_app calls `validate_fleet_plugin_configuration` before extension import; the agents branch validates shared storage then unconditionally raises. Actual service start/ownership/events precede RunManager, orphan reconciliation and heartbeat in `gateway/deps.py`.
- [x] Production recipe exists but is unexecuted. C09 fixture image/context uses six production/fixture wheels, no-deps installation and COPY fixture modules, so it cannot stand in for this build.
- [x] Current original Python base is available at `python@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f` (arm64). Cached dependency base is `deerflow-c08-dependencies@sha256:da399e40963d6d8ad1beff9bbe592e40a327f7417b14c6e929e4c61a4804ff5c`; inspect its actual distributions before using it. No complete cp312 C wheelhouse exists in host cache; B cp314 binaries cannot substitute.
- The user explicitly requires one real main before subordinate checks, few cases and no whole-backend/all-Fleet repeats. Use one C12 main and one concentrated fault neighbor initially. Reuse valid C06–C09 evidence with exact source/installed-byte scope rather than mechanically re-executing every historical case. Only actual failures/source changes/unresolved requirements justify another check.
- Existing C06 proves supported transactional memory, repositories, Store/definitions and extension fencing; C07 proves actual Redis faults and durable tails; C08 proves accepted workspaces/artifacts/process quiescence; C09 proves real partition/stock Node restart/residual stops. These do not prove normal startup or this new production image. A stateless/no-op memory main must never be called proof of transactional memory mutation; retain and match the separate supported-memory evidence. Unsafe memory/extension profiles continue to fail closed.

## Files and responsibilities

| File | Responsibility |
|---|---|
| `backend/app/fleet/runtime.py` | Retain pre-import shared-PG/events/heartbeat checks; replace obsolete rejection with an actual post-start C readiness validator. |
| `backend/app/gateway/deps.py` | Invoke validator after original RunManager/heartbeat initialization and before serving; register teardown so rejected startup releases owned resources. |
| `docker/fleet/agent.Dockerfile` | Original production recipe; change only if an actual build/installed-contract failure requires it, never COPY runtime fixture modules. |
| `scripts/fleet_agent_build.py` | Operator-oriented preparation/build of frozen local approved artifacts using this recipe; no test-provider hardcoding, implicit pulls or online dependency resolution. Verify source/wheel/lock/context inputs and record actual build identity. |
| `backend/tests/fleet/c12_integration_fixture.py` | Isolated actual create_app/lifespan/PG/Redis/HTTP model and stock Node lifecycle, private config/credentials, observed SQL/process receipts and owned cleanup. Reuse original helpers; never assemble app.state as a substitute for startup. |
| `backend/tests/fleet/test_c12_remote_agent_operations.py` | One production main, then one concentrated delivery/revocation/restart fault case; only add a case for an actual uncovered requirement. |
| `scripts/fleet_c_gate.py` | Explicit C prerequisites, a fresh report, required case selection without broad whole-stage globs, nonzero failure/skip rejection and inspectable provenance for reused evidence. |
| `docs/ecs-fleet-c12-runtime.md`, `docs/ecs-fleet-c12-acceptance.md` | Actual supported startup/image/runtime boundary, evidence/limitations and release requirement coverage. |
| README, backend/Gateway AGENTS, deployment, original C plan, roadmap/progress and OpenSpec tasks | Root-owned actual capability/status updates within guidance byte limits. |

## Task 1: Actual production main

- [x] Prepare the main through actual config loading and normal create_app. Use a new private temporary config/NAS sentinel/UUID PostgreSQL schema and trusted installed Fleet plugin record, shared PG checkpointer/Store, DB run events, ownership heartbeat, approved explicit profile/binding and a real user session. Do not monkeypatch startup validation or replace run_agent/RunManager. The first required capability assertion is normal startup of valid C configuration; the current obsolete rejection is the genuine RED. Missing package/config/service/image is setup failure, never a passing RED.
- [x] Run only the main and preserve the exact RED log/receipt without overwriting:

```bash
backend/.venv/bin/python -m pytest backend/tests/fleet/test_c12_remote_agent_operations.py::test_c12_production_gateway_runner_main -q --tb=short
```

Run from repo root with the cached backend test Python import conventions, explicit isolated TEST_POSTGRES_URI/local Docker opt-in and NO_PROXY for loopback. Secrets are private fixture files; never print environment or command-line credentials. If normal pytest invocation requires backend cwd, use the same absolute interpreter and `tests/fleet/...` node ID from backend.

- [x] Implement actual startup readiness. The obsolete branch must retain this preflight:

```python
if plugin.config.get("agents_enabled", False):
    if host_config is None:
        raise RuntimeError("Remote Agent requires actual host persistence configuration")
    validate_remote_agent_host_configuration(host_config)
```

Before runtime serves, validate the original app's ready Fleet service/session factory, installed ownership/routing/control/ticket/workspace participants, durable run store, database events and actual shared PG checkpointer/Store selection, Fleet gateway stream bridge and initialized RunManager heartbeat. Validate compatible configured memory/extension contracts through existing preflight boundaries. Do not call `/opt/deerflow` compatibility on Gateway: the installed image computes it. Preserve B/Local behavior when agents are disabled, and fail startup if a requested C participant is missing rather than silently routing Local.

- [x] Inspect the pinned dependency base's actual inventory and the four current production wheel METADATA. Prepare immutable approved model/runtime/workspace JSON and skills. Prefer the actual installed `langchain_openai:ChatOpenAI` provider with a local OpenAI-compatible HTTP response fixture outside the image; this executes the original installed model adapter/graph/tool loop without test-provider code in the image. Model API credentials remain in private scoped bootstrap. Do not disable original credential or model-binding checks to reach the fixture endpoint.
- [x] Resolve the missing offline closure only from verified base distributions/local compatible wheels. If actual missing wheels require retrieval, freeze exact versions/hashes and record that concrete prerequisite; do not silently resolve newest versions. A preinstalled dependency base is acceptable only with an explicit pinned image and verified installed closure; the recipe still runs hash-locked offline installation and pip check. Document which packages are base-installed versus supplied wheels.
- [x] Run the original recipe with the named frozen context, without network or pulls, retain actual stdout/natural exit/build inputs/image ID:

```bash
docker buildx build --load --network=none --pull=false \
  --build-arg AGENT_BASE=<audited-name@sha256:digest> \
  --build-context agent_artifacts=<absolute-frozen-context> \
  --file docker/fleet/agent.Dockerfile --tag deerflow-agent:c12-main .
```

Inspect actual installed four distributions/unique gateway entry point/bootstrap/collector, approved artifacts and computed installed compatibility. Use the resulting exact image and compatibility in the actual test profile; no environment-string-only claim or old fixture image substitution.
- [x] Start the normal Gateway lifespan and real authenticated HTTP transport, register/credential the owned Node through supported routes, and run the stock daemon/AgentContainers against the new image. Submit an owned remote run through the ordinary run endpoint. Observe original claim/start, actual parent/tool process, committed checkpoint/Store/outbox/terminal facts, accepted artifact bytes, owner-scoped SSE tail/END and real STOP/released resources. Compare the same deterministic task with original Local execution. Save actual SQL rows/public JSON/process/image evidence; never write expected-valued evidence.
- [x] Run main to natural GREEN before adding fault neighbors. Preserve every attempt uniquely; observation timeout resumes a live handle, never restarts it. No broad suite, image matrix or formatting-only main rerun.

## Task 2: Concentrated release fault neighbor

- [x] Reuse existing RedisFaultProxy and original installed delivery logic: interrupt actual Redis transport after durable outbox commit; confirm original durable tail/terminal survives, restart delivery and observe ordered nonduplicate hints/END. Do not fake Redis state or redefine DB authority.
- [x] Use the production main's original run/attempt identity and real stock Node to revoke/partition a running execution, observe an actual late mutation rejection, and inspect every relevant SQL table plus physical parent/child and resource ledgers. Stop/restart the owned stock Node with its durable journal; residual stop precedes new claim/retry. No second writer or stale checkpoint/memory/Store/outbox/terminal/accepted artifact mutation is allowed. Reuse unchanged transactional-memory fencing proof explicitly where this runtime profile has no memory writes, with source/input matching and no blanket all-profile claim.
- [x] Run only the focused fault case:

```bash
backend/.venv/bin/python -m pytest backend/tests/fleet/test_c12_remote_agent_operations.py::test_c12_production_runner_revocation_and_delivery_recovery -q --tb=short
```

Required integration cases execute with no skip. C12 evidence must name actual participating mutations/tables and inherited proofs, not merely a hardcoded stale_mutation_count=0.
- [x] Create explicit gate selection for this main/fault proof and necessary already-existing directly affected checks. Fresh report only, reject empty/failure/skip/missing required case; report reused proofs separately with their real scope. Do not copy B's all-phase glob selector or accept old XML as a newly executed result.

## Task 3: Review and acceptance

- [x] Root audits every C12 release requirement against actual source/installed image/process/PG/Redis/HTTP evidence and preserved C06–C11 scopes. Missing evidence means pending work, not release approval. Actual Local/B parity and unsafe profile rejection remain requirements; do not silently shrink C to stateless-only execution.
- [x] Changed backend Ruff check/format, diffcheck, guidance and strict OpenSpec. No frontend change is planned; do not repeat unrelated UI tests. Source change triggers only its affected proof; docs/format alone never trigger image rebuild.
- [x] Whole fresh SPEC→QUALITY, fixes for actual findings only, final source/evidence audit and explicit C12 slice commit. Then mark OpenSpec12.1–12.4 and document C completion. No real config activation/push/merge implied. C→B→C remains active required work; start its main only after full C release acceptance.

## Evidence ownership

Root retains unique command/exit receipts, genuine RED/GREEN, actual public/SQL/process observations, original source/wheel/installed/image digests and isolated cleanup under `.local/fleet-evidence/c12/`. Keep private config/credentials outside images and NAS. Do not drop unowned schemas, prune global Docker, overwrite previous evidence or relabel interrupted/failed historical batches as PASS. Root and the implementer use the authorized feature checkout explicitly; the default desktop cwd is a different checkout.


Startup capability RED observed and inspected by Root (2026-10-06): one main test
failed naturally1/2.34s at create_app→validate_fleet_plugin_configuration's obsolete
unconditional rejection, after actual valid AppConfig shared-PG/events/heartbeat/NAS
checks. `.local/fleet-evidence/c12/main-red-01.log` retains the failure. This initial
phase proves missing startup capability, not a PG connection or image execution;
normal full lifespan and actual production image proof are still pending. No neighbor
case has started. The same main will be extended to the real chain before GREEN.


Actual production recipe build01 observed (2026-10-06): the original recipe hash
`fffbb8a0a4231162379f83698108c0a4b2870ccc41445433c585ae6820dd4e58`
passed its frozen artifact checks, offline require-hashes installation and pip check
("No broken requirements found"), naturalexit0. Actual built image receipt:
`sha256:ec94965910d2d2a570a1c98a7396e41ff240fc77c88c008c81ac540f70b6f6e5`.
The existing host uv override explicitly retains websockets16.0 despite SDK0.4.2's
metadata upper bound. This independent Agent image uses a frozen official15.0.1
wheel to satisfy its actual closure; host pyproject/lock/environment remain unchanged.
The initial closure audit failure remains recorded. BuildKit metadata observation
was slow, but the original session95199 naturally completed; no timeout-triggered
restart or OCI recipe adaptation occurred. Root matched original recipe/frozen
manifest hashes and inspected actual pipcheck/build output. Installed compatibility,
normal full lifespan and real Runner main remain pending; no fault neighbor started.


Main progress audit (2026-10-06, attempts02–05): actual installed compatibility
is recorded as `sha256:3a9a814458a996e6fb2be2d4451fb0d53419c97ba89b63cd4a1628b6ba8009c0`
in attempt02. Normal Gateway lifespan, session/CSRF authentication, supported Node
registration/credential routes and owned remote admission executed. These are
partial observations, not main acceptance. Attempt02 bootstrap failed and its
original container log was not captured; preserve that provenance gap. Attempt03
exposed the disabled-memory configuration still selecting an unsupported default
manager; the isolated main now explicitly selects the supported stateless noop
profile. This does not prove transactional memory mutation. Attempts04–05 reached
the installed provider/tool loop and durable terminal path, but failed naturally
(exit1) on incorrect HTTP-model fixture expectations: successful empty bash stdout
and the actual present_files response. Root inspected attempt05's failure at the
fixture's expected filename in tool output; actual response was `Successfully
presented files`. Main remains pending. Only the same main case is being corrected;
no fault neighbors, test matrix or broad suite have run. The production image is
reused unchanged. Store startup presence is not a claim of actual Store writes.


First actual main GREEN (2026-10-06): `main-attempt-06/test.log` records
1 passed in17.56s, `command-receipt.json` records naturalexit0. Root inspected
the executed case and actual `main-observations.json`: production image ec9496
and installed compatibility3a9a81, task/run/placement succeeded, accepted/final
workspace pointers equal, Docker exited0/Pid0, Node report settled, stopped_at
persisted and reservation released. Actual artifact SHA256 is
`b033213a4c522e4a1d2d2ab8aeef793ff2760e175e76bfd5c5853aef9cb045a2`.
The original ChatOpenAI adapter made three remote and three Local provider calls
with the same bash→present_files→final choices; actual artifact bytes matched.
The executed case also verifies committed checkpoint content, nonempty DB outbox
and owner HTTP SSE END. This stateless main proves no transactional memory or
Store mutation. Only the planned concentrated fault neighbor is next; C12
release/reviews/commit and C→B→C remain pending. No broad suite or extra matrix.


Concentrated fault GREEN inspected by Root: attempt05 naturalexit0/1passed40.21s.
Actual stock CLI journal restart changes container Running=true→false/exit137
and computes STOP/released only afterwards; task/placement recovery_required and
original run stillrunning preserve unsafe-recovery semantics. Actual original
checkpoint journal rejected after rotation; an independent installed-saver probe
is separately labeled. Eight immutable compared domains match; outbox excludes
only published_at and records delivery changes separately. Installed audit732
production members, two copied libexec files and three approved JSONs match frozen
inputs. Final reused-memory/Store/source scope, thin gate and reviews/commit remain
pending. No broad suite or additional case was run.

Root release evidence audit (2026-10-06): current732 packaged source members match frozen wheels;27 native participating sources match their qualified memory/Store/Redis and C11 cancellation proofs. Observed37/39/8 native PASSED lines remain native scopes within the original INTERRUPTED aggregate. Both owned C12 cases naturally passed without skips and schemas were dropped. Whole SPEC Ready and QUALITY Ready; the latter found one main log-failure cleanup issue, now source-reviewed and fixed. Original test bytes remain retained; new gate qualification permits only that main finally AST delta and rejects all other changes. Root post-review receipt verifier naturally exited0, fresh_execution=false. A final provenance SPEC supplement, static checks and the C12 slice commit remain pending. No runtime/build rerun was introduced.

Final provenance SPEC supplement is Ready; Root receipt-only verifier exit0 confirms the unique qualified main cleanup delta and retained original execution hashes. Changed-six Ruff check/format, strict remote/BC OpenSpec, guidance24/0errors/6soft warnings and diffcheck pass. Final sequential QUALITY receipt and explicit source slice commit follow; C→B→C remains required.

C12 accepted source slice: `8ac8c00a47864b03e84ca90c25a5921df412d3ac`. Final sequential QUALITY Ready; current reviewed7 technical blobs match commit. OpenSpec12.1–12.4 checked after commit. C delivery is locally complete, C→B→C starts next; real activation/push/merge not performed. Historical progress entries above retain their original pending/failure state at each attempt.
