# BC01 owned child links and sealed wait groups

> Execute through Superpowers subagent-driven-development: one source/test implementer; Root owns docs, OpenSpec, evidence and commits. Fresh SPEC then QUALITY reviews follow the verified slice. User scope overrides old broad regression examples.

**Goal:** Persist legitimate C→B ownership and immutable wait membership through the existing submission service, as the first foundation for C→B→C. BC02 cooperative yield and BC03 continuation admission remain separate required work.

**Prerequisite:** B and C01–C12 locally accepted; C12 source `8ac8c00a47864b03e84ca90c25a5921df412d3ac`, acceptance receipt docs `62845411`. No real operator activation is authorized.

## Current-source audit

- FleetJobService.submit owns the existing staged Job transaction: invocation advisory lock, optional scheduled dedupe lock, then job rows. It currently records no Agent-task/generation child ownership.
- FleetMutationCapability validates the private original execution on task→run→placement→node→reservation→attempt locks, using a fresh database clock. Its authority cannot come from model arguments or graph configuration.
- FleetTaskDriver passes the persisted invocation ID and server-issued tracking ID to the actual job service. The private Runner currently registers only the ordinary MCP driver; submit_fleet_job reads the global Fleet carrier and always uses detached mode. The source-level private Fleet bridge must be explicit, and no isolated repository should be called a complete tool integration.
- The latest migration is f0010_scheduler_tickets. The old planned f0004_continuations name is occupied by f0004_recovery. Add f0011_continuations after the current head without rewriting prior revisions.
- C12 image/source execution evidence stays bound to its original accepted commit. Foundation source edits do not require a formatting-only image build and are not retroactively attributed to that image.

## Locked implementation boundaries

1. A host BoundJobParent adapter exposes immutable parent user/thread/run/task/generation and validates the original remote mutation capability on the same SQL transaction used to insert/reuse a job and its link. Fleet modules do not import app.
2. Only the host-bound capability supplies parent identity. Validate matching current private context and request user/thread/source_run before changes and after actual flush, including waits and idempotency paths. A stale generation or lost attempt leaves no new job, invocation or child link.
3. Local/Gateway detached B submissions retain their original behavior. A requested awaited job requires enabled continuations and an authorized current C parent; absence of the private carrier must fail closed.
4. Persist job links and wait groups with owner constraints and stable unique continuation keys. Reference immutable launch identity rather than the mutable current task generation, so future generation changes do not invalidate historical links.
5. Seal sorted owned jobs only. A sealed group's membership, policy, parent and continuation key cannot change through repeated requests or raw SQL; a repeat returns the original group. Do not forbid later BC02/BC03 proof/state transitions by making the whole row unconditionally immutable.
6. Serialize shared task/generation first, preserving original capability lock order and documenting the group/job locks added for BC. No participating path may lock a child before its parent. Sorted job IDs avoid opposite-order child locks.
7. Register/scoped-bind the private Fleet driver explicitly where needed. Source availability and genuine installed full Runner execution are distinct claims; BC01 does not claim yielding, STOP barriers, resumed runs or full C→B→C.

## Execution and evidence

- [x] Inspect the existing submission service, capability, private runner tool carriers and actual migration head.
- [x] Write one real PostgreSQL main through McpTaskService→FleetTaskDriver→FleetJobService, migrated schema and actual accepted parent capability. Confirm the existing implementation fails on missing persisted child links, not environment/import errors.
- [x] Implement migration/repositories and actual service/host wiring; repeat the same main to natural GREEN. Observe persisted parent ownership, repeat seal identity and unchanged sealed membership.
- [x] After the main passes, run at most one concentrated directly necessary boundary case covering cross-user/task/run rejection, stale generation rollback and database immutability. Any further check requires a concrete source change, failure or missing requirement.
- [x] Preserve each attempt's literal argv/cwd/natural exit, actual database observations, source hashes and owned UUID schema cleanup under .local/fleet-evidence/bc01. Environment carries the existing private local test database URI; never put it in argv/output/evidence.
- [x] Changed-file Ruff and diff checks; fresh SPEC→QUALITY; Root independently inspects actual source/observations, then synchronizes README/relevant guidance/OpenSpec and creates the explicit BC01 slice commit. Only after that mark BC01 1.1–1.4 complete.

No complete Fleet/backend suite, container build, global cleanup, production configuration activation, push or merge is required by this foundation slice. No test-probe expected values may replace real SQL or service observations.

Root inspected genuine main RED (2026-10-06): original McpTaskService→FleetTaskDriver→FleetJobService created two jobs/tracking rows on a valid admitted parent, but actual fleet_job_links count was0 instead of2. main-red-01 naturally exited1, 1failed3.12s; owned UUID schema dropped. This is missing durable child ownership, not a fixture/connection failure. No subordinate case, container build or broad suite has run. Main implementation continues; BC01 remains unchecked in OpenSpec.

Final native source freeze: main-final-01 passed1/2.73s and boundary-final-01 passed1/2.04s, both natural0/no skips/owned-schema drop. Actual loaded ToolNode/private-driver path is included. Source maps match current13 changed files and both final execution maps. The direct Local detached regression has its own retained RED and corrected final boundary. The final two executions had already started before Root requested no formatting-only rerun; their original handles completed naturally and no further batch was launched. Fresh SPEC and QUALITY are Ready after whole-slice review; current source and both execution maps match. Final static checks and explicit commit receipt follow.

Accepted source commit `ce92de4ab1f47eeac25e03544019bb7d619b23dd`. Changed13 Ruff check/format and diff exit0; SPEC→QUALITY Ready; OpenSpec1.1–1.4 recorded after commit. No new runtime batch.
