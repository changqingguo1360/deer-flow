# BC05 shared scheduling runtime (in development)

BC05 is being implemented against BC04 source `f24a88d7142e83a4a57077a1cafaa4411c067d42` and receipt `529819da84d31df4ede2caf971d3e5463a5fffaf`. It has not passed local acceptance. BC06–BC10 remain required; operator activation and ECS deployment are outside the current authorization.

The current [implementation plan](superpowers/plans/2026-10-06-ecs-fleet-bc05-shared-scheduling.md) requires one installed C→B→C main first, then one concentrated necessary boundary. Initial main RED reached the original authenticated node claim route: `kind=mixed` returned 422. That failure proves the missing mixed claim interface, not an executed container sequence. Preliminary image builds do not establish installed acceptance. The first installed GREEN attempt exited 1 after 10.25 seconds at compatibility preflight: the newly declared Fleet plugin did not match the reused empty-plugin workspace contract. No C/B claim or container spine ran; correcting this approved asset does not relax the original validator.

## Shared admission contract

With `continuations_enabled`, B claims, C claims and new scheduler tickets share a durable `SchedulingRow` singleton and the existing unreleased reservation ledger. Original execution or scheduled parent/occurrence locks precede the singleton; common node locks are acquired in node-ID order. Read-only candidate discovery does not lock the opposite execution queue. A committed new reservation advances the category turn; rollback and consumption of an already charged ticket do not advance it twice.

Categories alternate when both have genuinely eligible work, with FIFO within each category. Candidate eligibility includes current execution identity, cancellation/deadlines, worker session/capabilities, compatibility, operator profile allowlists and actual capacity. Keyset discovery must not permanently hide eligible work behind an incompatible first page. Capability advertisement indicates worker readiness and does not grant operator permissions.

`reserved` mode protects one designated standard B profile on one actual eligible B node. The B quota may already be used by B; an additional permanently idle B slot would cause C starvation under continuous B arrivals. The quota check excludes C/ticket allocations from that node's total resources. The independent physical fit check counts every unreleased B/C/ticket/unknown allocation, so this quota rule never frees charged resources or permits overselling. CPU and memory cannot be added across separate nodes to fake a usable B quota.

`serial` mode is explicit and permits at most one unreleased B/C reservation across the pool, including held tickets and unknown work. It is not an automatic fallback from resource shortage. C uses the original durable yield, publication and matching-process STOP/release path before B can execute; results use the original continuation admission path to queue the next C run.

## Worker and private Runner composition

A mixed worker uses one original node session, daemon, journal, lock and container owner. Per-kind preparation and completion reuse the existing B and Agent implementations. Session renewal clears old capabilities and Agent compatibility; job-only advertising cannot retain stale Agent readiness.

The installed private Runner must strictly validate the approved plugin snapshot. The built-in Fleet control-plane installer is represented by the exact `ecs-fleet` / `deerflow-ecs-fleet` / `deerflow_ecs_fleet:install` identity. Its private submission and yield capabilities use the existing bound driver; generic Gateway plugin installation must not start Fleet reconcilers or migrations in the private Runner. An unexpected identity using that installer is rejected; other approved plugins keep their original path.

The original Agent execution task must inherit the bound Fleet/yield scopes before graph construction and tool execution. A neutral optional `AgentEnvironment.execution_scope` supplies these bindings after the original mutation/workspace scopes and before executor creation. Setup/cleanup cancellation settlement scope does not extend into normal execution. main-green-3 exposed the missing scope through an actual invalid-tool reply; the earlier timeout explanation was withdrawn.

The independent original control observer retries only a `TimeoutError` from its single bounded read. Each read retains the original one-second limit; retry neither grants write authority nor renews a lease or execution budget. `read()`/`prepare()`, writer-stop and settlement failures, actual ownership rejection, database errors and external cancellation retain their original behavior. The installed main exposed this specific timeout through sanitized exception-type/call-frame diagnostics; the passing installed main qualifies the retry on the exercised runtime path, while the native boundary retains actual SQL authority rejection.

## Evidence limits

The installed main and concentrated native boundary passed at their recorded scopes; final runtime image/source alignment and changed-file static checks passed. Independent SPEC returned SpecReady. QUALITY and the final source/documentation receipt remain pending; BC05 is not yet accepted. See the [qualification record](ecs-fleet-bc05-acceptance.md) for actual evidence and its limits. BC10 still owns the final combined delivery gate; BC05 evidence must report the exact startup, container, SQL, STOP, workspace and cleanup scope that actually ran.
