# C12 production Agent runtime

C12 is in progress. The isolated production-image main and concentrated fault case passed; prior-receipt gate verification passed; reviews and acceptance commit remain pending. C→B→C has not started. No real operator configuration was enabled and no production ECS deployment is claimed.

## Normal startup

The proposed startup change preserves pre-import Fleet configuration checks: a required canonical plugin, fleet_ tables, configured NAS identity, matching application/checkpointer/Store PostgreSQL identity and schema, database run events and ownership heartbeat. After the normal Gateway lifecycle installs the Fleet service, routing/ownership, event bridge, workspaces, scheduler tickets and RunManager, a readiness validator runs before serving. It checks the ready session factory, required participants and live heartbeat. Rejection unwinds through the original lifecycle teardown. With agents disabled, the additional C validator returns without changing B/Local startup.

The Gateway does not calculate the Agent's installed compatibility. The stock Node obtains it from the actual image and the existing binding/claim/bootstrap contracts validate the frozen runtime, model and workspace identities. Supported memory and extension mutation contracts continue through the original remote preflight and transaction fencing; disabling memory alone does not select a supported manager. The isolated main explicitly selects the built-in stateless noop manager. Persistent memory remains a separately evidenced capability, not a claim of this main.

## Frozen offline image

[scripts/fleet_agent_build.py](../scripts/fleet_agent_build.py) accepts approved local wheels, model-bindings.json, runtime-bundle.json, workspace-contracts.json and a skills directory. Its prepare mode inspects a digest-pinned cached base, audits active dependency requirements against base distributions plus supplied wheels, and writes requirements.lock, closure-audit.json, frozen-inputs.json and SHA256SUMS. Build mode verifies the frozen inputs and original recipe, then invokes the offline recipe without pulls. It does not download packages or choose model credentials/provider configuration.

The executed build used the original [Agent Dockerfile](../docker/fleet/agent.Dockerfile), four current production wheels and a frozen websockets15.0.1 wheel, on the audited cached dependency base. Hash-locked offline installation and pip check succeeded. This independent image satisfies the SDK dependency metadata cap; the existing host override of websockets16.0 and host lock/environment were unchanged. The HTTP response fixture runs outside the image; the image contains the original ChatOpenAI adapter and original agent/tool loop, with no copied test-provider modules.

| Input/result | Actual identity |
|---|---|
| Base | deerflow-c08-dependencies@sha256:da399e40963d6d8ad1beff9bbe592e40a327f7417b14c6e929e4c61a4804ff5c |
| Production recipe SHA256 | fffbb8a0a4231162379f83698108c0a4b2870ccc41445433c585ae6820dd4e58 |
| Built image | sha256:ec94965910d2d2a570a1c98a7396e41ff240fc77c88c008c81ac540f70b6f6e5 |
| Installed runtime compatibility | sha256:3a9a814458a996e6fb2be2d4451fb0d53419c97ba89b63cd4a1628b6ba8009c0 |

## Executed main and remaining boundary

The single main uses normal create_app/full lifespan, an owned PostgreSQL schema, real session/CSRF HTTP, supported Node registration/credential routes and stock NodeDaemon/AgentContainers. It submits an owned remote request, executes original run_agent with ChatOpenAI against a deterministic external HTTP model fixture, runs actual bash and present_files, and verifies committed checkpoint/outbox, accepted artifact bytes, durable SSE END, actual container exit and STOP/resource release. The same deterministic task executes through Local and returns identical artifact bytes and tool choices. This is execution through the installed model adapter, not a live commercial model benchmark.

The task did not mutate persistent memory or Store. Native prior fencing evidence is retained at its exact source scope and cannot be relabeled as this image's writes. The concentrated fault case also proved delivery interruption/recovery, actual original-process late checkpoint rejection and stock Node journal restart/residual STOP before resource release. Final gate verification and reviews remain required before C release. See [the implementation plan](superpowers/plans/2026-10-06-ecs-fleet-c12-release.md) and [acceptance status](ecs-fleet-c12-acceptance.md).


## Operator validation commands

Run helpers from the repository root with the installed backend interpreter.
Preparation requires an already approved local wheelhouse and nonsecret assets;
it does not retrieve packages. Use a new receipt/output path for each build.

```bash
backend/.venv/bin/python scripts/fleet_agent_build.py prepare \
  --artifacts /absolute/frozen-agent-artifacts \
  --base approved-base@sha256:<approved-base-digest>
backend/.venv/bin/python scripts/fleet_agent_build.py build \
  --artifacts /absolute/frozen-agent-artifacts \
  --base approved-base@sha256:<approved-base-digest> \
  --tag deerflow-agent:approved --receipt /absolute/new-build-receipt.json
```

For a new isolated validation run, configure DEERFLOW_TEST_POSTGRES_URL, the
production FLEET_AGENT_TEST_IMAGE content ID, absolute C12_DOCKER and a new
absolute C12_EVIDENCE_DIR privately. If TEST_POSTGRES_URI is also configured,
it must identify the same database. The existing owned Redis fixture uses the
local redis-server binary. Then run:

```bash
backend/.venv/bin/python scripts/fleet_c_gate.py --report /absolute/new-gate-manifest.json
```

Default gate mode checks prerequisites, then executes only the explicit main and
fault case, each with its own evidence directory and actual JUnit report. The
manifest references those reports and rejects failure, skip, duplicate or missing
required cases. It does not run all Fleet/backend tests. The current slice instead
used this distinct evidence-verification mode:

```bash
backend/.venv/bin/python scripts/fleet_c_gate.py \
  --verify-receipts .local/fleet-evidence/c12
```

This last command validates preserved prior executions and emits
fresh_execution=false; it does not rerun the runtime, test memory/Store mutations,
or substitute for their separately qualified native evidence. Final C acceptance
still requires the review and coverage audit recorded in the acceptance report.
