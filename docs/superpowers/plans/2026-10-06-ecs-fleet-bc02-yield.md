# BC02 cooperative yield and physical-stop barrier

Execute through Superpowers subagent-driven-development. One source/test implementer; Root owns docs/OpenSpec/evidence/commits; fresh SPEC then QUALITY. User requires one main first and only directly necessary boundary validation.

## Prerequisite and current source

BC01 native foundation accepted at `ce92de4a`, receipt `62784b46`; B and C01–C12 locally accepted. BC03–BC10 remain required. No operator activation or deployment.

BC01 persists immutable server-bound child links and sealed wait membership. WaitGroups.seal currently owns a transaction; BC02 must join actual terminal operations through the same session where atomicity is required, rather than commit independent fake readiness.

The old planned `deerflow_ecs_fleet/recovery_points.py` does not exist. Actual checkpoint/workspace pairing is owned by `backend/app/fleet/workspace.py`, original stock worker lifecycle and Fleet workspace repositories. Its success boundary currently defaults the aggregate task to succeeded; cooperative yield must preserve run success while selecting waiting_jobs on the aggregate through the original publication/terminal transaction.

Current `WorkspaceBoundaryIdentity` validation and both WorkspaceRequestRow/WorkspacePointRow SQL CHECK constraints exclude waiting_jobs. Add a forward f0012 migration after accepted f0011 and update the actual protocol/model constraints; never rewrite historical migrations or bypass publication pairing by an unrelated state update. AgentTaskRow already permits waiting_jobs.

Source/design reconciliation: accepted C workspace terminal transactions deliberately retain task/placement finishing until original physical STOP. Keep that behavior; atomically persist run.success and exact accepted desired waiting_jobs outcome, then actual STOP applies waiting_jobs/succeeded and releases reservation. First-principles9.2 wording is aligned to this existing barrier; continuation still cannot start before STOP/release.

## Required behavior

1. Expose await through the actual private bound tool path, preserving Local/B behavior and remote fail-closed authority. Harness receives neutral injected interfaces and never imports app/Fleet.
2. Explicit await and natural graph completion with remaining unconsumed awaited children both form the original sealed group. Detached children never hold the aggregate waiting. Preserve immutable task/generation/run ownership.
3. Complete every actual tool-call/message pair before a safe graph end; a ordinary thrown exception or incomplete interrupt cannot masquerade as success. No extra model call after the yield boundary.
4. Settle original graph/checkpointer/writers, then seal the exact root checkpoint and workspace. Original run terminal and aggregate waiting state must reference that exact accepted pair and shared SQL transaction.
5. Persist honest physical readiness: sealed group, old run terminal, actual stopped acknowledgement and released original reservation. Lease expiry, process intent, mock exit or DB status alone is not physical stop. BC03 will use this barrier for continuation admission; BC02 does not pretend a resumed run exists.
6. Keep existing task-first lock order, fresh same-transaction pre/post authority fences, original cancellation rules and resource teardown behavior.

## Main-first execution

- [x] Audit actual graph ToolNode termination, stock worker completion, host publication, physical STOP and group schemas; resolve integration choices from current source.
- [x] Write one actual PG main in `test_bc02_fleet_agent_job_dependencies.py`, run meaningful missing-behavior RED, then implement the actual original path to GREEN.
- [x] Observe paired tool calls, old run success, aggregate waiting_jobs and no readiness before actual STOP/release. Include normal-end autoawait in the main scenario if it exercises the same production path; do not create a speculative matrix.
- [x] After main passes, at most one concentrated boundary for a concrete uncovered rejection or regression. No full backend/Fleet suite or formatting-only runtime rerun/build.
- [x] Preserve literal argv/cwd/natural exit, source maps, actual SQL/checkpoint/process observations and owned UUID-schema cleanup. Reuse the private existing test PG carrier through environment only.
- [x] Changed-file Ruff/diff, fresh whole-slice SPEC then QUALITY, requirement/evidence inspection, actual capability docs, explicit source commit and postcommit OpenSpec2.1–2.4 receipt.

This plan is not execution evidence. Full installed C→B→C belongs to the later combination gate; any narrower native proof is labeled at its actual scope. Failed attempts remain unchanged.

Root inspected meaningful main RED red4.log: original owning AgentRunner process exit0, paired submit/await/sibling tool calls, core run success, actual task succeeded instead of waiting_jobs and one extra model invocation after await. Earlier3 attempts are fixture failures, not missing-behavior RED. Production implementation now proceeds; BC02 is not accepted or checked in OpenSpec.

Final native main1passed6.59s and concentrated normal-end boundary1passed9.18s, both natural pytest0/original AgentRunner child0/owned-schema drop. Two awaited jobs, complete tool pairs, no extra explicit-await model call, exact sealed output bytes and group/checkpoint/point IDs observed. Before STOP core success/task+placement finishing/reservation reserved/readinessfalse; after authenticated matching-process STOP waiting_jobs/released/readinesstrue. Boundary collects two terminal unconsumed awaited results and excludes one detached child; requested public tracking IDs are validated. Root matched23 frozen hashes and qualified the sole post-run two-line docstring clarification by tested-byte SHA and unchanged executable AST. Fresh SPEC and QUALITY are Ready; no further runtime batch or image build.

Accepted BC02 source commit `c08b8014801c762c3d2a9157e90ca1462a6a35f3`; OpenSpec2.1–2.4 marked only after explicit commit. Installed combination and continuation admission remain future required work.
