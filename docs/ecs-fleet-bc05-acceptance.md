> Historical slice acceptance: preserve original cases, counts, source/image identities and scope below. Current B→C→combined status is in [the delivery record](ecs-fleet-delivery.md); later completion does not turn this record into a fresh current full gate.

# BC05 local acceptance

BC05 is locally accepted. The installed main and single concentrated boundary have passed; independent SPEC and QUALITY are ready. Accepted source commit `c153ad1951fcf1f8edaa5f5a4a0483d9228c3346` matches all 22 reviewed participant blobs. BC06–BC10 remain required. No deployment or operator activation is implied.

## Installed main

`main-green-8` ran only `test_bc05_single_slot_cbc_main`, naturally exiting 0 with **1 passed in 38.76 seconds**. It used build-8 image `sha256:b25bba66da679ba1b6c4cf742bfca08977b0f9b81b510a2d6c0b6d70519ad747`, from the original production recipe and the approved cached base. Runtime source did not change between this image build and the passing main.

Root independently checked the actual step records: **Agent → job → Agent**, all three Docker exits 0, each stopped attempt with released reservation, two successful core runs and one succeeded Agent task. Before the first Agent STOP, its container was Running, exactly one reservation charged 1000 CPU millis / 2048 MiB / one Agent unit, and the queued B claim was denied. The normal Gateway created the continuation through the original admission path. The final deterministic provider request contained the continuation marker and child result reference; this establishes result-reference flow, not a live external model or arbitrary artifact-content reasoning.

The fixture uses the normal Gateway lifecycle/authenticated TCP routes and original NodeDaemon/AgentContainers/DockerContainers/publication implementations. It drives stock daemon components in the test process; it does not claim execution of the standalone worker CLI or deployed ECS fleet. Final combined entrypoint qualification remains BC10.

The owned schema is identified by the attempt's actual receipt; no schema name is inferred here. Its receipt reports created/dropped. Container cleanup reports no errors and no remaining owned containers. The evidence is under `.local/fleet-evidence/bc05/main-green-8/`; its pre-execution source map is preserved. Before adding the separate boundary, Root verified the entire test file matched that passed map and recorded exact main/provider function bytes and AST fingerprints in `root-main-participants.json`.

## Concentrated native boundary

`boundary-2` ran only `test_bc05_shared_turn_and_reserved_capacity`, naturally exiting 0 with **1 passed in 4.89 seconds**. Actual native SQL claims alternated job/Agent across four waves of competing eligible arrivals, retained category FIFO and survived scheduler reconstruction. The same case qualified active B credit within the protected quota, rejected fragmented cross-node capacity, accepted one actual B-fit node, and blocked serial admission while unknown work remained charged.

The original scheduled occurrence/ticket path rejected duplicate reservation, transferred the same reservation exactly once into the Agent claim and retained the category turn. Original SQL authority checks rejected an inactive, never-started execution identity. This proves hard inactive-authority rejection, not a revocation scenario. Boundary allocations were never start-authorized, remained charged until owned-schema drop and have no physical container/STOP qualification. The schema receipt reports created/dropped.

The boundary was appended after the installed main passed. SPEC independently confirmed the entire previously qualified module remains an exact current prefix through line 259, and checked all 22 final participant hashes and the boundary map. Historical maps remain unchanged; main evidence carries forward at its original prefix scope only.

## Reviews and source receipt

Independent SPEC returned **SpecReady**, without a material source/spec gap. Final source/image alignment and changed-file static checks are recorded under `.local/fleet-evidence/bc05/final-qualification/`: the image audit covers 735 runtime source files, not tests; Ruff format/check and diff checks passed for the final 22 participants. Three old migration-head test files were maintained but not run.

Independent QUALITY returned **QualityReady**, with no actionable Critical, Important or Minor issue. Fresh-agent creation repeatedly reached the tool thread limit; an existing independent auditor received a new BC05-only read-only task. It had not authored BC05 or performed its SPEC review. This reuse is recorded rather than described as fresh-agent review. Source commit `c153ad1951fcf1f8edaa5f5a4a0483d9228c3346` matches all 22 reviewed Git blobs; the local review and commit-audit receipts preserve this scope. Historical failures remain in their original attempt directories; invalid-tool diagnosis replaced the withdrawn timeout-argument guess. The passing main followed an actual scope fix and a fixture writer-lifecycle correction. No failed attempt is counted as full CBC acceptance.
