> Historical slice acceptance: preserve original cases, counts, source/image identities and scope below. Current B→C→combined status is in [the delivery record](ecs-fleet-delivery.md); later completion does not turn this record into a fresh current full gate.

# BC07 local acceptance

BC07 is locally accepted at source `097b789a39810be58ab0e894780b61221ba26421`. Independent SPEC and fresh independent QUALITY are Ready. Root verified all22 committed Python blobs against the reviewed fingerprints. B, C01–C12 and BC01–BC06 retain their recorded scopes; BC08–BC10 and the final installed combined entrypoint remain required.

## Requirement evidence

| Requirement | Authoritative observation | Qualification |
| --- | --- | --- |
| Task run budget survives repeated execution and generation change | main-cumulative-03: C1→B1→C2→B2→C3, three executed/admitted runs, generation2, fourth same-task owned resume409 | Original native executor/Node/PG and authenticated HTTP; no full installed bootstrap claim |
| Token budget checked before transport across runs | boundary-cumulative-01: actual serialized request bound11713 fits frozen12000, prior measured1606 makes13319; original new-run owner refused with no extra HTTP | Cached actual SDK and original middleware; second owner admitted/claimed/start-authorized, no second graph/STOP claim |
| Logical jobs retain cumulative charges across runs | Four real jobs counted in main; focused boundary retains two prior successful accepted-manifest jobs and rejects new-run submission without another job row | Original JobService/parent capability and SQL |
| Usage settlement and uncertainty retention | Main five measured SDK requests7188tokens, five settled tickets/reserved0; historical boundary05 complete SSE missing usage retains4305, invalid usage END retains6891 with original paused pair/STOP | Boundary05 recovery tail bytes/helper AST and all20 production files unchanged; not freshly rerun |
| Unknown child reaches deadline without capacity refund | Historical boundary05 real clock expiry with child alive, original lease30s, explicit task_deadline_unknown_child, quarantined1000mCPU/512MiB retained | Recorded native observation carried at identical source scope |
| Parent crash cannot resume from child results alone | Historical boundary05 actual parent SIGKILL(-9), original wire137 STOP/release, two real children succeed, original scan admits no run; gen1/run1 retained and recovery_required | No accepted waiting pair fabricated; no automatic continuation |
| Owned summary reports budget/recovery safely | Historical boundary05 original owned HTTP reports safe counters/reason, execution uncertainty and confirmed STOP/released parent | Added conservative placement-unknown/missing-attempt branches source reviewed only |

`main-cumulative-03` naturally exited0,1passed12.06s; `boundary-cumulative-01` naturally exited0,1passed9.54s with explicit token/job-only scope. Owned schemas were dropped. Root independently checked provider usage, SQL, result-input manifests, accepted source lineage, process exits/STOP/releases and cleanup. Prior deficient main-green-01, boundary failures and two revised-main failures remain preserved; none is advertised as current full qualification.

## Source and reviews

Frozen patch `ac8de52a0ed5786b46bb6fefcc7f182ad18e9e15822eeab3c51b449ef25ec9bb` covers22 Python files. Root independently matched all current fingerprints. Author static verification: Ruff check/format-check22files and actual Python3.12.11 compile all exit0; Root diff whitespace0, strict OpenSpec0, guidance24files/0errors/6existing soft warnings. SPEC re-review checked this same source/evidence and returned Ready. Fresh independent QUALITY also returned Ready with no Critical/Important findings. The22 committed source blobs at `097b789a39810be58ab0e894780b61221ba26421` match the reviewed fingerprints.

Evidence under `.local/fleet-evidence/bc07/`: `final-source-02`, `main-cumulative-03/root-audit.json`, `boundary-cumulative-01/root-audit.json`, historical `boundary-05`, `reviews/spec-02.json`, `reviews/quality-01.json` and `source-commit-audit.json`.

## Remaining qualification

Full private `build_agent_environment` bootstrap, installed image/normal Gateway lifespan, combined two-worker C→B→C release, sync/memory/concurrency runtime variants and ECS deployment are not inferred from native evidence. BC10 owns the mandatory installed combined gate. No push, merge, operator activation or deployment is claimed. BC08 implementation may now begin; BC08–BC10 remain required.
