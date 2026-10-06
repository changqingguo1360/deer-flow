# BC03 native acceptance

Status: locally accepted. Final native qualification and fresh independent SPEC→QUALITY rechecks are Ready. Accepted source commit `87e0d42876db67c01afc58c37a359afc28a22175`; all14 current reviewed blobs and3 assertion-only blobs independently match that commit. BC04–BC10 and installed production C→B→C remain required.

| Requirement | Current evidence |
| --- | --- |
| Exactly one continuation under concurrency | qualified-main-v2: two distinct coordinators rendezvous before original UoW; one new run/placement/spec, same task/generation, old run success, budget2→1 |
| Results before parent seal | Two actual child commands produce report.txt; original NAS sealing/manifests, child STOP and released reservations |
| Results after seal and restart/lost reply | qualified-boundary-v2: disabled continuations leave one old run; enable creates one receipt; fresh manager returns it, including disabled flags |
| Resumed source and valid input | Native AgentRunner PID13933 on node-bc03-new restores original bytes/checkpoint; one model call sees framed continuation data; no orphan tool messages |
| Publication and physical completion | Both runs success; new accepted final point differs from source; actual new process exit0, daemon STOP/release; jobs remain2 |
| Bounded valid observations | Representative128child Unicode/escaped valid artifact metadata stays within64KiB; every identity/state/manifest reference retained, exact included paths, deterministic order and no input mutation |
| Awaited capacity at admission | Original private FleetJobService with fixture limit2 reuses existing child and rejects new admission before a third child appears; production limit128, detached excluded |
| Source provenance |14tested hashes match both pre-execution maps;13current files identical; exact helper import-sort reversal reconstructs the remaining tested hash |
| Minimal verification | main1passed8.30s; boundary1passed11.09s; naturalexit0, separate owned schemas dropped; current14-file and separate3-head-file Ruff/format checks passed |
| Independent review | SPECv2 and QUALITYv2 whole-sliceReady; no remaining Critical/Important finding |

Evidence lives in .local/fleet-evidence/bc03/qualified-main-v2 and qualified-boundary-v2, with literal argv/cwd, natural exit, pre-execution map, owned-schema cleanup and SQL/process/NAS observations. frozen-source-v2.json stores tested bytes; current-source-v2.json stores current bytes. Root's root-final-source-audit-v2.json independently validates both maps and the exact import-only delta. qualification-delta-v2.json/post-qualification-import-only.patch delimit this difference; cached SQLAlchemy/Fleet imports precede actual helper use. There is no identical-AST claim or runtime repeat for formatting.

The3 migration-head assertion files contain exactly4 f0012_yield→f0013_continuation_receipt substitutions. migration-head-assertion-fix.json records hashes/static checks separately. These assertions were not executed in the native qualification.

Historical failures remain preserved. main-red1–3 are fixture failures; main-red4 is meaningful missing-continuation RED (1run vs2required). boundary-red1 is empty-directory restore mismatch; red2 exposes frozen tuple decoding; red3 exposes yielded-final source rejection. Initial GREEN and final-main-and-boundary were provisional: the combined attempt overwrote per-case observations and omitted one changed harness source. Separate v1 qualification resolved that provenance gap. QUALITYv1 then found valid metadata could overflow64KiB and permanently block admission. bounds-red-v2 reproduces that actual ValueError (1failed4.16s); native resumed execution was not reached in that RED. Identity-first global budgeting and original awaited admission capacity were repaired, and V2 requalified the same main/boundary before fresh reviews.

Qualification is native fixture processes/model/protocol components, not installed B/Agent images, full normal Gateway startup, product delivery semantics or installed C→B→C. Representative128metadata and capacity2 proof do not claim128actual workloads. Legacy larger-group deployment state is unverified; retain membership. No operator activation, deployment, push or merge occurred.
