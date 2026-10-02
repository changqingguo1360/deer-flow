# C06 durable mutation acceptance

C06 is accepted locally on 2026-10-03. Supported remote durable mutations are fenced by the original execution identity in the actual writer transaction. Remote Agent activation remains closed while C07-C12 are pending. This does not report production ECS deployment or completion of B/C continuations.

## Accepted behavior

- Run, ThreadMeta, event, Store/vector/TTL and SQL Agent-definition mutations validate original ownership on the same connection and transaction, after lock waits and before commit. Unsupported remote administrative/file paths reject before effects; terminal bookkeeping has a narrow consistent-state allowlist.
- Configured PostgreSQL memory and adapted extension callbacks retain original private authority across queues, native threads and loops. Positive memory drain and actual owned observer completion reach joint quiescence before/after services and before resource teardown. Unsafe stateful profiles reject before startup.
- Both scheduler completion transactions validate the original run/occurrence/parent association. Runner MCP tracking uses the real private submitter/repository path; Gateway service ownership remains compatible.
- The original LangGraph astream and checkpoint Tasks are explicitly settled in single/multiple stream modes. Pending cleanup retains the actual Task, original scope and complete resource stack. The first live cleanup starts one cumulative 120-second budget; retries do not reset it and normal execution does not start it. Settled errors preserve original identity while remaining resources close. Only the dedicated isolated entry has the physical exit-70 deadline policy.

## Frozen build and installed runtime

Whole-C06 review base: `cd251cbf`; pre-slice HEAD: `b3e5089ad8944b8a710aa8623efae985b30ab3b3`, plus all frozen changed/untracked technical files. Source freeze has 60 files and 18 build inputs. Original source JSON SHA256: `7d46ff1c00465ecc91a7e08703249be5247006b0634bb5bd153bd05b246c0db6`; image-populated JSON: `564cf373fdaadcbcc80312f6e17252fc4878d555f2573fe80afbb962d3eb8fd5`. Removing only images metadata exactly reconstructs the original SHA.

Runner: `sha256:a2eda1bbdd7d9897068c342bb3e12a8c8ad87ee1d572969ba0572cb234098c06`. Alternate provider: `sha256:ff113e45973509dcfaeccf3b4fd4ccf84188d5d097cd30ccebcac45944b3c7e9`.

Six original wheel hashes/context are retained in `/private/tmp/c06c-six-wheels-final-v12.json`; root and both reviewers independently verified them. Build stage1-13 checks frozen source/input hashes before/after operations. Installed byte proof independently matches runner50/provider7 files, including the hardened absolute `/opt/deerflow/libexec_bootstrap.py` and wheel module separately. Normal installed daemon: 6 passes, zero failures/errors/skips, natural exit0. Fresh installed verification matrix: 1916 passes, zero failures/errors/skips, natural exit0. This matrix combines actual installed container pipeline, host-native PostgreSQL fixtures and Local24/Local21 neighbors; it does not mean every case executed inside a container. Native shortened-budget physical rollback/deadline fixtures are a separate proof layer.

## Independent root gates

| Gate | Actual result |
| --- | --- |
| C06 mutation/profiles/tracking | 442 passed, zero skipped |
| C05 checkpoints | 130 passed, zero skipped |
| C04 runner | 96 passed, zero skipped |
| C01-C03 | 74 passed, zero skipped |
| Local worker/checkpointer/ownership neighbors | 753 passed, zero skipped |
| Local memory/extensions/scheduler/MCP neighbors | 421 passed, zero skipped |
| Actual PostgreSQL scheduler/schema | 2 + 2 passed, zero skipped |
| Original unchanged B image gate | 259 passed, zero skipped |
| Full backend make test | 13302 passed, 826 default-suite skips, 1 deselected |
| Blocking I/O | 75 passed |
| Boundaries | 74 passed |
| Ruff and format | Clean; 1428 formatted files |
| Agent guidance | 24 guides, zero errors, three soft-limit warnings |
| Strict OpenSpec | Three changes passed |
| git diff --check | Passed |

Root driver session92763 naturally exited0. Its log ends ALL C06 ROOT GATES PASSED; it verifies frozen source/input hashes before/after every gate. Full backend ran without UV_NO_SYNC/UV_OFFLINE. Required PostgreSQL/container cases were separately executed with zero skips; default-suite optional skips are not counted as required integration evidence.

Retained evidence: `/private/tmp/c06c-final-v12-handoff.md`, `/private/tmp/c06-full-spec-review-v12.md`, `/private/tmp/c06-whole-quality-review-v12.md`, `/private/tmp/fleet-c06-root-final-v12-driver.log`, and the matching per-gate XML/log files. SOURCE and runtime SPEC/QUALITY approvals cover whole C06; original v11 root deadlocks were real failures and historical green results did not override them. Actual stream-lifetime and settled-error RED-to-GREEN proofs close those defects. Final acceptance documentation does not change production/tests/packaged guides or wheel bytes.

Final nonpackaged documentation checks: guidance24/zero errors/six soft-limit chain warnings (all within hard limits), strict OpenSpec3/0 and diff0. The three-warning guidance result above belongs to the frozen-build root gate before the acceptance orientation update. Production/tests/packaged guides/build inputs and wheel bytes remain unchanged.
