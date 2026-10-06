# BC01 acceptance status

BC01 source/native foundation accepted after independent SPEC→QUALITY review and current-source/evidence verification. Final native main and one concentrated boundary case passed. The explicit source commit is recorded separately after final static checks. BC02–BC10 remain required.

| Requirement | Current evidence | Scope/status |
|---|---|---|
| Server-bound child owner and same-TX fence | Actual loaded ToolNode/McpTaskService/driver/PG main and host BoundJobParent | Native main passed |
| Immutable child links/group membership | f0011 actual migration, foreign keys/triggers and direct SQL rejection | Native boundary passed |
| Stable idempotent/concurrent seal | Original group IDs/key and actual row counts | Native main/boundary passed |
| Cross-user/task/run and stale generation reject | Actual PermissionError/OwnershipRejected and before/after SQL counts | Native rejection proven; no public HTTP403 claimed |
| Local detached B preserved | boundary-local-red-01 then actual corrected final boundary | Direct regression closed |
| Postflush authority loss rolls back job/link/invocation | Actual SQL trigger expires lease after insert, guard rejects; counts and lease rollback observed | Native boundary passed |
| Runner source private driver/carrier | Production host registration and loaded ToolNode | Source/native wiring only, no installed execution |
| Reviews and source match | Root/current source matched13 files and both execution maps; fresh SPEC and QUALITY Ready | Accepted native foundation; commit receipt follows |

Root inspected final natural0 logs, literal command receipts, actual observations and owned schema cleanup. .local/fleet-evidence/bc01/root-current-source-evidence-audit-v1.json records current13 changed-source matches and both execution source maps. Source hash ef2fe0293a9fe2189363c3348d862a1b29be2b05b4b57b9dd06f731a0e9c018e belongs to the actual final two executions. Earlier attempts keep their original failures/partial scopes. No complete Fleet/backend suite, new Docker image or fake HTTP test adapter was introduced.

Accepted source commit: `ce92de4ab1f47eeac25e03544019bb7d619b23dd`. Changed13 Ruff check and format check, git diff check, strict OpenSpec and guidance checks passed (guidance retains6 existing soft warnings).
