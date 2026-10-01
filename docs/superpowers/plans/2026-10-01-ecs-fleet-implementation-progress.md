# ECS Fleet implementation evidence

Date: 2026-10-01. Goal: deliver B, then C, then C → B → C continuations.

Implementation has started in the personal-agent-ecs worktree. None of the three
OpenSpec changes has met its release gate; do not archive them or mark IMPLEMENTED.

## Current code

- B01: standalone optional package, strict profiles and identity-free JobSpec;
  disabled installation does not import the host runtime. Host dependency manager
  installation and deployable configuration remain to be exercised.
- B02: six private fleet_ tables plus independent fleet_alembic_version migration;
  service startup serializes migrations using a Postgres advisory transaction lock.
- B03: host-only bearer authentication for worker routes, persisted hashed node
  credentials, node session fencing and heartbeat. Attempt endpoints and cross-node
  attempt ownership tests will accompany B06. Management routers remain pending.
- B04: FIFO queued-job claims create attempts and reservations in one transaction;
  all unreleased capacity counts, including quarantine. Drain survives heartbeat.
  Stop proof is required before releasing capacity. Node session rotation takes no
  execution locks, avoiding inversion of execution → node lock order.
- B05: staged submission with user-scoped unique key, immutable payload verification,
  owner/thread/handle/driver-scoped tracking handshake, staged timeout, Fleet task
  driver registration and stoppable background reconciliation. Real McpTaskService
  is used; there is no second user task tracking store.

B05 still needs stable invocation identity through the model-visible tool/runtime,
retries that return the existing tracking row, scheduled dedupe handling, and an
explicit persistence-failure compensation test. The current driver requires a
server-provided invocation_id; no public Fleet submission tool exists yet.
B06–B12, all C and all continuation tasks remain pending. No container has been
started by Fleet, no remote Agent run has executed, and no ECS has been deployed.

## Verification evidence

Tests use a temporary local Postgres 16 instance on loopback and random schemas;
they do not touch business databases or NAS. TEST_POSTGRES_URI must target a test DB.
Run from backend using the existing venv, with optional package path injected by
fleet/conftest.py:

```bash
TEST_POSTGRES_URI=<local-test-uri> PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/fleet tests/test_mcp_task_service.py -q -p no:cacheprovider
```

Observed: 79 passed, zero skipped. Includes 43 Fleet tests and 36 task-service tests.
Combined pre-commit verification: 242 passed, zero skipped, two existing
Starlette/httpx deprecation warnings. Command adds tests/test_auth_middleware.py,
tests/test_csrf_middleware.py, tests/test_pat_auth.py, tests/test_extension_config.py
and tests/test_extension_api_contracts.py to the command above. This is adjacent
regression coverage, not the B release gate or the entire backend suite.

RED → GREEN was observed for the node-session lock inversion, B04 atomic capacity,
B05 staged submission, generic task driver and host startup binding. Tests call real
SQL repositories and HTTP middleware directly; a generic FleetProbe is unnecessary.

Scoped Ruff check and format pass. Standalone hatchling wheel build succeeds; wheel
output is kept in /private/tmp, outside the repository. Build is not a release gate.

## Next steps

1. Finish B05 invocation/retry/compensation behavior while keeping staged unclaimable.
2. Implement B06 host attempt endpoints, worker container lifecycle, start authorization,
   local monotonic lease watchdog and restart reconciliation with real Docker tests.
3. Deliver B07–B12 before enabling C. Keep agents/continuations flags off.
