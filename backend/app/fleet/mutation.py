"""Original-execution transaction validation supplied by the trusted host."""

import asyncio

from sqlalchemy import text

from deerflow.runtime.execution.mutation_context import MutationTarget, OwnershipRejected, RemoteMutationContext, current_remote_mutation_context


class _FleetExecutionGuard:
    """Bind the original accepted execution to writes on the saver connection."""

    def __init__(self, identity, spec):
        self._identity = identity
        self._spec = spec

    async def validate(self, cursor, *, thread_id, operation, allow_terminal=False):
        import hmac

        identity, spec = self._identity, self._spec

        def reject():
            raise OwnershipRejected("Checkpoint ownership fence rejected execution")

        if thread_id != spec.thread_id or (identity.agent_task_id, identity.generation) != (spec.agent_task_id, spec.generation):
            reject()
        # Lock in the shared admission/renewal order on this exact psycopg TX.
        rows = []
        for statement, value in (
            ("SELECT * FROM fleet_agent_tasks WHERE id=%s FOR UPDATE", spec.agent_task_id),
            ("SELECT * FROM runs WHERE run_id=%s FOR UPDATE", spec.run_id),
            ("SELECT * FROM fleet_run_placements WHERE run_id=%s FOR UPDATE", spec.run_id),
            ("SELECT * FROM fleet_nodes WHERE id=%s FOR UPDATE", identity.node_id),
            ("SELECT * FROM fleet_reservations WHERE attempt_id=%s FOR UPDATE", identity.attempt_id),
            ("SELECT * FROM fleet_attempts WHERE id=%s FOR UPDATE", identity.attempt_id),
        ):
            await cursor.execute(statement, (value,))
            row = await cursor.fetchone()
            if row is None:
                reject()
            rows.append(row)
        task, run, placement, node, reservation, attempt = rows
        await cursor.execute("SELECT clock_timestamp() AS now")
        now = (await cursor.fetchone())["now"]
        owner = (spec.user_id, spec.thread_id)
        if any((row["user_id"], row["thread_id"]) != owner for row in (task, run, placement)):
            reject()
        if (
            task["current_run_id"] != spec.run_id
            or task["generation"] != spec.generation
            or task["state"] not in {"queued", "running"}
            or placement["agent_task_id"] != spec.agent_task_id
            or placement["generation"] != spec.generation
            or placement["active_attempt_id"] != identity.attempt_id
            or placement["node_id"] != identity.node_id
            or placement["state"] not in {"claimed", "running"}
            or run["owner_worker_id"] != identity.owner_worker_id
            or (run["status"] not in {"pending", "running"} and not (allow_terminal and run["status"] in {"success", "error", "interrupted", "timeout"}))
            or (run["kwargs_json"] or {}).get("execution_backend") != "fleet"
            or node["session_id"] != identity.node_session_id
            or attempt["kind"] != "agent"
            or attempt["run_id"] != spec.run_id
            or attempt["node_id"] != identity.node_id
            or attempt["node_session_id"] != identity.node_session_id
            or not hmac.compare_digest(attempt["token_hash"], identity.token_stamp)
            or attempt["state"] not in {"starting", "running"}
            or attempt["stopped_at"] is not None
            or attempt["start_authorized_at"] is None
            or attempt["process_ref"] != "fleet-" + identity.attempt_id
            or (attempt["launch_spec"] or {}).get("launch_spec") != spec.canonical_payload()
            or reservation["node_id"] != identity.node_id
            or reservation["state"] not in {"reserved", "active"}
            or reservation["released_at"] is not None
            or reservation["agent_units"] != 1
            or reservation["cpu_millis"] <= 0
            or reservation["memory_mib"] <= 0
            or run["lease_expires_at"] is None
            or attempt["lease_expires_at"] != run["lease_expires_at"]
            or run["lease_expires_at"] <= now
            or task["deadline"] <= now
            or attempt["execution_deadline"] is None
            or attempt["execution_deadline"] <= now
            or spec.execution_deadline <= now
        ):
            reject()

        return run


class FleetCheckpointFence:
    def __init__(self, identity, spec):
        self._guard = _FleetExecutionGuard(identity, spec)

    async def validate(self, cursor, *, thread_id, operation):
        await self._guard.validate(cursor, thread_id=thread_id, operation=operation)


class _SessionCursor:
    def __init__(self, session, *, synchronous=False):
        self.session = session
        self.synchronous = synchronous
        self.result = None

    async def execute(self, statement, parameters=()):
        for index in range(len(parameters)):
            statement = statement.replace("%s", ":p" + str(index), 1)
        result = self.session.execute(text(statement), {"p" + str(i): value for i, value in enumerate(parameters)})
        self.result = result if self.synchronous else await result

    async def fetchone(self):
        row = self.result.mappings().first()
        return dict(row) if row is not None else None


_ACTIVE = frozenset(
    {
        "run.start",
        "run.attach",
        "run.status",
        "run.finalize",
        "run.completion",
        "run.progress",
        "run.model",
        "thread.create",
        "thread.ensure",
        "thread.display",
        "thread.checkpoint_title",
        "thread.status",
        "thread.metadata",
        "events.put",
        "events.batch",
        "events.singleton",
        "events.delete_run",
    }
)
_TERMINAL = frozenset({"run.status", "run.finalize", "run.completion", "thread.status", "thread.checkpoint_title"})


class FleetMutationCapability:
    def __init__(self, identity, spec):
        self._context = RemoteMutationContext(
            user_id=spec.user_id,
            thread_id=spec.thread_id,
            run_id=spec.run_id,
            agent_task_id=spec.agent_task_id,
            generation=spec.generation,
            node_id=identity.node_id,
            node_session_id=identity.node_session_id,
            attempt_id=identity.attempt_id,
            owner_worker_id=identity.owner_worker_id,
            token_stamp=identity.token_stamp,
            launch_spec_digest=spec.payload_digest(),
        )
        self._guard = _FleetExecutionGuard(identity, spec)

    @property
    def context(self):
        return self._context

    async def validate_cursor(self, cursor, *, context, operation, targets):
        if context is None or context != self.context or current_remote_mutation_context() != self.context or operation not in _ACTIVE or not targets:
            raise OwnershipRejected("Remote mutation context or operation rejected")
        for target in targets:
            if not isinstance(target, MutationTarget) or any(value is not None and value != getattr(self.context, name) for name, value in (("run_id", target.run_id), ("thread_id", target.thread_id), ("user_id", target.user_id))):
                raise OwnershipRejected("Remote mutation target rejected")
        row = await self._guard.validate(cursor, thread_id=self.context.thread_id, operation=operation, allow_terminal=operation in _TERMINAL)
        if operation.startswith("thread."):
            await cursor.execute("SELECT user_id FROM threads_meta WHERE thread_id=%s FOR UPDATE", (self.context.thread_id,))
            target_row = await cursor.fetchone()
            if (target_row is None and operation not in {"thread.create", "thread.ensure"}) or (target_row is not None and target_row["user_id"] != self.context.user_id):
                raise OwnershipRejected("Remote mutation thread target ownership rejected")
        if row["status"] not in {"pending", "running"}:
            if operation in {"run.status", "run.finalize", "run.completion"} and any(target.status != row["status"] for target in targets):
                raise OwnershipRejected("Remote mutation cannot change terminal result")
            if operation in {"run.status", "run.finalize", "run.completion"} and any(
                (target.error is not None and target.error != row["error"]) or (target.stop_reason is not None and target.stop_reason != row["stop_reason"]) for target in targets
            ):
                raise OwnershipRejected("Remote mutation cannot change terminal outcome details")
            if operation == "thread.status" and any(target.status != ("idle" if row["status"] == "success" else row["status"]) for target in targets):
                raise OwnershipRejected("Remote mutation terminal thread status mismatch")

    async def validate_async(self, session, **kwargs):
        if session.get_bind().dialect.name != "postgresql":
            raise OwnershipRejected("Remote mutation requires PostgreSQL")
        await self.validate_cursor(_SessionCursor(session), **kwargs)

    def validate_sync(self, session, **kwargs):
        if session.get_bind().dialect.name != "postgresql":
            raise OwnershipRejected("Remote mutation requires PostgreSQL")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.validate_cursor(_SessionCursor(session, synchronous=True), **kwargs))
            return
        raise OwnershipRejected("Synchronous mutation validation requires an external thread")
