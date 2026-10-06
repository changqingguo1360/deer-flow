"""Original-execution transaction validation supplied by the trusted host."""

import asyncio
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import text

from deerflow.runtime.execution.mutation_context import ExecutionCancellationRequested, MutationTarget, OwnershipRejected, RemoteMutationContext, current_remote_mutation_context


@dataclass(frozen=True, repr=False)
class _CancellationAuthority:
    context: RemoteMutationContext
    deadline: float
    operation: str | None = None


_cancellation_authority = ContextVar("fleet_private_cancellation_authority", default=None)
_CANCEL_TAILS = frozenset(
    {
        "run.status",
        "run.finalize",
        "run.completion",
        "thread.status",
        "thread.checkpoint_title",
        "stream.seal",
        "extension.task_stop",
        "scheduler.occurrence.complete",
        "scheduler.task.complete",
        "mcp.cancel",
        "workspace.publish",
        "workspace.accept",
        "workspace.process.settle",
    }
)


def cancellation_outcome(action):
    if action == "interrupt":
        return "interrupted", None
    if action == "rollback":
        return "error", "Rolled back by user"
    raise OwnershipRejected("Unknown original cancellation action")


def cancellation_boundary_matches(action, identity):
    status, error = cancellation_outcome(action)
    return (identity.kind, identity.desired_core_status, identity.desired_task_status, identity.desired_placement_status, identity.error) == ("paused", status, "paused", "cancelled", error)


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
        # Only the host selects these domains; adapted contributors cannot
        # choose a lock key. Keep this on the writer TX before the fresh clock.
        if operation in {"memory.write", "extension.write"}:
            domain = "deerflow:memory:" + spec.user_id if operation == "memory.write" else "deerflow:extension"
            await cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (domain,))
        finishing = task["state"] == placement["state"] == "finishing"
        if finishing:
            if not allow_terminal or operation not in {"control.observe", "workspace.accept", "workspace.node.idle", "workspace.process.settle", *_TERMINAL}:
                reject()
            await cursor.execute("SELECT * FROM fleet_workspace_points WHERE id=%s", (task["accepted_workspace_point_id"],))
            point = await cursor.fetchone()
            if (
                task["accepted_workspace_point_id"] != placement["final_workspace_point_id"]
                or point is None
                or point["kind"] not in {"final", "paused"}
                or any(
                    point[name] != value
                    for name, value in (
                        ("run_id", spec.run_id),
                        ("user_id", spec.user_id),
                        ("thread_id", spec.thread_id),
                        ("agent_task_id", spec.agent_task_id),
                        ("generation", spec.generation),
                        ("attempt_id", identity.attempt_id),
                        ("node_id", identity.node_id),
                        ("node_session_id", identity.node_session_id),
                        ("owner_worker_id", identity.owner_worker_id),
                        ("token_stamp", identity.token_stamp),
                        ("launch_spec_digest", spec.payload_digest()),
                        ("desired_core_status", run["status"]),
                    )
                )
            ):
                reject()
            await cursor.execute("SELECT checkpoint_id,metadata FROM checkpoints WHERE thread_id=%s AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1", (spec.thread_id,))
            checkpoint = await cursor.fetchone()
            if (
                checkpoint is None
                or checkpoint["checkpoint_id"] != point["checkpoint_id"]
                or checkpoint["metadata"].get("deerflow_execution_run_id") != spec.run_id
                or run["error"] != point["error"]
                or run["stop_reason"] != point["stop_reason"]
            ):
                reject()
        elif run["status"] not in {"pending", "running"} and operation not in {"workspace.accept", "control.observe"}:
            # A stage/prepared candidate cannot authorize terminal tails.
            reject()
        await cursor.execute("SELECT clock_timestamp() AS now")
        now = (await cursor.fetchone())["now"]
        owner = (spec.user_id, spec.thread_id)
        if any((row["user_id"], row["thread_id"]) != owner for row in (task, run, placement)):
            reject()
        if (
            task["current_run_id"] != spec.run_id
            or task["generation"] != spec.generation
            or (task["state"] not in {"queued", "running"} and not finishing)
            or placement["agent_task_id"] != spec.agent_task_id
            or placement["generation"] != spec.generation
            or placement["active_attempt_id"] != identity.attempt_id
            or placement["node_id"] != identity.node_id
            or (placement["state"] not in {"claimed", "running"} and not finishing)
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

        if task["cancel_requested_at"] is not None:
            reject()
        if run["cancel_action"] is not None:
            deadline = min(run["cancel_requested_at"] + timedelta(seconds=120), task["deadline"], attempt["execution_deadline"], spec.execution_deadline)
            if deadline <= now:
                reject()
            if operation != "control.observe":
                authority = _cancellation_authority.get()
                valid = (
                    authority is not None
                    and authority.context
                    == RemoteMutationContext(
                        spec.user_id, spec.thread_id, spec.run_id, spec.agent_task_id, spec.generation, identity.node_id, identity.node_session_id, identity.attempt_id, identity.owner_worker_id, identity.token_stamp, spec.payload_digest()
                    )
                    and authority.deadline > time.monotonic()
                )
                cleanup = valid and (operation in _CANCEL_TAILS or (authority.operation in {"checkpoint.cancel_title", "checkpoint.cancel_rollback"} and operation in {"aput", "aput_writes"}))
                if not cleanup:
                    raise ExecutionCancellationRequested(run["cancel_action"])
                if run["cancel_action"] not in {"interrupt", "rollback"} or (authority.operation == "checkpoint.cancel_rollback" and run["cancel_action"] != "rollback"):
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
        "store.write",
        "memory.write",
        "extension.write",
        "extension.task_stop",
        "scheduler.occurrence.complete",
        "scheduler.task.complete",
        "mcp.create",
        "mcp.cancel",
        "definition.agent.create",
        "definition.agent.update",
        "definition.agent.delete",
        "definition.managed.create",
        "definition.managed.update",
        "definition.managed.delete",
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
        "stream.seal",
        "events.put",
        "events.batch",
        "events.singleton",
        "events.delete_run",
    }
)
_TERMINAL = frozenset({"stream.seal", "run.status", "run.finalize", "run.completion", "thread.status", "thread.checkpoint_title", "extension.task_stop", "scheduler.occurrence.complete", "scheduler.task.complete", "mcp.cancel"})


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
        self._cancel_deadline = None

    async def observe_cancellation_async(self, session):
        row = await self._guard.validate(_SessionCursor(session), thread_id=self.context.thread_id, operation="control.observe", allow_terminal=True)
        return row["cancel_action"]

    def retain_cancellation_deadline(self, deadline):
        self._cancel_deadline = min(deadline, self._cancel_deadline) if self._cancel_deadline is not None else deadline

    def begin_cancellation(self, *, deadline):
        if current_remote_mutation_context() != self.context:
            raise OwnershipRejected("Original cancellation scope required")
        current = _cancellation_authority.get()
        deadline = min(deadline, current.deadline) if current is not None else deadline
        self._cancel_deadline = min(deadline, self._cancel_deadline) if self._cancel_deadline is not None else deadline
        _cancellation_authority.set(_CancellationAuthority(self.context, self._cancel_deadline))

    @contextmanager
    def cancellation_settlement_scope(self):
        if self._cancel_deadline is None:
            yield
            return
        if current_remote_mutation_context() != self.context or self._cancel_deadline <= time.monotonic():
            raise OwnershipRejected("Original cancellation settlement deadline elapsed")
        token = _cancellation_authority.set(_CancellationAuthority(self.context, self._cancel_deadline))
        try:
            yield
        finally:
            _cancellation_authority.reset(token)

    @contextmanager
    def cancellation_checkpoint_scope(self):
        current = _cancellation_authority.get()
        if current is None or current.context != self.context or current.deadline <= time.monotonic():
            raise OwnershipRejected("Original bounded cancellation checkpoint scope required")
        token = _cancellation_authority.set(_CancellationAuthority(self.context, current.deadline, "checkpoint.cancel_title"))
        try:
            yield
        finally:
            _cancellation_authority.reset(token)

    @contextmanager
    def cancellation_rollback_scope(self):
        current = _cancellation_authority.get()
        if current is None or current.context != self.context or current.deadline <= time.monotonic():
            raise OwnershipRejected("Original bounded rollback checkpoint scope required")
        token = _cancellation_authority.set(_CancellationAuthority(self.context, current.deadline, "checkpoint.cancel_rollback"))
        try:
            yield
        finally:
            _cancellation_authority.reset(token)

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
        if (
            row["cancel_action"] is not None
            and operation in {"run.status", "run.finalize", "run.completion", "thread.status", "stream.seal"}
            and any(
                target.status != cancellation_outcome(row["cancel_action"])[0]
                or (operation in {"run.status", "run.finalize"} and target.error != cancellation_outcome(row["cancel_action"])[1])
                or (target.error is not None and target.error != cancellation_outcome(row["cancel_action"])[1])
                for target in targets
            )
        ):
            raise OwnershipRejected("Original cancellation permits only the winning outcome")
        if operation.startswith("thread."):
            await cursor.execute("SELECT user_id FROM threads_meta WHERE thread_id=%s FOR UPDATE", (self.context.thread_id,))
            target_row = await cursor.fetchone()
            if (target_row is None and operation not in {"thread.create", "thread.ensure"}) or (target_row is not None and target_row["user_id"] != self.context.user_id):
                raise OwnershipRejected("Remote mutation thread target ownership rejected")
            row = await self._guard.validate(cursor, thread_id=self.context.thread_id, operation=operation, allow_terminal=operation in _TERMINAL)
        if operation == "stream.seal" and (row["status"] not in {"success", "error", "interrupted", "timeout"} or any(target.status != row["status"] or target.event_types for target in targets)):
            raise OwnershipRejected("Remote stream terminal closure rejected")
        if operation == "extension.task_stop":
            expected = {"success": "completed", "error": "failed", "timeout": "failed", "interrupted": "aborted"}.get(row["status"])
            if row["cancel_action"] == "rollback" and row["status"] == "error" and row["error"] == "Rolled back by user":
                # The guard above has checked the original identity, immutable
                # accepted root and bounded finishing authority. Intent alone
                # cannot authorize an extension STOP receipt.
                await cursor.execute(
                    "SELECT p.kind,p.desired_core_status,p.error,p.desired_task_status,p.desired_placement_status "
                    "FROM fleet_agent_tasks t JOIN fleet_run_placements rp ON rp.run_id=t.current_run_id "
                    "JOIN fleet_workspace_points p ON p.id=t.accepted_workspace_point_id "
                    "WHERE t.id=%s AND rp.run_id=%s AND p.run_id=rp.run_id "
                    "AND rp.final_workspace_point_id=p.id AND t.state='finishing' AND rp.state='finishing'",
                    (self.context.agent_task_id, self.context.run_id),
                )
                point = await cursor.fetchone()
                if point is not None and tuple(point[name] for name in ("kind", "desired_core_status", "error", "desired_task_status", "desired_placement_status")) == ("paused", "error", "Rolled back by user", "paused", "cancelled"):
                    expected = "aborted"

            if expected is None or any(target.status != expected or target.event_types != ("run.extension.task_stop",) for target in targets):
                raise OwnershipRejected("Extension terminal outcome rejected")
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
