"""Trusted host control for the original durable Agent execution."""

import asyncio
import time
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.manager import CancelOutcome

_STOP_READ_SECONDS = 1.0
_STOP_READ_ERRORS = (TimeoutError, SQLAlchemyError, OSError)


async def _stop_read(session_factory, statement, deadline):
    """Direct close avoids AsyncSession.__aexit__'s detached shielded task."""
    session = None
    externally_cancelled = False
    try:
        async with asyncio.timeout_at(deadline):
            session = session_factory()
            await session.__aenter__()
            return (await session.execute(statement)).one_or_none()
    except asyncio.CancelledError:
        externally_cancelled = True
        raise
    finally:
        if session is not None:
            try:
                # Closing shares the read's absolute bound, including when the
                # query has already consumed it. No new cleanup budget or task.
                async with asyncio.timeout_at(deadline):
                    await session.close()
            except _STOP_READ_ERRORS:
                if not externally_cancelled:
                    raise


class FleetAgentRunControl:
    def __init__(self, session_factory):
        self.sf = session_factory

    async def request_cancel(self, run_id, *, action):
        from deerflow_ecs_fleet.launch_spec import LaunchSpec
        from deerflow_ecs_fleet.persistence.models import AttemptRow, RunPlacementRow

        async with self.sf.begin() as session:
            locator = await session.get(RunPlacementRow, run_id)
            if locator is None:
                return None
            # Rollback has a distinct workspace restoration contract (C09 Task2).
            # Never silently reinterpret the public action as interrupt.
            attempt = await session.get(AttemptRow, locator.active_attempt_id) if locator.active_attempt_id else None
            if attempt is None:
                return CancelOutcome.not_cancellable
            spec = LaunchSpec.model_validate(attempt.launch_spec["launch_spec"])
            from deerflow_ecs_fleet.persistence.attempts import AgentAttempts

            task, run, placement, node, reservation, attempt = await AgentAttempts().locked(session, attempt.id, run_locker=lambda session, key: session.get(RunRow, key, with_for_update=True))
            now = await session.scalar(select(func.clock_timestamp()))
            if (
                task is None
                or run is None
                or placement is None
                or node is None
                or reservation is None
                or task.current_run_id != run_id
                or task.generation != placement.generation
                or task.generation != spec.generation
                or (task.user_id, task.thread_id) != (run.user_id, run.thread_id)
                or (placement.user_id, placement.thread_id) != (run.user_id, run.thread_id)
                or placement.active_attempt_id != attempt.id
                or placement.node_id != attempt.node_id
                or run.owner_worker_id != "fleet-agent:" + attempt.id
                or (run.kwargs_json or {}).get("execution_backend") != "fleet"
            ):
                return CancelOutcome.not_cancellable
            if run.cancel_action is not None:
                return CancelOutcome.cancelled if attempt.stopped_at is not None else CancelOutcome.requested
            if action not in {"interrupt", "rollback"}:
                return CancelOutcome.not_cancellable
            if run.status not in {"pending", "running"} or attempt.stopped_at is not None or task.state not in {"queued", "running"} or placement.state not in {"claimed", "running"}:
                return CancelOutcome.not_cancellable
            run.cancel_action = action
            run.cancel_requested_at = now
            run.updated_at = now
            await session.flush()
            return CancelOutcome.requested

    async def wait_stopped(self, run_id, *, disconnected=None):
        from deerflow_ecs_fleet.persistence.models import AgentTaskRow, AttemptRow, RunPlacementRow

        try:
            if disconnected is not None and await disconnected():
                return False
            before_clock = time.monotonic()
            original = await _stop_read(
                self.sf,
                select(AttemptRow.id, AgentTaskRow.generation, RunRow.cancel_requested_at, AgentTaskRow.deadline, AttemptRow.execution_deadline, func.clock_timestamp(), AttemptRow.stopped_at, RunRow.kwargs_json, RunPlacementRow.run_id)
                .select_from(RunRow)
                .outerjoin(RunPlacementRow, RunPlacementRow.run_id == RunRow.run_id)
                .outerjoin(AttemptRow, (AttemptRow.id == RunPlacementRow.active_attempt_id) & (AttemptRow.run_id == RunRow.run_id) & (AttemptRow.kind == "agent"))
                .outerjoin(AgentTaskRow, AgentTaskRow.id == RunPlacementRow.agent_task_id)
                .where(RunRow.run_id == run_id),
                before_clock + _STOP_READ_SECONDS,
            )
            if original is None:
                return False
            # Only a successful read proving no Agent placement/backend may
            # delegate Local/B. Queued or incomplete remote admission stays
            # pending; unavailable classification never falls through to END.
            if original[8] is None and (original[7] or {}).get("execution_backend") != "fleet":
                return None
            if original[0] is None or original[2] is None or original[3] is None or original[4] is None:
                return False
            if original[6] is not None:
                return True
            deadline = min(original[2] + timedelta(seconds=120), original[3], original[4])
            # Sampling monotonic time before the DB clock conservatively charges
            # pool/query/close time and never extends the immutable stop budget.
            monotonic_deadline = before_clock + max(0, (deadline - original[5]).total_seconds())
            while True:
                if disconnected is not None and await disconnected():
                    return False
                now = time.monotonic()
                if now >= monotonic_deadline:
                    return False
                row = await _stop_read(
                    self.sf,
                    select(AttemptRow.stopped_at, AgentTaskRow.generation, func.clock_timestamp())
                    .join(RunPlacementRow, RunPlacementRow.active_attempt_id == AttemptRow.id)
                    .join(AgentTaskRow, AgentTaskRow.id == RunPlacementRow.agent_task_id)
                    .where(RunPlacementRow.run_id == run_id, AttemptRow.id == original[0], AttemptRow.run_id == run_id),
                    min(now + _STOP_READ_SECONDS, monotonic_deadline),
                )
                if row is None:
                    return False
                # Durable stop of the frozen attempt survives later admission.
                if row[0] is not None:
                    return True
                if row[1] != original[1] or row[2] >= deadline:
                    return False
                if disconnected is not None and await disconnected():
                    return False
                await asyncio.sleep(min(0.05, max(0, monotonic_deadline - time.monotonic())))
        except _STOP_READ_ERRORS:
            # Accepted intent remains pending when observation is unavailable.
            # Genuine external CancelledError is deliberately not intercepted.
            return False


async def observe_original_control(session_factory, capability):
    """Read-only short poll; no renewed lease or ordinary write authority."""
    from .mutation import _SessionCursor

    async with asyncio.timeout(1.0):
        async with session_factory.begin() as session:
            from sqlalchemy import text

            await session.execute(text("SET LOCAL lock_timeout='500ms'"))
            await session.execute(text("SET LOCAL statement_timeout='750ms'"))
            row = await capability._guard.validate(_SessionCursor(session), thread_id=capability.context.thread_id, operation="control.observe", allow_terminal=True)
            if row["cancel_action"] is None:
                return None
            monotonic_before_clock = time.monotonic()
            now = await session.scalar(select(func.clock_timestamp()))
            from deerflow_ecs_fleet.persistence.models import AgentTaskRow, AttemptRow

            task = await session.get(AgentTaskRow, capability.context.agent_task_id)
            attempt = await session.get(AttemptRow, capability.context.attempt_id)
            deadline = min(row["cancel_requested_at"] + timedelta(seconds=120), task.deadline, attempt.execution_deadline, capability._guard._spec.execution_deadline)
            return {"action": row["cancel_action"], "monotonic_deadline": monotonic_before_clock + max(0, (deadline - now).total_seconds())}


class OriginalAgentCancellation:
    """Private original executor and retained host cleanup share one immutable bound."""

    def __init__(self, sf, capability, manager, teardown, controller):
        self.sf, self.capability, self.manager, self.teardown, self.controller = sf, capability, manager, teardown, controller

    async def read(self):
        control = await observe_original_control(self.sf, self.capability)
        return control["action"] if control is not None else None

    async def prepare(self, record):
        from deerflow.runtime.execution.mutation_context import OwnershipRejected

        control = await observe_original_control(self.sf, self.capability)
        if control is None or control["action"] not in {"interrupt", "rollback"}:
            raise OwnershipRejected("Original durable winning cancellation required")
        deadline = self.teardown.budget.tighten(min(control["monotonic_deadline"], self.controller.execution_deadline))
        self.capability.begin_cancellation(deadline=deadline)
        record.abort_action = control["action"]
        record.abort_event.set()

    async def observe(self):
        while True:
            control = await observe_original_control(self.sf, self.capability)
            if control is not None:
                deadline = self.teardown.budget.tighten(min(control["monotonic_deadline"], self.controller.execution_deadline))
                self.capability.retain_cancellation_deadline(deadline)
                self.controller.close_admission(final=True)

                async def stop_original_writers():
                    await asyncio.to_thread(self.controller.request_process_stop, deadline=deadline)

                # This phase owns its actual Task through the retained deadline.
                # Signal original supervisors before graph cancellation can
                # wait for sync bash; later publication still proves settlement.
                with self.teardown.private_scope():
                    await self.teardown.phase("cancellation-process-stop", stop_original_writers)
                await self.manager.signal_execution_cancel(self.capability.context.run_id, action=control["action"])
                return
            await asyncio.sleep(0.05)

    def retain(self, task, *, role="original-executor"):
        if task is None:
            self.teardown._phases.pop(role, None)
        else:
            self.teardown._phases[role] = task
