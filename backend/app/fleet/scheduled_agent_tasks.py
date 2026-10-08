"""Immutable original scheduled-goal association and authoritative resolution.

Schedule admission owns parent/occurrence locks; STOP owns execution locks.
The latter never acquires schedule locks. Schedule readers never lock tasks or
receipts. The immutable resolution makes later human generations independent.
"""

from deerflow_ecs_fleet.persistence.models import AgentTaskRow, AttemptRow, ReservationRow, RunPlacementRow, ScheduledAgentTaskRow, SchedulerTicketRow, TaskOperationReceiptRow, WaitGroupRow, WorkspacePointRow
from sqlalchemy import exists, func, select, update

from deerflow.persistence.run.model import RunRow
from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow
from deerflow.runtime.runs.manager import ConflictError

_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "timed_out"})
_PARENT_STATUS = {"succeeded": "completed", "failed": "failed", "cancelled": "cancelled", "timed_out": "failed"}


class FleetScheduledAgentTasks:
    def __init__(self, session_factory, config):
        self.sf, self.config = session_factory, config
        self._recovery_cursor = None

    async def _original(self, session, *, task, occurrence, run_id):
        if task is None or occurrence is None or occurrence.task_id != task.id or occurrence.run_id != run_id:
            return None
        run = await session.get(RunRow, run_id)
        placement = await session.get(RunPlacementRow, run_id)
        if run is None or placement is None:
            return None
        metadata = run.metadata_json or {}
        if (
            (run.user_id, run.thread_id, placement.user_id, placement.thread_id) != (task.user_id, occurrence.thread_id, task.user_id, occurrence.thread_id)
            or run.idempotency_key != "scheduled-task:" + occurrence.id
            or (run.kwargs_json or {}).get("execution_backend") != "fleet"
            or (metadata.get("scheduled_task_id"), metadata.get("scheduled_task_run_id")) != (task.id, occurrence.id)
        ):
            return None
        agent = await session.get(AgentTaskRow, placement.agent_task_id)
        if agent is None or (agent.user_id, agent.thread_id) != (run.user_id, run.thread_id):
            return None
        return run, placement, agent

    async def associate(self, session, *, task, occurrence, admitted, agent_task_id):
        if not self.config.continuations_enabled:
            return
        original = await self._original(session, task=task, occurrence=occurrence, run_id=admitted["run_id"])
        if original is None or original[1].agent_task_id != agent_task_id:
            raise ConflictError("Original scheduled aggregate association conflicts")
        run, placement, agent = original
        if run.status != "pending" or agent.current_run_id != run.run_id or agent.generation != placement.generation:
            raise ConflictError("Original scheduled aggregate admission conflicts")
        await self._associate_original(session, task=task, occurrence=occurrence, original=original)

    @staticmethod
    async def _associate_original(session, *, task, occurrence, original):
        run, placement, agent = original
        values = dict(occurrence_id=occurrence.id, scheduled_task_id=task.id, user_id=task.user_id, thread_id=occurrence.thread_id, original_run_id=run.run_id, agent_task_id=agent.id, source_generation=placement.generation)
        row = await session.get(ScheduledAgentTaskRow, occurrence.id)
        if row is not None:
            if any(getattr(row, name) != value for name, value in values.items()):
                raise ConflictError("Immutable scheduled aggregate identity conflicts")
            return row
        row = ScheduledAgentTaskRow(**values, state="pending")
        session.add(row)
        await session.flush()
        return row

    async def _receipt(self, session, *, task_id, occurrence_id, run_id):
        row = await session.get(ScheduledAgentTaskRow, occurrence_id)
        if row is not None:
            if (row.scheduled_task_id, row.original_run_id) != (task_id, run_id):
                raise ConflictError("Scheduled aggregate completion identity conflicts")
            return row
        return None

    async def suppress_parent_completion(self, session, *, task_id, occurrence_id, run_id):
        receipt = await self._receipt(session, task_id=task_id, occurrence_id=occurrence_id, run_id=run_id)
        if receipt is not None:
            return receipt.state == "pending"
        if not self.config.continuations_enabled:
            return False
        # Older consumed tickets are still a durable association. Absence of a
        # resolution receipt is never proof of aggregate completion.
        task = await session.get(ScheduledTaskRow, task_id)
        occurrence = await session.get(ScheduledTaskRunRow, occurrence_id)
        ticket = await session.scalar(select(SchedulerTicketRow.id).where(SchedulerTicketRow.occurrence_id == occurrence_id, SchedulerTicketRow.run_id == run_id).limit(1))
        return ticket is not None and await self._original(session, task=task, occurrence=occurrence, run_id=run_id) is not None

    async def completion_outcome(self, session, *, task_id, occurrence_id, run_id):
        receipt = await self._receipt(session, task_id=task_id, occurrence_id=occurrence_id, run_id=run_id)
        if receipt is None or receipt.state != "resolved":
            return None
        return {"status": _PARENT_STATUS[receipt.resolved_status], "last_error": receipt.resolved_error}

    async def blocks(self, session, *, schedule_id, exclude_occurrence_id=None):
        pending = select(ScheduledAgentTaskRow.occurrence_id).where(ScheduledAgentTaskRow.scheduled_task_id == schedule_id, ScheduledAgentTaskRow.state == "pending")
        if exclude_occurrence_id is not None:
            pending = pending.where(ScheduledAgentTaskRow.occurrence_id != exclude_occurrence_id)
        if await session.scalar(pending.limit(1)) is not None:
            return True
        if not self.config.continuations_enabled:
            return False
        # Filter valid historical associations before limiting; stale tickets
        # cannot hide a later unresolved original goal.
        old = self._historical().where(SchedulerTicketRow.scheduled_task_id == schedule_id)
        if exclude_occurrence_id is not None:
            old = old.where(SchedulerTicketRow.occurrence_id != exclude_occurrence_id)
        return (await session.execute(old.limit(1))).first() is not None

    @staticmethod
    def _historical():
        return (
            select(SchedulerTicketRow.scheduled_task_id, SchedulerTicketRow.occurrence_id, SchedulerTicketRow.run_id)
            .join(
                ScheduledTaskRunRow,
                ScheduledTaskRunRow.id == SchedulerTicketRow.occurrence_id,
            )
            .join(ScheduledTaskRow, ScheduledTaskRow.id == SchedulerTicketRow.scheduled_task_id)
            .join(
                RunRow,
                RunRow.run_id == SchedulerTicketRow.run_id,
            )
            .join(RunPlacementRow, RunPlacementRow.run_id == RunRow.run_id)
            .join(
                AgentTaskRow,
                AgentTaskRow.id == RunPlacementRow.agent_task_id,
            )
            .where(
                ScheduledTaskRunRow.task_id == ScheduledTaskRow.id,
                ScheduledTaskRunRow.run_id == RunRow.run_id,
                ScheduledTaskRow.user_id == RunRow.user_id,
                ScheduledTaskRunRow.thread_id == RunRow.thread_id,
                RunPlacementRow.user_id == RunRow.user_id,
                RunPlacementRow.thread_id == RunRow.thread_id,
                AgentTaskRow.user_id == RunRow.user_id,
                AgentTaskRow.thread_id == RunRow.thread_id,
                RunRow.idempotency_key == "scheduled-task:" + ScheduledTaskRunRow.id,
                RunRow.kwargs_json["execution_backend"].as_string() == "fleet",
                RunRow.metadata_json["scheduled_task_id"].as_string() == ScheduledTaskRow.id,
                RunRow.metadata_json["scheduled_task_run_id"].as_string() == ScheduledTaskRunRow.id,
                ~exists(select(ScheduledAgentTaskRow.occurrence_id).where(ScheduledAgentTaskRow.occurrence_id == SchedulerTicketRow.occurrence_id)),
            )
            .distinct()
        )

    async def _resolve(self, session, receipts, *, run, placement, status, error, now, kind, attempt=None, point=None, operation=None):
        for receipt in receipts:
            if receipt.source_generation != placement.generation and not await self._follows_original_goal(session, receipt=receipt, run_id=run.run_id, generation=placement.generation):
                continue
            await session.execute(
                update(ScheduledAgentTaskRow)
                .where(ScheduledAgentTaskRow.occurrence_id == receipt.occurrence_id, ScheduledAgentTaskRow.state == "pending")
                .values(
                    state="resolved",
                    resolution_kind=kind,
                    resolved_run_id=run.run_id,
                    resolved_generation=placement.generation,
                    resolved_attempt_id=attempt.id if attempt else None,
                    resolved_node_session_id=attempt.node_session_id if attempt else None,
                    resolved_workspace_point_id=point.id if point else None,
                    resolved_operation_id=operation.id if operation else None,
                    resolved_status=status,
                    resolved_error=error,
                    resolved_at=now,
                )
            )
        await session.flush()

    async def resolve_stopped(self, session, *, task, run, placement, attempt, point, reservation, now):
        from deerflow_ecs_fleet.persistence.workspace_points import accepted_final

        if point is None or point.desired_task_status not in _TERMINAL:
            return
        accepted = await accepted_final(session, task=task, run=run, placement=placement, attempt=attempt)
        if (
            accepted is None
            or accepted.id != point.id
            or point.kind != "final"
            or task.state != point.desired_task_status
            or attempt.stopped_at is None
            or attempt.finished_at is None
            or attempt.process_ref != "fleet-" + attempt.id
            or reservation.attempt_id != attempt.id
            or reservation.state != "released"
            or reservation.released_at is None
            or placement.state != point.desired_placement_status
            or attempt.node_session_id != point.node_session_id
        ):
            return
        receipts = (
            await session.scalars(
                select(ScheduledAgentTaskRow).where(
                    ScheduledAgentTaskRow.agent_task_id == task.id,
                    ScheduledAgentTaskRow.user_id == run.user_id,
                    ScheduledAgentTaskRow.thread_id == run.thread_id,
                    ScheduledAgentTaskRow.state == "pending",
                    ScheduledAgentTaskRow.source_generation <= placement.generation,
                )
            )
        ).all()
        await self._resolve(session, receipts, run=run, placement=placement, status=point.desired_task_status, error=point.error, now=now, kind="stopped", attempt=attempt, point=point)

    async def resolve_cancelled(self, session, *, task, run, placement, attempt, point, reservation, operation, now):
        """Original completed owned cancellation consumes its accepted STOP.

        The operation deliberately advances the task generation. Its immutable
        source tuple, not the new generation, identifies the stopped execution.
        Requested handoffs and stop_only calls cannot grant this authority.
        """
        from deerflow_ecs_fleet.persistence.workspace_points import accepted_source_identity

        accepted = await accepted_source_identity(session, point=point, run=run, placement=placement, attempt=attempt)
        outstanding = await session.scalar(
            select(AttemptRow.id)
            .join(RunPlacementRow, RunPlacementRow.run_id == AttemptRow.run_id)
            .where(
                RunPlacementRow.agent_task_id == task.id,
                (AttemptRow.stopped_at.is_(None)) | AttemptRow.state.in_(["unknown", "quarantined"]),
            )
            .limit(1)
        )
        if (
            accepted is None
            or operation.operation != "cancel"
            or operation.state != "completed"
            or outstanding is not None
            or (operation.agent_task_id, operation.user_id, operation.thread_id, operation.source_run_id, operation.source_generation) != (task.id, run.user_id, run.thread_id, run.run_id, placement.generation)
            or (operation.source_workspace_point_id, operation.source_checkpoint_id) != (point.id, point.checkpoint_id)
            or operation.target_generation != operation.source_generation + 1
            or task.generation != operation.target_generation
            or task.current_run_id != run.run_id
            or task.state != "cancelled"
            or task.accepted_workspace_point_id != point.id
            or (task.user_id, task.thread_id) != (run.user_id, run.thread_id)
            or attempt.stopped_at is None
            or attempt.finished_at is None
            or attempt.process_ref != "fleet-" + attempt.id
            or attempt.node_session_id != point.node_session_id
            or reservation.attempt_id != attempt.id
            or reservation.state != "released"
            or reservation.released_at is None
        ):
            raise ConflictError("Scheduled aggregate cancellation lacks original completed operation proof")
        receipts = (
            await session.scalars(
                select(ScheduledAgentTaskRow).where(
                    ScheduledAgentTaskRow.agent_task_id == task.id,
                    ScheduledAgentTaskRow.user_id == run.user_id,
                    ScheduledAgentTaskRow.thread_id == run.thread_id,
                    ScheduledAgentTaskRow.state == "pending",
                    ScheduledAgentTaskRow.source_generation <= operation.source_generation,
                )
            )
        ).all()
        await self._resolve(session, receipts, run=run, placement=placement, status="cancelled", error=None, now=now, kind="cancelled", attempt=attempt, point=point, operation=operation)

    async def resolve_unassigned(self, session, *, task, run, placement, now):
        # Called only after the original lock_pending retirement authorizes the
        # pending tuple and releases its tickets. A prior assignment cannot pass.
        assigned = await session.scalar(select(AttemptRow.id).where(AttemptRow.run_id == run.run_id).limit(1))
        live = await session.scalar(select(ReservationRow.id).join(SchedulerTicketRow, SchedulerTicketRow.id == ReservationRow.ticket_id).where(SchedulerTicketRow.run_id == run.run_id, ReservationRow.state != "released").limit(1))
        outstanding = await session.scalar(
            select(AttemptRow.id)
            .join(RunPlacementRow, RunPlacementRow.run_id == AttemptRow.run_id)
            .join(ReservationRow, ReservationRow.attempt_id == AttemptRow.id)
            .where(
                RunPlacementRow.agent_task_id == task.id,
                (AttemptRow.stopped_at.is_(None)) | (ReservationRow.state != "released"),
            )
            .limit(1)
        )
        if (
            assigned is not None
            or live is not None
            or outstanding is not None
            or run.status != "interrupted"
            or run.owner_worker_id is not None
            or run.lease_expires_at is not None
            or task.state != "cancelled"
            or placement.state != "cancelled"
            or placement.active_attempt_id is not None
            or (task.current_run_id, task.generation, placement.agent_task_id) != (run.run_id, placement.generation, task.id)
            or (task.user_id, task.thread_id, placement.user_id, placement.thread_id) != (run.user_id, run.thread_id, run.user_id, run.thread_id)
        ):
            raise ConflictError("Scheduled aggregate retirement lacks original unassigned proof")
        receipts = (
            await session.scalars(
                select(ScheduledAgentTaskRow).where(
                    ScheduledAgentTaskRow.agent_task_id == task.id,
                    ScheduledAgentTaskRow.user_id == run.user_id,
                    ScheduledAgentTaskRow.thread_id == run.thread_id,
                    ScheduledAgentTaskRow.state == "pending",
                    ScheduledAgentTaskRow.source_generation <= placement.generation,
                )
            )
        ).all()
        await self._resolve(session, receipts, run=run, placement=placement, status="cancelled", error=run.error, now=now, kind="unassigned")

    async def _next_original_run(self, session, *, receipt, run, placement, attempt, point):
        """An earlier terminal boundary can never lead to a new goal outcome."""
        if point.desired_task_status in _TERMINAL:
            return None
        cancelled = await session.scalar(
            select(TaskOperationReceiptRow.id)
            .where(
                TaskOperationReceiptRow.agent_task_id == receipt.agent_task_id,
                TaskOperationReceiptRow.user_id == receipt.user_id,
                TaskOperationReceiptRow.thread_id == receipt.thread_id,
                TaskOperationReceiptRow.source_generation == placement.generation,
                TaskOperationReceiptRow.source_run_id == run.run_id,
                TaskOperationReceiptRow.operation == "cancel",
                TaskOperationReceiptRow.state == "completed",
            )
            .limit(1)
        )
        if cancelled is not None:
            return None
        group = (
            await session.scalar(
                select(WaitGroupRow)
                .where(
                    WaitGroupRow.parent_run_id == run.run_id,
                    WaitGroupRow.agent_task_id == receipt.agent_task_id,
                    WaitGroupRow.generation == placement.generation,
                    WaitGroupRow.user_id == receipt.user_id,
                    WaitGroupRow.thread_id == receipt.thread_id,
                    WaitGroupRow.workspace_point_id == point.id,
                    WaitGroupRow.checkpoint_id == point.checkpoint_id,
                    WaitGroupRow.state == "dispatched",
                    WaitGroupRow.continuation_run_id.is_not(None),
                    WaitGroupRow.dispatched_at.is_not(None),
                )
                .limit(1)
            )
            if point.kind == "final" and point.desired_task_status == "waiting_jobs"
            else None
        )
        if group is not None:
            next_run_id, next_generation = group.continuation_run_id, placement.generation
        else:
            # Human continuation of an unresolved goal is admitted from this
            # exact stopped nonterminal point. Resume after succeeded cannot
            # pass the earlier terminal check, even if old proof is missing.
            operation = await session.scalar(
                select(TaskOperationReceiptRow)
                .where(
                    TaskOperationReceiptRow.agent_task_id == receipt.agent_task_id,
                    TaskOperationReceiptRow.user_id == receipt.user_id,
                    TaskOperationReceiptRow.thread_id == receipt.thread_id,
                    TaskOperationReceiptRow.source_run_id == run.run_id,
                    TaskOperationReceiptRow.source_generation == placement.generation,
                    TaskOperationReceiptRow.source_point_generation == placement.generation,
                    TaskOperationReceiptRow.target_generation == placement.generation + 1,
                    TaskOperationReceiptRow.source_workspace_point_id == point.id,
                    TaskOperationReceiptRow.source_checkpoint_id == point.checkpoint_id,
                    TaskOperationReceiptRow.operation.in_(["message", "resume", "checkpoint_write", "delete"]),
                    TaskOperationReceiptRow.state == "admitted",
                    TaskOperationReceiptRow.admitted_run_id.is_not(None),
                )
                .limit(1)
            )
            if operation is None:
                return None
            next_run_id, next_generation = operation.admitted_run_id, operation.target_generation
        from deerflow_ecs_fleet.persistence.placements import RunPlacements

        next_placement = await session.get(RunPlacementRow, next_run_id)
        if next_placement is None or (next_placement.agent_task_id, next_placement.generation, next_placement.user_id, next_placement.thread_id) != (
            receipt.agent_task_id,
            next_generation,
            receipt.user_id,
            receipt.thread_id,
        ):
            return None
        spec = await RunPlacements().load_launch_spec(session, run_id=next_run_id, user_id=receipt.user_id, thread_id=receipt.thread_id)
        if (spec.agent_task_id, spec.generation, spec.source_workspace_point_id, spec.source_workspace_checkpoint_id) != (
            receipt.agent_task_id,
            next_generation,
            point.id,
            point.checkpoint_id,
        ):
            return None
        return next_run_id

    async def _follows_original_goal(self, session, *, receipt, run_id, generation):
        from deerflow_ecs_fleet.persistence.workspace_points import accepted_source_identity

        current, seen = receipt.original_run_id, set()
        while current not in seen:
            seen.add(current)
            run = await session.get(RunRow, current)
            placement = await session.get(RunPlacementRow, current)
            if (
                run is None
                or placement is None
                or (placement.agent_task_id, placement.user_id, placement.thread_id, run.user_id, run.thread_id)
                != (
                    receipt.agent_task_id,
                    receipt.user_id,
                    receipt.thread_id,
                    receipt.user_id,
                    receipt.thread_id,
                )
                or placement.generation < receipt.source_generation
            ):
                return False
            if current == run_id:
                return placement.generation == generation
            point = await session.get(WorkspacePointRow, placement.final_workspace_point_id) if placement.final_workspace_point_id else None
            # Missing proof cannot be replaced by a later human result. Check
            # the old terminal boundary before attempting to validate its proof.
            if point is None or point.desired_task_status in _TERMINAL:
                return False
            attempt = await session.get(AttemptRow, placement.active_attempt_id) if placement.active_attempt_id else None
            reservation = await session.scalar(select(ReservationRow).where(ReservationRow.attempt_id == attempt.id)) if attempt else None
            if (
                attempt is None
                or reservation is None
                or attempt.stopped_at is None
                or attempt.finished_at is None
                or attempt.process_ref != "fleet-" + attempt.id
                or reservation.state != "released"
                or reservation.released_at is None
                or placement.state != point.desired_placement_status
                or await accepted_source_identity(session, point=point, run=run, placement=placement, attempt=attempt, require_latest_checkpoint=False) is None
            ):
                return False
            current = await self._next_original_run(session, receipt=receipt, run=run, placement=placement, attempt=attempt, point=point)
            if current is None:
                return False
        return False

    async def _recover_original_resolution(self, session, *, receipt):
        """First terminal proof in the original unresolved goal lineage."""
        from deerflow_ecs_fleet.persistence.workspace_points import accepted_source_identity

        run_id = receipt.original_run_id
        seen = set()
        while run_id not in seen:
            seen.add(run_id)
            run = await session.get(RunRow, run_id)
            placement = await session.get(RunPlacementRow, run_id)
            if (
                run is None
                or placement is None
                or (placement.agent_task_id, placement.user_id, placement.thread_id, run.user_id, run.thread_id) != (receipt.agent_task_id, receipt.user_id, receipt.thread_id, receipt.user_id, receipt.thread_id)
                or placement.generation < receipt.source_generation
            ):
                return
            attempt = await session.get(AttemptRow, placement.active_attempt_id) if placement.active_attempt_id else None
            point = await session.get(WorkspacePointRow, placement.final_workspace_point_id) if placement.final_workspace_point_id else None
            reservation = await session.scalar(select(ReservationRow).where(ReservationRow.attempt_id == attempt.id)) if attempt else None
            if (
                attempt is None
                or point is None
                or reservation is None
                or attempt.stopped_at is None
                or attempt.finished_at is None
                or attempt.process_ref != "fleet-" + attempt.id
                or reservation.state != "released"
                or reservation.released_at is None
                or placement.state != point.desired_placement_status
                or await accepted_source_identity(session, point=point, run=run, placement=placement, attempt=attempt, require_latest_checkpoint=False) is None
            ):
                return
            now = await session.scalar(select(func.clock_timestamp()))
            if point.kind == "final" and point.desired_task_status in _TERMINAL:
                await self._resolve(session, [receipt], run=run, placement=placement, status=point.desired_task_status, error=point.error, now=now, kind="stopped", attempt=attempt, point=point)
                return
            operation = await session.scalar(
                select(TaskOperationReceiptRow)
                .where(
                    TaskOperationReceiptRow.agent_task_id == receipt.agent_task_id,
                    TaskOperationReceiptRow.user_id == receipt.user_id,
                    TaskOperationReceiptRow.thread_id == receipt.thread_id,
                    TaskOperationReceiptRow.source_run_id == run.run_id,
                    TaskOperationReceiptRow.source_generation == placement.generation,
                    TaskOperationReceiptRow.target_generation == placement.generation + 1,
                    TaskOperationReceiptRow.operation == "cancel",
                    TaskOperationReceiptRow.state == "completed",
                    TaskOperationReceiptRow.source_workspace_point_id == point.id,
                    TaskOperationReceiptRow.source_checkpoint_id == point.checkpoint_id,
                )
                .limit(1)
            )
            if operation is not None:
                outstanding = await session.scalar(
                    select(AttemptRow.id)
                    .join(RunPlacementRow, RunPlacementRow.run_id == AttemptRow.run_id)
                    .where(
                        RunPlacementRow.agent_task_id == receipt.agent_task_id,
                        RunPlacementRow.generation == placement.generation,
                        (AttemptRow.stopped_at.is_(None)) | AttemptRow.state.in_(["unknown", "quarantined"]),
                    )
                    .limit(1)
                )
                if outstanding is None:
                    await self._resolve(session, [receipt], run=run, placement=placement, status="cancelled", error=None, now=now, kind="cancelled", attempt=attempt, point=point, operation=operation)
                return
            run_id = await self._next_original_run(session, receipt=receipt, run=run, placement=placement, attempt=attempt, point=point)
            if run_id is None:
                return

    async def reconcile_once(self):
        """Trusted parent/occurrence writer reads immutable terminal receipts.

        Historical admission can be repaired from its original ticket/placement.
        Historical resolution needs the actual accepted final plus STOP/ledger;
        task.state or the initial core success alone never grants authority.
        """
        async with self.sf.begin() as session:
            missing = (await session.execute(self._historical().order_by(SchedulerTicketRow.scheduled_task_id, SchedulerTicketRow.occurrence_id, SchedulerTicketRow.run_id).limit(64))).all() if self.config.continuations_enabled else []
            missing_runs = {(task_id, occurrence_id): run_id for task_id, occurrence_id, run_id in missing}
            pending_query = select(ScheduledAgentTaskRow).where(ScheduledAgentTaskRow.state == "pending").order_by(ScheduledAgentTaskRow.occurrence_id)
            page = (await session.scalars(pending_query.where(ScheduledAgentTaskRow.occurrence_id > self._recovery_cursor).limit(64) if self._recovery_cursor is not None else pending_query.limit(64))).all()
            if not page and self._recovery_cursor is not None:
                page = (await session.scalars(pending_query.limit(64))).all()
            pending_keys = {(row.scheduled_task_id, row.occurrence_id) for row in page}
            actionable = (
                await session.execute(
                    select(ScheduledAgentTaskRow.scheduled_task_id, ScheduledAgentTaskRow.occurrence_id)
                    .join(
                        ScheduledTaskRow,
                        ScheduledTaskRow.id == ScheduledAgentTaskRow.scheduled_task_id,
                    )
                    .where(
                        ScheduledTaskRow.schedule_type == "once",
                        ScheduledTaskRow.last_run_id == ScheduledAgentTaskRow.original_run_id,
                        ((ScheduledAgentTaskRow.state == "resolved") & (ScheduledTaskRow.status == "running")) | ((ScheduledAgentTaskRow.state == "pending") & (ScheduledTaskRow.status == "completed")),
                    )
                    .order_by(ScheduledAgentTaskRow.scheduled_task_id, ScheduledAgentTaskRow.occurrence_id)
                    .limit(64)
                )
            ).all()
            changed = 0
            # All three bounded sources merge before any lock. Concurrent host
            # reconciliation always takes parents/occurrences in the same order.
            for task_id, occurrence_id in sorted(set(missing_runs) | pending_keys | set(actionable)):
                parent = await session.get(ScheduledTaskRow, task_id, with_for_update=True)
                occurrence = await session.get(ScheduledTaskRunRow, occurrence_id, with_for_update=True)
                receipt = await session.get(ScheduledAgentTaskRow, occurrence_id, populate_existing=True)
                if receipt is None and (task_id, occurrence_id) in missing_runs:
                    original = await self._original(session, task=parent, occurrence=occurrence, run_id=missing_runs[task_id, occurrence_id])
                    if original is None:
                        continue
                    receipt = await self._associate_original(session, task=parent, occurrence=occurrence, original=original)
                if receipt is None:
                    continue
                if receipt.state == "pending":
                    original = await self._original(session, task=parent, occurrence=occurrence, run_id=receipt.original_run_id)
                    if original is None or (original[1].agent_task_id, original[1].generation) != (receipt.agent_task_id, receipt.source_generation):
                        continue
                    await self._recover_original_resolution(session, receipt=receipt)
                    receipt = await session.get(ScheduledAgentTaskRow, occurrence_id, populate_existing=True)
                if (
                    parent is None
                    or occurrence is None
                    or parent.schedule_type != "once"
                    or (parent.user_id, parent.last_thread_id, parent.last_run_id, occurrence.task_id, occurrence.run_id, occurrence.thread_id)
                    != (receipt.user_id, receipt.thread_id, receipt.original_run_id, parent.id, receipt.original_run_id, receipt.thread_id)
                ):
                    continue
                if receipt.state == "pending":
                    if parent.status == "completed":
                        parent.status, parent.last_error = "running", None
                        changed += 1
                    continue
                if parent.status != "running":
                    continue
                parent.status, parent.last_error = _PARENT_STATUS[receipt.resolved_status], receipt.resolved_error
                parent.updated_at = await session.scalar(select(func.clock_timestamp()))
                changed += 1
            self._recovery_cursor = page[-1].occurrence_id if page else None
            return changed
