"""Owned task cancellation freezes original generation until proven STOP."""

from uuid import uuid4

from sqlalchemy import select, text

from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.manager import CancelOutcome, ConflictError


async def cancel_owned_task(ownership, *, user_id, thread_id, task_id, expected_generation, idempotency_key, operation="cancel", request_digest=None, stop_only=False):
    from deerflow_ecs_fleet.job_service import FleetJobService
    from deerflow_ecs_fleet.persistence.models import AgentTaskRow, AttemptRow, JobLinkRow, JobRow, NodeRow, ReservationRow, RunPlacementRow, TaskOperationReceiptRow, WaitGroupRow

    from .agent_control import FleetAgentRunControl
    from .execution import fleet_before_thread_guard

    async with ownership.sf.begin() as session:
        await fleet_before_thread_guard(session, user_id=user_id, thread_id=thread_id)
        task = await session.get(AgentTaskRow, task_id, with_for_update=True)
        if task is None or (task.user_id, task.thread_id) != (user_id, thread_id):
            raise LookupError("Agent task not found")
        receipt = await session.scalar(
            select(TaskOperationReceiptRow).where(TaskOperationReceiptRow.agent_task_id == task_id, TaskOperationReceiptRow.operation == operation, TaskOperationReceiptRow.idempotency_key == idempotency_key).with_for_update()
        )
        if receipt is not None:
            if (receipt.user_id, receipt.thread_id, receipt.source_generation, receipt.request_digest) != (user_id, thread_id, expected_generation, request_digest):
                raise ConflictError("Original cancellation identity conflicts")
            if receipt.state == "completed":
                return dict(task_id=task_id, generation=receipt.target_generation, state="cancelled", operation_id=receipt.id)
            if receipt.state != "requested" or task.generation != expected_generation or task.current_run_id != receipt.source_run_id:
                raise ConflictError("Original cancellation changed")
        elif task.generation != expected_generation or task.state not in {"queued", "running", "waiting_jobs"}:
            raise ConflictError("Task cancellation generation/state conflicts")
        group = await session.get(WaitGroupRow, task.wait_group_id, with_for_update=True) if task.wait_group_id else None
        if group is None and stop_only:
            prior = await session.scalar(
                select(TaskOperationReceiptRow).where(TaskOperationReceiptRow.agent_task_id == task.id, TaskOperationReceiptRow.wait_group_id.is_not(None)).order_by(TaskOperationReceiptRow.created_at.desc()).limit(1)
            )
            group = await session.get(WaitGroupRow, prior.wait_group_id, with_for_update=True) if prior else await session.scalar(select(WaitGroupRow).where(WaitGroupRow.continuation_run_id == task.current_run_id).with_for_update())
        if stop_only and group is None:
            raise ConflictError("Human handoff requires original waiting lineage")
        if group is not None and ((group.agent_task_id, group.user_id, group.thread_id) != (task.id, user_id, thread_id) or group.generation > task.generation or not stop_only and group.generation != task.generation):
            raise ConflictError("Original waiting cancellation conflicts")
        run = await session.get(RunRow, task.current_run_id, with_for_update=True)
        placement = await session.get(RunPlacementRow, task.current_run_id, with_for_update=True)
        if run is None or placement is None or (run.user_id, run.thread_id, placement.agent_task_id, placement.generation) != (user_id, thread_id, task.id, task.generation):
            raise ConflictError("Original task cancellation execution conflicts")
        links = list(
            (
                await session.scalars(
                    select(JobLinkRow)
                    .where(
                        JobLinkRow.agent_task_id == task.id,
                        JobLinkRow.generation == task.generation,
                        JobLinkRow.link_mode == "awaited",
                    )
                    .order_by(JobLinkRow.job_id)
                )
            ).all()
        )
        for parent_id in sorted({link.parent_run_id for link in links}):
            original_run = await session.get(RunRow, parent_id, with_for_update=True)
            original_placement = await session.get(RunPlacementRow, parent_id, with_for_update=True)
            if (
                original_run is None
                or original_placement is None
                or (original_run.user_id, original_run.thread_id, original_placement.agent_task_id, original_placement.user_id, original_placement.thread_id, original_placement.generation)
                != (user_id, thread_id, task.id, user_id, thread_id, task.generation)
            ):
                raise ConflictError("Original awaited parent execution conflicts")
        jobs = []
        for link in links:
            job = await session.get(JobRow, link.job_id, with_for_update=True)
            if job is None or (link.user_id, link.thread_id, job.user_id, job.thread_id, job.source_run_id) != (user_id, thread_id, user_id, thread_id, link.parent_run_id):
                raise ConflictError("Original awaited cancellation owner conflicts")
            jobs.append(job)
        attempts = list((await session.scalars(select(AttemptRow).where((AttemptRow.run_id == run.run_id) | AttemptRow.job_id.in_([job.id for job in jobs])).order_by(AttemptRow.id))).all())
        for node_id in sorted({attempt.node_id for attempt in attempts}):
            await session.get(NodeRow, node_id, with_for_update=True)
        for attempt in attempts:
            await session.scalar(select(ReservationRow).where(ReservationRow.attempt_id == attempt.id).with_for_update())
        for attempt in attempts:
            await session.get(AttemptRow, attempt.id, with_for_update=True, populate_existing=True)
        if receipt is None:
            receipt = TaskOperationReceiptRow(
                id="operation-" + uuid4().hex,
                agent_task_id=task.id,
                user_id=user_id,
                thread_id=thread_id,
                operation=operation,
                idempotency_key=idempotency_key,
                request_digest=request_digest,
                source_generation=expected_generation,
                target_generation=expected_generation,
                source_run_id=run.run_id,
                wait_group_id=group.id if group else None,
                state="requested",
            )
            session.add(receipt)
        service = FleetJobService(None, ownership.config, tracking_reader=None)
        for job in jobs:
            await service.cancel_locked(session, job)
        parent = next((attempt for attempt in attempts if attempt.id == placement.active_attempt_id), None)
        if parent is None:
            from .scheduler_tickets import FleetSchedulerTickets

            outcome = await FleetSchedulerTickets(ownership.sf, None).cancel_unassigned(session, run_id=run.run_id, action="interrupt")
            if outcome != CancelOutcome.cancelled:
                raise ConflictError("Unassigned cancellation conflicts")
        elif parent.stopped_at is None:
            outcome = await FleetAgentRunControl(ownership.sf)._request_assigned_cancel(session, run_id=run.run_id, attempt=parent, action="interrupt")
            if outcome not in {CancelOutcome.requested, CancelOutcome.cancelled}:
                raise ConflictError("Original assigned cancellation conflicts")
            # Preserve all publication fields until the original controlled STOP.
            await session.flush()
            return dict(task_id=task_id, generation=task.generation, state="cancellation_requested", operation_id=receipt.id)
        else:
            reservation = await session.scalar(select(ReservationRow).where(ReservationRow.attempt_id == parent.id))
            from deerflow_ecs_fleet.persistence.workspace_points import accepted_final

            point = await accepted_final(session, task=task, run=run, placement=placement, attempt=parent)
            unresolved = await session.scalar(
                select(AttemptRow.id)
                .join(RunPlacementRow, RunPlacementRow.run_id == AttemptRow.run_id)
                .where(RunPlacementRow.agent_task_id == task.id, (AttemptRow.stopped_at.is_(None)) | AttemptRow.state.in_(["unknown", "quarantined"]))
                .limit(1)
            )
            if point is None or unresolved is not None or parent.finished_at is None or parent.process_ref != "fleet-" + parent.id or reservation is None or reservation.state != "released" or reservation.released_at is None:
                raise ConflictError("Cancellation requires exact accepted STOP and release")
            receipt.source_workspace_point_id, receipt.source_checkpoint_id = point.id, point.checkpoint_id
        if stop_only:
            await session.flush()
            return dict(task_id=task_id, generation=task.generation, state="stopped", operation_id=receipt.id)
        now = await session.scalar(text("SELECT clock_timestamp()"))
        task.generation += 1
        task.state, task.cancel_requested_at = "cancelled", now
        receipt.target_generation, receipt.state = task.generation, "completed"
        await session.flush()
        return dict(task_id=task_id, generation=task.generation, state=task.state, operation_id=receipt.id)


async def stop_before_human(ownership, parameters, key, request, *, operation="message", digest=None, expected=None):
    """A requested receipt cannot grant a new generation before original STOP."""
    from deerflow_ecs_fleet.persistence.models import AgentTaskRow, TaskOperationReceiptRow

    from .agent_control import FleetAgentRunControl
    from .execution import AGENT_TASK_ACTIVE

    if ownership is None:
        return
    async with ownership.sf() as session:
        receipt = await session.scalar(
            select(TaskOperationReceiptRow).where(
                TaskOperationReceiptRow.user_id == parameters.user_id, TaskOperationReceiptRow.thread_id == parameters.thread_id, TaskOperationReceiptRow.operation == operation, TaskOperationReceiptRow.idempotency_key == key
            )
        )
        if receipt is not None and receipt.state == "admitted":
            return
        task = (
            await session.get(AgentTaskRow, receipt.agent_task_id)
            if receipt
            else await session.scalar(select(AgentTaskRow).where(AgentTaskRow.user_id == parameters.user_id, AgentTaskRow.thread_id == parameters.thread_id, text(AGENT_TASK_ACTIVE)))
        )
        if expected is not None and (task is None or (task.id, receipt.source_generation if receipt else task.generation) != expected):
            raise ConflictError("Task generation changed before original STOP")
        if task is None or task.state == "waiting_jobs":
            return
        if receipt is None and task.state not in {"queued", "running"}:
            return
        task_id, generation, run_id = task.id, receipt.source_generation if receipt else task.generation, task.current_run_id
    from app.gateway.authz import require_cancel_permission_if

    require_cancel_permission_if(request, True)
    await cancel_owned_task(ownership, user_id=parameters.user_id, thread_id=parameters.thread_id, task_id=task_id, expected_generation=generation, idempotency_key=key, operation=operation, request_digest=digest, stop_only=True)
    if not await FleetAgentRunControl(ownership.sf).wait_stopped(run_id, disconnected=getattr(request, "is_disconnected", None)):
        raise ConflictError("Original cancellation remains pending until matching STOP")
