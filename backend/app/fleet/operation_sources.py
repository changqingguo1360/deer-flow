"""Only a precise cancelled unassigned receipt chain can reuse an older point."""

from sqlalchemy import select

from deerflow.persistence.run.model import RunRow


async def unassigned_source(session, *, task_id, user_id, thread_id, generation, run_id, point, check_chain=True):
    from deerflow_ecs_fleet.persistence.models import AttemptRow, ReservationRow, RunPlacementRow, SchedulerTicketRow, TaskOperationReceiptRow, WaitGroupRow
    from deerflow_ecs_fleet.persistence.placements import RunPlacements

    run = await session.get(RunRow, run_id)
    placement = await session.get(RunPlacementRow, run_id)
    attempt = await session.scalar(select(AttemptRow.id).where(AttemptRow.run_id == run_id).limit(1))
    charged = await session.scalar(
        select(ReservationRow.id).join(SchedulerTicketRow, SchedulerTicketRow.id == ReservationRow.ticket_id).where(SchedulerTicketRow.run_id == run_id, (ReservationRow.state != "released") | ReservationRow.released_at.is_(None)).limit(1)
    )
    if (
        run is None
        or placement is None
        or attempt is not None
        or charged is not None
        or placement.active_attempt_id is not None
        or placement.state != "cancelled"
        or run.status != "interrupted"
        or run.cancel_action != "interrupt"
        or run.cancel_requested_at is None
        or (run.user_id, run.thread_id, placement.agent_task_id, placement.user_id, placement.thread_id, placement.generation) != (user_id, thread_id, task_id, user_id, thread_id, generation)
    ):
        return False, None
    spec = await RunPlacements().load_launch_spec(session, run_id=run_id, user_id=user_id, thread_id=thread_id)
    if (spec.agent_task_id, spec.generation, spec.source_workspace_point_id, spec.source_workspace_checkpoint_id) != (task_id, generation, point.id, point.checkpoint_id):
        return False, None
    if generation == point.generation:
        group = await session.scalar(select(WaitGroupRow).where(WaitGroupRow.continuation_run_id == run_id))
        return (
            group is not None
            and group.state == "dispatched"
            and group.dispatched_at is not None
            and (group.agent_task_id, group.user_id, group.thread_id, group.generation, group.parent_run_id, group.workspace_point_id, group.checkpoint_id)
            == (task_id, user_id, thread_id, generation, point.run_id, point.id, point.checkpoint_id)
        ), None
    prior = await session.scalar(select(TaskOperationReceiptRow).where(TaskOperationReceiptRow.admitted_run_id == run_id))
    if prior is None or check_chain and not await receipt_chain(session, receipt=prior, point=point):
        return False, None
    if prior.target_generation != generation:
        return False, None
    return True, prior.id


async def receipt_chain(session, *, receipt, point):
    from deerflow_ecs_fleet.persistence.models import TaskOperationReceiptRow

    seen = set()
    current = receipt
    for _ in range(64):
        if current.id in seen:
            return False
        seen.add(current.id)
        if (
            current.state != "admitted"
            or current.operation not in {"message", "resume"}
            or current.target_generation != current.source_generation + 1
            or (current.agent_task_id, current.user_id, current.thread_id, current.source_run_id, current.source_workspace_point_id, current.source_checkpoint_id, current.source_point_generation)
            != (point.agent_task_id, point.user_id, point.thread_id, point.run_id, point.id, point.checkpoint_id, point.generation)
        ):
            return False
        if current.source_generation == point.generation:
            return current.preceding_receipt_id is None
        if current.preceding_receipt_id is None or current.stopped_run_id is None:
            return False
        valid, preceding_id = await unassigned_source(
            session, task_id=current.agent_task_id, user_id=current.user_id, thread_id=current.thread_id, generation=current.source_generation, run_id=current.stopped_run_id, point=point, check_chain=False
        )
        if not valid or preceding_id != current.preceding_receipt_id:
            return False
        preceding = await session.get(TaskOperationReceiptRow, current.preceding_receipt_id)
        if preceding is None or preceding.target_generation != current.source_generation:
            return False
        current = preceding
    return False
