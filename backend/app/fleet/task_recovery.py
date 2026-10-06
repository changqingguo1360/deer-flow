"""Bounded nonautomatic adjudication; deadlines never release physical charges."""

from deerflow_ecs_fleet.persistence.models import AgentTaskRow, AttemptRow, JobLinkRow, JobRow, NodeRow, ReservationRow, RunPlacementRow, TaskBudgetRow, TaskOperationReceiptRow
from deerflow_ecs_fleet.persistence.workspace_points import accepted_final
from sqlalchemy import select, text, tuple_

from deerflow.persistence.run.model import RunRow


class FleetTaskRecovery:
    def __init__(self, session_factory):
        self.sf, self.cursor = session_factory, None

    async def scan(self):
        async with self.sf() as session:
            query = select(AgentTaskRow.id, AgentTaskRow.created_at)
            if self.cursor is not None:
                query = query.where(tuple_(AgentTaskRow.created_at, AgentTaskRow.id) > self.cursor)
            tasks = (await session.execute(query.order_by(AgentTaskRow.created_at, AgentTaskRow.id).limit(64))).all()
            self.cursor = (tasks[-1].created_at, tasks[-1].id) if tasks else None
        for task_id, _ in tasks:
            await self.reconcile(task_id)

    async def reconcile(self, task_id):
        async with self.sf.begin() as session:
            task = await session.get(AgentTaskRow, task_id, with_for_update=True)
            if task is None or task.current_run_id is None:
                return
            run = await session.get(RunRow, task.current_run_id, with_for_update=True)
            placement = await session.get(RunPlacementRow, task.current_run_id, with_for_update=True)
            budget = await session.get(TaskBudgetRow, task.id, with_for_update=True)
            if run is None or placement is None or budget is None:
                return
            jobs = list((await session.scalars(select(JobRow).join(JobLinkRow, JobLinkRow.job_id == JobRow.id).where(JobLinkRow.agent_task_id == task.id).order_by(JobRow.id).with_for_update(of=JobRow))).all())
            locators = list((await session.scalars(select(AttemptRow).where((AttemptRow.id == placement.active_attempt_id) | AttemptRow.job_id.in_([job.id for job in jobs])).order_by(AttemptRow.id))).all())
            for node_id in sorted({attempt.node_id for attempt in locators}):
                await session.get(NodeRow, node_id, with_for_update=True)
            reservations, attempts = {}, {}
            for attempt_id in sorted(attempt.id for attempt in locators):
                reservations[attempt_id] = await session.scalar(select(ReservationRow).where(ReservationRow.attempt_id == attempt_id).with_for_update())
            for attempt_id in sorted(reservations):
                attempts[attempt_id] = await session.get(AttemptRow, attempt_id, with_for_update=True, populate_existing=True)
            parent = attempts.get(placement.active_attempt_id)
            now = await session.scalar(text("SELECT clock_timestamp()"))
            neutral = await session.scalar(select(TaskOperationReceiptRow.id).where(TaskOperationReceiptRow.agent_task_id == task.id, TaskOperationReceiptRow.state == "blocked").limit(1))
            reason = budget.blocked_reason
            if neutral is not None:
                reason = reason or "human_operation_recovery_required"
            if task.deadline <= now:
                uncertain_children = any(job.state in {"unknown", "quarantined"} for job in jobs) or any(
                    attempt.job_id is not None and (attempt.state in {"unknown", "quarantined"} or attempt.start_authorized_at is not None and attempt.stopped_at is None) for attempt in attempts.values()
                )
                reason = reason or ("task_deadline_unknown_child" if uncertain_children else "task_deadline_elapsed")
            if parent is None:
                if reason is not None:
                    budget.blocked_reason = reason
                return
            point = await accepted_final(session, task=task, run=run, placement=placement, attempt=parent)
            stopped = parent.stopped_at is not None and parent.finished_at is not None and reservations[parent.id] is not None and reservations[parent.id].state == "released" and reservations[parent.id].released_at is not None
            if parent.state in {"unknown", "quarantined"}:
                reason = reason or "parent_execution_unknown"
            if stopped and point is None:
                reason = reason or "parent_stopped_without_accepted_pair"
            if reason is None:
                return
            budget.blocked_reason = budget.blocked_reason or reason
            # An assigned publication is never changed before original STOP.
            # Even settled children cannot substitute for the parent's pair.
            if stopped and (point is None or neutral is not None or reason in {"task_deadline_unknown_child", "legacy_usage_unknown"}):
                task.state = "recovery_required"
            # A valid ORIGINAL paused boundary is already manual. Do not turn
            # a historical final point into a synthetic paused checkpoint.
