"""Bounded owner-scoped projection; private Fleet records never cross HTTP."""

from deerflow_ecs_fleet.persistence.agent_tasks import AgentTasks
from deerflow_ecs_fleet.persistence.models import AgentTaskRow, ArtifactManifestRow, AttemptRow, JobLinkRow, JobRow, ReservationRow, RunPlacementRow, SchedulerTicketRow, TaskBudgetRow, TaskOperationReceiptRow
from sqlalchemy import and_, exists, or_, select

from deerflow.persistence.run.model import RunRow

TERMINAL_TASKS = ("succeeded", "failed", "cancelled", "timed_out")


class FleetTaskSummaries:
    def __init__(self, session_factory):
        self.sf = session_factory

    async def read(self, *, user_id, thread_id, task_id=None, limit=50, offset=0):
        task, placement, run, attempt = AgentTaskRow, RunPlacementRow, RunRow, AttemptRow
        # Cover every physical reservation for this original run, including a
        # scheduler ticket whose reservation has not yet transferred to Attempt.
        held = exists(
            select(ReservationRow.id).where(
                ReservationRow.state != "released",
                or_(
                    ReservationRow.attempt_id.in_(select(AttemptRow.id).where(AttemptRow.kind == "agent", AttemptRow.run_id == task.current_run_id).correlate(task)),
                    ReservationRow.ticket_id.in_(
                        select(SchedulerTicketRow.id).where(SchedulerTicketRow.run_id == task.current_run_id, SchedulerTicketRow.user_id == task.user_id, SchedulerTicketRow.thread_id == task.thread_id).correlate(task)
                    ),
                ),
            )
        )
        historical = exists(select(AttemptRow.id).where(AttemptRow.kind == "agent", AttemptRow.run_id == task.current_run_id).correlate(task))
        unsettled = or_(task.state.not_in(TERMINAL_TASKS), held)
        # A completed owned cancellation advances the goal, retaining its exact
        # original source run. This read-only receipt is not admission authority.
        cancelled_source = exists(
            select(TaskOperationReceiptRow.id)
            .where(
                task.state == "cancelled",
                TaskOperationReceiptRow.agent_task_id == task.id,
                TaskOperationReceiptRow.user_id == task.user_id,
                TaskOperationReceiptRow.thread_id == task.thread_id,
                TaskOperationReceiptRow.operation == "cancel",
                TaskOperationReceiptRow.state == "completed",
                TaskOperationReceiptRow.source_run_id == task.current_run_id,
                TaskOperationReceiptRow.source_generation == placement.generation,
                TaskOperationReceiptRow.target_generation == task.generation,
                TaskOperationReceiptRow.target_generation == TaskOperationReceiptRow.source_generation + 1,
            )
            .correlate(task, placement)
        )
        query = (
            select(
                task.id,
                task.state,
                task.current_run_id,
                task.generation,
                run.status.label("run_status"),
                placement.profile,
                task.cancel_requested_at.label("task_cancel"),
                run.cancel_requested_at.label("run_cancel"),
                placement.state.label("placement_state"),
                placement.active_attempt_id,
                attempt.stopped_at,
                attempt.id.label("attempt_id"),
                held.label("resources_held"),
                historical.label("historical"),
                TaskBudgetRow,
            )
            .select_from(task)
            .join(
                placement,
                and_(
                    placement.run_id == task.current_run_id,
                    placement.agent_task_id == task.id,
                    or_(placement.generation == task.generation, cancelled_source),
                    placement.user_id == task.user_id,
                    placement.thread_id == task.thread_id,
                ),
            )
            .join(run, and_(run.run_id == placement.run_id, run.user_id == task.user_id, run.thread_id == task.thread_id))
            .outerjoin(attempt, and_(attempt.id == placement.active_attempt_id, attempt.kind == "agent", attempt.run_id == run.run_id))
            .outerjoin(TaskBudgetRow, TaskBudgetRow.agent_task_id == task.id)
            .where(task.user_id == user_id, task.thread_id == thread_id)
        )
        if task_id is not None:
            query = query.where(task.id == task_id)
        query = query.order_by(unsettled.desc(), task.updated_at.desc(), task.id).limit(min(max(limit, 1), 100)).offset(min(max(offset, 0), 10000))
        async with self.sf() as session:
            rows = (await session.execute(query)).mappings().all()
            summaries = []
            for row in rows:
                related = await self.related(session, row, user_id=user_id, thread_id=thread_id)
                summaries.append({**self.public(row), **related})
        return summaries

    @staticmethod
    async def related(session, row, *, user_id, thread_id):
        link, job, placement, run, attempt, manifest = JobLinkRow, JobRow, RunPlacementRow, RunRow, AttemptRow, ArtifactManifestRow
        jobs = (
            select(link.job_id, link.generation, link.parent_run_id, link.link_mode, job.state, manifest.id.label("accepted_manifest_id"))
            .select_from(link)
            .join(job, and_(job.id == link.job_id, job.user_id == link.user_id, job.thread_id == link.thread_id, job.source_run_id == link.parent_run_id))
            .join(
                placement,
                and_(placement.run_id == link.parent_run_id, placement.agent_task_id == link.agent_task_id, placement.generation == link.generation, placement.user_id == link.user_id, placement.thread_id == link.thread_id),
            )
            .join(run, and_(run.run_id == placement.run_id, run.user_id == placement.user_id, run.thread_id == placement.thread_id))
            .outerjoin(attempt, and_(attempt.id == job.active_attempt_id, attempt.kind == "job", attempt.job_id == job.id, attempt.state == "succeeded", attempt.stopped_at.is_not(None), attempt.finished_at.is_not(None)))
            .outerjoin(
                manifest,
                and_(manifest.id == job.accepted_manifest_id, manifest.attempt_id == attempt.id, manifest.user_id == job.user_id, manifest.thread_id == job.thread_id, job.state == "succeeded"),
            )
            .where(link.agent_task_id == row["id"], link.user_id == user_id, link.thread_id == thread_id, link.generation <= row["generation"])
            .order_by(link.generation.desc(), link.created_at.desc(), link.job_id)
            .limit(21)
        )
        runs = (
            select(run.run_id, placement.generation, run.status.label("run_status"))
            .select_from(run)
            .join(placement, and_(placement.run_id == run.run_id, placement.user_id == run.user_id, placement.thread_id == run.thread_id))
            .where(placement.agent_task_id == row["id"], placement.user_id == user_id, placement.thread_id == thread_id, placement.generation <= row["generation"])
            .order_by((run.run_id == row["current_run_id"]).desc(), placement.generation.desc(), run.created_at.desc(), run.run_id)
            .limit(21)
        )
        job_rows = (await session.execute(jobs)).mappings().all()
        run_rows = (await session.execute(runs)).mappings().all()
        return {
            "jobs": [dict(item) for item in job_rows[:20]],
            "runs": [dict(item) for item in run_rows[:20]],
            "jobs_truncated": len(job_rows) > 20,
            "runs_truncated": len(run_rows) > 20,
        }

    @staticmethod
    def public(row):
        never_assigned = row["active_attempt_id"] is None and not row["historical"]
        stop = "not_started" if never_assigned else "confirmed" if row["stopped_at"] is not None else "unconfirmed"
        # A current Attempt STOP cannot confirm release of another reservation
        # still held by this original run (including scheduler-ticket capacity).
        if row["resources_held"] and (row["state"] in TERMINAL_TASKS or row["run_status"] in {"success", "error", "interrupted", "timeout"}):
            stop = "unconfirmed"
        budget = row.get("TaskBudgetRow")
        budget_projection = AgentTasks.public_budget(budget, state=row["state"])
        return {
            **budget_projection,
            "execution_uncertain": budget_projection["execution_uncertain"] or row["placement_state"] in {"unknown", "recovery_required"} or (not never_assigned and row["attempt_id"] is None),
            "task_id": row["id"],
            "state": row["state"],
            "current_run_id": row["current_run_id"],
            "generation": row["generation"],
            "run_status": row["run_status"],
            "profile": row["profile"],
            "location": "queued" if never_assigned else "remote",
            "cancel_requested": row["task_cancel"] is not None or row["run_cancel"] is not None,
            "recovery_required": budget is None
            or budget.legacy_usage_unknown
            or row["state"] in {"unknown", "recovery_required"}
            or row["placement_state"] in {"unknown", "recovery_required"}
            or (not never_assigned and row["attempt_id"] is None),
            "stop_state": stop,
            "resources_held": bool(row["resources_held"]),
        }
