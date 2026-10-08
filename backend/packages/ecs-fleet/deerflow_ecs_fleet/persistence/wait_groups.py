"""Seal once under the original task-first transaction fence."""

from uuid import uuid4

from sqlalchemy import select

from .models import JobLinkRow, JobRow, WaitGroupRow


class WaitGroups:
    def __init__(self, session_factory, *, parent_capability):
        self.sf = session_factory
        self.parent_capability = parent_capability

    @staticmethod
    def summary(group):
        return {name: getattr(group, name) for name in ("id", "continuation_key", "agent_task_id", "generation", "parent_run_id", "user_id", "thread_id", "job_ids", "policy")}

    async def seal(self, *, continuation_key, job_ids):
        if not isinstance(continuation_key, str) or not continuation_key.strip() or len(continuation_key) > 128:
            raise ValueError("Invalid continuation key")
        owner = self.parent_capability.owner
        async with self.sf.begin() as session:
            # Same task -> run -> placement -> physical owner order as submission.
            # Future coordinators must retain task-first order before group/jobs.
            await self.parent_capability.validate(session, user_id=owner.user_id, thread_id=owner.thread_id, source_run_id=owner.parent_run_id)
            existing = (await session.execute(select(WaitGroupRow).where(WaitGroupRow.continuation_key == continuation_key).with_for_update())).scalar_one_or_none()
            if existing is not None:
                if any(getattr(existing, name) != getattr(owner, name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")):
                    raise PermissionError("Continuation key belongs to another execution")
                await self.parent_capability.validate(session, user_id=owner.user_id, thread_id=owner.thread_id, source_run_id=owner.parent_run_id)
                return self.summary(existing)
            if not isinstance(job_ids, list) or not job_ids or any(not isinstance(value, str) or not value for value in job_ids):
                raise ValueError("Wait group requires child jobs")
            members = sorted(set(job_ids))
            for job_id in members:
                job = await session.get(JobRow, job_id, with_for_update=True)
                link = await session.get(JobLinkRow, job_id)
                if job is None or link is None or link.link_mode != "awaited" or any(getattr(link, name) != getattr(owner, name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")):
                    raise PermissionError("Wait group requires original owned awaited jobs")
            group = WaitGroupRow(id=str(uuid4()), continuation_key=continuation_key, job_ids=members, policy="all_settled", **{name: getattr(owner, name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")})
            session.add(group)
            await session.flush()
            await self.parent_capability.validate(session, user_id=owner.user_id, thread_id=owner.thread_id, source_run_id=owner.parent_run_id)
            return self.summary(group)

    async def prepare_remaining(self):
        import hashlib

        owner = self.parent_capability.owner
        async with self.sf.begin() as session:
            await self.parent_capability.validate(session, user_id=owner.user_id, thread_id=owner.thread_id, source_run_id=owner.parent_run_id)
            key = "await-" + hashlib.sha256(owner.parent_run_id.encode()).hexdigest()
            existing = await session.scalar(select(WaitGroupRow).where(WaitGroupRow.continuation_key == key).with_for_update())
            if existing is not None:
                if any(getattr(existing, name) != getattr(owner, name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")):
                    raise PermissionError("Original wait group owner conflicts")
                await self.parent_capability.validate(session, user_id=owner.user_id, thread_id=owner.thread_id, source_run_id=owner.parent_run_id)
                return self.summary(existing)
            members = list(
                (
                    await session.scalars(
                        select(JobLinkRow.job_id)
                        .where(
                            JobLinkRow.agent_task_id == owner.agent_task_id,
                            JobLinkRow.generation == owner.generation,
                            JobLinkRow.parent_run_id == owner.parent_run_id,
                            JobLinkRow.user_id == owner.user_id,
                            JobLinkRow.thread_id == owner.thread_id,
                            JobLinkRow.link_mode == "awaited",
                        )
                        .order_by(JobLinkRow.job_id)
                    )
                ).all()
            )
            if not members:
                return None
            for job_id in members:
                await session.get(JobRow, job_id, with_for_update=True)
            row = WaitGroupRow(
                id=str(uuid4()), continuation_key=key, job_ids=members, policy="all_settled", state="preparing", **{name: getattr(owner, name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")}
            )
            session.add(row)
            await session.flush()
            await self.parent_capability.validate(session, user_id=owner.user_id, thread_id=owner.thread_id, source_run_id=owner.parent_run_id)
            return self.summary(row)

    async def readiness(self, group_id):
        """Read exact original publication/STOP readiness; never admit a run."""
        from deerflow.persistence.run.model import RunRow

        from .models import AgentTaskRow, AttemptRow, ReservationRow, RunPlacementRow
        from .workspace_points import accepted_final

        async with self.sf() as session:
            group = await session.get(WaitGroupRow, group_id)
            if group is None or group.state != "waiting_jobs":
                return False
            task = await session.get(AgentTaskRow, group.agent_task_id)
            placement = await session.get(RunPlacementRow, group.parent_run_id)
            run = await session.get(RunRow, group.parent_run_id)
            if task is None or placement is None or run is None or placement.active_attempt_id is None or task.cancel_requested_at is not None or run.cancel_action is not None:
                return False
            attempt = await session.get(AttemptRow, placement.active_attempt_id)
            reservation = await session.scalar(select(ReservationRow).where(ReservationRow.attempt_id == placement.active_attempt_id))
            if attempt is None or reservation is None or attempt.stopped_at is None or reservation.state != "released" or reservation.released_at is None:
                return False
            point = await accepted_final(session, task=task, run=run, placement=placement, attempt=attempt)
            if point is None or (group.checkpoint_id, group.workspace_point_id, task.wait_group_id, task.state, placement.state, attempt.state, run.status) != (
                point.checkpoint_id,
                point.id,
                group.id,
                "waiting_jobs",
                "succeeded",
                "succeeded",
                "success",
            ):
                return False
            if any(getattr(group, name) != getattr(point, "run_id" if name == "parent_run_id" else name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")) or point.desired_task_status != "waiting_jobs":
                return False
            jobs = list((await session.scalars(select(JobRow).where(JobRow.id.in_(group.job_ids)))).all())
            return len(jobs) == len(group.job_ids) and all(job.state in {"succeeded", "failed", "cancelled"} and (job.user_id, job.thread_id, job.source_run_id) == (group.user_id, group.thread_id, group.parent_run_id) for job in jobs)
