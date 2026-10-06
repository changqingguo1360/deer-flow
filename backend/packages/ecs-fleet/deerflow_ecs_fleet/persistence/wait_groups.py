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
