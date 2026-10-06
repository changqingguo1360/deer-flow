"""Host injected identity; no graph or request field can supply parent authority."""

from dataclasses import dataclass

from sqlalchemy import func, select

from ..continuation_payload import MAX_CHILDREN
from .models import JobLinkRow


@dataclass(frozen=True)
class ParentJobOwner:
    agent_task_id: str
    generation: int
    parent_run_id: str
    user_id: str
    thread_id: str


class JobLinks:
    # The immutable link_mode is the durable delivery authority:
    # awaited belongs to the wait group; detached uses generic notification.
    async def check_capacity(self, session, *, owner):
        # Caller retains the original parent task lock throughout admission.
        count = await session.scalar(
            select(func.count())
            .select_from(JobLinkRow)
            .where(
                *(getattr(JobLinkRow, name) == getattr(owner, name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")),
                JobLinkRow.link_mode == "awaited",
            )
        )
        if count >= MAX_CHILDREN:
            raise ValueError("Awaited child limit reached for original parent execution")

    async def attach(self, session, *, owner, job, link_mode):
        if (job.user_id, job.thread_id, job.source_run_id) != (owner.user_id, owner.thread_id, owner.parent_run_id):
            raise PermissionError("Child job belongs to another execution")
        expected = dict(agent_task_id=owner.agent_task_id, generation=owner.generation, parent_run_id=owner.parent_run_id, user_id=owner.user_id, thread_id=owner.thread_id, link_mode=link_mode)
        existing = await session.get(JobLinkRow, job.id)
        if existing is not None:
            if any(getattr(existing, key) != value for key, value in expected.items()):
                raise PermissionError("Child link ownership is immutable")
            return existing
        if link_mode == "awaited":
            await self.check_capacity(session, owner=owner)
        link = JobLinkRow(job_id=job.id, **expected)
        session.add(link)
        return link
