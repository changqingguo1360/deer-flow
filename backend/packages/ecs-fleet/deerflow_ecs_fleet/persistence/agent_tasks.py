"""Private durable goals; participate in the caller's run-admission transaction."""

import re
from datetime import UTC, datetime

from sqlalchemy import select

from ..config import NAME_PATTERN
from .models import AgentTaskRow


def identity(value):
    if not isinstance(value, str) or re.fullmatch(NAME_PATTERN, value) is None:
        raise ValueError("Invalid Agent execution identity")


def utc_deadline(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset().total_seconds() != 0:
        raise ValueError("Agent deadline must be UTC")
    return value.astimezone(UTC)


class AgentTasks:
    async def create(self, session, *, task_id, user_id, thread_id, deadline, continuation_budget):
        for value in (task_id, user_id, thread_id):
            identity(value)
        deadline = utc_deadline(deadline)
        if type(continuation_budget) is not int or continuation_budget < 0:
            raise ValueError("Invalid continuation budget")
        existing = await session.get(AgentTaskRow, task_id, with_for_update=True)
        if existing is not None:
            if (existing.user_id, existing.thread_id, existing.deadline, existing.continuation_budget) != (user_id, thread_id, deadline, continuation_budget):
                raise ValueError("Agent task admission identity conflicts")
            return existing
        task = AgentTaskRow(id=task_id, user_id=user_id, thread_id=thread_id, deadline=deadline, continuation_budget=continuation_budget)
        session.add(task)
        await session.flush()
        return task

    async def owned(self, session, *, task_id, user_id, thread_id, lock=False):
        query = select(AgentTaskRow).where(AgentTaskRow.id == task_id, AgentTaskRow.user_id == user_id, AgentTaskRow.thread_id == thread_id)
        if lock:
            query = query.with_for_update()
        task = (await session.execute(query)).scalar_one_or_none()
        if task is None:
            raise LookupError("Agent task not found")
        return task

    async def public_summary(self, session, *, task_id, user_id, thread_id):
        task = await self.owned(session, task_id=task_id, user_id=user_id, thread_id=thread_id)
        return {
            "task_id": task.id,
            "state": task.state,
            "current_run_id": task.current_run_id,
            "generation": task.generation,
            "cancel_requested": task.cancel_requested_at is not None,
            "recovery_required": task.state in {"unknown", "recovery_required"},
        }
