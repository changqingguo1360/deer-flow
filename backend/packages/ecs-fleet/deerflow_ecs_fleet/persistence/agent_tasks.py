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

    async def consume_continuation(self, session, *, task, group, run_id):
        """Caller holds original task/group fence on the run admission TX."""
        if (
            task.state != "waiting_jobs"
            or task.current_run_id != group.parent_run_id
            or task.generation != group.generation
            or task.wait_group_id != group.id
            or group.state != "waiting_jobs"
            or group.continuation_run_id is not None
            or task.continuation_budget <= 0
        ):
            raise ValueError("Original continuation admission changed")
        task.continuation_budget -= 1
        task.state = "queued"
        task.wait_group_id = None

    async def public_summary(self, session, *, task_id, user_id, thread_id):
        task = await self.owned(session, task_id=task_id, user_id=user_id, thread_id=thread_id)
        from .models import TaskBudgetRow

        budget = await session.get(TaskBudgetRow, task.id)
        return {
            **self.public_budget(budget, state=task.state),
            "task_id": task.id,
            "state": task.state,
            "current_run_id": task.current_run_id,
            "generation": task.generation,
            "cancel_requested": task.cancel_requested_at is not None,
            "recovery_required": task.state in {"unknown", "recovery_required"} or budget is None or budget.legacy_usage_unknown,
        }

    @staticmethod
    def public_budget(budget, *, state):
        """The same safe projection serves both original owner-scoped readers."""
        return {
            "budget": None
            if budget is None
            else {key: getattr(budget, key) for key in ("run_limit", "job_limit", "token_limit", "admitted_runs", "submitted_jobs", "spent_tokens", "reserved_tokens", "legacy_usage_unknown", "blocked_reason")},
            "blocked_reason": "task_budget_unavailable" if budget is None else budget.blocked_reason,
            "execution_uncertain": state in {"unknown", "recovery_required"}
            or budget is None
            or budget.legacy_usage_unknown
            or budget.reserved_tokens > 0
            or budget.blocked_reason in {"model_usage_unknown", "legacy_usage_unknown", "parent_execution_unknown", "parent_stopped_without_accepted_pair", "task_deadline_unknown_child", "human_operation_recovery_required"},
        }
