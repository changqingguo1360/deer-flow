"""Original-run scheduler association and terminal field validation.

Called inside each actual completion writer transaction after execution locks,
then parent -> occurrence target locks. Metadata alone never grants authority.
"""

from types import SimpleNamespace
from typing import TypedDict

from deerflow.persistence.run.model import RunRow
from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow
from deerflow.runtime.execution.mutation_context import OwnershipRejected, validate_mutation


class AggregateCompletionOutcome(TypedDict):
    status: str
    last_error: str | None


async def aggregate_parent_outcome(callback, session, *, task_id, occurrence_id, run_id) -> AggregateCompletionOutcome | None:
    if callback is None:
        return None
    outcome = await callback(session, task_id=task_id, occurrence_id=occurrence_id, run_id=run_id)
    if outcome is not None and (
        not isinstance(outcome, dict)
        or set(outcome) != {"status", "last_error"}
        or not isinstance(outcome["status"], str)
        or outcome["status"] not in {"completed", "failed", "cancelled"}
        or outcome["last_error"] is not None
        and not isinstance(outcome["last_error"], str)
    ):
        raise OwnershipRejected("Scheduled aggregate completion outcome rejected")
    return outcome


async def lock_completion(session, capability, *, operation, task_id, occurrence_id, run_id, user_id=None, aggregate_completion_outcome=None, trusted=False):
    if capability is None and not trusted:
        return None
    if capability is not None:
        await validate_mutation(capability, session, operation, run_id=run_id, user_id=user_id)
        context = capability.context
        core = await session.get(RunRow, context.run_id)
    else:
        # Internal trusted host completion only; caller must still provide the
        # genuine original identity and owner, never client-writable metadata.
        core = await session.get(RunRow, run_id)
        if core is None or core.user_id != user_id:
            raise OwnershipRejected("Scheduled completion original owner rejected")
        context = SimpleNamespace(run_id=run_id, user_id=user_id, thread_id=core.thread_id)
    metadata = core.metadata_json or {}
    if run_id != context.run_id or task_id != metadata.get("scheduled_task_id") or occurrence_id != metadata.get("scheduled_task_run_id") or core.status not in {"success", "error", "timeout", "interrupted"}:
        raise OwnershipRejected("Scheduled completion original run association rejected")
    task = await session.get(ScheduledTaskRow, task_id, with_for_update=True)
    occurrence = await session.get(ScheduledTaskRunRow, occurrence_id, with_for_update=True)
    if (
        task is None
        or occurrence is None
        or task.user_id != context.user_id
        or task.last_run_id != context.run_id
        or task.last_thread_id != context.thread_id
        or occurrence.task_id != task.id
        or occurrence.run_id != context.run_id
        or occurrence.thread_id != context.thread_id
    ):
        raise OwnershipRejected("Scheduled completion persisted target association rejected")
    if capability is not None:
        await validate_mutation(capability, session, operation, run_id=run_id, user_id=user_id)
    status = "success" if core.status == "success" else "interrupted" if core.status == "interrupted" else "failed"
    error = None if core.status == "success" else core.error or ("run was interrupted before completion" if core.status == "interrupted" else None)
    if task.schedule_type == "once" and task.status in {"completed", "failed", "cancelled"}:
        outcome = await aggregate_parent_outcome(aggregate_completion_outcome, session, task_id=task_id, occurrence_id=occurrence_id, run_id=run_id)
        expected_parent = outcome["status"] if outcome is not None else {"success": "completed", "interrupted": "cancelled", "failed": "failed"}[status]
        expected_error = outcome["last_error"] if outcome is not None else error
        if task.status != expected_parent or task.last_error != expected_error:
            raise OwnershipRejected("Scheduled completion cannot change terminal parent")
    if occurrence.status not in {"running", status} or (occurrence.status == status and occurrence.error != error):
        raise OwnershipRejected("Scheduled completion cannot change terminal occurrence")
    return task, occurrence, status, error
