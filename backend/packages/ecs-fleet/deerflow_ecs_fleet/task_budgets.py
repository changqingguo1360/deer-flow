"""Task-lifetime accounting. All callers retain the original task-first lock."""

from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from .persistence.models import AgentTaskRow, JobLinkRow, ModelReservationRow, RunPlacementRow, TaskBudgetChargeRow, TaskBudgetDecisionRow, TaskBudgetRow


class TaskBudgetExceeded(ValueError):
    pass


async def ledger(session, task_id):
    # Ledger serialization follows the original task lock, never reverses it.
    await session.get(AgentTaskRow, task_id, with_for_update=True)
    row = await session.get(TaskBudgetRow, task_id, with_for_update=True)
    if row is None:
        # No missing historical ledger can grant a fresh allowance. Reconstruct
        # only durable logical counters; historic token usage stays uncertain.
        runs = await session.scalar(select(func.count()).select_from(RunPlacementRow).where(RunPlacementRow.agent_task_id == task_id))
        jobs = await session.scalar(select(func.count()).select_from(JobLinkRow).where(JobLinkRow.agent_task_id == task_id))
        row = TaskBudgetRow(agent_task_id=task_id, run_limit=runs, job_limit=jobs, token_limit=0, admitted_runs=runs, submitted_jobs=jobs, spent_tokens=0, reserved_tokens=0, legacy_usage_unknown=True, blocked_reason="legacy_usage_unknown")
        session.add(row)
        await session.flush()
    return row


async def record_denial(session_factory, identity, reason, *, request_digest=None):
    async with session_factory.begin() as session:
        task = await session.get(AgentTaskRow, identity["agent_task_id"], with_for_update=True)
        if task is None or (task.user_id, task.thread_id) != (identity["user_id"], identity["thread_id"]):
            raise TaskBudgetExceeded("Original budget denial owner conflicts")
        row = await ledger(session, task.id)
        session.add(TaskBudgetDecisionRow(id="decision-" + uuid4().hex, **identity, request_digest=request_digest, reason=reason))
        # A lost generation race remains an immutable ORIGINAL decision, and
        # cannot relabel another generation's current publication/blocked state.
        if (task.generation, task.current_run_id) == (identity["generation"], identity["source_run_id"]):
            row.blocked_reason = row.blocked_reason or reason


async def deny_admission(session, identity, reason):
    """Rollback the denied original UOW before its reason-only transaction."""
    factory = async_sessionmaker(session.bind, expire_on_commit=False)
    await session.rollback()
    await record_denial(factory, identity, reason)
    raise TaskBudgetExceeded(reason)


async def charge(session, *, task, kind, logical_id, config, source_generation=None):
    await session.get(AgentTaskRow, task.id, with_for_update=True)
    row = await session.get(TaskBudgetRow, task.id, with_for_update=True)
    if row is None:
        if task.current_run_id is not None:
            row = await ledger(session, task.id)
        else:
            row = TaskBudgetRow(
                agent_task_id=task.id, run_limit=config.task_run_limit, job_limit=config.task_job_limit, token_limit=config.task_token_limit, admitted_runs=0, submitted_jobs=0, spent_tokens=0, reserved_tokens=0, legacy_usage_unknown=False
            )
            session.add(row)
            await session.flush()
    existing = await session.get(TaskBudgetChargeRow, (task.id, kind, logical_id))
    if existing is not None:
        if existing.generation != task.generation:
            raise TaskBudgetExceeded("Original task charge identity conflicts")
        return
    denial_identity = {"agent_task_id": task.id, "user_id": task.user_id, "thread_id": task.thread_id, "generation": source_generation if source_generation is not None else task.generation, "source_run_id": task.current_run_id}
    counter, limit = ("admitted_runs", "run_limit") if kind == "run" else ("submitted_jobs", "job_limit")
    if row.legacy_usage_unknown or row.blocked_reason is not None or getattr(row, counter) >= getattr(row, limit):
        await deny_admission(session, denial_identity, "legacy_usage_unknown" if row.legacy_usage_unknown else row.blocked_reason or "task_" + kind + "_budget_exhausted")
    setattr(row, counter, getattr(row, counter) + 1)
    session.add(TaskBudgetChargeRow(agent_task_id=task.id, kind=kind, logical_id=logical_id, generation=task.generation))
    await session.flush()


def ticket_identity(row):
    return {key: getattr(row, key) for key in ("agent_task_id", "generation", "run_id", "attempt_id", "node_id", "node_session_id", "token_stamp", "provider_contract", "request_digest", "input_bound", "output_bound")}


async def reserve(session, *, identity, bound):
    row = await ledger(session, identity["agent_task_id"])
    amount = bound["input_bound"] + bound["output_bound"]
    if row.legacy_usage_unknown or row.blocked_reason is not None or row.spent_tokens + row.reserved_tokens + amount > row.token_limit:
        row.blocked_reason = "legacy_usage_unknown" if row.legacy_usage_unknown else row.blocked_reason or "task_token_budget_exhausted"
        return None, row.blocked_reason
    ticket = ModelReservationRow(
        id="call-" + uuid4().hex,
        **identity,
        provider_contract=bound["provider_contract"],
        request_digest=bound["request_digest"],
        input_bound=bound["input_bound"],
        output_bound=bound["output_bound"],
        reserved_tokens=amount,
        state="reserved",
    )
    session.add(ticket)
    row.reserved_tokens += amount
    await session.flush()
    return {"id": ticket.id, **ticket_identity(ticket)}, None


async def original_ticket(session, ticket):
    row = await ledger(session, ticket["agent_task_id"])
    receipt = await session.get(ModelReservationRow, ticket["id"], with_for_update=True)
    if receipt is None or ticket_identity(receipt) != {key: ticket[key] for key in ticket_identity(receipt)}:
        raise TaskBudgetExceeded("Original model reservation identity conflicts")
    return row, receipt


async def settle(session, *, ticket, usage):
    row, receipt = await original_ticket(session, ticket)
    if receipt.state == "settled":
        if receipt.measured_usage != usage:
            raise TaskBudgetExceeded("Immutable model usage receipt conflicts")
        return
    if receipt.state != "reserved":
        raise TaskBudgetExceeded("Uncertain model reservation cannot be refunded")
    if (
        not isinstance(usage, dict)
        or set(usage) != {"input_tokens", "output_tokens", "total_tokens"}
        or any(type(value) is not int or value < 0 for value in usage.values())
        or usage["input_tokens"] + usage["output_tokens"] != usage["total_tokens"]
        or usage["input_tokens"] > receipt.input_bound
        or usage["output_tokens"] > receipt.output_bound
    ):
        raise TaskBudgetExceeded("Missing or invalid authoritative model usage")
    row.reserved_tokens -= receipt.reserved_tokens
    row.spent_tokens += usage["total_tokens"]
    receipt.state, receipt.measured_usage = "settled", dict(usage)
    await session.flush()


async def unknown(session, *, ticket, reason):
    row, receipt = await original_ticket(session, ticket)
    if receipt.state == "settled":
        return
    receipt.state = "unknown"
    receipt.unknown_reason = receipt.unknown_reason or reason
    row.blocked_reason = row.blocked_reason or "model_usage_unknown"
    # Reservation and spent tokens are deliberately untouched.
