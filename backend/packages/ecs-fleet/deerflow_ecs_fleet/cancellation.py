"""Apply cancellation only when durable evidence already proves physical stop."""

from .persistence.models import AttemptRow, NodeRow


async def cancel_stopped_attempt(session, job, now):
    if job.active_attempt_id is None:
        return False
    locator = await session.get(AttemptRow, job.active_attempt_id)
    if locator is None or locator.job_id != job.id or locator.kind != "job":
        raise ValueError("Active job attempt unavailable")
    # The caller holds the job lock. Preserve execution -> node -> attempt order.
    await session.get(NodeRow, locator.node_id, with_for_update=True)
    attempt = await session.get(AttemptRow, locator.id, with_for_update=True, populate_existing=True)
    if attempt.stopped_at is None:
        return False
    attempt.state = job.state = "cancelled"
    attempt.finished_at = job.finished_at = now
    return True
