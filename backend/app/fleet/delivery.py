"""Exclusive delivery authority derived from the immutable accepted child link."""

from sqlalchemy import exists, select

from deerflow.persistence.mcp_tasks.model import McpTaskRow


class FleetDeliveryPolicy:
    @staticmethod
    def predicate(tracking):
        from deerflow_ecs_fleet.persistence.models import JobLinkRow, JobRow

        # No Fleet locks: the link is immutable and committed before tracking
        # can be claimed. B takes job -> tracking locks, never the reverse.
        return ~exists(
            select(JobRow.id)
            .join(JobLinkRow, JobLinkRow.job_id == JobRow.id)
            .where(
                JobRow.tracking_task_id == tracking.id,
                JobRow.user_id == tracking.user_id,
                JobRow.thread_id == tracking.thread_id,
                JobLinkRow.user_id == tracking.user_id,
                JobLinkRow.thread_id == tracking.thread_id,
                JobLinkRow.link_mode == "awaited",
            )
        )

    async def permits(self, session, *, task_id, user_id, thread_id):
        return bool(await session.scalar(select(McpTaskRow.id).where(McpTaskRow.id == task_id, McpTaskRow.user_id == user_id, McpTaskRow.thread_id == thread_id, self.predicate(McpTaskRow))))
