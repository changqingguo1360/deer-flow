"""FIFO job claims atomically consume the shared Fleet resource ledger."""

import hashlib
import secrets
from dataclasses import dataclass, field
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import func, select

from .persistence.models import AttemptRow, JobRow, NodeRow
from .persistence.reservations import reserve
from .protocol import JobSpec


@dataclass(frozen=True)
class JobClaim:
    job_id: str
    attempt_id: str
    token: str = field(repr=False)
    spec: dict
    lease_seconds: int
    output_prefix: str


class FleetScheduler:
    def __init__(self, session_factory, config):
        self.sf = session_factory
        self.config = config

    async def claim_job(self, node_id: str, *, node_session_id: str) -> JobClaim | None:
        if not self.config.jobs_enabled:
            return None
        async with self.sf.begin() as session:
            # execution -> node -> reservation. SKIP LOCKED permits independent
            # nodes to progress without claiming the same queued execution.
            jobs = (
                (
                    await session.execute(
                        select(JobRow)
                        .where(
                            JobRow.state == "queued",
                            JobRow.cancel_requested_at.is_(None),
                            JobRow.queue_deadline > func.clock_timestamp(),
                        )
                        .order_by(JobRow.queued_at, JobRow.id)
                        .limit(64)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            node = await session.get(NodeRow, node_id, with_for_update=True)
            if node is None or node.session_id != node_session_id:
                raise ValueError("Stale node session")
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            if node.admin_state != "enabled" or node.health != "online" or node.last_seen_at is None or node.last_seen_at + timedelta(seconds=self.config.lease_seconds) <= now:
                return None
            for job in jobs:
                spec = JobSpec.model_validate(job.spec)
                profile = self.config.profiles.get(spec.profile)
                if profile is None or profile.kind != "job":
                    continue
                number = (await session.execute(select(func.coalesce(func.max(AttemptRow.attempt_no), 0)).where(AttemptRow.job_id == job.id))).scalar_one() + 1
                attempt_id = str(uuid4())
                token = secrets.token_urlsafe(48)
                prefix = f"{job.user_id}/{job.thread_id}/jobs/{job.id}/attempts/{attempt_id}"
                attempt = AttemptRow(
                    id=attempt_id,
                    kind="job",
                    job_id=job.id,
                    attempt_no=number,
                    node_id=node.id,
                    node_session_id=node.session_id,
                    token_hash=hashlib.sha256(token.encode()).hexdigest(),
                    state="claimed",
                    output_prefix=prefix,
                    lease_expires_at=now + timedelta(seconds=self.config.lease_seconds),
                )
                if not await reserve(session, node, attempt, cpu_millis=profile.cpu_millis, memory_mib=profile.memory_mib):
                    continue
                job.state = "claimed"
                job.active_attempt_id = attempt_id
                job.updated_at = now
                return JobClaim(job.id, attempt_id, token, spec.model_dump(), self.config.lease_seconds, prefix)
            return None
