"""FIFO job claims atomically consume the shared Fleet resource ledger."""

import hashlib
import secrets
from dataclasses import dataclass, field
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import func, select

from .persistence.inputs import resolve_inputs
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
        self.agent_candidates = None

    async def claim_job(self, node_id: str, *, node_session_id: str) -> JobClaim | None:
        if not self.config.jobs_enabled:
            return None
        from .admission_policy import queued_keys

        query = select(JobRow.id, JobRow.queued_at, JobRow.id).where(JobRow.state == "queued", JobRow.cancel_requested_at.is_(None), JobRow.queue_deadline > func.clock_timestamp())
        async for job_id, _, _ in queued_keys(self.sf, query, (JobRow.queued_at, JobRow.id)):
            async with self.sf.begin() as session:
                job = await session.get(JobRow, job_id, with_for_update={"skip_locked": True})
                if job is None or job.state != "queued" or job.cancel_requested_at is not None:
                    continue
                window = None
                if self.config.continuations_enabled:
                    from .admission_policy import SharedAdmissionPolicy

                    window = await SharedAdmissionPolicy(self.config, agent_candidates=self.agent_candidates).lock(session)
                    node = next((row for row in window.nodes if row.id == node_id), None)
                else:
                    node = await session.get(NodeRow, node_id, with_for_update=True)
                if node is None or node.session_id != node_session_id:
                    raise ValueError("Stale node session")
                now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
                if node.admin_state != "enabled" or node.health != "online" or node.last_seen_at is None or node.last_seen_at + timedelta(seconds=self.config.lease_seconds) <= now:
                    return None
                if job.queue_deadline <= now:
                    continue
                spec = JobSpec.model_validate(job.spec)
                profile = self.config.profiles.get(spec.profile)
                if profile is None or profile.kind != "job" or (node.profile_allowlist is not None and spec.profile not in node.profile_allowlist):
                    continue
                try:
                    inputs = await resolve_inputs(session, user_id=job.user_id, thread_id=job.thread_id, spec=spec, max_bytes=self.config.max_input_bytes)
                except (PermissionError, ValueError):
                    job.state = "failed"
                    job.error = "Pinned input unavailable or exceeds operator budget"
                    job.finished_at = now
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
                    launch_spec={"schema_version": 1, "spec": spec.model_dump(), "profile": profile.job_wire(), "user_id": job.user_id, "thread_id": job.thread_id, "inputs": inputs},
                    output_prefix=prefix,
                    lease_expires_at=now + timedelta(seconds=self.config.lease_seconds),
                )
                if window is not None and not await window.permits("job", job.id, node, profile):
                    continue
                if not await reserve(session, node, attempt, cpu_millis=profile.cpu_millis, memory_mib=profile.memory_mib):
                    continue
                if window is not None:
                    window.reserved("job")
                job.state = "claimed"
                job.active_attempt_id = attempt_id
                job.updated_at = now
                return JobClaim(job.id, attempt_id, token, spec.model_dump(), self.config.lease_seconds, prefix)
        return None
