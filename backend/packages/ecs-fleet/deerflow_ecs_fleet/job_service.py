"""Durable staged jobs activate only after their matching tracking row commits."""

import re
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from .cancellation import cancel_stopped_attempt
from .config import NAME_PATTERN
from .persistence.inputs import resolve_inputs
from .persistence.models import JobRow
from .protocol import JobSpec


class FleetJobService:
    def __init__(self, session_factory, config, *, tracking_reader):
        self.sf = session_factory
        self.config = config
        self.tracking_reader = tracking_reader

    @staticmethod
    def summary(job):
        return {"id": job.id, "user_id": job.user_id, "thread_id": job.thread_id, "tracking_task_id": job.tracking_task_id, "state": job.state, "accepted_manifest_id": job.accepted_manifest_id, "error": job.error}

    async def submit(self, *, user_id: str, thread_id: str, source_run_id: str | None, tracking_task_id: str, idempotency_key: str, spec: JobSpec, dedupe_group: str | None = None) -> dict:
        if not self.config.jobs_enabled:
            raise ValueError("Fleet jobs disabled")
        spec = JobSpec.model_validate(spec.model_dump())
        if spec.link_mode == "awaited" and not self.config.continuations_enabled:
            raise ValueError("awaited jobs require continuations")
        for value in (user_id, thread_id, tracking_task_id):
            if not re.fullmatch(NAME_PATTERN, value):
                raise ValueError("Invalid server identity")
        if not idempotency_key.strip() or len(idempotency_key) > 128:
            raise ValueError("Invalid submission key")
        if dedupe_group is not None and (not dedupe_group.strip() or len(dedupe_group) > 128):
            raise ValueError("Invalid dedupe group")
        profile = self.config.profiles.get(spec.profile)
        if profile is None or profile.kind != "job":
            raise ValueError("Unknown job profile")
        if spec.execution_timeout_seconds > profile.execution_timeout_seconds or spec.queue_timeout_seconds > self.config.queue_timeout_seconds:
            raise ValueError("Job exceeds operator time budget")
        payload = spec.model_dump()
        async with self.sf.begin() as session:
            await resolve_inputs(session, user_id=user_id, thread_id=thread_id, spec=spec, max_bytes=self.config.max_input_bytes)
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            values = dict(
                id=str(uuid4()),
                user_id=user_id,
                thread_id=thread_id,
                source_run_id=source_run_id,
                tracking_task_id=tracking_task_id,
                idempotency_key=idempotency_key,
                dedupe_group=dedupe_group,
                spec=payload,
                state="staged",
                staged_deadline=now + timedelta(seconds=self.config.staged_timeout_seconds),
                queue_deadline=now + timedelta(seconds=spec.queue_timeout_seconds),
            )
            # Concurrent identical submissions serialize on the unique index;
            # the winner's immutable tracking identity is always returned.
            await session.execute(insert(JobRow).values(**values).on_conflict_do_nothing(index_elements=["user_id", "idempotency_key"]))
            job = (await session.execute(select(JobRow).where(JobRow.user_id == user_id, JobRow.idempotency_key == idempotency_key).with_for_update())).scalar_one()
            if job.thread_id != thread_id or job.source_run_id != source_run_id or job.spec != payload or job.dedupe_group != dedupe_group:
                raise ValueError("Submission key belongs to different input")
            return self.summary(job)

    async def get(self, job_id: str, *, user_id: str, thread_id: str) -> dict:
        async with self.sf() as session:
            job = await session.get(JobRow, job_id)
            if job is None or job.user_id != user_id or job.thread_id != thread_id:
                raise PermissionError("Job unavailable")
            return self.summary(job)

    async def reconcile(self, job_id: str) -> str:
        async with self.sf.begin() as session:
            job = await session.get(JobRow, job_id, with_for_update=True)
            if job is None:
                raise ValueError("Unknown job")
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            if job.state == "queued" and now >= job.queue_deadline:
                job.state = "failed"
                job.error = "Queue deadline elapsed before execution"
                job.finished_at = job.updated_at = now
            if job.state != "staged":
                return job.state
            tracking = await self.tracking_reader(session, job.tracking_task_id)
            matched = tracking and all(
                tracking.get(key) == value
                for key, value in {
                    "id": job.tracking_task_id,
                    "user_id": job.user_id,
                    "thread_id": job.thread_id,
                    "remote_task_id": job.id,
                    "driver_name": "fleet",
                    "server_name": "fleet",
                }.items()
            )
            if matched and (tracking.get("status") == "cancelled" or tracking.get("cancel_requested_at") is not None):
                job.state = "cancelled"
                job.cancel_requested_at = job.finished_at = now
            elif now >= job.staged_deadline or now >= job.queue_deadline:
                job.state = "failed"
                job.error = "Tracking handshake or queue deadline elapsed before execution"
                job.finished_at = now
            elif matched and tracking.get("status") in {"submitted", "working"}:
                job.state = "queued"
                job.queued_at = now
            job.updated_at = now
            return job.state

    async def cancel(self, job_id: str, *, user_id: str, thread_id: str) -> dict:
        async with self.sf.begin() as session:
            job = await session.get(JobRow, job_id, with_for_update=True)
            if job is None or job.user_id != user_id or job.thread_id != thread_id:
                raise PermissionError("Job unavailable")
            if job.state in {"succeeded", "failed", "cancelled"}:
                return self.summary(job)
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            job.cancel_requested_at = job.cancel_requested_at or now
            if job.state in {"staged", "queued"}:
                job.state = "cancelled"
                job.finished_at = now
            else:
                await cancel_stopped_attempt(session, job, now)
            # Cancellation cannot invent stop evidence. Started/claimed work
            # retains state and capacity until a physical stop is durable.
            job.updated_at = now
            return self.summary(job)
