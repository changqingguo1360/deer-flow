"""Durable attempt grants; execution locks always precede node and ledger locks."""

import hashlib
import hmac
from datetime import timedelta

from sqlalchemy import func, select

from .models import AttemptRow, JobRow, NodeRow, ReservationRow
from .reservations import release_stopped


class JobAttempts:
    def __init__(self, session_factory, config):
        self.sf = session_factory
        self.config = config

    async def locked(self, session, attempt_id):
        locator = await session.get(AttemptRow, attempt_id)
        if locator is None or locator.kind != "job":
            raise PermissionError("Attempt unavailable")
        job = await session.get(JobRow, locator.job_id, with_for_update=True)
        node = await session.get(NodeRow, locator.node_id, with_for_update=True)
        attempt = await session.get(AttemptRow, attempt_id, with_for_update=True, populate_existing=True)
        return job, node, attempt

    async def authenticate(self, session, *, node_id, node_session_id, attempt_id, token, require_lease=True):
        job, node, attempt = await self.locked(session, attempt_id)
        if not isinstance(token, str) or len(token) > 256 or attempt.node_id != node_id or not hmac.compare_digest(attempt.token_hash, hashlib.sha256(token.encode()).hexdigest()):
            raise PermissionError("Attempt unavailable")
        if node.session_id != node_session_id or (require_lease and attempt.node_session_id != node_session_id):
            raise ValueError("Stale node session")
        if job.active_attempt_id != attempt.id:
            raise ValueError("Attempt no longer active")
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        if require_lease and (attempt.lease_expires_at <= now or attempt.state not in {"claimed", "starting", "running"} or attempt.stopped_at is not None):
            raise ValueError("Attempt lease no longer valid")
        return job, node, attempt, now

    @staticmethod
    def grant(attempt, now):
        return {
            "authorized": True,
            "attempt_id": attempt.id,
            "process_ref": attempt.process_ref,
            "launch_spec": attempt.launch_spec,
            "lease_seconds_remaining": max(0, (attempt.lease_expires_at - now).total_seconds()),
            "execution_seconds_remaining": max(0, (attempt.execution_deadline - now).total_seconds()),
        }

    async def authorize_start(self, **identity):
        async with self.sf.begin() as session:
            job, node, attempt, now = await self.authenticate(session, **identity)
            if job.cancel_requested_at is not None or node.admin_state == "disabled":
                raise ValueError("Execution cancellation requested")
            if attempt.start_authorized_at is None:
                attempt.start_authorized_at = now
                seconds = min(attempt.launch_spec["spec"]["execution_timeout_seconds"], attempt.launch_spec["profile"]["execution_timeout_seconds"])
                attempt.execution_deadline = now + timedelta(seconds=seconds)
                attempt.process_ref = "fleet-" + attempt.id
                attempt.state = "starting"
            if attempt.execution_deadline <= now:
                raise ValueError("Execution deadline elapsed")
            return self.grant(attempt, now)

    async def renew(self, *, running=False, **identity):
        async with self.sf.begin() as session:
            job, node, attempt, now = await self.authenticate(session, **identity)
            if attempt.start_authorized_at is None:
                raise ValueError("Start grant required before renewal")
            if job.cancel_requested_at is not None or node.admin_state == "disabled":
                return {"stop": True, "reason": "cancel_requested"}
            if now >= attempt.execution_deadline:
                return {"stop": True, "reason": "execution_deadline"}
            attempt.lease_expires_at = min(now + timedelta(seconds=self.config.lease_seconds), attempt.execution_deadline)
            if running:
                attempt.state = job.state = "running"
                attempt.started_at = attempt.started_at or now
            return {"stop": False, **self.grant(attempt, now)}

    async def stopped(self, *, reason, exit_code, **identity):
        if reason not in {"exit", "cancelled", "lease_lost", "execution_deadline"}:
            raise ValueError("Invalid stop reason")
        async with self.sf.begin() as session:
            job, node, attempt, now = await self.authenticate(session, require_lease=False, **identity)
            if attempt.stopped_at is not None:
                return {"state": job.state, "stopped": True}
            if attempt.start_authorized_at is None:
                raise ValueError("No process was authorized")
            attempt.stopped_at = now
            attempt.outcome = {"exit_code": exit_code, "stop_reason": reason}
            if job.cancel_requested_at is not None:
                attempt.state = job.state = "cancelled"
                attempt.finished_at = job.finished_at = now
            elif reason != "exit" or attempt.lease_expires_at <= now or attempt.node_session_id != node.session_id:
                attempt.state = job.state = "unknown"
            elif exit_code != 0:
                attempt.state = job.state = "failed"
                job.error = f"Execution exited with code {exit_code}"
                attempt.finished_at = job.finished_at = now
            else:
                # Success requires B07's sealed and accepted manifest. A stop
                # acknowledgement alone cannot fabricate a successful result.
                attempt.state = job.state = "running"
            await release_stopped(session, attempt)
            return {"state": job.state, "stopped": True}

    async def expire_pending(self):
        async with self.sf() as session:
            ids = (await session.execute(select(AttemptRow.id).where(AttemptRow.kind == "job", AttemptRow.state.in_(["claimed", "starting", "running"]), AttemptRow.lease_expires_at <= func.clock_timestamp()).limit(100))).scalars().all()
        for attempt_id in ids:
            async with self.sf.begin() as session:
                job, node, attempt = await self.locked(session, attempt_id)
                now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
                if job.active_attempt_id != attempt.id or attempt.lease_expires_at > now or attempt.state not in {"claimed", "starting", "running"}:
                    continue
                if attempt.start_authorized_at is None:
                    # Fenced, never granted executions have no possible process.
                    attempt.state = "expired"
                    attempt.stopped_at = attempt.finished_at = now
                    job.active_attempt_id = None
                    job.state = "queued" if job.queue_deadline > now and job.cancel_requested_at is None else ("cancelled" if job.cancel_requested_at is not None else "failed")
                    if job.state != "queued":
                        job.finished_at = now
                    await release_stopped(session, attempt)
                else:
                    attempt.state = job.state = "unknown"
                    reservation = (await session.execute(select(ReservationRow).where(ReservationRow.attempt_id == attempt.id).with_for_update())).scalar_one()
                    if reservation.state != "released":
                        reservation.state = "quarantined"
