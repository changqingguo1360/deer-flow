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
        if attempt is None or not isinstance(token, str) or len(token) > 256 or attempt.node_id != node_id or not hmac.compare_digest(attempt.token_hash, hashlib.sha256(token.encode()).hexdigest()):
            raise PermissionError("Attempt unavailable")
        if node.session_id != node_session_id or (require_lease and attempt.node_session_id != node_session_id):
            raise ValueError("Stale node session")
        if job.active_attempt_id != attempt.id and (require_lease or attempt.stopped_at is None):
            raise ValueError("Attempt no longer active")
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        if require_lease and (attempt.lease_expires_at <= now or attempt.state not in {"claimed", "starting", "running"} or attempt.stopped_at is not None):
            raise ValueError("Attempt lease no longer valid")
        return job, node, attempt, now

    @staticmethod
    def grant(attempt, now):
        return {
            "authorized": True,
            "node_id": attempt.node_id,
            "node_session_id": attempt.node_session_id,
            "attempt_id": attempt.id,
            "job_id": attempt.job_id,
            "output_prefix": attempt.output_prefix,
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
                attempt.lease_expires_at = min(attempt.lease_expires_at, attempt.execution_deadline)
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
                attempt.state = "expired"
                attempt.stopped_at = attempt.finished_at = now
                job.active_attempt_id = None
                job.state = "queued" if job.queue_deadline > now and job.cancel_requested_at is None else ("cancelled" if job.cancel_requested_at is not None else "failed")
                if job.state != "queued":
                    job.finished_at = now
                await release_stopped(session, attempt)
                return {"state": job.state, "stopped": True}
            attempt.stopped_at = now
            attempt.outcome = {"exit_code": exit_code, "stop_reason": reason}
            if job.cancel_requested_at is not None:
                attempt.state = job.state = "cancelled"
                attempt.finished_at = job.finished_at = now
            elif job.state in {"unknown", "quarantined"} or attempt.state in {"unknown", "quarantined"} or reason != "exit" or attempt.lease_expires_at <= now or attempt.node_session_id != node.session_id:
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


def agent_owner(attempt_id):
    return "fleet-agent:" + attempt_id


class AgentAttempts:
    """Private lock/authentication boundary; the host injects core run locking."""

    async def locked(self, session, attempt_id, *, run_locker):
        from .models import AgentTaskRow, RunPlacementRow

        locator = await session.get(AttemptRow, attempt_id)
        if locator is None or locator.kind != "agent":
            raise PermissionError("Agent attempt unavailable")
        location = await session.get(RunPlacementRow, locator.run_id)
        if location is None:
            raise PermissionError("Agent placement unavailable")
        task = await session.get(AgentTaskRow, location.agent_task_id, with_for_update=True)
        run = await run_locker(session, locator.run_id)
        placement = await session.get(RunPlacementRow, locator.run_id, with_for_update=True, populate_existing=True)
        node = await session.get(NodeRow, locator.node_id, with_for_update=True)
        reservation = (await session.execute(select(ReservationRow).where(ReservationRow.attempt_id == attempt_id).with_for_update())).scalar_one_or_none()
        attempt = await session.get(AttemptRow, attempt_id, with_for_update=True, populate_existing=True)
        return task, run, placement, node, reservation, attempt

    async def authenticate(self, session, *, attempt_id, node_id, node_session_id, token, run_locker, require_lease=True, allow_terminal_run=False):
        rows = await self.locked(session, attempt_id, run_locker=run_locker)
        node, attempt = rows[3], rows[5]
        if attempt is None or not isinstance(token, str) or len(token) > 256 or attempt.node_id != node_id or not hmac.compare_digest(attempt.token_hash, hashlib.sha256(token.encode()).hexdigest()):
            raise PermissionError("Agent attempt unavailable")
        if node is None or node.session_id != node_session_id or attempt.node_session_id != node_session_id:
            raise ValueError("Stale node session")
        return await self._validated_rows(session, rows, node_id=node_id, token=token, require_lease=require_lease, allow_terminal_run=allow_terminal_run)

    async def authenticate_stop_reconciliation(self, session, *, attempt_id, node_id, node_session_id, original_node_session_id, token, run_locker):
        rows = await self.locked(session, attempt_id, run_locker=run_locker)
        node, attempt = rows[3], rows[5]
        if attempt is None or not isinstance(token, str) or len(token) > 256 or attempt.node_id != node_id or not hmac.compare_digest(attempt.token_hash, hashlib.sha256(token.encode()).hexdigest()):
            raise PermissionError("Agent attempt unavailable")
        if node is None or node.session_id != node_session_id or not original_node_session_id or attempt.node_session_id != original_node_session_id:
            raise ValueError("Stop reconciliation session mismatch")
        if attempt.start_authorized_at is None or attempt.process_ref != "fleet-" + attempt.id:
            raise ValueError("Started original process identity required")
        historical_stopped = attempt.stopped_at is not None
        if historical_stopped:
            reservation = rows[4]
            if reservation is None or reservation.state != "released" or reservation.released_at is None or reservation.attempt_id != attempt.id or reservation.node_id != attempt.node_id:
                raise ValueError("Durable stopped reservation required")
        return await self._validated_rows(session, rows, node_id=node_id, token=token, require_lease=False, allow_terminal_run=False, historical_stopped=historical_stopped)

    async def _validated_rows(self, session, rows, *, node_id, token, require_lease, allow_terminal_run, historical_stopped=False):
        task, run, placement, node, reservation, attempt = rows
        if attempt is None or not isinstance(token, str) or len(token) > 256 or attempt.node_id != node_id or not hmac.compare_digest(attempt.token_hash, hashlib.sha256(token.encode()).hexdigest()):
            raise PermissionError("Agent attempt unavailable")
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        if task is None or run is None or placement is None or reservation is None:
            raise ValueError("Agent ownership unavailable")
        if (
            attempt.kind != "agent"
            or attempt.run_id != run.run_id
            or task.id != placement.agent_task_id
            or (task.user_id, task.thread_id) != (run.user_id, run.thread_id)
            or (placement.user_id, placement.thread_id) != (run.user_id, run.thread_id)
        ):
            raise ValueError("Agent execution identity mismatch")
        if historical_stopped:
            frozen = attempt.launch_spec.get("launch_spec", {})
            if require_lease or attempt.stopped_at is None or reservation.state != "released" or reservation.released_at is None:
                raise ValueError("Historical STOP receipt unavailable")
            if (frozen.get("run_id"), frozen.get("agent_task_id"), frozen.get("user_id"), frozen.get("thread_id")) != (run.run_id, task.id, run.user_id, run.thread_id):
                raise ValueError("Historical STOP frozen identity mismatch")
        permitted_finishing = False
        if allow_terminal_run and task.state == placement.state == "finishing":
            from .workspace_points import accepted_final

            permitted_finishing = await accepted_final(session, task=task, run=run, placement=placement, attempt=attempt) is not None
            if not permitted_finishing:
                raise ValueError("Exact accepted final cleanup authority required")
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        if (
            (not historical_stopped and (task.current_run_id != run.run_id or task.generation != placement.generation))
            or placement.generation != attempt.launch_spec.get("launch_spec", {}).get("generation")
            or (require_lease and task.state not in {"queued", "running"} and not permitted_finishing)
        ):
            raise ValueError("Stale Agent generation")
        if placement.active_attempt_id != attempt.id or (require_lease and placement.state not in {"claimed", "running"} and not permitted_finishing) or run.owner_worker_id != agent_owner(attempt.id):
            raise ValueError("Agent attempt no longer owns run")
        if attempt.launch_spec.get("kind") != "agent" or "execution_profile" not in attempt.launch_spec:
            raise ValueError("Frozen Agent execution profile required")
        limits = attempt.launch_spec.get("input_limits")
        if not isinstance(limits, dict) or set(limits) != {"max_input_bytes"} or type(limits["max_input_bytes"]) is not int or not 0 < limits["max_input_bytes"] <= 2**31 - 1:
            raise ValueError("Frozen Agent input limits required")
        permitted_cleanup = allow_terminal_run and run.status in {"success", "error", "interrupted", "timeout"} and attempt.start_authorized_at is not None and attempt.process_ref == "fleet-" + attempt.id
        if require_lease and (
            (run.status not in {"pending", "running"} and not permitted_cleanup) or attempt.state not in {"claimed", "starting", "running"} or attempt.stopped_at is not None or reservation.state not in {"reserved", "active"}
        ):
            raise ValueError("Agent execution no longer active")
        if require_lease and (run.lease_expires_at is None or attempt.lease_expires_at != run.lease_expires_at or attempt.lease_expires_at <= now or task.deadline <= now or attempt.execution_deadline <= now):
            raise ValueError("Agent lease no longer valid")
        return rows, now
