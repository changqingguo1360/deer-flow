"""Admin-only callers may close uncertain work after durable stop and review."""

import re
from uuid import uuid4

from sqlalchemy import func, select

from .config import NAME_PATTERN
from .persistence.models import AttemptRow, JobRow, RecoveryEventRow, ReservationRow


class FleetRecovery:
    def __init__(self, session_factory, *, attempts):
        self.sf = session_factory
        self.attempts = attempts

    @staticmethod
    def response(event):
        return {"job_id": event.job_id, "attempt_id": event.attempt_id, "state": "failed", "recovery_event_id": event.id}

    async def list_unresolved(self, *, limit=100, offset=0):
        if not isinstance(limit, int) or not 1 <= limit <= 100 or not isinstance(offset, int) or not 0 <= offset <= 1_000_000:
            raise ValueError("Invalid recovery page")
        async with self.sf() as session:
            rows = (
                await session.execute(
                    select(JobRow.id, JobRow.state, JobRow.active_attempt_id, JobRow.cancel_requested_at, AttemptRow.node_id, AttemptRow.stopped_at, ReservationRow.state.label("capacity_state"))
                    .outerjoin(AttemptRow, (AttemptRow.id == JobRow.active_attempt_id) & (AttemptRow.job_id == JobRow.id))
                    .outerjoin(ReservationRow, ReservationRow.attempt_id == AttemptRow.id)
                    .where(JobRow.state.in_(["unknown", "quarantined"]))
                    .order_by(JobRow.created_at, JobRow.id)
                    .limit(limit)
                    .offset(offset)
                )
            ).all()
            return [
                {
                    "job_id": row.id,
                    "state": row.state,
                    "attempt_id": row.active_attempt_id,
                    "node_id": row.node_id,
                    "stop_confirmed": row.stopped_at is not None,
                    "capacity_released": row.capacity_state == "released",
                    "cancel_requested": row.cancel_requested_at is not None,
                }
                for row in rows
            ]

    async def events(self, job_id):
        async with self.sf() as session:
            if await session.get(JobRow, job_id) is None:
                raise LookupError("Job unavailable")
            rows = (await session.execute(select(RecoveryEventRow).where(RecoveryEventRow.job_id == job_id).order_by(RecoveryEventRow.created_at, RecoveryEventRow.id))).scalars().all()
            return [{"id": row.id, "attempt_id": row.attempt_id, "operator_id": row.operator_id, "action": row.action, "note": row.note, "created_at": row.created_at.isoformat()} for row in rows]

    async def resolve(self, *, job_id, expected_attempt_id, operator_id, note, side_effects_reviewed):
        if not isinstance(operator_id, str) or re.fullmatch(NAME_PATTERN, operator_id) is None:
            raise ValueError("Invalid operator identity")
        if side_effects_reviewed is not True or not isinstance(note, str) or not note.strip() or len(note) > 1000:
            raise ValueError("Explicit side-effect review and bounded note required")
        async with self.sf.begin() as session:
            job = await session.get(JobRow, job_id, with_for_update=True)
            if job is None:
                raise LookupError("Job unavailable")
            if job.active_attempt_id != expected_attempt_id:
                raise ValueError("Active attempt changed")
            job, node, attempt = await self.attempts.locked(session, expected_attempt_id)
            if job.id != job_id:
                raise ValueError("Attempt does not belong to job")
            existing = (await session.execute(select(RecoveryEventRow).where(RecoveryEventRow.attempt_id == attempt.id))).scalar_one_or_none()
            if existing is not None:
                if existing.operator_id != operator_id or existing.note != note or existing.job_id != job_id or job.state != "failed":
                    raise ValueError("Recorded resolution is immutable")
                return self.response(existing)
            if job.state not in {"unknown", "quarantined"} or attempt.state not in {"unknown", "quarantined"} or job.accepted_manifest_id is not None:
                raise ValueError("Only uncertain execution can be reconciled")
            if attempt.stopped_at is None:
                raise ValueError("Durable physical stop confirmation required")
            reservation = (await session.execute(select(ReservationRow).where(ReservationRow.attempt_id == attempt.id).with_for_update())).scalar_one()
            if reservation.state != "released" or reservation.released_at is None:
                raise ValueError("Stop reconciliation has not released capacity")
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            event = RecoveryEventRow(id=str(uuid4()), job_id=job.id, attempt_id=attempt.id, operator_id=operator_id, action="fail_stopped", note=note, created_at=now)
            session.add(event)
            # Never create a successful outcome, accept artifacts or requeue the
            # old execution. Any subsequent execution requires a new submission.
            attempt.state = job.state = "failed"
            attempt.finished_at = job.finished_at = job.updated_at = now
            job.error = "Stopped execution outcome remains unknown; operator review recorded"
            await session.flush()
            return self.response(event)
