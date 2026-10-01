"""A verified stopped attempt's manifest is the durable completion commit point."""

import asyncio
from uuid import uuid4

from sqlalchemy import func, select

from ..artifacts import SealedManifest
from .models import ArtifactManifestRow


class FleetManifests:
    def __init__(self, session_factory, *, attempts, workspace):
        self.sf = session_factory
        self.attempts = attempts
        self.workspace = workspace

    @staticmethod
    def outcome(row):
        return {"state": "succeeded", "manifest_id": row.id}

    @staticmethod
    def snapshot(row):
        return {
            "id": row.id,
            "attempt_id": row.attempt_id,
            "user_id": row.user_id,
            "thread_id": row.thread_id,
            "schema_version": 1,
            "output_prefix": row.output_prefix,
            "files": row.files,
            "total_bytes": row.total_bytes,
            "sealed_at": row.sealed_at.isoformat(),
        }

    async def complete(self, *, manifest, **identity):
        manifest = SealedManifest.model_validate(manifest)
        async with self.sf.begin() as session:
            job, node, attempt, now = await self.attempts.authenticate(session, require_lease=False, **identity)
            expected_prefix = attempt.output_prefix + "/sealed"
            if manifest.output_prefix != expected_prefix:
                raise ValueError("Manifest prefix does not belong to current attempt")
            accepted = (await session.execute(select(ArtifactManifestRow).where(ArtifactManifestRow.attempt_id == attempt.id))).scalar_one_or_none()
            if accepted is not None:
                if job.accepted_manifest_id != accepted.id or job.state != "succeeded" or accepted.files != [row.model_dump() for row in manifest.files] or accepted.total_bytes != manifest.total_bytes:
                    raise ValueError("Completed attempt manifest is immutable")
                return self.outcome(accepted)
            if job.active_attempt_id != attempt.id or job.state != "running" or attempt.state != "running" or attempt.node_session_id != node.session_id or job.cancel_requested_at is not None:
                raise ValueError("Attempt is not eligible for completion")
            if attempt.start_authorized_at is None or attempt.stopped_at is None or attempt.outcome != {"exit_code": 0, "stop_reason": "exit"}:
                raise ValueError("Successful physical stop proof is required")
            if attempt.lease_expires_at <= now or attempt.execution_deadline <= now:
                raise ValueError("Attempt lease or execution deadline elapsed")
            limit = min(attempt.launch_spec["profile"]["max_output_bytes"], 2**31 - 1)
            if manifest.total_bytes > limit:
                raise ValueError("Manifest exceeds authorized output budget")
            # Job -> node -> attempt locks serialize cancel/retry. Validation
            # does real NAS I/O off the event loop, then rechecks the DB clock.
            await asyncio.to_thread(self.workspace.verify_manifest, manifest.model_dump())
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            if attempt.lease_expires_at <= now or attempt.execution_deadline <= now:
                raise ValueError("Attempt lease or execution deadline elapsed during validation")
            row = ArtifactManifestRow(
                id=str(uuid4()), attempt_id=attempt.id, user_id=job.user_id, thread_id=job.thread_id, output_prefix=expected_prefix, files=[item.model_dump() for item in manifest.files], total_bytes=manifest.total_bytes, sealed_at=now
            )
            session.add(row)
            job.accepted_manifest_id = row.id
            attempt.state = job.state = "succeeded"
            attempt.finished_at = job.finished_at = now
            job.updated_at = now
            await session.flush()
            return self.outcome(row)

    async def get(self, manifest_id, *, user_id, thread_id):
        async with self.sf() as session:
            row = (await session.execute(select(ArtifactManifestRow).where(ArtifactManifestRow.id == manifest_id, ArtifactManifestRow.user_id == user_id, ArtifactManifestRow.thread_id == thread_id))).scalar_one_or_none()
            if row is None:
                raise LookupError("Artifact manifest unavailable")
            return self.snapshot(row)

    async def open_artifact(self, manifest_id, relative_path, *, user_id, thread_id):
        row = await self.get(manifest_id, user_id=user_id, thread_id=thread_id)
        manifest = {key: row[key] for key in ("schema_version", "output_prefix", "files", "total_bytes")}
        pending = asyncio.create_task(asyncio.to_thread(self.workspace.open_artifact, manifest, relative_path))
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:

            def close_when_ready(task):
                if not task.cancelled() and task.exception() is None:
                    asyncio.create_task(asyncio.to_thread(task.result().close))

            pending.add_done_callback(close_when_ready)
            raise
