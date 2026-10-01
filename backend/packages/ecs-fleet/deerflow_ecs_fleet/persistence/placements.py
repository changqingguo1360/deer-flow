"""Versioned placement inputs, without a shadow RunStore or a remote claim path."""

from uuid import uuid4

from sqlalchemy import select

from ..launch_spec import LaunchSpec
from .agent_tasks import AgentTasks, utc_deadline
from .models import LaunchSpecRow, RunPlacementRow


class RunPlacements:
    async def create(self, session, *, spec: LaunchSpec, queue_deadline, requested_backend="remote"):
        # Revalidate even Python model_copy(update=...) before persistence.
        spec = LaunchSpec.model_validate_json(spec.model_dump_json())
        queue_deadline = utc_deadline(queue_deadline)
        if requested_backend not in {"remote", "auto"} or queue_deadline > spec.execution_deadline:
            raise ValueError("Invalid remote placement admission budget")
        # C02 must already hold task before its core thread/run lock. Taking
        # this same task lock is safe; no method here locks node or attempt.
        task = await AgentTasks().owned(session, task_id=spec.agent_task_id, user_id=spec.user_id, thread_id=spec.thread_id, lock=True)
        if task.generation != spec.generation or task.cancel_requested_at is not None or task.state not in {"queued", "running"} or spec.execution_deadline > task.deadline:
            raise ValueError("Agent generation or execution budget is unavailable")
        existing = await session.get(RunPlacementRow, spec.run_id, with_for_update=True)
        if existing is not None:
            stored = await self.load_launch_spec(session, run_id=spec.run_id, user_id=spec.user_id, thread_id=spec.thread_id)
            if stored.payload_digest() != spec.payload_digest() or existing.requested_backend != requested_backend or existing.queue_deadline != queue_deadline:
                raise ValueError("Immutable launch admission conflicts")
            return existing
        ref = "launch-" + uuid4().hex
        payload = LaunchSpecRow(
            id=ref, run_id=spec.run_id, agent_task_id=spec.agent_task_id, generation=spec.generation, user_id=spec.user_id, thread_id=spec.thread_id, payload=spec.canonical_payload(), payload_digest=spec.payload_digest()
        )
        session.add(payload)
        await session.flush()
        placement = RunPlacementRow(
            run_id=spec.run_id,
            agent_task_id=spec.agent_task_id,
            generation=spec.generation,
            user_id=spec.user_id,
            thread_id=spec.thread_id,
            requested_backend=requested_backend,
            profile=spec.profile,
            launch_spec_ref=ref,
            queue_deadline=queue_deadline,
        )
        session.add(placement)
        task.current_run_id = spec.run_id
        await session.flush()
        return placement

    async def load_launch_spec(self, session, *, run_id, user_id, thread_id):
        row = (
            await session.execute(
                select(LaunchSpecRow).join(RunPlacementRow, RunPlacementRow.launch_spec_ref == LaunchSpecRow.id).where(RunPlacementRow.run_id == run_id, RunPlacementRow.user_id == user_id, RunPlacementRow.thread_id == thread_id)
            )
        ).scalar_one_or_none()
        if row is None:
            raise LookupError("Run placement not found")
        spec = LaunchSpec.model_validate(row.payload)
        if (spec.run_id, spec.agent_task_id, spec.generation, spec.user_id, spec.thread_id, spec.payload_digest()) != (row.run_id, row.agent_task_id, row.generation, row.user_id, row.thread_id, row.payload_digest):
            raise ValueError("Persisted launch identity or digest mismatch")
        return spec

    async def compatible_launch(self, session, *, worker, **identity):
        spec = await self.load_launch_spec(session, **identity)
        if (spec.runtime_digest, spec.skill_snapshot, spec.plugin_snapshot) != (worker.runtime_digest, worker.skill_snapshot, worker.plugin_snapshot):
            raise ValueError("Worker runtime or snapshot is incompatible")
        return spec

    async def public_summary(self, session, **identity):
        spec = await self.load_launch_spec(session, **identity)
        placement = await session.get(RunPlacementRow, spec.run_id)
        return spec.public_summary() | {"requested_backend": placement.requested_backend, "state": placement.state}
