"""Host bridge atomically joins private Fleet attempts with the actual core run."""

import hashlib
import secrets
from dataclasses import dataclass, field
from datetime import timedelta
from uuid import uuid4

from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
from deerflow_ecs_fleet.persistence.attempts import AgentAttempts, agent_owner
from deerflow_ecs_fleet.persistence.models import AgentTaskRow, AttemptRow, NodeRow, RunPlacementRow
from deerflow_ecs_fleet.persistence.placements import RunPlacements
from deerflow_ecs_fleet.persistence.reservations import reserve
from sqlalchemy import exists, func, select

from deerflow.persistence.run.model import RunRow


@dataclass(frozen=True)
class AgentClaim:
    run_id: str
    agent_task_id: str
    attempt_id: str
    token: str = field(repr=False)
    lease_seconds: float
    owner_worker_id: str
    launch_spec: dict = field(repr=False)


class FleetRunOwnership:
    def __init__(self, session_factory, config):
        # Closed admission flags/profiles must not cut off accepted renewals.
        self.sf = session_factory
        self.config = config
        self.attempts = AgentAttempts()

    @staticmethod
    async def lock_run(session, run_id):
        return await session.get(RunRow, run_id, with_for_update=True)

    async def claim_agent(self, node_id, *, node_session_id, worker: WorkerCompatibility):
        worker = WorkerCompatibility.model_validate(worker.model_dump(mode="json"))
        if not self.config.enabled or not self.config.agents_enabled or not self.config.jobs_enabled:
            return None
        async with self.sf() as session:
            candidates = (
                await session.execute(
                    select(RunPlacementRow.run_id, RunPlacementRow.agent_task_id)
                    .where(RunPlacementRow.state == "queued", RunPlacementRow.queue_deadline > func.clock_timestamp())
                    .order_by(RunPlacementRow.created_at, RunPlacementRow.run_id)
                    .limit(64)
                )
            ).all()
        for run_id, task_id in candidates:
            async with self.sf.begin() as session:
                task = await session.get(AgentTaskRow, task_id, with_for_update={"skip_locked": True})
                if task is None:
                    continue
                run = await self.lock_run(session, run_id)
                placement = await session.get(RunPlacementRow, run_id, with_for_update=True)
                node = await session.get(NodeRow, node_id, with_for_update=True)
                now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
                if node is None or node.session_id != node_session_id:
                    raise ValueError("Stale node session")
                if node.admin_state != "enabled" or node.health != "online" or node.last_seen_at is None or node.last_seen_at + timedelta(seconds=self.config.lease_seconds) <= now or node.agent_limit <= 0:
                    continue
                if (
                    run is None
                    or placement is None
                    or placement.state != "queued"
                    or placement.active_attempt_id is not None
                    or placement.queue_deadline <= now
                    or run.status != "pending"
                    or run.owner_worker_id is not None
                    or run.lease_expires_at is not None
                ):
                    continue
                if run.user_id != placement.user_id or run.thread_id != placement.thread_id or task.user_id != placement.user_id or task.thread_id != placement.thread_id:
                    continue
                if task.state != "queued" or task.cancel_requested_at is not None or run.cancel_action is not None or task.current_run_id != run_id or task.generation != placement.generation or task.deadline <= now:
                    continue
                profile = self.config.profiles.get(placement.profile)
                # Legacy NULL is compatible only with jobs, never Agent grants.
                if profile is None or profile.kind != "agent" or node.profile_allowlist is None or placement.profile not in node.profile_allowlist:
                    continue
                try:
                    spec = await RunPlacements().compatible_launch(session, run_id=run_id, user_id=placement.user_id, thread_id=placement.thread_id, worker=worker)
                except ValueError:
                    continue
                if spec.runtime_digest != profile.runtime_digest or spec.resources.cpu_millis != profile.cpu_millis or spec.resources.memory_mib != profile.memory_mib or spec.generation != task.generation or spec.execution_deadline <= now:
                    continue
                number = (await session.execute(select(func.coalesce(func.max(AttemptRow.attempt_no), 0)).where(AttemptRow.run_id == run_id))).scalar_one() + 1
                attempt_id, token = str(uuid4()), secrets.token_urlsafe(48)
                expiry = min(now + timedelta(seconds=self.config.lease_seconds), spec.execution_deadline, task.deadline)
                attempt = AttemptRow(
                    id=attempt_id,
                    kind="agent",
                    run_id=run_id,
                    attempt_no=number,
                    node_id=node_id,
                    node_session_id=node_session_id,
                    token_hash=hashlib.sha256(token.encode()).hexdigest(),
                    state="claimed",
                    launch_spec=spec.canonical_payload(),
                    output_prefix=f"{spec.user_id}/{spec.thread_id}/agents/{task.id}/attempts/{attempt_id}",
                    lease_expires_at=expiry,
                    execution_deadline=spec.execution_deadline,
                )
                if not await reserve(session, node, attempt, cpu_millis=spec.resources.cpu_millis, memory_mib=spec.resources.memory_mib, agent_units=1):
                    continue
                run.owner_worker_id = agent_owner(attempt_id)
                run.lease_expires_at = expiry
                run.updated_at = now
                placement.node_id, placement.active_attempt_id, placement.state = node_id, attempt_id, "claimed"
                placement.updated_at = now
                task.state, task.updated_at = "running", now
                await session.flush()
                return AgentClaim(run_id, task.id, attempt_id, token, (expiry - now).total_seconds(), run.owner_worker_id, spec.canonical_payload())
        return None

    async def renew(self, *, running=False, **identity):
        async with self.sf.begin() as session:
            rows, now = await self.attempts.authenticate(session, run_locker=self.lock_run, **identity)
            task, run, placement, node, reservation, attempt = rows
            if node.admin_state == "disabled" or task.cancel_requested_at is not None or run.cancel_action is not None:
                return {"stop": True, "reason": "cancel_requested"}
            expiry = min(now + timedelta(seconds=self.config.lease_seconds), task.deadline, attempt.execution_deadline)
            run.lease_expires_at = attempt.lease_expires_at = expiry
            run.updated_at = placement.updated_at = task.updated_at = now
            if running:
                attempt.state = placement.state = "running"
                run.status = "running"
                attempt.started_at = attempt.started_at or now
                reservation.state = "active"
            await session.flush()
            return {"stop": False, "run_id": run.run_id, "attempt_id": attempt.id, "owner_worker_id": run.owner_worker_id, "lease_expires_at": expiry.isoformat(), "lease_seconds_remaining": (expiry - now).total_seconds()}


def install_fleet_ownership(app, session_factory):
    from app.fleet.runtime import fleet_runtime

    runtime = fleet_runtime(app)
    if runtime is None or not runtime.ready or session_factory is None:
        return
    app.state.fleet_ownership = FleetRunOwnership(session_factory, runtime.config)
    # Added to the generic persisted backend fence, in SQL scan and UPDATE.
    app.state.run_store.set_local_recovery_predicate(~exists(select(RunPlacementRow.run_id).where(RunPlacementRow.run_id == RunRow.run_id)))
