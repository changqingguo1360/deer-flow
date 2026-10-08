"""Host bridge atomically joins private Fleet attempts with the actual core run."""

import asyncio
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
        if worker.workspace_contract_version != 1:
            raise ValueError("Installed workspace contract capability required for new Agent claims")
        # Existing NodeClient claim advertisement is readiness, not an operator grant.
        # Persist it against exactly this incarnation before scanning any work.
        async with self.sf.begin() as session:
            node = await session.get(NodeRow, node_id, with_for_update=True)
            if node is None or node.session_id != node_session_id:
                raise ValueError("Stale node session")
            if node.claim_kinds is None:
                node.claim_kinds = ["agent"]
            node.agent_compatibility = worker.model_dump(mode="json")
            node.runtime_digest = worker.runtime_digest
        # New-admission switches do not revoke accepted queued placements.
        if not self.config.enabled:
            return None
        from deerflow_ecs_fleet.admission_policy import queued_keys

        query = select(RunPlacementRow.run_id, RunPlacementRow.agent_task_id, RunPlacementRow.created_at, RunPlacementRow.run_id).where(RunPlacementRow.state == "queued", RunPlacementRow.queue_deadline > func.clock_timestamp())
        async for run_id, task_id, _, _ in queued_keys(self.sf, query, (RunPlacementRow.created_at, RunPlacementRow.run_id)):
            async with self.sf.begin() as session:
                from deerflow_ecs_fleet.persistence.models import ReservationRow, SchedulerTicketRow

                from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
                from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow

                scheduled = await session.scalar(select(SchedulerTicketRow).where(SchedulerTicketRow.run_id == run_id).order_by(SchedulerTicketRow.created_at.desc()).limit(1))
                occurrence = None
                if scheduled is not None:
                    await session.get(ScheduledTaskRow, scheduled.scheduled_task_id, with_for_update=True)
                    occurrence = await session.get(ScheduledTaskRunRow, scheduled.occurrence_id, with_for_update=True)
                task = await session.get(AgentTaskRow, task_id, with_for_update={"skip_locked": True})
                if task is None:
                    continue
                run = await self.lock_run(session, run_id)
                placement = await session.get(RunPlacementRow, run_id, with_for_update=True)
                window = None
                if self.config.continuations_enabled:
                    from deerflow_ecs_fleet.admission_policy import SharedAdmissionPolicy

                    from .admission import agent_candidates

                    window = await SharedAdmissionPolicy(self.config, agent_candidates=agent_candidates).lock(session)
                    node = next((row for row in window.nodes if row.id == node_id), None)
                else:
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
                    launch_spec={"kind": "agent", "launch_spec": spec.canonical_payload(), "execution_profile": profile.model_dump(mode="json"), "input_limits": {"max_input_bytes": self.config.max_input_bytes}},
                    output_prefix=f"{spec.user_id}/{spec.thread_id}/agents/{task.id}/attempts/{attempt_id}",
                    lease_expires_at=expiry,
                    execution_deadline=spec.execution_deadline,
                )
                if scheduled is None:
                    if window is not None and not await window.permits("agent", run_id, node, profile):
                        continue
                    if not await reserve(session, node, attempt, cpu_millis=spec.resources.cpu_millis, memory_mib=spec.resources.memory_mib, agent_units=1):
                        continue
                    if window is not None:
                        window.reserved("agent")
                else:
                    ticket = await session.get(SchedulerTicketRow, scheduled.id, with_for_update=True, populate_existing=True)
                    reservation = await session.scalar(select(ReservationRow).where(ReservationRow.ticket_id == ticket.id).with_for_update())
                    now = await session.scalar(select(func.clock_timestamp()))
                    if (
                        ticket.state != "consumed"
                        or ticket.expires_at <= now
                        or ticket.run_id != run_id
                        or ticket.node_id != node_id
                        or ticket.node_session_id != node_session_id
                        or occurrence is None
                        or occurrence.status not in {"launching", "running"}
                        or occurrence.run_id != run_id
                        or run.idempotency_key != "scheduled-task:" + occurrence.id
                        or reservation is None
                        or reservation.state != "reserved"
                        or reservation.attempt_id is not None
                        or (reservation.cpu_millis, reservation.memory_mib, reservation.agent_units) != (spec.resources.cpu_millis, spec.resources.memory_mib, 1)
                    ):
                        continue
                    session.add(attempt)
                    await session.flush()
                    reservation.attempt_id = attempt.id
                    occurrence.status = "running"
                    occurrence.lease_owner = occurrence.lease_expires_at = None
                run.owner_worker_id = agent_owner(attempt_id)
                run.lease_expires_at = expiry
                run.updated_at = now
                placement.node_id, placement.active_attempt_id, placement.state = node_id, attempt_id, "claimed"
                placement.updated_at = now
                task.state, task.updated_at = "running", now
                await session.flush()
                return AgentClaim(run_id, task.id, attempt_id, token, (expiry - now).total_seconds(), run.owner_worker_id, spec.canonical_payload())
        return None

    @staticmethod
    def grant(rows, now):
        task, run, placement, node, reservation, attempt = rows
        envelope = attempt.launch_spec
        if envelope.get("kind") != "agent" or "execution_profile" not in envelope:
            raise ValueError("Frozen Agent execution profile required")
        return {
            "authorized": True,
            "kind": "agent",
            "node_id": node.id,
            "node_session_id": attempt.node_session_id,
            "run_id": run.run_id,
            "agent_task_id": task.id,
            "generation": task.generation,
            "attempt_id": attempt.id,
            "owner_worker_id": run.owner_worker_id,
            "process_ref": attempt.process_ref,
            "launch_spec": envelope["launch_spec"],
            "execution_profile": envelope["execution_profile"],
            "input_limits": envelope["input_limits"],
            "lease_seconds_remaining": max(0, (attempt.lease_expires_at - now).total_seconds()),
            "execution_seconds_remaining": max(0, (attempt.execution_deadline - now).total_seconds()),
        }

    async def authorize_start(self, **identity):
        from deerflow_ecs_fleet.launch_spec import LaunchSpec

        from .workspace_files import FleetWorkspaceFiles

        # Authenticate before any NAS reads, then release the original locks.
        async with self.sf.begin() as session:
            rows, now = await self.attempts.authenticate(session, run_locker=self.lock_run, **identity)
            task, run, placement, node, reservation, attempt = rows
            spec = LaunchSpec.model_validate(attempt.launch_spec["launch_spec"])
            original_digest = spec.payload_digest()
            if task.state not in {"queued", "running"} or placement.state not in {"claimed", "running"}:
                raise ValueError("Remote recovery or finishing blocks start")
            if node.admin_state == "disabled" or task.cancel_requested_at is not None or run.cancel_action is not None:
                raise ValueError("Execution cancellation requested")
        source = None
        if spec.source_workspace_point_id is not None:
            files = FleetWorkspaceFiles(self.sf, self.config)
            try:
                metadata, manifest = await files.selected(user_id=spec.user_id, thread_id=spec.source_workspace_thread_id or spec.thread_id, point_id=spec.source_workspace_point_id)
                if (spec.source_workspace_checkpoint_id or spec.normalized_config["configurable"].get("checkpoint_id")) != metadata["checkpoint_id"]:
                    raise ValueError("Original accepted checkpoint selector conflicts")
                await asyncio.to_thread(files.versions.verify, manifest)
                source = dict(point_id=metadata["point_id"], checkpoint_id=metadata["checkpoint_id"], manifest=manifest.model_dump(mode="json"))
            except (LookupError, ValueError, OSError):
                # A failed verification has no start authority; persist recovery
                # only through the still-original node/attempt/lease fence.
                async with self.sf.begin() as session:
                    rows, now = await self.attempts.authenticate(session, run_locker=self.lock_run, **identity)
                    task, run, placement, node, reservation, attempt = rows
                    if LaunchSpec.model_validate(attempt.launch_spec["launch_spec"]).payload_digest() != original_digest:
                        raise ValueError("Original start inputs changed")
                    task.state = placement.state = "recovery_required"
                    attempt.state = "unknown"
                    await session.flush()
                raise ValueError("Accepted workspace verification requires recovery") from None
        source_changed = False
        async with self.sf.begin() as session:
            rows, now = await self.attempts.authenticate(session, run_locker=self.lock_run, **identity)
            task, run, placement, node, reservation, attempt = rows
            fresh = LaunchSpec.model_validate(attempt.launch_spec["launch_spec"])
            if fresh.payload_digest() != original_digest or task.state not in {"queued", "running"} or placement.state not in {"claimed", "running"}:
                raise ValueError("Original start identity changed during verification")
            if node.admin_state == "disabled" or task.cancel_requested_at is not None or run.cancel_action is not None:
                raise ValueError("Execution cancellation requested")
            if source is not None:
                from deerflow_ecs_fleet.persistence.models import WorkspaceManifestRow, WorkspacePointRow

                from .workspace import FleetWorkspaceNodeService

                point = await session.get(WorkspacePointRow, source["point_id"])
                stored = FleetWorkspaceNodeService._manifest(await session.get(WorkspaceManifestRow, source["manifest"]["manifest_id"]))
                root = await session.scalar(
                    __import__("sqlalchemy").text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"),
                    {"thread": spec.source_workspace_thread_id or spec.thread_id, "checkpoint": source["checkpoint_id"]},
                )
                if point is None or stored != manifest or point.manifest_id != stored.manifest_id or point.checkpoint_id != source["checkpoint_id"] or root is None or root.get("deerflow_execution_run_id") != point.run_id:
                    # This is still the freshly authenticated original attempt.
                    # Commit its recovery state before reporting the rejection;
                    # raising inside the TX would undo the recovery transition.
                    task.state = placement.state = "recovery_required"
                    attempt.state = "unknown"
                    source_changed = True
            if source_changed:
                await session.flush()
                # The original identity rows remain locked; the intentional
                # recovery state cannot pass active-start authentication again.
                # Recheck the fresh database clock after the recovery flush.
                now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
                if run.lease_expires_at is None or attempt.lease_expires_at != run.lease_expires_at or attempt.lease_expires_at <= now or task.deadline <= now or attempt.execution_deadline <= now:
                    raise ValueError("Agent lease no longer valid")
            else:
                if attempt.start_authorized_at is None:
                    attempt.start_authorized_at = now
                    attempt.process_ref = "fleet-" + attempt.id
                    attempt.state = "starting"
                await session.flush()
                # Lock/flush waits must finish before the final database-clock check.
                rows, now = await self.attempts.authenticate(session, run_locker=self.lock_run, **identity)
                grant = self.grant(rows, now)
                if source is not None:
                    grant.update(accepted_workspace=source, nas_identity=self.config.nas_identity)
                return grant
        raise ValueError("Original accepted source changed during verification")

    async def stopped(self, *, reason, exit_code, process_ref, physical_stopped=False, **identity):
        if physical_stopped is not True:
            raise ValueError("Trusted node physical stop proof required")
        async with self.sf.begin() as session:
            rows, now = await self.attempts.authenticate(session, run_locker=self.lock_run, require_lease=False, **identity)
            return await self._stopped_locked(session, rows, now, reason=reason, exit_code=exit_code, process_ref=process_ref)

    async def reconcile_stopped(self, *, reason, exit_code, process_ref, physical_stopped=False, **identity):
        if physical_stopped is not True:
            raise ValueError("Trusted node physical stop proof required")
        async with self.sf.begin() as session:
            rows, now = await self.attempts.authenticate_stop_reconciliation(session, run_locker=self.lock_run, **identity)
            return await self._stopped_locked(session, rows, now, reason=reason, exit_code=exit_code, process_ref=process_ref)

    async def _stopped_locked(self, session, rows, now, *, reason, exit_code, process_ref):
        from deerflow_ecs_fleet.persistence.reservations import release_stopped

        task, run, placement, node, reservation, attempt = rows
        if attempt.process_ref != process_ref or process_ref != "fleet-" + attempt.id:
            raise ValueError("Physical stop execution identity mismatch")
        if attempt.stopped_at is not None:
            return {"state": placement.state, "stopped": True}
        attempt.stopped_at = now
        attempt.outcome = {"exit_code": exit_code, "stop_reason": reason}
        from deerflow_ecs_fleet.persistence.workspace_points import accepted_final

        point = await accepted_final(session, task=task, run=run, placement=placement, attempt=attempt)
        if point is not None:
            # Immutable final authority survives every authenticated physical STOP ACK.
            # Transport stop reasons remain observations, not outcome authority.
            # Leases grant new writes; they cannot rewrite an accepted pair.
            task.state = point.desired_task_status
            placement.state = point.desired_placement_status
            attempt.state = "expired" if point.desired_placement_status == "timed_out" else point.desired_placement_status
            attempt.finished_at = now
        else:
            # A stopped runner without the exact terminal pair requires
            # explicit recovery. It never authorizes END or tool replay.
            task.state = placement.state = "recovery_required"
            attempt.state = "unknown"
        await release_stopped(session, attempt)
        await session.flush()
        from .scheduled_agent_tasks import FleetScheduledAgentTasks

        await FleetScheduledAgentTasks(self.sf, self.config).resolve_stopped(session, task=task, run=run, placement=placement, attempt=attempt, point=point, reservation=reservation, now=now)
        from app.fleet.events import FleetStreamSeals

        await FleetStreamSeals(self.sf).recover_locked(session, run=run, placement=placement, attempt=attempt, reservation=reservation)
        return {"state": placement.state, "stopped": True}

    async def renew(self, *, running=False, **identity):
        async with self.sf.begin() as session:
            rows, now = await self.attempts.authenticate(session, run_locker=self.lock_run, allow_terminal_run=not running, **identity)
            task, run, placement, node, reservation, attempt = rows
            if node.admin_state == "disabled" or task.cancel_requested_at is not None:
                return {"stop": True, "reason": "cancel_requested"}
            cleanup_deadline = min(run.cancel_requested_at + timedelta(seconds=120), task.deadline, attempt.execution_deadline) if run.cancel_action is not None else None
            if cleanup_deadline is not None and (run.cancel_action not in {"interrupt", "rollback"} or cleanup_deadline <= now):
                return {"stop": True, "reason": "cancel_requested"}
            expiry = min(now + timedelta(seconds=self.config.lease_seconds), task.deadline, attempt.execution_deadline, cleanup_deadline or task.deadline)
            run.lease_expires_at = attempt.lease_expires_at = expiry
            run.updated_at = placement.updated_at = task.updated_at = now
            if running and run.cancel_action is None:
                attempt.state = placement.state = "running"
                run.status = "running"
                attempt.started_at = attempt.started_at or now
                reservation.state = "active"
            await session.flush()
            rows, now = await self.attempts.authenticate(session, run_locker=self.lock_run, allow_terminal_run=not running, **identity)
            return {
                "stop": False,
                **self.grant(rows, now),
                "lease_expires_at": expiry.isoformat(),
                **({"control": "cancel", "cancel_action": run.cancel_action, "cleanup_deadline": cleanup_deadline.isoformat()} if cleanup_deadline is not None else {}),
            }


def install_fleet_ownership(app, session_factory):
    from app.fleet.runtime import fleet_runtime

    runtime = fleet_runtime(app)
    if runtime is None or not runtime.ready or session_factory is None:
        return
    from .execution import BoundFleetRunBackend, fleet_before_thread_guard, fleet_thread_admission_guard

    app.state.run_store.set_before_thread_admission_guard(fleet_before_thread_guard)
    app.state.run_store.set_thread_admission_guard(fleet_thread_admission_guard)
    from .continuations import install_fleet_continuations

    install_fleet_continuations(app, session_factory, runtime)
    app.state.bound_run_execution_backend = BoundFleetRunBackend(session_factory, runtime.config)
    from .admission import agent_candidates

    runtime.scheduler.agent_candidates = agent_candidates
    app.state.fleet_ownership = FleetRunOwnership(session_factory, runtime.config)
    app.state.fleet_routing_config = runtime.config
    from .scheduler_tickets import FleetSchedulerTickets

    app.state.fleet_scheduler_tickets = FleetSchedulerTickets(session_factory, runtime.config)
    from .agent_control import FleetAgentRunControl

    app.state.run_store.set_agent_run_control(FleetAgentRunControl(session_factory))
    from .workspace import FleetWorkspaceNodeService

    app.state.fleet_workspaces = FleetWorkspaceNodeService(session_factory, app.state.fleet_ownership)
    from .workspace_files import FleetWorkspaceFiles

    app.state.fleet_workspace_files = FleetWorkspaceFiles(session_factory, runtime.config)
    # Added to the generic persisted backend fence, in SQL scan and UPDATE.
    app.state.run_store.set_local_recovery_predicate(~exists(select(RunPlacementRow.run_id).where(RunPlacementRow.run_id == RunRow.run_id)))
