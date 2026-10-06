"""Short scheduler capacity reservations share B/C's physical resource ledger."""

from datetime import timedelta
from uuid import uuid4

from deerflow_ecs_fleet.persistence.models import AgentTaskRow, NodeRow, ReservationRow, RunPlacementRow, SchedulerTicketRow
from deerflow_ecs_fleet.persistence.reservations import has_capacity
from sqlalchemy import func, select

from deerflow.persistence.run.model import RunRow
from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow
from deerflow.runtime.execution.contracts import ExecutionPlan
from deerflow.runtime.runs.manager import ConflictError


class FleetSchedulerTickets:
    def __init__(self, session_factory, config):
        self.sf, self.config = session_factory, config
        from .scheduled_agent_tasks import FleetScheduledAgentTasks

        self.scheduled_agent_tasks = FleetScheduledAgentTasks(session_factory, config)

    async def reserve(self, session, *, task, occurrence, lease_owner, lease_expires_at):
        from .routing import approved_binding

        self.scheduled_agent_tasks.config = self.config
        if await self.scheduled_agent_tasks.blocks(session, schedule_id=task.id, exclude_occurrence_id=occurrence.id):
            return False
        binding = approved_binding(self.config, task.execution, task.user_id)
        if binding is None:
            return None
        selection = task.execution
        profile = self.config.profiles[selection["profile"]]
        now = await session.scalar(select(func.clock_timestamp()))
        # Parent/occurrence are already locked by the original queue repository.
        old = await session.scalar(select(SchedulerTicketRow).where(SchedulerTicketRow.occurrence_id == occurrence.id, SchedulerTicketRow.state != "released"))
        if old is not None:
            return False
        window = None
        if self.config.continuations_enabled:
            from deerflow_ecs_fleet.admission_policy import SharedAdmissionPolicy

            from .admission import agent_candidates

            window = await SharedAdmissionPolicy(self.config, agent_candidates=agent_candidates).lock(session)
            nodes = window.nodes
            now = window.now
        else:
            nodes = (await session.scalars(select(NodeRow).where(NodeRow.admin_state == "enabled", NodeRow.health == "online").order_by(NodeRow.id).with_for_update(skip_locked=True))).all()
        for node in nodes:
            if (
                node.agent_compatibility != binding.compatibility
                or node.session_id is None
                or node.last_seen_at is None
                or node.last_seen_at + timedelta(seconds=self.config.lease_seconds) <= now
                or node.profile_allowlist is None
                or selection["profile"] not in node.profile_allowlist
            ):
                continue
            if not await has_capacity(session, node, cpu_millis=profile.cpu_millis, memory_mib=profile.memory_mib, agent_units=1):
                continue
            if window is not None:
                key = occurrence.run_id or "schedule:" + occurrence.id
                projected = await agent_candidates(session, window, occurrence_id=occurrence.id)
                candidate = next((row for row in projected if row.key == key), None)
                if candidate is None or not await window.permits("agent", key, node, profile, candidate=candidate):
                    continue
            ticket = SchedulerTicketRow(
                id=str(uuid4()),
                occurrence_id=occurrence.id,
                scheduled_task_id=task.id,
                user_id=task.user_id,
                thread_id=occurrence.thread_id,
                profile=selection["profile"],
                node_id=node.id,
                node_session_id=node.session_id,
                lease_owner=lease_owner,
                run_id=occurrence.run_id,
                expires_at=min(now + timedelta(seconds=self.config.ticket_seconds), lease_expires_at),
                state="held",
            )
            session.add(ticket)
            await session.flush()
            session.add(ReservationRow(id=str(uuid4()), ticket_id=ticket.id, node_id=node.id, cpu_millis=profile.cpu_millis, memory_mib=profile.memory_mib, agent_units=1))
            await session.flush()
            if window is not None:
                window.reserved("agent")
            return ticket.id
        return False

    async def lock_occurrence(self, session, ticket_id, lease_owner, parameters):
        locator = await session.get(SchedulerTicketRow, ticket_id)
        if locator is None:
            raise ConflictError("Scheduled capacity ticket missing")
        task = await session.get(ScheduledTaskRow, locator.scheduled_task_id, with_for_update=True)
        occurrence = await session.get(ScheduledTaskRunRow, locator.occurrence_id, with_for_update=True)
        now = await session.scalar(select(func.clock_timestamp()))
        if (
            task is None
            or occurrence is None
            or occurrence.task_id != task.id
            or not (
                occurrence.status == "launching"
                and occurrence.lease_owner == lease_owner
                and occurrence.lease_expires_at is not None
                and occurrence.lease_expires_at > now
                or occurrence.status == "running"
                and locator.state == "consumed"
                and occurrence.run_id == locator.run_id
            )
            or (task.user_id, occurrence.thread_id) != (parameters.user_id, parameters.thread_id)
            or locator.lease_owner != lease_owner
        ):
            raise ConflictError("Scheduled launch claim no longer owns its occurrence")
        return occurrence

    async def consume(self, session, ticket_id, occurrence, admitted, profile):
        locator = await session.get(SchedulerTicketRow, ticket_id)
        placement = await session.get(RunPlacementRow, admitted["run_id"], with_for_update=True)
        node = await session.get(NodeRow, locator.node_id, with_for_update=True)
        ticket = await session.get(SchedulerTicketRow, ticket_id, with_for_update=True, populate_existing=True)
        reservation = await session.scalar(select(ReservationRow).where(ReservationRow.ticket_id == ticket_id).with_for_update())
        now = await session.scalar(select(func.clock_timestamp()))
        if (
            placement is None
            or placement.state != "queued"
            or placement.active_attempt_id is not None
            or node is None
            or ticket.state != "held"
            or ticket.expires_at <= now
            or ticket.occurrence_id != occurrence.id
            or ticket.profile != profile
            or (ticket.user_id, ticket.thread_id) != (admitted["user_id"], admitted["thread_id"])
            or node.session_id != ticket.node_session_id
            or node.health != "online"
            or node.admin_state != "enabled"
            or reservation is None
            or reservation.attempt_id is not None
            or reservation.state != "reserved"
            or admitted["idempotency_key"] != "scheduled-task:" + occurrence.id
            or admitted["owner_worker_id"] is not None
        ):
            raise ConflictError("Scheduled capacity ticket no longer valid")
        ticket.state, ticket.run_id, ticket.consumed_at = "consumed", admitted["run_id"], now
        placement.node_id = node.id
        occurrence.run_id = admitted["run_id"]
        await self.scheduled_agent_tasks.associate(session, task=await session.get(ScheduledTaskRow, occurrence.task_id), occurrence=occurrence, admitted=admitted, agent_task_id=placement.agent_task_id)
        await session.flush()

    async def original_workspace(self, ticket_id, parameters):
        from deerflow_ecs_fleet.persistence.placements import RunPlacements

        async with self.sf() as session:
            ticket = await session.get(SchedulerTicketRow, ticket_id)
            if ticket is None or (ticket.user_id, ticket.thread_id) != (parameters.user_id, parameters.thread_id):
                raise ConflictError("Original scheduled ticket identity conflicts")
            if ticket.run_id is None:
                return None
            spec = await RunPlacements().load_launch_spec(session, run_id=ticket.run_id, user_id=parameters.user_id, thread_id=parameters.thread_id)
            return spec.workspace_manifest_ref

    @staticmethod
    async def lock_pending(session, run_id):
        locator = await session.get(RunPlacementRow, run_id)
        if locator is None:
            return None
        task = await session.get(AgentTaskRow, locator.agent_task_id, with_for_update=True)
        run = await session.get(RunRow, run_id, with_for_update=True)
        placement = await session.get(RunPlacementRow, run_id, with_for_update=True, populate_existing=True)
        from deerflow_ecs_fleet.persistence.models import AttemptRow

        ever_assigned = await session.scalar(select(AttemptRow.id).where(AttemptRow.run_id == run_id).limit(1))
        if (
            ever_assigned is not None
            or run is None
            or task is None
            or placement.active_attempt_id is not None
            or placement.state != "queued"
            or run.status != "pending"
            or run.owner_worker_id is not None
            or run.lease_expires_at is not None
            or task.state != "queued"
        ):
            return None
        return task, run, placement

    async def retire_waiting(self, session, *, task, occurrence, error, now):
        """Caller owns scheduled parent/occurrence; no Attempt is authority to retire."""
        if occurrence.run_id is None:
            return
        persisted = await session.get(RunRow, occurrence.run_id)
        if persisted is None or (persisted.kwargs_json or {}).get("execution_backend") != "fleet":
            return
        rows = await self.lock_pending(session, occurrence.run_id)
        if rows is None:
            raise ConflictError("Scheduled execution already assigned; stop confirmation required")
        agent, run, placement = rows
        if run.idempotency_key != "scheduled-task:" + occurrence.id or (run.user_id, run.thread_id) != (task.user_id, occurrence.thread_id):
            raise ConflictError("Scheduled queued run identity conflicts")
        tickets = (await session.scalars(select(SchedulerTicketRow).where(SchedulerTicketRow.occurrence_id == occurrence.id, SchedulerTicketRow.state != "released"))).all()
        for ticket in tickets:
            await session.get(NodeRow, ticket.node_id, with_for_update=True)
            await session.get(SchedulerTicketRow, ticket.id, with_for_update=True)
            reservation = await session.scalar(select(ReservationRow).where(ReservationRow.ticket_id == ticket.id).with_for_update())
            if reservation.attempt_id is not None:
                raise ConflictError("Assigned reservation requires STOP")
            ticket.state, ticket.released_at = "released", now
            reservation.state, reservation.released_at = "released", now
        run.status, run.error, run.updated_at = "interrupted", error, now
        agent.state, agent.updated_at = "cancelled", now
        placement.state, placement.updated_at = "cancelled", now
        await self.scheduled_agent_tasks.resolve_unassigned(session, task=agent, run=run, placement=placement, now=now)
        await session.flush()

    async def cancel_unassigned(self, session, *, run_id, action):
        from deerflow.runtime.runs.manager import CancelOutcome

        if action not in {"interrupt", "rollback"}:
            return CancelOutcome.not_cancellable
        ticket = await session.scalar(select(SchedulerTicketRow).where(SchedulerTicketRow.run_id == run_id).order_by(SchedulerTicketRow.created_at.desc()).limit(1))
        parent = occurrence = None
        if ticket is not None:
            parent = await session.get(ScheduledTaskRow, ticket.scheduled_task_id, with_for_update=True)
            occurrence = await session.get(ScheduledTaskRunRow, ticket.occurrence_id, with_for_update=True)
        rows = await self.lock_pending(session, run_id)
        if rows is None:
            run = await session.get(RunRow, run_id)
            placement = await session.get(RunPlacementRow, run_id)
            # lock_pending has acquired Task/Run/placement in execution order.
            # Claim may have committed after the initial unlocked routing read;
            # preserve its assignment and record the original cancel intent.
            if placement is not None and placement.active_attempt_id is not None:
                from deerflow_ecs_fleet.persistence.models import AttemptRow

                from .agent_control import FleetAgentRunControl

                attempt = await session.get(AttemptRow, placement.active_attempt_id)
                if attempt is not None:
                    return await FleetAgentRunControl(self.sf)._request_assigned_cancel(session, run_id=run_id, attempt=attempt, action=action)
            return (
                CancelOutcome.cancelled
                if run is not None and placement is not None and run.status == "interrupted" and run.cancel_action is not None and placement.state == "cancelled" and placement.active_attempt_id is None
                else CancelOutcome.not_cancellable
            )
        agent, run, placement = rows
        if (run.kwargs_json or {}).get("execution_backend") != "fleet" or agent.current_run_id != run_id or agent.generation != placement.generation or (agent.user_id, agent.thread_id) != (run.user_id, run.thread_id):
            return CancelOutcome.not_cancellable
        now = await session.scalar(select(func.clock_timestamp()))
        if occurrence is not None:
            if parent is None:
                return CancelOutcome.not_cancellable
            await self.retire_waiting(session, task=parent, occurrence=occurrence, error="Cancelled before assignment", now=now)
            occurrence.status, occurrence.finished_at, occurrence.error = "interrupted", now, "Cancelled before assignment"
            occurrence.lease_owner = occurrence.lease_expires_at = None
            parent.last_error = occurrence.error
            if parent.schedule_type == "once":
                parent.status = "cancelled"
        else:
            run.status, run.error, run.updated_at = "interrupted", "Cancelled before assignment", now
            agent.state, agent.updated_at = "cancelled", now
            placement.state, placement.updated_at = "cancelled", now
            await self.scheduled_agent_tasks.resolve_unassigned(session, task=agent, run=run, placement=placement, now=now)
        run.cancel_action, run.cancel_requested_at = action, now
        await session.flush()
        return CancelOutcome.cancelled

    async def reconcile(self):
        self.scheduled_agent_tasks.config = self.config
        await self.scheduled_agent_tasks.reconcile_once()
        async with self.sf() as session:
            keys = (
                await session.execute(
                    select(SchedulerTicketRow.id, SchedulerTicketRow.scheduled_task_id, SchedulerTicketRow.occurrence_id)
                    .where(SchedulerTicketRow.state != "released", SchedulerTicketRow.expires_at <= func.clock_timestamp())
                    .order_by(SchedulerTicketRow.scheduled_task_id, SchedulerTicketRow.occurrence_id)
                )
            ).all()
        for ticket_id, parent_id, occurrence_id in keys:
            async with self.sf.begin() as session:
                parent = await session.get(ScheduledTaskRow, parent_id, with_for_update=True)
                occurrence = await session.get(ScheduledTaskRunRow, occurrence_id, with_for_update=True)
                locator = await session.get(SchedulerTicketRow, ticket_id)
                if locator.state == "released":
                    continue
                pending = None
                if locator.run_id is not None:
                    pending = await self.lock_pending(session, locator.run_id)
                    if pending is None:
                        continue  # Assigned execution is STOP-only.
                    if occurrence is None or parent is None or pending[1].idempotency_key != "scheduled-task:" + occurrence_id:
                        raise ConflictError("Expired scheduled ticket original identity missing")
                if pending is not None and (
                    occurrence.status not in {"queued", "launching", "running"}
                    or pending[0].deadline <= await session.scalar(select(func.clock_timestamp()))
                    or pending[2].queue_deadline <= await session.scalar(select(func.clock_timestamp()))
                ):
                    now = await session.scalar(select(func.clock_timestamp()))
                    await self.retire_waiting(session, task=parent, occurrence=occurrence, error=occurrence.error or "Scheduled remote admission expired before assignment", now=now)
                    if occurrence.status in {"queued", "launching", "running"}:
                        occurrence.status, occurrence.finished_at, occurrence.error = "failed", now, "Scheduled remote admission expired before assignment"
                        occurrence.lease_owner = occurrence.lease_expires_at = None
                        parent.last_error = occurrence.error
                        if parent.schedule_type == "once":
                            parent.status = "failed"
                    continue
                await session.get(NodeRow, locator.node_id, with_for_update=True)
                ticket = await session.get(SchedulerTicketRow, ticket_id, with_for_update=True, populate_existing=True)
                reservation = await session.scalar(select(ReservationRow).where(ReservationRow.ticket_id == ticket_id).with_for_update())
                now = await session.scalar(select(func.clock_timestamp()))
                if ticket.state == "released" or ticket.expires_at > now or reservation.attempt_id is not None:
                    continue
                reservation.state, reservation.released_at = "released", now
                ticket.state, ticket.released_at = "released", now
                if pending is not None:
                    pending[2].node_id = None
                if occurrence is not None and occurrence.status in {"launching", "running"}:
                    occurrence.status = "queued"
                    occurrence.lease_owner = occurrence.lease_expires_at = None
                    # Admission identity/run association and waiting age survive.
                await session.flush()


class TicketBackend:
    def __init__(self, backend, tickets, ticket_id, lease_owner):
        self.backend, self.tickets, self.ticket_id, self.lease_owner = backend, tickets, ticket_id, lease_owner

    def plan(self, parameters):
        plan = self.backend.plan(parameters)
        return ExecutionPlan(store_only=True, public_kwargs=plan.public_kwargs, participant=TicketAdmission(plan.participant, self.tickets, self.ticket_id, self.lease_owner))


class TicketAdmission:
    def __init__(self, inner, tickets, ticket_id, lease_owner):
        self.inner, self.tickets, self.ticket_id, self.lease_owner = inner, tickets, ticket_id, lease_owner

    async def before_thread_lock(self, session):
        self.occurrence = await self.tickets.lock_occurrence(session, self.ticket_id, self.lease_owner, self.inner.parameters)

    async def guard_thread(self, session, **kwargs):
        return await self.inner.guard_thread(session, **kwargs)

    async def prepare(self, session):
        await self.inner.prepare(session)

    async def insert(self, session, admitted):
        await self.inner.insert(session, admitted)
        await self.tickets.consume(session, self.ticket_id, self.occurrence, admitted, self.inner.backend.profile_name)

    async def validate_reuse(self, session, stored):
        await self.inner.validate_reuse(session, stored)
        locator = await session.get(SchedulerTicketRow, self.ticket_id)
        if locator.state == "consumed" and locator.run_id == stored["run_id"]:
            return
        await self.tickets.lock_pending(session, stored["run_id"])
        await self.tickets.consume(session, self.ticket_id, self.occurrence, stored, self.inner.backend.profile_name)
