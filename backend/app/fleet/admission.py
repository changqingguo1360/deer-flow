"""Host read-only candidate projection; mutation remains in original admission paths."""

from deerflow_ecs_fleet.admission_policy import QueueCandidate
from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
from deerflow_ecs_fleet.persistence.models import AgentTaskRow, RunPlacementRow, SchedulerTicketRow
from deerflow_ecs_fleet.persistence.placements import RunPlacements
from sqlalchemy import exists, func, select, tuple_

from deerflow.persistence.run.model import RunRow


async def agent_candidates(session, window, *, occurrence_id=None):
    cfg = window.policy.config
    result = []
    cursor = None
    while True:
        query = (
            select(RunPlacementRow)
            .join(AgentTaskRow, AgentTaskRow.id == RunPlacementRow.agent_task_id)
            .join(RunRow, RunRow.run_id == RunPlacementRow.run_id)
            .where(
                RunPlacementRow.state == "queued",
                RunPlacementRow.active_attempt_id.is_(None),
                RunPlacementRow.queue_deadline > window.now,
                AgentTaskRow.state == "queued",
                AgentTaskRow.cancel_requested_at.is_(None),
                AgentTaskRow.deadline > window.now,
                AgentTaskRow.current_run_id == RunPlacementRow.run_id,
                AgentTaskRow.generation == RunPlacementRow.generation,
                AgentTaskRow.user_id == RunPlacementRow.user_id,
                AgentTaskRow.thread_id == RunPlacementRow.thread_id,
                RunRow.status == "pending",
                RunRow.owner_worker_id.is_(None),
                RunRow.lease_expires_at.is_(None),
                RunRow.cancel_action.is_(None),
                RunRow.user_id == RunPlacementRow.user_id,
                RunRow.thread_id == RunPlacementRow.thread_id,
                ~exists(select(SchedulerTicketRow.id).where(SchedulerTicketRow.run_id == RunPlacementRow.run_id, SchedulerTicketRow.state != "released")),
            )
            .order_by(RunPlacementRow.created_at, RunPlacementRow.run_id)
            .limit(64)
        )
        if cursor is not None:
            query = query.where(tuple_(RunPlacementRow.created_at, RunPlacementRow.run_id) > cursor)
        page = (await session.scalars(query)).all()
        for placement in page:
            profile = cfg.profiles.get(placement.profile)
            if profile is None or profile.kind != "agent":
                continue
            eligible = set()
            for node in window.nodes:
                if not window.live(node, "agent", placement.profile) or not window.capacity(node, profile) or node.agent_compatibility is None:
                    continue
                try:
                    spec = await RunPlacements().compatible_launch(session, run_id=placement.run_id, user_id=placement.user_id, thread_id=placement.thread_id, worker=WorkerCompatibility.model_validate(node.agent_compatibility))
                except ValueError:
                    continue
                if spec.execution_deadline > window.now and spec.runtime_digest == profile.runtime_digest and (spec.resources.cpu_millis, spec.resources.memory_mib) == (profile.cpu_millis, profile.memory_mib):
                    eligible.add(node.id)
            if eligible:
                result.append(QueueCandidate("agent", placement.run_id, placement.created_at, placement.profile, frozenset(eligible)))
        if len(page) < 64:
            break
        cursor = (page[-1].created_at, page[-1].run_id)
    from deerflow.config import get_app_config

    scheduler = get_app_config().scheduler
    if cfg.agent_bindings and (scheduler.enabled or occurrence_id is not None):
        from datetime import timedelta

        from fastapi import HTTPException
        from pydantic import ValidationError

        from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
        from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow

        from .routing import approved_binding

        query = (
            select(ScheduledTaskRunRow, ScheduledTaskRow)
            .join(ScheduledTaskRow, ScheduledTaskRow.id == ScheduledTaskRunRow.task_id)
            .where(
                ScheduledTaskRunRow.status.in_(["queued", "launching"]),
                ~exists(select(SchedulerTicketRow.id).where(SchedulerTicketRow.occurrence_id == ScheduledTaskRunRow.id, SchedulerTicketRow.state != "released")),
            )
        )
        if not scheduler.enabled:
            query = query.where(ScheduledTaskRunRow.id == occurrence_id)
        rows = (await session.execute(query)).all()
        executing = await session.scalar(select(func.count()).select_from(ScheduledTaskRunRow).where(ScheduledTaskRunRow.status.in_(["launching", "running"])))
        from .scheduled_agent_tasks import FleetScheduledAgentTasks

        aggregate = FleetScheduledAgentTasks(None, cfg)
        for occurrence, task in rows:
            if await aggregate.blocks(session, schedule_id=task.id, exclude_occurrence_id=occurrence.id):
                continue
            if task.status not in {"enabled", "dispatching", "paused"} or task.status == "paused" and occurrence.trigger != "manual":
                continue
            if occurrence.created_at + timedelta(seconds=scheduler.queue_timeout_seconds) <= window.now or occurrence.status == "queued" and executing >= scheduler.max_concurrent_runs:
                continue
            older = await session.scalar(
                select(ScheduledTaskRunRow.id)
                .where(
                    ScheduledTaskRunRow.thread_id == occurrence.thread_id,
                    ScheduledTaskRunRow.status.in_(["queued", "launching", "running"]),
                    tuple_(ScheduledTaskRunRow.created_at, ScheduledTaskRunRow.id) < (occurrence.created_at, occurrence.id),
                )
                .limit(1)
            )
            busy = await session.scalar(select(RunRow.run_id).where(RunRow.thread_id == occurrence.thread_id, RunRow.status.in_(["pending", "running"]), RunRow.run_id != (occurrence.run_id or "")).limit(1))
            goal = await session.scalar(
                select(AgentTaskRow.id)
                .where(
                    AgentTaskRow.user_id == task.user_id, AgentTaskRow.thread_id == occurrence.thread_id, AgentTaskRow.state.not_in(["succeeded", "failed", "cancelled", "timed_out"]), AgentTaskRow.current_run_id != (occurrence.run_id or "")
                )
                .limit(1)
            )
            if older is not None or busy is not None or goal is not None or task.context_mode != "reuse_thread":
                continue
            try:
                binding = approved_binding(cfg, task.execution, task.user_id)
            except (HTTPException, ValidationError):
                continue
            if binding is None or occurrence.created_at is None:
                continue
            if occurrence.status == "launching" and (occurrence.lease_expires_at is None or occurrence.lease_expires_at <= window.now):
                continue
            profile_name = task.execution["profile"]
            profile = cfg.profiles[profile_name]
            ids = frozenset(node.id for node in window.nodes if window.live(node, "agent", profile_name) and node.agent_compatibility == binding.compatibility and window.capacity(node, profile))
            if ids:
                key = occurrence.run_id or "schedule:" + occurrence.id
                if occurrence.run_id is not None and all(candidate.key != key for candidate in result):
                    continue
                result = [candidate for candidate in result if candidate.key != key]
                result.append(QueueCandidate("agent", key, occurrence.created_at, profile_name, ids))
    return result
