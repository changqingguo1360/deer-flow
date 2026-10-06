"""Bind committed host task tracking to the optional Fleet runtime."""

from sqlalchemy import select

from deerflow.persistence.mcp_tasks.model import McpTaskRow


async def read_tracking(session, task_id: str):
    row = (await session.execute(select(McpTaskRow).where(McpTaskRow.id == task_id).with_for_update())).scalar_one_or_none()
    return row.to_dict() if row is not None else None


def register_fleet_driver(app, drivers) -> None:
    runtimes = [runtime for _, runtime in getattr(getattr(app.state, "extensions", None), "services", ()) if getattr(runtime, "fleet_protocol_version", None) == 1]
    if not runtimes:
        return
    if len(runtimes) != 1:
        raise ValueError("Exactly one Fleet runtime is permitted")
    runtime = runtimes[0]
    if not runtime.ready:
        return
    drivers.register("fleet", runtime.bind_tracking(read_tracking))


class BoundJobParent:
    """Host-only adapter from accepted execution capability to child ownership."""

    def __init__(self, capability):
        from deerflow_ecs_fleet.persistence.job_links import ParentJobOwner

        self._capability = capability
        context = capability.context
        self.owner = ParentJobOwner(context.agent_task_id, context.generation, context.run_id, context.user_id, context.thread_id)

    async def validate(self, session, *, user_id, thread_id, source_run_id):
        from deerflow.runtime.execution.mutation_context import MutationTarget, OwnershipRejected, current_remote_mutation_context

        context = self._capability.context
        if (user_id, thread_id, source_run_id) != (context.user_id, context.thread_id, context.run_id):
            raise OwnershipRejected("Fleet child submission original target rejected")
        await self._capability.validate_async(session, context=current_remote_mutation_context(), operation="mcp.create", targets=(MutationTarget(user_id=user_id, thread_id=thread_id, run_id=source_run_id),))


def bind_private_fleet_driver(session_factory, drivers, mutation_capability, plugins):
    """Bind accepted operator config without starting Gateway background services."""
    candidates = [plugin for plugin in plugins if plugin.enabled and plugin.use == "deerflow_ecs_fleet:install" and plugin.config.get("enabled", False)]
    if not candidates:
        return None
    if len(candidates) != 1:
        raise ValueError("Exactly one private Fleet configuration is permitted")
    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_ecs_fleet.job_service import FleetJobService
    from deerflow_ecs_fleet.mcp_driver import FleetTaskDriver

    config = FleetConfig.model_validate(candidates[0].config)
    if not config.jobs_enabled:
        return None
    jobs = FleetJobService(session_factory, config, tracking_reader=read_tracking, parent_capability=BoundJobParent(mutation_capability))
    drivers.register("fleet", FleetTaskDriver(jobs))
    return config


class BoundFleetYield:
    """Private dependency collector; shares the original publication SQL authority."""

    def __init__(self, session_factory, parent):
        self.sf, self.parent = session_factory, parent

    def check_context(self):
        from deerflow.runtime.execution.mutation_context import OwnershipRejected, current_remote_mutation_context

        if current_remote_mutation_context() != self.parent._capability.context:
            raise OwnershipRejected("Original Fleet yield scope required")

    async def validate_requested(self, job_ids):
        from deerflow_ecs_fleet.persistence.models import JobLinkRow, JobRow

        from deerflow.runtime.execution.mutation_context import OwnershipRejected

        self.check_context()
        if not isinstance(job_ids, list) or not job_ids or len(job_ids) > 128 or any(not isinstance(value, str) or not value for value in job_ids):
            raise ValueError("Await requires bounded background task IDs")
        owner = self.parent.owner
        async with self.sf.begin() as session:
            await self.parent.validate(session, user_id=owner.user_id, thread_id=owner.thread_id, source_run_id=owner.parent_run_id)
            for value in sorted(set(job_ids)):
                job = await session.scalar(select(JobRow).where(JobRow.tracking_task_id == value, JobRow.user_id == owner.user_id).with_for_update())
                link = await session.get(JobLinkRow, job.id) if job is not None else None
                if link is None or link.link_mode != "awaited" or any(getattr(link, name) != getattr(owner, name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")):
                    raise OwnershipRejected("Await requires original owned awaited background tasks")
            await self.parent.validate(session, user_id=owner.user_id, thread_id=owner.thread_id, source_run_id=owner.parent_run_id)

    async def prepare(self):
        from deerflow_ecs_fleet.persistence.wait_groups import WaitGroups

        self.check_context()
        return await WaitGroups(self.sf, parent_capability=self.parent).prepare_remaining()

    async def finalize(self, session, group, identity, candidate):
        from deerflow_ecs_fleet.persistence.models import AgentTaskRow, WaitGroupRow

        from deerflow.runtime.execution.mutation_context import OwnershipRejected

        self.check_context()
        row = await session.get(WaitGroupRow, group["id"], with_for_update=True)
        expected = self.parent.owner
        if row is None or any(getattr(row, name) != getattr(expected, name) for name in ("agent_task_id", "generation", "parent_run_id", "user_id", "thread_id")) or row.job_ids != group["job_ids"]:
            raise OwnershipRejected("Original prepared dependency group changed")
        if row.state not in {"preparing", "waiting_jobs"} or (row.checkpoint_id is not None and row.checkpoint_id != identity.checkpoint_id) or (row.workspace_point_id is not None and row.workspace_point_id != identity.request_id):
            raise OwnershipRejected("Original dependency proof conflicts")
        task = await session.get(AgentTaskRow, identity.agent_task_id)
        if task.wait_group_id not in {None, row.id}:
            raise OwnershipRejected("Original task dependency group conflicts")
        task.wait_group_id = row.id
        row.checkpoint_id, row.workspace_point_id, row.state = identity.checkpoint_id, identity.request_id, "waiting_jobs"
        await session.flush()
