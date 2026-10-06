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
