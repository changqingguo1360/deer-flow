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
