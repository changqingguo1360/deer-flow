"""Shared resource ledger; callers lock execution before entering this boundary."""

from uuid import uuid4

from sqlalchemy import func, select

from .models import NodeRow, ReservationRow


async def has_capacity(session, node, *, cpu_millis: int, memory_mib: int, agent_units: int = 0) -> bool:
    """Node must already be locked; all unreleased states consume capacity."""
    if cpu_millis <= 0 or memory_mib <= 0 or agent_units < 0:
        raise ValueError("Invalid resource budget")
    used = (
        await session.execute(
            select(
                func.coalesce(func.sum(ReservationRow.cpu_millis), 0),
                func.coalesce(func.sum(ReservationRow.memory_mib), 0),
                func.coalesce(func.sum(ReservationRow.agent_units), 0),
            ).where(ReservationRow.node_id == node.id, ReservationRow.state != "released")
        )
    ).one()
    if used[0] + cpu_millis > node.cpu_millis or used[1] + memory_mib > node.memory_mib or used[2] + agent_units > node.agent_limit:
        return False
    return True


async def reserve(session, node, attempt, *, cpu_millis: int, memory_mib: int, agent_units: int = 0) -> bool:
    if not await has_capacity(session, node, cpu_millis=cpu_millis, memory_mib=memory_mib, agent_units=agent_units):
        return False
    session.add(attempt)
    await session.flush()
    session.add(ReservationRow(id=str(uuid4()), node_id=node.id, attempt_id=attempt.id, cpu_millis=cpu_millis, memory_mib=memory_mib, agent_units=agent_units))
    await session.flush()
    return True


async def release_stopped(session, attempt) -> bool:
    """Never infer physical stop from lease expiry or a terminal business state."""
    if attempt.stopped_at is None:
        raise ValueError("Physical stop proof required")
    await session.get(NodeRow, attempt.node_id, with_for_update=True)
    reservation = (await session.execute(select(ReservationRow).where(ReservationRow.attempt_id == attempt.id).with_for_update())).scalar_one()
    if reservation.state == "released":
        return False
    reservation.state = "released"
    reservation.released_at = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    await session.flush()
    return True
