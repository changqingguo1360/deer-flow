"""Node sessions fence previous incarnations without freeing unknown work."""

from uuid import uuid4

from sqlalchemy import func, or_, select

from .persistence.models import AttemptRow, NodeRow


class NodeRegistry:
    def __init__(self, session_factory):
        self.sf = session_factory

    async def open_session(self, node_id: str, *, protocol_version: int) -> dict:
        if protocol_version != 1:
            raise ValueError("Incompatible Fleet protocol")
        async with self.sf.begin() as session:
            node = await session.get(NodeRow, node_id, with_for_update=True)
            if node is None or node.admin_state == "disabled":
                raise PermissionError("Node unavailable")
            attempts = (await session.execute(select(AttemptRow).where(AttemptRow.node_id == node_id, AttemptRow.state.in_(["claimed", "starting", "running", "unknown", "quarantined"])))).scalars().all()
            # Fence the old incarnation using only the node lock. Reconciliation
            # acquires execution -> node -> reservation locks separately. Old
            # reservations remain charged until physical stop is established.
            node.session_id = str(uuid4())
            node.protocol_version = protocol_version
            node.health = "unknown"
            node.last_seen_at = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            return {"node_id": node_id, "node_session_id": node.session_id, "reconcile_required": bool(attempts)}

    async def heartbeat(self, node_id: str, *, node_session_id: str, protocol_version: int) -> dict:
        async with self.sf.begin() as session:
            node = await session.get(NodeRow, node_id, with_for_update=True)
            if node is None or node.admin_state == "disabled":
                raise PermissionError("Node unavailable")
            if node.session_id != node_session_id or protocol_version != node.protocol_version:
                raise ValueError("Stale node session or incompatible protocol")
            unresolved = (
                await session.execute(
                    select(func.count())
                    .select_from(AttemptRow)
                    .where(AttemptRow.node_id == node_id, or_(AttemptRow.state.in_(["unknown", "quarantined"]), (AttemptRow.state.in_(["claimed", "starting", "running"])) & (AttemptRow.node_session_id != node.session_id)))
                )
            ).scalar_one()
            node.health = "unknown" if unresolved else "online"
            node.last_seen_at = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            return {"node_id": node_id, "health": node.health, "admin_state": node.admin_state}

    async def set_admin_state(self, node_id: str, state: str) -> None:
        if state not in {"enabled", "draining", "disabled"}:
            raise ValueError("Invalid node admin state")
        from .persistence.models import ReservationRow

        async with self.sf.begin() as session:
            node = await session.get(NodeRow, node_id, with_for_update=True)
            if node is None:
                raise ValueError("Unknown node")
            if state == "disabled":
                charged = (await session.execute(select(func.count()).select_from(ReservationRow).where(ReservationRow.node_id == node_id, ReservationRow.state != "released"))).scalar_one()
                if charged:
                    raise ValueError("Node has unreleased execution capacity; drain and stop first")
            node.admin_state = state
            node.updated_at = (await session.execute(select(func.clock_timestamp()))).scalar_one()

    async def delete(self, node_id: str) -> None:
        async with self.sf.begin() as session:
            node = await session.get(NodeRow, node_id, with_for_update=True)
            if node is None:
                return
            history = (await session.execute(select(func.count()).select_from(AttemptRow).where(AttemptRow.node_id == node_id))).scalar_one()
            if history:
                raise ValueError("Node has execution history; retain the disabled record")
            if node.admin_state != "disabled":
                raise ValueError("Disable node before deletion")
            await session.delete(node)
