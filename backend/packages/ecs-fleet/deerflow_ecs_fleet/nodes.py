"""Node sessions fence previous incarnations without freeing unknown work."""

from uuid import uuid4

from sqlalchemy import delete, func, or_, select

from .persistence.models import AttemptRow, NodeRow


class NodeRegistry:
    def __init__(self, session_factory, *, configured_profiles=(), default_profiles=None, agent_profiles=()):
        self.sf = session_factory
        self.agent_profiles = frozenset(agent_profiles)
        self.configured_profiles = frozenset(configured_profiles)
        self.default_profiles = self.configured_profiles if default_profiles is None else frozenset(default_profiles)

    async def register(self, *, node_id: str, name: str, cpu_millis: int, memory_mib: int, profile_allowlist: list[str] | None = None, registered_by: str | None = None, agent_limit: int = 0) -> None:
        import re

        from .config import NAME_PATTERN

        if not re.fullmatch(NAME_PATTERN, node_id) or not re.fullmatch(NAME_PATTERN, name):
            raise ValueError("Invalid operator node identity")
        if type(cpu_millis) is not int or not 1 <= cpu_millis <= 1_000_000 or type(memory_mib) is not int or not 1 <= memory_mib <= 4_194_304:
            raise ValueError("Invalid operator node capacity")
        if type(agent_limit) is not int or not 0 <= agent_limit <= 1_000_000:
            raise ValueError("Invalid operator agent capacity")
        profiles = sorted(self.default_profiles) if profile_allowlist is None else profile_allowlist
        if not isinstance(profiles, list) or any(type(name) is not str or name not in self.configured_profiles for name in profiles) or len(profiles) != len(set(profiles)):
            raise ValueError("Invalid configured node profiles")
        if self.agent_profiles.intersection(profiles) and agent_limit <= 0:
            raise ValueError("Agent profiles require positive agent capacity")
        async with self.sf.begin() as session:
            # Existing budgets cannot be silently changed on re-registration.
            session.add(NodeRow(id=node_id, name=name, cpu_millis=cpu_millis, memory_mib=memory_mib, profile_allowlist=profiles, registered_by=registered_by, agent_limit=agent_limit))

    async def status(self, node_id: str) -> dict:
        from .persistence.models import ReservationRow

        async with self.sf() as session:
            node = await session.get(NodeRow, node_id)
            if node is None:
                raise ValueError("Unknown node")
            attempts = (await session.execute(select(AttemptRow.id, AttemptRow.job_id, AttemptRow.run_id, AttemptRow.state, AttemptRow.process_ref, AttemptRow.stopped_at).where(AttemptRow.node_id == node_id))).mappings().all()
            reservations = (
                (
                    await session.execute(
                        select(ReservationRow.attempt_id, ReservationRow.state, ReservationRow.cpu_millis, ReservationRow.memory_mib, ReservationRow.agent_units).where(ReservationRow.node_id == node_id, ReservationRow.state != "released")
                    )
                )
                .mappings()
                .all()
            )
            return {
                "node": {
                    "id": node.id,
                    "name": node.name,
                    "admin_state": node.admin_state,
                    "health": node.health,
                    "cpu_millis": node.cpu_millis,
                    "memory_mib": node.memory_mib,
                    "agent_limit": node.agent_limit,
                    "profile_allowlist": node.profile_allowlist,
                    "registered_by": node.registered_by,
                },
                "attempts": [dict(row) | {"stopped_at": row["stopped_at"].isoformat() if row["stopped_at"] else None} for row in attempts],
                "unreleased_reservations": [dict(row) for row in reservations],
            }

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
            from .persistence.models import CredentialRow

            # Issuance locks this same node, so deletion invalidates only this
            # safely disabled, history-free node's credentials atomically.
            await session.execute(delete(CredentialRow).where(CredentialRow.node_id == node_id))
            await session.delete(node)
