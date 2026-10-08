"""One SQL turn on the existing shared ledger, after execution locks and before nodes."""

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import case, func, select

from .persistence.models import JobRow, NodeRow, ReservationRow, SchedulingRow
from .protocol import JobSpec


@dataclass(frozen=True)
class QueueCandidate:
    kind: str
    key: str
    created_at: object
    profile: str
    node_ids: frozenset[str]


class SharedAdmissionPolicy:
    def __init__(self, config, *, agent_candidates=None):
        self.config = config
        self.agent_candidates = agent_candidates

    async def lock(self, session):
        turn = await session.get(SchedulingRow, "shared", with_for_update=True)
        if turn is None:
            raise ValueError("Shared scheduling migration required")
        nodes = (await session.scalars(select(NodeRow).order_by(NodeRow.id).with_for_update())).all()
        now = await session.scalar(select(func.clock_timestamp()))
        used = {node.id: (0, 0, 0) for node in nodes}
        for node_id, cpu, memory, agents in (
            await session.execute(
                select(ReservationRow.node_id, func.sum(ReservationRow.cpu_millis), func.sum(ReservationRow.memory_mib), func.sum(ReservationRow.agent_units)).where(ReservationRow.state != "released").group_by(ReservationRow.node_id)
            )
        ).all():
            used[node_id] = (cpu, memory, agents)
        agents = {node.id: (0, 0) for node in nodes}
        for node_id, cpu, memory in (
            await session.execute(
                select(ReservationRow.node_id, func.sum(case((ReservationRow.agent_units > 0, ReservationRow.cpu_millis), else_=0)), func.sum(case((ReservationRow.agent_units > 0, ReservationRow.memory_mib), else_=0)))
                .where(ReservationRow.state != "released")
                .group_by(ReservationRow.node_id)
            )
        ).all():
            agents[node_id] = (cpu, memory)
        return AdmissionWindow(self, session, turn, nodes, now, used, agents)


class AdmissionWindow:
    def __init__(self, policy, session, turn, nodes, now, used, agents):
        self.policy, self.session, self.turn, self.nodes, self.now, self.used = policy, session, turn, nodes, now, used
        self.agents = agents
        self.queues = None

    def live(self, node, kind, profile):
        return (
            node.session_id is not None
            and node.admin_state == "enabled"
            and node.health == "online"
            and node.last_seen_at is not None
            and node.last_seen_at + timedelta(seconds=self.policy.config.lease_seconds) > self.now
            and kind in (node.claim_kinds or [])
            and (node.profile_allowlist is None and kind == "job" or node.profile_allowlist is not None and profile in node.profile_allowlist)
            and (kind != "agent" or node.agent_limit > 0)
        )

    def fits(self, node, profile, *, extra=(0, 0, 0)):
        used = self.used[node.id]
        return used[0] + extra[0] + profile.cpu_millis <= node.cpu_millis and used[1] + extra[1] + profile.memory_mib <= node.memory_mib and used[2] + extra[2] + (profile.kind == "agent") <= node.agent_limit

    def capacity(self, node, profile):
        if self.policy.config.scheduling_mode == "serial" and any(any(value) for value in self.used.values()):
            return False
        if not self.fits(node, profile):
            return False
        if profile.kind == "agent" and self.policy.config.scheduling_mode == "reserved":
            name = self.policy.config.reserved_job_profile
            standard = self.policy.config.profiles[name]
            return any(
                self.live(other, "job", name)
                and other.cpu_millis - self.agents[other.id][0] - (profile.cpu_millis if other.id == node.id else 0) >= standard.cpu_millis
                and other.memory_mib - self.agents[other.id][1] - (profile.memory_mib if other.id == node.id else 0) >= standard.memory_mib
                for other in self.nodes
            )
        return True

    async def candidates(self):
        if self.queues is not None:
            return self.queues
        cfg = self.policy.config
        jobs = []
        # Keyset pages are read-only: never take another execution lock under
        # the turn/node locks, and incompatible first pages cannot hide work.
        cursor = None
        while True:
            query = select(JobRow).where(JobRow.state == "queued", JobRow.cancel_requested_at.is_(None), JobRow.queue_deadline > self.now).order_by(JobRow.queued_at, JobRow.id).limit(64)
            if cursor is not None:
                from sqlalchemy import tuple_

                query = query.where(tuple_(JobRow.queued_at, JobRow.id) > cursor)
            page = (await self.session.scalars(query)).all()
            for job in page:
                try:
                    spec = JobSpec.model_validate(job.spec)
                except ValueError:
                    continue
                profile = cfg.profiles.get(spec.profile)
                if profile is None or profile.kind != "job":
                    continue
                ids = frozenset(node.id for node in self.nodes if self.live(node, "job", spec.profile) and self.capacity(node, profile))
                if ids:
                    jobs.append(QueueCandidate("job", job.id, job.queued_at, spec.profile, ids))
            if len(page) < 64:
                break
            cursor = (page[-1].queued_at, page[-1].id)
        agents = await self.policy.agent_candidates(self.session, self) if self.policy.agent_candidates else []
        self.queues = {"job": sorted(jobs, key=lambda row: (row.created_at, row.key)), "agent": sorted(agents, key=lambda row: (row.created_at, row.key))}
        return self.queues

    async def permits(self, kind, key, node, profile, *, candidate=None):
        self.now = await self.session.scalar(select(func.clock_timestamp()))
        if not self.live(node, kind, candidate.profile if candidate else next(name for name, value in self.policy.config.profiles.items() if value is profile)) or not self.capacity(node, profile):
            return False
        queues = await self.candidates()
        own = list(queues[kind])
        if candidate is not None and all(row.key != candidate.key for row in own):
            own.append(candidate)
            own.sort(key=lambda row: (row.created_at, row.key))
        if not own or own[0].key != key or node.id not in own[0].node_ids:
            return False
        other = "agent" if kind == "job" else "job"
        return self.turn.next_kind == kind or not queues[other]

    def reserved(self, kind):
        self.turn.next_kind = "agent" if kind == "job" else "job"


async def queued_keys(session_factory, query, columns):
    """Bounded unlocked discovery; every caller locks its execution afterwards."""
    cursor = None
    from sqlalchemy import tuple_

    while True:
        page_query = query.order_by(*columns).limit(64)
        if cursor is not None:
            page_query = page_query.where(tuple_(*columns) > cursor)
        async with session_factory() as session:
            rows = (await session.execute(page_query)).all()
        for row in rows:
            yield row
        if len(rows) < 64:
            break
        cursor = tuple(rows[-1][-len(columns) :])
