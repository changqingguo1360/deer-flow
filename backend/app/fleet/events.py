"""Trusted host adapters for committed remote streams.

Private execution authority stays in the host; harness event stores only know
about their neutral same-transaction participant.
"""

from dataclasses import dataclass

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert

from deerflow.runtime.events.catalog import STREAM_FRAME_EVENT
from deerflow.runtime.execution.mutation_context import OwnershipRejected, current_remote_mutation_context, validate_mutation, validate_mutation_after_sql
from deerflow.runtime.stream_bridge.base import StreamBridge


@dataclass(frozen=True)
class RemoteStreamIdentity:
    run_id: str
    thread_id: str
    user_id: str
    attempt_id: str
    generation: int
    launch_spec_digest: str

    @classmethod
    def from_context(cls, context):
        return cls(**{name: getattr(context, name) for name in cls.__dataclass_fields__})


class FleetEventParticipant:
    def __init__(self, *, identity, spec, capability, outbox):
        self.identity = identity
        self.spec = spec
        self.capability = capability
        self.outbox = outbox

    async def sequence_floor(self, session, *, thread_id):
        from deerflow_ecs_fleet.persistence.models import EventOutboxRow

        if current_remote_mutation_context() != self.capability.context or thread_id != self.identity.thread_id:
            raise OwnershipRejected("Remote stream sequence requires its original scope")
        return await session.scalar(select(func.max(EventOutboxRow.seq)).where(EventOutboxRow.thread_id == thread_id)) or 0

    async def insert(self, session, *, event_id, record):
        if record["event_type"] != STREAM_FRAME_EVENT.event_type:
            return
        context = self.capability.context
        if current_remote_mutation_context() != context or RemoteStreamIdentity.from_context(context) != self.identity:
            raise OwnershipRejected("Remote stream requires its original mutation context")
        if (record["run_id"], record["thread_id"], record["user_id"], record["category"]) != (self.identity.run_id, self.identity.thread_id, self.identity.user_id, STREAM_FRAME_EVENT.category):
            raise OwnershipRejected("Remote stream event identity rejected")
        if self.spec.payload_digest() != self.identity.launch_spec_digest:
            raise OwnershipRejected("Remote stream launch specification rejected")
        await self.outbox.insert(session, identity=self.identity, event_id=event_id, seq=record["seq"])


TERMINAL_RESULTS = {"success": "succeeded", "error": "failed", "interrupted": "cancelled", "timeout": "timed_out"}


class FleetStreamSeals:
    def __init__(self, session_factory):
        self.sf = session_factory

    @staticmethod
    async def _insert(session, *, identity, status, source):
        from deerflow_ecs_fleet.persistence.models import EventOutboxRow, StreamSealRow

        last_seq = await session.scalar(select(func.max(EventOutboxRow.seq)).where(EventOutboxRow.run_id == identity.run_id, EventOutboxRow.attempt_id == identity.attempt_id))
        expected = {name: getattr(identity, name) for name in identity.__dataclass_fields__}
        expected.update(last_seq=last_seq or 0, core_status=status)
        await session.execute(
            insert(StreamSealRow)
            .values(
                **expected,
                source=source,
            )
            .on_conflict_do_nothing(index_elements=[StreamSealRow.run_id, StreamSealRow.attempt_id])
        )
        # Writer and physical-stop recovery may repeat the same receipt. Keep
        # the first source, but never silently accept a conflicting closure.
        seal = await session.scalar(
            select(StreamSealRow)
            .where(
                StreamSealRow.run_id == identity.run_id,
                StreamSealRow.attempt_id == identity.attempt_id,
            )
            .with_for_update()
        )
        if seal is None or any(getattr(seal, name) != value for name, value in expected.items()):
            raise OwnershipRejected("Remote stream seal conflict")
        return seal

    async def writer_seal(self, *, identity, spec, capability, original_record):
        def closure():
            status = getattr(original_record.status, "value", original_record.status)
            if (
                (original_record.run_id, original_record.thread_id, original_record.user_id, original_record.owner_worker_id) != (identity.run_id, identity.thread_id, identity.user_id, capability.context.owner_worker_id)
                or original_record.store_only
                or original_record.finalizing
                or original_record.ownership_lost
                or status not in TERMINAL_RESULTS
            ):
                raise OwnershipRejected("Original remote stream has not completed closure")
            if RemoteStreamIdentity.from_context(capability.context) != identity or spec.payload_digest() != identity.launch_spec_digest:
                raise OwnershipRejected("Remote stream seal identity rejected")
            return status

        status = closure()
        target = dict(run_id=identity.run_id, thread_id=identity.thread_id, user_id=identity.user_id, status=status)
        async with self.sf.begin() as session:
            await validate_mutation(capability, session, "stream.seal", **target)
            seal = await self._insert(session, identity=identity, status=status, source="writer")
            await validate_mutation_after_sql(capability, session, "stream.seal", **target)
            if closure() != status:
                raise OwnershipRejected("Original stream terminal result changed")
        return seal

    async def recover_locked(self, session, *, run, placement, attempt, reservation):
        from deerflow_ecs_fleet.persistence.models import LaunchSpecRow

        result = TERMINAL_RESULTS.get(run.status)
        expected_attempt = "expired" if result == "timed_out" else result
        if (
            result is None
            or placement.state != result
            or attempt.state != expected_attempt
            or placement.active_attempt_id != attempt.id
            or attempt.run_id != run.run_id
            or attempt.kind != "agent"
            or reservation.attempt_id != attempt.id
            or reservation.node_id != attempt.node_id
            or placement.node_id != attempt.node_id
            or (placement.user_id, placement.thread_id) != (run.user_id, run.thread_id)
            or attempt.stopped_at is None
            or reservation.state != "released"
            or reservation.released_at is None
        ):
            return False
        from deerflow_ecs_fleet.persistence.models import AgentTaskRow
        from deerflow_ecs_fleet.persistence.workspace_points import accepted_final

        task = await session.get(AgentTaskRow, placement.agent_task_id)
        point = await accepted_final(session, task=task, run=run, placement=placement, attempt=attempt) if task else None
        if point is None or task.state != point.desired_task_status or placement.state != point.desired_placement_status:
            return False
        spec = await session.get(LaunchSpecRow, placement.launch_spec_ref)
        if (
            spec is None
            or (spec.run_id, spec.agent_task_id, spec.generation, spec.user_id, spec.thread_id) != (run.run_id, placement.agent_task_id, placement.generation, run.user_id, run.thread_id)
            or (attempt.launch_spec or {}).get("launch_spec") != spec.payload
        ):
            return False
        from deerflow_ecs_fleet.launch_spec import LaunchSpec

        if LaunchSpec.model_validate(spec.payload).payload_digest() != spec.payload_digest:
            return False
        identity = RemoteStreamIdentity(run_id=run.run_id, thread_id=run.thread_id, user_id=run.user_id, attempt_id=attempt.id, generation=placement.generation, launch_spec_digest=spec.payload_digest)
        await self._insert(session, identity=identity, status=run.status, source="physical_stop")
        return True

    async def recover_accepted_batch(self, *, limit=64):
        from deerflow_ecs_fleet.persistence.models import AgentTaskRow, AttemptRow, NodeRow, ReservationRow, RunPlacementRow, StreamSealRow

        from deerflow.persistence.run.model import RunRow

        async with self.sf() as session:
            candidates = (
                await session.execute(
                    select(RunPlacementRow.run_id, RunPlacementRow.agent_task_id)
                    .outerjoin(StreamSealRow, (StreamSealRow.run_id == RunPlacementRow.run_id) & (StreamSealRow.attempt_id == RunPlacementRow.active_attempt_id))
                    .where(RunPlacementRow.state.in_(list(TERMINAL_RESULTS.values())), StreamSealRow.run_id.is_(None))
                    .order_by(RunPlacementRow.run_id)
                    .limit(limit)
                )
            ).all()
        recovered = 0
        for run_id, task_id in candidates:
            async with self.sf.begin() as session:
                task = await session.get(AgentTaskRow, task_id, with_for_update=True)
                run = await session.get(RunRow, run_id, with_for_update=True)
                placement = await session.get(RunPlacementRow, run_id, with_for_update=True)
                if task is None or run is None or placement is None or placement.node_id is None or placement.active_attempt_id is None:
                    continue
                node = await session.get(NodeRow, placement.node_id, with_for_update=True)
                reservation = await session.scalar(select(ReservationRow).where(ReservationRow.attempt_id == placement.active_attempt_id).with_for_update())
                attempt = await session.get(AttemptRow, placement.active_attempt_id, with_for_update=True)
                if node is None or reservation is None or attempt is None:
                    continue
                recovered += bool(await self.recover_locked(session, run=run, placement=placement, attempt=attempt, reservation=reservation))
        return recovered


class FleetProducerBridge(StreamBridge):
    def __init__(self, *, event_store, identity, spec, capability, seals, manager):
        super().__init__()
        self.event_store = event_store
        self.identity = identity
        self.spec = spec
        self.capability = capability
        self.seals = seals
        self.manager = manager
        self._original_record = None

    async def _record(self, run_id):
        if run_id != self.identity.run_id:
            raise OwnershipRejected("Remote stream run identity rejected")
        record = await self.manager.get(run_id, raise_on_store_error=True)
        if record is None or record.store_only or (record.thread_id, record.user_id, record.owner_worker_id) != (self.identity.thread_id, self.identity.user_id, self.capability.context.owner_worker_id):
            raise OwnershipRejected("Remote stream requires the original executor record")
        if self._original_record is None:
            self._original_record = record
        if record is not self._original_record or record.ownership_lost:
            raise OwnershipRejected("Remote stream original executor lost ownership")
        return record

    async def publish(self, run_id, event, data):
        await self._record(run_id)
        if not isinstance(event, str) or not event:
            raise ValueError("Remote stream requires an event name")
        envelope = {"event": event, "data": data}
        if event.split("|", 1)[0] in {"debug", "events"} and self.event_store.serialized_content_size(envelope) > self.event_store.max_trace_content:
            raise ValueError("Remote trace stream frame exceeds max_trace_content")
        await self.event_store.put(
            run_id=run_id,
            thread_id=self.identity.thread_id,
            event_type=STREAM_FRAME_EVENT.event_type,
            category=STREAM_FRAME_EVENT.category,
            content=envelope,
            metadata={"stream_version": 1},
        )

    async def publish_end(self, run_id):
        record = await self._record(run_id)
        await self.seals.writer_seal(identity=self.identity, spec=self.spec, capability=self.capability, original_record=record)

    async def cleanup(self, run_id, *, delay=0):
        # Durable transport history is owned by the existing event retention policy.
        return None

    def subscribe(self, run_id, *, last_event_id=None, heartbeat_interval=None):
        raise RuntimeError("Remote execution bridge cannot subscribe")


@dataclass(frozen=True)
class PreparedRemoteSubscription:
    identity: RemoteStreamIdentity | None
    after_seq: int
    expected_user_id: str | None = None
    expected_thread_id: str | None = None


class InvalidRemoteCursor(ValueError):
    pass


class RemoteHistoryUnavailable(ValueError):
    pass


def remote_cursor(identity, seq):
    import base64

    def encode(value):
        return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")

    return f"fleet.v1.{encode(identity.run_id)}.{encode(identity.attempt_id)}.{seq}"


def _finishing_read_predicate(*, placement="fleet_run_placements", attempt="fleet_attempts", run="runs", spec="fleet_launch_specs"):
    """Exact terminal pair retains reads while physical ownership stays held.

    Aliases are fixed internal query identifiers. This predicate grants no writes
    and changes no accepted historical or uncertain-attempt read behavior.
    """
    return f"""
        ({placement}.state='finishing' AND {attempt}.state IN ('starting','running') AND EXISTS (
            SELECT 1 FROM fleet_agent_tasks AS finishing_task
            JOIN fleet_workspace_points AS finishing_point ON finishing_point.id=finishing_task.accepted_workspace_point_id
            JOIN fleet_workspace_requests AS finishing_request ON finishing_request.id=finishing_point.request_id
            WHERE finishing_task.id={placement}.agent_task_id
              AND finishing_task.state='finishing'
              AND finishing_task.current_run_id={run}.run_id
              AND finishing_task.generation={placement}.generation
              AND {run}.kwargs_json->>'execution_backend'='fleet'
              AND {spec}.id={placement}.launch_spec_ref
              AND {spec}.run_id={placement}.run_id
              AND {spec}.agent_task_id={placement}.agent_task_id
              AND {spec}.generation={placement}.generation
              AND {spec}.user_id={placement}.user_id
              AND {spec}.thread_id={placement}.thread_id
              AND finishing_point.id={placement}.final_workspace_point_id
              AND finishing_point.kind IN ('final','paused')
              AND finishing_point.run_id={run}.run_id
              AND finishing_point.user_id={run}.user_id
              AND finishing_point.thread_id={run}.thread_id
              AND finishing_point.agent_task_id={placement}.agent_task_id
              AND finishing_point.generation={placement}.generation
              AND finishing_point.attempt_id={attempt}.id
              AND finishing_point.node_id={attempt}.node_id
              AND finishing_point.node_session_id={attempt}.node_session_id
              AND finishing_point.token_stamp={attempt}.token_hash
              AND finishing_point.process_ref={attempt}.process_ref
              AND finishing_point.owner_worker_id={run}.owner_worker_id
              AND finishing_point.launch_spec_digest={spec}.payload_digest
              AND {attempt}.launch_spec->'launch_spec'={spec}.payload
              AND finishing_point.desired_core_status={run}.status
              AND finishing_point.error IS NOT DISTINCT FROM {run}.error
              AND finishing_point.stop_reason IS NOT DISTINCT FROM {run}.stop_reason
              AND finishing_request.state='accepted'
              AND finishing_request.candidate_manifest_id=finishing_point.manifest_id
              AND finishing_request.request_digest=finishing_point.request_digest
              AND finishing_request.barrier_epoch>0
              AND finishing_point.checkpoint_id=(
                  SELECT finishing_root.checkpoint_id FROM checkpoints AS finishing_root
                  WHERE finishing_root.thread_id={run}.thread_id AND finishing_root.checkpoint_ns=''
                  ORDER BY finishing_root.checkpoint_id DESC LIMIT 1
              )
              AND {run}.run_id=(
                  SELECT finishing_root.metadata->>'deerflow_execution_run_id' FROM checkpoints AS finishing_root
                  WHERE finishing_root.thread_id={run}.thread_id AND finishing_root.checkpoint_ns=''
                  ORDER BY finishing_root.checkpoint_id DESC LIMIT 1
              )
        ))
    """


class FleetStreamReader:
    def __init__(self, session_factory):
        self.sf = session_factory

    async def is_remote(self, run_id):
        from deerflow_ecs_fleet.persistence.models import RunPlacementRow

        async with self.sf() as session:
            return await session.get(RunPlacementRow, run_id) is not None

    async def identity(self, run_id):
        from sqlalchemy import text

        async with self.sf() as session:
            row = (
                (
                    await session.execute(
                        text(f"""
                SELECT p.run_id,p.thread_id,p.user_id,p.generation,
                       p.active_attempt_id AS attempt_id,s.payload_digest AS launch_spec_digest,
                       s.payload,p.state AS placement_state,a.state AS attempt_state,r.status,
                       a.launch_spec,{_finishing_read_predicate(placement="p", attempt="a", run="r", spec="s")} AS finishing_accepted
                FROM fleet_run_placements p
                JOIN runs r ON r.run_id=p.run_id AND r.user_id=p.user_id AND r.thread_id=p.thread_id
                JOIN fleet_launch_specs s ON s.id=p.launch_spec_ref AND s.run_id=p.run_id
                     AND s.agent_task_id=p.agent_task_id AND s.generation=p.generation
                     AND s.user_id=p.user_id AND s.thread_id=p.thread_id
                JOIN fleet_attempts a ON a.id=p.active_attempt_id AND a.run_id=p.run_id
                     AND a.kind='agent' AND a.node_id=p.node_id
                WHERE p.run_id=:run AND r.kwargs_json->>'execution_backend'='fleet'
            """),
                        {"run": run_id},
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            return None
        result = TERMINAL_RESULTS.get(row["status"])
        accepted = result is not None and row["placement_state"] == result and row["attempt_state"] == ("expired" if result == "timed_out" else result)
        active = row["placement_state"] in {"claimed", "running"} and row["attempt_state"] in {"claimed", "starting", "running"}
        uncertain = row["placement_state"] == "unknown" and row["attempt_state"] in {"unknown", "quarantined"}
        if not (active or accepted or uncertain or row["finishing_accepted"]) or (row["launch_spec"] or {}).get("launch_spec") != row["payload"]:
            return None
        from deerflow_ecs_fleet.launch_spec import LaunchSpec

        if LaunchSpec.model_validate(row["payload"]).payload_digest() != row["launch_spec_digest"]:
            return None
        return RemoteStreamIdentity(**{name: row[name] for name in RemoteStreamIdentity.__dataclass_fields__})

    async def unassigned_stream_state(self, run_id, prepared):
        """Prove an empty committed original stream without fabricating Attempt."""
        from deerflow_ecs_fleet.persistence.models import AttemptRow, EventOutboxRow, LaunchSpecRow, RunPlacementRow
        from sqlalchemy import exists

        from deerflow.persistence.models.run_event import RunEventRow
        from deerflow.persistence.run.model import RunRow

        if prepared.expected_user_id is None or prepared.expected_thread_id is None:
            return None  # Only a prepared owned subscription has empty-stream authority.
        placement, run, spec = RunPlacementRow, RunRow, LaunchSpecRow
        query = (
            select(placement.state, run.status)
            .join(run, and_(run.run_id == placement.run_id, run.user_id == placement.user_id, run.thread_id == placement.thread_id))
            .join(
                spec,
                and_(
                    spec.id == placement.launch_spec_ref,
                    spec.run_id == placement.run_id,
                    spec.agent_task_id == placement.agent_task_id,
                    spec.generation == placement.generation,
                    spec.user_id == placement.user_id,
                    spec.thread_id == placement.thread_id,
                ),
            )
            .where(
                placement.run_id == run_id,
                placement.user_id == prepared.expected_user_id,
                placement.thread_id == prepared.expected_thread_id,
                placement.active_attempt_id.is_(None),
                run.kwargs_json["execution_backend"].as_string() == "fleet",
                ~exists(select(AttemptRow.id).where(AttemptRow.kind == "agent", AttemptRow.run_id == run_id)),
                ~exists(select(EventOutboxRow.seq).where(EventOutboxRow.run_id == run_id)),
                ~exists(select(RunEventRow.id).where(RunEventRow.run_id == run_id, RunEventRow.event_type == STREAM_FRAME_EVENT.event_type, RunEventRow.category == STREAM_FRAME_EVENT.category)),
            )
        )
        async with self.sf() as session:
            row = (await session.execute(query)).first()
        if row is None:
            return None
        if row == ("queued", "pending"):
            return "queued"
        if TERMINAL_RESULTS.get(row.status) == row.state:
            return "terminal"
        return None

    async def prepare(self, record, cursor):
        from deerflow_ecs_fleet.persistence.models import RunPlacementRow

        from deerflow.persistence.run.model import RunRow

        async with self.sf() as session:
            owner = (
                await session.execute(select(RunPlacementRow.user_id, RunPlacementRow.thread_id, RunRow.user_id, RunRow.thread_id).join(RunRow, RunRow.run_id == RunPlacementRow.run_id).where(RunPlacementRow.run_id == record.run_id))
            ).first()
        if owner is None or tuple(owner) != (record.user_id, record.thread_id, record.user_id, record.thread_id):
            raise InvalidRemoteCursor("Remote stream owner mismatch")
        identity = await self.identity(record.run_id)
        if identity is not None and (identity.user_id, identity.thread_id) != (record.user_id, record.thread_id):
            raise InvalidRemoteCursor("Remote stream owner mismatch")
        if cursor is None:
            prepared = PreparedRemoteSubscription(identity, 0, record.user_id, record.thread_id)
            await self.ensure_history_available(prepared)
            return prepared
        if not isinstance(cursor, str) or len(cursor) > 1024 or identity is None:
            raise InvalidRemoteCursor("Invalid remote stream cursor")
        parts = cursor.split(".")
        if len(parts) != 5 or parts[:2] != ["fleet", "v1"] or not parts[4].isascii() or not parts[4].isdecimal() or parts[4].startswith("0"):
            raise InvalidRemoteCursor("Invalid remote stream cursor")
        seq = int(parts[4])
        if remote_cursor(identity, seq) != cursor or seq > 9223372036854775807:
            raise InvalidRemoteCursor("Foreign or malformed remote stream cursor")
        rows = await self._committed_page(PreparedRemoteSubscription(identity, seq - 1), limit=1)
        if not rows or rows[0]["seq"] != seq:
            raise InvalidRemoteCursor("Remote stream cursor is not an accepted committed frame")
        prepared = PreparedRemoteSubscription(identity, seq, record.user_id, record.thread_id)
        await self.ensure_history_available(prepared)
        return prepared

    @staticmethod
    def _host_frame_matches(pointer):
        from deerflow.persistence.models.run_event import RunEventRow

        return and_(
            RunEventRow.id == pointer.event_id,
            RunEventRow.run_id == pointer.run_id,
            RunEventRow.thread_id == pointer.thread_id,
            RunEventRow.user_id == pointer.user_id,
            RunEventRow.seq == pointer.seq,
            RunEventRow.event_type == STREAM_FRAME_EVENT.event_type,
            RunEventRow.category == STREAM_FRAME_EVENT.category,
        )

    @staticmethod
    def _original_pointer_query(identity):
        from deerflow_ecs_fleet.persistence.models import AttemptRow, EventOutboxRow, LaunchSpecRow, RunPlacementRow

        from deerflow.persistence.run.model import RunRow

        # Read authority keeps its query-time original mapping/core guards.
        # Task-current generation is deliberately not a historical-read guard.
        return (
            select(EventOutboxRow)
            .join(RunPlacementRow, RunPlacementRow.run_id == EventOutboxRow.run_id)
            .join(AttemptRow, AttemptRow.id == RunPlacementRow.active_attempt_id)
            .join(LaunchSpecRow, LaunchSpecRow.id == RunPlacementRow.launch_spec_ref)
            .join(RunRow, and_(RunRow.run_id == RunPlacementRow.run_id, RunRow.user_id == RunPlacementRow.user_id, RunRow.thread_id == RunPlacementRow.thread_id))
            .where(
                *(getattr(EventOutboxRow, name) == getattr(identity, name) for name in RemoteStreamIdentity.__dataclass_fields__),
                RunPlacementRow.active_attempt_id == identity.attempt_id,
                RunPlacementRow.generation == identity.generation,
                RunPlacementRow.user_id == identity.user_id,
                RunPlacementRow.thread_id == identity.thread_id,
                LaunchSpecRow.payload_digest == identity.launch_spec_digest,
                AttemptRow.run_id == identity.run_id,
                AttemptRow.kind == "agent",
                AttemptRow.node_id == RunPlacementRow.node_id,
                RunRow.kwargs_json["execution_backend"].as_string() == "fleet",
                or_(
                    and_(RunPlacementRow.state.in_(["claimed", "running"]), AttemptRow.state.in_(["claimed", "starting", "running"])),
                    text(_finishing_read_predicate()),
                    and_(RunPlacementRow.state == "unknown", AttemptRow.state.in_(["unknown", "quarantined"])),
                    *(and_(RunRow.status == status, RunPlacementRow.state == result, AttemptRow.state == ("expired" if result == "timed_out" else result)) for status, result in TERMINAL_RESULTS.items()),
                ),
            )
        )

    async def ensure_history_available(self, prepared, *, through_seq=None, expected_seq=None):
        from deerflow_ecs_fleet.persistence.models import EventOutboxRow

        from deerflow.persistence.models.run_event import RunEventRow

        identity = prepared.identity
        if identity is None:
            return True
        if expected_seq is not None:
            # One indexed original pointer rechecks the accepted mapping/core
            # authority before a cached frame can be emitted. No payload/hash
            # hydration and no scan of future history is needed for this fence.
            async with self.sf() as session:
                accepted = await session.scalar(self._original_pointer_query(identity).with_only_columns(EventOutboxRow.seq).where(EventOutboxRow.seq == expected_seq).limit(1))
            if accepted is None:
                return False
        # Unbounded suffix validation is only the once-per-request preheader
        # check. Runtime pre-yield calls constrain the already fetched pointer
        # window (at most128); semantic sequence gaps do not widen that window.
        query = self._original_pointer_query(identity).with_only_columns(EventOutboxRow.seq).outerjoin(RunEventRow, self._host_frame_matches(EventOutboxRow)).where(EventOutboxRow.seq > prepared.after_seq, RunEventRow.id.is_(None))
        if through_seq is not None:
            query = query.where(EventOutboxRow.seq <= through_seq)
        async with self.sf() as session:
            missing = await session.scalar(query.limit(1))
        if missing is not None:
            raise RemoteHistoryUnavailable("Remote stream history is unavailable")
        return True

    async def page(self, prepared, *, limit=128):
        from deerflow_ecs_fleet.persistence.models import EventOutboxRow
        from sqlalchemy.orm import aliased

        from deerflow.persistence.models.run_event import RunEventRow
        from deerflow.runtime.events.store.db import DbRunEventStore

        identity = prepared.identity
        if identity is None or await self.identity(identity.run_id) != identity:
            return None
        # Limit original pointers first. A missing host row must occupy its
        # pointer slot, rather than being hidden by inner-join pagination.
        window = self._original_pointer_query(identity).where(EventOutboxRow.seq > prepared.after_seq).order_by(EventOutboxRow.seq).limit(min(limit, 128)).subquery()
        pointer = aliased(EventOutboxRow, window)
        async with self.sf() as session:
            rows = (await session.execute(select(RunEventRow, pointer.seq).select_from(pointer).outerjoin(RunEventRow, self._host_frame_matches(pointer)).order_by(pointer.seq))).all()
        if await self.identity(identity.run_id) != identity:
            return None
        if any(row is None for row, _ in rows):
            raise RemoteHistoryUnavailable("Remote stream history is unavailable")
        return [DbRunEventStore._row_to_dict(row) for row, _ in rows]

    async def _committed_page(self, prepared, *, limit=128):
        from deerflow_ecs_fleet.persistence.models import AttemptRow, EventOutboxRow, LaunchSpecRow, RunPlacementRow

        from deerflow.persistence.models.run_event import RunEventRow
        from deerflow.runtime.events.store.db import DbRunEventStore

        identity = prepared.identity
        if identity is None or await self.identity(identity.run_id) != identity:
            return None
        async with self.sf() as session:
            rows = (
                (
                    await session.execute(
                        select(RunEventRow)
                        .join(EventOutboxRow, EventOutboxRow.event_id == RunEventRow.id)
                        .join(RunPlacementRow, RunPlacementRow.run_id == EventOutboxRow.run_id)
                        .join(AttemptRow, AttemptRow.id == RunPlacementRow.active_attempt_id)
                        .join(LaunchSpecRow, LaunchSpecRow.id == RunPlacementRow.launch_spec_ref)
                        .where(
                            RunPlacementRow.active_attempt_id == identity.attempt_id,
                            RunPlacementRow.generation == identity.generation,
                            RunPlacementRow.user_id == identity.user_id,
                            RunPlacementRow.thread_id == identity.thread_id,
                            LaunchSpecRow.payload_digest == identity.launch_spec_digest,
                            AttemptRow.run_id == identity.run_id,
                            EventOutboxRow.run_id == identity.run_id,
                            EventOutboxRow.attempt_id == identity.attempt_id,
                            EventOutboxRow.generation == identity.generation,
                            EventOutboxRow.user_id == identity.user_id,
                            EventOutboxRow.thread_id == identity.thread_id,
                            EventOutboxRow.launch_spec_digest == identity.launch_spec_digest,
                            RunEventRow.run_id == identity.run_id,
                            RunEventRow.thread_id == identity.thread_id,
                            RunEventRow.user_id == identity.user_id,
                            RunEventRow.seq == EventOutboxRow.seq,
                            RunEventRow.event_type == STREAM_FRAME_EVENT.event_type,
                            RunEventRow.category == STREAM_FRAME_EVENT.category,
                            RunEventRow.seq > prepared.after_seq,
                        )
                        .order_by(RunEventRow.seq)
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )
            records = [DbRunEventStore._row_to_dict(row) for row in rows]
        if await self.identity(identity.run_id) != identity:
            return None
        return records

    async def seal(self, identity):
        from deerflow_ecs_fleet.persistence.models import AttemptRow, LaunchSpecRow, RunPlacementRow, StreamSealRow

        from deerflow.persistence.run.model import RunRow

        if await self.identity(identity.run_id) != identity:
            return None
        async with self.sf() as session:
            return await session.scalar(
                select(StreamSealRow.last_seq)
                .join(RunPlacementRow, RunPlacementRow.run_id == StreamSealRow.run_id)
                .join(AttemptRow, AttemptRow.id == RunPlacementRow.active_attempt_id)
                .join(RunRow, RunRow.run_id == StreamSealRow.run_id)
                .join(LaunchSpecRow, LaunchSpecRow.id == RunPlacementRow.launch_spec_ref)
                .where(
                    RunRow.status == StreamSealRow.core_status,
                    RunRow.user_id == identity.user_id,
                    RunRow.thread_id == identity.thread_id,
                    RunPlacementRow.generation == identity.generation,
                    RunPlacementRow.user_id == identity.user_id,
                    RunPlacementRow.thread_id == identity.thread_id,
                    AttemptRow.kind == "agent",
                    AttemptRow.run_id == identity.run_id,
                    AttemptRow.node_id == RunPlacementRow.node_id,
                    RunPlacementRow.active_attempt_id == identity.attempt_id,
                    RunPlacementRow.state != "unknown",
                    AttemptRow.state.not_in(["unknown", "quarantined"]),
                    or_(
                        RunPlacementRow.state != "finishing",
                        and_(text(_finishing_read_predicate()), LaunchSpecRow.payload_digest == StreamSealRow.launch_spec_digest),
                    ),
                    StreamSealRow.run_id == identity.run_id,
                    StreamSealRow.attempt_id == identity.attempt_id,
                    StreamSealRow.generation == identity.generation,
                    StreamSealRow.thread_id == identity.thread_id,
                    StreamSealRow.user_id == identity.user_id,
                    StreamSealRow.launch_spec_digest == identity.launch_spec_digest,
                )
            )

    async def candidate_pointers(self, session, *, limit=64, run_id=None):
        from deerflow_ecs_fleet.persistence.models import AttemptRow, EventOutboxRow, LaunchSpecRow, RunPlacementRow

        from deerflow.persistence.models.run_event import RunEventRow
        from deerflow.persistence.run.model import RunRow

        # Source-side authority selection prevents an arbitrary number of stale
        # pointers from head-blocking an accepted attempt. Missing host events
        # are excluded here and never treated as successful Redis delivery.
        return (
            (
                await session.execute(
                    select(EventOutboxRow)
                    .join(RunPlacementRow, (RunPlacementRow.run_id == EventOutboxRow.run_id) & (RunPlacementRow.active_attempt_id == EventOutboxRow.attempt_id))
                    .join(AttemptRow, AttemptRow.id == EventOutboxRow.attempt_id)
                    .join(LaunchSpecRow, LaunchSpecRow.id == RunPlacementRow.launch_spec_ref)
                    .join(RunRow, RunRow.run_id == EventOutboxRow.run_id)
                    .join(RunEventRow, RunEventRow.id == EventOutboxRow.event_id)
                    .where(
                        EventOutboxRow.published_at.is_(None),
                        True if run_id is None else EventOutboxRow.run_id == run_id,
                        EventOutboxRow.generation == RunPlacementRow.generation,
                        EventOutboxRow.user_id == RunPlacementRow.user_id,
                        EventOutboxRow.thread_id == RunPlacementRow.thread_id,
                        EventOutboxRow.launch_spec_digest == LaunchSpecRow.payload_digest,
                        LaunchSpecRow.run_id == RunPlacementRow.run_id,
                        LaunchSpecRow.agent_task_id == RunPlacementRow.agent_task_id,
                        LaunchSpecRow.generation == RunPlacementRow.generation,
                        LaunchSpecRow.user_id == RunPlacementRow.user_id,
                        LaunchSpecRow.thread_id == RunPlacementRow.thread_id,
                        RunRow.user_id == EventOutboxRow.user_id,
                        RunRow.thread_id == EventOutboxRow.thread_id,
                        RunEventRow.run_id == EventOutboxRow.run_id,
                        RunEventRow.thread_id == EventOutboxRow.thread_id,
                        RunEventRow.user_id == EventOutboxRow.user_id,
                        RunEventRow.seq == EventOutboxRow.seq,
                        RunEventRow.event_type == STREAM_FRAME_EVENT.event_type,
                        RunEventRow.category == STREAM_FRAME_EVENT.category,
                        AttemptRow.kind == "agent",
                        AttemptRow.run_id == RunPlacementRow.run_id,
                        AttemptRow.node_id == RunPlacementRow.node_id,
                        text("runs.kwargs_json->>'execution_backend' = 'fleet'"),
                        text("fleet_attempts.launch_spec->'launch_spec' = fleet_launch_specs.payload"),
                        or_(
                            and_(RunPlacementRow.state.in_(["claimed", "running"]), AttemptRow.state.in_(["claimed", "starting", "running"])),
                            text(_finishing_read_predicate()),
                            and_(RunPlacementRow.state == "unknown", AttemptRow.state.in_(["unknown", "quarantined"])),
                            *(and_(RunRow.status == status, RunPlacementRow.state == result, AttemptRow.state == ("expired" if result == "timed_out" else result)) for status, result in TERMINAL_RESULTS.items()),
                        ),
                    )
                    .distinct(EventOutboxRow.run_id)
                    .order_by(EventOutboxRow.run_id, EventOutboxRow.seq)
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )

    async def load_committed(self, pointer):
        identity = await self.identity(pointer.run_id)
        if identity is None or any(getattr(pointer, name) != getattr(identity, name) for name in identity.__dataclass_fields__):
            return None
        rows = await self._committed_page(PreparedRemoteSubscription(identity, pointer.seq - 1), limit=1)
        return rows[0] if rows and rows[0]["seq"] == pointer.seq else None


class FleetGatewayBridge(StreamBridge):
    supports_cross_process = True

    def __init__(self, *, local_bridge, reader, publisher=None):
        super().__init__(heartbeat_interval=local_bridge.heartbeat_interval)
        self.local_bridge = local_bridge
        self.reader = reader
        self.publisher = publisher

    async def start(self):
        if self.publisher is not None:
            await self.publisher.start()

    async def close(self):
        if self.publisher is not None:
            await self.publisher.stop()
        client = getattr(self, "_publisher_redis", None)
        if client is not None:
            await client.aclose()

    async def is_remote(self, run_id):
        return await self.reader.is_remote(run_id)

    async def prepare(self, record, last_event_id):
        if await self.is_remote(record.run_id):
            try:
                return await self.reader.prepare(record, last_event_id)
            except RemoteHistoryUnavailable as exc:
                from fastapi import HTTPException

                raise HTTPException(status_code=410, detail=str(exc)) from exc
            except InvalidRemoteCursor as exc:
                from fastapi import HTTPException

                raise HTTPException(status_code=400, detail=str(exc)) from exc
        if record.store_only and not self.local_bridge.supports_cross_process:
            from fastapi import HTTPException

            raise HTTPException(status_code=409, detail=f"Run {record.run_id} is not active on this worker and cannot be streamed")
        return None

    async def stream_exists(self, run_id):
        if await self.is_remote(run_id):
            # Remote closure comes only from a durable seal, even after Redis loss.
            return True
        return await self.local_bridge.stream_exists(run_id)

    async def publish(self, run_id, event, data):
        if await self.is_remote(run_id):
            raise OwnershipRejected("Remote publication requires its original execution adapter")
        await self.local_bridge.publish(run_id, event, data)

    async def publish_end(self, run_id):
        if await self.is_remote(run_id):
            raise OwnershipRejected("Remote closure requires its original execution adapter")
        await self.local_bridge.publish_end(run_id)

    async def cleanup(self, run_id, *, delay=0):
        if not await self.is_remote(run_id):
            await self.local_bridge.cleanup(run_id, delay=delay)

    async def subscribe_prepared(self, run_id, prepared, *, heartbeat_interval=None):
        import asyncio
        from dataclasses import replace

        from deerflow.runtime.stream_bridge.base import END_SENTINEL, HEARTBEAT_SENTINEL, StreamEvent

        interval = self._resolve_heartbeat_interval(heartbeat_interval)
        next_heartbeat = asyncio.get_running_loop().time() + interval
        current = prepared
        while True:
            if current.identity is None:
                identity = await self.reader.identity(run_id)
                if identity is None:
                    state = await self.reader.unassigned_stream_state(run_id, current)
                    if state == "terminal":
                        yield END_SENTINEL
                        return
                    if state != "queued":
                        return  # Missing/inconsistent/history mappings never prove END.
                else:
                    if (current.expected_user_id is not None and identity.user_id != current.expected_user_id) or (current.expected_thread_id is not None and identity.thread_id != current.expected_thread_id):
                        return
                    current = replace(current, identity=identity)
            if current.identity is not None:
                try:
                    rows = await self.reader.page(current)
                    if rows is None:
                        return  # Accepted identity changed. A reconnect revalidates its cursor.
                    if rows:
                        through_seq = rows[-1]["seq"]
                        for row in rows:
                            # Check only this bounded prefetched pointer window;
                            # a future page is checked when it is fetched.
                            if not await self.reader.ensure_history_available(current, through_seq=through_seq, expected_seq=row["seq"]):
                                return  # Accepted original identity changed after prefetch.
                            content = row["content"]
                            yield StreamEvent(id=remote_cursor(current.identity, row["seq"]), event=content["event"], data=content["data"])
                            current = replace(current, after_seq=row["seq"])
                        continue
                    # Empty original pointer page proves there is no unconsumed
                    # missing slot. Only then may the accepted seal yield END.
                    last_seq = await self.reader.seal(current.identity)
                except RemoteHistoryUnavailable:
                    return  # No END; reconnect preflight reports unavailable history.
                if last_seq is not None and current.after_seq >= last_seq:
                    yield END_SENTINEL
                    return
            now = asyncio.get_running_loop().time()
            if now >= next_heartbeat:
                yield HEARTBEAT_SENTINEL
                next_heartbeat = now + interval
            await asyncio.sleep(min(0.25, max(0.01, next_heartbeat - now)))

    async def subscribe(self, run_id, *, last_event_id=None, heartbeat_interval=None):
        if not await self.is_remote(run_id):
            async for item in self.local_bridge.subscribe(run_id, last_event_id=last_event_id, heartbeat_interval=heartbeat_interval):
                yield item
            return
        identity = await self.reader.identity(run_id)
        if last_event_id is not None:
            # HTTP routes preflight before headers; this also protects non-HTTP callers.
            from types import SimpleNamespace

            if identity is None:
                raise InvalidRemoteCursor("Remote attempt is unavailable")
            prepared = await self.reader.prepare(SimpleNamespace(run_id=run_id, user_id=identity.user_id, thread_id=identity.thread_id), last_event_id)
        else:
            prepared = PreparedRemoteSubscription(identity, 0)
        async for item in self.subscribe_prepared(run_id, prepared, heartbeat_interval=heartbeat_interval):
            yield item


def install_fleet_events(app, session_factory):
    from app.fleet.runtime import fleet_runtime

    runtime = fleet_runtime(app)
    if runtime is None or not runtime.ready or session_factory is None:
        return None
    local = app.state.stream_bridge
    reader = FleetStreamReader(session_factory)
    client = None
    publisher = None
    if runtime.config.agents_enabled:
        from deerflow_ecs_fleet.event_bridge import CommittedEventPublisher

        if getattr(local, "_redis_url", None) is not None:
            from redis.asyncio import Redis

            client = Redis.from_url(local._redis_url, decode_responses=True, socket_connect_timeout=1.0, socket_timeout=1.0)
        publisher = CommittedEventPublisher(
            session_factory=session_factory,
            candidate_pointers=reader.candidate_pointers,
            load_committed=reader.load_committed,
            redis_client=client,
            key_prefix=getattr(local, "_key_prefix", "deerflow:stream_bridge") + ":fleet",
            recover_seals=FleetStreamSeals(session_factory).recover_accepted_batch,
        )
    bridge = FleetGatewayBridge(local_bridge=local, reader=reader, publisher=publisher)
    bridge._publisher_redis = client
    app.state.stream_bridge = bridge
    return bridge
