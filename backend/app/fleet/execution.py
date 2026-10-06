"""Host-owned Fleet admission adapter; never starts a runner or selects routing."""

import json
from datetime import timedelta
from uuid import uuid4

from deerflow_ecs_fleet.launch_spec import Snapshot, build_launch_spec
from deerflow_ecs_fleet.persistence.agent_tasks import AgentTasks
from deerflow_ecs_fleet.persistence.models import AGENT_TASK_ACTIVE, AgentTaskRow
from deerflow_ecs_fleet.persistence.placements import RunPlacements
from langchain_core.messages import BaseMessage, message_to_dict, messages_from_dict
from langgraph.types import Command
from sqlalchemy import select, text

from deerflow.runtime.execution.contracts import ExecutionPlan, RunExecutionParameters
from deerflow.runtime.runs.manager import ConflictError

INPUT_FORMAT = "deerflow-normalized-input-v1"


def encode_graph_input(value):
    """Preserve message fields and Command semantics in a versioned JSON envelope."""
    if isinstance(value, Command):
        encoded = {"format": INPUT_FORMAT, "kind": "command", "value": {"graph": value.graph, "update": value.update, "resume": value.resume, "goto": value.goto}}
    elif isinstance(value, dict):
        state = dict(value)
        if isinstance(state.get("messages"), list):
            state["messages"] = [message_to_dict(message) if isinstance(message, BaseMessage) else message for message in state["messages"]]
        encoded = {"format": INPUT_FORMAT, "kind": "state", "value": state}
    else:
        raise ValueError("Unsupported normalized Agent input")
    # Do not use a lossy str()/default serializer on any run parameter.
    return json.loads(json.dumps(encoded, allow_nan=False))


def decode_graph_input(value):
    if value.get("format") != INPUT_FORMAT or value.get("kind") not in {"command", "state"}:
        raise ValueError("Unsupported normalized Agent input format")
    if value["kind"] == "command":
        return Command(**value["value"])
    state = dict(value["value"])
    if isinstance(state.get("messages"), list):
        state["messages"] = [messages_from_dict([message])[0] if isinstance(message, dict) and "type" in message and "data" in message else message for message in state["messages"]]
    return state


class FleetExecutionBackend:
    def __init__(self, *, config, profile_name, model_name, model_version, skill_snapshot, plugin_snapshot, workspace_manifest_ref, secret_refs=(), continuation_budget=0):
        self.config = config
        self.profile_name = profile_name
        self.profile = config.profiles.get(profile_name)
        if not config.enabled or not config.agents_enabled or self.profile is None or self.profile.kind != "agent":
            raise ValueError("Remote admission requires an operator agent profile")
        if type(continuation_budget) is not int or continuation_budget < 0:
            raise ValueError("Invalid continuation budget")
        self.model_name = model_name
        self.model_version = model_version
        self.skill_snapshot = Snapshot.model_validate(skill_snapshot)
        self.plugin_snapshot = Snapshot.model_validate(plugin_snapshot)
        self.workspace_manifest_ref = workspace_manifest_ref
        self.secret_refs = secret_refs
        self.continuation_budget = continuation_budget

    def plan(self, parameters: RunExecutionParameters):
        if parameters.model_name is not None and parameters.model_name != self.model_name:
            raise ValueError("Run model conflicts with operator execution binding")
        participant = FleetRunAdmission(self, parameters)
        return ExecutionPlan(store_only=True, participant=participant, public_kwargs={"execution_backend": "fleet", "profile": self.profile_name, "schema_version": 1})


class FleetRunAdmission:
    def __init__(self, backend, parameters):
        self.backend = backend
        self.parameters = parameters
        self.task = None
        self.resume_point = None
        normalized_config = dict(parameters.normalized_config)
        normalized_context = dict(normalized_config.get("context") or {})
        # Fleet runtime identity comes from the authenticated admission owner,
        # including internal callers whose public context omits user_id.
        normalized_context["user_id"] = parameters.user_id
        normalized_config["context"] = normalized_context
        self.inputs = {
            "schema_version": 1,
            "user_id": parameters.user_id,
            "thread_id": parameters.thread_id,
            "assistant_id": parameters.assistant_id or "lead_agent",
            "model_name": backend.model_name,
            "model_version": backend.model_version,
            "input": encode_graph_input(parameters.graph_input),
            "normalized_config": normalized_config,
            "stream_modes": parameters.stream_modes,
            "stream_subgraphs": parameters.stream_subgraphs,
            "interrupt_before": parameters.interrupt_before,
            "interrupt_after": parameters.interrupt_after,
            "recursion_limit": parameters.normalized_config["recursion_limit"],
            "skill_snapshot": backend.skill_snapshot,
            "plugin_snapshot": backend.plugin_snapshot,
            "workspace_manifest_ref": backend.workspace_manifest_ref,
            "secret_refs": backend.secret_refs,
        }

    async def guard_thread(self, session, **kwargs):
        await fleet_thread_admission_guard(session, participant=self, **{name: value for name, value in kwargs.items() if name != "participant"})

    async def validate_paused_resume(self, session, task):
        from deerflow_ecs_fleet.persistence.attempts import AgentAttempts
        from deerflow_ecs_fleet.persistence.models import AttemptRow, RunPlacementRow
        from deerflow_ecs_fleet.persistence.workspace_points import accepted_final

        from deerflow.persistence.run.model import RunRow

        command = self.parameters.graph_input
        if not isinstance(command, Command) or not isinstance(command.resume, dict) or not command.resume or any(not isinstance(key, str) or not key for key in command.resume):
            raise ConflictError("Paused remote execution requires keyed graph input")
        if task.state not in {"paused", "input_required"} or task.cancel_requested_at is not None or task.current_run_id is None or task.continuation_budget < 0:
            raise ConflictError("Remote task cannot accept human input")
        location = await session.get(RunPlacementRow, task.current_run_id)
        if location is None or location.active_attempt_id is None:
            raise ConflictError("Paused remote execution has no original attempt")

        async def lock_run(current, run_id):
            return await current.get(RunRow, run_id, with_for_update=True)

        rows = await AgentAttempts().locked(session, location.active_attempt_id, run_locker=lock_run)
        original, run, placement, node, reservation, attempt = rows
        if any(value is None for value in rows) or original.id != task.id or (run.user_id, run.thread_id) != (self.parameters.user_id, self.parameters.thread_id):
            raise ConflictError("Paused remote execution identity conflicts")
        point = await accepted_final(session, task=task, run=run, placement=placement, attempt=attempt)
        selector = self.parameters.normalized_config.get("configurable", {}).get("checkpoint_id")
        now = await session.scalar(text("SELECT clock_timestamp()"))
        unresolved = await session.scalar(
            select(AttemptRow.id)
            .join(RunPlacementRow, RunPlacementRow.run_id == AttemptRow.run_id)
            .where(RunPlacementRow.agent_task_id == task.id, (AttemptRow.stopped_at.is_(None)) | AttemptRow.state.in_(["unknown", "quarantined"]))
            .limit(1)
        )
        if (
            point is None
            or point.kind != "paused"
            or point.desired_task_status != task.state
            or run.status != point.desired_core_status
            or placement.state != point.desired_placement_status
            or attempt.state != point.desired_placement_status
            or attempt.stopped_at is None
            or attempt.finished_at is None
            or attempt.process_ref != "fleet-" + attempt.id
            or point.node_session_id != attempt.node_session_id
            or reservation.state != "released"
            or reservation.released_at is None
            or unresolved is not None
            or task.deadline <= now
            or attempt.execution_deadline <= now
            or selector is not None
            and selector != point.checkpoint_id
        ):
            raise ConflictError("Paused remote execution requires its accepted stopped source")
        self.task, self.resume_point = task, point
        self.inputs["source_workspace_point_id"] = point.id
        self.inputs["source_workspace_checkpoint_id"] = point.checkpoint_id

    async def prepare(self, session):
        from deerflow_ecs_fleet.persistence.models import WorkspacePointRow, WorkspaceRequestRow

        from deerflow.persistence.run.model import ThreadExecutionBindingRow

        binding = await session.get(ThreadExecutionBindingRow, (self.parameters.user_id, self.parameters.thread_id))
        origin = binding.source_workspace if binding is not None else None
        selector = self.parameters.normalized_config.get("configurable", {}).get("checkpoint_id")
        source_thread = self.parameters.thread_id
        own_accepted = await session.scalar(
            select(WorkspacePointRow)
            .join(AgentTaskRow, AgentTaskRow.accepted_workspace_point_id == WorkspacePointRow.id)
            .where(AgentTaskRow.user_id == self.parameters.user_id, AgentTaskRow.thread_id == self.parameters.thread_id)
            .order_by(AgentTaskRow.created_at.desc())
            .limit(1)
        )
        if origin and (selector == origin.get("target_checkpoint_id") or selector is None and own_accepted is None):
            source_thread = origin["source_thread_id"]
            selector = origin["source_checkpoint_id"]
            self.inputs["source_workspace_thread_id"] = source_thread
            self.inputs["source_workspace_checkpoint_id"] = selector
        if selector is None:
            accepted = own_accepted
            if accepted is not None:
                selector = accepted.checkpoint_id
                latest = await session.scalar(text("SELECT checkpoint_id FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1"), {"thread": self.parameters.thread_id})
                if latest != selector:
                    raise ConflictError("Latest checkpoint has no matching accepted remote workspace")
                self.inputs["source_workspace_checkpoint_id"] = selector
        if selector is not None:
            points = (
                (
                    await session.execute(
                        select(WorkspacePointRow)
                        .join(WorkspaceRequestRow, WorkspaceRequestRow.id == WorkspacePointRow.request_id)
                        .where(WorkspacePointRow.user_id == self.parameters.user_id, WorkspacePointRow.thread_id == source_thread, WorkspacePointRow.checkpoint_id == selector, WorkspaceRequestRow.state == "accepted")
                        .order_by(WorkspacePointRow.accepted_at.desc(), WorkspacePointRow.id)
                    )
                )
                .scalars()
                .all()
            )
            if not points:
                raise ConflictError("Checkpoint has no accepted remote workspace")
            source = points[0]
            root = (await session.execute(text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": source_thread, "checkpoint": selector})).scalar_one_or_none()
            if root is None or root.get("deerflow_execution_run_id") != source.run_id:
                raise ConflictError("Accepted source checkpoint execution conflicts")
            if origin and source_thread != self.parameters.thread_id and source.id != origin["point_id"]:
                raise ConflictError("Branch original accepted point conflicts")
            self.inputs["source_workspace_point_id"] = source.id
        if session.get_bind().dialect.name != "postgresql":
            raise RuntimeError("Remote admission requires PostgreSQL")
        # Serialize even an absent goal without locking a core run first.
        # The schema namespace isolates independent installations on one PG.
        key = json.dumps([self.parameters.user_id, self.parameters.thread_id])
        await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '|fleet-agent-goal|' || :key,0))"), {"key": key})
        self.task = (await session.execute(select(AgentTaskRow).where(AgentTaskRow.user_id == self.parameters.user_id, AgentTaskRow.thread_id == self.parameters.thread_id, text(AGENT_TASK_ACTIVE)).with_for_update())).scalar_one_or_none()
        if self.task is None:
            now = (await session.execute(text("SELECT clock_timestamp()"))).scalar_one()
            self.task = await AgentTasks().create(
                session,
                task_id="agent-" + uuid4().hex,
                user_id=self.parameters.user_id,
                thread_id=self.parameters.thread_id,
                deadline=now + timedelta(seconds=self.backend.profile.execution_timeout_seconds),
                continuation_budget=self.backend.continuation_budget,
            )

    def spec_for(self, *, run_id, agent_task_id, generation, execution_deadline):
        return build_launch_spec(
            profile_name=self.backend.profile_name, profile=self.backend.profile, run_parameters=self.inputs | {"run_id": run_id, "agent_task_id": agent_task_id, "generation": generation, "execution_deadline": execution_deadline}
        )

    async def insert(self, session, admitted_run):
        if admitted_run["user_id"] != self.parameters.user_id or admitted_run["thread_id"] != self.parameters.thread_id:
            raise ValueError("Run admission owner conflicts with launch identity")
        if self.resume_point is not None:
            # Same admission TX retains the original task/run/ledger locks.
            # Human input is not a B continuation and does not spend its budget.
            await self.validate_paused_resume(session, self.task)
            if self.inputs.get("source_workspace_point_id") != self.resume_point.id:
                raise ConflictError("Resume source workspace changed")
            self.task.generation += 1
            self.task.state = "queued"
        elif self.task.current_run_id is not None:
            raise ConflictError("Agent task already has a current run")
        spec = self.spec_for(run_id=admitted_run["run_id"], agent_task_id=self.task.id, generation=self.task.generation, execution_deadline=self.task.deadline)
        now = (await session.execute(text("SELECT clock_timestamp()"))).scalar_one()
        await RunPlacements().create(session, spec=spec, queue_deadline=min(spec.execution_deadline, now + timedelta(seconds=self.backend.config.queue_timeout_seconds)))

    async def validate_reuse(self, session, stored_run):
        if stored_run["user_id"] != self.parameters.user_id or stored_run["thread_id"] != self.parameters.thread_id or stored_run["owner_worker_id"] is not None and stored_run["kwargs"].get("execution_backend") != "fleet":
            raise ValueError("Run idempotency identity conflicts")
        try:
            spec = await RunPlacements().load_launch_spec(session, run_id=stored_run["run_id"], user_id=self.parameters.user_id, thread_id=self.parameters.thread_id)
        except LookupError:
            raise ValueError("Run idempotency backend conflicts") from None
        # Original immutable IDs/deadlines are reused; only stable request and
        # operator inputs are compared. A fresh clock/UUID is not a conflict.
        for name in ("source_workspace_point_id", "source_workspace_thread_id", "source_workspace_checkpoint_id"):
            if getattr(spec, name) is not None:
                self.inputs[name] = getattr(spec, name)
        expected = self.spec_for(run_id=spec.run_id, agent_task_id=spec.agent_task_id, generation=spec.generation, execution_deadline=spec.execution_deadline)
        if spec.payload_digest() != expected.payload_digest():
            raise ValueError("Run idempotency execution inputs conflict")


async def fleet_thread_admission_guard(session, *, user_id, thread_id, backend, requested_backend, operation, participant=None):
    """No lease/core-status shortcut releases an unfinished remote task."""
    if backend != "fleet":
        raise ConflictError("Thread execution backend is unsupported")
    tasks = (await session.execute(select(AgentTaskRow).where(AgentTaskRow.user_id == user_id, AgentTaskRow.thread_id == thread_id).order_by(AgentTaskRow.created_at, AgentTaskRow.id).with_for_update())).scalars().all()
    active = [task for task in tasks if task.state not in {"succeeded", "failed", "cancelled", "timed_out"}]
    if active:
        if operation != "run" or requested_backend != "fleet" or not isinstance(participant, FleetRunAdmission) or len(active) != 1:
            raise ConflictError("Thread has unfinished remote execution or recovery")
        await participant.validate_paused_resume(session, active[0])
    if operation == "artifact_write":
        raise ConflictError("Accepted remote workspace is immutable")
    if requested_backend != "fleet" and operation == "run":
        raise ConflictError("Thread requires its original remote execution backend")


class BoundFleetRunBackend:
    """Resolve human input on existing server bindings; never select initial routing."""

    def __init__(self, session_factory, config):
        self.sf, self.config = session_factory, config

    async def __call__(self, parameters):
        from deerflow_ecs_fleet.persistence.models import AttemptRow, RunPlacementRow, WorkspacePointRow

        from deerflow.persistence.run.model import ThreadExecutionBindingRow

        if not isinstance(parameters.graph_input, Command) or parameters.graph_input.resume is None:
            return None
        async with self.sf() as session:
            binding = await session.get(ThreadExecutionBindingRow, (parameters.user_id, parameters.thread_id))
            if binding is None or binding.backend != "fleet":
                return None
            task = await session.scalar(select(AgentTaskRow).where(AgentTaskRow.user_id == parameters.user_id, AgentTaskRow.thread_id == parameters.thread_id, text(AGENT_TASK_ACTIVE)))
            if binding.recovery_required or task is None or task.current_run_id is None:
                raise ConflictError("Bound remote human input requires its original task")
            spec = await RunPlacements().load_launch_spec(session, run_id=task.current_run_id, user_id=parameters.user_id, thread_id=parameters.thread_id)
            placement = await session.get(RunPlacementRow, spec.run_id)
            source = await session.get(WorkspacePointRow, spec.source_workspace_point_id) if spec.source_workspace_point_id else None
            attempt_id = placement.active_attempt_id or (source.attempt_id if source is not None else None)
            attempt = await session.get(AttemptRow, attempt_id) if attempt_id else None
            frozen_profile = attempt.launch_spec.get("execution_profile") if attempt is not None else None
            profile = self.config.profiles.get(spec.profile)
            if profile is None or frozen_profile != profile.model_dump(mode="json") or profile.runtime_digest != spec.runtime_digest or profile.cpu_millis != spec.resources.cpu_millis or profile.memory_mib != spec.resources.memory_mib:
                raise ConflictError("Bound remote execution profile changed")
            return FleetExecutionBackend(
                config=self.config,
                profile_name=spec.profile,
                model_name=spec.model_name,
                model_version=spec.model_version,
                skill_snapshot=spec.skill_snapshot,
                plugin_snapshot=spec.plugin_snapshot,
                workspace_manifest_ref=spec.workspace_manifest_ref,
                secret_refs=spec.secret_refs,
                continuation_budget=task.continuation_budget,
            )
