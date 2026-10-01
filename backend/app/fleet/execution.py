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
        self.inputs = {
            "schema_version": 1,
            "user_id": parameters.user_id,
            "thread_id": parameters.thread_id,
            "assistant_id": parameters.assistant_id or "lead_agent",
            "model_name": backend.model_name,
            "model_version": backend.model_version,
            "input": encode_graph_input(parameters.graph_input),
            "normalized_config": parameters.normalized_config,
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

    async def prepare(self, session):
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
        if self.task.current_run_id is not None:
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
        expected = self.spec_for(run_id=spec.run_id, agent_task_id=spec.agent_task_id, generation=spec.generation, execution_deadline=spec.execution_deadline)
        if spec.payload_digest() != expected.payload_digest():
            raise ValueError("Run idempotency execution inputs conflict")
