"""Trusted human intent joins the host's original admission transaction."""

import hashlib
import json
from datetime import timedelta
from uuid import uuid4

from deerflow_ecs_fleet.persistence.models import AttemptRow, JobLinkRow, JobRow, NodeRow, ReservationRow, RunPlacementRow, TaskOperationReceiptRow, WaitGroupRow
from deerflow_ecs_fleet.persistence.workspace_points import accepted_final
from sqlalchemy import select, text

from app.fleet.execution import FleetRunAdmission
from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.manager import ConflictError


class HumanOperationNeedsStop(ConflictError):
    """The final owned generation changed from waiting to an active run."""

    def __init__(self, task_id, generation):
        super().__init__("Human operation requires original active-run STOP")
        self.task_id, self.generation = task_id, generation


class FleetHumanRunAdmission(FleetRunAdmission):
    def __init__(self, backend, parameters, *, task_id, expected_generation, operation="message", idempotency_key=None, group_id=None, request_digest=None, allow_cancel=True):
        super().__init__(backend, parameters)
        self.task_id, self.expected_generation = task_id, expected_generation
        self.operation, self.operation_key = operation, idempotency_key or uuid4().hex
        self.allow_cancel = allow_cancel
        self.request_digest = request_digest
        self.result_group_id = group_id
        self.operation_receipt = self.operation_source = self.old_group = None
        self.jobs = []

    async def validate_human_operation(self, session, task):
        if task.id != self.task_id or (task.user_id, task.thread_id) != (self.parameters.user_id, self.parameters.thread_id) or task.generation != self.expected_generation:
            raise ConflictError("Task generation changed")
        pending = await session.scalar(
            select(TaskOperationReceiptRow).where(TaskOperationReceiptRow.agent_task_id == task.id, TaskOperationReceiptRow.operation == self.operation, TaskOperationReceiptRow.idempotency_key == self.operation_key).with_for_update()
        )
        self.pending_operation = pending
        if pending is None and self.operation in {"message", "checkpoint_write", "delete"} and task.state in {"queued", "running"} and task.cancel_requested_at is None:
            raise HumanOperationNeedsStop(task.id, task.generation)
        controlled = (
            pending is not None
            and pending.state == "requested"
            and (pending.user_id, pending.thread_id, pending.source_generation, pending.source_run_id, pending.request_digest) == (task.user_id, task.thread_id, self.expected_generation, task.current_run_id, self.request_digest)
        )
        if task.state not in ({"waiting_jobs", "succeeded"} if self.operation == "resume" else ({"waiting_jobs", "paused", "cancelled"} if controlled else {"waiting_jobs"})) or (
            task.cancel_requested_at is not None and not (controlled and task.state == "cancelled")
        ):
            raise ConflictError("Human operation requires its stopped waiting task")
        self.task = task
        group = await session.get(WaitGroupRow, self.result_group_id or task.wait_group_id or (pending.wait_group_id if controlled else None), with_for_update=True)
        if group is None or (group.agent_task_id, group.user_id, group.thread_id) != (task.id, task.user_id, task.thread_id) or group.generation > task.generation or group.policy != "all_settled":
            raise ConflictError("Original results identity conflicts")
        if self.operation != "resume" and not controlled and (group.generation, group.parent_run_id) != (task.generation, task.current_run_id):
            raise ConflictError("Original waiting identity conflicts")
        self.old_group = group
        run = await session.get(RunRow, task.current_run_id, with_for_update=True)
        placement = await session.get(RunPlacementRow, task.current_run_id, with_for_update=True)
        unassigned = controlled and task.state == "cancelled" and placement is not None and placement.active_attempt_id is None
        if unassigned:
            from deerflow_ecs_fleet.persistence.models import WorkspacePointRow

            from .operation_sources import unassigned_source

            source = await session.get(WorkspacePointRow, task.accepted_workspace_point_id)
            if (
                run is None
                or run.status != "interrupted"
                or run.cancel_action != "interrupt"
                or placement.state != "cancelled"
                or (placement.agent_task_id, placement.generation, placement.user_id, placement.thread_id) != (task.id, task.generation, task.user_id, task.thread_id)
                or source is None
            ):
                raise ConflictError("Unassigned human operation requires its original accepted source")
            run = await session.get(RunRow, source.run_id, with_for_update=True)
            placement = await session.get(RunPlacementRow, source.run_id, with_for_update=True)
            valid, self.preceding_receipt_id = await unassigned_source(session, task_id=task.id, user_id=task.user_id, thread_id=task.thread_id, generation=task.generation, run_id=task.current_run_id, point=source)
            if not valid:
                raise ConflictError("Unassigned source requires its exact trusted operation chain")
            if self.preceding_receipt_id is not None:
                preceding = await session.get(TaskOperationReceiptRow, self.preceding_receipt_id)
                if preceding.wait_group_id != group.id:
                    raise ConflictError("Original operation results group conflicts")
        if run is None or placement is None or placement.active_attempt_id is None:
            raise ConflictError("Original parent execution unavailable")
        locator = await session.get(AttemptRow, placement.active_attempt_id)
        if locator is None:
            raise ConflictError("Original parent attempt unavailable")
        self.jobs = []
        for job_id in sorted(group.job_ids):
            job = await session.get(JobRow, job_id, with_for_update=True, populate_existing=True)
            link = await session.get(JobLinkRow, job_id)
            if (
                job is None
                or link is None
                or link.link_mode != "awaited"
                or (link.agent_task_id, link.generation, link.parent_run_id, link.user_id, link.thread_id) != (task.id, group.generation, group.parent_run_id, task.user_id, task.thread_id)
            ):
                raise ConflictError("Original awaited membership conflicts")
            if (job.user_id, job.thread_id, job.source_run_id) != (task.user_id, task.thread_id, group.parent_run_id):
                raise ConflictError("Original awaited job owner conflicts")
            self.jobs.append(job)
        if not self.allow_cancel and any(job.state not in {"succeeded", "failed", "cancelled"} for job in self.jobs):
            from fastapi import HTTPException

            raise HTTPException(403, "Permission denied: runs:cancel")
        children = list((await session.scalars(select(AttemptRow).where(AttemptRow.job_id.in_(group.job_ids)).order_by(AttemptRow.id))).all())
        for node_id in sorted({locator.node_id, *(child.node_id for child in children)}):
            await session.get(NodeRow, node_id, with_for_update=True)
        reservations = {}
        for attempt_id in sorted({locator.id, *(child.id for child in children)}):
            reservations[attempt_id] = await session.scalar(select(ReservationRow).where(ReservationRow.attempt_id == attempt_id).with_for_update())
        attempts = {}
        for attempt_id in sorted(reservations):
            attempts[attempt_id] = await session.get(AttemptRow, attempt_id, with_for_update=True, populate_existing=True)
        attempt, reservation = attempts[locator.id], reservations[locator.id]
        if unassigned:
            from deerflow_ecs_fleet.persistence.workspace_points import accepted_source_identity

            point = await accepted_source_identity(session, point=source, run=run, placement=placement, attempt=attempt)
        else:
            point = await accepted_final(session, task=task, run=run, placement=placement, attempt=attempt)
        now = await session.scalar(text("SELECT clock_timestamp()"))
        unresolved = await session.scalar(
            select(AttemptRow.id)
            .join(RunPlacementRow, RunPlacementRow.run_id == AttemptRow.run_id)
            .where(RunPlacementRow.agent_task_id == task.id, (AttemptRow.stopped_at.is_(None)) | AttemptRow.state.in_(["unknown", "quarantined"]))
            .limit(1)
        )
        if (
            point is None
            or point.kind != (source.kind if unassigned else "paused" if controlled and task.state == "paused" else "final")
            or point.desired_task_status != (source.desired_task_status if unassigned else task.state)
            or placement.state != point.desired_placement_status
            or attempt.state != point.desired_placement_status
            or attempt.stopped_at is None
            or attempt.finished_at is None
            or attempt.process_ref != "fleet-" + attempt.id
            or reservation is None
            or reservation.state != "released"
            or reservation.released_at is None
            or unresolved is not None
            or task.deadline <= now
        ):
            raise ConflictError("Human operation requires exact accepted STOP and release")
        if self.operation != "resume" and not controlled and (point.kind != "final" or run.status != "success" or (point.id, point.checkpoint_id) != (group.workspace_point_id, group.checkpoint_id)):
            raise ConflictError("Original waiting accepted source conflicts")
        if self.operation == "resume":
            for child in children:
                current, released = attempts[child.id], reservations[child.id]
                if (
                    current.stopped_at is None
                    or current.finished_at is None
                    or current.state in {"unknown", "quarantined", "claimed", "starting", "running"}
                    or released is None
                    or released.state != "released"
                    or released.released_at is None
                ):
                    raise ConflictError("Result collection requires every original child STOP and release")
            if any(job.state not in {"succeeded", "failed", "cancelled"} or job.finished_at is None for job in self.jobs):
                raise ConflictError("Results have not settled")
            from deerflow_ecs_fleet.persistence.models import ArtifactManifestRow

            for job in self.jobs:
                if job.state == "succeeded":
                    manifest = await session.get(ArtifactManifestRow, job.accepted_manifest_id) if job.accepted_manifest_id else None
                    active = attempts.get(job.active_attempt_id)
                    if (
                        manifest is None
                        or active is None
                        or active.state != "succeeded"
                        or manifest.attempt_id != active.id
                        or (manifest.user_id, manifest.thread_id) != (job.user_id, job.thread_id)
                        or manifest.output_prefix != active.output_prefix + "/sealed"
                        or active.outcome != {"exit_code": 0, "stop_reason": "exit"}
                        or active.start_authorized_at is None
                        or active.process_ref != "fleet-" + active.id
                    ):
                        raise ConflictError("Successful child requires its original accepted result")
            from .continuations import graph_input, observed_results
            from .execution import encode_graph_input

            self.inputs["input"] = encode_graph_input(graph_input(group, await observed_results(session, group)))
        selector = self.parameters.normalized_config.get("configurable", {}).get("checkpoint_id")
        if selector is not None and selector != point.checkpoint_id:
            raise ConflictError("Human source conflicts with current accepted checkpoint")
        self.operation_source = point
        self.inputs["source_workspace_point_id"] = point.id
        self.inputs["source_workspace_checkpoint_id"] = point.checkpoint_id
        self.inputs["normalized_config"]["configurable"]["checkpoint_id"] = point.checkpoint_id

    async def prepare(self, session):
        if self.operation_source is None:
            raise ConflictError("Trusted human operation was not validated")
        from deerflow_ecs_fleet.job_service import FleetJobService

        jobs = FleetJobService(None, self.backend.config, tracking_reader=None)
        for job in self.jobs:
            await jobs.cancel_locked(session, job)
        task, point = self.task, self.operation_source
        losers = await session.scalars(
            select(TaskOperationReceiptRow).where(TaskOperationReceiptRow.agent_task_id == task.id, TaskOperationReceiptRow.source_generation == self.expected_generation, TaskOperationReceiptRow.state == "requested").with_for_update()
        )
        for other in losers:
            if other is not getattr(self, "pending_operation", None):
                other.state = "superseded"
        task.generation += 1
        task.state = "paused"
        task.cancel_requested_at = None
        task.wait_group_id = None
        self.operation_receipt = TaskOperationReceiptRow(
            id="operation-" + uuid4().hex,
            agent_task_id=task.id,
            user_id=task.user_id,
            thread_id=task.thread_id,
            operation=self.operation,
            idempotency_key=self.operation_key,
            request_digest=self.request_digest,
            source_point_generation=point.generation,
            preceding_receipt_id=getattr(self, "preceding_receipt_id", None),
            stopped_run_id=task.current_run_id if getattr(self, "pending_operation", None) is not None else None,
            source_generation=self.expected_generation,
            target_generation=task.generation,
            source_run_id=point.run_id,
            source_workspace_point_id=point.id,
            source_checkpoint_id=point.checkpoint_id,
            wait_group_id=self.old_group.id,
            state="admitted",
        )
        if getattr(self, "pending_operation", None) is not None:
            pending = self.pending_operation
            for field in ("source_point_generation", "preceding_receipt_id", "stopped_run_id", "source_run_id", "source_workspace_point_id", "source_checkpoint_id", "target_generation", "state", "wait_group_id"):
                setattr(pending, field, getattr(self.operation_receipt, field))
            self.operation_receipt = pending
        else:
            session.add(self.operation_receipt)
        await session.flush()

    async def insert(self, session, admitted_run):
        if (admitted_run["user_id"], admitted_run["thread_id"]) != (self.task.user_id, self.task.thread_id):
            raise ConflictError("Human admission owner conflicts")
        from deerflow_ecs_fleet.persistence.placements import RunPlacements
        from deerflow_ecs_fleet.task_budgets import TaskBudgetExceeded, charge

        try:
            await charge(session, task=self.task, kind="run", logical_id=admitted_run["run_id"], config=self.backend.config, source_generation=self.expected_generation)
        except TaskBudgetExceeded as error:
            raise ConflictError(str(error)) from error
        spec = self.spec_for(run_id=admitted_run["run_id"], agent_task_id=self.task.id, generation=self.task.generation, execution_deadline=self.task.deadline)
        now = await session.scalar(text("SELECT clock_timestamp()"))
        self.task.state = "queued"
        await RunPlacements().create(session, spec=spec, queue_deadline=min(spec.execution_deadline, now + timedelta(seconds=self.backend.config.queue_timeout_seconds)))
        self.operation_receipt.admitted_run_id = admitted_run["run_id"]
        await session.flush()

    async def validate_reuse(self, session, stored_run):
        receipt = await session.scalar(
            select(TaskOperationReceiptRow).where(TaskOperationReceiptRow.agent_task_id == self.task_id, TaskOperationReceiptRow.operation == self.operation, TaskOperationReceiptRow.idempotency_key == self.operation_key)
        )
        if (
            receipt is None
            or receipt.request_digest != self.request_digest
            or receipt.state != "admitted"
            or receipt.target_generation != receipt.source_generation + 1
            or (stored_run["user_id"], stored_run["thread_id"], stored_run.get("kwargs", {}).get("execution_backend")) != (self.parameters.user_id, self.parameters.thread_id, "fleet")
            or (receipt.user_id, receipt.thread_id, receipt.source_generation, receipt.admitted_run_id) != (self.parameters.user_id, self.parameters.thread_id, self.expected_generation, stored_run["run_id"])
        ):
            raise ConflictError("Original task operation reuse conflicts")


async def owned_resume_backend(ownership, *, user_id, thread_id, task_id, expected_generation, idempotency_key, operation="resume"):
    """Owned result collection separates current source from old results."""
    from deerflow_ecs_fleet.persistence.models import AgentTaskRow, WorkspacePointRow
    from deerflow_ecs_fleet.persistence.placements import RunPlacements

    from .execution import FleetExecutionBackend

    async with ownership.sf() as session:
        task = await session.get(AgentTaskRow, task_id)
        if task is None or (task.user_id, task.thread_id) != (user_id, thread_id):
            raise LookupError("Agent task not found")
        receipt = await session.scalar(select(TaskOperationReceiptRow).where(TaskOperationReceiptRow.agent_task_id == task.id, TaskOperationReceiptRow.operation == operation, TaskOperationReceiptRow.idempotency_key == idempotency_key))
        if receipt is not None:
            if receipt.source_generation != expected_generation:
                raise ConflictError("Original resume generation conflicts")
            source_run_id, group_id = receipt.source_run_id, receipt.wait_group_id
        else:
            if task.generation != expected_generation:
                raise ConflictError("Task generation changed")
            source_run_id = task.current_run_id
            old = await session.scalar(select(TaskOperationReceiptRow).where(TaskOperationReceiptRow.agent_task_id == task.id, TaskOperationReceiptRow.wait_group_id.is_not(None)).order_by(TaskOperationReceiptRow.created_at.desc()).limit(1))
            group_id = task.wait_group_id or (old.wait_group_id if old is not None else None)
        if source_run_id is None or group_id is None:
            raise ConflictError("Original accepted results unavailable")
        spec = await RunPlacements().load_launch_spec(session, run_id=source_run_id, user_id=user_id, thread_id=thread_id)
        placement = await session.get(RunPlacementRow, source_run_id)
        source_point = await session.get(WorkspacePointRow, spec.source_workspace_point_id) if spec.source_workspace_point_id else None
        attempt_id = placement.active_attempt_id if placement and placement.active_attempt_id else source_point.attempt_id if source_point else None
        attempt = await session.get(AttemptRow, attempt_id) if attempt_id else None
        profile = ownership.config.profiles.get(spec.profile)
        if attempt is None or profile is None or attempt.launch_spec.get("execution_profile") != profile.model_dump(mode="json"):
            raise ConflictError("Original resume profile conflicts")
        backend = FleetExecutionBackend(
            config=ownership.config,
            profile_name=spec.profile,
            model_name=spec.model_name,
            model_version=spec.model_version,
            skill_snapshot=spec.skill_snapshot,
            plugin_snapshot=spec.plugin_snapshot,
            workspace_manifest_ref=spec.workspace_manifest_ref,
            secret_refs=spec.secret_refs,
            continuation_budget=task.continuation_budget,
        )
        backend.human_operation = dict(task_id=task_id, expected_generation=expected_generation, operation=operation, idempotency_key=idempotency_key, group_id=group_id)
        return backend


def message_digest(parameters):
    value = dict(
        parameters.public_kwargs,
        assistant_id=parameters.assistant_id,
        stream_modes=list(parameters.stream_modes),
        stream_subgraphs=parameters.stream_subgraphs,
        interrupt_before=parameters.interrupt_before,
        interrupt_after=parameters.interrupt_after,
    )
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def scoped_operation_key(user_id, thread_id, key):
    return "fleet-message:" + hashlib.sha256(json.dumps([user_id, thread_id, key], separators=(",", ":")).encode()).hexdigest()


async def owned_message_backend(resolver, parameters, key, *, allow_fresh=True, operation="message"):
    """Resolve a lost reply before looking at the task's changed state."""
    from types import SimpleNamespace

    from deerflow_ecs_fleet.persistence.models import AgentTaskRow

    digest = message_digest(parameters)
    async with resolver.sf() as session:
        receipt = await session.scalar(
            select(TaskOperationReceiptRow).where(
                TaskOperationReceiptRow.user_id == parameters.user_id, TaskOperationReceiptRow.thread_id == parameters.thread_id, TaskOperationReceiptRow.operation == operation, TaskOperationReceiptRow.idempotency_key == key
            )
        )
        if receipt is not None:
            if receipt.state not in {"admitted", "requested"} or receipt.request_digest != digest or (not allow_fresh and receipt.state != "admitted"):
                raise ConflictError("Original human message identity conflicts")
            task = await session.get(AgentTaskRow, receipt.agent_task_id)
            if task is None or (task.user_id, task.thread_id) != (parameters.user_id, parameters.thread_id):
                raise ConflictError("Original human message owner conflicts")
            # Reuse the exact old launch/profile. No new STOP or generation grant.
            backend = await owned_resume_backend(
                SimpleNamespace(sf=resolver.sf, config=resolver.config), user_id=parameters.user_id, thread_id=parameters.thread_id, task_id=task.id, expected_generation=receipt.source_generation, idempotency_key=key, operation=operation
            )
            backend.operation_reused = receipt.state == "admitted"
            backend.human_operation["request_digest"] = digest
            return backend
    if not allow_fresh:
        raise ConflictError("Human event expired without an original admitted receipt")
    backend = await resolver.resolve_human(parameters)
    if backend is not None and getattr(backend, "human_operation", None):
        backend.human_operation.update(idempotency_key=key, request_digest=digest, operation=operation)
    return backend


class FleetNeutralAdmission(FleetHumanRunAdmission):
    """Intent/reservation are atomic; subsequent host mutation is another TX."""

    admission_backend = "fleet"

    async def insert(self, session, admitted_run):
        self.operation_receipt.admitted_run_id = admitted_run["run_id"]
        self.operation_receipt.state = "reserved"
        from deerflow.persistence.run.model import ThreadExecutionBindingRow

        binding = await session.get(ThreadExecutionBindingRow, (self.parameters.user_id, self.parameters.thread_id), with_for_update=True)
        if binding is None:
            raise ConflictError("Original neutral binding unavailable")
        binding.recovery_required = True
        await session.flush()

    async def finish_operation(self, error):
        from deerflow.persistence.run.model import ThreadExecutionBindingRow

        async with self.sf.begin() as session:
            from .execution import fleet_before_thread_guard

            await fleet_before_thread_guard(session, user_id=self.parameters.user_id, thread_id=self.parameters.thread_id)
            receipt = await session.get(TaskOperationReceiptRow, self.operation_receipt.id, with_for_update=True)
            binding = await session.get(ThreadExecutionBindingRow, (self.parameters.user_id, self.parameters.thread_id), with_for_update=True)
            # No host-only checkpoint/delete can publish a paired remote workspace.
            # Even unchanged old root has older generation after intent committed.
            if receipt is not None:
                receipt.state = "blocked"
                if self.operation == "delete" and error is None:
                    from deerflow_ecs_fleet.persistence.models import AgentTaskRow

                    root = await session.scalar(text("SELECT checkpoint_id FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' LIMIT 1"), {"thread": self.parameters.thread_id})
                    metadata = await self.thread_store.get(self.parameters.thread_id)
                    if root is None and metadata is None:
                        task = await session.get(AgentTaskRow, self.task_id, with_for_update=True)
                        if task is not None and task.generation == receipt.target_generation:
                            task.state = "cancelled"
                            receipt.state = "completed"
            if binding is not None:
                binding.recovery_required = True


class FleetMutationObservation:
    """Early permission/identity observation never commits supersession."""

    observe_human_mutation = True


async def neutral_participant(request, thread_id, operation, user_id, *, operation_key=None, expected=None):
    from app.gateway.auth_disabled import AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_PAT, AUTH_SOURCE_SESSION
    from deerflow.runtime.execution.contracts import RunExecutionParameters

    from .execution import BoundFleetRunBackend

    if getattr(request.state, "auth_source", None) not in {AUTH_SOURCE_SESSION, AUTH_SOURCE_PAT, AUTH_SOURCE_AUTH_DISABLED}:
        return None
    ownership = getattr(request.app.state, "fleet_ownership", None)
    if ownership is None:
        return None
    parameters = RunExecutionParameters(
        thread_id=thread_id,
        assistant_id=None,
        user_id=user_id,
        graph_input={"messages": []},
        normalized_config={"configurable": {}, "context": {}},
        stream_modes=(),
        stream_subgraphs=False,
        interrupt_before=None,
        interrupt_after=None,
        public_kwargs={"operation": operation, "body_digest": hashlib.sha256(await request.body()).hexdigest()},
        model_name=None,
    )
    resolver = BoundFleetRunBackend(ownership.sf, ownership.config)
    key = operation_key or request.headers.get("Idempotency-Key") or uuid4().hex
    if len(key) > 128:
        from fastapi import HTTPException

        raise HTTPException(400, "Idempotency-Key must contain 1 to 128 characters")
    from .task_operations import stop_before_human

    # Only original authenticated server input reaches this operation factory.
    await stop_before_human(ownership, parameters, key, request, operation=operation, digest=message_digest(parameters), expected=expected)
    backend = await owned_message_backend(resolver, parameters, key, operation=operation)
    if backend is None:
        return None
    await bind_cancel_permission(request, backend)
    from dataclasses import replace

    from deerflow_ecs_fleet.launch_spec import thaw
    from deerflow_ecs_fleet.persistence.models import AgentTaskRow
    from deerflow_ecs_fleet.persistence.placements import RunPlacements

    async with ownership.sf() as session:
        task = await session.get(AgentTaskRow, backend.human_operation["task_id"])
        spec = await RunPlacements().load_launch_spec(session, run_id=task.current_run_id, user_id=user_id, thread_id=thread_id)
    parameters = replace(parameters, normalized_config=thaw(spec.normalized_config))
    participant = FleetNeutralAdmission(backend, parameters, **dict(backend.human_operation, operation=operation))
    participant.sf = ownership.sf
    if operation == "delete":
        from app.gateway.deps import get_thread_store

        participant.thread_store = get_thread_store(request)
    return participant


async def bind_cancel_permission(request, backend):
    """Server auth state is carried into the original locked admission guard."""
    human = getattr(backend, "human_operation", None)
    if human:
        auth = getattr(request.state, "auth", None)
        human["allow_cancel"] = auth is None or auth.has_permission("runs", "cancel")
